// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rulekit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var defaultExtractors = map[schema.GroupVersionKind]func(client.Object, string) (any, error){
	corev1.SchemeGroupVersion.WithKind("Node"): func(obj client.Object, fieldManager string) (any, error) {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return nil, fmt.Errorf("expected *corev1.Node, got %T", obj)
		}
		return corev1ac.ExtractNode(node, fieldManager)
	},
}

type executor struct {
	fieldManager string
	dryRun       bool
	apiReader    client.Reader
	writer       client.Writer
	recorder     record.EventRecorder
	extractors   map[schema.GroupVersionKind]func(client.Object, string) (any, error)

	// warned holds, per key, the warnings emitted while they stay asserted.
	warnedMu sync.Mutex
	warned   map[Key]map[warningID]bool
}

func newExecutor(
	fieldManager string,
	dryRun bool,
	apiReader client.Reader,
	writer client.Writer,
	recorder record.EventRecorder,
) *executor {
	return &executor{
		fieldManager: fieldManager,
		dryRun:       dryRun,
		apiReader:    apiReader,
		writer:       writer,
		recorder:     recorder,
		extractors:   defaultExtractors,
		warned:       make(map[Key]map[warningID]bool),
	}
}

type plannedObjectDiff struct {
	po           PlannedObject
	obsObj       client.Object
	exists       bool
	changed      bool
	extractedMap map[string]any
}

type plannedTaintDiff struct {
	pt      PlannedTaint
	obsNode *corev1.Node
	changed bool
}

type diffResult struct {
	objects []plannedObjectDiff
	taints  []plannedTaintDiff
}

// Execute applies the planned effects against the cluster in structured steps:
// warnings -> diff -> dry-run -> preconditions -> applies -> taints ->
// deletes -> finalizers -> taint removals -> wake.
func (e *executor) Execute(ctx context.Context, rd *memoReader, plan *Plan) (reconcile.Result, error) {
	// 0. Warnings: Events, not cluster state, so they skip everything below.
	e.emitWarnings(ctx, plan.Key, plan.Warnings)

	// 1. Diff / No-op detection
	diffs, allNoop, err := e.diff(ctx, rd, plan)
	if err != nil {
		return reconcile.Result{}, err
	}
	finalizers := pendingFinalizerRemovals(plan.Finalizers)
	untaints := pendingTaintRemovals(plan.TaintRemovals)
	if allNoop && len(plan.Deletes) == 0 && len(finalizers) == 0 && len(untaints) == 0 {
		return wakeResult(plan.Wake, rd.Now()), nil
	}

	// 2. DryRun
	if e.dryRun {
		e.logDryRun(ctx, diffs, plan, finalizers, untaints)
		return wakeResult(plan.Wake, rd.Now()), nil
	}

	// 3. Preconditions
	if res, stop, err := e.checkPreconditions(ctx, plan.Preconditions); err != nil || stop {
		return res, err
	}

	// 4. Applies (changed only)
	applyOut, err := e.executeApplies(ctx, diffs.objects)
	if err != nil || applyOut.stop {
		return applyOut.result, err
	}

	// 5. Taints (changed only, Node objects)
	if res, stop, err := e.executeTaints(ctx, diffs.taints, applyOut.latestRVs); err != nil || stop {
		return res, err
	}

	// 6. Deletes
	if res, stop, err := e.executeDeletes(ctx, plan.Deletes); err != nil || stop {
		return res, err
	}

	// 7. Finalizers (after Deletes, so a deleted object can go away)
	if res, stop, err := e.executeFinalizerRemovals(ctx, finalizers); err != nil || stop {
		return res, err
	}

	// 8. Taint removals (taints owned by others, whose owner is gone)
	if res, stop, err := e.executeTaintRemovals(ctx, untaints); err != nil || stop {
		return res, err
	}

	// 9. Wake
	return wakeResult(plan.Wake, rd.Now()), nil
}

// 1. diff evaluates each planned object and taint against observed state.
func (e *executor) diff(ctx context.Context, rd *memoReader, plan *Plan) (diffResult, bool, error) {
	objectDiffs := make([]plannedObjectDiff, len(plan.Objects))
	allObjectsNoop := true
	for idx := range plan.Objects {
		objDiff, err := e.diffPlannedObject(ctx, rd, &plan.Objects[idx])
		if err != nil {
			return diffResult{}, false, err
		}
		if objDiff.changed {
			allObjectsNoop = false
		}
		objectDiffs[idx] = objDiff
	}

	taintDiffs := make([]plannedTaintDiff, len(plan.Taints))
	allTaintsNoop := true
	for idx := range plan.Taints {
		taintDiff, err := diffPlannedTaint(ctx, rd, &plan.Taints[idx])
		if err != nil {
			return diffResult{}, false, err
		}
		if taintDiff.changed {
			allTaintsNoop = false
		}
		taintDiffs[idx] = taintDiff
	}

	res := diffResult{objects: objectDiffs, taints: taintDiffs}
	allNoop := allObjectsNoop && allTaintsNoop
	return res, allNoop, nil
}

func (e *executor) diffPlannedObject(ctx context.Context, rd *memoReader, po *PlannedObject) (plannedObjectDiff, error) {
	obsObj, exists, err := rd.observed(ctx, &po.Ref)
	if err != nil {
		return plannedObjectDiff{}, fmt.Errorf("observe %s: %w", po.Ref, err)
	}

	normDesired := normalize(po.Desired)
	isIdentityOnly := len(normDesired) == 0

	if !exists {
		return plannedObjectDiff{
			po:      *po,
			obsObj:  obsObj,
			exists:  false,
			changed: !isIdentityOnly, // Object absent and Desired identity-only: no-op.
		}, nil
	}

	extractor, ok := e.extractors[po.Ref.GVK]
	if !ok {
		// Unregistered GVK: treat as changed (always apply).
		return plannedObjectDiff{
			po:      *po,
			obsObj:  obsObj,
			exists:  true,
			changed: true,
		}, nil
	}

	ac, err := extractor(obsObj, e.fieldManager)
	if err != nil {
		return plannedObjectDiff{}, fmt.Errorf("extract %s: %w", po.Ref, err)
	}
	var extractedMap map[string]any
	if ac != nil {
		data, err := json.Marshal(ac)
		if err != nil {
			return plannedObjectDiff{}, fmt.Errorf("marshal extracted %s: %w", po.Ref, err)
		}
		if err := json.Unmarshal(data, &extractedMap); err != nil {
			return plannedObjectDiff{}, fmt.Errorf("unmarshal extracted %s: %w", po.Ref, err)
		}
	}
	normExtracted := normalize(extractedMap)
	return plannedObjectDiff{
		po:           *po,
		obsObj:       obsObj,
		exists:       true,
		changed:      !reflect.DeepEqual(normExtracted, normDesired),
		extractedMap: extractedMap,
	}, nil
}

func diffPlannedTaint(ctx context.Context, rd *memoReader, pt *PlannedTaint) (plannedTaintDiff, error) {
	nodeRef := Ref{
		GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
		Name: pt.Node,
	}
	obsNodeObj, exists, err := rd.observed(ctx, &nodeRef)
	if err != nil {
		return plannedTaintDiff{}, fmt.Errorf("observe node %s for taint: %w", pt.Node, err)
	}
	if !exists || obsNodeObj == nil {
		// Taints on absent nodes: if the node isn't observed, a PlannedTaint
		// must be a no-op (nothing to taint/untaint) instead of patching a missing node.
		return plannedTaintDiff{pt: *pt, changed: false}, nil
	}

	obsNode, existingTaint, hasTaint := findObservedNodeTaint(obsNodeObj, pt.Key, pt.Effect)
	changed := hasTaint != pt.Present
	if !changed && pt.Present && pt.Taint != nil && existingTaint != nil {
		changed = existingTaint.Value != pt.Taint.Value
	}
	return plannedTaintDiff{
		pt:      *pt,
		obsNode: obsNode,
		changed: changed,
	}, nil
}

func findObservedNodeTaint(obsNodeObj client.Object, key string, effect corev1.TaintEffect) (*corev1.Node, *corev1.Taint, bool) {
	n, ok := obsNodeObj.(*corev1.Node)
	if !ok {
		return nil, nil, false
	}
	for _, t := range n.Spec.Taints {
		if t.Key == key && t.Effect == effect {
			tCopy := t
			return n, &tCopy, true
		}
	}
	return n, nil, false
}

// 2. logDryRun logs every would-be write without performing API calls.
func (e *executor) logDryRun(
	ctx context.Context,
	diffs diffResult,
	plan *Plan,
	finalizers []Attributed[FinalizerRemoval],
	untaints []Attributed[TaintRemoval],
) {
	logger := log.FromContext(ctx)
	for i := range diffs.objects {
		d := &diffs.objects[i]
		if !d.changed {
			continue
		}
		if len(d.po.Rules) == 0 {
			logger.Info("would apply (releasing)", "ref", d.po.Ref, "content", d.po.Desired)
		} else {
			logger.Info("would apply", "ref", d.po.Ref, "rules", d.po.Rules, "content", d.po.Desired)
		}
	}
	for i := range diffs.taints {
		d := &diffs.taints[i]
		if !d.changed {
			continue
		}
		if d.pt.Present {
			logger.Info("would taint",
				"node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect,
				"taint", d.pt.Taint, "rules", d.pt.Rules)
			continue
		}
		if len(d.pt.Rules) == 0 {
			logger.Info("would untaint",
				"node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect,
				"reason", "no rule asserts it")
		} else {
			logger.Info("would untaint",
				"node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect,
				"rules", d.pt.Rules)
		}
	}
	for _, del := range plan.Deletes {
		logger.Info("would delete", "rule", del.Rule, "object", del.Value)
	}
	for _, fin := range finalizers {
		logger.Info("would remove finalizer", "rule", fin.Rule, "object", fin.Value.Object.GetName(), "finalizer", fin.Value.Finalizer)
	}
	for _, rt := range untaints {
		logger.Info("would remove taint", "rule", rt.Rule, "node", rt.Value.Node.GetName(), "taint", rt.Value.Taint.ToString())
	}
}

// 3. checkPreconditions checks live preconditions against apiReader.
func (e *executor) checkPreconditions(ctx context.Context, pres []Attributed[Precondition]) (reconcile.Result, bool, error) {
	logger := log.FromContext(ctx)
	for _, prec := range pres {
		if err := prec.Value.Check(ctx, e.apiReader); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				logger.Info("precondition failed", "rule", prec.Rule, "precondition", prec.Value.Description, "error", err)
				return reconcile.Result{RequeueAfter: 1 * time.Second}, true, nil
			}
			return reconcile.Result{}, false, err
		}
	}
	return reconcile.Result{}, false, nil
}

type applyResult struct {
	latestRVs map[Ref]string
	result    reconcile.Result
	stop      bool
}

// 4. executeApplies runs server-side apply for changed objects.
func (e *executor) executeApplies(ctx context.Context, objectDiffs []plannedObjectDiff) (applyResult, error) {
	latestRVs := make(map[Ref]string)
	logger := log.FromContext(ctx)

	for i := range objectDiffs {
		objDiff := &objectDiffs[i]
		if !objDiff.changed {
			continue
		}

		eventMsg := buildAppliedEventMessage(objDiff.po.Desired, objDiff.extractedMap, objDiff.po.Rules)

		unstrObj := &unstructured.Unstructured{
			Object: runtime.DeepCopyJSON(objDiff.po.Desired),
		}

		if objDiff.exists && objDiff.obsObj != nil {
			// SSA is an upsert; setting metadata.uid makes the apiserver reject
			// re-creating a deleted object if it was deleted between our read and apply
			// (verified on a real apiserver). Setting resourceVersion provides optimistic
			// concurrency against the observed snapshot.
			unstrObj.SetUID(objDiff.obsObj.GetUID())
			unstrObj.SetResourceVersion(objDiff.obsObj.GetResourceVersion())
		}

		err := e.writer.Patch(ctx, unstrObj, client.Apply, client.FieldOwner(e.fieldManager), client.ForceOwnership)
		if err != nil {
			if isStaleObservation(err) {
				logger.Info("stale observation, requeueing", "ref", objDiff.po.Ref, "error", err)
				return applyResult{result: reconcile.Result{Requeue: true}, stop: true}, nil
			}
			return applyResult{}, fmt.Errorf("apply %s: %w", objDiff.po.Ref, err)
		}

		rv := unstrObj.GetResourceVersion()
		latestRVs[objDiff.po.Ref] = rv

		if e.recorder != nil {
			e.recorder.Eventf(unstrObj, corev1.EventTypeNormal, "Applied", "%s", eventMsg)
		}

		if len(objDiff.po.Rules) == 0 {
			logger.Info("applied object (releasing)", "ref", objDiff.po.Ref, "resourceVersion", rv)
		} else {
			logger.Info("applied object", "ref", objDiff.po.Ref, "rules", objDiff.po.Rules, "resourceVersion", rv)
		}
	}

	return applyResult{latestRVs: latestRVs}, nil
}

// 5. executeTaints updates node taints via JSON merge patch.
func (e *executor) executeTaints(
	ctx context.Context,
	taintDiffs []plannedTaintDiff,
	latestRVs map[Ref]string,
) (reconcile.Result, bool, error) {
	currentTaintsByNode := make(map[string][]corev1.Taint)
	for _, d := range taintDiffs {
		if d.obsNode != nil {
			if _, exists := currentTaintsByNode[d.pt.Node]; !exists {
				currentTaintsByNode[d.pt.Node] = append([]corev1.Taint(nil), d.obsNode.Spec.Taints...)
			}
		}
	}

	logger := log.FromContext(ctx)

	for _, taintDiff := range taintDiffs {
		if !taintDiff.changed {
			continue
		}

		nodeRef := Ref{
			GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
			Name: taintDiff.pt.Node,
		}

		rv := latestRVs[nodeRef]
		if rv == "" && taintDiff.obsNode != nil {
			rv = taintDiff.obsNode.GetResourceVersion()
		}

		// Taints are listType=atomic in the Kubernetes Node schema. Server-side apply would
		// claim ownership of the entire taint list, clobbering or conflicting with taints
		// managed by other actors (e.g. kubelet, cloud-provider). We instead use a
		// read-modify-write JSON merge patch with optimistic concurrency via metadata.resourceVersion.
		currentList := currentTaintsByNode[taintDiff.pt.Node]
		var newTaints []corev1.Taint
		for _, t := range currentList {
			if t.Key == taintDiff.pt.Key && t.Effect == taintDiff.pt.Effect {
				continue
			}
			newTaints = append(newTaints, t)
		}
		if taintDiff.pt.Present && taintDiff.pt.Taint != nil {
			newTaints = append(newTaints, *taintDiff.pt.Taint)
		}

		patchData := map[string]any{
			"metadata": map[string]any{
				"resourceVersion": rv,
			},
		}
		if len(newTaints) == 0 {
			patchData["spec"] = map[string]any{
				"taints": nil,
			}
		} else {
			patchData["spec"] = map[string]any{
				"taints": newTaints,
			}
		}

		patchBytes, err := json.Marshal(patchData)
		if err != nil {
			return reconcile.Result{}, false, fmt.Errorf("marshal taint patch for %s: %w", taintDiff.pt.Node, err)
		}

		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: taintDiff.pt.Node,
			},
		}
		err = e.writer.Patch(ctx, nodeObj, client.RawPatch(types.MergePatchType, patchBytes), client.FieldOwner(e.fieldManager))
		if err != nil {
			if isStaleObservation(err) {
				logger.Info("stale observation, requeueing", "node", taintDiff.pt.Node, "error", err)
				return reconcile.Result{Requeue: true}, true, nil
			}
			return reconcile.Result{}, false, fmt.Errorf("patch node taints for %s: %w", taintDiff.pt.Node, err)
		}

		latestRVs[nodeRef] = nodeObj.GetResourceVersion()
		currentTaintsByNode[taintDiff.pt.Node] = newTaints

		e.recordTaintChange(ctx, nodeObj, &taintDiff.pt)
	}

	return reconcile.Result{}, false, nil
}

func (e *executor) recordTaintChange(ctx context.Context, nodeObj *corev1.Node, pt *PlannedTaint) {
	logger := log.FromContext(ctx)
	if pt.Present && pt.Taint != nil {
		if e.recorder != nil {
			msg := fmt.Sprintf(
				"Tainted %s=%s:%s by rules [%s]",
				pt.Taint.Key, pt.Taint.Value, pt.Taint.Effect, strings.Join(pt.Rules, ", "))
			e.recorder.Eventf(nodeObj, corev1.EventTypeNormal, "Tainted", "%s", msg)
		}
		logger.Info("tainted node", "node", pt.Node, "key", pt.Key, "effect", pt.Effect, "rules", pt.Rules)
		return
	}
	if e.recorder != nil {
		var msg string
		if len(pt.Rules) == 0 {
			msg = fmt.Sprintf("Untainted %s:%s: no rule asserts it", pt.Key, pt.Effect)
		} else {
			msg = fmt.Sprintf("Untainted %s:%s by rules [%s]", pt.Key, pt.Effect, strings.Join(pt.Rules, ", "))
		}
		e.recorder.Eventf(nodeObj, corev1.EventTypeNormal, "Untainted", "%s", msg)
	}
	if len(pt.Rules) == 0 {
		logger.Info("untainted node", "node", pt.Node, "key", pt.Key, "effect", pt.Effect, "reason", "no rule asserts it")
	} else {
		logger.Info("untainted node", "node", pt.Node, "key", pt.Key, "effect", pt.Effect, "rules", pt.Rules)
	}
}

// 6. executeDeletes deletes unowned objects.
func (e *executor) executeDeletes(ctx context.Context, deletes []Attributed[client.Object]) (reconcile.Result, bool, error) {
	logger := log.FromContext(ctx)
	for _, del := range deletes {
		uid := del.Value.GetUID()
		var deleteOpts []client.DeleteOption
		if uid != "" {
			deleteOpts = append(deleteOpts, client.Preconditions{UID: &uid})
		}
		err := e.writer.Delete(ctx, del.Value, deleteOpts...)
		switch {
		case apierrors.IsNotFound(err):
			continue
		case isStaleObservation(err):
			logger.Info("stale observation, requeueing", "object", del.Value.GetName(), "error", err)
			return reconcile.Result{Requeue: true}, true, nil
		case err != nil:
			return reconcile.Result{}, false, fmt.Errorf("delete %s: %w", del.Value.GetName(), err)
		}
		if e.recorder != nil {
			e.recorder.Eventf(del.Value, corev1.EventTypeNormal, "Deleted", "Deleted by rule %s", del.Rule)
		}
		logger.Info("deleted object", "object", del.Value.GetName(), "rule", del.Rule)
	}
	return reconcile.Result{}, false, nil
}

// pendingFinalizerRemovals drops removals whose object, as the rules saw it,
// does not carry the finalizer.
func pendingFinalizerRemovals(removals []Attributed[FinalizerRemoval]) []Attributed[FinalizerRemoval] {
	var pending []Attributed[FinalizerRemoval]
	for _, fin := range removals {
		if slices.Contains(fin.Value.Object.GetFinalizers(), fin.Value.Finalizer) {
			pending = append(pending, fin)
		}
	}
	return pending
}

// 7. executeFinalizerRemovals strips finalizers with a JSON patch that first
// tests the object's UID and the finalizer's position, so it never edits a
// newer object with the same name or removes the wrong entry.
func (e *executor) executeFinalizerRemovals(
	ctx context.Context,
	removals []Attributed[FinalizerRemoval],
) (reconcile.Result, bool, error) {
	logger := log.FromContext(ctx)
	for _, fin := range removals {
		obj, finalizer := fin.Value.Object, fin.Value.Finalizer
		path := fmt.Sprintf("/metadata/finalizers/%d", slices.Index(obj.GetFinalizers(), finalizer))
		patch, err := json.Marshal([]map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": string(obj.GetUID())},
			{"op": "test", "path": path, "value": finalizer},
			{"op": "remove", "path": path},
		})
		if err != nil {
			return reconcile.Result{}, false, fmt.Errorf("marshal finalizer patch for %s: %w", obj.GetName(), err)
		}
		// Patch decodes the response into its argument; keep the observed object intact.
		target, ok := obj.DeepCopyObject().(client.Object)
		if !ok {
			return reconcile.Result{}, false, fmt.Errorf("type %T does not implement client.Object", obj)
		}
		err = e.writer.Patch(ctx, target, client.RawPatch(types.JSONPatchType, patch), client.FieldOwner(e.fieldManager))
		switch {
		case apierrors.IsNotFound(err):
			continue
		case apierrors.IsConflict(err) || apierrors.IsInvalid(err):
			// A failed JSON patch test is Invalid: the object changed since
			// the rules saw it.
			logger.Info("stale observation, requeueing", "object", obj.GetName(), "error", err)
			return reconcile.Result{Requeue: true}, true, nil
		case err != nil:
			return reconcile.Result{}, false, fmt.Errorf("remove finalizer %s from %s: %w", finalizer, obj.GetName(), err)
		}
		if e.recorder != nil {
			e.recorder.Eventf(obj, corev1.EventTypeNormal, "FinalizerRemoved", "Removed finalizer %s by rule %s", finalizer, fin.Rule)
		}
		logger.Info("removed finalizer", "object", obj.GetName(), "finalizer", finalizer, "rule", fin.Rule)
	}
	return reconcile.Result{}, false, nil
}

// pendingTaintRemovals drops removals whose node, as the rules saw it, does
// not carry the taint.
func pendingTaintRemovals(removals []Attributed[TaintRemoval]) []Attributed[TaintRemoval] {
	var pending []Attributed[TaintRemoval]
	for _, rt := range removals {
		if taintIndex(rt.Value.Node, rt.Value.Taint) >= 0 {
			pending = append(pending, rt)
		}
	}
	return pending
}

// taintIndex returns the position of taint in node's taints, compared in
// full (key, value, effect and timeAdded), or -1.
func taintIndex(node *corev1.Node, taint corev1.Taint) int {
	return slices.IndexFunc(node.Spec.Taints, func(t corev1.Taint) bool {
		return t.Key == taint.Key && t.Value == taint.Value && t.Effect == taint.Effect &&
			t.TimeAdded.Equal(taint.TimeAdded)
	})
}

// 8. executeTaintRemovals removes taints other components own with a JSON
// patch that first tests the node's UID and the exact taint at its observed
// position, so it never edits a newer node with the same name, nor removes a
// taint its owner re-added or changed meanwhile.
func (e *executor) executeTaintRemovals(
	ctx context.Context,
	removals []Attributed[TaintRemoval],
) (reconcile.Result, bool, error) {
	logger := log.FromContext(ctx)
	for _, rt := range removals {
		node, taint := rt.Value.Node, rt.Value.Taint
		index := taintIndex(node, taint)
		path := fmt.Sprintf("/spec/taints/%d", index)
		patch, err := json.Marshal([]map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": string(node.UID)},
			{"op": "test", "path": path, "value": node.Spec.Taints[index]},
			{"op": "remove", "path": path},
		})
		if err != nil {
			return reconcile.Result{}, false, fmt.Errorf("marshal taint removal patch for %s: %w", node.Name, err)
		}
		// Patch decodes the response into its argument; keep the observed node intact.
		target := node.DeepCopy()
		err = e.writer.Patch(ctx, target, client.RawPatch(types.JSONPatchType, patch), client.FieldOwner(e.fieldManager))
		switch {
		case apierrors.IsNotFound(err):
			continue
		case apierrors.IsConflict(err) || apierrors.IsInvalid(err):
			// A failed JSON patch test is Invalid: the node changed since
			// the rules saw it.
			logger.Info("stale observation, requeueing", "object", node.Name, "error", err)
			return reconcile.Result{Requeue: true}, true, nil
		case err != nil:
			return reconcile.Result{}, false, fmt.Errorf("remove taint %s from %s: %w", taint.ToString(), node.Name, err)
		}
		if e.recorder != nil {
			e.recorder.Eventf(node, corev1.EventTypeNormal, "Untainted", "Removed taint %s by rule %s", taint.ToString(), rt.Rule)
		}
		logger.Info("removed taint", "node", node.Name, "taint", taint.ToString(), "rule", rt.Rule)
	}
	return reconcile.Result{}, false, nil
}

// warningID identifies a warning for deduplication.
type warningID struct {
	uid     types.UID
	name    string
	reason  string
	message string
}

// 0. emitWarnings emits every warning the key asserts that was not already
// emitted while continuously asserted, and forgets the warnings the key no
// longer asserts, so they are emitted again if they recur.
func (e *executor) emitWarnings(ctx context.Context, key Key, warnings []Attributed[Warning]) {
	asserted := make(map[warningID]bool, len(warnings))
	var fresh []Attributed[Warning]
	e.warnedMu.Lock()
	previous := e.warned[key]
	for _, w := range warnings {
		id := warningID{w.Value.Object.GetUID(), w.Value.Object.GetName(), w.Value.Reason, w.Value.Message}
		if asserted[id] {
			continue
		}
		asserted[id] = true
		if !previous[id] {
			fresh = append(fresh, w)
		}
	}
	if len(asserted) == 0 {
		delete(e.warned, key)
	} else {
		e.warned[key] = asserted
	}
	e.warnedMu.Unlock()

	logger := log.FromContext(ctx)
	for _, w := range fresh {
		if e.dryRun {
			logger.Info("would warn",
				"rule", w.Rule, "object", w.Value.Object.GetName(),
				"reason", w.Value.Reason, "message", w.Value.Message)
			continue
		}
		if e.recorder != nil {
			e.recorder.Eventf(w.Value.Object, corev1.EventTypeWarning, w.Value.Reason, "%s (rule %s)", w.Value.Message, w.Rule)
		}
		logger.Info("warned", "rule", w.Rule, "object", w.Value.Object.GetName(), "reason", w.Value.Reason, "message", w.Value.Message)
	}
}

func isStaleObservation(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsConflict(err) {
		return true
	}
	if apierrors.IsInvalid(err) && strings.Contains(strings.ToLower(err.Error()), "uid") {
		return true
	}
	return false
}

func normalize(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any)
	for k, v := range m {
		if k == "apiVersion" || k == "kind" || k == "name" || k == "namespace" || k == "uid" || k == "resourceVersion" {
			continue
		}
		if sub, ok := v.(map[string]any); ok {
			if c := normalize(sub); len(c) > 0 {
				out[k] = c
			}
		} else if v != nil {
			out[k] = v
		}
	}
	return out
}

func wakeResult(wake *Attributed[time.Time], now time.Time) reconcile.Result {
	var res reconcile.Result
	if wake != nil {
		d := wake.Value.Sub(now)
		if d < 100*time.Millisecond {
			d = 100 * time.Millisecond
		}
		res.RequeueAfter = d
	}
	return res
}

func buildAppliedEventMessage(desired, extracted map[string]any, rules []string) string {
	dKeys := getKeys(desired)
	eKeys := getKeys(extracted)

	var setLabels, setAnnos, otherSet []string
	var relLabels, relAnnos, otherRel []string

	for k := range dKeys {
		if !eKeys[k] {
			categorizeKey(k, &setLabels, &setAnnos, &otherSet)
		}
	}
	for k := range eKeys {
		if !dKeys[k] {
			categorizeKey(k, &relLabels, &relAnnos, &otherRel)
		}
	}

	var changeParts []string
	if p := formatParts(setLabels, setAnnos, otherSet); p != "" {
		changeParts = append(changeParts, "set "+p)
	}
	if p := formatParts(relLabels, relAnnos, otherRel); p != "" {
		changeParts = append(changeParts, "released "+p)
	}

	ruleStr := "(no rules assert fields; releasing)"
	if len(rules) > 0 {
		ruleStr = fmt.Sprintf("by rules [%s]", strings.Join(rules, ", "))
	}

	msg := "Applied " + ruleStr
	if len(changeParts) > 0 {
		msg += ": " + strings.Join(changeParts, "; ")
	}
	return msg
}

func getKeys(m map[string]any) map[string]bool {
	keys := make(map[string]bool)
	if meta, ok := m["metadata"].(map[string]any); ok {
		if lbls, ok := meta["labels"].(map[string]any); ok {
			for k := range lbls {
				keys["labels["+k+"]"] = true
			}
		}
		if annos, ok := meta["annotations"].(map[string]any); ok {
			for k := range annos {
				keys["annotations["+k+"]"] = true
			}
		}
	}
	for k, v := range m {
		if k == "apiVersion" || k == "kind" || k == "metadata" {
			continue
		}
		if sub, ok := v.(map[string]any); ok {
			for subK := range sub {
				keys[k+"."+subK] = true
			}
		} else {
			keys[k] = true
		}
	}
	return keys
}

func categorizeKey(k string, labels, annos, other *[]string) {
	switch {
	case strings.HasPrefix(k, "labels["):
		*labels = append(*labels, strings.TrimSuffix(strings.TrimPrefix(k, "labels["), "]"))
	case strings.HasPrefix(k, "annotations["):
		*annos = append(*annos, strings.TrimSuffix(strings.TrimPrefix(k, "annotations["), "]"))
	default:
		*other = append(*other, k)
	}
}

func formatParts(labels, annos, other []string) string {
	slices.Sort(labels)
	slices.Sort(annos)
	slices.Sort(other)
	var parts []string
	if len(labels) > 0 {
		parts = append(parts, fmt.Sprintf("labels=%v", labels))
	}
	if len(annos) > 0 {
		parts = append(parts, fmt.Sprintf("annotations=%v", annos))
	}
	if len(other) > 0 {
		parts = append(parts, fmt.Sprintf("fields=%v", other))
	}
	return strings.Join(parts, " ")
}
