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

package ctrl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	}
}

type plannedObjectDiff struct {
	po           rulekit.PlannedObject
	obsObj       client.Object
	exists       bool
	changed      bool
	extractedMap map[string]any
}

type plannedTaintDiff struct {
	pt      rulekit.PlannedTaint
	obsNode *corev1.Node
	changed bool
}

type diffResult struct {
	objects []plannedObjectDiff
	taints  []plannedTaintDiff
}

// Execute applies the planned effects against the cluster in structured steps:
// diff -> dry-run -> preconditions -> applies -> taints -> deletes -> wake.
func (e *executor) Execute(ctx context.Context, rd *memoReader, plan *rulekit.Plan) (reconcile.Result, error) {
	// 1. Diff / No-op detection
	diffs, allNoop, err := e.diff(ctx, rd, plan)
	if err != nil {
		return reconcile.Result{}, err
	}
	if allNoop && len(plan.Deletes) == 0 {
		return wakeResult(plan.Wake, rd.Now()), nil
	}

	// 2. DryRun
	if e.dryRun {
		e.logDryRun(ctx, diffs, plan)
		return wakeResult(plan.Wake, rd.Now()), nil
	}

	// 3. Preconditions
	if res, stop, err := e.checkPreconditions(ctx, plan.Preconditions); err != nil || stop {
		return res, err
	}

	// 4. Applies (changed only)
	latestRVs, res, stop, err := e.executeApplies(ctx, diffs.objects)
	if err != nil || stop {
		return res, err
	}

	// 5. Taints (changed only, Node objects)
	if res, stop, err := e.executeTaints(ctx, diffs.taints, latestRVs); err != nil || stop {
		return res, err
	}

	// 6. Deletes
	if res, stop, err := e.executeDeletes(ctx, plan.Deletes); err != nil || stop {
		return res, err
	}

	// 7. Wake
	return wakeResult(plan.Wake, rd.Now()), nil
}

// 1. diff evaluates each planned object and taint against observed state.
func (e *executor) diff(ctx context.Context, rd *memoReader, plan *rulekit.Plan) (diffResult, bool, error) {
	objectDiffs := make([]plannedObjectDiff, len(plan.Objects))
	allObjectsNoop := true
	for i, po := range plan.Objects {
		obsObj, exists, err := rd.observed(ctx, po.Ref)
		if err != nil {
			return diffResult{}, false, fmt.Errorf("observe %s: %w", po.Ref, err)
		}

		normDesired := normalizeMap(po.Desired)
		isIdentityOnly := len(normDesired) == 0

		var changed bool
		var extractedMap map[string]any
		if !exists {
			if isIdentityOnly {
				changed = false // Object absent and Desired identity-only: no-op.
			} else {
				changed = true
			}
		} else {
			extractor, ok := e.extractors[po.Ref.GVK]
			if !ok {
				// Unregistered GVK: treat as changed (always apply).
				changed = true
			} else {
				ac, err := extractor(obsObj, e.fieldManager)
				if err != nil {
					return diffResult{}, false, fmt.Errorf("extract %s: %w", po.Ref, err)
				}
				if ac != nil {
					data, err := json.Marshal(ac)
					if err != nil {
						return diffResult{}, false, fmt.Errorf("marshal extracted %s: %w", po.Ref, err)
					}
					if err := json.Unmarshal(data, &extractedMap); err != nil {
						return diffResult{}, false, fmt.Errorf("unmarshal extracted %s: %w", po.Ref, err)
					}
				}
				normExtracted := normalizeMap(extractedMap)
				changed = !reflect.DeepEqual(normExtracted, normDesired)
			}
		}

		if changed {
			allObjectsNoop = false
		}
		objectDiffs[i] = plannedObjectDiff{
			po:           po,
			obsObj:       obsObj,
			exists:       exists,
			changed:      changed,
			extractedMap: extractedMap,
		}
	}

	taintDiffs := make([]plannedTaintDiff, len(plan.Taints))
	allTaintsNoop := true
	for i, pt := range plan.Taints {
		nodeRef := rulekit.Ref{
			GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
			Name: pt.Node,
		}
		obsNodeObj, exists, err := rd.observed(ctx, nodeRef)
		if err != nil {
			return diffResult{}, false, fmt.Errorf("observe node %s for taint: %w", pt.Node, err)
		}

		var changed bool
		var obsNode *corev1.Node
		if !exists || obsNodeObj == nil {
			// Taints on absent nodes: if the node isn't observed, a PlannedTaint
			// must be a no-op (nothing to taint/untaint) instead of patching a missing node.
			changed = false
		} else {
			var hasTaint bool
			var existingTaint *corev1.Taint
			if n, ok := obsNodeObj.(*corev1.Node); ok {
				obsNode = n
				for _, t := range n.Spec.Taints {
					if t.Key == pt.Key && t.Effect == pt.Effect {
						hasTaint = true
						tCopy := t
						existingTaint = &tCopy
						break
					}
				}
			}

			changed = (hasTaint != pt.Present)
			if !changed && pt.Present && pt.Taint != nil && existingTaint != nil {
				if existingTaint.Value != pt.Taint.Value {
					changed = true
				}
			}
		}

		if changed {
			allTaintsNoop = false
		}
		taintDiffs[i] = plannedTaintDiff{
			pt:      pt,
			obsNode: obsNode,
			changed: changed,
		}
	}

	res := diffResult{objects: objectDiffs, taints: taintDiffs}
	allNoop := allObjectsNoop && allTaintsNoop
	return res, allNoop, nil
}

// 2. logDryRun logs every would-be write without performing API calls.
func (e *executor) logDryRun(ctx context.Context, diffs diffResult, plan *rulekit.Plan) {
	logger := log.FromContext(ctx)
	for _, d := range diffs.objects {
		if d.changed {
			if len(d.po.Rules) == 0 {
				logger.Info("would apply (releasing)", "ref", d.po.Ref, "content", d.po.Desired)
			} else {
				logger.Info("would apply", "ref", d.po.Ref, "rules", d.po.Rules, "content", d.po.Desired)
			}
		}
	}
	for _, d := range diffs.taints {
		if d.changed {
			if d.pt.Present {
				logger.Info("would taint", "node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect, "taint", d.pt.Taint, "rules", d.pt.Rules)
			} else {
				if len(d.pt.Rules) == 0 {
					logger.Info("would untaint", "node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect, "reason", "no rule asserts it")
				} else {
					logger.Info("would untaint", "node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect, "rules", d.pt.Rules)
				}
			}
		}
	}
	for _, del := range plan.Deletes {
		logger.Info("would delete", "rule", del.Rule, "object", del.Object)
	}
}

// 3. checkPreconditions checks live preconditions against apiReader.
func (e *executor) checkPreconditions(ctx context.Context, pres []rulekit.RecordedPrecondition) (reconcile.Result, bool, error) {
	logger := log.FromContext(ctx)
	for _, prec := range pres {
		if err := prec.Precondition.Check(ctx, e.apiReader); err != nil {
			if errors.Is(err, rulekit.ErrPreconditionFailed) {
				logger.Info("precondition failed", "rule", prec.Rule, "precondition", prec.Precondition.String(), "error", err)
				return reconcile.Result{RequeueAfter: 1 * time.Second}, true, nil
			}
			return reconcile.Result{}, false, err
		}
	}
	return reconcile.Result{}, false, nil
}

// 4. executeApplies runs server-side apply for changed objects.
func (e *executor) executeApplies(ctx context.Context, objectDiffs []plannedObjectDiff) (map[rulekit.Ref]string, reconcile.Result, bool, error) {
	latestRVs := make(map[rulekit.Ref]string)
	logger := log.FromContext(ctx)

	for _, d := range objectDiffs {
		if !d.changed {
			continue
		}

		eventMsg := buildAppliedEventMessage(d.po.Desired, d.extractedMap, d.po.Rules)

		u := &unstructured.Unstructured{
			Object: deepCopyMap(d.po.Desired),
		}

		if d.exists && d.obsObj != nil {
			// SSA is an upsert; setting metadata.uid makes the apiserver reject
			// re-creating a deleted object if it was deleted between our read and apply
			// (verified on a real apiserver). Setting resourceVersion provides optimistic
			// concurrency against the observed snapshot.
			u.SetUID(d.obsObj.GetUID())
			u.SetResourceVersion(d.obsObj.GetResourceVersion())
		}

		err := e.writer.Patch(ctx, u, client.Apply, client.FieldOwner(e.fieldManager), client.ForceOwnership)
		if err != nil {
			if isStaleObservation(err) {
				logger.Info("stale observation, requeueing", "ref", d.po.Ref, "error", err)
				return nil, reconcile.Result{Requeue: true}, true, nil
			}
			return nil, reconcile.Result{}, false, fmt.Errorf("apply %s: %w", d.po.Ref, err)
		}

		rv := u.GetResourceVersion()
		latestRVs[d.po.Ref] = rv

		if e.recorder != nil {
			e.recorder.Eventf(u, corev1.EventTypeNormal, "Applied", "%s", eventMsg)
		}

		if len(d.po.Rules) == 0 {
			logger.Info("applied object (releasing)", "ref", d.po.Ref, "resourceVersion", rv)
		} else {
			logger.Info("applied object", "ref", d.po.Ref, "rules", d.po.Rules, "resourceVersion", rv)
		}
	}

	return latestRVs, reconcile.Result{}, false, nil
}

// 5. executeTaints updates node taints via JSON merge patch.
func (e *executor) executeTaints(ctx context.Context, taintDiffs []plannedTaintDiff, latestRVs map[rulekit.Ref]string) (reconcile.Result, bool, error) {
	currentTaintsByNode := make(map[string][]corev1.Taint)
	for _, d := range taintDiffs {
		if d.obsNode != nil {
			if _, exists := currentTaintsByNode[d.pt.Node]; !exists {
				currentTaintsByNode[d.pt.Node] = append([]corev1.Taint(nil), d.obsNode.Spec.Taints...)
			}
		}
	}

	logger := log.FromContext(ctx)

	for _, d := range taintDiffs {
		if !d.changed {
			continue
		}

		nodeRef := rulekit.Ref{
			GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
			Name: d.pt.Node,
		}

		rv := latestRVs[nodeRef]
		if rv == "" && d.obsNode != nil {
			rv = d.obsNode.GetResourceVersion()
		}

		// Taints are listType=atomic in the Kubernetes Node schema. Server-side apply would
		// claim ownership of the entire taint list, clobbering or conflicting with taints
		// managed by other actors (e.g. kubelet, cloud-provider). We instead use a
		// read-modify-write JSON merge patch with optimistic concurrency via metadata.resourceVersion.
		currentList := currentTaintsByNode[d.pt.Node]
		var newTaints []corev1.Taint
		for _, t := range currentList {
			if t.Key == d.pt.Key && t.Effect == d.pt.Effect {
				continue
			}
			newTaints = append(newTaints, t)
		}
		if d.pt.Present && d.pt.Taint != nil {
			newTaints = append(newTaints, *d.pt.Taint)
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
			return reconcile.Result{}, false, fmt.Errorf("marshal taint patch for %s: %w", d.pt.Node, err)
		}

		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: d.pt.Node,
			},
		}
		err = e.writer.Patch(ctx, nodeObj, client.RawPatch(types.MergePatchType, patchBytes))
		if err != nil {
			if isStaleObservation(err) {
				logger.Info("stale observation, requeueing", "node", d.pt.Node, "error", err)
				return reconcile.Result{Requeue: true}, true, nil
			}
			return reconcile.Result{}, false, fmt.Errorf("patch node taints for %s: %w", d.pt.Node, err)
		}

		latestRVs[nodeRef] = nodeObj.GetResourceVersion()
		currentTaintsByNode[d.pt.Node] = newTaints

		if d.pt.Present && d.pt.Taint != nil {
			if e.recorder != nil {
				msg := fmt.Sprintf("Tainted %s=%s:%s by rules [%s]", d.pt.Taint.Key, d.pt.Taint.Value, d.pt.Taint.Effect, strings.Join(d.pt.Rules, ", "))
				e.recorder.Eventf(nodeObj, corev1.EventTypeNormal, "Tainted", "%s", msg)
			}
			logger.Info("tainted node", "node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect, "rules", d.pt.Rules)
		} else {
			if e.recorder != nil {
				var msg string
				if len(d.pt.Rules) == 0 {
					msg = fmt.Sprintf("Untainted %s:%s: no rule asserts it", d.pt.Key, d.pt.Effect)
				} else {
					msg = fmt.Sprintf("Untainted %s:%s by rules [%s]", d.pt.Key, d.pt.Effect, strings.Join(d.pt.Rules, ", "))
				}
				e.recorder.Eventf(nodeObj, corev1.EventTypeNormal, "Untainted", "%s", msg)
			}
			if len(d.pt.Rules) == 0 {
				logger.Info("untainted node", "node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect, "reason", "no rule asserts it")
			} else {
				logger.Info("untainted node", "node", d.pt.Node, "key", d.pt.Key, "effect", d.pt.Effect, "rules", d.pt.Rules)
			}
		}
	}

	return reconcile.Result{}, false, nil
}

// 6. executeDeletes deletes unowned objects.
func (e *executor) executeDeletes(ctx context.Context, deletes []rulekit.RecordedDelete) (reconcile.Result, bool, error) {
	logger := log.FromContext(ctx)
	for _, del := range deletes {
		uid := del.Object.GetUID()
		var deleteOpts []client.DeleteOption
		if uid != "" {
			deleteOpts = append(deleteOpts, client.Preconditions{UID: &uid})
		}
		err := e.writer.Delete(ctx, del.Object, deleteOpts...)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				if isStaleObservation(err) {
					logger.Info("stale observation, requeueing", "object", del.Object.GetName(), "error", err)
					return reconcile.Result{Requeue: true}, true, nil
				}
				return reconcile.Result{}, false, fmt.Errorf("delete %s: %w", del.Object.GetName(), err)
			}
		} else {
			if e.recorder != nil {
				e.recorder.Eventf(del.Object, corev1.EventTypeNormal, "Deleted", "Deleted by rule %s", del.Rule)
			}
			logger.Info("deleted object", "object", del.Object.GetName(), "rule", del.Rule)
		}
	}
	return reconcile.Result{}, false, nil
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

func wakeResult(wake *rulekit.RecordedWake, now time.Time) reconcile.Result {
	var res reconcile.Result
	if wake != nil {
		d := wake.At.Sub(now)
		if d < 100*time.Millisecond {
			d = 100 * time.Millisecond
		}
		res.RequeueAfter = d
	}
	return res
}

func deepCopyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = deepCopyVal(v)
	}
	return cp
}

func deepCopyVal(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return deepCopyMap(val)
	case []any:
		cp := make([]any, len(val))
		for i, subV := range val {
			cp[i] = deepCopyVal(subV)
		}
		return cp
	default:
		return val
	}
}

func normalizeMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any)
	for k, v := range m {
		if k == "apiVersion" || k == "kind" {
			continue
		}
		if k == "metadata" {
			if metaMap, ok := v.(map[string]any); ok {
				normMeta := normalizeMetadata(metaMap)
				if len(normMeta) > 0 {
					out["metadata"] = normMeta
				}
			}
			continue
		}
		cleaned := cleanValue(v)
		if cleaned != nil {
			out[k] = cleaned
		}
	}
	return out
}

func normalizeMetadata(meta map[string]any) map[string]any {
	out := make(map[string]any)
	for k, v := range meta {
		if k == "name" || k == "namespace" || k == "uid" || k == "resourceVersion" {
			continue
		}
		cleaned := cleanValue(v)
		if cleaned != nil {
			out[k] = cleaned
		}
	}
	return out
}

func cleanValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		cleaned := make(map[string]any)
		for k, subV := range val {
			c := cleanValue(subV)
			if c != nil {
				cleaned[k] = c
			}
		}
		if len(cleaned) == 0 {
			return nil
		}
		return cleaned
	default:
		return val
	}
}

func buildAppliedEventMessage(desired, extracted map[string]any, rules []string) string {
	var setLabels, setAnnotations, otherSet []string
	if meta, ok := desired["metadata"].(map[string]any); ok {
		if lbls, ok := meta["labels"].(map[string]any); ok {
			for k := range lbls {
				setLabels = append(setLabels, k)
			}
			slices.Sort(setLabels)
		}
		if annos, ok := meta["annotations"].(map[string]any); ok {
			for k := range annos {
				setAnnotations = append(setAnnotations, k)
			}
			slices.Sort(setAnnotations)
		}
	}
	for k, v := range desired {
		if k == "apiVersion" || k == "kind" || k == "metadata" {
			continue
		}
		if m, ok := v.(map[string]any); ok {
			for subK := range m {
				otherSet = append(otherSet, fmt.Sprintf("%s.%s", k, subK))
			}
		} else {
			otherSet = append(otherSet, k)
		}
	}
	slices.Sort(otherSet)

	var prevLabels, prevAnnotations, otherPrev []string
	if meta, ok := extracted["metadata"].(map[string]any); ok {
		if lbls, ok := meta["labels"].(map[string]any); ok {
			for k := range lbls {
				prevLabels = append(prevLabels, k)
			}
		}
		if annos, ok := meta["annotations"].(map[string]any); ok {
			for k := range annos {
				prevAnnotations = append(prevAnnotations, k)
			}
		}
	}
	for k, v := range extracted {
		if k == "apiVersion" || k == "kind" || k == "metadata" {
			continue
		}
		if m, ok := v.(map[string]any); ok {
			for subK := range m {
				otherPrev = append(otherPrev, fmt.Sprintf("%s.%s", k, subK))
			}
		} else {
			otherPrev = append(otherPrev, k)
		}
	}

	var releasedLabels, releasedAnnotations, otherReleased []string
	desiredMeta, _ := desired["metadata"].(map[string]any)
	desiredLblMap, _ := desiredMeta["labels"].(map[string]any)
	desiredAnnoMap, _ := desiredMeta["annotations"].(map[string]any)

	for _, k := range prevLabels {
		if _, ok := desiredLblMap[k]; !ok {
			releasedLabels = append(releasedLabels, k)
		}
	}
	slices.Sort(releasedLabels)

	for _, k := range prevAnnotations {
		if _, ok := desiredAnnoMap[k]; !ok {
			releasedAnnotations = append(releasedAnnotations, k)
		}
	}
	slices.Sort(releasedAnnotations)

	for _, path := range otherPrev {
		if !slices.Contains(otherSet, path) {
			otherReleased = append(otherReleased, path)
		}
	}
	slices.Sort(otherReleased)

	var setParts []string
	if len(setLabels) > 0 {
		setParts = append(setParts, fmt.Sprintf("labels=%v", setLabels))
	}
	if len(setAnnotations) > 0 {
		setParts = append(setParts, fmt.Sprintf("annotations=%v", setAnnotations))
	}
	if len(otherSet) > 0 {
		setParts = append(setParts, fmt.Sprintf("fields=%v", otherSet))
	}

	var relParts []string
	if len(releasedLabels) > 0 {
		relParts = append(relParts, fmt.Sprintf("labels=%v", releasedLabels))
	}
	if len(releasedAnnotations) > 0 {
		relParts = append(relParts, fmt.Sprintf("annotations=%v", releasedAnnotations))
	}
	if len(otherReleased) > 0 {
		relParts = append(relParts, fmt.Sprintf("fields=%v", otherReleased))
	}

	var changeParts []string
	if len(setParts) > 0 {
		changeParts = append(changeParts, "set "+strings.Join(setParts, " "))
	}
	if len(relParts) > 0 {
		changeParts = append(changeParts, "released "+strings.Join(relParts, " "))
	}

	if len(rules) == 0 {
		msg := "Applied (no rules assert fields; releasing)"
		if len(changeParts) > 0 {
			msg += ": " + strings.Join(changeParts, "; ")
		}
		return msg
	}

	msg := fmt.Sprintf("Applied by rules [%s]", strings.Join(rules, ", "))
	if len(changeParts) > 0 {
		msg += ": " + strings.Join(changeParts, "; ")
	}
	return msg
}
