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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Evaluate runs all rules against the snapshot and merges their effects into a Plan.
// It returns an error if any rules conflict.
func (c Controller[T]) Evaluate(key Key, data T) (*Plan, error) {
	var fired []string
	var errs []error
	fx := &Effects{}

	for _, rule := range c.Rules {
		if rule.When(data) {
			fired = append(fired, rule.Name)
			fx.rule = rule.Name
			if err := rule.Then(data, fx); err != nil {
				errs = append(errs, fmt.Errorf("rule %q: %w", rule.Name, err))
			}
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	var owns []Ref
	if c.Owns != nil {
		owns = c.Owns(key)
	}
	var taints []ManagedTaint
	if c.ManagedTaints != nil {
		taints = c.ManagedTaints(key)
	}

	plan, err := buildPlan(fx, fired, owns, taints)
	if err != nil {
		return nil, err
	}
	plan.Key = key
	return plan, nil
}

// Plan is the pure outcome of evaluating rules for one reconcile key.
// Plan is the merged desired state of all rules that fired for a key.
type Plan struct {
	Key           Key             // the key the plan is for
	Fired         []string        // rule IDs whose Should was true, in order
	Objects       []PlannedObject // one per owned Ref (in Owns order), merged
	Taints        []PlannedTaint  // one per ManagedTaint
	Deletes       []Attributed[client.Object]
	Finalizers    []Attributed[FinalizerRemoval] // removed after Deletes
	TaintRemovals []Attributed[TaintRemoval]     // removed after Finalizers
	Warnings      []Attributed[Warning]
	Preconditions []Attributed[Precondition]
	Wake          *Attributed[time.Time]
}

// PlannedObject is the merged desired state for one owned object.
type PlannedObject struct {
	Ref Ref
	// Desired holds unstructured content: apiVersion, kind, metadata.name(/namespace)
	// are always set, plus merged fields. Only identity fields means release everything.
	Desired map[string]any
	Rules   []string // rules that contributed
}

// PlannedTaint is the desired state of one managed node taint.
type PlannedTaint struct {
	ManagedTaint
	Present bool
	Taint   *corev1.Taint // the asserted taint if Present
	Rules   []string
}

type appliedEntry struct {
	rule string
	data map[string]any
}

type taintKey struct {
	node   string
	key    string
	effect corev1.TaintEffect
}

// BuildPlan merges all rules' effects into a Plan, detecting policy conflicts.
func buildPlan(fx *Effects, fired []string, owns []Ref, taints []ManagedTaint) (*Plan, error) {
	var errs []error

	refApplies := collectApplies(fx, owns, &errs)

	// Merge objects in Owns order
	objects := make([]PlannedObject, 0, len(owns))
	for i := range owns {
		ref := &owns[i]
		entries := refApplies[*ref]
		if len(entries) == 0 {
			desired := identityOnly(ref)
			objects = append(objects, PlannedObject{
				Ref:     *ref,
				Desired: desired,
				Rules:   nil,
			})
			continue
		}

		merged := make(map[string]any)
		fieldOwners := make(map[string]string)
		var contributingRules []string

		for _, entry := range entries {
			if !slices.Contains(contributingRules, entry.rule) {
				contributingRules = append(contributingRules, entry.rule)
			}
			mergeMaps(merged, entry.data, "", fieldOwners, entry.rule, &errs)
		}

		// Ensure identity fields are present
		ensureIdentityFields(merged, ref)

		objects = append(objects, PlannedObject{
			Ref:     *ref,
			Desired: merged,
			Rules:   contributingRules,
		})
	}

	// Process managed taints
	managedIndex := make(map[taintKey]int, len(taints))
	for i, mt := range taints {
		managedIndex[taintKey{node: mt.Node, key: mt.Key, effect: mt.Effect}] = i
	}

	plannedTaints := collectPlannedTaints(fx, taints, managedIndex, &errs)

	var deletes []Attributed[client.Object]
	var finalizers []Attributed[FinalizerRemoval]
	var untaints []Attributed[TaintRemoval]
	var warnings []Attributed[Warning]
	var pres []Attributed[Precondition]
	var wake *Attributed[time.Time]
	if fx != nil {
		deletes = fx.deletes
		finalizers = fx.finalizers
		warnings = fx.warnings
		pres = fx.pres
		wake = fx.wake
		for _, rt := range fx.untaints {
			// A managed taint's presence is decided by Taint assertions alone;
			// removing it here would fight the managed-taint diff.
			tk := taintKey{node: rt.Value.Node.GetName(), key: rt.Value.Taint.Key, effect: rt.Value.Taint.Effect}
			if _, ok := managedIndex[tk]; ok {
				errs = append(errs, fmt.Errorf("rule %q removes taint %s:%s from node %s, which is a managed taint; "+
					"assert it with Taint or leave it unasserted instead", rt.Rule, rt.Value.Taint.Key, rt.Value.Taint.Effect, tk.node))
				continue
			}
			untaints = append(untaints, rt)
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Plan{
		Fired:         fired,
		Objects:       objects,
		Taints:        plannedTaints,
		Deletes:       deletes,
		Finalizers:    finalizers,
		TaintRemovals: untaints,
		Warnings:      warnings,
		Preconditions: pres,
		Wake:          wake,
	}, nil
}

func collectApplies(fx *Effects, owns []Ref, errs *[]error) map[Ref][]appliedEntry {
	refApplies := make(map[Ref][]appliedEntry)
	if fx == nil {
		return refApplies
	}
	for _, app := range fx.applies {
		data, err := json.Marshal(app.Value)
		if err != nil {
			*errs = append(*errs, fmt.Errorf("rule %q apply: marshal config: %w", app.Rule, err))
			continue
		}

		var objMap map[string]any
		if err := json.Unmarshal(data, &objMap); err != nil {
			*errs = append(*errs, fmt.Errorf("rule %q apply: unmarshal config: %w", app.Rule, err))
			continue
		}

		var apiVersion, kind string
		if v, ok := objMap["apiVersion"].(string); ok {
			apiVersion = v
		}
		if k, ok := objMap["kind"].(string); ok {
			kind = k
		}
		meta := extractApplyMeta(objMap, app.Rule, errs)

		if apiVersion == "" || kind == "" || meta.name == "" {
			*errs = append(*errs, fmt.Errorf("rule %q apply configuration missing apiVersion, kind, or metadata.name", app.Rule))
			continue
		}

		gvk := schema.FromAPIVersionAndKind(apiVersion, kind)
		ref := Ref{GVK: gvk, Namespace: meta.namespace, Name: meta.name}

		if !slices.Contains(owns, ref) {
			*errs = append(*errs, fmt.Errorf(
				"rule %q applied to %s which the controller does not declare in Owns",
				app.Rule, ref.String()))
			continue
		}

		refApplies[ref] = append(refApplies[ref], appliedEntry{rule: app.Rule, data: objMap})
	}
	return refApplies
}

type applyMeta struct {
	name      string
	namespace string
}

func extractApplyMeta(objMap map[string]any, rule string, errs *[]error) applyMeta {
	meta, ok := objMap["metadata"].(map[string]any)
	if !ok || meta == nil {
		return applyMeta{}
	}
	var out applyMeta
	if nameStr, ok := meta["name"].(string); ok {
		out.name = nameStr
	}
	if nsStr, ok := meta["namespace"].(string); ok {
		out.namespace = nsStr
	}
	if _, ok := meta["uid"]; ok {
		*errs = append(*errs, fmt.Errorf("rule %q set metadata.uid; rules must not set metadata.uid", rule))
	}
	if _, ok := meta["resourceVersion"]; ok {
		*errs = append(*errs, fmt.Errorf(
			"rule %q set metadata.resourceVersion; rules must not set metadata.resourceVersion", rule))
	}
	return out
}

func collectPlannedTaints(
	fx *Effects,
	taints []ManagedTaint,
	managedIndex map[taintKey]int,
	errs *[]error,
) []PlannedTaint {
	taintAsserted := make(map[taintKey]*corev1.Taint)
	taintRules := make(map[taintKey][]string)

	if fx != nil {
		for _, rt := range fx.taints {
			tk := taintKey{node: rt.Value.Node, key: rt.Value.Taint.Key, effect: rt.Value.Taint.Effect}
			if _, ok := managedIndex[tk]; !ok {
				*errs = append(*errs, fmt.Errorf(
					"rule %q asserted taint for node %s key %s effect %s which the controller does not declare in ManagedTaints",
					rt.Rule, rt.Value.Node, rt.Value.Taint.Key, rt.Value.Taint.Effect))
				continue
			}

			existing, ok := taintAsserted[tk]
			if !ok {
				tCopy := rt.Value.Taint
				taintAsserted[tk] = &tCopy
				taintRules[tk] = []string{rt.Rule}
				continue
			}
			if existing.Value != rt.Value.Taint.Value {
				owner := taintRules[tk][0]
				*errs = append(*errs, fmt.Errorf(
					"conflict on managed taint (%s, %s, %s) between rules %q and %q: %q != %q",
					rt.Value.Node, rt.Value.Taint.Key, rt.Value.Taint.Effect, owner, rt.Rule, existing.Value, rt.Value.Taint.Value))
				continue
			}
			if !slices.Contains(taintRules[tk], rt.Rule) {
				taintRules[tk] = append(taintRules[tk], rt.Rule)
			}
		}
	}

	plannedTaints := make([]PlannedTaint, len(taints))
	for i, mt := range taints {
		tk := taintKey{node: mt.Node, key: mt.Key, effect: mt.Effect}
		if t, ok := taintAsserted[tk]; ok {
			plannedTaints[i] = PlannedTaint{
				ManagedTaint: mt,
				Present:      true,
				Taint:        t,
				Rules:        taintRules[tk],
			}
		} else {
			plannedTaints[i] = PlannedTaint{
				ManagedTaint: mt,
				Present:      false,
				Taint:        nil,
				Rules:        nil,
			}
		}
	}
	return plannedTaints
}

func identityOnly(ref *Ref) map[string]any {
	meta := map[string]any{
		"name": ref.Name,
	}
	if ref.Namespace != "" {
		meta["namespace"] = ref.Namespace
	}
	return map[string]any{
		"apiVersion": ref.GVK.GroupVersion().String(),
		"kind":       ref.GVK.Kind,
		"metadata":   meta,
	}
}

func ensureIdentityFields(m map[string]any, ref *Ref) {
	m["apiVersion"] = ref.GVK.GroupVersion().String()
	m["kind"] = ref.GVK.Kind
	meta, ok := m["metadata"].(map[string]any)
	if !ok {
		meta = make(map[string]any)
		m["metadata"] = meta
	}
	meta["name"] = ref.Name
	if ref.Namespace != "" {
		meta["namespace"] = ref.Namespace
	}
}

func formatPath(parent, key string) string {
	if parent == "" {
		return key
	}
	if strings.ContainsAny(key, "./[]") || strings.HasSuffix(parent, "labels") || strings.HasSuffix(parent, "annotations") {
		return parent + "[" + key + "]"
	}
	return parent + "." + key
}

func recordOwners(path string, val any, owners map[string]string, rule string) {
	owners[path] = rule
	if m, ok := val.(map[string]any); ok {
		for k, v := range m {
			recordOwners(formatPath(path, k), v, owners, rule)
		}
	}
}

func deepCopyJSONValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, subV := range val {
			cp[k] = deepCopyJSONValue(subV)
		}
		return cp
	case []any:
		cp := make([]any, len(val))
		for i, subV := range val {
			cp[i] = deepCopyJSONValue(subV)
		}
		return cp
	default:
		return val
	}
}

func mergeMaps(dst, src map[string]any, parentPath string, owners map[string]string, currentRule string, errs *[]error) {
	for k, srcVal := range src {
		currPath := formatPath(parentPath, k)
		dstVal, exists := dst[k]
		if !exists {
			dst[k] = deepCopyJSONValue(srcVal)
			recordOwners(currPath, srcVal, owners, currentRule)
			continue
		}

		dstMap, dstIsMap := dstVal.(map[string]any)
		srcMap, srcIsMap := srcVal.(map[string]any)
		if dstIsMap && srcIsMap {
			mergeMaps(dstMap, srcMap, currPath, owners, currentRule, errs)
			continue
		}

		if dstIsMap != srcIsMap {
			ownerRule := owners[currPath]
			if ownerRule == "" {
				ownerRule = "unknown"
			}
			*errs = append(*errs, fmt.Errorf(
				"conflict on %s between rules %q and %q: %v != %v",
				currPath, ownerRule, currentRule, dstVal, srcVal))
			continue
		}

		if !reflect.DeepEqual(dstVal, srcVal) {
			ownerRule := owners[currPath]
			if ownerRule == "" {
				ownerRule = "unknown"
			}
			*errs = append(*errs, fmt.Errorf(
				"conflict on %s between rules %q and %q: %v != %v",
				currPath, ownerRule, currentRule, dstVal, srcVal))
			continue
		}
	}
}
