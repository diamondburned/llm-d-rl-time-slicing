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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Plan is the pure outcome of evaluating rules for one reconcile key.
type Plan struct {
	Fired         []string        // rule IDs whose Should was true, in order
	Objects       []PlannedObject // one per owned Ref (in Owns order), merged
	Taints        []PlannedTaint  // one per ManagedTaint
	Deletes       []RecordedDelete
	Preconditions []RecordedPrecondition
	Wake          *RecordedWake
}

// PlannedObject is the desired state of one owned object.
type PlannedObject struct {
	Ref     Ref
	Desired map[string]any // unstructured content: apiVersion, kind, metadata.name(/namespace) always set; plus merged fields. Only identity fields => release everything.
	Rules   []string       // rules that contributed
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
func BuildPlan(fx *Effects, fired []string, owns []Ref, taints []ManagedTaint) (*Plan, error) {
	var errs []error

	refApplies := make(map[Ref][]appliedEntry)

	if fx != nil {
		for _, app := range fx.Applies() {
			data, err := json.Marshal(app.Config)
			if err != nil {
				errs = append(errs, fmt.Errorf("rule %s apply: marshal config: %w", app.Rule, err))
				continue
			}

			var m map[string]any
			if err := json.Unmarshal(data, &m); err != nil {
				errs = append(errs, fmt.Errorf("rule %s apply: unmarshal config: %w", app.Rule, err))
				continue
			}

			apiVersion, _ := m["apiVersion"].(string)
			kind, _ := m["kind"].(string)
			meta, _ := m["metadata"].(map[string]any)
			var name, namespace string
			if meta != nil {
				name, _ = meta["name"].(string)
				namespace, _ = meta["namespace"].(string)

				if _, ok := meta["uid"]; ok {
					errs = append(errs, fmt.Errorf("rule %s set metadata.uid; rules must not set metadata.uid", app.Rule))
				}
				if _, ok := meta["resourceVersion"]; ok {
					errs = append(errs, fmt.Errorf("rule %s set metadata.resourceVersion; rules must not set metadata.resourceVersion", app.Rule))
				}
			}

			if apiVersion == "" || kind == "" || name == "" {
				errs = append(errs, fmt.Errorf("rule %s apply configuration missing apiVersion, kind, or metadata.name", app.Rule))
				continue
			}

			gvk := schema.FromAPIVersionAndKind(apiVersion, kind)
			ref := Ref{GVK: gvk, Namespace: namespace, Name: name}

			if !slices.Contains(owns, ref) {
				errs = append(errs, fmt.Errorf("rule %s applied to %s which the controller does not declare in Owns", app.Rule, ref.String()))
				continue
			}

			refApplies[ref] = append(refApplies[ref], appliedEntry{rule: app.Rule, data: m})
		}
	}

	// Merge objects in Owns order
	objects := make([]PlannedObject, 0, len(owns))
	for _, ref := range owns {
		entries := refApplies[ref]
		if len(entries) == 0 {
			desired := identityOnly(ref)
			objects = append(objects, PlannedObject{
				Ref:     ref,
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
			Ref:     ref,
			Desired: merged,
			Rules:   contributingRules,
		})
	}

	// Process managed taints
	managedIndex := make(map[taintKey]int, len(taints))
	for i, mt := range taints {
		managedIndex[taintKey{node: mt.Node, key: mt.Key, effect: mt.Effect}] = i
	}

	taintAsserted := make(map[taintKey]*corev1.Taint)
	taintRules := make(map[taintKey][]string)

	if fx != nil {
		for _, rt := range fx.Taints() {
			tk := taintKey{node: rt.Node, key: rt.Taint.Key, effect: rt.Taint.Effect}
			if _, ok := managedIndex[tk]; !ok {
				errs = append(errs, fmt.Errorf("rule %s asserted taint for node %s key %s effect %s which the controller does not declare in ManagedTaints", rt.Rule, rt.Node, rt.Taint.Key, rt.Taint.Effect))
				continue
			}

			if existing, ok := taintAsserted[tk]; ok {
				if existing.Value != rt.Taint.Value {
					owner := taintRules[tk][0]
					errs = append(errs, fmt.Errorf("conflict on managed taint (%s, %s, %s) between rules %s and %s: %q != %q", rt.Node, rt.Taint.Key, rt.Taint.Effect, owner, rt.Rule, existing.Value, rt.Taint.Value))
					continue
				}
				if !slices.Contains(taintRules[tk], rt.Rule) {
					taintRules[tk] = append(taintRules[tk], rt.Rule)
				}
			} else {
				tCopy := rt.Taint
				taintAsserted[tk] = &tCopy
				taintRules[tk] = []string{rt.Rule}
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

	var deletes []RecordedDelete
	var pres []RecordedPrecondition
	var wake *RecordedWake
	if fx != nil {
		deletes = fx.Deletes()
		pres = fx.Preconditions()
		wake = fx.Wake()
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Plan{
		Fired:         fired,
		Objects:       objects,
		Taints:        plannedTaints,
		Deletes:       deletes,
		Preconditions: pres,
		Wake:          wake,
	}, nil
}

func identityOnly(ref Ref) map[string]any {
	desired := map[string]any{
		"apiVersion": ref.GVK.GroupVersion().String(),
		"kind":       ref.GVK.Kind,
		"metadata": map[string]any{
			"name": ref.Name,
		},
	}
	if ref.Namespace != "" {
		desired["metadata"].(map[string]any)["namespace"] = ref.Namespace
	}
	return desired
}

func ensureIdentityFields(m map[string]any, ref Ref) {
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
			*errs = append(*errs, fmt.Errorf("conflict on %s between rules %s and %s: %v != %v", currPath, ownerRule, currentRule, dstVal, srcVal))
			continue
		}

		if !reflect.DeepEqual(dstVal, srcVal) {
			ownerRule := owners[currPath]
			if ownerRule == "" {
				ownerRule = "unknown"
			}
			*errs = append(*errs, fmt.Errorf("conflict on %s between rules %s and %s: %v != %v", currPath, ownerRule, currentRule, dstVal, srcVal))
			continue
		}
	}
}
