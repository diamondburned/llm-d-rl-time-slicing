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
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
)

func TestBuildPlan_MergeTwoRules(t *testing.T) {
	nodeRef := Ref{
		GVK:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"},
		Name: "node-1",
	}

	fx := &Effects{}
	func() *Effects { fx.rule = "rule-labels"; return fx }().Apply(
		corev1ac.Node("node-1").WithLabels(map[string]string{
			"timeslice.io/donor": "true",
			"shared-label":       "same",
		}),
	)
	func() *Effects { fx.rule = "rule-annotations"; return fx }().Apply(
		corev1ac.Node("node-1").
			WithAnnotations(map[string]string{
				"timeslice.io/idle-since": "2026-10-06T00:00:00Z",
			}).
			WithLabels(map[string]string{
				"shared-label": "same", // identical overlap
			}),
	)

	plan, err := buildPlan(fx, []string{"rule-labels", "rule-annotations"}, []Ref{nodeRef}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(plan.Objects) != 1 {
		t.Fatalf("expected 1 planned object, got %d", len(plan.Objects))
	}

	obj := plan.Objects[0]
	if obj.Ref != nodeRef {
		t.Errorf("expected ref %v, got %v", nodeRef, obj.Ref)
	}
	if !reflect.DeepEqual(obj.Rules, []string{"rule-labels", "rule-annotations"}) {
		t.Errorf("expected rules [rule-labels rule-annotations], got %v", obj.Rules)
	}

	meta, ok := obj.Desired["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata map, got %T", obj.Desired["metadata"])
	}
	labels, ok := meta["labels"].(map[string]any)
	if !ok {
		t.Fatalf("expected labels map, got %T", meta["labels"])
	}
	if labels["timeslice.io/donor"] != "true" || labels["shared-label"] != "same" {
		t.Errorf("unexpected labels: %v", labels)
	}

	annotations, ok := meta["annotations"].(map[string]any)
	if !ok {
		t.Fatalf("expected annotations map, got %T", meta["annotations"])
	}
	if annotations["timeslice.io/idle-since"] != "2026-10-06T00:00:00Z" {
		t.Errorf("unexpected annotations: %v", annotations)
	}
}

func TestBuildPlan_ConflictErrorNamesPathAndRules(t *testing.T) {
	nodeRef := Ref{
		GVK:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"},
		Name: "node-1",
	}

	fx := &Effects{}
	func() *Effects { fx.rule = "rule-a"; return fx }().Apply(
		corev1ac.Node("node-1").WithLabels(map[string]string{
			"timeslice.io/donor": "true",
		}),
	)
	func() *Effects { fx.rule = "rule-b"; return fx }().Apply(
		corev1ac.Node("node-1").WithLabels(map[string]string{
			"timeslice.io/donor": "false",
		}),
	)

	_, err := buildPlan(fx, []string{"rule-a", "rule-b"}, []Ref{nodeRef}, nil)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "metadata.labels[timeslice.io/donor]") {
		t.Errorf("expected error to name path 'metadata.labels[timeslice.io/donor]', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "rule-a") || !strings.Contains(errMsg, "rule-b") {
		t.Errorf("expected error to name both rule-a and rule-b, got: %s", errMsg)
	}
}

func TestBuildPlan_UIDOrResourceVersionSet(t *testing.T) {
	nodeRef := Ref{
		GVK:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"},
		Name: "node-1",
	}

	t.Run("uid set", func(t *testing.T) {
		fx := &Effects{}
		func() *Effects { fx.rule = "rule-uid"; return fx }().Apply(
			corev1ac.Node("node-1").WithUID("12345"),
		)
		_, err := buildPlan(fx, []string{"rule-uid"}, []Ref{nodeRef}, nil)
		if err == nil {
			t.Fatal("expected error when UID is set, got nil")
		}
		if !strings.Contains(err.Error(), "metadata.uid") || !strings.Contains(err.Error(), "rule-uid") {
			t.Errorf("expected error to mention metadata.uid and rule-uid, got: %v", err)
		}
	})

	t.Run("resourceVersion set", func(t *testing.T) {
		fx := &Effects{}
		func() *Effects { fx.rule = "rule-rv"; return fx }().Apply(
			corev1ac.Node("node-1").WithResourceVersion("42"),
		)
		_, err := buildPlan(fx, []string{"rule-rv"}, []Ref{nodeRef}, nil)
		if err == nil {
			t.Fatal("expected error when resourceVersion is set, got nil")
		}
		if !strings.Contains(err.Error(), "metadata.resourceVersion") || !strings.Contains(err.Error(), "rule-rv") {
			t.Errorf("expected error to mention metadata.resourceVersion and rule-rv, got: %v", err)
		}
	})
}

func TestBuildPlan_UndeclaredRef(t *testing.T) {
	nodeRef := Ref{
		GVK:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"},
		Name: "node-1",
	}

	fx := &Effects{}
	func() *Effects { fx.rule = "rule-x"; return fx }().Apply(
		corev1ac.Node("node-2").WithLabels(map[string]string{"foo": "bar"}),
	)

	_, err := buildPlan(fx, []string{"rule-x"}, []Ref{nodeRef}, nil)
	if err == nil {
		t.Fatal("expected error for undeclared ref, got nil")
	}

	expected := "rule \"rule-x\" applied to Node/node-2 which the controller does not declare in Owns"
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("expected error containing %q, got: %v", expected, err)
	}
}

func TestBuildPlan_UnassertedOwnedRefIdentityOnly(t *testing.T) {
	nodeRef := Ref{
		GVK:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"},
		Name: "node-1",
	}

	plan, err := buildPlan(nil, []string{}, []Ref{nodeRef}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(plan.Objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(plan.Objects))
	}

	obj := plan.Objects[0]
	if obj.Ref != nodeRef {
		t.Errorf("expected ref %v, got %v", nodeRef, obj.Ref)
	}
	if len(obj.Rules) != 0 {
		t.Errorf("expected no contributing rules, got %v", obj.Rules)
	}

	if obj.Desired["apiVersion"] != "v1" {
		t.Errorf("expected apiVersion v1, got %v", obj.Desired["apiVersion"])
	}
	if obj.Desired["kind"] != "Node" {
		t.Errorf("expected kind Node, got %v", obj.Desired["kind"])
	}
	meta, ok := obj.Desired["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata map, got %T", obj.Desired["metadata"])
	}
	if meta["name"] != "node-1" {
		t.Errorf("expected name node-1, got %v", meta["name"])
	}
	if len(meta) != 1 {
		t.Errorf("expected metadata to only have 'name', got: %v", meta)
	}
}

func TestBuildPlan_Taints(t *testing.T) {
	managed := []ManagedTaint{
		{Node: "node-1", Key: "timeslice.io/isolated", Effect: corev1.TaintEffectNoSchedule},
		{Node: "node-1", Key: "timeslice.io/unasserted", Effect: corev1.TaintEffectNoSchedule},
	}

	t.Run("present and absent", func(t *testing.T) {
		fx := &Effects{}
		func() *Effects { fx.rule = "rule-taint"; return fx }().Taint("node-1", corev1.Taint{
			Key:    "timeslice.io/isolated",
			Value:  "true",
			Effect: corev1.TaintEffectNoSchedule,
		})

		plan, err := buildPlan(fx, []string{"rule-taint"}, nil, managed)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(plan.Taints) != 2 {
			t.Fatalf("expected 2 planned taints, got %d", len(plan.Taints))
		}

		t0 := plan.Taints[0]
		if !t0.Present || t0.Taint == nil || t0.Taint.Value != "true" {
			t.Errorf("expected taint 0 to be present with value true, got: %+v", t0)
		}
		if !reflect.DeepEqual(t0.Rules, []string{"rule-taint"}) {
			t.Errorf("expected rules [rule-taint], got: %v", t0.Rules)
		}

		t1 := plan.Taints[1]
		if t1.Present || t1.Taint != nil || len(t1.Rules) != 0 {
			t.Errorf("expected taint 1 to be absent, got: %+v", t1)
		}
	})

	t.Run("undeclared taint", func(t *testing.T) {
		fx := &Effects{}
		func() *Effects { fx.rule = "rule-bad"; return fx }().Taint("node-1", corev1.Taint{
			Key:    "undeclared-key",
			Value:  "true",
			Effect: corev1.TaintEffectNoSchedule,
		})

		_, err := buildPlan(fx, []string{"rule-bad"}, nil, managed)
		if err == nil {
			t.Fatal("expected error for undeclared taint, got nil")
		}
		if !strings.Contains(err.Error(), "undeclared-key") || !strings.Contains(err.Error(), "rule-bad") {
			t.Errorf("expected error to mention undeclared-key and rule-bad, got: %v", err)
		}
	})

	t.Run("conflict taint values", func(t *testing.T) {
		fx := &Effects{}
		func() *Effects { fx.rule = "rule-t1"; return fx }().Taint("node-1", corev1.Taint{
			Key:    "timeslice.io/isolated",
			Value:  "val-1",
			Effect: corev1.TaintEffectNoSchedule,
		})
		func() *Effects { fx.rule = "rule-t2"; return fx }().Taint("node-1", corev1.Taint{
			Key:    "timeslice.io/isolated",
			Value:  "val-2",
			Effect: corev1.TaintEffectNoSchedule,
		})

		_, err := buildPlan(fx, []string{"rule-t1", "rule-t2"}, nil, managed)
		if err == nil {
			t.Fatal("expected error for taint conflict, got nil")
		}
		errMsg := err.Error()
		if !strings.Contains(errMsg, "rule-t1") || !strings.Contains(errMsg, "rule-t2") {
			t.Errorf("expected error to name both rule-t1 and rule-t2, got: %s", errMsg)
		}
	})
}

func TestBuildPlan_Passthroughs(t *testing.T) {
	fx := &Effects{}
	now := time.Now()
	wakeTime := now.Add(5 * time.Second)
	func() *Effects { fx.rule = "rule-wake"; return fx }().WakeAt(wakeTime)

	pod := &corev1.Pod{}
	pod.Name = "pod-to-delete"
	func() *Effects { fx.rule = "rule-del"; return fx }().Delete(pod)

	plan, err := buildPlan(fx, []string{"rule-wake", "rule-del"}, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(plan.Fired, []string{"rule-wake", "rule-del"}) {
		t.Errorf("expected Fired [rule-wake rule-del], got: %v", plan.Fired)
	}
	if plan.Wake == nil || !plan.Wake.Value.Equal(wakeTime) || plan.Wake.Rule != "rule-wake" {
		t.Errorf("unexpected Wake: %+v", plan.Wake)
	}
	if len(plan.Deletes) != 1 || plan.Deletes[0].Rule != "rule-del" {
		t.Errorf("unexpected Deletes: %+v", plan.Deletes)
	}
}

func TestEvaluate_KeyAndPassThrough(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host1"}}

	rule := Rule[string]{
		Name: "test-rule",
		When: func(data string) bool { return true },
		Then: func(data string, fx *Effects) error {
			fx.RemoveFinalizer(node, "remove-me")
			fx.Warn(node, "SomeReason", "some msg")
			return nil
		},
	}

	ctrl := Controller[string]{
		Rules: []Rule[string]{rule},
	}

	plan, err := ctrl.Evaluate(Key{Name: "host1"}, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.Key.Name != "host1" {
		t.Errorf("expected plan.Key.Name = host1, got %q", plan.Key.Name)
	}

	if len(plan.Finalizers) != 1 {
		t.Errorf("expected 1 finalizer removal, got %d", len(plan.Finalizers))
	} else {
		fin := plan.Finalizers[0]
		if fin.Rule != "test-rule" {
			t.Errorf("expected fin.Rule = test-rule, got %q", fin.Rule)
		}
		if fin.Value.Finalizer != "remove-me" {
			t.Errorf("expected fin.Value.Finalizer = remove-me, got %q", fin.Value.Finalizer)
		}
	}

	if len(plan.Warnings) != 1 {
		t.Errorf("expected 1 warning, got %d", len(plan.Warnings))
	} else {
		warn := plan.Warnings[0]
		if warn.Rule != "test-rule" {
			t.Errorf("expected warn.Rule = test-rule, got %q", warn.Rule)
		}
		if warn.Value.Reason != "SomeReason" || warn.Value.Message != "some msg" {
			t.Errorf("unexpected warning: %+v", warn.Value)
		}
	}
}

func TestBuildPlan_RemoveTaint(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}
	managed := []ManagedTaint{{Node: "n1", Key: "managed", Effect: corev1.TaintEffectNoSchedule}}

	t.Run("foreign taint -> carried in the plan", func(t *testing.T) {
		fx := &Effects{rule: "r1"}
		fx.RemoveTaint(node, corev1.Taint{Key: "foreign", Effect: corev1.TaintEffectNoSchedule})
		plan, err := buildPlan(fx, []string{"r1"}, nil, managed)
		if err != nil {
			t.Fatalf("buildPlan: %v", err)
		}
		if len(plan.TaintRemovals) != 1 || plan.TaintRemovals[0].Rule != "r1" || plan.TaintRemovals[0].Value.Taint.Key != "foreign" {
			t.Errorf("TaintRemovals = %+v, want the foreign taint from r1", plan.TaintRemovals)
		}
	})

	t.Run("managed taint -> conflict", func(t *testing.T) {
		fx := &Effects{rule: "r1"}
		fx.RemoveTaint(node, corev1.Taint{Key: "managed", Effect: corev1.TaintEffectNoSchedule})
		_, err := buildPlan(fx, []string{"r1"}, nil, managed)
		if err == nil || !strings.Contains(err.Error(), "managed taint") || !strings.Contains(err.Error(), `"r1"`) {
			t.Errorf("buildPlan error = %v, want a managed-taint conflict naming r1", err)
		}
	})
}
