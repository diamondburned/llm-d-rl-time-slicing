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
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestExecutor_SSAApplyAndExtractNode(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	fieldManager := "test-manager"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			UID:  "uid-1",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithReturnManagedFields().
		Build()

	exec := newExecutor(fieldManager, false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{
		GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
		Name: "node-1",
	}

	desired := map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]any{
			"name": "node-1",
			"labels": map[string]any{
				"timeslice.io/donor": "true",
			},
		},
	}

	plan := &Plan{
		Objects: []PlannedObject{
			{Ref: nodeRef, Desired: desired, Rules: []string{"rule-donor"}},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	ctx := context.Background()

	res, err := exec.Execute(ctx, mr, plan)
	if err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got %v", res.RequeueAfter)
	}

	var appliedNode corev1.Node
	if err := cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &appliedNode); err != nil {
		t.Fatalf("get applied node failed: %v", err)
	}
	if appliedNode.Labels["timeslice.io/donor"] != "true" {
		t.Fatalf("expected label donor=true, got %v", appliedNode.Labels)
	}

	ac, err := corev1ac.ExtractNode(&appliedNode, fieldManager)
	if err != nil {
		t.Fatalf("ExtractNode failed: %v", err)
	}
	if ac == nil || ac.Labels == nil || ac.Labels["timeslice.io/donor"] != "true" {
		t.Fatalf("expected ExtractNode to return donor=true, got: %+v", ac)
	}
}

func TestExecutor_ReleaseOwnedLabels(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	fieldManager := "donor-manager"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			UID:  "uid-1",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithReturnManagedFields().
		Build()

	ctx := context.Background()

	// Apply label under "other-manager"
	otherAc := corev1ac.Node("node-1").
		WithLabels(map[string]string{"unrelated": "survives"})
	otherData, err := corev1ac.ExtractNode(node, "other-manager")
	if err != nil {
		t.Fatalf("ExtractNode failed: %v", err)
	}
	_ = otherData
	otherUnstructured := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				"unrelated": "survives",
			},
		},
	}
	_ = otherUnstructured
	_ = otherAc

	// Apply under other-manager via Apply
	patchDataOther := []byte(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1","labels":{"unrelated":"survives"}}}`)
	if err := cl.Patch(
		ctx, node, client.RawPatch(types.ApplyPatchType, patchDataOther),
		client.FieldOwner("other-manager"), client.ForceOwnership,
	); err != nil {
		t.Fatalf("apply other-manager failed: %v", err)
	}

	// Apply under donor-manager
	patchDataDonor := []byte(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1","labels":{"donor":"true"}}}`)
	if err := cl.Patch(
		ctx, node, client.RawPatch(types.ApplyPatchType, patchDataDonor),
		client.FieldOwner(fieldManager), client.ForceOwnership,
	); err != nil {
		t.Fatalf("apply donor-manager failed: %v", err)
	}

	// Verify both labels are present
	var current corev1.Node
	if err := cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &current); err != nil {
		t.Fatalf("get node-1: %v", err)
	}
	if current.Labels["unrelated"] != "survives" || current.Labels["donor"] != "true" {
		t.Fatalf("setup labels failed: %v", current.Labels)
	}

	// Now run executor with identity-only desired (release everything owned by fieldManager)
	exec := newExecutor(fieldManager, false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{
		GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
		Name: "node-1",
	}
	identityDesired := map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]any{
			"name": "node-1",
		},
	}
	plan := &Plan{
		Objects: []PlannedObject{
			{Ref: nodeRef, Desired: identityDesired, Rules: nil},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	if _, err := exec.Execute(ctx, mr, plan); err != nil {
		t.Fatalf("execute release failed: %v", err)
	}

	var after corev1.Node
	if err := cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &after); err != nil {
		t.Fatalf("get after release failed: %v", err)
	}
	if _, exists := after.Labels["donor"]; exists {
		t.Errorf("expected donor label to be removed, but still present: %v", after.Labels)
	}
	if after.Labels["unrelated"] != "survives" {
		t.Errorf("expected unrelated label to survive, got %v", after.Labels)
	}

	ac, err := corev1ac.ExtractNode(&after, fieldManager)
	if err != nil {
		t.Fatalf("ExtractNode failed: %v", err)
	}
	if ac != nil && len(ac.Labels) > 0 {
		t.Errorf("expected ExtractNode to return no labels for fieldManager, got: %v", ac.Labels)
	}
}

func TestExecutor_NoopPlanZeroWrites(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	fieldManager := "test-manager"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			UID:  "uid-1",
		},
	}

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			cl client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			atomic.AddInt32(&writeCount, 1)
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			atomic.AddInt32(&writeCount, 1)
			return cl.Delete(ctx, obj, opts...)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithReturnManagedFields().
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor(fieldManager, false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{
		GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
		Name: "node-1",
	}

	// Desired is identity-only, node currently owns nothing -> no-op!
	identityDesired := map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]any{
			"name": "node-1",
		},
	}
	plan := &Plan{
		Objects: []PlannedObject{
			{Ref: nodeRef, Desired: identityDesired, Rules: nil},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	ctx := context.Background()

	atomic.StoreInt32(&writeCount, 0)
	if _, err := exec.Execute(ctx, mr, plan); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if writes := atomic.LoadInt32(&writeCount); writes != 0 {
		t.Fatalf("expected 0 writes for no-op plan, got %d", writes)
	}
}

func TestExecutor_PreconditionFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			cl client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			atomic.AddInt32(&writeCount, 1)
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{
		GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
		Name: "node-1",
	}

	plan := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name":   "node-1",
						"labels": map[string]any{"donor": "true"},
					},
				},
				Rules: []string{"rule-1"},
			},
		},
		Preconditions: []Attributed[Precondition]{
			{
				Rule: "rule-1",
				Value: Precondition{
					Description: "live state not ready",
					Check: func(ctx context.Context, live client.Reader) error {
						return ErrPreconditionFailed
					},
				},
			},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	ctx := context.Background()

	res, err := exec.Execute(ctx, mr, plan)
	if err != nil {
		t.Fatalf("expected nil error on ErrPreconditionFailed, got: %v", err)
	}
	if res.RequeueAfter != 1*time.Second {
		t.Fatalf("expected RequeueAfter=1s, got %v", res.RequeueAfter)
	}
	if writes := atomic.LoadInt32(&writeCount); writes != 0 {
		t.Fatalf("expected 0 writes on precondition failure, got %d", writes)
	}

	// Test non-precondition-failed API error
	apiErrPlan := &Plan{
		Objects: plan.Objects,
		Preconditions: []Attributed[Precondition]{
			{
				Rule: "rule-1",
				Value: Precondition{
					Description: "connection refused",
					Check: func(ctx context.Context, live client.Reader) error {
						return errors.New("connection refused")
					},
				},
			},
		},
	}
	_, errAPI := exec.Execute(ctx, mr, apiErrPlan)
	if errAPI == nil || errors.Is(errAPI, ErrPreconditionFailed) {
		t.Fatalf("expected API error to be returned, got: %v", errAPI)
	}
}

func TestFakeClient_UIDMismatch(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			UID:  "uid-1",
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).WithReturnManagedFields().Build()
	ctx := context.Background()

	u := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]any{
				"name": "node-1",
				"uid":  "uid-2",
			},
		},
	}
	err := cl.Patch(ctx, u, client.Apply, client.FieldOwner("test"), client.ForceOwnership)
	t.Logf("Mismatched UID error: %v (isConflict: %v, isInvalid: %v, type: %T)",
		err, apierrors.IsConflict(err), apierrors.IsInvalid(err), err)

	// Object absent SSA apply with UID
	uAbsent := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]any{
				"name": "absent-node",
				"uid":  "uid-absent",
			},
		},
	}
	errAbsent := cl.Patch(ctx, uAbsent, client.Apply, client.FieldOwner("test"), client.ForceOwnership)
	t.Logf("Absent with UID error: %v (isConflict: %v, isInvalid: %v, type: %T)",
		errAbsent, apierrors.IsConflict(errAbsent), apierrors.IsInvalid(errAbsent), errAbsent)
}

func TestExecutor_DeleteWithUIDAndNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "victim",
			Namespace: "default",
			UID:       "victim-uid-123",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{corev1.SchemeGroupVersion.WithKind("Pod"): true}, scheme)
	ctx := context.Background()

	t.Run("delete with UID", func(t *testing.T) {
		plan := &Plan{
			Deletes: []Attributed[client.Object]{
				{Rule: "rule-clean", Value: pod},
			},
		}
		if _, err := exec.Execute(ctx, mr, plan); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
		var check corev1.Pod
		err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "victim"}, &check)
		if err == nil {
			t.Fatal("expected pod to be deleted, but still exists")
		}
	})

	t.Run("delete NotFound ok", func(t *testing.T) {
		ghost := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ghost-pod",
				Namespace: "default",
				UID:       "ghost-uid",
			},
		}
		plan := &Plan{
			Deletes: []Attributed[client.Object]{
				{Rule: "rule-clean", Value: ghost},
			},
		}
		if _, err := exec.Execute(ctx, mr, plan); err != nil {
			t.Fatalf("expected NotFound delete to succeed, got: %v", err)
		}
	})
}

func TestExecutor_TaintPreservesUnrelated(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:    "unrelated-taint",
					Value:  "keep-me",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	ctx := context.Background()
	nodeGVK := corev1.SchemeGroupVersion.WithKind("Node")

	// 1. Add managed taint
	planAdd := &Plan{
		Taints: []PlannedTaint{
			{
				ManagedTaint: ManagedTaint{
					Node:   "node-1",
					Key:    "timeslice.io/isolated",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Present: true,
				Taint: &corev1.Taint{
					Key:    "timeslice.io/isolated",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Rules: []string{"rule-taint"},
			},
		},
	}

	mr1 := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeGVK: true}, scheme)
	if _, err := exec.Execute(ctx, mr1, planAdd); err != nil {
		t.Fatalf("execute taint add failed: %v", err)
	}

	var updatedNode corev1.Node
	if err := cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &updatedNode); err != nil {
		t.Fatalf("get updated node: %v", err)
	}
	if len(updatedNode.Spec.Taints) != 2 {
		t.Fatalf("expected 2 taints, got: %v", updatedNode.Spec.Taints)
	}

	// 2. Remove managed taint
	planRemove := &Plan{
		Taints: []PlannedTaint{
			{
				ManagedTaint: ManagedTaint{
					Node:   "node-1",
					Key:    "timeslice.io/isolated",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Present: false,
				Taint:   nil,
				Rules:   nil,
			},
		},
	}

	mr2 := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeGVK: true}, scheme)
	if _, err := exec.Execute(ctx, mr2, planRemove); err != nil {
		t.Fatalf("execute taint remove failed: %v", err)
	}

	var finalNode corev1.Node
	if err := cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &finalNode); err != nil {
		t.Fatalf("get final node: %v", err)
	}
	if len(finalNode.Spec.Taints) != 1 {
		t.Fatalf("expected 1 taint remaining, got: %v", finalNode.Spec.Taints)
	}
	if finalNode.Spec.Taints[0].Key != "unrelated-taint" {
		t.Errorf("expected unrelated-taint to be preserved, got: %v", finalNode.Spec.Taints[0])
	}
}

func TestExecutor_DryRun(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			cl client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			atomic.AddInt32(&writeCount, 1)
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			atomic.AddInt32(&writeCount, 1)
			return cl.Delete(ctx, obj, opts...)
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", true, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name":   "node-1",
						"labels": map[string]any{"donor": "true"},
					},
				},
				Rules: []string{"rule-donor"},
			},
		},
		Taints: []PlannedTaint{
			{
				ManagedTaint: ManagedTaint{
					Node:   "node-1",
					Key:    "timeslice.io/isolated",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Present: true,
				Taint: &corev1.Taint{
					Key:    "timeslice.io/isolated",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Rules: []string{"rule-taint"},
			},
		},
		Deletes: []Attributed[client.Object]{
			{
				Rule:  "rule-del",
				Value: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"}},
			},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	ctx := context.Background()

	atomic.StoreInt32(&writeCount, 0)
	if _, err := exec.Execute(ctx, mr, plan); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if writes := atomic.LoadInt32(&writeCount); writes != 0 {
		t.Fatalf("expected 0 writes in dry-run mode, got %d", writes)
	}
}

func TestExecutor_EventsRecordedWithRuleIDs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", UID: "pod-uid"},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, pod).Build()
	rec := record.NewFakeRecorder(100)
	exec := newExecutor("test-manager", false, cl, cl, rec)
	nodeRef := Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name":   "node-1",
						"labels": map[string]any{"donor": "true"},
					},
				},
				Rules: []string{"rule-apply-labels"},
			},
		},
		Taints: []PlannedTaint{
			{
				ManagedTaint: ManagedTaint{
					Node:   "node-1",
					Key:    "timeslice.io/isolated",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Present: true,
				Taint: &corev1.Taint{
					Key:    "timeslice.io/isolated",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Rules: []string{"rule-taint-node"},
			},
		},
		Deletes: []Attributed[client.Object]{
			{
				Rule:  "rule-del-pod",
				Value: pod,
			},
		},
	}

	declaredGVKs := map[schema.GroupVersionKind]bool{
		nodeRef.GVK: true,
		corev1.SchemeGroupVersion.WithKind("Pod"): true,
	}
	mr := newMemoReader(cl, time.Now(), declaredGVKs, scheme)
	ctx := context.Background()

	if _, err := exec.Execute(ctx, mr, plan); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var events []string
drain:
	for {
		select {
		case ev := <-rec.Events:
			events = append(events, ev)
		default:
			break drain
		}
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events (Applied, Tainted, Deleted), got %d: %v", len(events), events)
	}

	var foundApplied, foundTainted, foundDeleted bool
	for _, ev := range events {
		if strings.Contains(ev, "Applied") && strings.Contains(ev, "rule-apply-labels") {
			foundApplied = true
		}
		if strings.Contains(ev, "Tainted") && strings.Contains(ev, "rule-taint-node") {
			foundTainted = true
		}
		if strings.Contains(ev, "Deleted") && strings.Contains(ev, "rule-del-pod") {
			foundDeleted = true
		}
	}

	if !foundApplied || !foundTainted || !foundDeleted {
		t.Errorf("missing expected events with rule IDs. Found: %v", events)
	}
}

func TestExecutor_AbsentObjectIdentityOnlyNoWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			cl client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			atomic.AddInt32(&writeCount, 1)
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "nonexistent"}

	plan := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name": "nonexistent",
					},
				},
				Rules: nil,
			},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	ctx := context.Background()

	atomic.StoreInt32(&writeCount, 0)
	if _, err := exec.Execute(ctx, mr, plan); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if writes := atomic.LoadInt32(&writeCount); writes != 0 {
		t.Fatalf("expected 0 writes for absent object with identity-only desired, got %d", writes)
	}
}

func TestExecutor_StaleObservationConflictRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	funcs := interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			cl client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			return apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, "node-1", errors.New("stale RV"))
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "uid-1"},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name":   "node-1",
						"labels": map[string]any{"donor": "true"},
					},
				},
				Rules: []string{"rule-1"},
			},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	ctx := context.Background()

	res, err := exec.Execute(ctx, mr, plan)
	if err != nil {
		t.Fatalf("expected nil error on stale observation conflict, got: %v", err)
	}
	if res != (reconcile.Result{Requeue: true}) {
		t.Fatalf("expected Requeue=true on stale observation conflict, got: %+v", res)
	}
}

func TestExecutor_StaleObservationUIDMismatchRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	funcs := interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			cl client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			return apierrors.NewInvalid(
				schema.GroupKind{Kind: "Node"},
				"node-1",
				field.ErrorList{field.Invalid(field.NewPath("metadata", "uid"), "uid-2", "uid mismatch")},
			)
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "uid-1"},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name":   "node-1",
						"labels": map[string]any{"donor": "true"},
					},
				},
				Rules: []string{"rule-1"},
			},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true}, scheme)
	ctx := context.Background()

	res, err := exec.Execute(ctx, mr, plan)
	if err != nil {
		t.Fatalf("expected nil error on UID mismatch rejection, got: %v", err)
	}
	if res != (reconcile.Result{Requeue: true}) {
		t.Fatalf("expected Requeue=true on UID mismatch rejection, got: %+v", res)
	}
}

func TestExecutor_TaintOnAbsentNodeIsNoop(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			cl client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			atomic.AddInt32(&writeCount, 1)
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	nodeGVK := corev1.SchemeGroupVersion.WithKind("Node")
	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeGVK: true}, scheme)
	ctx := context.Background()

	// 1. Taint asserted (Present: true) on absent node -> must be no-op, 0 writes
	planPresent := &Plan{
		Taints: []PlannedTaint{
			{
				ManagedTaint: ManagedTaint{
					Node:   "missing-node",
					Key:    "timeslice.io/isolated",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Present: true,
				Taint: &corev1.Taint{
					Key:    "timeslice.io/isolated",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Rules: []string{"rule-taint"},
			},
		},
	}

	atomic.StoreInt32(&writeCount, 0)
	if _, err := exec.Execute(ctx, mr, planPresent); err != nil {
		t.Fatalf("unexpected error on taint for absent node: %v", err)
	}
	if writes := atomic.LoadInt32(&writeCount); writes != 0 {
		t.Fatalf("expected 0 writes for taint on absent node, got %d", writes)
	}

	// 2. Taint unasserted (Present: false) on absent node -> must be no-op, 0 writes
	planAbsent := &Plan{
		Taints: []PlannedTaint{
			{
				ManagedTaint: ManagedTaint{
					Node:   "missing-node",
					Key:    "timeslice.io/isolated",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Present: false,
				Taint:   nil,
				Rules:   nil,
			},
		},
	}

	atomic.StoreInt32(&writeCount, 0)
	if _, err := exec.Execute(ctx, mr, planAbsent); err != nil {
		t.Fatalf("unexpected error on absent taint for absent node: %v", err)
	}
	if writes := atomic.LoadInt32(&writeCount); writes != 0 {
		t.Fatalf("expected 0 writes for absent taint on absent node, got %d", writes)
	}
}

func TestExecutor_EventMessages_AssertedReleasedUntainted(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	fieldManager := "donor-controller"
	ctx := context.Background()
	nodeGVK := corev1.SchemeGroupVersion.WithKind("Node")
	nodeRef := Ref{GVK: nodeGVK, Name: "node-1"}

	// 1. Seed node with a foreign label from another manager and existing isolation taint
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			UID:  "node-uid-1",
			Labels: map[string]string{
				"kubernetes.io/foreign-label": "foreign-value",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:    "timeslice.io/isolated",
					Value:  "true",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithReturnManagedFields().
		Build()

	rec := record.NewFakeRecorder(100)
	exec := newExecutor(fieldManager, false, cl, cl, rec)

	// Step 1: Rule asserts donor label and idle-since annotation
	planApply := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name": "node-1",
						"labels": map[string]any{
							"timeslice.io/donor": "true",
						},
						"annotations": map[string]any{
							"timeslice.io/idle-since": "2026-10-06T00:00:00Z",
						},
					},
				},
				Rules: []string{"rule-1", "rule-2"},
			},
		},
	}

	mr1 := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeGVK: true}, scheme)
	if _, err := exec.Execute(ctx, mr1, planApply); err != nil {
		t.Fatalf("execute apply failed: %v", err)
	}

	// Drain events and verify
	var ev1 string
	select {
	case ev1 = <-rec.Events:
	default:
		t.Fatal("expected an Applied event, got none")
	}

	// Must contain asserted keys
	wantAppliedMsg := "Applied by rules [rule-1, rule-2]: " +
		"set labels=[timeslice.io/donor] annotations=[timeslice.io/idle-since]"
	if !strings.Contains(ev1, wantAppliedMsg) {
		t.Fatalf("unexpected Applied event message: %s", ev1)
	}
	// Must NOT contain the foreign label!
	if strings.Contains(ev1, "foreign") {
		t.Fatalf("foreign label leaked into Applied event message: %s", ev1)
	}

	// Step 2: Release fields (no rules assert fields) and release taint (no rule asserts taint)
	planRelease := &Plan{
		Objects: []PlannedObject{
			{
				Ref: nodeRef,
				Desired: map[string]any{
					"apiVersion": "v1",
					"kind":       "Node",
					"metadata": map[string]any{
						"name": "node-1",
					},
				},
				Rules: nil,
			},
		},
		Taints: []PlannedTaint{
			{
				ManagedTaint: ManagedTaint{
					Node:   "node-1",
					Key:    "timeslice.io/isolated",
					Effect: corev1.TaintEffectNoSchedule,
				},
				Present: false,
				Taint:   nil,
				Rules:   nil,
			},
		},
	}

	mr2 := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeGVK: true}, scheme)
	if _, err := exec.Execute(ctx, mr2, planRelease); err != nil {
		t.Fatalf("execute release failed: %v", err)
	}

	var events2 []string
drain2:
	for {
		select {
		case ev := <-rec.Events:
			events2 = append(events2, ev)
		default:
			break drain2
		}
	}

	if len(events2) != 2 {
		t.Fatalf("expected 2 events on release (Applied and Untainted), got %d: %v", len(events2), events2)
	}

	var foundReleased, foundUntainted bool
	for _, ev := range events2 {
		if strings.Contains(ev, "Applied (no rules assert fields; releasing)") &&
			strings.Contains(ev, "released labels=[timeslice.io/donor] annotations=[timeslice.io/idle-since]") {
			foundReleased = true
		}
		if strings.Contains(ev, "Untainted timeslice.io/isolated:NoSchedule: no rule asserts it") {
			foundUntainted = true
		}
	}

	if !foundReleased {
		t.Errorf("missing or incorrect Applied release event: %v", events2)
	}
	if !foundUntainted {
		t.Errorf("missing or incorrect Untainted release event: %v", events2)
	}
}

func TestExecutor_RemoveFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	t.Run("deletes then strips only the named finalizer, keeps others", func(t *testing.T) {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "node-1",
				UID:        "uid-1",
				Finalizers: []string{"keep-this", "remove-me", "keep-that"},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, cl, cl, recorder)

		plan := &Plan{
			Key: Key{Name: "host1"},
			Finalizers: []Attributed[FinalizerRemoval]{
				{
					Rule: "rule-remove-finalizer",
					Value: FinalizerRemoval{
						Object:    node.DeepCopy(),
						Finalizer: "remove-me",
					},
				},
			},
		}

		mr := newMemoReader(cl, time.Now(), nil, scheme)
		res, err := exec.Execute(context.Background(), mr, plan)
		if err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if res != (reconcile.Result{}) {
			t.Errorf("expected no requeue, got %v", res)
		}

		var appliedNode corev1.Node
		if err := cl.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &appliedNode); err != nil {
			t.Fatalf("get applied node failed: %v", err)
		}
		wantFinalizers := []string{"keep-this", "keep-that"}
		if !reflect.DeepEqual(appliedNode.Finalizers, wantFinalizers) {
			t.Errorf("Finalizers = %v, want %v", appliedNode.Finalizers, wantFinalizers)
		}

		// check event
		select {
		case ev := <-recorder.Events:
			if !strings.Contains(ev, "FinalizerRemoved") || !strings.Contains(ev, "rule-remove-finalizer") {
				t.Errorf("unexpected event: %s", ev)
			}
		default:
			t.Error("expected FinalizerRemoved event, got none")
		}
	})

	t.Run("no-op when object lacks finalizer", func(t *testing.T) {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "node-1",
				Finalizers: []string{"keep-this"},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
		// Wrap with interceptor to verify no patch was sent
		patched := false
		clWrap := interceptor.NewClient(cl, interceptor.Funcs{
			Patch: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				patch client.Patch,
				opts ...client.PatchOption,
			) error {
				patched = true
				return cl.Patch(ctx, obj, patch, opts...)
			},
		})
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, clWrap, clWrap, recorder)

		plan := &Plan{
			Key: Key{Name: "host1"},
			Finalizers: []Attributed[FinalizerRemoval]{
				{
					Rule: "rule-remove-finalizer",
					Value: FinalizerRemoval{
						Object:    node.DeepCopy(),
						Finalizer: "remove-me",
					},
				},
			},
		}

		mr := newMemoReader(clWrap, time.Now(), nil, scheme)
		_, err := exec.Execute(context.Background(), mr, plan)
		if err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if patched {
			t.Error("expected no patch sent when finalizer is already gone")
		}
	})

	t.Run("JSON-patch test failure (changed since observation) -> Requeue", func(t *testing.T) {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "node-1",
				UID:        "uid-1",
				Finalizers: []string{"keep-this", "remove-me"},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
		clWrap := interceptor.NewClient(cl, interceptor.Funcs{
			Patch: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				patch client.Patch,
				opts ...client.PatchOption,
			) error {
				err := cl.Patch(ctx, obj, patch, opts...)
				if err != nil && strings.Contains(err.Error(), "test failed") {
					return apierrors.NewInvalid(schema.GroupKind{Group: "", Kind: "Node"}, obj.GetName(), nil)
				}
				return err
			},
		})
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, clWrap, clWrap, recorder)

		// Create a snapshot missing one of the finalizers to trigger JSON patch test fail
		snap := node.DeepCopy()
		snap.Finalizers = []string{"remove-me"}

		plan := &Plan{
			Key: Key{Name: "host1"},
			Finalizers: []Attributed[FinalizerRemoval]{
				{
					Rule: "rule-remove-finalizer",
					Value: FinalizerRemoval{
						Object:    snap,
						Finalizer: "remove-me",
					},
				},
			},
		}

		mr := newMemoReader(clWrap, time.Now(), nil, scheme)
		res, err := exec.Execute(context.Background(), mr, plan)
		if err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if res != (reconcile.Result{Requeue: true}) {
			t.Errorf("expected Requeue=true on patch test failure, got %v", res)
		}
	})

	t.Run("dry run -> no write", func(t *testing.T) {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "node-1",
				Finalizers: []string{"remove-me"},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
		patched := false
		clWrap := interceptor.NewClient(cl, interceptor.Funcs{
			Patch: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				patch client.Patch,
				opts ...client.PatchOption,
			) error {
				patched = true
				return cl.Patch(ctx, obj, patch, opts...)
			},
		})
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", true, clWrap, clWrap, recorder)

		plan := &Plan{
			Key: Key{Name: "host1"},
			Finalizers: []Attributed[FinalizerRemoval]{
				{
					Rule: "rule-remove-finalizer",
					Value: FinalizerRemoval{
						Object:    node.DeepCopy(),
						Finalizer: "remove-me",
					},
				},
			},
		}

		mr := newMemoReader(clWrap, time.Now(), nil, scheme)
		_, err := exec.Execute(context.Background(), mr, plan)
		if err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if patched {
			t.Error("expected no patch sent during dry run")
		}
	})
}

func TestExecutor_WarnDeduplicated(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	mr := newMemoReader(cl, time.Now(), nil, scheme)
	ctx := context.Background()

	t.Run("same warning across 3 executes -> exactly one event", func(t *testing.T) {
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, cl, cl, recorder)
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host1"}}

		plan := &Plan{
			Key: Key{Name: "host1"},
			Warnings: []Attributed[Warning]{
				{
					Rule: "warn-rule",
					Value: Warning{
						Object:  node,
						Reason:  "SomeReason",
						Message: "some message",
					},
				},
			},
		}

		for i := 0; i < 3; i++ {
			if _, err := exec.Execute(ctx, mr, plan); err != nil {
				t.Fatalf("Execute %d: %v", i, err)
			}
		}

		if len(recorder.Events) != 1 {
			t.Errorf("expected exactly 1 event, got %d", len(recorder.Events))
		}
		<-recorder.Events // drain
	})

	t.Run("run without it then with it again -> emitted again", func(t *testing.T) {
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, cl, cl, recorder)
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host1"}}

		planWith := &Plan{
			Key: Key{Name: "host1"},
			Warnings: []Attributed[Warning]{
				{
					Rule: "warn-rule",
					Value: Warning{
						Object:  node,
						Reason:  "SomeReason",
						Message: "some message",
					},
				},
			},
		}

		planWithout := &Plan{Key: Key{Name: "host1"}}

		// Run 1: with warning
		if _, err := exec.Execute(ctx, mr, planWith); err != nil {
			t.Fatalf("Execute 1: %v", err)
		}
		if len(recorder.Events) != 1 {
			t.Errorf("expected 1 event on first run, got %d", len(recorder.Events))
		}
		<-recorder.Events // drain

		// Run 2: without warning
		if _, err := exec.Execute(ctx, mr, planWithout); err != nil {
			t.Fatalf("Execute 2: %v", err)
		}
		if len(recorder.Events) != 0 {
			t.Errorf("expected 0 events on run without warning, got %d", len(recorder.Events))
		}

		// Run 3: with warning again
		if _, err := exec.Execute(ctx, mr, planWith); err != nil {
			t.Fatalf("Execute 3: %v", err)
		}
		if len(recorder.Events) != 1 {
			t.Errorf("expected 1 event when warning returns, got %d", len(recorder.Events))
		}
		<-recorder.Events // drain
	})

	t.Run("two different messages -> two events", func(t *testing.T) {
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, cl, cl, recorder)
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host1"}}

		plan := &Plan{
			Key: Key{Name: "host1"},
			Warnings: []Attributed[Warning]{
				{
					Rule: "warn-rule",
					Value: Warning{
						Object:  node,
						Reason:  "SomeReason",
						Message: "msg1",
					},
				},
				{
					Rule: "warn-rule",
					Value: Warning{
						Object:  node,
						Reason:  "SomeReason",
						Message: "msg2",
					},
				},
			},
		}

		if _, err := exec.Execute(ctx, mr, plan); err != nil {
			t.Fatalf("Execute: %v", err)
		}

		if len(recorder.Events) != 2 {
			t.Errorf("expected 2 events for 2 different messages, got %d", len(recorder.Events))
		}
	})

	t.Run("different keys are independent", func(t *testing.T) {
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, cl, cl, recorder)
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host1"}}

		w := Attributed[Warning]{
			Rule: "warn-rule",
			Value: Warning{
				Object:  node,
				Reason:  "SomeReason",
				Message: "msg",
			},
		}

		plan1 := &Plan{Key: Key{Name: "host1"}, Warnings: []Attributed[Warning]{w}}
		plan2 := &Plan{Key: Key{Name: "host2"}, Warnings: []Attributed[Warning]{w}}

		if _, err := exec.Execute(ctx, mr, plan1); err != nil {
			t.Fatalf("Execute plan1: %v", err)
		}
		if _, err := exec.Execute(ctx, mr, plan2); err != nil {
			t.Fatalf("Execute plan2: %v", err)
		}

		if len(recorder.Events) != 2 {
			t.Errorf("expected 2 events for 2 independent keys, got %d", len(recorder.Events))
		}
	})

	t.Run("dry run -> no event", func(t *testing.T) {
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", true, cl, cl, recorder)
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host1"}}

		plan := &Plan{
			Key: Key{Name: "host1"},
			Warnings: []Attributed[Warning]{
				{
					Rule: "warn-rule",
					Value: Warning{
						Object:  node,
						Reason:  "SomeReason",
						Message: "some msg",
					},
				},
			},
		}

		if _, err := exec.Execute(ctx, mr, plan); err != nil {
			t.Fatalf("Execute: %v", err)
		}

		if len(recorder.Events) != 0 {
			t.Errorf("expected 0 events on dry run, got %d", len(recorder.Events))
		}
	})

	t.Run("warnings emitted even when precondition fails", func(t *testing.T) {
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, cl, cl, recorder)
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host1"}}

		plan := &Plan{
			Key: Key{Name: "host1"},
			Warnings: []Attributed[Warning]{
				{
					Rule: "warn-rule",
					Value: Warning{
						Object:  node,
						Reason:  "SomeReason",
						Message: "some msg",
					},
				},
			},
			Deletes: []Attributed[client.Object]{
				{
					Rule:  "delete-rule",
					Value: node,
				},
			},
			Preconditions: []Attributed[Precondition]{
				{
					Rule: "prec-rule",
					Value: Precondition{
						Description: "fail me",
						Check: func(ctx context.Context, live client.Reader) error {
							return ErrPreconditionFailed
						},
					},
				},
			},
		}

		res, err := exec.Execute(ctx, mr, plan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.RequeueAfter == 0 {
			t.Fatal("expected RequeueAfter > 0 due to precondition failure")
		}

		if len(recorder.Events) != 1 {
			t.Errorf("expected 1 event even when precondition fails, got %d", len(recorder.Events))
		}
	})
}

func TestExecutor_RemoveTaint(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	gpu := corev1.Taint{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule}
	fence := corev1.Taint{Key: "timeslice.io/gpu-fence", Value: "vk-1", Effect: corev1.TaintEffectNoSchedule}
	other := corev1.Taint{Key: "e2e/foreign", Value: "x", Effect: corev1.TaintEffectNoSchedule}
	fencedNode := func() *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "uid-1"},
			Spec:       corev1.NodeSpec{Taints: []corev1.Taint{gpu, fence, other}},
		}
	}
	planRemoving := func(observed *corev1.Node) *Plan {
		return &Plan{
			Key: Key{Name: "node-1"},
			TaintRemovals: []Attributed[TaintRemoval]{
				{Rule: "rule-remove-taint", Value: TaintRemoval{Node: observed, Taint: fence}},
			},
		}
	}
	countingPatches := func(cl client.WithWatch, patched *int) client.WithWatch {
		return interceptor.NewClient(cl, interceptor.Funcs{
			Patch: func(
				ctx context.Context,
				c client.WithWatch,
				obj client.Object,
				patch client.Patch,
				opts ...client.PatchOption,
			) error {
				*patched++
				err := c.Patch(ctx, obj, patch, opts...)
				if err != nil && strings.Contains(err.Error(), "test failed") {
					return apierrors.NewInvalid(schema.GroupKind{Group: "", Kind: "Node"}, obj.GetName(), nil)
				}
				return err
			},
		})
	}

	t.Run("removes only that taint, keeps the others in order", func(t *testing.T) {
		node := fencedNode()
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
		recorder := record.NewFakeRecorder(100)
		exec := newExecutor("test-manager", false, cl, cl, recorder)

		mr := newMemoReader(cl, time.Now(), nil, scheme)
		res, err := exec.Execute(context.Background(), mr, planRemoving(node.DeepCopy()))
		if err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if res != (reconcile.Result{}) {
			t.Errorf("expected no requeue, got %v", res)
		}
		var got corev1.Node
		if err := cl.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &got); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if want := []corev1.Taint{gpu, other}; !reflect.DeepEqual(got.Spec.Taints, want) {
			t.Errorf("Taints = %v, want %v", got.Spec.Taints, want)
		}
		select {
		case ev := <-recorder.Events:
			if !strings.Contains(ev, "Untainted") || !strings.Contains(ev, "timeslice.io/gpu-fence=vk-1:NoSchedule") ||
				!strings.Contains(ev, "rule-remove-taint") {
				t.Errorf("unexpected event: %s", ev)
			}
		default:
			t.Error("expected Untainted event, got none")
		}
	})

	t.Run("no-op when the observed node lacks the taint", func(t *testing.T) {
		node := fencedNode()
		node.Spec.Taints = []corev1.Taint{gpu}
		patched := 0
		cl := countingPatches(fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build(), &patched)
		exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))

		mr := newMemoReader(cl, time.Now(), nil, scheme)
		if _, err := exec.Execute(context.Background(), mr, planRemoving(node.DeepCopy())); err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if patched != 0 {
			t.Errorf("patches = %d, want 0", patched)
		}
	})

	t.Run("owner re-added the fence for another virtual node -> Requeue, taint kept", func(t *testing.T) {
		live := fencedNode()
		live.Spec.Taints[1].Value = "vk-2"
		patched := 0
		cl := countingPatches(fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build(), &patched)
		exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))

		mr := newMemoReader(cl, time.Now(), nil, scheme)
		res, err := exec.Execute(context.Background(), mr, planRemoving(fencedNode()))
		if err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if res != (reconcile.Result{Requeue: true}) {
			t.Errorf("expected Requeue=true on patch test failure, got %v", res)
		}
		var got corev1.Node
		if err := cl.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &got); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if !reflect.DeepEqual(got.Spec.Taints, live.Spec.Taints) {
			t.Errorf("Taints = %v, want unchanged %v", got.Spec.Taints, live.Spec.Taints)
		}
	})

	t.Run("recreated node (new UID) -> Requeue, taint kept", func(t *testing.T) {
		live := fencedNode()
		live.UID = "uid-2"
		cl := countingPatches(fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build(), new(int))
		exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))

		mr := newMemoReader(cl, time.Now(), nil, scheme)
		res, err := exec.Execute(context.Background(), mr, planRemoving(fencedNode()))
		if err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if res != (reconcile.Result{Requeue: true}) {
			t.Errorf("expected Requeue=true on UID test failure, got %v", res)
		}
	})

	t.Run("dry run -> no write", func(t *testing.T) {
		node := fencedNode()
		patched := 0
		cl := countingPatches(fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build(), &patched)
		exec := newExecutor("test-manager", true, cl, cl, record.NewFakeRecorder(100))

		mr := newMemoReader(cl, time.Now(), nil, scheme)
		if _, err := exec.Execute(context.Background(), mr, planRemoving(node.DeepCopy())); err != nil {
			t.Fatalf("unexpected execute error: %v", err)
		}
		if patched != 0 {
			t.Errorf("patches = %d, want 0 during dry run", patched)
		}
	})
}
