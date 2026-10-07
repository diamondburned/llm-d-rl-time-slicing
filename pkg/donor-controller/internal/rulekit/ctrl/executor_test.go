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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
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
)

type failPrecondition struct {
	msg string
}

func (f failPrecondition) String() string { return f.msg }
func (f failPrecondition) Check(ctx context.Context, live client.Reader) error {
	return fmt.Errorf("%w: %s", rulekit.ErrPreconditionFailed, f.msg)
}

func TestExecutor_SSAApplyAndExtractNode(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

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
	nodeRef := rulekit.Ref{
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

	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
	_ = clientgoscheme.AddToScheme(scheme)

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
	otherData, _ := corev1ac.ExtractNode(node, "other-manager")
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
	if err := cl.Patch(ctx, node, client.RawPatch(types.ApplyPatchType, patchDataOther), client.FieldOwner("other-manager"), client.ForceOwnership); err != nil {
		t.Fatalf("apply other-manager failed: %v", err)
	}

	// Apply under donor-manager
	patchDataDonor := []byte(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1","labels":{"donor":"true"}}}`)
	if err := cl.Patch(ctx, node, client.RawPatch(types.ApplyPatchType, patchDataDonor), client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		t.Fatalf("apply donor-manager failed: %v", err)
	}

	// Verify both labels are present
	var current corev1.Node
	_ = cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &current)
	if current.Labels["unrelated"] != "survives" || current.Labels["donor"] != "true" {
		t.Fatalf("setup labels failed: %v", current.Labels)
	}

	// Now run executor with identity-only desired (release everything owned by fieldManager)
	exec := newExecutor(fieldManager, false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := rulekit.Ref{
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
	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
	_ = clientgoscheme.AddToScheme(scheme)

	fieldManager := "test-manager"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			UID:  "uid-1",
		},
	}

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Delete(ctx, obj, opts...)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithReturnManagedFields().
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor(fieldManager, false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := rulekit.Ref{
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
	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
	_ = clientgoscheme.AddToScheme(scheme)

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Patch(ctx, obj, patch, opts...)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := rulekit.Ref{
		GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
		Name: "node-1",
	}

	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
		Preconditions: []rulekit.RecordedPrecondition{
			{
				Rule:         "rule-1",
				Precondition: failPrecondition{msg: "live state not ready"},
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
	apiErrPlan := &rulekit.Plan{
		Objects: plan.Objects,
		Preconditions: []rulekit.RecordedPrecondition{
			{
				Rule:         "rule-1",
				Precondition: apiErrPrecondition{msg: "connection refused"},
			},
		},
	}
	_, errApi := exec.Execute(ctx, mr, apiErrPlan)
	if errApi == nil || errors.Is(errApi, rulekit.ErrPreconditionFailed) {
		t.Fatalf("expected API error to be returned, got: %v", errApi)
	}
}

type apiErrPrecondition struct {
	msg string
}

func (a apiErrPrecondition) String() string { return a.msg }
func (a apiErrPrecondition) Check(ctx context.Context, live client.Reader) error {
	return errors.New(a.msg)
}

func TestFakeClient_UIDMismatch(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

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
	t.Logf("Mismatched UID error: %v (isConflict: %v, isInvalid: %v, type: %T)", err, apierrors.IsConflict(err), apierrors.IsInvalid(err), err)

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
	t.Logf("Absent with UID error: %v (isConflict: %v, isInvalid: %v, type: %T)", errAbsent, apierrors.IsConflict(errAbsent), apierrors.IsInvalid(errAbsent), errAbsent)
}

func TestExecutor_DeleteWithUIDAndNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

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
		plan := &rulekit.Plan{
			Deletes: []rulekit.RecordedDelete{
				{Rule: "rule-clean", Object: pod},
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
		plan := &rulekit.Plan{
			Deletes: []rulekit.RecordedDelete{
				{Rule: "rule-clean", Object: ghost},
			},
		}
		if _, err := exec.Execute(ctx, mr, plan); err != nil {
			t.Fatalf("expected NotFound delete to succeed, got: %v", err)
		}
	})
}

func TestExecutor_TaintPreservesUnrelated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

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
	planAdd := &rulekit.Plan{
		Taints: []rulekit.PlannedTaint{
			{
				ManagedTaint: rulekit.ManagedTaint{
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
	_ = cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &updatedNode)
	if len(updatedNode.Spec.Taints) != 2 {
		t.Fatalf("expected 2 taints, got: %v", updatedNode.Spec.Taints)
	}

	// 2. Remove managed taint
	planRemove := &rulekit.Plan{
		Taints: []rulekit.PlannedTaint{
			{
				ManagedTaint: rulekit.ManagedTaint{
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
	_ = cl.Get(ctx, types.NamespacedName{Name: "node-1"}, &finalNode)
	if len(finalNode.Spec.Taints) != 1 {
		t.Fatalf("expected 1 taint remaining, got: %v", finalNode.Spec.Taints)
	}
	if finalNode.Spec.Taints[0].Key != "unrelated-taint" {
		t.Errorf("expected unrelated-taint to be preserved, got: %v", finalNode.Spec.Taints[0])
	}
}

func TestExecutor_DryRun(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Delete(ctx, obj, opts...)
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
	nodeRef := rulekit.Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
		Taints: []rulekit.PlannedTaint{
			{
				ManagedTaint: rulekit.ManagedTaint{
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
		Deletes: []rulekit.RecordedDelete{
			{
				Rule:   "rule-del",
				Object: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"}},
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
	_ = clientgoscheme.AddToScheme(scheme)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", UID: "pod-uid"},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, pod).Build()
	rec := record.NewFakeRecorder(100)
	exec := newExecutor("test-manager", false, cl, cl, rec)
	nodeRef := rulekit.Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
		Taints: []rulekit.PlannedTaint{
			{
				ManagedTaint: rulekit.ManagedTaint{
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
		Deletes: []rulekit.RecordedDelete{
			{
				Rule:   "rule-del-pod",
				Object: pod,
			},
		},
	}

	mr := newMemoReader(cl, time.Now(), map[schema.GroupVersionKind]bool{nodeRef.GVK: true, corev1.SchemeGroupVersion.WithKind("Pod"): true}, scheme)
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
	_ = clientgoscheme.AddToScheme(scheme)

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Patch(ctx, obj, patch, opts...)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(funcs).
		Build()

	exec := newExecutor("test-manager", false, cl, cl, record.NewFakeRecorder(100))
	nodeRef := rulekit.Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "nonexistent"}

	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
	_ = clientgoscheme.AddToScheme(scheme)

	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
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
	nodeRef := rulekit.Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
	if !res.Requeue {
		t.Fatalf("expected Requeue=true on stale observation conflict, got: %+v", res)
	}
}

func TestExecutor_StaleObservationUIDMismatchRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
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
	nodeRef := rulekit.Ref{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: "node-1"}

	plan := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
	if !res.Requeue {
		t.Fatalf("expected Requeue=true on UID mismatch rejection, got: %+v", res)
	}
}

func TestExecutor_TaintOnAbsentNodeIsNoop(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Patch(ctx, obj, patch, opts...)
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
	planPresent := &rulekit.Plan{
		Taints: []rulekit.PlannedTaint{
			{
				ManagedTaint: rulekit.ManagedTaint{
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
	planAbsent := &rulekit.Plan{
		Taints: []rulekit.PlannedTaint{
			{
				ManagedTaint: rulekit.ManagedTaint{
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
	_ = clientgoscheme.AddToScheme(scheme)

	fieldManager := "donor-controller"
	ctx := context.Background()
	nodeGVK := corev1.SchemeGroupVersion.WithKind("Node")
	nodeRef := rulekit.Ref{GVK: nodeGVK, Name: "node-1"}

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
	planApply := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
				Rules: []string{"W1", "W2"},
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
	if !strings.Contains(ev1, "Applied by rules [W1, W2]: set labels=[timeslice.io/donor] annotations=[timeslice.io/idle-since]") {
		t.Fatalf("unexpected Applied event message: %s", ev1)
	}
	// Must NOT contain the foreign label!
	if strings.Contains(ev1, "foreign") {
		t.Fatalf("foreign label leaked into Applied event message: %s", ev1)
	}

	// Step 2: Release fields (no rules assert fields) and release taint (no rule asserts taint)
	planRelease := &rulekit.Plan{
		Objects: []rulekit.PlannedObject{
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
		Taints: []rulekit.PlannedTaint{
			{
				ManagedTaint: rulekit.ManagedTaint{
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
