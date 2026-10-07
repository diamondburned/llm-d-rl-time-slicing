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
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestWatchPredicate_UpdateEventPassesWhenOldMatches(t *testing.T) {
	filter := func(obj client.Object) bool {
		return obj.GetLabels()["timeslice.io/donor"] == "true"
	}
	pred := newWatchPredicate(filter)

	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"timeslice.io/donor": "true"},
		},
	}
	newPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{}, // label removed
		},
	}

	// Old matches, new does not -> must pass
	ev := event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}
	if !pred.Update(ev) {
		t.Fatal("expected Update event to pass when old object matches filter and new does not")
	}

	// Neither matches -> must drop
	unrelatedOld := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{}}
	ev2 := event.UpdateEvent{ObjectOld: unrelatedOld, ObjectNew: newPod}
	if pred.Update(ev2) {
		t.Fatal("expected Update event to be dropped when neither old nor new matches filter")
	}

	// Both match -> must pass
	ev3 := event.UpdateEvent{ObjectOld: oldPod, ObjectNew: oldPod}
	if !pred.Update(ev3) {
		t.Fatal("expected Update event to pass when both old and new match filter")
	}

	// Only new matches -> must pass
	ev4 := event.UpdateEvent{ObjectOld: unrelatedOld, ObjectNew: oldPod}
	if !pred.Update(ev4) {
		t.Fatal("expected Update event to pass when only new object matches filter")
	}
}

type toyEvaluator struct {
	nodeName string
	bound    bool
	wakeAt   time.Time
}

func (e *toyEvaluator) Should(ctx context.Context) (bool, error) {
	return e.bound, nil
}

func (e *toyEvaluator) Do(ctx context.Context, fx *rulekit.Effects) error {
	fx.Apply(
		corev1ac.Node(e.nodeName).
			WithLabels(map[string]string{
				"timeslice.io/bound": "true",
			}),
	)
	fx.WakeAt(e.wakeAt)
	return nil
}

type toyRule struct {
	wakeAt time.Time
}

func (r *toyRule) ID() string                { return "label-bound-node" }
func (r *toyRule) Sources() []rulekit.Source { return nil }
func (r *toyRule) Gather(ctx context.Context, rd rulekit.Reader, key rulekit.Key) (rulekit.Evaluator, error) {
	var podList corev1.PodList
	if err := rd.List(ctx, &podList, client.InNamespace("default")); err != nil {
		return nil, err
	}

	bound := false
	for _, p := range podList.Items {
		if p.Spec.NodeName == key.Name && p.Labels["foo"] == "bar" {
			bound = true
			break
		}
	}

	return &toyEvaluator{
		nodeName: key.Name,
		bound:    bound,
		wakeAt:   r.wakeAt,
	}, nil
}

func TestReconcile_EndToEndIdempotenceAndWake(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	var writeCount int32
	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			atomic.AddInt32(&writeCount, 1)
			return client.Patch(ctx, obj, patch, opts...)
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-x",
			UID:  "node-uid-x",
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
			Labels:    map[string]string{"foo": "bar"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-x",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node, pod).
		WithReturnManagedFields().
		WithInterceptorFuncs(funcs).
		Build()

	fixedNow := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	wakeTime := fixedNow.Add(2 * time.Second)

	c := rulekit.Controller{
		Name:         "toy-controller",
		FieldManager: "toy-manager",
		Rules: []rulekit.Rule{
			&toyRule{wakeAt: wakeTime},
		},
		Owns: func(ctx context.Context, rd rulekit.Reader, key rulekit.Key) ([]rulekit.Ref, error) {
			return []rulekit.Ref{
				{
					GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
					Name: key.Name,
				},
			}, nil
		},
	}

	declaredGVKs := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Node"): true,
		corev1.SchemeGroupVersion.WithKind("Pod"):  true,
	}

	fakeRec := record.NewFakeRecorder(100)
	exec := newExecutor(c.FieldManager, false, cl, cl, fakeRec)
	rec := newReconciler(c, func() time.Time { return fixedNow }, scheme, cl, exec, declaredGVKs)

	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "node-x"}}

	// First Reconcile: must apply labels and set RequeueAfter
	atomic.StoreInt32(&writeCount, 0)
	res1, err := rec.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if writes := atomic.LoadInt32(&writeCount); writes != 1 {
		t.Fatalf("expected 1 write on first reconcile, got %d", writes)
	}
	if res1.RequeueAfter != 2*time.Second {
		t.Fatalf("expected RequeueAfter=2s, got %v", res1.RequeueAfter)
	}

	var updatedNode corev1.Node
	_ = cl.Get(ctx, types.NamespacedName{Name: "node-x"}, &updatedNode)
	if updatedNode.Labels["timeslice.io/bound"] != "true" {
		t.Fatalf("expected label bound=true on node, got: %v", updatedNode.Labels)
	}

	// Second Reconcile: IDEMPOTENT -> zero writes, but preserves WakeAt
	atomic.StoreInt32(&writeCount, 0)
	res2, err := rec.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if writes := atomic.LoadInt32(&writeCount); writes != 0 {
		t.Fatalf("expected 0 writes on second reconcile (idempotent), got %d", writes)
	}
	if res2.RequeueAfter != 2*time.Second {
		t.Fatalf("expected RequeueAfter=2s on second reconcile, got %v", res2.RequeueAfter)
	}
}

type conflictingRule struct {
	id  string
	val string
}

func (r *conflictingRule) ID() string                { return r.id }
func (r *conflictingRule) Sources() []rulekit.Source { return nil }
func (r *conflictingRule) Gather(ctx context.Context, rd rulekit.Reader, key rulekit.Key) (rulekit.Evaluator, error) {
	return &conflictEvaluator{nodeName: key.Name, val: r.val}, nil
}

type conflictEvaluator struct {
	nodeName string
	val      string
}

func (e *conflictEvaluator) Should(ctx context.Context) (bool, error) { return true, nil }
func (e *conflictEvaluator) Do(ctx context.Context, fx *rulekit.Effects) error {
	fx.Apply(
		corev1ac.Node(e.nodeName).
			WithLabels(map[string]string{"conflict-key": e.val}),
	)
	return nil
}

func TestReconcile_PolicyConflictReturnsError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()

	c := rulekit.Controller{
		Name:         "conflict-controller",
		FieldManager: "conflict-manager",
		Rules: []rulekit.Rule{
			&conflictingRule{id: "rule-1", val: "val-1"},
			&conflictingRule{id: "rule-2", val: "val-2"},
		},
		Owns: func(ctx context.Context, rd rulekit.Reader, key rulekit.Key) ([]rulekit.Ref, error) {
			return []rulekit.Ref{
				{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: key.Name},
			}, nil
		},
	}

	declaredGVKs := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Node"): true,
	}

	exec := newExecutor(c.FieldManager, false, cl, cl, record.NewFakeRecorder(100))
	rec := newReconciler(c, time.Now, scheme, cl, exec, declaredGVKs)

	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}

	_, err := rec.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}
