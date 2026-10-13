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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

func createToyRule(id, nodeName string, labels map[string]string, wakeAt time.Time) Rule[string] {
	return Rule[string]{
		Name: id,
		When: func(s string) bool { return true },
		Then: func(s string, fx *Effects) error {
			fx.Apply(corev1ac.Node(nodeName).WithLabels(labels))
			if !wakeAt.IsZero() {
				fx.WakeAt(wakeAt)
			}
			return nil
		},
	}
}

func TestReconcile_PolicyConflictReturnsError(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()

	c := Controller[string]{
		Name:         "conflict-controller",
		FieldManager: "conflict-manager",
		Fetch:        func(ctx context.Context, rd Reader, key Key) (string, error) { return "data", nil },
		Rules: []Rule[string]{
			createConflictingRule("rule-1", "val-1"),
			createConflictingRule("rule-2", "val-2"),
		},
		Owns: func(key Key) []Ref {
			return []Ref{{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: key.Name}}
		},
	}

	declaredGVKs := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Node"): true,
	}

	exec := newExecutor(c.FieldManager, false, cl, cl, record.NewFakeRecorder(100))
	rec := &reconciler[string]{
		c:            c,
		clock:        time.Now,
		scheme:       scheme,
		cacheReader:  cl,
		exec:         exec,
		declaredGVKs: declaredGVKs,
	}

	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}

	_, err := rec.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}

func TestMemoReader_RepeatableGet(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				"initial": "true",
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
	fixedTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	declared := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Node"): true,
	}

	mr := newMemoReader(cl, fixedTime, declared, scheme)
	ctx := context.Background()

	var got1 corev1.Node
	if err := mr.Get(ctx, types.NamespacedName{Name: "node-1"}, &got1); err != nil {
		t.Fatalf("first Get failed: %v", err)
	}
	if got1.Labels["initial"] != "true" {
		t.Fatalf("expected initial=true, got %v", got1.Labels)
	}

	// Update object through the client between reads
	node.Labels["initial"] = "modified"
	if err := cl.Update(ctx, node); err != nil {
		t.Fatalf("update node failed: %v", err)
	}

	// Second read must return first value
	var got2 corev1.Node
	if err := mr.Get(ctx, types.NamespacedName{Name: "node-1"}, &got2); err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	if got2.Labels["initial"] != "true" {
		t.Fatalf("expected memoized initial=true, got %v", got2.Labels["initial"])
	}
}

func TestMemoReader_RepeatableList(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod1).Build()
	fixedTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	declared := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Pod"): true,
	}

	mr := newMemoReader(cl, fixedTime, declared, scheme)
	ctx := context.Background()

	var list1 corev1.PodList
	if err := mr.List(ctx, &list1, client.InNamespace("default")); err != nil {
		t.Fatalf("first List failed: %v", err)
	}
	if len(list1.Items) != 1 {
		t.Fatalf("expected 1 pod, got %d", len(list1.Items))
	}

	// Add a new pod directly to client
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-2",
			Namespace: "default",
		},
	}
	if err := cl.Create(ctx, pod2); err != nil {
		t.Fatalf("create pod-2 failed: %v", err)
	}

	// Second list must return original memoized list (1 item)
	var list2 corev1.PodList
	if err := mr.List(ctx, &list2, client.InNamespace("default")); err != nil {
		t.Fatalf("second List failed: %v", err)
	}
	if len(list2.Items) != 1 {
		t.Fatalf("expected memoized 1 pod, got %d", len(list2.Items))
	}
}

func TestMemoReader_NotFoundMemoized(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	fixedTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	declared := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Node"): true,
	}

	mr := newMemoReader(cl, fixedTime, declared, scheme)
	ctx := context.Background()

	var node corev1.Node
	err := mr.Get(ctx, types.NamespacedName{Name: "absent"}, &node)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound error, got: %v", err)
	}

	// Create node in client
	newNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "absent"},
	}
	if err := cl.Create(ctx, newNode); err != nil {
		t.Fatalf("create node failed: %v", err)
	}

	// Second read must still return NotFound
	var node2 corev1.Node
	err2 := mr.Get(ctx, types.NamespacedName{Name: "absent"}, &node2)
	if err2 == nil || !apierrors.IsNotFound(err2) {
		t.Fatalf("expected memoized NotFound, got: %v", err2)
	}
}

func TestMemoReader_UndeclaredGVK(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	fixedTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	// Node is declared, Pod is NOT declared
	declared := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Node"): true,
	}

	mr := newMemoReader(cl, fixedTime, declared, scheme)
	ctx := context.Background()

	var pod corev1.Pod
	err := mr.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pod-1"}, &pod)
	if err == nil {
		t.Fatal("expected error for undeclared Pod, got nil")
	}
	expected := "rulekit: read of undeclared type"
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got: %v", expected, err)
	}

	var podList corev1.PodList
	err = mr.List(ctx, &podList)
	if err == nil {
		t.Fatal("expected error for undeclared PodList, got nil")
	}
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got: %v", expected, err)
	}
}

func TestMemoReader_NowFixed(t *testing.T) {
	fixedTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	mr := newMemoReader(nil, fixedTime, nil, nil)
	if !mr.Now().Equal(fixedTime) {
		t.Fatalf("expected Now() to be %v, got %v", fixedTime, mr.Now())
	}
}

func createConflictingRule(id, val string) Rule[string] {
	return Rule[string]{
		Name: id,
		When: func(s string) bool { return true },
		Then: func(s string, fx *Effects) error {
			fx.Apply(corev1ac.Node("node-1").WithLabels(map[string]string{"conflict-key": val}))
			return nil
		},
	}
}

type toyState struct {
	nodeName string
	bound    bool
}

func TestReconcile_EndToEndIdempotenceAndWake(t *testing.T) {
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

	ctrl := Controller[toyState]{
		Name:         "toy-controller",
		FieldManager: "toy-manager",
		Fetch: func(ctx context.Context, rd Reader, key Key) (toyState, error) {
			var podList corev1.PodList
			if err := rd.List(ctx, &podList, client.InNamespace("default")); err != nil {
				return toyState{}, err
			}
			bound := false
			for _, p := range podList.Items {
				if p.Spec.NodeName == key.Name && p.Labels["foo"] == "bar" {
					bound = true
					break
				}
			}
			return toyState{
				nodeName: key.Name,
				bound:    bound,
			}, nil
		},
		Rules: []Rule[toyState]{
			{
				Name: "label-bound-node",
				When: func(s toyState) bool { return s.bound },
				Then: func(s toyState, fx *Effects) error {
					fx.Apply(
						corev1ac.Node(s.nodeName).
							WithLabels(map[string]string{
								"timeslice.io/bound": "true",
							}),
					)
					fx.WakeAt(wakeTime)
					return nil
				},
			},
		},
		Owns: func(key Key) []Ref {
			return []Ref{
				{
					GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
					Name: key.Name,
				},
			}
		},
	}

	declaredGVKs := map[schema.GroupVersionKind]bool{
		corev1.SchemeGroupVersion.WithKind("Node"): true,
		corev1.SchemeGroupVersion.WithKind("Pod"):  true,
	}

	fakeRec := record.NewFakeRecorder(100)
	exec := newExecutor(ctrl.FieldManager, false, cl, cl, fakeRec)
	rec := &reconciler[toyState]{
		c:            ctrl,
		clock:        func() time.Time { return fixedNow },
		scheme:       scheme,
		cacheReader:  cl,
		exec:         exec,
		declaredGVKs: declaredGVKs,
	}

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
	if err := cl.Get(ctx, types.NamespacedName{Name: "node-x"}, &updatedNode); err != nil {
		t.Fatalf("get updated node: %v", err)
	}
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
