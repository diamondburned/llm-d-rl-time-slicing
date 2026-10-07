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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMemoReader_RepeatableGet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

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
	_ = clientgoscheme.AddToScheme(scheme)

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
	_ = clientgoscheme.AddToScheme(scheme)

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
	_ = clientgoscheme.AddToScheme(scheme)

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
