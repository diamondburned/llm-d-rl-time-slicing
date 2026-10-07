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

package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func buildPreconditionsTestClient(objs ...client.Object) client.Reader {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, IndexPodNodeName, func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		Build()
}

func TestNoDonorPodsOn(t *testing.T) {
	ctx := context.Background()

	t.Run("no pods on node -> holds", func(t *testing.T) {
		cl := buildPreconditionsTestClient()
		p := NoDonorPodsOn{NodeName: "host1"}
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("running donor pod on node -> fails", func(t *testing.T) {
		runningDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		cl := buildPreconditionsTestClient(runningDonor)
		p := NoDonorPodsOn{NodeName: "host1"}
		err := p.Check(ctx, cl)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, rulekit.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
		}
	})

	t.Run("succeeded donor pod on node -> holds", func(t *testing.T) {
		succeededDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}
		cl := buildPreconditionsTestClient(succeededDonor)
		p := NoDonorPodsOn{NodeName: "host1"}
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("failed donor pod on node -> holds", func(t *testing.T) {
		failedDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		}
		cl := buildPreconditionsTestClient(failedDonor)
		p := NoDonorPodsOn{NodeName: "host1"}
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("running donor pod on different node -> holds", func(t *testing.T) {
		otherNodePod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: "host2"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		cl := buildPreconditionsTestClient(otherNodePod)
		p := NoDonorPodsOn{NodeName: "host1"}
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("running non-donor pod on node -> holds", func(t *testing.T) {
		nonDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		cl := buildPreconditionsTestClient(nonDonor)
		p := NoDonorPodsOn{NodeName: "host1"}
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestNodeAbsent(t *testing.T) {
	ctx := context.Background()

	t.Run("node absent -> holds", func(t *testing.T) {
		cl := buildPreconditionsTestClient()
		p := NodeAbsent{Name: "host1"}
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("node exists -> fails", func(t *testing.T) {
		n := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1"},
		}
		cl := buildPreconditionsTestClient(n)
		p := NodeAbsent{Name: "host1"}
		err := p.Check(ctx, cl)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, rulekit.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
		}
	})
}
