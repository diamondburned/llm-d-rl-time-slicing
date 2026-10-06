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

package controller

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHostKeys(t *testing.T) {
	t.Run("PodHostKeys", func(t *testing.T) {
		if keys := PodHostKeys(nil); len(keys) != 0 {
			t.Errorf("expected empty for nil pod, got %v", keys)
		}
		if keys := PodHostKeys(&corev1.Pod{}); len(keys) != 0 {
			t.Errorf("expected empty for empty nodeName, got %v", keys)
		}
		if keys := PodHostKeys(&corev1.Pod{Spec: corev1.PodSpec{NodeName: "host-1"}}); !slices.Equal(keys, []string{"host-1"}) {
			t.Errorf("expected [host-1], got %v", keys)
		}
		if keys := PodHostKeys(&corev1.Pod{Spec: corev1.PodSpec{NodeName: "vk-host-1"}}); !slices.Equal(keys, []string{"vk-host-1", "host-1"}) {
			t.Errorf("expected [vk-host-1 host-1], got %v", keys)
		}
		if keys := PodHostKeys(&corev1.Pod{Spec: corev1.PodSpec{NodeName: "vk-"}}); !slices.Equal(keys, []string{"vk-"}) {
			t.Errorf("expected [vk-], got %v", keys)
		}
	})

	t.Run("NodeHostKeys", func(t *testing.T) {
		if keys := NodeHostKeys(nil); len(keys) != 0 {
			t.Errorf("expected empty for nil node, got %v", keys)
		}
		if keys := NodeHostKeys(&corev1.Node{}); len(keys) != 0 {
			t.Errorf("expected empty for empty name, got %v", keys)
		}
		if keys := NodeHostKeys(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "host-1"}}); !slices.Equal(keys, []string{"host-1"}) {
			t.Errorf("expected [host-1], got %v", keys)
		}
		if keys := NodeHostKeys(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "vk-host-1"}}); !slices.Equal(keys, []string{"vk-host-1", "host-1"}) {
			t.Errorf("expected [vk-host-1 host-1], got %v", keys)
		}
		if keys := NodeHostKeys(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "vk-"}}); !slices.Equal(keys, []string{"vk-"}) {
			t.Errorf("expected [vk-], got %v", keys)
		}
	})
}

func TestController_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := fake.NewClientset()

	// Simple test rules defined only for this test:
	// Rule 1: label node donor=true when donor pods present
	// Rule 2: delete VN when node absent
	rules := []policy.Rule{
		{
			ID:  "test-label-donor",
			Doc: "label node donor=true when donor pods present",
			When: func(obs *policy.Observed) bool {
				return obs.Node != nil && len(obs.DonorPods) > 0
			},
			Then: func(obs *policy.Observed, b *policy.Builder) {
				b.SetLabel(policy.DonorLabel, "true")
			},
		},
		{
			ID:  "test-delete-vn",
			Doc: "delete VN when node absent",
			When: func(obs *policy.Observed) bool {
				return obs.Node == nil && obs.VirtualNode != nil
			},
			Then: func(obs *policy.Observed, b *policy.Builder) {
				b.Require(policy.NodeAbsent{Name: obs.Host})
				b.DeleteVirtualNode(policy.NodeRef{
					Name: obs.VirtualNode.Name,
					UID:  obs.VirtualNode.UID,
				})
			},
		},
	}

	opts := Options{
		Workers:      1,
		ResyncPeriod: 10 * time.Minute,
		Clock:        time.Now,
	}

	cfg := policy.Config{}
	ctrl, err := New(client, cfg, rules, opts)
	if err != nil {
		t.Fatalf("failed to construct controller: %v", err)
	}

	go func() {
		if err := ctrl.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("controller Run exited with error: %v", err)
		}
	}()

	// 1. Creating a node + a donor pod with spec.nodeName -> node gets the label
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
		},
	}
	if _, err := client.CoreV1().Nodes().Create(ctx, node1, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node-1: %v", err)
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "donor-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	if _, err := client.CoreV1().Pods("default").Create(ctx, pod1, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create donor-pod-1: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		n, err := client.CoreV1().Nodes().Get(ctx, "node-1", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return n.Labels[policy.DonorLabel] == "true", nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for node-1 to get donor label: %v", err)
	}

	// 2. Deleting the pod -> label released
	if err := client.CoreV1().Pods("default").Delete(ctx, "donor-pod-1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete donor-pod-1: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		n, err := client.CoreV1().Nodes().Get(ctx, "node-1", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		_, hasDonorLabel := n.Labels[policy.DonorLabel]
		return !hasDonorLabel, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for node-1 to release donor label: %v", err)
	}

	// 3. Creating node n, VN vk-n (guest label) then deleting n -> vk-n deleted
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-2",
		},
	}
	if _, err := client.CoreV1().Nodes().Create(ctx, node2, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node-2: %v", err)
	}

	vn2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vk-node-2",
			UID:  types.UID("vn2-uid-123"),
			Labels: map[string]string{
				policy.GuestLabel: "true",
			},
		},
	}
	if _, err := client.CoreV1().Nodes().Create(ctx, vn2, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create vk-node-2: %v", err)
	}

	// Delete real node n2
	if err := client.CoreV1().Nodes().Delete(ctx, "node-2", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete node-2: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().Nodes().Get(ctx, "vk-node-2", metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		t.Fatalf("timed out waiting for vk-node-2 to be deleted: %v", err)
	}

	// 4. A real node named vk-x WITHOUT guest label is not deleted when node x is absent
	realNodeNamedVK := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vk-node-x",
			UID:  types.UID("real-node-uid-999"),
			// No guest label!
		},
	}
	if _, err := client.CoreV1().Nodes().Create(ctx, realNodeNamedVK, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create vk-node-x: %v", err)
	}

	// Host "node-x" is absent. Wait a moment and verify vk-node-x is NOT deleted.
	time.Sleep(300 * time.Millisecond)
	storedRealVK, err := client.CoreV1().Nodes().Get(ctx, "vk-node-x", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("vk-node-x without guest label should not be deleted: %v", err)
	}
	if storedRealVK == nil {
		t.Fatal("vk-node-x unexpectedly nil")
	}
}
