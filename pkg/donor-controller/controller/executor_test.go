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
	"errors"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func hasWriteAction(actions []clienttesting.Action) bool {
	for _, a := range actions {
		verb := a.GetVerb()
		if verb == "create" || verb == "update" || verb == "patch" || verb == "delete" || verb == "apply" {
			return true
		}
	}
	return false
}

func TestExecutor_Apply_SetsLabelsAndExtractNode(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}
	client := fake.NewClientset(node)
	exec := &Executor{Client: client}

	obs := &policy.Observed{
		Host: "test-node",
		Node: node,
		Now:  time.Now(),
	}

	plan := policy.Plan{
		Host: "test-node",
		Node: &policy.NodeIntent{
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
		Fired: []string{"W1"},
	}

	if err := exec.Execute(ctx, obs, plan); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	storedNode, err := client.CoreV1().Nodes().Get(ctx, "test-node", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get stored node: %v", err)
	}

	if storedNode.Labels[policy.DonorLabel] != "true" {
		t.Errorf("expected donor label on stored node, got %v", storedNode.Labels)
	}

	extracted, err := corev1ac.ExtractNode(storedNode, policy.FieldManager)
	if err != nil {
		t.Fatalf("ExtractNode failed: %v", err)
	}

	if extracted == nil || extracted.Labels == nil || extracted.Labels[policy.DonorLabel] != "true" {
		t.Errorf("expected extracted labels to contain donor label, got %v", extracted)
	}
}

func TestExecutor_Releasing(t *testing.T) {
	ctx := context.Background()
	// Node has a pre-existing label applied by a different manager / normal create
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				"custom.io/keep": "yes",
			},
		},
	}
	client := fake.NewClientset(node)
	exec := &Executor{Client: client}

	obs := &policy.Observed{
		Host: "test-node",
		Node: node,
		Now:  time.Now(),
	}

	// 1. Apply {donor, group}
	plan1 := policy.Plan{
		Host: "test-node",
		Node: &policy.NodeIntent{
			Labels: map[string]string{
				policy.DonorLabel:                      "true",
				policy.NodeGroupLabelPrefix + "groupA": "true",
			},
		},
		Fired: []string{"W1", "W2"},
	}

	if err := exec.Execute(ctx, obs, plan1); err != nil {
		t.Fatalf("Execute plan1 failed: %v", err)
	}

	storedNode1, err := client.CoreV1().Nodes().Get(ctx, "test-node", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get stored node: %v", err)
	}
	if storedNode1.Labels[policy.DonorLabel] != "true" || storedNode1.Labels[policy.NodeGroupLabelPrefix+"groupA"] != "true" {
		t.Fatalf("expected donor and group labels to be present, got %v", storedNode1.Labels)
	}
	if storedNode1.Labels["custom.io/keep"] != "yes" {
		t.Fatalf("pre-existing label lost after plan1: %v", storedNode1.Labels)
	}

	// 2. Now apply empty intent. The owned labels should be released, but custom.io/keep must survive.
	extracted, err := corev1ac.ExtractNode(storedNode1, policy.FieldManager)
	if err != nil {
		t.Fatalf("ExtractNode failed: %v", err)
	}
	obs2 := &policy.Observed{
		Host: "test-node",
		Node: storedNode1,
		Owned: policy.NodeMeta{
			Labels: extracted.Labels,
		},
		Now: time.Now(),
	}

	plan2 := policy.Plan{
		Host:  "test-node",
		Node:  &policy.NodeIntent{},
		Fired: []string{"W3"},
	}

	if err := exec.Execute(ctx, obs2, plan2); err != nil {
		t.Fatalf("Execute plan2 failed: %v", err)
	}

	storedNode2, err := client.CoreV1().Nodes().Get(ctx, "test-node", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get stored node: %v", err)
	}

	if _, ok := storedNode2.Labels[policy.DonorLabel]; ok {
		t.Errorf("donor label should have been released, but still present: %v", storedNode2.Labels)
	}
	if _, ok := storedNode2.Labels[policy.NodeGroupLabelPrefix+"groupA"]; ok {
		t.Errorf("group label should have been released, but still present: %v", storedNode2.Labels)
	}
	if storedNode2.Labels["custom.io/keep"] != "yes" {
		t.Errorf("pre-existing label custom.io/keep should survive, got: %v", storedNode2.Labels)
	}
}

func TestExecutor_IsNoop(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
	}
	client := fake.NewClientset(node)
	exec := &Executor{Client: client}

	obs := &policy.Observed{
		Host: "test-node",
		Node: node,
		Owned: policy.NodeMeta{
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
		Now: time.Now(),
	}

	plan := policy.Plan{
		Host: "test-node",
		Node: &policy.NodeIntent{
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
	}

	client.ClearActions()
	if err := exec.Execute(ctx, obs, plan); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(client.Actions()) != 0 {
		t.Errorf("expected 0 actions for IsNoop plan, got %v", client.Actions())
	}
}

func TestExecutor_NoDonorPodsOn_Violated(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
		},
	}
	donorPod := &corev1.Pod{
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

	client := fake.NewClientset(node, donorPod)
	exec := &Executor{Client: client}

	obs := &policy.Observed{
		Host: "node-1",
		Node: node,
		Now:  time.Now(),
	}

	plan := policy.Plan{
		Host: "node-1",
		Node: &policy.NodeIntent{
			Labels: map[string]string{
				"new-label": "val",
			},
		},
		Preconditions: []policy.Precondition{
			policy.NoDonorPodsOn{NodeName: "node-1"},
		},
	}

	client.ClearActions()
	err := exec.Execute(ctx, obs, plan)
	if err == nil {
		t.Fatal("expected ErrPreconditionFailed, got nil")
	}
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected errors.Is(err, ErrPreconditionFailed), got %v", err)
	}

	if hasWriteAction(client.Actions()) {
		t.Errorf("expected zero write actions when precondition failed, got actions: %v", client.Actions())
	}
}

func TestExecutor_NoDonorPodsOn_Holds(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
		},
	}
	podOnOtherNode := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "donor-pod-other",
			Namespace: "default",
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-2",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	podSucceeded := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "donor-pod-done",
			Namespace: "default",
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	client := fake.NewClientset(node, podOnOtherNode, podSucceeded)
	exec := &Executor{Client: client}

	obs := &policy.Observed{
		Host: "node-1",
		Node: node,
		Now:  time.Now(),
	}

	plan := policy.Plan{
		Host: "node-1",
		Node: &policy.NodeIntent{
			Labels: map[string]string{
				"result": "applied",
			},
		},
		Preconditions: []policy.Precondition{
			policy.NoDonorPodsOn{NodeName: "node-1"},
		},
	}

	if err := exec.Execute(ctx, obs, plan); err != nil {
		t.Fatalf("expected precondition to hold, but got: %v", err)
	}

	storedNode, err := client.CoreV1().Nodes().Get(ctx, "node-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get stored node: %v", err)
	}
	if storedNode.Labels["result"] != "applied" {
		t.Errorf("expected label result=applied, got %v", storedNode.Labels)
	}
}

func TestExecutor_DeleteVirtualNode(t *testing.T) {
	ctx := context.Background()

	t.Run("matching UID deletes", func(t *testing.T) {
		vn := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "vk-node-1",
				UID:  types.UID("test-uid-123"),
			},
		}
		client := fake.NewClientset(vn)
		exec := &Executor{Client: client}

		obs := &policy.Observed{
			Host: "node-1",
		}
		plan := policy.Plan{
			Host: "node-1",
			DeleteVirtualNode: &policy.NodeRef{
				Name: "vk-node-1",
				UID:  types.UID("test-uid-123"),
			},
		}

		if err := exec.Execute(ctx, obs, plan); err != nil {
			t.Fatalf("expected delete to succeed, got: %v", err)
		}

		_, err := client.CoreV1().Nodes().Get(ctx, "vk-node-1", metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected node to be deleted (NotFound), got err: %v", err)
		}
	})

	t.Run("NotFound counts as success", func(t *testing.T) {
		client := fake.NewClientset()
		exec := &Executor{Client: client}

		obs := &policy.Observed{
			Host: "node-1",
		}
		plan := policy.Plan{
			Host: "node-1",
			DeleteVirtualNode: &policy.NodeRef{
				Name: "vk-node-1",
				UID:  types.UID("test-uid-123"),
			},
		}

		if err := exec.Execute(ctx, obs, plan); err != nil {
			t.Fatalf("expected NotFound to count as success, got: %v", err)
		}
	})

	t.Run("NodeAbsent violated -> no delete", func(t *testing.T) {
		realNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
			},
		}
		vn := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "vk-node-1",
				UID:  types.UID("test-uid-123"),
			},
		}
		client := fake.NewClientset(realNode, vn)
		exec := &Executor{Client: client}

		obs := &policy.Observed{
			Host: "node-1",
			Node: realNode,
		}
		plan := policy.Plan{
			Host: "node-1",
			DeleteVirtualNode: &policy.NodeRef{
				Name: "vk-node-1",
				UID:  types.UID("test-uid-123"),
			},
			Preconditions: []policy.Precondition{
				policy.NodeAbsent{Name: "node-1"},
			},
		}

		client.ClearActions()
		err := exec.Execute(ctx, obs, plan)
		if err == nil {
			t.Fatal("expected ErrPreconditionFailed, got nil")
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("expected errors.Is(err, ErrPreconditionFailed), got %v", err)
		}

		// Virtual node should still exist
		storedVN, err := client.CoreV1().Nodes().Get(ctx, "vk-node-1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("virtual node should still exist, got: %v", err)
		}
		if storedVN == nil {
			t.Fatal("virtual node unexpectedly nil")
		}
	})
}

func TestExecutor_TaintAddAndRemovePreservesUnrelatedTaint(t *testing.T) {
	ctx := context.Background()
	unrelatedTaint := corev1.Taint{
		Key:    "example.com/unrelated",
		Value:  "foo",
		Effect: corev1.TaintEffectNoSchedule,
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{unrelatedTaint},
		},
	}

	isoTaint := &corev1.Taint{
		Key:    "timeslice.io/isolated",
		Value:  "true",
		Effect: corev1.TaintEffectNoSchedule,
	}

	client := fake.NewClientset(node)
	exec := &Executor{
		Client:         client,
		IsolationTaint: isoTaint,
	}

	obs := &policy.Observed{
		Host:              "test-node",
		Node:              node,
		HasIsolationTaint: false,
		Now:               time.Now(),
	}

	// 1. Add isolation taint
	planAdd := policy.Plan{
		Host: "test-node",
		Node: &policy.NodeIntent{
			IsolationTaint: isoTaint,
		},
		Fired: []string{"W5"},
	}

	if err := exec.Execute(ctx, obs, planAdd); err != nil {
		t.Fatalf("failed to add taint: %v", err)
	}

	storedNode1, err := client.CoreV1().Nodes().Get(ctx, "test-node", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node failed: %v", err)
	}

	hasUnrelated := false
	hasIso := false
	for _, t := range storedNode1.Spec.Taints {
		if t.Key == unrelatedTaint.Key {
			hasUnrelated = true
		}
		if t.Key == isoTaint.Key {
			hasIso = true
		}
	}
	if !hasUnrelated || !hasIso {
		t.Fatalf("expected both taints present, got %v", storedNode1.Spec.Taints)
	}

	// 2. Remove isolation taint
	obs2 := &policy.Observed{
		Host:              "test-node",
		Node:              storedNode1,
		HasIsolationTaint: true,
		Now:               time.Now(),
	}

	planRemove := policy.Plan{
		Host: "test-node",
		Node: &policy.NodeIntent{
			IsolationTaint: nil,
		},
		Fired: []string{"W3"},
	}

	if err := exec.Execute(ctx, obs2, planRemove); err != nil {
		t.Fatalf("failed to remove taint: %v", err)
	}

	storedNode2, err := client.CoreV1().Nodes().Get(ctx, "test-node", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node failed: %v", err)
	}

	hasUnrelated2 := false
	hasIso2 := false
	for _, t := range storedNode2.Spec.Taints {
		if t.Key == unrelatedTaint.Key {
			hasUnrelated2 = true
		}
		if t.Key == isoTaint.Key {
			hasIso2 = true
		}
	}
	if !hasUnrelated2 {
		t.Errorf("unrelated taint was lost: %v", storedNode2.Spec.Taints)
	}
	if hasIso2 {
		t.Errorf("isolation taint should have been removed: %v", storedNode2.Spec.Taints)
	}
}

func TestExecutor_DryRun_ZeroActions(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}
	client := fake.NewClientset(node)
	exec := &Executor{
		Client: client,
		DryRun: true,
	}

	obs := &policy.Observed{
		Host: "test-node",
		Node: node,
		Now:  time.Now(),
	}

	plan := policy.Plan{
		Host: "test-node",
		Node: &policy.NodeIntent{
			Labels: map[string]string{
				policy.DonorLabel: "true",
			},
		},
		DeleteVirtualNode: &policy.NodeRef{
			Name: "vk-test-node",
			UID:  types.UID("123"),
		},
		Preconditions: []policy.Precondition{
			policy.NoDonorPodsOn{NodeName: "test-node"},
		},
		Fired: []string{"W1", "W4"},
	}

	client.ClearActions()
	if err := exec.Execute(ctx, obs, plan); err != nil {
		t.Fatalf("Execute in DryRun failed: %v", err)
	}

	if len(client.Actions()) != 0 {
		t.Errorf("expected 0 client actions in DryRun, got %v", client.Actions())
	}
}
