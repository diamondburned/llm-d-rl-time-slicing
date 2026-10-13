// Copyright 2026 The llm-state Authors.
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

package donorcontroller

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type testReader struct {
	client.Reader
	now time.Time
}

func (r testReader) Now() time.Time {
	return r.now
}

type fakeLockSource struct {
	groupLock func(group string) (GroupLock, error)
	called    []string
}

func (f *fakeLockSource) GroupLock(ctx context.Context, group string) (GroupLock, error) {
	f.called = append(f.called, group)
	if f.groupLock != nil {
		return f.groupLock(group)
	}
	return GroupLockIdle, nil
}

func buildTestClient(objs ...client.Object) client.Reader {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithReturnManagedFields().
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, indexPodNodeName, func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		WithIndex(&corev1.Node{}, indexVirtualNodeHost, func(obj client.Object) []string {
			if node, ok := obj.(*corev1.Node); ok {
				if ref := virtualNodeHostReference(node); ref != nil {
					return []string{ref.Name}
				}
			}
			return nil
		}).
		Build()
}

func TestFetchHostState(t *testing.T) {
	fixedNow := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	t.Run("virtual nodes via ownerRef", func(t *testing.T) {
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1", UID: "uid-host1"},
		}
		vnNodeOwned := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "vn-1",
				Labels: map[string]string{VirtualNodeLabel: LabelValueTrue},
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "v1", Kind: "Node", Name: "host1", UID: "uid-host1"},
				},
			},
		}
		vnNodeOtherHost := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "vn-2",
				Labels: map[string]string{VirtualNodeLabel: LabelValueTrue},
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "v1", Kind: "Node", Name: "host2", UID: "uid-host2"},
				},
			},
		}
		nodeWithoutLabel := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "vn-3",
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "v1", Kind: "Node", Name: "host1", UID: "uid-host1"},
				},
			},
		}

		cl := buildTestClient(hostNode, vnNodeOwned, vnNodeOtherHost, nodeWithoutLabel)
		rd := testReader{Reader: cl, now: fixedNow}

		state, err := fetchHostState(ctx, rd, rulekit.Key{Name: "host1"}, nil)
		if err != nil {
			t.Fatalf("fetchHostState failed: %v", err)
		}

		if len(state.VirtualNodes) != 1 || state.VirtualNodes[0].Name != "vn-1" {
			names := make([]string, 0, len(state.VirtualNodes))
			for _, vn := range state.VirtualNodes {
				names = append(names, vn.Name)
			}
			t.Errorf("VirtualNodes = %v, want only vn-1", names)
		}
	})

	t.Run("key naming a virtual node", func(t *testing.T) {
		vnNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "vn-1",
				Labels: map[string]string{VirtualNodeLabel: LabelValueTrue},
			},
		}

		cl := buildTestClient(vnNode)
		rd := testReader{Reader: cl, now: fixedNow}

		state, err := fetchHostState(ctx, rd, rulekit.Key{Name: "vn-1"}, nil)
		if err != nil {
			t.Fatalf("fetchHostState failed: %v", err)
		}

		if !state.KeyIsVirtualNode {
			t.Errorf("KeyIsVirtualNode = false, want true")
		}
		if state.Node.Name != "vn-1" {
			t.Errorf("Node.Name = %s, want vn-1", state.Node.Name)
		}
		if len(state.VirtualNodes) > 0 {
			t.Errorf("VirtualNodes = %v, want empty", state.VirtualNodes)
		}
	})

	t.Run("donor pods non-terminal and sorted", func(t *testing.T) {
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1"},
		}

		// pods to test sorting
		t1 := metav1.NewTime(fixedNow.Add(-10 * time.Minute))
		t2 := metav1.NewTime(fixedNow.Add(-5 * time.Minute))

		p2 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "p2", Namespace: "default", Labels: map[string]string{LabelDonor: LabelValueTrue},
				CreationTimestamp: t2,
			},
			Spec: corev1.PodSpec{NodeName: "host1"}, Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		p1a := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "p1a", Namespace: "default", Labels: map[string]string{LabelDonor: LabelValueTrue},
				CreationTimestamp: t1,
			},
			Spec: corev1.PodSpec{NodeName: "host1"}, Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		p1b := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "p1b", Namespace: "other", Labels: map[string]string{LabelDonor: LabelValueTrue},
				CreationTimestamp: t1, // same time as p1a, namespace should break tie (default < other)
			},
			Spec: corev1.PodSpec{NodeName: "host1"}, Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}

		// terminal pods
		pSucc := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "succ", Namespace: "default", Labels: map[string]string{LabelDonor: LabelValueTrue},
				CreationTimestamp: t1,
			},
			Spec: corev1.PodSpec{NodeName: "host1"}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}
		pFail := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "fail", Namespace: "default", Labels: map[string]string{LabelDonor: LabelValueTrue},
				CreationTimestamp: t1,
			},
			Spec: corev1.PodSpec{NodeName: "host1"}, Status: corev1.PodStatus{Phase: corev1.PodFailed},
		}

		cl := buildTestClient(hostNode, p2, p1b, p1a, pSucc, pFail) // pass in mixed order
		rd := testReader{Reader: cl, now: fixedNow}

		state, err := fetchHostState(ctx, rd, rulekit.Key{Name: "host1"}, nil)
		if err != nil {
			t.Fatalf("fetchHostState failed: %v", err)
		}

		gotNames := make([]string, 0, len(state.DonorPods))
		for _, pod := range state.DonorPods {
			gotNames = append(gotNames, pod.Namespace+"/"+pod.Name)
		}
		wantNames := []string{"default/p1a", "other/p1b", "default/p2"}
		if !reflect.DeepEqual(gotNames, wantNames) {
			t.Errorf("DonorPods = %v, want %v", gotNames, wantNames)
		}
	})

	t.Run("fence and mirror pods", func(t *testing.T) {
		fence := corev1.Taint{Key: FenceTaintKey, Value: "vk-host1", Effect: corev1.TaintEffectNoSchedule}
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1"},
			Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: "nvidia.com/gpu", Effect: corev1.TaintEffectNoSchedule}, fence}},
		}
		mirror := func(name, node string, labels map[string]string, phase corev1.PodPhase) *corev1.Pod {
			return &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
				Spec:       corev1.PodSpec{NodeName: node},
				Status:     corev1.PodStatus{Phase: phase},
			}
		}
		running := mirror("running-m", "host1", map[string]string{MirrorNodeLabel: "vk-host1"}, corev1.PodRunning)
		background := mirror("bg-m", "host1", map[string]string{MirrorRoleLabel: "background"}, corev1.PodPending)
		finished := mirror("done-m", "host1", map[string]string{MirrorNodeLabel: "vk-host1"}, corev1.PodSucceeded)
		elsewhere := mirror("other-m", "host2", map[string]string{MirrorNodeLabel: "vk-host2"}, corev1.PodRunning)
		plain := mirror("plain", "host1", map[string]string{"app": "x"}, corev1.PodRunning)

		cl := buildTestClient(hostNode, running, background, finished, elsewhere, plain)
		state, err := fetchHostState(ctx, testReader{Reader: cl, now: fixedNow}, rulekit.Key{Name: "host1"}, nil)
		if err != nil {
			t.Fatalf("fetchHostState failed: %v", err)
		}
		if state.Fence == nil || !reflect.DeepEqual(*state.Fence, fence) {
			t.Errorf("Fence = %+v, want %+v", state.Fence, fence)
		}
		if state.FenceVirtualNodeExists {
			t.Errorf("FenceVirtualNodeExists = true, want false (vk-host1 does not exist)")
		}
		gotNames := make([]string, 0, len(state.MirrorPods))
		for _, pod := range state.MirrorPods {
			gotNames = append(gotNames, pod.Name)
		}
		slices.Sort(gotNames)
		if want := []string{"bg-m", "running-m"}; !reflect.DeepEqual(gotNames, want) {
			t.Errorf("MirrorPods = %v, want %v", gotNames, want)
		}
		if len(state.DonorPods) != 0 {
			t.Errorf("DonorPods = %v, want none (mirrors are not donors)", state.DonorPods)
		}

		vk := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "vk-host1"}}
		withVirtualNode := testReader{Reader: buildTestClient(hostNode, vk), now: fixedNow}
		state, err = fetchHostState(ctx, withVirtualNode, rulekit.Key{Name: "host1"}, nil)
		if err != nil {
			t.Fatalf("fetchHostState failed: %v", err)
		}
		if !state.FenceVirtualNodeExists {
			t.Errorf("FenceVirtualNodeExists = false, want true")
		}
	})

	t.Run("group lock fetched", func(t *testing.T) {
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "host1",
				ManagedFields: []metav1.ManagedFieldsEntry{
					{
						Manager:    DonorControllerFieldManager,
						Operation:  metav1.ManagedFieldsOperationApply,
						APIVersion: "v1",
						FieldsType: "FieldsV1",
						FieldsV1: &metav1.FieldsV1{
							Raw: []byte(`{"f:metadata":{"f:labels":{"f:timeslice.io/group":{}}}}`),
						},
					},
				},
				Labels: map[string]string{
					LabelGroup: "g1",
				},
			},
		}

		cl := buildTestClient(hostNode)
		rd := testReader{Reader: cl, now: fixedNow}

		// nil source -> Idle
		state, err := fetchHostState(ctx, rd, rulekit.Key{Name: "host1"}, nil)
		if err != nil {
			t.Fatalf("fetchHostState: %v", err)
		}
		if state.GroupLock != GroupLockIdle {
			t.Errorf("GroupLock = %v, want Idle with nil source", state.GroupLock)
		}

		// valid source
		ls := &fakeLockSource{
			groupLock: func(g string) (GroupLock, error) {
				return GroupLockActive, nil
			},
		}
		state, err = fetchHostState(ctx, rd, rulekit.Key{Name: "host1"}, ls)
		if err != nil {
			t.Fatalf("fetchHostState: %v", err)
		}
		if state.GroupLock != GroupLockActive {
			t.Errorf("GroupLock = %v, want Active", state.GroupLock)
		}
		if len(ls.called) != 1 || ls.called[0] != "g1" {
			t.Errorf("Lock source called with %v, want [g1]", ls.called)
		}

		// error source
		lsError := &fakeLockSource{
			groupLock: func(g string) (GroupLock, error) {
				return GroupLockIdle, fmt.Errorf("fake error")
			},
		}
		state, err = fetchHostState(ctx, rd, rulekit.Key{Name: "host1"}, lsError)
		if err != nil {
			t.Fatalf("fetchHostState: %v", err)
		}
		if state.GroupLock != GroupLockUnknown {
			t.Errorf("GroupLock = %v, want Unknown on error", state.GroupLock)
		}
		if state.GroupLockError != "fake error" {
			t.Errorf("GroupLockError = %q, want 'fake error'", state.GroupLockError)
		}

		// no owned group -> not called
		noGroupNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host2"},
		}
		rdNoGroup := testReader{Reader: buildTestClient(noGroupNode), now: fixedNow}
		lsNotCalled := &fakeLockSource{}
		state, err = fetchHostState(ctx, rdNoGroup, rulekit.Key{Name: "host2"}, lsNotCalled)
		if err != nil {
			t.Fatalf("fetchHostState: %v", err)
		}
		if state.GroupLock != GroupLockIdle {
			t.Errorf("GroupLock = %v, want Idle without owned group", state.GroupLock)
		}
		if len(lsNotCalled.called) > 0 {
			t.Errorf("Lock source called when it shouldn't have been: %v", lsNotCalled.called)
		}
	})
}

func TestHostKeysForNode(t *testing.T) {
	vnNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "vn-1",
			Labels: map[string]string{VirtualNodeLabel: LabelValueTrue},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Node", Name: "host1", UID: "uid-host1"},
			},
		},
	}
	keys := hostKeysForNode(vnNode)
	if len(keys) != 1 || keys[0].Name != "host1" {
		t.Errorf("Keys for virtual node = %v, want [{host1}]", keys)
	}

	vnNoOwner := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "vn-2",
			Labels: map[string]string{VirtualNodeLabel: LabelValueTrue},
		},
	}
	keys = hostKeysForNode(vnNoOwner)
	if len(keys) != 0 {
		t.Errorf("Keys for ownerless virtual node = %v, want []", keys)
	}

	normalNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "host1"},
	}
	keys = hostKeysForNode(normalNode)
	if len(keys) != 1 || keys[0].Name != "host1" {
		t.Errorf("Keys for normal node = %v, want [{host1}]", keys)
	}
}

func TestTrimPod(t *testing.T) {
	now := metav1.Now()
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "p1",
			Namespace:         "default",
			UID:               "uid-p1",
			Labels:            map[string]string{"foo": "bar"},
			CreationTimestamp: now,
			ResourceVersion:   "123",
		},
		Spec: corev1.PodSpec{
			NodeName:   "host1",
			Containers: []corev1.Container{{Name: "c"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "1.2.3.4",
		},
	}

	trimmedObj, err := TrimPod(pod)
	if err != nil {
		t.Fatalf("TrimPod: %v", err)
	}
	trimmed, ok := trimmedObj.(*corev1.Pod)
	if !ok {
		t.Fatalf("TrimPod returned %T, want *corev1.Pod", trimmedObj)
	}

	if trimmed.CreationTimestamp != now {
		t.Errorf("CreationTimestamp = %v, want %v", trimmed.CreationTimestamp, now)
	}
	if len(trimmed.Spec.Containers) != 0 {
		t.Errorf("TrimPod kept containers")
	}
	if trimmed.Status.PodIP != "" {
		t.Errorf("TrimPod kept PodIP")
	}
	if trimmed.Spec.NodeName != "host1" {
		t.Errorf("NodeName = %q, want host1", trimmed.Spec.NodeName)
	}
	if trimmed.Status.Phase != corev1.PodRunning {
		t.Errorf("Phase = %v, want %v", trimmed.Status.Phase, corev1.PodRunning)
	}
}

func TestSharedLabelsMatchOtherComponents(t *testing.T) {
	if LabelGroup != infrastructure.PodLabelKey {
		t.Errorf("LabelGroup (%q) != infrastructure.PodLabelKey (%q)",
			LabelGroup, infrastructure.PodLabelKey)
	}
	if LabelDonor != "timeslice.io/donor" {
		t.Errorf("LabelDonor (%q) != \"timeslice.io/donor\" (%q)",
			LabelDonor, "timeslice.io/donor")
	}
	if AnnotationIdleSince != "timeslice.io/idle-since" {
		t.Errorf("AnnotationIdleSince (%q) != \"timeslice.io/idle-since\" (%q)",
			AnnotationIdleSince, "timeslice.io/idle-since")
	}
}
