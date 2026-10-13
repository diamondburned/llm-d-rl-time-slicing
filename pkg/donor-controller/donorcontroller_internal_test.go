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

package donorcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type testNodeWithOwned struct {
	node  *corev1.Node
	owned map[string]string
}

func node(name string, ownedLabels ...string) testNodeWithOwned {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{},
		},
	}
	owned := map[string]string{}
	for _, l := range ownedLabels {
		parts := strings.SplitN(l, "=", 2)
		k, v := parts[0], LabelValueTrue
		if len(parts) == 2 {
			v = parts[1]
		}
		n.Labels[k] = v
		owned[k] = v
	}
	return testNodeWithOwned{node: n, owned: owned}
}

func nodeWithGroup(name, group string, ownedLabels ...string) testNodeWithOwned {
	res := node(name, ownedLabels...)
	res.node.Labels[LabelGroup] = group
	return res
}

func pod(name, group string) *corev1.Pod {
	labels := map[string]string{
		LabelDonor: LabelValueTrue,
	}
	if group != "" {
		labels[LabelGroup] = group
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			NodeName: "host1",
		},
	}
}

func virtualNode(host string, uid types.UID) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vk-" + string(uid),
			UID:  uid,
			Labels: map[string]string{
				VirtualNodeLabel: LabelValueTrue,
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Node", Name: host, UID: uid},
			},
		},
	}
}

// fencedHost returns an unshared host1 carrying the GPU fence of vk-host1,
// with staleSince as its owned stale-fence-since annotation (none if empty).
func fencedHost(now time.Time, staleSince string, virtualNodeExists bool) *hostState {
	res := node("host1")
	n, owned := res.node, res.owned
	n.Spec.Taints = []corev1.Taint{
		{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule},
		{Key: FenceTaintKey, Value: "vk-host1", Effect: corev1.TaintEffectNoSchedule},
	}
	h := &hostState{
		Host:                   "host1",
		Now:                    now,
		Node:                   n,
		OwnedLabels:            owned,
		Fence:                  &n.Spec.Taints[1],
		FenceVirtualNodeExists: virtualNodeExists,
	}
	if staleSince != "" {
		h.OwnedAnnotations = map[string]string{AnnotationStaleFenceSince: staleSince}
	}
	return h
}

// mirrorPod returns a guest kubelet mirror pod of virtualNode, bound to host.
func mirrorPod(name, host, virtualNode string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{MirrorNodeLabel: virtualNode},
		},
		Spec: corev1.PodSpec{NodeName: host},
	}
}

type wantDelete struct {
	Name string
	UID  types.UID
}

type wantWarning struct {
	ObjName string
	Reason  string
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func ptrTo[T any](v T) *T { return &v }

func TestRulesTable(t *testing.T) {
	fixedNow := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	defaultTTL := 5 * time.Minute
	defaultCfg := Config{IdleGracePeriod: defaultTTL}

	isoTaint := &corev1.Taint{
		Key:    "timeslice.io/shared",
		Value:  "true",
		Effect: corev1.TaintEffectNoSchedule,
	}
	cfgWithTaint := Config{
		IdleGracePeriod: defaultTTL,
		IsolationTaint:  isoTaint,
	}
	cfgClearFences := Config{
		IdleGracePeriod:       defaultTTL,
		ClearStaleFences:      true,
		StaleFenceGracePeriod: time.Minute,
	}
	fenceTaintOnHost1 := corev1.Taint{Key: FenceTaintKey, Value: "vk-host1", Effect: corev1.TaintEffectNoSchedule}

	tests := []struct {
		name                 string
		cfg                  *Config
		data                 func() *hostState
		wantFired            []string
		wantLabels           map[string]string
		wantAnnotations      map[string]string
		wantTaint            *corev1.Taint
		wantDeleteVN         *wantDelete
		wantPreconditions    []rulekit.Precondition
		wantFinalizerRemoval *string
		wantTaintRemoval     *corev1.Taint
		wantWakeAt           *time.Time
		wantWarnings         []wantWarning
	}{
		{
			name: "unshared node, no donors -> fired none",
			data: func() *hostState {
				res := node("host1")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
				}
			},
			wantFired: nil,
		},
		{
			name: "donor pod bound (group g) -> share with {donor, group=g}",
			data: func() *hostState {
				res := node("host1")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					DonorPods:   []*corev1.Pod{pod("dp1", "g")},
				}
			},
			wantFired: []string{"share node while donor pods are bound"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
		},
		{
			name: "two donors g (older) and h on unlabeled host -> share as g + reject fires",
			data: func() *hostState {
				res := node("host1")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					DonorPods:   []*corev1.Pod{pod("dp1", "g"), pod("dp2", "h")},
				}
			},
			wantFired: []string{"share node while donor pods are bound", "reject donor pods outside the node's group"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantWarnings: []wantWarning{
				{ObjName: "dp2", Reason: ReasonGroupConflict},
				{ObjName: "host1", Reason: ReasonGroupConflict},
			},
		},
		{
			name: "host already labeled group=h by someone else + donor g -> no share, GroupConflict",
			data: func() *hostState {
				res := nodeWithGroup("host1", "h")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					DonorPods:   []*corev1.Pod{pod("dp1", "g")},
				}
			},
			wantFired:  []string{"reject donor pods outside the node's group"},
			wantLabels: map[string]string{}, // none applied
			wantWarnings: []wantWarning{
				{ObjName: "dp1", Reason: ReasonGroupConflict},
				{ObjName: "host1", Reason: ReasonGroupConflict},
			},
		},
		{
			name: "donor without group / invalid group -> InvalidGroup warning, no share",
			data: func() *hostState {
				res := node("host1")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					DonorPods:   []*corev1.Pod{pod("dp1", "invalid!")},
				}
			},
			wantFired:  []string{"reject donor pods outside the node's group"},
			wantLabels: map[string]string{},
			wantWarnings: []wantWarning{
				{ObjName: "dp1", Reason: ReasonInvalidGroup},
				{ObjName: "host1", Reason: ReasonInvalidGroup},
			},
		},
		{
			name: "shared owned g + only h donors -> idle clock for g and reject warnings",
			data: func() *hostState {
				res := node("host1", LabelDonor, LabelGroup+"=g")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					DonorPods:   []*corev1.Pod{pod("dp1", "h")},
					GroupLock:   GroupLockIdle,
				}
			},
			wantFired: []string{"reject donor pods outside the node's group", "keep node shared during idle grace period"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantAnnotations: map[string]string{
				AnnotationIdleSince: fixedNow.Format(time.RFC3339),
			},
			wantWakeAt: ptrTo(fixedNow.Add(defaultTTL)),
			wantWarnings: []wantWarning{
				{ObjName: "dp1", Reason: ReasonGroupConflict},
				{ObjName: "host1", Reason: ReasonGroupConflict},
			},
		},
		{
			name: "shared idle no annotation (no lock source) -> keep during grace, wake at now+grace",
			data: func() *hostState {
				res := node("host1", LabelDonor, LabelGroup+"=g")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					GroupLock:   GroupLockIdle,
				}
			},
			wantFired: []string{"keep node shared during idle grace period"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantAnnotations: map[string]string{
				AnnotationIdleSince: fixedNow.Format(time.RFC3339),
			},
			wantWakeAt: ptrTo(fixedNow.Add(defaultTTL)),
		},
		{
			name: "shared idle annotation now-2m -> wake in 3m unchanged",
			data: func() *hostState {
				twoMinsAgo := fixedNow.Add(-2 * time.Minute)
				res := node("host1", LabelDonor, LabelGroup+"=g", AnnotationIdleSince+"="+twoMinsAgo.Format(time.RFC3339))
				return &hostState{
					Host:             "host1",
					Now:              fixedNow,
					Node:             res.node,
					OwnedLabels:      res.owned,
					OwnedAnnotations: map[string]string{AnnotationIdleSince: twoMinsAgo.Format(time.RFC3339)},
					GroupLock:        GroupLockIdle,
				}
			},
			wantFired: []string{"keep node shared during idle grace period"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantAnnotations: map[string]string{
				AnnotationIdleSince: fixedNow.Add(-2 * time.Minute).Format(time.RFC3339),
			},
			wantWakeAt: ptrTo(fixedNow.Add(-2 * time.Minute).Add(defaultTTL)),
		},
		{
			name: "elapsed -> unshare with precondition",
			data: func() *hostState {
				sixMinsAgo := fixedNow.Add(-6 * time.Minute)
				res := node("host1", LabelDonor, LabelGroup+"=g", AnnotationIdleSince+"="+sixMinsAgo.Format(time.RFC3339))
				return &hostState{
					Host:             "host1",
					Now:              fixedNow,
					Node:             res.node,
					OwnedLabels:      res.owned,
					OwnedAnnotations: map[string]string{AnnotationIdleSince: sixMinsAgo.Format(time.RFC3339)},
					GroupLock:        GroupLockIdle,
				}
			},
			wantFired: []string{"unshare node after idle grace period"},
			wantPreconditions: []rulekit.Precondition{
				noDonorPodsOfGroupBoundToNode("host1", "g"),
			},
			wantLabels:      nil,
			wantAnnotations: nil,
		},
		{
			name: "grace 0 -> unshare",
			cfg:  &Config{IdleGracePeriod: 0},
			data: func() *hostState {
				res := node("host1", LabelDonor, LabelGroup+"=g")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					GroupLock:   GroupLockIdle,
				}
			},
			wantFired: []string{"unshare node after idle grace period"},
			wantPreconditions: []rulekit.Precondition{
				noDonorPodsOfGroupBoundToNode("host1", "g"),
			},
		},
		{
			name: "isolation taint present while shared",
			cfg:  &cfgWithTaint,
			data: func() *hostState {
				res := node("host1")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					DonorPods:   []*corev1.Pod{pod("dp1", "g")},
				}
			},
			wantFired: []string{"share node while donor pods are bound"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantTaint: isoTaint,
		},
		{
			name: "isolation taint absent after unshare",
			cfg:  &cfgWithTaint,
			data: func() *hostState {
				sixMinsAgo := fixedNow.Add(-6 * time.Minute)
				res := node("host1", LabelDonor, LabelGroup+"=g", AnnotationIdleSince+"="+sixMinsAgo.Format(time.RFC3339))
				return &hostState{
					Host:             "host1",
					Now:              fixedNow,
					Node:             res.node,
					OwnedLabels:      res.owned,
					OwnedAnnotations: map[string]string{AnnotationIdleSince: sixMinsAgo.Format(time.RFC3339)},
					GroupLock:        GroupLockIdle,
				}
			},
			wantFired: []string{"unshare node after idle grace period"},
			wantPreconditions: []rulekit.Precondition{
				noDonorPodsOfGroupBoundToNode("host1", "g"),
			},
			wantLabels: nil,
			wantTaint:  nil,
		},
		{
			name: "donor returns during clock -> share and annotation released",
			data: func() *hostState {
				twoMinsAgo := fixedNow.Add(-2 * time.Minute)
				res := node("host1", LabelDonor, LabelGroup+"=g", AnnotationIdleSince+"="+twoMinsAgo.Format(time.RFC3339))
				return &hostState{
					Host:             "host1",
					Now:              fixedNow,
					Node:             res.node,
					OwnedLabels:      res.owned,
					OwnedAnnotations: map[string]string{AnnotationIdleSince: twoMinsAgo.Format(time.RFC3339)},
					DonorPods:        []*corev1.Pod{pod("dp1", "g")},
				}
			},
			wantFired: []string{"share node while donor pods are bound"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantAnnotations: nil,
		},
		{
			name: "lock Active -> keep-while-lock rule, no annotation, wake at now+poll",
			cfg:  &Config{IdleGracePeriod: defaultTTL, GroupLocks: &fakeLockSource{}, GroupLockPollInterval: time.Minute},
			data: func() *hostState {
				res := node("host1", LabelDonor, LabelGroup+"=g")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					GroupLock:   GroupLockActive,
				}
			},
			wantFired: []string{"keep node shared while group lock is active or unknown"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantWakeAt: ptrTo(fixedNow.Add(time.Minute)),
		},
		{
			name: "lock Unknown with owned annotation -> annotation kept unchanged, warning, wake now+poll",
			cfg:  &Config{IdleGracePeriod: defaultTTL, GroupLocks: &fakeLockSource{}, GroupLockPollInterval: time.Minute},
			data: func() *hostState {
				twoMinsAgo := fixedNow.Add(-2 * time.Minute)
				res := node("host1", LabelDonor, LabelGroup+"=g", AnnotationIdleSince+"="+twoMinsAgo.Format(time.RFC3339))
				return &hostState{
					Host:             "host1",
					Now:              fixedNow,
					Node:             res.node,
					OwnedLabels:      res.owned,
					OwnedAnnotations: map[string]string{AnnotationIdleSince: twoMinsAgo.Format(time.RFC3339)},
					GroupLock:        GroupLockUnknown,
					GroupLockError:   "fake err",
				}
			},
			wantFired: []string{"keep node shared while group lock is active or unknown"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantAnnotations: map[string]string{
				AnnotationIdleSince: fixedNow.Add(-2 * time.Minute).Format(time.RFC3339),
			},
			wantWarnings: []wantWarning{
				{ObjName: "host1", Reason: ReasonGroupLockUnknown},
			},
			wantWakeAt: ptrTo(fixedNow.Add(time.Minute)),
		},
		{
			name: "lock Unknown without annotation -> no annotation",
			cfg:  &Config{IdleGracePeriod: defaultTTL, GroupLocks: &fakeLockSource{}, GroupLockPollInterval: time.Minute},
			data: func() *hostState {
				res := node("host1", LabelDonor, LabelGroup+"=g")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					GroupLock:   GroupLockUnknown,
				}
			},
			wantFired: []string{"keep node shared while group lock is active or unknown"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantWarnings: []wantWarning{
				{ObjName: "host1", Reason: ReasonGroupLockUnknown},
			},
			wantWakeAt: ptrTo(fixedNow.Add(time.Minute)),
		},
		{
			name: "lock Idle + source configured -> grace rule wake = min(expiry, now+poll)",
			cfg:  &Config{IdleGracePeriod: defaultTTL, GroupLocks: &fakeLockSource{}, GroupLockPollInterval: time.Minute},
			data: func() *hostState {
				res := node("host1", LabelDonor, LabelGroup+"=g")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					GroupLock:   GroupLockIdle,
				}
			},
			wantFired: []string{"keep node shared during idle grace period"},
			wantLabels: map[string]string{
				LabelDonor: LabelValueTrue,
				LabelGroup: "g",
			},
			wantAnnotations: map[string]string{
				AnnotationIdleSince: fixedNow.Format(time.RFC3339),
			},
			wantWakeAt: ptrTo(fixedNow.Add(time.Minute)), // The effect framework usually returns min Wake, we can check logic
		},
		{
			name: "unshare with source -> two preconditions",
			cfg:  &Config{IdleGracePeriod: 0, GroupLocks: &fakeLockSource{}},
			data: func() *hostState {
				res := node("host1", LabelDonor, LabelGroup+"=g")
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: res.owned,
					GroupLock:   GroupLockIdle,
				}
			},
			wantFired: []string{"unshare node after idle grace period"},
			wantPreconditions: []rulekit.Precondition{
				noDonorPodsOfGroupBoundToNode("host1", "g"),
				groupLockIsIdle(&fakeLockSource{}, "g"), // dummy precondition
			},
		},
		{
			name: "host deleted + virtual node owned by it -> release rule",
			data: func() *hostState {
				vn := virtualNode("host1", "uid-host1")
				return &hostState{
					Host:         "host1",
					Now:          fixedNow,
					VirtualNodes: []*corev1.Node{vn}, // Node is nil (deleted)
				}
			},
			wantFired:    []string{"release virtual nodes of dead host"},
			wantDeleteVN: &wantDelete{Name: "vk-uid-host1", UID: "uid-host1"},
			wantPreconditions: []rulekit.Precondition{
				nodeIsGoneOrRecreated("host1", "uid-host1"),
			},
			wantFinalizerRemoval: ptrTo(VirtualNodeFinalizer),
		},
		{
			name: "host recreated (Node exists with UID different from ownerRef UID) -> release fires",
			data: func() *hostState {
				res := node("host1")
				res.node.UID = "uid-host1-new"
				vn := virtualNode("host1", "uid-host1-old")
				return &hostState{
					Host:         "host1",
					Now:          fixedNow,
					Node:         res.node,
					OwnedLabels:  res.owned,
					VirtualNodes: []*corev1.Node{vn},
				}
			},
			wantFired:    []string{"release virtual nodes of dead host"},
			wantDeleteVN: &wantDelete{Name: "vk-uid-host1-old", UID: "uid-host1-old"},
			wantPreconditions: []rulekit.Precondition{
				nodeIsGoneOrRecreated("host1", "uid-host1-old"),
			},
			wantFinalizerRemoval: ptrTo(VirtualNodeFinalizer),
		},
		{
			name: "host exists with matching UID -> no release",
			data: func() *hostState {
				res := node("host1")
				res.node.UID = "uid-host1"
				vn := virtualNode("host1", "uid-host1")
				return &hostState{
					Host:         "host1",
					Now:          fixedNow,
					Node:         res.node,
					OwnedLabels:  res.owned,
					VirtualNodes: []*corev1.Node{vn},
				}
			},
			wantFired: nil,
		},
		{
			name: "KeyIsVirtualNode -> nothing",
			data: func() *hostState {
				res := node("vn-1")
				return &hostState{
					Host:             "vn-1",
					Now:              fixedNow,
					Node:             res.node,
					KeyIsVirtualNode: true,
					OwnedLabels:      res.owned,
				}
			},
			wantFired: nil,
		},
		{
			name: "human-applied donor label not owned + no donors -> nothing",
			data: func() *hostState {
				res := node("host1")
				res.node.Labels[LabelDonor] = LabelValueTrue
				// Not owned!
				return &hostState{
					Host:        "host1",
					Now:         fixedNow,
					Node:        res.node,
					OwnedLabels: map[string]string{}, // empty
				}
			},
			wantFired: nil,
		},
		{
			name: "stale fence, clearing disabled -> nothing",
			data: func() *hostState { return fencedHost(fixedNow, "", false) },
		},
		{
			name: "fence whose virtual node exists -> nothing",
			cfg:  &cfgClearFences,
			data: func() *hostState { return fencedHost(fixedNow, "", true) },
		},
		{
			name: "fence with a mirror pod left -> nothing",
			cfg:  &cfgClearFences,
			data: func() *hostState {
				h := fencedHost(fixedNow, "", false)
				h.MirrorPods = []*corev1.Pod{mirrorPod("guest-m", "host1", "vk-host1")}
				return h
			},
		},
		{
			name:            "stale fence, no annotation -> start clock, wake at now+grace",
			cfg:             &cfgClearFences,
			data:            func() *hostState { return fencedHost(fixedNow, "", false) },
			wantFired:       []string{"wait out stale gpu fence"},
			wantAnnotations: map[string]string{AnnotationStaleFenceSince: fixedNow.Format(time.RFC3339)},
			wantWakeAt:      ptrTo(fixedNow.Add(time.Minute)),
		},
		{
			name: "stale fence since now-30s -> clock unchanged, wake in 30s",
			cfg:  &cfgClearFences,
			data: func() *hostState {
				return fencedHost(fixedNow, fixedNow.Add(-30*time.Second).Format(time.RFC3339), false)
			},
			wantFired:       []string{"wait out stale gpu fence"},
			wantAnnotations: map[string]string{AnnotationStaleFenceSince: fixedNow.Add(-30 * time.Second).Format(time.RFC3339)},
			wantWakeAt:      ptrTo(fixedNow.Add(30 * time.Second)),
		},
		{
			name:             "stale fence grace elapsed -> remove fence (live checks), release clock",
			cfg:              &cfgClearFences,
			data:             func() *hostState { return fencedHost(fixedNow, fixedNow.Add(-time.Minute).Format(time.RFC3339), false) },
			wantFired:        []string{"remove stale gpu fence"},
			wantTaintRemoval: &fenceTaintOnHost1,
			wantPreconditions: []rulekit.Precondition{
				virtualNodeIsGone("vk-host1"),
				noMirrorPodsBoundToNode("host1"),
			},
		},
		{
			name: "virtual node came back during the clock -> clock released",
			cfg:  &cfgClearFences,
			data: func() *hostState {
				return fencedHost(fixedNow, fixedNow.Add(-30*time.Second).Format(time.RFC3339), true)
			},
		},
		{
			name: "stale fence on a shared host with donors -> share and clock side by side",
			cfg:  &cfgClearFences,
			data: func() *hostState {
				h := fencedHost(fixedNow, "", false)
				h.DonorPods = []*corev1.Pod{pod("dp1", "g")}
				return h
			},
			wantFired:       []string{"share node while donor pods are bound", "wait out stale gpu fence"},
			wantLabels:      map[string]string{LabelDonor: LabelValueTrue, LabelGroup: "g"},
			wantAnnotations: map[string]string{AnnotationStaleFenceSince: fixedNow.Format(time.RFC3339)},
			wantWakeAt:      ptrTo(fixedNow.Add(time.Minute)),
		},
		{
			name: "fence on a virtual-node key -> nothing",
			cfg:  &cfgClearFences,
			data: func() *hostState {
				h := fencedHost(fixedNow, "", false)
				h.KeyIsVirtualNode = true
				return h
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultCfg
			if tc.cfg != nil {
				cfg = *tc.cfg
			}
			state := tc.data()
			ctrl := newController(cfg)
			plan, err := ctrl.Evaluate(rulekit.Key{Name: state.Host}, state)
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			fired := plan.Fired
			allApplies := plan.Objects
			allTaints := plan.Taints
			allDeletes := plan.Deletes
			allPreconditions := plan.Preconditions
			allFinalizerRemovals := plan.Finalizers
			allTaintRemovals := plan.TaintRemovals
			allWarnings := plan.Warnings
			wake := plan.Wake

			// Fired rules assertion
			if !slices.Equal(fired, tc.wantFired) && (len(fired) != 0 || len(tc.wantFired) != 0) {
				t.Errorf("Fired = %v, want %v", fired, tc.wantFired)
			}

			// Extract labels and annotations from allApplies
			var gotLabels, gotAnnotations map[string]string
			for _, app := range allApplies {
				data, err := json.Marshal(app.Desired)
				if err != nil {
					t.Fatalf("marshal apply config: %v", err)
				}
				var n corev1.Node
				if err := json.Unmarshal(data, &n); err != nil {
					t.Fatalf("unmarshal into node: %v", err)
				}
				if len(n.Labels) > 0 {
					if gotLabels == nil {
						gotLabels = map[string]string{}
					}
					for k, v := range n.Labels {
						gotLabels[k] = v
					}
				}
				if len(n.Annotations) > 0 {
					if gotAnnotations == nil {
						gotAnnotations = map[string]string{}
					}
					for k, v := range n.Annotations {
						gotAnnotations[k] = v
					}
				}
			}

			if !mapsEqual(gotLabels, tc.wantLabels) {
				t.Errorf("Labels = %v, want %v", gotLabels, tc.wantLabels)
			}
			if !mapsEqual(gotAnnotations, tc.wantAnnotations) {
				t.Errorf("Annotations = %v, want %v", gotAnnotations, tc.wantAnnotations)
			}

			// Taints
			var presentTaints []rulekit.PlannedTaint
			for _, tnt := range allTaints {
				if tnt.Present {
					presentTaints = append(presentTaints, tnt)
				}
			}
			if tc.wantTaint != nil {
				if len(presentTaints) != 1 {
					t.Errorf("Taints count = %d, want 1", len(presentTaints))
				} else if presentTaints[0].Node != state.Host || !reflect.DeepEqual(presentTaints[0].Taint, tc.wantTaint) {
					t.Errorf("Taint = %+v on %s, want %+v on %s", presentTaints[0].Taint, presentTaints[0].Node, tc.wantTaint, state.Host)
				}
			} else {
				if len(presentTaints) != 0 {
					t.Errorf("Taints = %+v, want none", presentTaints)
				}
			}

			// Deletes
			func() {
				if tc.wantDeleteVN != nil {
					if len(allDeletes) != 1 {
						t.Errorf("Deletes count = %d, want 1", len(allDeletes))
						return
					}
					delNode, ok := allDeletes[0].Value.(*corev1.Node)
					if !ok || delNode.Name != tc.wantDeleteVN.Name || delNode.UID != tc.wantDeleteVN.UID {
						t.Errorf("Deleted obj = %v, want %s (UID %s)", allDeletes[0].Value, tc.wantDeleteVN.Name, tc.wantDeleteVN.UID)
					}
				} else if len(allDeletes) != 0 {
					t.Errorf("Deletes = %+v, want none", allDeletes)
				}
			}()

			// Finalizers
			if tc.wantFinalizerRemoval != nil {
				if len(allFinalizerRemovals) != 1 {
					t.Errorf("FinalizerRemovals count = %d, want 1", len(allFinalizerRemovals))
				} else if allFinalizerRemovals[0].Value.Finalizer != *tc.wantFinalizerRemoval {
					t.Errorf("FinalizerRemovals[0].Finalizer = %q, want %q", allFinalizerRemovals[0].Value.Finalizer, *tc.wantFinalizerRemoval)
				}
			} else {
				if len(allFinalizerRemovals) != 0 {
					t.Errorf("FinalizerRemovals = %+v, want none", allFinalizerRemovals)
				}
			}

			// Taint removals
			if tc.wantTaintRemoval != nil {
				switch {
				case len(allTaintRemovals) != 1:
					t.Errorf("TaintRemovals count = %d, want 1", len(allTaintRemovals))
				case allTaintRemovals[0].Value.Node.Name != state.Host ||
					!reflect.DeepEqual(allTaintRemovals[0].Value.Taint, *tc.wantTaintRemoval):
					got := allTaintRemovals[0].Value
					t.Errorf("TaintRemovals[0] = %+v on %s, want %+v on %s", got.Taint, got.Node.Name, *tc.wantTaintRemoval, state.Host)
				}
			} else if len(allTaintRemovals) != 0 {
				t.Errorf("TaintRemovals = %+v, want none", allTaintRemovals)
			}

			// Warnings
			gotWarnings := make([]wantWarning, 0, len(allWarnings))
			for _, w := range allWarnings {
				gotWarnings = append(gotWarnings, wantWarning{ObjName: w.Value.Object.GetName(), Reason: w.Value.Reason})
			}
			if !reflect.DeepEqual(gotWarnings, tc.wantWarnings) && (len(gotWarnings) != 0 || len(tc.wantWarnings) != 0) {
				t.Errorf("Warnings = %v, want %v", gotWarnings, tc.wantWarnings)
			}

			// Preconditions
			gotPreconditions := make([]string, 0, len(allPreconditions))
			for _, p := range allPreconditions {
				gotPreconditions = append(gotPreconditions, p.Value.Description)
			}
			wantPreconditions := make([]string, 0, len(tc.wantPreconditions))
			for _, p := range tc.wantPreconditions {
				wantPreconditions = append(wantPreconditions, p.Description)
			}
			if !reflect.DeepEqual(gotPreconditions, wantPreconditions) && (len(gotPreconditions) != 0 || len(wantPreconditions) != 0) {
				t.Errorf("Preconditions = %v, want %v", gotPreconditions, wantPreconditions)
			}

			// Wake
			if tc.wantWakeAt != nil {
				if wake == nil {
					t.Errorf("Wake = nil, want %v", *tc.wantWakeAt)
				} else if !wake.Value.Equal(*tc.wantWakeAt) {
					t.Errorf("Wake = %v, want %v", wake.Value, *tc.wantWakeAt)
				}
			} else {
				if wake != nil {
					t.Errorf("Wake = %v, want nil", wake)
				}
			}
		})
	}
}

func buildPreconditionsTestClient(objs ...client.Object) client.Reader {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, indexPodNodeName, func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		Build()
}

func TestNoDonorPodsOfGroupBoundToNode(t *testing.T) {
	ctx := context.Background()

	t.Run("no pods on node -> holds", func(t *testing.T) {
		cl := buildPreconditionsTestClient()
		p := noDonorPodsOfGroupBoundToNode("host1", "g")
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("running donor pod of same group on node -> fails", func(t *testing.T) {
		runningDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{LabelDonor: LabelValueTrue, LabelGroup: "g"},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		cl := buildPreconditionsTestClient(runningDonor)
		p := noDonorPodsOfGroupBoundToNode("host1", "g")
		err := p.Check(ctx, cl)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, rulekit.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
		}
	})

	t.Run("running donor pod of different group on node -> holds", func(t *testing.T) {
		otherGroupDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{LabelDonor: LabelValueTrue, LabelGroup: "h"},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		cl := buildPreconditionsTestClient(otherGroupDonor)
		p := noDonorPodsOfGroupBoundToNode("host1", "g")
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("succeeded donor pod of same group on node -> holds", func(t *testing.T) {
		succeededDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{LabelDonor: LabelValueTrue, LabelGroup: "g"},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}
		cl := buildPreconditionsTestClient(succeededDonor)
		p := noDonorPodsOfGroupBoundToNode("host1", "g")
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("failed donor pod of same group on node -> holds", func(t *testing.T) {
		failedDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{LabelDonor: LabelValueTrue, LabelGroup: "g"},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		}
		cl := buildPreconditionsTestClient(failedDonor)
		p := noDonorPodsOfGroupBoundToNode("host1", "g")
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("running donor pod of same group on different node -> holds", func(t *testing.T) {
		otherNodePod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "p1",
				Namespace: "default",
				Labels:    map[string]string{LabelDonor: LabelValueTrue, LabelGroup: "g"},
			},
			Spec:   corev1.PodSpec{NodeName: "host2"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		cl := buildPreconditionsTestClient(otherNodePod)
		p := noDonorPodsOfGroupBoundToNode("host1", "g")
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestNodeIsGoneOrRecreated(t *testing.T) {
	ctx := context.Background()

	t.Run("node absent -> holds", func(t *testing.T) {
		cl := buildPreconditionsTestClient()
		p := nodeIsGoneOrRecreated("host1", "uid-old")
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("node exists with different uid -> holds", func(t *testing.T) {
		n := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1", UID: "uid-new"},
		}
		cl := buildPreconditionsTestClient(n)
		p := nodeIsGoneOrRecreated("host1", "uid-old")
		if err := p.Check(ctx, cl); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("node exists with same uid -> fails", func(t *testing.T) {
		n := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1", UID: "uid-same"},
		}
		cl := buildPreconditionsTestClient(n)
		p := nodeIsGoneOrRecreated("host1", "uid-same")
		err := p.Check(ctx, cl)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, rulekit.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
		}
	})
}

func TestGroupLockIsIdle(t *testing.T) {
	ctx := context.Background()

	t.Run("idle -> nil", func(t *testing.T) {
		ls := &fakeLockSource{groupLock: func(g string) (GroupLock, error) { return GroupLockIdle, nil }}
		p := groupLockIsIdle(ls, "g")
		if err := p.Check(ctx, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("active -> ErrPreconditionFailed", func(t *testing.T) {
		ls := &fakeLockSource{groupLock: func(g string) (GroupLock, error) { return GroupLockActive, nil }}
		p := groupLockIsIdle(ls, "g")
		err := p.Check(ctx, nil)
		if !errors.Is(err, rulekit.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
		}
	})

	t.Run("error -> ErrPreconditionFailed wrapping", func(t *testing.T) {
		ls := &fakeLockSource{groupLock: func(g string) (GroupLock, error) { return GroupLockIdle, errors.New("fake error") }}
		p := groupLockIsIdle(ls, "g")
		err := p.Check(ctx, nil)
		if !errors.Is(err, rulekit.ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
		}
		if !strings.Contains(err.Error(), "fake error") {
			t.Errorf("error %v does not contain 'fake error'", err)
		}
	})
}

func TestVirtualNodeIsGone(t *testing.T) {
	ctx := context.Background()

	t.Run("node absent -> holds", func(t *testing.T) {
		if err := virtualNodeIsGone("vk-host1").Check(ctx, buildPreconditionsTestClient()); err != nil {
			t.Errorf("Check() = %v, want nil", err)
		}
	})
	t.Run("node present -> fails", func(t *testing.T) {
		vk := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "vk-host1"}}
		err := virtualNodeIsGone("vk-host1").Check(ctx, buildPreconditionsTestClient(vk))
		if !errors.Is(err, rulekit.ErrPreconditionFailed) {
			t.Errorf("Check() = %v, want ErrPreconditionFailed", err)
		}
	})
}

func TestNoMirrorPodsBoundToNode(t *testing.T) {
	ctx := context.Background()
	withPhase := func(p *corev1.Pod, phase corev1.PodPhase) *corev1.Pod {
		p.Status.Phase = phase
		return p
	}
	roleOnly := mirrorPod("bg-m", "host1", "")
	roleOnly.Labels = map[string]string{MirrorRoleLabel: "background"}

	tests := []struct {
		name     string
		pods     []client.Object
		wantFail bool
	}{
		{name: "no pods -> holds"},
		{name: "donor pod only -> holds", pods: []client.Object{pod("dp1", "g")}},
		{
			name:     "running mirror (mirror-node label) -> fails",
			pods:     []client.Object{mirrorPod("m", "host1", "vk-host1")},
			wantFail: true,
		},
		{name: "running mirror (role label only) -> fails", pods: []client.Object{roleOnly}, wantFail: true},
		{name: "succeeded mirror -> holds", pods: []client.Object{withPhase(mirrorPod("m", "host1", "vk-host1"), corev1.PodSucceeded)}},
		{name: "failed mirror -> holds", pods: []client.Object{withPhase(mirrorPod("m", "host1", "vk-host1"), corev1.PodFailed)}},
		{name: "mirror on another node -> holds", pods: []client.Object{mirrorPod("m", "host2", "vk-host2")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := noMirrorPodsBoundToNode("host1").Check(ctx, buildPreconditionsTestClient(tc.pods...))
			if tc.wantFail != errors.Is(err, rulekit.ErrPreconditionFailed) || (!tc.wantFail && err != nil) {
				t.Errorf("Check() = %v, want failure %v", err, tc.wantFail)
			}
		})
	}
}
