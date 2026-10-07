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
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func node(name string, ownedLabels ...string) (*corev1.Node, map[string]string) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{},
		},
	}
	owned := map[string]string{}
	for _, l := range ownedLabels {
		parts := strings.SplitN(l, "=", 2)
		k, v := parts[0], wellknown.LabelValueTrue
		if len(parts) == 2 {
			v = parts[1]
		}
		n.Labels[k] = v
		owned[k] = v
	}
	return n, owned
}

func pod(name, nodeName, group string) *corev1.Pod {
	labels := map[string]string{
		wellknown.LabelDonor: wellknown.LabelValueTrue,
	}
	if group != "" {
		labels[wellknown.LabelGroup] = group
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
	}
}

func guestPod(name, nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				wellknown.LabelGuest: wellknown.LabelValueTrue,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
	}
}

func virtualNode(host string, uid types.UID) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: wellknown.VirtualNodeName(host),
			UID:  uid,
			Labels: map[string]string{
				wellknown.LabelGuest: wellknown.LabelValueTrue,
			},
		},
	}
}

type wantDelete struct {
	Name string
	UID  types.UID
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

func TestRulesTable(t *testing.T) {
	fixedNow := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	defaultTTL := 5 * time.Minute
	defaultCfg := Config{IdleTTL: defaultTTL}

	isoTaint := &corev1.Taint{
		Key:    "timeslice.io/shared",
		Value:  "true",
		Effect: corev1.TaintEffectNoSchedule,
	}
	cfgWithTaint := Config{
		IdleTTL:        defaultTTL,
		IsolationTaint: isoTaint,
	}

	tests := []struct {
		name              string
		cfg               *Config
		data              func() *hostData
		wantFired         []string
		wantLabels        map[string]string
		wantAnnotations   map[string]string
		wantTaint         *corev1.Taint
		wantDeleteVN      *wantDelete
		wantPreconditions []rulekit.Precondition
		wantWakeAt        *time.Time
	}{
		{
			name: "unshared node, no donors -> fired none",
			data: func() *hostData {
				n, owned := node("host1")
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
				}
			},
			wantFired: nil,
		},
		{
			name: "donor pod bound (group g) -> W1/W2, labels {donor:true, group.timeslice.io/g:true}",
			data: func() *hostData {
				n, owned := node("host1")
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					Donors:      []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor:          wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("g"): wellknown.LabelValueTrue,
			},
		},
		{
			name: "two donors groups g,h -> both group labels",
			data: func() *hostData {
				n, owned := node("host1")
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					Donors:      []*corev1.Pod{pod("dp1", "host1", "g"), pod("dp2", "host1", "h")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor:          wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("g"): wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("h"): wellknown.LabelValueTrue,
			},
		},
		{
			name: "donor pod with no group label -> only donor label",
			data: func() *hostData {
				n, owned := node("host1")
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					Donors:      []*corev1.Pod{pod("dp1", "host1", "")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor: wellknown.LabelValueTrue,
			},
		},
		{
			name: "donor pod with invalid group value (e.g. \"bad value!\") -> skipped",
			data: func() *hostData {
				n, owned := node("host1")
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					Donors:      []*corev1.Pod{pod("dp1", "host1", "bad value!")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor: wellknown.LabelValueTrue,
			},
		},
		{
			name: "already shared with owned exactly matching",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor, wellknown.NodeGroupLabel("g"))
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					Donors:      []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor:          wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("g"): wellknown.LabelValueTrue,
			},
		},
		{
			name: "era active, owned groups {g}, donors now only group h -> labels include both g and h",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor, wellknown.NodeGroupLabel("g"))
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					Donors:      []*corev1.Pod{pod("dp1", "host1", "h")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor:          wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("g"): wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("h"): wellknown.LabelValueTrue,
			},
		},
		{
			name: "era active, no donors, VN present -> W3/hold, labels kept, NO idle annotation",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor, wellknown.NodeGroupLabel("g"))
				vn := virtualNode("host1", "uid-vn")
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					VNode:       vn,
					OwnedLabels: owned,
				}
			},
			wantFired: []string{"W3/hold"},
			wantLabels: map[string]string{
				wellknown.LabelDonor:          wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("g"): wellknown.LabelValueTrue,
			},
			wantAnnotations: nil,
		},
		{
			name: "era active, no donors, guest pods but VN nil -> W3/hold",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					Guests:      []*corev1.Pod{guestPod("gp1", wellknown.VirtualNodeName("host1"))},
				}
			},
			wantFired: []string{"W3/hold"},
			wantLabels: map[string]string{
				wellknown.LabelDonor: wellknown.LabelValueTrue,
			},
			wantAnnotations: nil,
		},
		{
			name: "era active, empty, no annotation -> W3/idle-clock, annotation == Now (RFC3339), WakeAt == 5m",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
				}
			},
			wantFired: []string{"W3/idle-clock"},
			wantLabels: map[string]string{
				wellknown.LabelDonor: wellknown.LabelValueTrue,
			},
			wantAnnotations: map[string]string{
				wellknown.AnnotationIdleSince: fixedNow.Format(time.RFC3339),
			},
			wantWakeAt: func() *time.Time { t := fixedNow.Add(defaultTTL); return &t }(),
		},
		{
			name: "era active, empty, annotation Now-2m -> WakeAt 3m from now, annotation unchanged",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				idleTime := fixedNow.Add(-2 * time.Minute)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					OwnedAnnotations: map[string]string{
						wellknown.AnnotationIdleSince: idleTime.Format(time.RFC3339),
					},
				}
			},
			wantFired: []string{"W3/idle-clock"},
			wantLabels: map[string]string{
				wellknown.LabelDonor: wellknown.LabelValueTrue,
			},
			wantAnnotations: map[string]string{
				wellknown.AnnotationIdleSince: fixedNow.Add(-2 * time.Minute).Format(time.RFC3339),
			},
			wantWakeAt: func() *time.Time { t := fixedNow.Add(-2 * time.Minute).Add(defaultTTL); return &t }(),
		},
		{
			name: "era active, empty, annotation Now-5m (exactly TTL) -> W3/unshare, precondition NoDonorPodsOn{host}",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				idleTime := fixedNow.Add(-5 * time.Minute)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					OwnedAnnotations: map[string]string{
						wellknown.AnnotationIdleSince: idleTime.Format(time.RFC3339),
					},
				}
			},
			wantFired:         []string{"W3/unshare"},
			wantPreconditions: []rulekit.Precondition{NoDonorPodsOn{NodeName: "host1"}},
		},
		{
			name: "era active, empty, unparseable annotation -> treated as Now (idle-clock)",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					OwnedAnnotations: map[string]string{
						wellknown.AnnotationIdleSince: "invalid-timestamp",
					},
				}
			},
			wantFired: []string{"W3/idle-clock"},
			wantLabels: map[string]string{
				wellknown.LabelDonor: wellknown.LabelValueTrue,
			},
			wantAnnotations: map[string]string{
				wellknown.AnnotationIdleSince: fixedNow.Format(time.RFC3339),
			},
			wantWakeAt: func() *time.Time { t := fixedNow.Add(defaultTTL); return &t }(),
		},
		{
			name: "IdleTTL 0 -> immediate W3/unshare",
			cfg:  &Config{IdleTTL: 0},
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
				}
			},
			wantFired:         []string{"W3/unshare"},
			wantPreconditions: []rulekit.Precondition{NoDonorPodsOn{NodeName: "host1"}},
		},
		{
			name: "isolation taint configured: shared -> taint set",
			cfg:  &cfgWithTaint,
			data: func() *hostData {
				n, owned := node("host1")
				return &hostData{
					Host:              "host1",
					Now:               fixedNow,
					Node:              n,
					OwnedLabels:       owned,
					HasIsolationTaint: false,
					Donors:            []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor:          wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("g"): wellknown.LabelValueTrue,
			},
			wantTaint: isoTaint,
		},
		{
			name: "isolation taint configured: unshared after TTL -> taint nil",
			cfg:  &cfgWithTaint,
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				idleTime := fixedNow.Add(-5 * time.Minute)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					OwnedAnnotations: map[string]string{
						wellknown.AnnotationIdleSince: idleTime.Format(time.RFC3339),
					},
					HasIsolationTaint: true,
				}
			},
			wantFired:         []string{"W3/unshare"},
			wantTaint:         nil,
			wantPreconditions: []rulekit.Precondition{NoDonorPodsOn{NodeName: "host1"}},
		},
		{
			name: "era active + idle annotation present, donor returns -> W1/W2 and annotation absent (clock cleared)",
			data: func() *hostData {
				n, owned := node("host1", wellknown.LabelDonor)
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: owned,
					OwnedAnnotations: map[string]string{
						wellknown.AnnotationIdleSince: fixedNow.Add(-2 * time.Minute).Format(time.RFC3339),
					},
					Donors: []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				wellknown.LabelDonor:          wellknown.LabelValueTrue,
				wellknown.NodeGroupLabel("g"): wellknown.LabelValueTrue,
			},
			wantAnnotations: nil,
		},
		{
			name: "Node nil, VN present -> W4, Delete(VN), precondition NodeAbsent{host}",
			data: func() *hostData {
				vn := virtualNode("host1", "uid-1234")
				return &hostData{
					Host:  "host1",
					Now:   fixedNow,
					Node:  nil,
					VNode: vn,
				}
			},
			wantFired:         []string{"W4"},
			wantDeleteVN:      &wantDelete{Name: wellknown.VirtualNodeName("host1"), UID: "uid-1234"},
			wantPreconditions: []rulekit.Precondition{NodeAbsent{Name: "host1"}},
		},
		{
			name: "Node nil, VN nil, donor pods still listed -> nothing fired",
			data: func() *hostData {
				return &hostData{
					Host:   "host1",
					Now:    fixedNow,
					Node:   nil,
					Donors: []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: nil,
		},
		{
			name: "human-applied donor label (on node.Labels but NOT in Owned) and no donors -> nothing fired",
			data: func() *hostData {
				n := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "host1",
						Labels: map[string]string{
							wellknown.LabelDonor: wellknown.LabelValueTrue,
						},
					},
				}
				return &hostData{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					OwnedLabels: map[string]string{},
				}
			},
			wantFired: nil,
		},
		{
			name: "IsVirtual -> nothing fires",
			data: func() *hostData {
				n := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "vk-host1",
						Labels: map[string]string{
							wellknown.LabelGuest: wellknown.LabelValueTrue,
							wellknown.LabelDonor: wellknown.LabelValueTrue,
						},
					},
				}
				return &hostData{
					Host:        "vk-host1",
					Now:         fixedNow,
					Node:        n,
					IsVirtual:   true,
					OwnedLabels: map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
					Donors:      []*corev1.Pod{pod("dp1", "vk-host1", "g")},
				}
			},
			wantFired: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultCfg
			if tc.cfg != nil {
				cfg = *tc.cfg
			}
			d := tc.data()
			rules := Rules(cfg)

			var fired []string
			var allApplies []rulekit.RecordedApply
			var allTaints []rulekit.RecordedTaint
			var allDeletes []rulekit.RecordedDelete
			var allPreconditions []rulekit.RecordedPrecondition
			var wake *rulekit.RecordedWake

			for _, r := range rules {
				fn, ok := r.(rulekit.Func[*hostData])
				if !ok {
					t.Fatalf("rule %s is not rulekit.Func[*hostData]", r.ID())
				}
				eval := fn.Evaluate(d)
				should, err := eval.Should(context.Background())
				if err != nil {
					t.Fatalf("rule %s Should: %v", r.ID(), err)
				}
				if !should {
					continue
				}
				fired = append(fired, r.ID())
				fx := (&rulekit.Effects{}).ForRule(r.ID())
				if err := eval.Do(context.Background(), fx); err != nil {
					t.Fatalf("rule %s Do: %v", r.ID(), err)
				}
				allApplies = append(allApplies, fx.Applies()...)
				allTaints = append(allTaints, fx.Taints()...)
				allDeletes = append(allDeletes, fx.Deletes()...)
				allPreconditions = append(allPreconditions, fx.Preconditions()...)
				if w := fx.Wake(); w != nil {
					if wake == nil || w.At.Before(wake.At) {
						wake = w
					}
				}
			}

			// Fired rules assertion
			if !slices.Equal(fired, tc.wantFired) && !(len(fired) == 0 && len(tc.wantFired) == 0) {
				t.Errorf("Fired = %v, want %v", fired, tc.wantFired)
			}

			// Extract labels and annotations from allApplies
			var gotLabels, gotAnnotations map[string]string
			for _, app := range allApplies {
				data, err := json.Marshal(app.Config)
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
			if tc.wantTaint != nil {
				if len(allTaints) != 1 {
					t.Errorf("Taints count = %d, want 1", len(allTaints))
				} else if allTaints[0].Node != d.Host || !reflect.DeepEqual(allTaints[0].Taint, *tc.wantTaint) {
					t.Errorf("Taint = %+v on %s, want %+v on %s", allTaints[0].Taint, allTaints[0].Node, *tc.wantTaint, d.Host)
				}
			} else {
				if len(allTaints) != 0 {
					t.Errorf("Taints = %+v, want none", allTaints)
				}
			}

			// Deletes
			if tc.wantDeleteVN != nil {
				if len(allDeletes) != 1 {
					t.Errorf("Deletes count = %d, want 1", len(allDeletes))
				} else {
					delNode, ok := allDeletes[0].Object.(*corev1.Node)
					if !ok || delNode.Name != tc.wantDeleteVN.Name || delNode.UID != tc.wantDeleteVN.UID {
						t.Errorf("Deleted obj = %v, want %s (UID %s)", allDeletes[0].Object, tc.wantDeleteVN.Name, tc.wantDeleteVN.UID)
					}
				}
			} else {
				if len(allDeletes) != 0 {
					t.Errorf("Deletes = %+v, want none", allDeletes)
				}
			}

			// Preconditions
			var gotPreconditions []rulekit.Precondition
			for _, p := range allPreconditions {
				gotPreconditions = append(gotPreconditions, p.Precondition)
			}
			if !reflect.DeepEqual(gotPreconditions, tc.wantPreconditions) && !(len(gotPreconditions) == 0 && len(tc.wantPreconditions) == 0) {
				t.Errorf("Preconditions = %v, want %v", gotPreconditions, tc.wantPreconditions)
			}

			// Wake
			if tc.wantWakeAt != nil {
				if wake == nil {
					t.Errorf("Wake = nil, want %v", *tc.wantWakeAt)
				} else if !wake.At.Equal(*tc.wantWakeAt) {
					t.Errorf("Wake = %v, want %v", wake.At, *tc.wantWakeAt)
				}
			} else {
				if wake != nil {
					t.Errorf("Wake = %v, want nil", wake)
				}
			}
		})
	}
}
