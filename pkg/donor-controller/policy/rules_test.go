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
	"bufio"
	"bytes"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func boolPtr(b bool) *bool { return &b }

func node(name string, ownedLabels ...string) (*corev1.Node, NodeMeta) {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{},
		},
	}
	meta := NodeMeta{
		Labels: map[string]string{},
	}
	for _, l := range ownedLabels {
		parts := strings.SplitN(l, "=", 2)
		k, v := parts[0], LabelTrue
		if len(parts) == 2 {
			v = parts[1]
		}
		n.Labels[k] = v
		meta.Labels[k] = v
	}
	return n, meta
}

func pod(name, nodeName, group string) *corev1.Pod {
	labels := map[string]string{
		DonorLabel: LabelTrue,
	}
	if group != "" {
		labels[PodGroupLabel] = group
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
				GuestLabel: LabelTrue,
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
			Name: VirtualNodeName(host),
			UID:  uid,
			Labels: map[string]string{
				GuestLabel: LabelTrue,
			},
		},
	}
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
		obs               func() *Observed
		wantFired         []string
		wantNodeNil       bool
		wantLabels        map[string]string
		wantAnnotations   map[string]string
		wantTaint         *corev1.Taint
		wantDeleteVN      *NodeRef
		wantPreconditions []Precondition
		wantRequeueAfter  time.Duration
		wantIsNoop        *bool
	}{
		{
			name: "unshared node, no donors -> fired none, empty intent, IsNoop true",
			obs: func() *Observed {
				n, owned := node("host1")
				return &Observed{
					Host:  "host1",
					Now:   fixedNow,
					Node:  n,
					Owned: owned,
				}
			},
			wantFired:   nil,
			wantNodeNil: false,
			wantIsNoop:  boolPtr(true),
		},
		{
			name: "donor pod bound (group g) -> W1/W2, labels {donor:true, group.timeslice.io/g:true}",
			obs: func() *Observed {
				n, owned := node("host1")
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel:                 LabelTrue,
				NodeGroupLabelPrefix + "g": LabelTrue,
			},
			wantIsNoop: boolPtr(false),
		},
		{
			name: "two donors groups g,h -> both group labels",
			obs: func() *Observed {
				n, owned := node("host1")
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "g"), pod("dp2", "host1", "h")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel:                 LabelTrue,
				NodeGroupLabelPrefix + "g": LabelTrue,
				NodeGroupLabelPrefix + "h": LabelTrue,
			},
			wantIsNoop: boolPtr(false),
		},
		{
			name: "donor pod with no group label -> only donor label",
			obs: func() *Observed {
				n, owned := node("host1")
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel: LabelTrue,
			},
			wantIsNoop: boolPtr(false),
		},
		{
			name: "donor pod with invalid group value (e.g. \"bad value!\") -> skipped",
			obs: func() *Observed {
				n, owned := node("host1")
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "bad value!")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel: LabelTrue,
			},
			wantIsNoop: boolPtr(false),
		},
		{
			name: "already shared with owned exactly matching -> IsNoop true",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel, NodeGroupLabelPrefix+"g")
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel:                 LabelTrue,
				NodeGroupLabelPrefix + "g": LabelTrue,
			},
			wantIsNoop: boolPtr(true),
		},
		{
			name: "era active, owned groups {g}, donors now only group h -> labels include both g and h",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel, NodeGroupLabelPrefix+"g")
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "h")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel:                 LabelTrue,
				NodeGroupLabelPrefix + "g": LabelTrue,
				NodeGroupLabelPrefix + "h": LabelTrue,
			},
			wantIsNoop: boolPtr(false),
		},
		{
			name: "era active, no donors, VN present -> W3/hold, labels kept, NO idle annotation",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel, NodeGroupLabelPrefix+"g")
				vn := virtualNode("host1", "uid-vn")
				return &Observed{
					Host:        "host1",
					Now:         fixedNow,
					Node:        n,
					VirtualNode: vn,
					Owned:       owned,
				}
			},
			wantFired: []string{"W3/hold"},
			wantLabels: map[string]string{
				DonorLabel:                 LabelTrue,
				NodeGroupLabelPrefix + "g": LabelTrue,
			},
			wantAnnotations: nil,
			wantIsNoop:      boolPtr(true),
		},
		{
			name: "era active, no donors, guest pods but VN nil -> W3/hold",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					GuestPods: []*corev1.Pod{guestPod("gp1", VirtualNodeName("host1"))},
				}
			},
			wantFired: []string{"W3/hold"},
			wantLabels: map[string]string{
				DonorLabel: LabelTrue,
			},
			wantAnnotations: nil,
			wantIsNoop:      boolPtr(true),
		},
		{
			name: "era active, empty, no annotation -> W3/idle-clock, annotation == Now (RFC3339), RequeueAfter == 5m",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				return &Observed{
					Host:  "host1",
					Now:   fixedNow,
					Node:  n,
					Owned: owned,
				}
			},
			wantFired: []string{"W3/idle-clock"},
			wantLabels: map[string]string{
				DonorLabel: LabelTrue,
			},
			wantAnnotations: map[string]string{
				IdleSinceAnnotation: fixedNow.Format(time.RFC3339),
			},
			wantRequeueAfter: defaultTTL,
			wantIsNoop:       boolPtr(false),
		},
		{
			name: "era active, empty, annotation Now-2m -> RequeueAfter 3m, annotation unchanged",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				idleTime := fixedNow.Add(-2 * time.Minute)
				owned.Annotations = map[string]string{
					IdleSinceAnnotation: idleTime.Format(time.RFC3339),
				}
				return &Observed{
					Host:  "host1",
					Now:   fixedNow,
					Node:  n,
					Owned: owned,
				}
			},
			wantFired: []string{"W3/idle-clock"},
			wantLabels: map[string]string{
				DonorLabel: LabelTrue,
			},
			wantAnnotations: map[string]string{
				IdleSinceAnnotation: fixedNow.Add(-2 * time.Minute).Format(time.RFC3339),
			},
			wantRequeueAfter: 3 * time.Minute,
			wantIsNoop:       boolPtr(true),
		},
		{
			name: "era active, empty, annotation Now-5m (exactly TTL) -> W3/unshare, empty intent, precondition NoDonorPodsOn{host}",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				idleTime := fixedNow.Add(-5 * time.Minute)
				owned.Annotations = map[string]string{
					IdleSinceAnnotation: idleTime.Format(time.RFC3339),
				}
				return &Observed{
					Host:  "host1",
					Now:   fixedNow,
					Node:  n,
					Owned: owned,
				}
			},
			wantFired:         []string{"W3/unshare"},
			wantNodeNil:       false,
			wantLabels:        nil,
			wantAnnotations:   nil,
			wantPreconditions: []Precondition{NoDonorPodsOn{NodeName: "host1"}},
			wantIsNoop:        boolPtr(false),
		},
		{
			name: "era active, empty, unparseable annotation -> treated as Now (idle-clock)",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				owned.Annotations = map[string]string{
					IdleSinceAnnotation: "invalid-timestamp",
				}
				return &Observed{
					Host:  "host1",
					Now:   fixedNow,
					Node:  n,
					Owned: owned,
				}
			},
			wantFired: []string{"W3/idle-clock"},
			wantLabels: map[string]string{
				DonorLabel: LabelTrue,
			},
			wantAnnotations: map[string]string{
				IdleSinceAnnotation: fixedNow.Format(time.RFC3339),
			},
			wantRequeueAfter: defaultTTL,
			wantIsNoop:       boolPtr(false),
		},
		{
			name: "IdleTTL 0 -> immediate W3/unshare",
			cfg:  &Config{IdleTTL: 0},
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				return &Observed{
					Host:  "host1",
					Now:   fixedNow,
					Node:  n,
					Owned: owned,
				}
			},
			wantFired:         []string{"W3/unshare"},
			wantNodeNil:       false,
			wantLabels:        nil,
			wantAnnotations:   nil,
			wantPreconditions: []Precondition{NoDonorPodsOn{NodeName: "host1"}},
			wantIsNoop:        boolPtr(false),
		},
		{
			name: "isolation taint configured: shared -> intent taint set",
			cfg:  &cfgWithTaint,
			obs: func() *Observed {
				n, owned := node("host1")
				return &Observed{
					Host:              "host1",
					Now:               fixedNow,
					Node:              n,
					Owned:             owned,
					HasIsolationTaint: false,
					DonorPods:         []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel:                 LabelTrue,
				NodeGroupLabelPrefix + "g": LabelTrue,
			},
			wantTaint:  isoTaint,
			wantIsNoop: boolPtr(false),
		},
		{
			name: "isolation taint configured: unshared after TTL -> taint nil",
			cfg:  &cfgWithTaint,
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				idleTime := fixedNow.Add(-5 * time.Minute)
				owned.Annotations = map[string]string{
					IdleSinceAnnotation: idleTime.Format(time.RFC3339),
				}
				return &Observed{
					Host:              "host1",
					Now:               fixedNow,
					Node:              n,
					Owned:             owned,
					HasIsolationTaint: true,
				}
			},
			wantFired:         []string{"W3/unshare"},
			wantNodeNil:       false,
			wantLabels:        nil,
			wantAnnotations:   nil,
			wantTaint:         nil,
			wantPreconditions: []Precondition{NoDonorPodsOn{NodeName: "host1"}},
			wantIsNoop:        boolPtr(false),
		},
		{
			name: "era active + idle annotation present, donor returns -> W1/W2 and annotation absent from intent (clock cleared)",
			obs: func() *Observed {
				n, owned := node("host1", DonorLabel)
				owned.Annotations = map[string]string{
					IdleSinceAnnotation: fixedNow.Add(-2 * time.Minute).Format(time.RFC3339),
				}
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      n,
					Owned:     owned,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired: []string{"W1/W2"},
			wantLabels: map[string]string{
				DonorLabel:                 LabelTrue,
				NodeGroupLabelPrefix + "g": LabelTrue,
			},
			wantAnnotations: nil,
			wantIsNoop:      boolPtr(false),
		},
		{
			name: "Node nil, VN present -> W4, plan.Node nil, DeleteVirtualNode with UID, precondition NodeAbsent{host}",
			obs: func() *Observed {
				vn := virtualNode("host1", "uid-1234")
				return &Observed{
					Host:        "host1",
					Now:         fixedNow,
					Node:        nil,
					VirtualNode: vn,
				}
			},
			wantFired:         []string{"W4"},
			wantNodeNil:       true,
			wantDeleteVN:      &NodeRef{Name: VirtualNodeName("host1"), UID: "uid-1234"},
			wantPreconditions: []Precondition{NodeAbsent{Name: "host1"}},
			wantIsNoop:        boolPtr(false),
		},
		{
			name: "Node nil, VN nil, donor pods still listed -> nothing fired, plan.Node nil",
			obs: func() *Observed {
				return &Observed{
					Host:      "host1",
					Now:       fixedNow,
					Node:      nil,
					DonorPods: []*corev1.Pod{pod("dp1", "host1", "g")},
				}
			},
			wantFired:   nil,
			wantNodeNil: true,
			wantIsNoop:  boolPtr(true),
		},
		{
			name: "human-applied donor label (on node.Labels but NOT in Owned) and no donors -> nothing fired, IsNoop true",
			obs: func() *Observed {
				n := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "host1",
						Labels: map[string]string{
							DonorLabel: LabelTrue,
						},
					},
				}
				return &Observed{
					Host:  "host1",
					Now:   fixedNow,
					Node:  n,
					Owned: NodeMeta{},
				}
			},
			wantFired:   nil,
			wantNodeNil: false,
			wantIsNoop:  boolPtr(true),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultCfg
			if tc.cfg != nil {
				cfg = *tc.cfg
			}
			obs := tc.obs()
			plan, err := Decide(Rules(cfg), obs)
			if err != nil {
				t.Fatalf("unexpected error from Decide: %v", err)
			}

			// Fired rules assertion
			if !slices.Equal(plan.Fired, tc.wantFired) && !(len(plan.Fired) == 0 && len(tc.wantFired) == 0) {
				t.Errorf("Fired = %v, want %v", plan.Fired, tc.wantFired)
			}

			// Plan.Node assertion
			if tc.wantNodeNil {
				if plan.Node != nil {
					t.Errorf("plan.Node = %v, want nil", plan.Node)
				}
			} else {
				if plan.Node == nil {
					t.Errorf("plan.Node = nil, want non-nil")
				} else {
					if !mapsEqual(plan.Node.Labels, tc.wantLabels) {
						t.Errorf("Labels = %v, want %v", plan.Node.Labels, tc.wantLabels)
					}
					if !mapsEqual(plan.Node.Annotations, tc.wantAnnotations) {
						t.Errorf("Annotations = %v, want %v", plan.Node.Annotations, tc.wantAnnotations)
					}
					if !reflect.DeepEqual(plan.Node.IsolationTaint, tc.wantTaint) {
						t.Errorf("IsolationTaint = %v, want %v", plan.Node.IsolationTaint, tc.wantTaint)
					}
				}
			}

			// DeleteVirtualNode assertion
			if !reflect.DeepEqual(plan.DeleteVirtualNode, tc.wantDeleteVN) {
				t.Errorf("DeleteVirtualNode = %v, want %v", plan.DeleteVirtualNode, tc.wantDeleteVN)
			}

			// Preconditions assertion
			if !reflect.DeepEqual(plan.Preconditions, tc.wantPreconditions) && !(len(plan.Preconditions) == 0 && len(tc.wantPreconditions) == 0) {
				t.Errorf("Preconditions = %v, want %v", plan.Preconditions, tc.wantPreconditions)
			}

			// RequeueAfter assertion
			if plan.RequeueAfter != tc.wantRequeueAfter {
				t.Errorf("RequeueAfter = %v, want %v", plan.RequeueAfter, tc.wantRequeueAfter)
			}

			// IsNoop assertion
			if tc.wantIsNoop != nil {
				gotNoop := plan.IsNoop(obs)
				if gotNoop != *tc.wantIsNoop {
					t.Errorf("IsNoop = %v, want %v", gotNoop, *tc.wantIsNoop)
				}
			}
		})
	}
}

func TestConflictDetection(t *testing.T) {
	rules := []Rule{
		{
			ID:  "rule1",
			Doc: "sets label foo=val1",
			When: func(obs *Observed) bool {
				return true
			},
			Then: func(obs *Observed, b *Builder) {
				b.SetLabel("foo", "val1")
			},
		},
		{
			ID:  "rule2",
			Doc: "sets label foo=val2",
			When: func(obs *Observed) bool {
				return true
			},
			Then: func(obs *Observed, b *Builder) {
				b.SetLabel("foo", "val2")
			},
		},
	}
	obs := &Observed{
		Host: "node1",
		Node: &corev1.Node{},
	}
	_, err := Decide(rules, obs)
	if err == nil {
		t.Fatal("expected conflict error from Decide, got nil")
	}
	if !strings.Contains(err.Error(), "conflict on label foo") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestPurity_NoClientGo(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go binary not found")
	}
	cmd := exec.Command(goBin, "list", "-deps", ".")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps failed: %v", err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		pkg := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(pkg, "k8s.io/client-go") {
			t.Errorf("policy package must not depend on client-go, found: %s", pkg)
		}
	}
}
