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

// Package policy is the pure decision layer of the donor controller.
//
// Everything in this package is a function of an [Observed] snapshot and
// returns a [Plan], which is inert data. This package must never import
// client-go: the import graph is what guarantees that every side effect lives
// in the controller package's executor, and nowhere else.
//
// The controller is level-triggered. A Plan does not say "add label X" or
// "remove label Y"; it states the complete set of node fields the controller
// wants to own right now. Anything the controller owned before and does not
// list is released (and removed by server-side apply). This is what makes
// add-vs-remove fights between rules impossible by construction.
package policy

import (
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Label and annotation contract. Inputs are written by other actors (webhook,
// guest kubelet, users); outputs are written only by this controller.
const (
	// FieldManager is the server-side apply field manager name. It defines
	// which node fields this controller owns.
	FieldManager = "timeslice-donor-controller"

	// DonorLabel marks donor pods (input) and shared real nodes (output, W1).
	// The snapshot-agent and guest-kubelet DaemonSets select on it.
	DonorLabel = "timeslice.io/donor"
	// GuestLabel marks virtual nodes and guest pods (input).
	GuestLabel = "timeslice.io/guest"
	// PodGroupLabel carries a donor pod's group (input, set by the webhook).
	PodGroupLabel = "timeslice.io/group"
	// NodeGroupLabelPrefix + <group> = "true" puts a real node in a group
	// (output, W2). This matches what the orchestrator's node watch selects.
	NodeGroupLabelPrefix = "group.timeslice.io/"
	// IdleSinceAnnotation records, in RFC 3339, when a shared node was first
	// observed empty (output, W3 clock). Keeping the clock on the node makes
	// it visible to kubectl and survives controller restarts.
	IdleSinceAnnotation = "timeslice.io/idle-since"
	// VirtualNodePrefix + <host> is the name of host's virtual node.
	VirtualNodePrefix = "vk-"

	// LabelTrue is the value used for boolean labels.
	LabelTrue = "true"
)

// VirtualNodeName returns the virtual node name for a real host.
func VirtualNodeName(host string) string { return VirtualNodePrefix + host }

// Config holds the policy knobs.
type Config struct {
	// IdleTTL is how long a shared node must stay empty (no donors, no guests,
	// no virtual node) before the controller unshares it (W3).
	IdleTTL time.Duration
	// IsolationTaint, if non-nil, is kept on shared nodes (W5). The controller
	// owns exactly the taint with this key; other taints are never touched.
	IsolationTaint *corev1.Taint
}

// Observed is everything a decision may look at for one real host. It is
// built from informer caches and must be treated as read-only.
type Observed struct {
	// Host is the real node name; it is the reconcile key.
	Host string
	// Now is the observation time. Rules must use this, never time.Now.
	Now time.Time

	// Node is the real node, or nil if it does not exist.
	Node *corev1.Node
	// VirtualNode is the node named VirtualNodeName(Host) carrying
	// GuestLabel=true, or nil if no such node exists.
	VirtualNode *corev1.Node
	// DonorPods are non-terminal pods labeled DonorLabel=true with
	// spec.nodeName == Host. Terminating pods are included.
	DonorPods []*corev1.Pod
	// GuestPods are non-terminal pods with spec.nodeName == VirtualNode name.
	GuestPods []*corev1.Pod

	// Owned is the node metadata this controller currently owns on Node, as
	// recorded in managedFields. It is the zero value if Node is nil.
	Owned NodeMeta
	// HasIsolationTaint reports whether Node currently carries a taint with
	// Config.IsolationTaint's key. Always false if no taint is configured.
	HasIsolationTaint bool
}

// NodeMeta is a set of node labels and annotations.
type NodeMeta struct {
	Labels      map[string]string
	Annotations map[string]string
}

// NodeIntent is the complete set of real-node fields the controller wants to
// own. Fields it owned before and that are absent here are released.
type NodeIntent struct {
	Labels      map[string]string
	Annotations map[string]string
	// IsolationTaint is the taint that must be present, or nil if the
	// controller's taint must be absent.
	IsolationTaint *corev1.Taint
}

// Plan is the inert output of a decision for one host. Only the controller's
// executor turns it into API calls.
type Plan struct {
	Host string
	// Node is the desired owned state of the real node. nil means "do not
	// touch the real node" (it does not exist).
	Node *NodeIntent
	// DeleteVirtualNode, if set, deletes the orphaned virtual node (W4).
	DeleteVirtualNode *NodeRef
	// Preconditions are re-checked against the live API (bypassing caches)
	// before any effect is applied. If any fails, no effect is applied and the
	// host is requeued.
	Preconditions []Precondition
	// RequeueAfter, if positive, asks for a re-evaluation after this delay
	// even if nothing changes (used by the W3 idle clock).
	RequeueAfter time.Duration
	// Fired lists the IDs of the rules that fired, in evaluation order.
	Fired []string
}

// NodeRef identifies a specific node incarnation.
type NodeRef struct {
	Name string
	UID  types.UID
}

// Precondition is a live check the executor performs before applying a plan.
// It narrows races between cache staleness and writes that are hard to undo.
type Precondition interface {
	fmt.Stringer
	isPrecondition()
}

// NoDonorPodsOn holds if a live LIST finds no non-terminal pods labeled
// DonorLabel=true bound to NodeName.
type NoDonorPodsOn struct{ NodeName string }

// NodeAbsent holds if a live GET of the node returns NotFound.
type NodeAbsent struct{ Name string }

func (NoDonorPodsOn) isPrecondition() {}
func (NodeAbsent) isPrecondition()    {}

func (p NoDonorPodsOn) String() string { return "no donor pods on " + p.NodeName }
func (p NodeAbsent) String() string    { return "node " + p.Name + " absent" }

// Rule is one "observe, filter, act" entry. When is the filter; Then
// contributes to the plan through the Builder. A rule never performs I/O.
type Rule struct {
	// ID ties the rule to the design doc (e.g. "W1/W2").
	ID string
	// Doc is a one-line human description, printed in logs and dry runs.
	Doc string
	// When reports whether the rule applies to the observation.
	When func(*Observed) bool
	// Then contributes the rule's desired state to the plan.
	Then func(*Observed, *Builder)
}

// Builder accumulates rule contributions into a Plan and detects conflicts
// between rules (two rules wanting different values for the same field).
type Builder struct {
	plan Plan
	errs []error
	// owner records which rule set each field, for conflict messages.
	owner map[string]string
	rule  string
}

// SetLabel requires label key=value on the real node.
func (b *Builder) SetLabel(key, value string) {
	b.set("label "+key, value, func(in *NodeIntent) *map[string]string { return &in.Labels }, key)
}

// SetAnnotation requires annotation key=value on the real node.
func (b *Builder) SetAnnotation(key, value string) {
	b.set("annotation "+key, value, func(in *NodeIntent) *map[string]string { return &in.Annotations }, key)
}

func (b *Builder) set(field, value string, m func(*NodeIntent) *map[string]string, key string) {
	if b.plan.Node == nil {
		b.errs = append(b.errs, fmt.Errorf("rule %s sets %s but the real node is absent", b.rule, field))
		return
	}
	mp := m(b.plan.Node)
	if *mp == nil {
		*mp = map[string]string{}
	}
	if old, ok := (*mp)[key]; ok && old != value {
		b.errs = append(b.errs, fmt.Errorf("conflict on %s: rule %s wants %q, rule %s wants %q",
			field, b.owner[field], old, b.rule, value))
		return
	}
	(*mp)[key] = value
	b.owner[field] = b.rule
}

// SetIsolationTaint requires the given taint on the real node.
func (b *Builder) SetIsolationTaint(t corev1.Taint) {
	if b.plan.Node == nil {
		b.errs = append(b.errs, fmt.Errorf("rule %s sets a taint but the real node is absent", b.rule))
		return
	}
	// MatchTaint compares only key and effect, so compare the value too.
	if old := b.plan.Node.IsolationTaint; old != nil && (!old.MatchTaint(&t) || old.Value != t.Value) {
		b.errs = append(b.errs, fmt.Errorf("conflict on isolation taint between rules %s and %s", b.owner["taint"], b.rule))
		return
	}
	b.plan.Node.IsolationTaint = &t
	b.owner["taint"] = b.rule
}

// DeleteVirtualNode requests deletion of the given virtual node.
func (b *Builder) DeleteVirtualNode(ref NodeRef) { b.plan.DeleteVirtualNode = &ref }

// Require adds a live precondition to the plan.
func (b *Builder) Require(p Precondition) { b.plan.Preconditions = append(b.plan.Preconditions, p) }

// RequeueAfter asks for re-evaluation after d. The shortest request wins.
func (b *Builder) RequeueAfter(d time.Duration) {
	if d <= 0 {
		d = time.Nanosecond
	}
	if b.plan.RequeueAfter == 0 || d < b.plan.RequeueAfter {
		b.plan.RequeueAfter = d
	}
}

// Decide evaluates every rule against obs and returns the merged plan. Rules
// run in order; every rule whose When holds contributes. A non-nil error
// means two rules disagreed, which is a policy bug, and the plan must not be
// executed.
func Decide(rules []Rule, obs *Observed) (Plan, error) {
	b := &Builder{owner: map[string]string{}}
	b.plan.Host = obs.Host
	if obs.Node != nil {
		// Start from "own nothing": anything not re-asserted by a rule is
		// released. This is the level-triggered default.
		b.plan.Node = &NodeIntent{}
	}
	for _, r := range rules {
		if !r.When(obs) {
			continue
		}
		b.rule = r.ID
		b.plan.Fired = append(b.plan.Fired, r.ID)
		r.Then(obs, b)
	}
	if len(b.errs) > 0 {
		msgs := make([]string, len(b.errs))
		for i, err := range b.errs {
			msgs[i] = err.Error()
		}
		return Plan{}, fmt.Errorf("policy conflict for host %s: %s", obs.Host, strings.Join(msgs, "; "))
	}
	return b.plan, nil
}

// IsNoop reports whether applying the plan would change nothing, given what
// the controller currently owns.
func (p Plan) IsNoop(obs *Observed) bool {
	if p.DeleteVirtualNode != nil {
		return false
	}
	if p.Node == nil {
		return true
	}
	if !mapsEqual(p.Node.Labels, obs.Owned.Labels) || !mapsEqual(p.Node.Annotations, obs.Owned.Annotations) {
		return false
	}
	return (p.Node.IsolationTaint != nil) == obs.HasIsolationTaint
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

// String renders the plan for logs and dry runs.
func (p Plan) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "host=%s fired=%v", p.Host, p.Fired)
	if p.Node != nil {
		fmt.Fprintf(&sb, " labels=%v annotations=%v", sortedPairs(p.Node.Labels), sortedPairs(p.Node.Annotations))
		if p.Node.IsolationTaint != nil {
			fmt.Fprintf(&sb, " taint=%s", p.Node.IsolationTaint.ToString())
		}
	}
	if p.DeleteVirtualNode != nil {
		fmt.Fprintf(&sb, " delete-vn=%s(uid=%s)", p.DeleteVirtualNode.Name, p.DeleteVirtualNode.UID)
	}
	for _, pre := range p.Preconditions {
		fmt.Fprintf(&sb, " require=[%s]", pre)
	}
	if p.RequeueAfter > 0 {
		fmt.Fprintf(&sb, " requeue=%s", p.RequeueAfter)
	}
	return sb.String()
}

func sortedPairs(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	slices.Sort(out)
	return out
}
