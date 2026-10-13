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

// Package rulekit runs a Kubernetes controller written as a table of rules
// instead of a growing Reconcile function.
//
// A [Controller] declares what it watches, how to fetch one snapshot of
// cluster state for a reconcile key, and a list of [Rule]s over that
// snapshot. For every key, the engine:
//
//  1. Fetches the snapshot through a read-only, cache-backed, memoized
//     [Reader] ([Controller.Fetch] is the only code that reads).
//  2. Runs every rule: When decides on the snapshot alone, and Then records
//     the desired outcome onto [Effects] as data (native apply
//     configurations plus a few explicit escape hatches).
//  3. Merges all recorded effects into a [Plan], rejecting conflicts
//     ([Controller.Evaluate]).
//  4. Executes the plan: emits deduplicated warnings, skips no-op writes,
//     honors dry run, checks live preconditions, then server-side applies,
//     patches taints, deletes, removes finalizers and foreign taints, and
//     emits Events naming the rules responsible.
//
// Everything is level-triggered. [Watch]es only say WHEN to re-evaluate
// WHICH key; they never carry data to rules. A missed event is never lost,
// only delayed until the next event, resync or wake-up ([Effects.WakeAt]).
//
// Rules never write. They only see the snapshot and [Effects]; this package
// is the only code in the controller that talks to the API server.
package rulekit

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Key identifies one reconcile unit. Cluster-scoped keys leave Namespace
// empty.
type Key = types.NamespacedName

// Controller is a complete declarative controller over snapshots of type T.
// Register it with [Setup].
type Controller[T any] struct {
	// Name names the controller in logs, metrics and as the Events source.
	Name string
	// FieldManager is the server-side apply field manager. It defines which
	// fields the controller owns.
	FieldManager string
	// Watches declare which object changes re-evaluate which keys. Reading a
	// type through the Reader that no Watch covers is an error, which keeps
	// Watches honest.
	Watches []Watch
	// Indexes are cache field indexes registered before the cache starts,
	// queried in Fetch with client.MatchingFields.
	Indexes []Index
	// Fetch reads everything the rules need for key into one snapshot. It
	// is the only code that reads cluster state.
	Fetch func(ctx context.Context, rd Reader, key Key) (T, error)
	// Rules are evaluated in order on every snapshot. A trigger for a key
	// always re-evaluates every rule.
	Rules []Rule[T]
	// Owns lists, for a key, the objects whose fields the controller owns.
	// Every reconcile, each owned object receives exactly the merged Apply
	// of all rules that fired, or an empty apply if none asserted anything,
	// which releases every field the controller owned on it. Applying to an
	// object not listed here is an error.
	Owns func(key Key) []Ref
	// ManagedTaints lists, for a key, the node taints the controller owns.
	// A managed taint is present iff some rule asserted it with
	// [Effects.Taint]. May be nil.
	ManagedTaints func(key Key) []ManagedTaint
}

// Rule is one "when the snapshot looks like this, the cluster should look
// like that" entry.
type Rule[T any] struct {
	// Name is a short phrase saying what the rule does in the bigger
	// picture, e.g. "share node while donor pods are bound". It appears
	// verbatim in logs, Events and errors: rule "<Name>": ...
	Name string
	// When reports whether the rule applies. It must be a pure function of
	// the snapshot: no reads, no clocks (use Reader.Now in Fetch).
	When func(T) bool
	// Then records the rule's desired outcome. It is only called if When
	// returned true, and must be pure like When.
	Then func(T, *Effects) error
}

// Watch enqueues keys when objects of one type are added, updated or deleted
// in the shared informer cache. On update, both the old and the new object
// are filtered, so an object that stops matching still re-evaluates its
// keys.
type Watch struct {
	// Object is a prototype of the watched type, e.g. &corev1.Pod{}.
	Object client.Object
	// Filter, if non-nil, drops events for objects it returns false for.
	Filter func(client.Object) bool
	// Keys maps an object to the keys it affects.
	Keys func(client.Object) []Key
}

// Index declares a cache field index, queried with
// client.MatchingFields{Field: value}.
type Index struct {
	Object  client.Object
	Field   string
	Extract func(client.Object) []string
}

// Ref identifies an object by kind and name.
type Ref struct {
	GVK       schema.GroupVersionKind
	Namespace string
	Name      string
}

func (r Ref) String() string {
	if r.Namespace == "" {
		return fmt.Sprintf("%s/%s", r.GVK.Kind, r.Name)
	}
	return fmt.Sprintf("%s/%s/%s", r.GVK.Kind, r.Namespace, r.Name)
}

// ManagedTaint is a node taint owned by the controller. Taints are an
// atomic list in the Node schema, so server-side apply would take ownership
// of every taint on the node; the engine instead read-modify-writes exactly
// the managed taint (matched by Key and Effect).
type ManagedTaint struct {
	Node   string
	Key    string
	Effect corev1.TaintEffect
}

// Reader is a read-only, cache-backed client. Within one reconcile, reads
// are repeatable: the first read of an object or list is memoized and every
// later read returns the same result, so all rules decide on one consistent
// snapshot.
type Reader interface {
	client.Reader
	// Now is the reconcile's observation time; it is fixed for the whole
	// reconcile so every rule sees the same instant.
	Now() time.Time
}

// Precondition is a live check run by the engine against the API server
// (bypassing caches) right before any effect is applied. If any
// precondition fails, nothing is applied and the key is retried. Use them
// to guard writes that are expensive to get wrong on stale cache data.
type Precondition struct {
	// Description says what must hold, e.g. "no donor pods bound to node
	// n1". It appears in logs.
	Description string
	// Check returns nil if the precondition holds, an error wrapping
	// ErrPreconditionFailed if it does not, or any other error if it could
	// not be evaluated.
	Check func(ctx context.Context, live client.Reader) error
}

// ErrPreconditionFailed is wrapped by Precondition.Check when a precondition
// does not hold.
var ErrPreconditionFailed = errors.New("precondition failed")

// Effects collects one reconcile's desired outcome from all rules. It never
// performs I/O; the engine turns it into API calls after every rule ran.
type Effects struct {
	rule       string
	applies    []Attributed[runtime.ApplyConfiguration]
	taints     []Attributed[nodeTaint]
	deletes    []Attributed[client.Object]
	finalizers []Attributed[FinalizerRemoval]
	untaints   []Attributed[TaintRemoval]
	warnings   []Attributed[Warning]
	pres       []Attributed[Precondition]
	wake       *Attributed[time.Time]
}

// Attributed is a value recorded on [Effects] together with the Name of the
// rule that recorded it.
type Attributed[V any] struct {
	Rule  string
	Value V
}

type nodeTaint struct {
	Node  string
	Taint corev1.Taint
}

// FinalizerRemoval is a finalizer to strip from an object, recorded with
// [Effects.RemoveFinalizer].
type FinalizerRemoval struct {
	Object    client.Object
	Finalizer string
}

// TaintRemoval is a taint the controller does not own to remove from a node,
// recorded with [Effects.RemoveTaint].
type TaintRemoval struct {
	Node  *corev1.Node
	Taint corev1.Taint
}

// Warning is a Warning Event recorded with [Effects.Warn].
type Warning struct {
	Object  client.Object
	Reason  string
	Message string
}

// Apply asserts desired owned state, as a native apply configuration (e.g.
// corev1ac.Node(name).WithLabels(...)). Configurations from all rules for
// the same object are merged; two rules setting the same field to different
// values is a conflict. Do not set metadata.uid or resourceVersion: the
// engine pins the incarnation the rules observed.
func (fx *Effects) Apply(ac runtime.ApplyConfiguration) {
	fx.applies = append(fx.applies, Attributed[runtime.ApplyConfiguration]{fx.rule, ac})
}

// Taint asserts that a managed taint is present on node.
func (fx *Effects) Taint(node string, t corev1.Taint) {
	fx.taints = append(fx.taints, Attributed[nodeTaint]{fx.rule, nodeTaint{node, t}})
}

// Delete deletes obj, which must be an object returned by the Reader. The
// deletion is preconditioned on obj's UID, so it never hits a newer object
// with the same name. Use it for objects the controller does not own.
func (fx *Effects) Delete(obj client.Object) {
	fx.deletes = append(fx.deletes, Attributed[client.Object]{fx.rule, obj})
}

// RemoveFinalizer strips finalizer from obj, which must be an object
// returned by the Reader. It runs after every Delete, so together they
// release an object whose own controller can no longer finish it. The patch
// is preconditioned on obj's UID and on the finalizer still being where the
// rules saw it; it is a no-op if obj does not carry the finalizer.
func (fx *Effects) RemoveFinalizer(obj client.Object, finalizer string) {
	fx.finalizers = append(fx.finalizers, Attributed[FinalizerRemoval]{fx.rule, FinalizerRemoval{obj, finalizer}})
}

// RemoveTaint removes taint, which another component owns, from node, which
// must be a Node returned by the Reader. Use it to release a taint whose
// owner is gone and can no longer remove it; taints the controller owns are
// declared in [Controller.ManagedTaints] and asserted with [Effects.Taint]
// instead. The patch is preconditioned on node's UID and on the exact taint
// still being where the rules saw it; it is a no-op if node does not carry
// the taint. It runs after finalizer removals.
func (fx *Effects) RemoveTaint(node *corev1.Node, taint corev1.Taint) {
	fx.untaints = append(fx.untaints, Attributed[TaintRemoval]{fx.rule, TaintRemoval{node, taint}})
}

// Warn records a Warning Event on obj, which must be an object returned by
// the Reader, for as long as rules keep asserting it. The engine emits each
// distinct warning once while it stays asserted for the key, and again only
// after it stopped and recurred, so a rule can assert it on every reconcile.
// Warnings are not cluster state: dry run logs them, and preconditions do
// not guard them.
func (fx *Effects) Warn(obj client.Object, reason, message string) {
	fx.warnings = append(fx.warnings, Attributed[Warning]{fx.rule, Warning{obj, reason, message}})
}

// Require adds a live precondition guarding every effect of this reconcile.
func (fx *Effects) Require(p Precondition) {
	fx.pres = append(fx.pres, Attributed[Precondition]{fx.rule, p})
}

// WakeAt asks for the key to be re-evaluated at t even if nothing changes.
// The earliest request wins.
func (fx *Effects) WakeAt(t time.Time) {
	if fx.wake == nil || t.Before(fx.wake.Value) {
		fx.wake = &Attributed[time.Time]{fx.rule, t}
	}
}
