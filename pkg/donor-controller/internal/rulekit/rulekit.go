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

// Package rulekit is a small declarative layer for writing Kubernetes
// controllers as tables of rules instead of growing Reconcile functions.
//
// A [Controller] is a list of [Rule]s plus what they watch, index and own.
// For one reconcile key, every rule:
//
//  1. Gathers what it needs through a read-only, cache-backed [Reader]
//     (the only place a rule reads), into a plain data value ([Evaluator]).
//  2. Decides on that data alone ([Evaluator.Should]).
//  3. If it should act, describes the outcome as data ([Evaluator.Do]) by
//     recording onto [Effects]: desired owned state as native apply
//     configurations, plus a few explicit escape hatches.
//
// The engine (package rulekit/ctrl) merges all rules' effects, detects
// conflicts, and is the only code that writes to the API server.
//
// Rules are level-triggered. [Source]s only say WHEN to re-evaluate WHICH
// key; they never carry data to rules. Everything a rule acts on comes from
// Gather at evaluation time, so a missed event can never be lost, only
// delayed until the next event, resync or alarm ([Effects.WakeAt]).
//
// This package must stay free of writers: it must not directly import the
// clientset, rest config, manager, cache or builder packages. A test in
// package policy enforces that for the donor controller.
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

// Rule is one declarative "observe, filter, act" entry.
type Rule interface {
	// ID ties the rule to a design doc entry (e.g. "W3/unshare"). It is
	// attached to every effect the rule records, in logs and Events.
	ID() string
	// Sources declare when this rule must be re-evaluated and for which
	// keys. Sources shared by several rules are deduplicated by the engine.
	Sources() []Source
	// Gather reads everything the rule needs for key, and returns it as an
	// Evaluator. It is the only method that may read cluster state.
	Gather(ctx context.Context, rd Reader, key Key) (Evaluator, error)
}

// Evaluator is a rule's gathered data plus its decision logic. Should and Do
// must be pure functions of the gathered data: no reads, no clocks (use
// Reader.Now in Gather), no goroutines.
type Evaluator interface {
	// Should reports whether the rule applies to the gathered data.
	Should(ctx context.Context) (bool, error)
	// Do records the rule's desired outcome. It is only called if Should
	// returned true.
	Do(ctx context.Context, fx *Effects) error
}

// Func is a [Rule] assembled from functions over a gathered data type T.
// Most rules are "fetch T, test T, record effects from T", and Func lets a
// controller declare them as a table instead of one type per rule. Rules
// that need errors from their decision logic implement [Rule] directly.
type Func[T any] struct {
	// Name is returned by ID.
	Name string
	// Watches is returned by Sources.
	Watches []Source
	// Fetch is the rule's Gather: the only place that may read.
	Fetch func(ctx context.Context, rd Reader, key Key) (T, error)
	// When is the rule's Should; pure over the fetched data.
	When func(T) bool
	// Then is the rule's Do; pure over the fetched data.
	Then func(T, *Effects)
}

func (f Func[T]) ID() string        { return f.Name }
func (f Func[T]) Sources() []Source { return f.Watches }

func (f Func[T]) Gather(ctx context.Context, rd Reader, key Key) (Evaluator, error) {
	data, err := f.Fetch(ctx, rd, key)
	if err != nil {
		return nil, err
	}
	return funcEvaluator[T]{f: f, data: data}, nil
}

// Evaluate returns the Evaluator for already-fetched data. Tests use it to
// exercise When/Then without a Reader.
func (f Func[T]) Evaluate(data T) Evaluator { return funcEvaluator[T]{f: f, data: data} }

type funcEvaluator[T any] struct {
	f    Func[T]
	data T
}

func (e funcEvaluator[T]) Should(context.Context) (bool, error) { return e.f.When(e.data), nil }

func (e funcEvaluator[T]) Do(_ context.Context, fx *Effects) error {
	e.f.Then(e.data, fx)
	return nil
}

// Reader is a read-only, cache-backed client. Within one reconcile, reads
// are repeatable: the first read of an object or list is memoized and every
// later read (by any rule) returns the same result, so all rules decide on
// one consistent snapshot. Reading a type that no Source watches is an
// error, which keeps Sources honest.
type Reader interface {
	client.Reader
	// Now is the reconcile's observation time; it is fixed for the whole
	// reconcile so every rule sees the same instant.
	Now() time.Time
}

// Source declares a trigger: when it fires, the listed keys are enqueued for
// re-evaluation. Construct with [Watch] or [FromChannel].
type Source interface{ isSource() }

// WatchSource enqueues keys when objects of a type are added, updated or
// deleted in the shared informer cache. On update, both the old and the new
// object are mapped, so a key that stops matching is still re-evaluated.
type WatchSource struct {
	// Object is a prototype of the watched type, e.g. &corev1.Pod{}.
	Object client.Object
	// Filter, if non-nil, drops events for objects it returns false for.
	Filter func(client.Object) bool
	// Keys maps an object to the keys it affects.
	Keys func(client.Object) []Key
}

func (WatchSource) isSource() {}

// Watch returns a Source that watches objects of obj's type.
func Watch(obj client.Object, filter func(client.Object) bool, keys func(client.Object) []Key) Source {
	return WatchSource{Object: obj, Filter: filter, Keys: keys}
}

// ChannelSource enqueues keys for each value received from a channel, e.g. a
// ticker or an external event stream. Values only select keys; rules never
// see them.
type ChannelSource struct {
	// ID is the channel itself; channel sources with the same ID are
	// deduplicated, so two rules can share one channel without splitting
	// its values between them.
	ID any
	// Run receives from the channel until ctx is done or the channel is
	// closed, calling enqueue for every key. enqueue never blocks.
	Run func(ctx context.Context, enqueue func(Key))
}

func (ChannelSource) isSource() {}

// FromChannel returns a Source that enqueues keys(v) for every v received
// from ch.
func FromChannel[T any](ch <-chan T, keys func(T) []Key) Source {
	return ChannelSource{
		ID: ch,
		Run: func(ctx context.Context, enqueue func(Key)) {
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-ch:
					if !ok {
						return
					}
					for _, k := range keys(v) {
						enqueue(k)
					}
				}
			}
		},
	}
}

// Index declares a cache field index, queried by rules with
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

// Controller is a complete declarative controller.
type Controller struct {
	// Name names the controller (logs, metrics, Events source).
	Name string
	// FieldManager is the server-side apply field manager. It defines which
	// fields the controller owns.
	FieldManager string
	// Rules are evaluated in order for every key.
	Rules []Rule
	// Indexes are registered on the cache before it starts.
	Indexes []Index
	// Owns lists, for a key, the objects whose fields the controller owns.
	// Every reconcile, each owned object receives exactly the merged Apply of
	// all rules that fired, or an empty apply if none asserted anything,
	// which releases every field the controller owned on it. Applying to an
	// object not listed here is an error.
	Owns func(ctx context.Context, rd Reader, key Key) ([]Ref, error)
	// ManagedTaints lists, for a key, the taints the controller owns. A
	// managed taint is present iff some rule asserted it via Effects.Taint.
	ManagedTaints func(key Key) []ManagedTaint
}

// Precondition is a live check run by the engine against the API server
// (bypassing caches) right before any effect is applied. If any
// precondition fails, nothing is applied and the key is retried. Use them
// to guard writes that are expensive to get wrong on stale cache data.
type Precondition interface {
	fmt.Stringer
	// Check returns nil if the precondition holds, an error wrapping
	// ErrPreconditionFailed if it does not, or any other error if it could
	// not be evaluated.
	Check(ctx context.Context, live client.Reader) error
}

// ErrPreconditionFailed is wrapped by Precondition.Check when a precondition
// does not hold.
var ErrPreconditionFailed = errors.New("precondition failed")

// Effects collects one reconcile's desired outcome from all rules. It never
// performs I/O; the engine turns it into API calls after every rule ran.
type Effects struct {
	rule    string
	applies []RecordedApply
	taints  []RecordedTaint
	deletes []RecordedDelete
	pres    []RecordedPrecondition
	wake    *RecordedWake
}

// RecordedApply is an Apply call and the rule that made it.
type RecordedApply struct {
	Rule   string
	Config runtime.ApplyConfiguration
}

// RecordedTaint is a Taint call and the rule that made it.
type RecordedTaint struct {
	Rule  string
	Node  string
	Taint corev1.Taint
}

// RecordedDelete is a Delete call and the rule that made it.
type RecordedDelete struct {
	Rule   string
	Object client.Object
}

// RecordedPrecondition is a Require call and the rule that made it.
type RecordedPrecondition struct {
	Rule         string
	Precondition Precondition
}

// RecordedWake is the earliest WakeAt call and the rule that made it.
type RecordedWake struct {
	Rule string
	At   time.Time
}

// ForRule returns the Effects view that attributes records to rule. The
// engine calls it before each rule's Do.
func (fx *Effects) ForRule(rule string) *Effects {
	fx.rule = rule
	return fx
}

// Apply asserts desired owned state, as a native apply configuration (e.g.
// corev1ac.Node(name).WithLabels(...)). Configurations from all rules for
// the same object are merged; two rules setting the same field to different
// values is a conflict. Do not set metadata.uid or resourceVersion: the
// engine pins the incarnation the rules observed.
func (fx *Effects) Apply(ac runtime.ApplyConfiguration) {
	fx.applies = append(fx.applies, RecordedApply{Rule: fx.rule, Config: ac})
}

// Taint asserts that a managed taint is present on node.
func (fx *Effects) Taint(node string, t corev1.Taint) {
	fx.taints = append(fx.taints, RecordedTaint{Rule: fx.rule, Node: node, Taint: t})
}

// Delete deletes obj, which must be an object returned by the Reader. The
// deletion is preconditioned on obj's UID, so it never hits a newer object
// with the same name. Use it for objects the controller does not own.
func (fx *Effects) Delete(obj client.Object) {
	fx.deletes = append(fx.deletes, RecordedDelete{Rule: fx.rule, Object: obj})
}

// Require adds a live precondition guarding every effect of this reconcile.
func (fx *Effects) Require(p Precondition) {
	fx.pres = append(fx.pres, RecordedPrecondition{Rule: fx.rule, Precondition: p})
}

// WakeAt asks for the key to be re-evaluated at t even if nothing changes.
// The earliest request wins.
func (fx *Effects) WakeAt(t time.Time) {
	if fx.wake == nil || t.Before(fx.wake.At) {
		fx.wake = &RecordedWake{Rule: fx.rule, At: t}
	}
}

// Applies returns the recorded Apply calls, in order.
func (fx *Effects) Applies() []RecordedApply { return fx.applies }

// Taints returns the recorded Taint calls, in order.
func (fx *Effects) Taints() []RecordedTaint { return fx.taints }

// Deletes returns the recorded Delete calls, in order.
func (fx *Effects) Deletes() []RecordedDelete { return fx.deletes }

// Preconditions returns the recorded Require calls, in order.
func (fx *Effects) Preconditions() []RecordedPrecondition { return fx.pres }

// Wake returns the earliest WakeAt request, or nil.
func (fx *Effects) Wake() *RecordedWake { return fx.wake }
