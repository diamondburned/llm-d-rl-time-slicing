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

package ctrl

import (
	"context"
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type reconciler struct {
	name          string
	fieldManager  string
	rules         []rulekit.Rule
	owns          func(context.Context, rulekit.Reader, rulekit.Key) ([]rulekit.Ref, error)
	managedTaints func(rulekit.Key) []rulekit.ManagedTaint
	declaredGVKs  map[schema.GroupVersionKind]bool
	scheme        *runtime.Scheme
	clock         func() time.Time
	cacheReader   client.Reader
	exec          *executor
}

func newReconciler(
	c rulekit.Controller,
	clock func() time.Time,
	scheme *runtime.Scheme,
	cacheReader client.Reader,
	exec *executor,
	declaredGVKs map[schema.GroupVersionKind]bool,
) *reconciler {
	return &reconciler{
		name:          c.Name,
		fieldManager:  c.FieldManager,
		rules:         c.Rules,
		owns:          c.Owns,
		managedTaints: c.ManagedTaints,
		declaredGVKs:  declaredGVKs,
		scheme:        scheme,
		clock:         clock,
		cacheReader:   cacheReader,
		exec:          exec,
	}
}

func (r *reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	now := r.clock()
	rd := newMemoReader(r.cacheReader, now, r.declaredGVKs, r.scheme)
	fx := &rulekit.Effects{}
	var fired []string
	for _, rule := range r.rules {
		ev, err := rule.Gather(ctx, rd, req.NamespacedName)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("rule %s gather: %w", rule.ID(), err)
		}
		ok, err := ev.Should(ctx)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("rule %s should: %w", rule.ID(), err)
		}
		if ok {
			fired = append(fired, rule.ID())
			if err := ev.Do(ctx, fx.ForRule(rule.ID())); err != nil {
				return reconcile.Result{}, fmt.Errorf("rule %s do: %w", rule.ID(), err)
			}
		}
	}

	owns, err := r.owns(ctx, rd, req.NamespacedName)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("owns: %w", err)
	}

	var taints []rulekit.ManagedTaint
	if r.managedTaints != nil {
		taints = r.managedTaints(req.NamespacedName)
	}

	plan, err := rulekit.BuildPlan(fx, fired, owns, taints)
	if err != nil {
		log := log.FromContext(ctx)
		log.Error(err, "policy conflict or plan build failure", "key", req.NamespacedName)
		return reconcile.Result{}, err
	}

	return r.exec.Execute(ctx, rd, plan)
}
