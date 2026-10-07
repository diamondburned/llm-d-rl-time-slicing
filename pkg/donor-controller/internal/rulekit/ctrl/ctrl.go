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

// Package ctrl is the only code in the controller that writes to the API server;
// it adapts rulekit.Controller onto controller-runtime and must stay small;
// Reconcile is generic and must never grow controller-specific logic.
package ctrl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Options configures controller setup.
type Options struct {
	DryRun                  bool
	MaxConcurrentReconciles int              // default 2
	Clock                   func() time.Time // default time.Now
}

// Setup registers and builds the controller using controller-runtime.
func Setup(mgr manager.Manager, c rulekit.Controller, opts Options) error {
	if c.Name == "" {
		return errors.New("rulekit/ctrl: controller Name must not be empty")
	}
	if c.FieldManager == "" {
		return errors.New("rulekit/ctrl: controller FieldManager must not be empty")
	}
	if c.Owns == nil {
		return errors.New("rulekit/ctrl: controller Owns must not be nil")
	}

	ctx := context.Background()
	for _, idx := range c.Indexes {
		idxObj := idx.Object
		idxField := idx.Field
		idxExtract := idx.Extract
		extract := func(o client.Object) []string {
			return idxExtract(o)
		}
		if err := mgr.GetFieldIndexer().IndexField(ctx, idxObj, idxField, extract); err != nil {
			return fmt.Errorf("rulekit/ctrl: index field %s on %T: %w", idxField, idxObj, err)
		}
	}

	var watchSources []rulekit.WatchSource
	var channelSources []rulekit.ChannelSource
	seenChannels := make(map[any]bool)
	declaredGVKs := make(map[schema.GroupVersionKind]bool)

	for _, rule := range c.Rules {
		for _, src := range rule.Sources() {
			switch s := src.(type) {
			case rulekit.WatchSource:
				watchSources = append(watchSources, s)
				gvk, err := apiutil.GVKForObject(s.Object, mgr.GetScheme())
				if err != nil {
					return fmt.Errorf("rulekit/ctrl: gvk for object %T: %w", s.Object, err)
				}
				declaredGVKs[gvk] = true
			case rulekit.ChannelSource:
				if s.ID != nil && !seenChannels[s.ID] {
					seenChannels[s.ID] = true
					channelSources = append(channelSources, s)
				}
			}
		}
	}

	maxConcurrent := opts.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = 2
	}

	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}

	ctrlOptions := controller.Options{
		MaxConcurrentReconciles: maxConcurrent,
	}

	blder := builder.ControllerManagedBy(mgr).
		Named(c.Name).
		WithOptions(ctrlOptions)

	for _, ws := range watchSources {
		watchSrc := ws
		mapFn := func(ctx context.Context, obj client.Object) []reconcile.Request {
			keys := watchSrc.Keys(obj)
			reqs := make([]reconcile.Request, len(keys))
			for i, k := range keys {
				reqs[i] = reconcile.Request{NamespacedName: k}
			}
			return reqs
		}
		pred := newWatchPredicate(watchSrc.Filter)
		blder = blder.WatchesRawSource(source.Kind(
			mgr.GetCache(),
			watchSrc.Object,
			handler.EnqueueRequestsFromMapFunc(mapFn),
			pred,
		))
	}

	for _, cs := range channelSources {
		chanSrc := cs
		blder = blder.WatchesRawSource(source.TypedFunc[reconcile.Request](func(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
			go chanSrc.Run(ctx, func(k rulekit.Key) {
				q.Add(reconcile.Request{NamespacedName: k})
			})
			return nil
		}))
	}

	exec := newExecutor(c.FieldManager, opts.DryRun, mgr.GetAPIReader(), mgr.GetClient(), mgr.GetEventRecorderFor(c.Name))
	rec := newReconciler(c, clock, mgr.GetScheme(), mgr.GetClient(), exec, declaredGVKs)

	return blder.Complete(rec)
}

func newWatchPredicate(filter func(client.Object) bool) predicate.Funcs {
	if filter == nil {
		return predicate.Funcs{
			CreateFunc:  func(e event.CreateEvent) bool { return true },
			DeleteFunc:  func(e event.DeleteEvent) bool { return true },
			GenericFunc: func(e event.GenericEvent) bool { return true },
			UpdateFunc:  func(e event.UpdateEvent) bool { return true },
		}
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return filter(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return filter(e.Object)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return filter(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return filter(e.ObjectOld) || filter(e.ObjectNew)
		},
	}
}
