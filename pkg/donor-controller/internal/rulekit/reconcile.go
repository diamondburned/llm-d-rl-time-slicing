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

package rulekit

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
func Setup[Data any](mgr manager.Manager, ctrl Controller[Data], opts Options) error {
	if ctrl.Name == "" {
		return errors.New("rulekit: controller Name must not be empty")
	}
	if ctrl.FieldManager == "" {
		return errors.New("rulekit: controller FieldManager must not be empty")
	}
	if ctrl.Owns == nil {
		return errors.New("rulekit: controller Owns must not be nil")
	}
	if ctrl.Fetch == nil {
		return errors.New("rulekit: controller Fetch must not be nil")
	}

	ctx := context.Background()
	for _, idx := range ctrl.Indexes {
		extract := func(o client.Object) []string {
			return idx.Extract(o)
		}
		if err := mgr.GetFieldIndexer().IndexField(ctx, idx.Object, idx.Field, extract); err != nil {
			return fmt.Errorf("rulekit: index field %s on %T: %w", idx.Field, idx.Object, err)
		}
	}

	declaredGVKs := make(map[schema.GroupVersionKind]bool)
	for _, ws := range ctrl.Watches {
		gvk, err := apiutil.GVKForObject(ws.Object, mgr.GetScheme())
		if err != nil {
			return fmt.Errorf("rulekit: gvk for object %T: %w", ws.Object, err)
		}
		declaredGVKs[gvk] = true
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
		Named(ctrl.Name).
		WithOptions(ctrlOptions)

	for _, ws := range ctrl.Watches {
		mapFn := func(ctx context.Context, obj client.Object) []reconcile.Request {
			keys := ws.Keys(obj)
			reqs := make([]reconcile.Request, len(keys))
			for i, k := range keys {
				reqs[i] = reconcile.Request{NamespacedName: k}
			}
			return reqs
		}
		pred := newWatchPredicate(ws.Filter)
		blder = blder.WatchesRawSource(source.Kind(
			mgr.GetCache(),
			ws.Object,
			handler.EnqueueRequestsFromMapFunc(mapFn),
			pred,
		))
	}

	exec := newExecutor(ctrl.FieldManager, opts.DryRun, mgr.GetAPIReader(), mgr.GetClient(), mgr.GetEventRecorderFor(ctrl.Name))
	rec := &reconciler[Data]{
		c:            ctrl,
		clock:        clock,
		declaredGVKs: declaredGVKs,
		scheme:       mgr.GetScheme(),
		cacheReader:  mgr.GetClient(),
		exec:         exec,
	}

	return blder.Complete(rec)
}

func newWatchPredicate(filter func(client.Object) bool) predicate.Funcs {
	if filter == nil {
		return predicate.Funcs{}
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return filter(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return filter(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return filter(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			// an object that stops matching must still re-evaluate its keys
			return filter(e.ObjectOld) || filter(e.ObjectNew)
		},
	}
}

type reconciler[T any] struct {
	c            Controller[T]
	clock        func() time.Time
	declaredGVKs map[schema.GroupVersionKind]bool
	scheme       *runtime.Scheme
	cacheReader  client.Reader
	exec         *executor
}

func (r *reconciler[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	now := r.clock()
	rd := newMemoReader(r.cacheReader, now, r.declaredGVKs, r.scheme)

	data, err := r.c.Fetch(ctx, rd, req.NamespacedName)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("fetch %s: %w", req.NamespacedName, err)
	}

	plan, err := r.c.Evaluate(req.NamespacedName, data)
	if err != nil {
		logger := log.FromContext(ctx)
		logger.Error(err, "policy conflict or plan build failure", "key", req.NamespacedName)
		return reconcile.Result{}, err
	}

	return r.exec.Execute(ctx, rd, plan)
}

type getKey struct {
	gvk schema.GroupVersionKind
	key types.NamespacedName
}

type getResult struct {
	obj client.Object
	err error
}

type listKey struct {
	gvk           schema.GroupVersionKind
	namespace     string
	labelSelector string
	fieldSelector string
	limit         int64
	continueStr   string
}

type listResult struct {
	list client.ObjectList
	err  error
}

// memoReader implements Reader by wrapping a cached client.Reader
// with per-reconcile memoization and undeclared GVK guards.
type memoReader struct {
	reader   client.Reader
	now      time.Time
	declared map[schema.GroupVersionKind]bool
	scheme   *runtime.Scheme

	mu       sync.Mutex
	getMemo  map[getKey]getResult
	listMemo map[listKey]listResult
}

func newMemoReader(
	reader client.Reader,
	now time.Time,
	declared map[schema.GroupVersionKind]bool,
	scheme *runtime.Scheme,
) *memoReader {
	return &memoReader{
		reader:   reader,
		now:      now,
		declared: declared,
		scheme:   scheme,
		getMemo:  make(map[getKey]getResult),
		listMemo: make(map[listKey]listResult),
	}
}

func (m *memoReader) Now() time.Time {
	return m.now
}

func (m *memoReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk, err := apiutil.GVKForObject(obj, m.scheme)
	if err != nil {
		return fmt.Errorf("rulekit/ctrl: gvk for %T: %w", obj, err)
	}

	if !m.declared[gvk] {
		return fmt.Errorf("rulekit: read of undeclared type %s; add a Watch source for it", gvk)
	}

	gk := getKey{gvk: gvk, key: key}

	m.mu.Lock()
	res, found := m.getMemo[gk]
	m.mu.Unlock()

	if found {
		if res.err != nil {
			return res.err
		}
		reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(res.obj.DeepCopyObject()).Elem())
		return nil
	}

	err = m.reader.Get(ctx, key, obj, opts...)
	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		m.getMemo[gk] = getResult{err: err}
		return err
	}

	stored, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("type %T does not implement client.Object", obj)
	}
	m.getMemo[gk] = getResult{obj: stored}
	return nil
}

func (m *memoReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listGVK, err := apiutil.GVKForObject(list, m.scheme)
	if err != nil {
		return fmt.Errorf("rulekit/ctrl: gvk for list %T: %w", list, err)
	}

	itemGVK := schema.GroupVersionKind{
		Group:   listGVK.Group,
		Version: listGVK.Version,
		Kind:    strings.TrimSuffix(listGVK.Kind, "List"),
	}

	if !m.declared[itemGVK] {
		return fmt.Errorf("rulekit: read of undeclared type %s; add a Watch source for it", itemGVK)
	}

	var listOpts client.ListOptions
	for _, opt := range opts {
		opt.ApplyToList(&listOpts)
	}

	var ls, fs string
	if listOpts.LabelSelector != nil {
		ls = listOpts.LabelSelector.String()
	}
	if listOpts.FieldSelector != nil {
		fs = listOpts.FieldSelector.String()
	}

	lk := listKey{
		gvk:           listGVK,
		namespace:     listOpts.Namespace,
		labelSelector: ls,
		fieldSelector: fs,
		limit:         listOpts.Limit,
		continueStr:   listOpts.Continue,
	}

	m.mu.Lock()
	res, found := m.listMemo[lk]
	m.mu.Unlock()

	if found {
		if res.err != nil {
			return res.err
		}
		reflect.ValueOf(list).Elem().Set(reflect.ValueOf(res.list.DeepCopyObject()).Elem())
		return nil
	}

	err = m.reader.List(ctx, list, opts...)
	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		m.listMemo[lk] = listResult{err: err}
		return err
	}

	stored, ok := list.DeepCopyObject().(client.ObjectList)
	if !ok {
		return fmt.Errorf("type %T does not implement client.ObjectList", list)
	}
	m.listMemo[lk] = listResult{list: stored}
	return nil
}

func (m *memoReader) observed(ctx context.Context, ref *Ref) (client.Object, bool, error) {
	gk := getKey{
		gvk: ref.GVK,
		key: types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name},
	}

	m.mu.Lock()
	res, found := m.getMemo[gk]
	m.mu.Unlock()

	if found {
		if res.err != nil {
			if apierrors.IsNotFound(res.err) {
				return nil, false, nil
			}
			return nil, false, res.err
		}
		copied, ok := res.obj.DeepCopyObject().(client.Object)
		if !ok {
			return nil, false, fmt.Errorf("type %T is not client.Object", res.obj)
		}
		return copied, true, nil
	}

	obj, err := m.scheme.New(ref.GVK)
	if err != nil {
		return nil, false, fmt.Errorf("scheme new %s: %w", ref.GVK, err)
	}
	clientObj, ok := obj.(client.Object)
	if !ok {
		return nil, false, fmt.Errorf("type %T is not client.Object", obj)
	}

	if err := m.Get(ctx, gk.key, clientObj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	copied, ok := clientObj.DeepCopyObject().(client.Object)
	if !ok {
		return nil, false, fmt.Errorf("type %T is not client.Object", clientObj)
	}
	return copied, true, nil
}
