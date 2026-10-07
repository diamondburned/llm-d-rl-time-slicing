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
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

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

// memoReader implements rulekit.Reader by wrapping a cached client.Reader
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

	stored := obj.DeepCopyObject().(client.Object)
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

	stored := list.DeepCopyObject().(client.ObjectList)
	m.listMemo[lk] = listResult{list: stored}
	return nil
}

func (m *memoReader) observed(ctx context.Context, ref rulekit.Ref) (client.Object, bool, error) {
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
		return res.obj.DeepCopyObject().(client.Object), true, nil
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
	return clientObj.DeepCopyObject().(client.Object), true, nil
}
