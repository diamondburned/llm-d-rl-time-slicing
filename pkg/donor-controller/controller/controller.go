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

package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Options configures the controller loop.
type Options struct {
	Workers      int
	ResyncPeriod time.Duration // informer resync; level-triggered safety net
	DryRun       bool
	Clock        func() time.Time // default time.Now; tests override
}

// Controller coordinates informers, observation, pure policy decisions, and execution.
type Controller struct {
	client kubernetes.Interface
	cfg    policy.Config
	rules  []policy.Rule
	opts   Options

	queue    workqueue.TypedRateLimitingInterface[string]
	executor *Executor

	nodeFactory     informers.SharedInformerFactory
	donorPodFactory informers.SharedInformerFactory
	guestPodFactory informers.SharedInformerFactory

	nodeInformer     cache.SharedIndexInformer
	donorPodInformer cache.SharedIndexInformer
	guestPodInformer cache.SharedIndexInformer

	nodeLister      corev1listers.NodeLister
	donorPodIndexer cache.Indexer
	guestPodIndexer cache.Indexer
}

// New constructs a new donor Controller.
func New(client kubernetes.Interface, cfg policy.Config, rules []policy.Rule, opts Options) (*Controller, error) {
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	if opts.ResyncPeriod <= 0 {
		opts.ResyncPeriod = 10 * time.Minute
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}

	nodeFactory := informers.NewSharedInformerFactory(client, opts.ResyncPeriod)
	nodeInformer := nodeFactory.Core().V1().Nodes()

	donorPodFactory := informers.NewSharedInformerFactoryWithOptions(client, opts.ResyncPeriod,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = policy.DonorLabel + "=true"
		}),
	)
	donorPodInformer := donorPodFactory.Core().V1().Pods()

	guestPodFactory := informers.NewSharedInformerFactoryWithOptions(client, opts.ResyncPeriod,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = policy.GuestLabel + "=true"
		}),
	)
	guestPodInformer := guestPodFactory.Core().V1().Pods()

	indexByNodeName := func(obj any) ([]string, error) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod == nil || pod.Spec.NodeName == "" {
			return nil, nil
		}
		return []string{pod.Spec.NodeName}, nil
	}

	if err := donorPodInformer.Informer().AddIndexers(cache.Indexers{"spec.nodeName": indexByNodeName}); err != nil {
		return nil, fmt.Errorf("add spec.nodeName indexer to donor pod informer: %w", err)
	}
	if err := guestPodInformer.Informer().AddIndexers(cache.Indexers{"spec.nodeName": indexByNodeName}); err != nil {
		return nil, fmt.Errorf("add spec.nodeName indexer to guest pod informer: %w", err)
	}

	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{
			Name: "donor-controller",
		},
	)

	executor := &Executor{
		Client:         client,
		IsolationTaint: cfg.IsolationTaint,
		DryRun:         opts.DryRun,
		Log:            slog.Default(),
	}

	c := &Controller{
		client:           client,
		cfg:              cfg,
		rules:            rules,
		opts:             opts,
		queue:            queue,
		executor:         executor,
		nodeFactory:      nodeFactory,
		donorPodFactory:  donorPodFactory,
		guestPodFactory:  guestPodFactory,
		nodeInformer:     nodeInformer.Informer(),
		donorPodInformer: donorPodInformer.Informer(),
		guestPodInformer: guestPodInformer.Informer(),
		nodeLister:       nodeInformer.Lister(),
		donorPodIndexer:  donorPodInformer.Informer().GetIndexer(),
		guestPodIndexer:  guestPodInformer.Informer().GetIndexer(),
	}

	return c, nil
}

// HasSynced reports whether all informer caches have synced.
func (c *Controller) HasSynced() bool {
	return c.nodeInformer.HasSynced() &&
		c.donorPodInformer.HasSynced() &&
		c.guestPodInformer.HasSynced()
}

// Run starts the controller, blocking until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	triggers := Triggers(c.nodeInformer, c.donorPodInformer, c.guestPodInformer)
	if err := RegisterTriggers(c.queue, triggers); err != nil {
		return fmt.Errorf("register triggers: %w", err)
	}

	c.nodeFactory.Start(ctx.Done())
	c.donorPodFactory.Start(ctx.Done())
	c.guestPodFactory.Start(ctx.Done())

	// Workers must NOT start before sync: an empty node cache would make it look like
	// every host died, causing W4 to delete every virtual node.
	if !cache.WaitForCacheSync(ctx.Done(), c.HasSynced) {
		return fmt.Errorf("failed to wait for informer caches to sync")
	}

	var wg sync.WaitGroup
	for i := 0; i < c.opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c.processNextWorkItem(ctx) {
			}
		}()
	}

	<-ctx.Done()
	c.queue.ShutDown()
	wg.Wait()
	return nil
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(ctx, key)
	if err != nil {
		slog.ErrorContext(ctx, "Reconcile failed", "host", key, "error", err)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *Controller) reconcile(ctx context.Context, host string) error {
	obs, err := c.observe(host, c.opts.Clock())
	if err != nil {
		return fmt.Errorf("observe %s: %w", host, err)
	}
	if obs == nil {
		return nil
	}

	plan, err := policy.Decide(c.rules, obs)
	if err != nil {
		slog.ErrorContext(ctx, "Policy decide failed", "host", host, "error", err)
		return err
	}

	if !plan.IsNoop(obs) {
		slog.InfoContext(ctx, "Executing plan", "host", host, "plan", plan.String())
	}

	if err := c.executor.Execute(ctx, obs, plan); err != nil {
		return fmt.Errorf("execute plan for %s: %w", host, err)
	}

	if plan.RequeueAfter > 0 {
		c.queue.AddAfter(host, plan.RequeueAfter)
	}

	return nil
}
