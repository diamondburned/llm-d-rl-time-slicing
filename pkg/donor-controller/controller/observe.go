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
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
)

// observe builds an immutable policy.Observed snapshot from informer listers/indexers.
// It is read-only and performs no network I/O.
func (c *Controller) observe(host string, now time.Time) (*policy.Observed, error) {
	if host == "" {
		return nil, nil
	}

	// 1. Get real node.
	node, err := c.nodeLister.Get(host)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get node %s: %w", host, err)
		}
		node = nil
	}

	// 2. If this key is itself a virtual node, skip it.
	if node != nil && node.Labels != nil && node.Labels[policy.GuestLabel] == policy.LabelTrue {
		return nil, nil
	}

	// 3. Get virtual node (vk-<host>). Keep only if it carries GuestLabel=true.
	vnName := policy.VirtualNodeName(host)
	vn, err := c.nodeLister.Get(vnName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get virtual node %s: %w", vnName, err)
		}
		vn = nil
	} else if vn.Labels == nil || vn.Labels[policy.GuestLabel] != policy.LabelTrue {
		vn = nil
	}

	// 4. DonorPods: index "spec.nodeName" for key host; keep pods with DonorLabel=true and non-terminal phase.
	donorObjs, err := c.donorPodIndexer.ByIndex("spec.nodeName", host)
	if err != nil {
		return nil, fmt.Errorf("index donor pods on %s: %w", host, err)
	}
	var donorPods []*corev1.Pod
	for _, obj := range donorObjs {
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod == nil {
			continue
		}
		if pod.Labels != nil && pod.Labels[policy.DonorLabel] == policy.LabelTrue &&
			pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			donorPods = append(donorPods, pod)
		}
	}

	// 5. GuestPods: guest-pod informer indexer by "spec.nodeName" with key VirtualNodeName(host); non-terminal only.
	guestObjs, err := c.guestPodIndexer.ByIndex("spec.nodeName", vnName)
	if err != nil {
		return nil, fmt.Errorf("index guest pods on %s: %w", vnName, err)
	}
	var guestPods []*corev1.Pod
	for _, obj := range guestObjs {
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod == nil {
			continue
		}
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			guestPods = append(guestPods, pod)
		}
	}

	// 6. Owned: node metadata this controller owns on Node from managedFields.
	var owned policy.NodeMeta
	if node != nil {
		ac, err := corev1ac.ExtractNode(node, policy.FieldManager)
		if err != nil {
			return nil, fmt.Errorf("extract node %s owned fields: %w", host, err)
		}
		if ac != nil && ac.ObjectMetaApplyConfiguration != nil {
			if ac.Labels != nil {
				owned.Labels = make(map[string]string, len(ac.Labels))
				for k, v := range ac.Labels {
					owned.Labels[k] = v
				}
			}
			if ac.Annotations != nil {
				owned.Annotations = make(map[string]string, len(ac.Annotations))
				for k, v := range ac.Annotations {
					owned.Annotations[k] = v
				}
			}
		}
	}

	// 7. HasIsolationTaint: true if cfg.IsolationTaint != nil and Node has a taint with same Key and Effect.
	hasIsolationTaint := false
	if c.cfg.IsolationTaint != nil && node != nil {
		for _, t := range node.Spec.Taints {
			if t.Key == c.cfg.IsolationTaint.Key && t.Effect == c.cfg.IsolationTaint.Effect {
				hasIsolationTaint = true
				break
			}
		}
	}

	return &policy.Observed{
		Host:              host,
		Now:               now,
		Node:              node,
		VirtualNode:       vn,
		DonorPods:         donorPods,
		GuestPods:         guestPods,
		Owned:             owned,
		HasIsolationTaint: hasIsolationTaint,
	}, nil
}
