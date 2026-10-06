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
	"strings"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Trigger maps informer events for one resource to reconcile keys (real host names).
type Trigger struct {
	Name     string
	Informer cache.SharedIndexInformer
	Keys     func(obj any) []string // obj is the typed object (tombstones already unwrapped)
}

// PodHostKeys maps a pod to reconcile keys (real host names).
// If pod.Spec.NodeName == "" -> none; else [nodeName] plus, if nodeName has
// policy.VirtualNodePrefix, also strings.TrimPrefix(nodeName, prefix).
func PodHostKeys(pod *corev1.Pod) []string {
	if pod == nil || pod.Spec.NodeName == "" {
		return nil
	}
	keys := []string{pod.Spec.NodeName}
	if strings.HasPrefix(pod.Spec.NodeName, policy.VirtualNodePrefix) {
		trimmed := strings.TrimPrefix(pod.Spec.NodeName, policy.VirtualNodePrefix)
		if trimmed != "" {
			keys = append(keys, trimmed)
		}
	}
	return keys
}

// NodeHostKeys maps a node to reconcile keys (real host names).
// [node.Name] plus trimmed name if it has policy.VirtualNodePrefix.
func NodeHostKeys(node *corev1.Node) []string {
	if node == nil || node.Name == "" {
		return nil
	}
	keys := []string{node.Name}
	if strings.HasPrefix(node.Name, policy.VirtualNodePrefix) {
		trimmed := strings.TrimPrefix(node.Name, policy.VirtualNodePrefix)
		if trimmed != "" {
			keys = append(keys, trimmed)
		}
	}
	return keys
}

// Triggers defines the declarative trigger table mapping informer events to reconcile keys.
func Triggers(nodeInformer cache.SharedIndexInformer, donorPodInformer cache.SharedIndexInformer, guestPodInformer cache.SharedIndexInformer) []Trigger {
	return []Trigger{
		{
			Name:     "donor pods",
			Informer: donorPodInformer,
			Keys: func(obj any) []string {
				if pod, ok := obj.(*corev1.Pod); ok {
					return PodHostKeys(pod)
				}
				return nil
			},
		},
		{
			Name:     "guest pods",
			Informer: guestPodInformer,
			Keys: func(obj any) []string {
				if pod, ok := obj.(*corev1.Pod); ok {
					return PodHostKeys(pod)
				}
				return nil
			},
		},
		{
			Name:     "nodes",
			Informer: nodeInformer,
			Keys: func(obj any) []string {
				if node, ok := obj.(*corev1.Node); ok {
					return NodeHostKeys(node)
				}
				return nil
			},
		},
	}
}

// RegisterTriggers registers event handlers for all triggers to enqueue host keys into the queue.
func RegisterTriggers(queue workqueue.TypedRateLimitingInterface[string], triggers []Trigger) error {
	for _, t := range triggers {
		trigger := t
		_, err := trigger.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				for _, key := range trigger.Keys(obj) {
					queue.Add(key)
				}
			},
			UpdateFunc: func(oldObj, newObj any) {
				oldKeys := trigger.Keys(oldObj)
				newKeys := trigger.Keys(newObj)
				seen := make(map[string]struct{}, len(oldKeys)+len(newKeys))
				for _, key := range oldKeys {
					if _, ok := seen[key]; !ok {
						seen[key] = struct{}{}
						queue.Add(key)
					}
				}
				for _, key := range newKeys {
					if _, ok := seen[key]; !ok {
						seen[key] = struct{}{}
						queue.Add(key)
					}
				}
			},
			DeleteFunc: func(obj any) {
				if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
					obj = tombstone.Obj
				}
				for _, key := range trigger.Keys(obj) {
					queue.Add(key)
				}
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
