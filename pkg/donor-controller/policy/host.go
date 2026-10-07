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

package policy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IndexPodNodeName is the field index for pod.Spec.NodeName.
const IndexPodNodeName = "spec.nodeName"

// PodNodeNameIndex registers an index on Pods by spec.nodeName.
func PodNodeNameIndex() rulekit.Index {
	return rulekit.Index{
		Object: &corev1.Pod{},
		Field:  IndexPodNodeName,
		Extract: func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		},
	}
}

// hostData is the pure gathered data snapshot for one real host.
type hostData struct {
	// Host is the real node name; it is the reconcile key.
	Host string
	// Now is the reconcile observation time.
	Now time.Time

	// Node is the real node, or nil if absent from the cluster.
	Node *corev1.Node
	// VNode is the virtual node named VirtualNodeName(Host), or nil if absent
	// or not labeled with LabelGuest=true.
	VNode *corev1.Node
	// Donors are non-terminal pods bound to Host labeled LabelDonor=true.
	Donors []*corev1.Pod
	// Guests are non-terminal pods bound to VirtualNodeName(Host) labeled LabelGuest=true.
	Guests []*corev1.Pod

	// OwnedLabels is derived once from Node.ManagedFields via corev1ac.ExtractNode.
	OwnedLabels map[string]string
	// OwnedAnnotations is derived once from Node.ManagedFields via corev1ac.ExtractNode.
	OwnedAnnotations map[string]string
	// HasIsolationTaint is derived: Node has a taint matching cfg.IsolationTaint Key and Effect.
	HasIsolationTaint bool
	// IsVirtual is true if the reconciled key is itself a virtual node (Node carries LabelGuest=true).
	IsVirtual bool
}

// gatherHost reads all data needed for reconciling key from the cache-backed Reader.
func gatherHost(ctx context.Context, rd rulekit.Reader, key rulekit.Key, cfg Config) (*hostData, error) {
	host := key.Name

	// 1. Get real node.
	var node corev1.Node
	var nodePtr *corev1.Node
	if err := rd.Get(ctx, client.ObjectKey{Name: host}, &node); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get node %s: %w", host, err)
		}
	} else {
		nodePtr = &node
	}

	// 2. If this key is itself a virtual node, return early with IsVirtual = true
	// so every rule's Should returns false.
	if nodePtr != nil && nodePtr.Labels != nil && nodePtr.Labels[wellknown.LabelGuest] == wellknown.LabelValueTrue {
		return &hostData{
			Host:      host,
			Now:       rd.Now(),
			Node:      nodePtr,
			IsVirtual: true,
		}, nil
	}

	// 3. Get virtual node (vk-<host>). Keep only if it carries LabelGuest=true.
	vnName := wellknown.VirtualNodeName(host)
	var vn corev1.Node
	var vnPtr *corev1.Node
	if err := rd.Get(ctx, client.ObjectKey{Name: vnName}, &vn); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get virtual node %s: %w", vnName, err)
		}
	} else if vn.Labels != nil && vn.Labels[wellknown.LabelGuest] == wellknown.LabelValueTrue {
		vnPtr = &vn
	}

	// 4. Donors: matching spec.nodeName == host and LabelDonor == true, excluding terminal pods.
	var donorPodList corev1.PodList
	if err := rd.List(ctx, &donorPodList,
		client.MatchingFields{IndexPodNodeName: host},
		client.MatchingLabels{wellknown.LabelDonor: wellknown.LabelValueTrue},
	); err != nil {
		return nil, fmt.Errorf("list donor pods on %s: %w", host, err)
	}
	var donors []*corev1.Pod
	for i := range donorPodList.Items {
		pod := &donorPodList.Items[i]
		if pod.Spec.NodeName == host &&
			pod.Labels != nil && pod.Labels[wellknown.LabelDonor] == wellknown.LabelValueTrue &&
			pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			donors = append(donors, pod)
		}
	}

	// 5. Guests: matching spec.nodeName == vnName and LabelGuest == true, excluding terminal pods.
	var guestPodList corev1.PodList
	if err := rd.List(ctx, &guestPodList,
		client.MatchingFields{IndexPodNodeName: vnName},
		client.MatchingLabels{wellknown.LabelGuest: wellknown.LabelValueTrue},
	); err != nil {
		return nil, fmt.Errorf("list guest pods on %s: %w", vnName, err)
	}
	var guests []*corev1.Pod
	for i := range guestPodList.Items {
		pod := &guestPodList.Items[i]
		if pod.Spec.NodeName == vnName &&
			pod.Labels != nil && pod.Labels[wellknown.LabelGuest] == wellknown.LabelValueTrue &&
			pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			guests = append(guests, pod)
		}
	}

	// 6. OwnedLabels, OwnedAnnotations: derived once from Node.managedFields.
	var ownedLabels, ownedAnnotations map[string]string
	if nodePtr != nil {
		ac, err := corev1ac.ExtractNode(nodePtr, wellknown.DonorControllerFieldManager)
		if err != nil {
			return nil, fmt.Errorf("extract node %s owned fields: %w", host, err)
		}
		if ac != nil && ac.ObjectMetaApplyConfiguration != nil {
			if ac.Labels != nil {
				ownedLabels = make(map[string]string, len(ac.Labels))
				for k, v := range ac.Labels {
					ownedLabels[k] = v
				}
			}
			if ac.Annotations != nil {
				ownedAnnotations = make(map[string]string, len(ac.Annotations))
				for k, v := range ac.Annotations {
					ownedAnnotations[k] = v
				}
			}
		}
	}

	// 7. HasIsolationTaint: true if cfg.IsolationTaint != nil and Node has a taint with same Key and Effect.
	hasIsolationTaint := false
	if cfg.IsolationTaint != nil && nodePtr != nil {
		for _, t := range nodePtr.Spec.Taints {
			if t.Key == cfg.IsolationTaint.Key && t.Effect == cfg.IsolationTaint.Effect {
				hasIsolationTaint = true
				break
			}
		}
	}

	return &hostData{
		Host:              host,
		Now:               rd.Now(),
		Node:              nodePtr,
		VNode:             vnPtr,
		Donors:            donors,
		Guests:            guests,
		OwnedLabels:       ownedLabels,
		OwnedAnnotations:  ownedAnnotations,
		HasIsolationTaint: hasIsolationTaint,
		IsVirtual:         false,
	}, nil
}

// donorsPresent reports whether any non-terminal donor pods are bound to Host.
func (d *hostData) donorsPresent() bool {
	return len(d.Donors) > 0
}

// eraActive reports whether the controller currently owns the donor label on Node.
func (d *hostData) eraActive() bool {
	return d.OwnedLabels != nil && d.OwnedLabels[wellknown.LabelDonor] == wellknown.LabelValueTrue
}

// vnBusy reports whether a virtual node exists or guest pods remain.
func (d *hostData) vnBusy() bool {
	return d.VNode != nil || len(d.Guests) > 0
}

// empty reports whether no donors are present and the virtual node is not busy.
func (d *hostData) empty() bool {
	return !d.donorsPresent() && !d.vnBusy()
}

// ownedGroups returns all group label keys currently owned on Node.
func (d *hostData) ownedGroups() []string {
	if d.OwnedLabels == nil {
		return nil
	}
	var groups []string
	for k := range d.OwnedLabels {
		if strings.HasPrefix(k, wellknown.NodeGroupLabelPrefix) {
			groups = append(groups, k)
		}
	}
	return groups
}

// donorGroups returns qualified group label keys derived from donor pods bound to Node.
func (d *hostData) donorGroups() []string {
	var groups []string
	for _, pod := range d.Donors {
		if pod == nil || pod.Labels == nil {
			continue
		}
		g := pod.Labels[wellknown.LabelGroup]
		if g == "" {
			continue
		}
		key := wellknown.NodeGroupLabel(g)
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			continue
		}
		groups = append(groups, key)
	}
	return groups
}

// idleSince returns the parsed idle timestamp from owned annotations, or Now truncated to seconds.
func (d *hostData) idleSince() time.Time {
	if d.OwnedAnnotations != nil {
		if raw, ok := d.OwnedAnnotations[wellknown.AnnotationIdleSince]; ok {
			if t, err := time.Parse(time.RFC3339, raw); err == nil {
				return t
			}
		}
	}
	return d.Now.Truncate(time.Second)
}

// keepShared asserts the donor label, all accumulated group labels, and isolation taint if configured.
func keepShared(d *hostData, fx *rulekit.Effects, cfg Config) {
	labels := map[string]string{
		wellknown.LabelDonor: wellknown.LabelValueTrue,
	}
	for _, k := range d.ownedGroups() {
		labels[k] = wellknown.LabelValueTrue
	}
	for _, k := range d.donorGroups() {
		labels[k] = wellknown.LabelValueTrue
	}
	fx.Apply(corev1ac.Node(d.Host).WithLabels(labels))
	if cfg.IsolationTaint != nil {
		fx.Taint(d.Host, *cfg.IsolationTaint)
	}
}

func podHostKeys(obj client.Object) []rulekit.Key {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil || pod.Spec.NodeName == "" {
		return nil
	}
	keys := []rulekit.Key{{Name: pod.Spec.NodeName}}
	if strings.HasPrefix(pod.Spec.NodeName, wellknown.VirtualNodePrefix) {
		trimmed := strings.TrimPrefix(pod.Spec.NodeName, wellknown.VirtualNodePrefix)
		if trimmed != "" {
			keys = append(keys, rulekit.Key{Name: trimmed})
		}
	}
	return keys
}

func nodeHostKeys(obj client.Object) []rulekit.Key {
	node, ok := obj.(*corev1.Node)
	if !ok || node == nil || node.Name == "" {
		return nil
	}
	keys := []rulekit.Key{{Name: node.Name}}
	if strings.HasPrefix(node.Name, wellknown.VirtualNodePrefix) {
		trimmed := strings.TrimPrefix(node.Name, wellknown.VirtualNodePrefix)
		if trimmed != "" {
			keys = append(keys, rulekit.Key{Name: trimmed})
		}
	}
	return keys
}

func podFilter(obj client.Object) bool {
	if obj == nil {
		return false
	}
	labels := obj.GetLabels()
	if labels == nil {
		return false
	}
	return labels[wellknown.LabelDonor] == wellknown.LabelValueTrue ||
		labels[wellknown.LabelGuest] == wellknown.LabelValueTrue
}

// hostSources returns the rulekit sources that trigger evaluation of host keys.
func hostSources() []rulekit.Source {
	return []rulekit.Source{
		rulekit.Watch(&corev1.Pod{}, podFilter, podHostKeys),
		rulekit.Watch(&corev1.Node{}, nil, nodeHostKeys),
	}
}

// TrimPod is a cache transform for pods. The controller caches all pods
// because label selectors cannot express (donor=true OR guest=true).
// TrimPod retains only the fields rules may rely on: TypeMeta, metadata
// (Name, Namespace, UID, Labels, ResourceVersion, DeletionTimestamp,
// OwnerReferences), Spec.NodeName, and Status.Phase. Rules may only rely on
// these fields.
func TrimPod(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	trimmed := &corev1.Pod{
		TypeMeta: pod.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name:              pod.Name,
			Namespace:         pod.Namespace,
			UID:               pod.UID,
			Labels:            pod.Labels,
			ResourceVersion:   pod.ResourceVersion,
			DeletionTimestamp: pod.DeletionTimestamp,
			OwnerReferences:   pod.OwnerReferences,
		},
		Spec: corev1.PodSpec{
			NodeName: pod.Spec.NodeName,
		},
		Status: corev1.PodStatus{
			Phase: pod.Status.Phase,
		},
	}
	return trimmed, nil
}
