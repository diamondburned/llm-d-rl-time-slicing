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

package donorcontroller

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// hostState is the snapshot the rules decide on for one host. The reconcile
// key is the host's Node name.
type hostState struct {
	// Host is the host's Node name.
	Host string
	// Now is the snapshot time.
	Now time.Time
	// KeyIsVirtualNode is true if the key names a virtual node (its Node is
	// labeled virtual-node=true) rather than a host. No rule acts on such
	// keys, so pods bound to virtual nodes never share anything.
	KeyIsVirtualNode bool

	// Node is the host's Node, or nil if it does not exist.
	Node *corev1.Node
	// VirtualNodes are the virtual nodes whose owner reference names Host,
	// whether or not a Node of that name still exists.
	VirtualNodes []*corev1.Node
	// DonorPods are the non-terminal donor=true pods bound to the host,
	// oldest first.
	DonorPods []*corev1.Pod

	// Fence is the guest kubelet's GPU fence taint on the host, or nil.
	Fence *corev1.Taint
	// FenceVirtualNodeExists reports whether the virtual node the fence
	// names (its value) exists.
	FenceVirtualNodeExists bool
	// MirrorPods are the non-terminal mirror pods bound to the host,
	// including terminating ones: their GPU work may still run.
	MirrorPods []*corev1.Pod

	// GroupLock is the lock state of the group the controller labeled the
	// host with. It is GroupLockIdle if there is no lock source or no such
	// group.
	GroupLock GroupLock
	// GroupLockError says why GroupLock is GroupLockUnknown.
	GroupLockError string

	// OwnedLabels and OwnedAnnotations are the fields of Node that the
	// controller's field manager owns, extracted from managedFields.
	OwnedLabels      map[string]string
	OwnedAnnotations map[string]string
}

// fetchHostState reads the snapshot for the host named by key. locks may be
// nil.
func fetchHostState(ctx context.Context, rd rulekit.Reader, key rulekit.Key, locks GroupLockSource) (*hostState, error) {
	state := &hostState{Host: key.Name, Now: rd.Now()}

	node, err := getNodeOrNil(ctx, rd, state.Host)
	if err != nil {
		return nil, err
	}
	state.Node = node
	if node != nil && node.Labels[VirtualNodeLabel] == LabelValueTrue {
		state.KeyIsVirtualNode = true
		return state, nil
	}

	if state.VirtualNodes, err = listVirtualNodesOfHost(ctx, rd, state.Host); err != nil {
		return nil, err
	}
	if state.DonorPods, err = listNonTerminalDonorPodsOn(ctx, rd, state.Host); err != nil {
		return nil, err
	}
	if state.MirrorPods, err = listNonTerminalMirrorPodsOn(ctx, rd, state.Host); err != nil {
		return nil, err
	}
	if state.Fence = fenceTaint(node); state.Fence != nil {
		vk, err := getNodeOrNil(ctx, rd, state.Fence.Value)
		if err != nil {
			return nil, err
		}
		state.FenceVirtualNodeExists = vk != nil
	}

	if node != nil {
		owned, err := corev1ac.ExtractNode(node, DonorControllerFieldManager)
		if err != nil {
			return nil, fmt.Errorf("extract fields owned on node %s: %w", state.Host, err)
		}
		if owned != nil && owned.ObjectMetaApplyConfiguration != nil {
			state.OwnedLabels = owned.Labels
			state.OwnedAnnotations = owned.Annotations
		}
	}

	if group := state.OwnedLabels[LabelGroup]; locks != nil && group != "" {
		state.GroupLock, err = locks.GroupLock(ctx, group)
		if err != nil {
			state.GroupLock, state.GroupLockError = GroupLockUnknown, err.Error()
		}
	}
	return state, nil
}

func getNodeOrNil(ctx context.Context, rd rulekit.Reader, name string) (*corev1.Node, error) {
	var node corev1.Node
	if err := rd.Get(ctx, client.ObjectKey{Name: name}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			//nolint:nilnil // returning nil node and nil error indicates node is not found
			return nil, nil
		}
		return nil, fmt.Errorf("get node %s: %w", name, err)
	}
	return &node, nil
}

func listVirtualNodesOfHost(ctx context.Context, rd rulekit.Reader, host string) ([]*corev1.Node, error) {
	var nodes corev1.NodeList
	if err := rd.List(ctx, &nodes, client.MatchingFields{indexVirtualNodeHost: host}); err != nil {
		return nil, fmt.Errorf("list virtual nodes of host %s: %w", host, err)
	}
	var matched []*corev1.Node
	for i := range nodes.Items {
		// Filter client-side too: fake clients ignore field selectors.
		if ref := virtualNodeHostReference(&nodes.Items[i]); ref != nil && ref.Name == host {
			matched = append(matched, &nodes.Items[i])
		}
	}
	return matched, nil
}

func listNonTerminalDonorPodsOn(ctx context.Context, rd rulekit.Reader, node string) ([]*corev1.Pod, error) {
	var pods corev1.PodList
	if err := rd.List(ctx, &pods,
		client.MatchingFields{indexPodNodeName: node},
		client.MatchingLabels{LabelDonor: LabelValueTrue},
	); err != nil {
		return nil, fmt.Errorf("list donor pods on node %s: %w", node, err)
	}
	var matched []*corev1.Pod
	for i := range pods.Items {
		// Filter client-side too: fake clients ignore field selectors.
		if pod := &pods.Items[i]; isNonTerminalDonorPodOn(pod, node) {
			matched = append(matched, pod)
		}
	}
	slices.SortFunc(matched, func(a, b *corev1.Pod) int {
		return cmp.Or(
			a.CreationTimestamp.Compare(b.CreationTimestamp.Time),
			cmp.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name),
		)
	})
	return matched, nil
}

func isNonTerminalDonorPodOn(pod *corev1.Pod, node string) bool {
	return pod.Spec.NodeName == node &&
		pod.Labels[LabelDonor] == LabelValueTrue &&
		pod.Status.Phase != corev1.PodSucceeded &&
		pod.Status.Phase != corev1.PodFailed
}

func listNonTerminalMirrorPodsOn(ctx context.Context, rd rulekit.Reader, node string) ([]*corev1.Pod, error) {
	var pods corev1.PodList
	// No label selector can say "either mirror label", so filter below.
	if err := rd.List(ctx, &pods, client.MatchingFields{indexPodNodeName: node}); err != nil {
		return nil, fmt.Errorf("list mirror pods on node %s: %w", node, err)
	}
	var matched []*corev1.Pod
	for i := range pods.Items {
		if pod := &pods.Items[i]; isNonTerminalMirrorPodOn(pod, node) {
			matched = append(matched, pod)
		}
	}
	return matched, nil
}

func isNonTerminalMirrorPodOn(pod *corev1.Pod, node string) bool {
	return pod.Spec.NodeName == node &&
		isMirrorPod(pod) &&
		pod.Status.Phase != corev1.PodSucceeded &&
		pod.Status.Phase != corev1.PodFailed
}

// fenceTaint returns node's GPU fence taint, or nil. node may be nil.
func fenceTaint(node *corev1.Node) *corev1.Taint {
	if node == nil {
		return nil
	}
	for i := range node.Spec.Taints {
		if taint := &node.Spec.Taints[i]; taint.Key == FenceTaintKey {
			return taint
		}
	}
	return nil
}

// virtualNodeHostReference returns the owner reference from a virtual node
// (labeled virtual-node=true) to its host Node, or nil if node is not a
// virtual node or names no host.
func virtualNodeHostReference(node *corev1.Node) *metav1.OwnerReference {
	if node.Labels[VirtualNodeLabel] != LabelValueTrue {
		return nil
	}
	for i := range node.OwnerReferences {
		if ref := &node.OwnerReferences[i]; ref.APIVersion == "v1" && ref.Kind == "Node" {
			return ref
		}
	}
	return nil
}

// isValidGroup reports whether group can be a node label value.
func isValidGroup(group string) bool {
	return group != "" && len(validation.IsValidLabelValue(group)) == 0
}

// GroupLockSource reports the lock state of a time-slicing group, e.g. from
// the orchestrator's GetGroupStatus.
type GroupLockSource interface {
	// GroupLock returns group's lock state. An error makes it
	// GroupLockUnknown.
	GroupLock(ctx context.Context, group string) (GroupLock, error)
}

// GroupLock is the lock state of a time-slicing group, as far as sharing a
// host is concerned.
type GroupLock int

const (
	// GroupLockIdle: no job holds the group's lock and no switch is in
	// progress, or the lock source does not know the group.
	GroupLockIdle GroupLock = iota
	// GroupLockActive: a job holds the group's lock, or a switch is in
	// progress.
	GroupLockActive
	// GroupLockUnknown: the state could not be read. The host stays shared,
	// and its idle clock neither starts nor restarts.
	GroupLockUnknown
)

func (l GroupLock) String() string {
	switch l {
	case GroupLockIdle:
		return "idle"
	case GroupLockActive:
		return "active"
	case GroupLockUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("GroupLock(%d)", int(l))
	}
}

// hostWatches re-evaluates a host when its Node, one of its virtual nodes,
// or a donor or mirror pod bound to it changes.
func hostWatches() []rulekit.Watch {
	return []rulekit.Watch{
		{
			Object: &corev1.Pod{},
			Filter: func(obj client.Object) bool { return isDonorPod(obj) || isMirrorPod(obj) },
			Keys: func(obj client.Object) []rulekit.Key {
				if pod, ok := obj.(*corev1.Pod); ok {
					if node := pod.Spec.NodeName; node != "" {
						return []rulekit.Key{{Name: node}}
					}
				}
				return nil
			},
		},
		{
			Object: &corev1.Node{},
			Keys:   hostKeysForNode,
		},
	}
}

// hostKeysForNode maps a Node to the host it affects: a virtual node's
// owner host, or the node itself.
func hostKeysForNode(obj client.Object) []rulekit.Key {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	if node.Labels[VirtualNodeLabel] == LabelValueTrue {
		if ref := virtualNodeHostReference(node); ref != nil {
			return []rulekit.Key{{Name: ref.Name}}
		}
		return nil
	}
	return []rulekit.Key{{Name: node.Name}}
}

// isDonorPod reports whether obj is labeled donor=true.
func isDonorPod(obj client.Object) bool {
	return obj.GetLabels()[LabelDonor] == LabelValueTrue
}

// isMirrorPod reports whether obj is a guest kubelet's mirror pod: it
// carries the mirror-node label (every mirror) or the role label (every
// background mirror).
func isMirrorPod(obj client.Object) bool {
	labels := obj.GetLabels()
	_, hasMirrorNode := labels[MirrorNodeLabel]
	_, hasRole := labels[MirrorRoleLabel]
	return hasMirrorNode || hasRole
}

// indexPodNodeName indexes pods by spec.nodeName.
const indexPodNodeName = "spec.nodeName"

func podNodeNameIndex() rulekit.Index {
	return rulekit.Index{
		Object: &corev1.Pod{},
		Field:  indexPodNodeName,
		Extract: func(obj client.Object) []string {
			if pod, ok := obj.(*corev1.Pod); ok {
				if node := pod.Spec.NodeName; node != "" {
					return []string{node}
				}
			}
			return nil
		},
	}
}

// indexVirtualNodeHost indexes virtual nodes by the name of their owner host.
const indexVirtualNodeHost = "virtualNodeHost"

func virtualNodeHostIndex() rulekit.Index {
	return rulekit.Index{
		Object: &corev1.Node{},
		Field:  indexVirtualNodeHost,
		Extract: func(obj client.Object) []string {
			if node, ok := obj.(*corev1.Node); ok {
				if ref := virtualNodeHostReference(node); ref != nil {
					return []string{ref.Name}
				}
			}
			return nil
		},
	}
}

// TrimPod is the cache transform for pods. The cache holds every pod, since
// no label selector matches both donor and mirror pods, so it keeps only
// what the controller reads: TypeMeta; metadata name, namespace, UID, labels,
// creationTimestamp, resourceVersion, deletionTimestamp and ownerReferences;
// spec.nodeName; and status.phase.
func TrimPod(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	return &corev1.Pod{
		TypeMeta: pod.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name:              pod.Name,
			Namespace:         pod.Namespace,
			UID:               pod.UID,
			Labels:            pod.Labels,
			CreationTimestamp: pod.CreationTimestamp,
			ResourceVersion:   pod.ResourceVersion,
			DeletionTimestamp: pod.DeletionTimestamp,
			OwnerReferences:   pod.OwnerReferences,
		},
		Spec:   corev1.PodSpec{NodeName: pod.Spec.NodeName},
		Status: corev1.PodStatus{Phase: pod.Status.Phase},
	}, nil
}

// Labels, annotations and names shared with the rest of the time-slicing
// stack (webhook, orchestrator, guest kubelet, DaemonSets, tests).
const (
	// LabelValueTrue is the value of every boolean label.
	LabelValueTrue = "true"

	// LabelDonor marks donor pods (set by users or the webhook) and shared
	// hosts (set by the donor controller). The guest-kubelet, gpu-shadow
	// and snapshot-agent DaemonSets select on it.
	LabelDonor = "timeslice.io/donor"
	// LabelGroup carries a donor pod's time-slicing group (set by the
	// webhook) and, on a shared host, the host's one group (set by the donor
	// controller). Mirrors infrastructure.PodLabelKey; the orchestrator reads
	// it from nodes with --node-group-labels=ns or either.
	LabelGroup = "timeslice.io/group"

	// AnnotationIdleSince records, in RFC 3339, when a shared host was first
	// seen with no donor pods of its group and its group's lock idle.
	AnnotationIdleSince = "timeslice.io/idle-since"

	// VirtualNodeLabel marks a virtual node (set by the guest kubelet). A
	// virtual node names its host with an owner reference of kind Node.
	VirtualNodeLabel = "timeslice.io/virtual-node"
	// VirtualNodeFinalizer keeps a virtual node, and so its guest pods,
	// through a guest kubelet outage. Only the guest kubelet removes it,
	// except when the host is gone and the donor controller releases the
	// virtual node.
	VirtualNodeFinalizer = "timeslice.io/virtual-node-protection"

	// FenceTaintKey is the guest kubelet's NoSchedule taint on a host while
	// a mirror still runs on a GPU whose donor left; its value names the
	// virtual node. The guest kubelet removes it once the mirror is gone.
	// The donor controller never adds it, and removes it only once the
	// guest kubelet is gone too (its virtual node absent for the grace
	// period) and no mirror pod is left.
	FenceTaintKey = "timeslice.io/gpu-fence"
	// MirrorNodeLabel names, on every mirror pod, the virtual node whose
	// guest it mirrors (set by the guest kubelet).
	MirrorNodeLabel = "timeslice.io/mirror-node"
	// MirrorRoleLabel marks background mirror pods (set by the guest
	// kubelet).
	MirrorRoleLabel = "timeslice.io/role"
	// AnnotationStaleFenceSince records, in RFC 3339, when a host was first
	// seen with a fence whose virtual node is gone and no mirror pod left.
	AnnotationStaleFenceSince = "timeslice.io/stale-fence-since"

	// DonorControllerFieldManager is the donor controller's server-side
	// apply field manager; it defines which host fields the controller owns.
	DonorControllerFieldManager = "timeslice-donor-controller"
)

// Reasons of the Warning Events the donor controller records.
const (
	// ReasonGroupConflict: a donor pod of another group is bound to a host
	// that already belongs to a group.
	ReasonGroupConflict = "GroupConflict"
	// ReasonInvalidGroup: a donor pod has no valid group label.
	ReasonInvalidGroup = "InvalidGroup"
	// ReasonGroupLockUnknown: the lock state of a shared host's group could
	// not be read.
	ReasonGroupLockUnknown = "GroupLockUnknown"
)
