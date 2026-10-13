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

// Package donorcontroller marks real nodes as shared while an RL job's donor
// pods run on them, and unmarks them once the job is done with them.
//
// For every real node (the "host"), the controller fetches one snapshot,
// [hostState], and evaluates the rules in [rules] on it:
//
//   - share node while donor pods are bound: label the host donor=true and
//     group=<group> (and taint it, if configured). The guest-kubelet,
//     gpu-shadow and snapshot-agent DaemonSets select on the donor label, so
//     they follow the label onto and off the host;
//   - reject donor pods of any other group: a host belongs to one group at a
//     time, and the first group wins;
//   - keep the host shared while its group's lock is active, and for an idle
//     grace period after its last donor pod left and its lock went idle;
//   - unshare the host once the grace period has elapsed;
//   - release the virtual nodes of a host that is gone, which their guest
//     kubelet, having died with the host, cannot do itself;
//   - remove a stale GPU fence: the guest kubelet's fence taint on a host
//     whose virtual node has been gone for a grace period, with no mirror
//     pod left, which the departed guest kubelet can no longer remove.
//
// Rules only describe the desired state of the host. Package
// internal/rulekit does every read and write; see README.md.
package donorcontroller

import (
	"context"
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Config holds the donor controller's policy knobs.
type Config struct {
	// IdleGracePeriod is how long a shared host must have had no donor pods
	// of its group, and its group no lock activity, before the controller
	// unshares it. Zero or negative unshares immediately.
	IdleGracePeriod time.Duration
	// IsolationTaint, if non-nil, is kept on shared hosts. The controller
	// owns exactly the taint with this key and effect; other taints are
	// never touched.
	IsolationTaint *corev1.Taint
	// GroupLocks, if non-nil, reports each group's lock state. A shared host
	// stays shared while its group's lock is active or unknown. If nil,
	// donor pods alone decide.
	GroupLocks GroupLockSource
	// GroupLockPollInterval is how often an idle shared host re-reads its
	// group's lock state. Lock state is not a Kubernetes object, so no
	// watch reports its changes.
	GroupLockPollInterval time.Duration
	// ClearStaleFences enables removing a host's GPU fence taint
	// ([FenceTaintKey]) once the virtual node it names has been gone for
	// StaleFenceGracePeriod and no mirror pod is left on the host.
	ClearStaleFences bool
	// StaleFenceGracePeriod is how long a fence must have been stale before
	// the controller removes it; it gives a restarting guest kubelet time
	// to re-register its virtual node. Zero or negative removes it at once.
	StaleFenceGracePeriod time.Duration
}

// Options configures how the donor controller runs.
type Options struct {
	// DryRun logs every write the controller would make instead of making it.
	DryRun bool
	// Workers is the number of hosts reconciled concurrently.
	Workers int
}

// Setup registers the donor controller with the controller-runtime manager.
// The manager's cache should hold every pod, with [TrimPod] as the Pod
// transform: the rules read both donor and mirror pods.
func Setup(mgr manager.Manager, cfg Config, opts Options) error {
	return rulekit.Setup(mgr, newController(cfg), rulekit.Options{
		DryRun:                  opts.DryRun,
		MaxConcurrentReconciles: opts.Workers,
		Clock:                   time.Now,
	})
}

func newController(cfg Config) rulekit.Controller[*hostState] {
	return rulekit.Controller[*hostState]{
		Name:         "donor-controller",
		FieldManager: DonorControllerFieldManager,
		Watches:      hostWatches(),
		Indexes:      []rulekit.Index{podNodeNameIndex(), virtualNodeHostIndex()},
		Fetch: func(ctx context.Context, rd rulekit.Reader, key rulekit.Key) (*hostState, error) {
			return fetchHostState(ctx, rd, key, cfg.GroupLocks)
		},
		Rules: rules(cfg),
		Owns: func(key rulekit.Key) []rulekit.Ref {
			return []rulekit.Ref{{GVK: corev1.SchemeGroupVersion.WithKind("Node"), Name: key.Name}}
		},
		ManagedTaints: func(key rulekit.Key) []rulekit.ManagedTaint {
			if cfg.IsolationTaint == nil {
				return nil
			}
			return []rulekit.ManagedTaint{{Node: key.Name, Key: cfg.IsolationTaint.Key, Effect: cfg.IsolationTaint.Effect}}
		},
	}
}

// rules returns the donor controller's rules, in evaluation order. Releasing
// is implicit: whatever no rule asserts (labels, the idle-since and
// stale-fence-since annotations, the isolation taint) is removed from the
// host.
//
// Rule names appear verbatim in logs, Events and errors; design doc writes in
// parentheses:
//
//   - share node while donor pods are bound (W1, W2, W5): the host exists and
//     donor pods of its group are bound -> donor + group labels, isolation taint.
//   - reject donor pods outside the node's group (W2): donor pods of another
//     (or no valid) group are bound -> Warning Events on the pods and host; no
//     writes.
//   - keep node shared while group lock is active or unknown (W3, hold): shared,
//     no donor pods of its group, lock not idle -> same; keep, never start, the
//     idle clock.
//   - keep node shared during idle grace period (W3, clock): shared and idle,
//     grace period not elapsed -> same, plus idle-since and wake-ups.
//   - unshare node after idle grace period (W3): shared and idle, grace period
//     elapsed -> require idle (live); everything released.
//   - release virtual nodes of dead host (W4): a virtual node's owner host is
//     gone or recreated -> delete it, remove its finalizer.
//   - wait out stale gpu fence: the fence names a missing virtual node and no
//     mirror pod is bound -> stale-fence-since and a wake-up.
//   - remove stale gpu fence: same, grace period elapsed -> require both
//     (live); remove the fence taint.
func rules(cfg Config) []rulekit.Rule[*hostState] {
	return []rulekit.Rule[*hostState]{
		{
			Name: "share node while donor pods are bound",
			When: func(h *hostState) bool {
				return h.hostNodeExists() && len(h.donorPodsInNodeGroup()) > 0
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				fillDonorNodeLabelsAndTaint(h, fx, cfg)
				return nil
			},
		},
		{
			Name: "reject donor pods outside the node's group",
			When: func(h *hostState) bool {
				return h.hostNodeExists() && len(h.donorPodsOutsideNodeGroup()) > 0
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				warnDonorPodsOutsideNodeGroup(h, fx)
				return nil
			},
		},
		{
			Name: "keep node shared while group lock is active or unknown",
			When: func(h *hostState) bool {
				return h.isIdleSharedHost() && h.GroupLock != GroupLockIdle
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				fillDonorNodeLabelsAndTaint(h, fx, cfg)
				if h.GroupLock == GroupLockUnknown {
					// Hold the idle clock where it is: missing information
					// neither unshares the host nor restarts the clock.
					if idleSince, ok := h.OwnedAnnotations[AnnotationIdleSince]; ok {
						fx.Apply(corev1ac.Node(h.Host).WithAnnotations(map[string]string{AnnotationIdleSince: idleSince}))
					}
					fx.Warn(h.Node, ReasonGroupLockUnknown, fmt.Sprintf(
						"lock state of group %q is unknown, keeping the node shared: %s", h.ownedGroup(), h.GroupLockError))
				}
				fx.WakeAt(h.Now.Add(cfg.GroupLockPollInterval))
				return nil
			},
		},
		{
			Name: "keep node shared during idle grace period",
			When: func(h *hostState) bool {
				return h.isIdleSharedHost() && h.GroupLock == GroupLockIdle && !h.idleGracePeriodElapsed(cfg.IdleGracePeriod)
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				fillDonorNodeLabelsAndTaint(h, fx, cfg)
				idleSince := h.idleSinceOrNow()
				fx.Apply(corev1ac.Node(h.Host).WithAnnotations(map[string]string{
					AnnotationIdleSince: idleSince.Format(time.RFC3339),
				}))
				fx.WakeAt(idleSince.Add(cfg.IdleGracePeriod))
				if cfg.GroupLocks != nil {
					// Lock activity during the grace period restarts the clock.
					fx.WakeAt(h.Now.Add(cfg.GroupLockPollInterval))
				}
				return nil
			},
		},
		{
			Name: "unshare node after idle grace period",
			When: func(h *hostState) bool {
				return h.isIdleSharedHost() && h.GroupLock == GroupLockIdle && h.idleGracePeriodElapsed(cfg.IdleGracePeriod)
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				// Asserting nothing releases the labels, annotation and taint.
				// The cache may not have seen a donor pod that just bound, nor
				// the lock just taken, so check both live first (E7).
				fx.Require(noDonorPodsOfGroupBoundToNode(h.Host, h.ownedGroup()))
				if cfg.GroupLocks != nil {
					fx.Require(groupLockIsIdle(cfg.GroupLocks, h.ownedGroup()))
				}
				return nil
			},
		},
		{
			Name: "release virtual nodes of dead host",
			When: func(h *hostState) bool {
				return len(h.virtualNodesOfDeadHost()) > 0
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				for _, vk := range h.virtualNodesOfDeadHost() {
					fx.Delete(vk)
					fx.RemoveFinalizer(vk, VirtualNodeFinalizer)
					fx.Require(nodeIsGoneOrRecreated(h.Host, virtualNodeHostReference(vk).UID))
				}
				return nil
			},
		},
		{
			Name: "wait out stale gpu fence",
			When: func(h *hostState) bool {
				return cfg.ClearStaleFences && h.hasStaleFence() && !h.staleFenceGracePeriodElapsed(cfg.StaleFenceGracePeriod)
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				staleSince := h.ownedTimeAnnotationOrNow(AnnotationStaleFenceSince)
				fx.Apply(corev1ac.Node(h.Host).WithAnnotations(map[string]string{
					AnnotationStaleFenceSince: staleSince.Format(time.RFC3339),
				}))
				fx.WakeAt(staleSince.Add(cfg.StaleFenceGracePeriod))
				return nil
			},
		},
		{
			Name: "remove stale gpu fence",
			When: func(h *hostState) bool {
				return cfg.ClearStaleFences && h.hasStaleFence() && h.staleFenceGracePeriodElapsed(cfg.StaleFenceGracePeriod)
			},
			Then: func(h *hostState, fx *rulekit.Effects) error {
				// Not asserting stale-fence-since releases it. The cache may
				// not have seen the guest kubelet re-register, nor a mirror
				// just created, so check both live first.
				fx.RemoveTaint(h.Node, *h.Fence)
				fx.Require(virtualNodeIsGone(h.Fence.Value))
				fx.Require(noMirrorPodsBoundToNode(h.Host))
				return nil
			},
		},
	}
}

// fillDonorNodeLabelsAndTaint asserts the donor label, the host's group
// label and the isolation taint, if configured.
func fillDonorNodeLabelsAndTaint(h *hostState, fx *rulekit.Effects, cfg Config) {
	labels := map[string]string{LabelDonor: LabelValueTrue}
	if group := h.nodeGroup(); group != "" {
		labels[LabelGroup] = group
	}
	fx.Apply(corev1ac.Node(h.Host).WithLabels(labels))
	if cfg.IsolationTaint != nil {
		fx.Taint(h.Host, *cfg.IsolationTaint)
	}
}

// warnDonorPodsOutsideNodeGroup records a Warning Event on every donor pod
// that cannot join the host's group, and on the host itself.
func warnDonorPodsOutsideNodeGroup(h *hostState, fx *rulekit.Effects) {
	nodeGroup := h.nodeGroup()
	for _, pod := range h.donorPodsOutsideNodeGroup() {
		group := pod.Labels[LabelGroup]
		reason, message := ReasonGroupConflict, fmt.Sprintf(
			"node %s is in group %q, so donor pod %s/%s of group %q does not share it; the first group wins",
			h.Host, nodeGroup, pod.Namespace, pod.Name, group)
		if !isValidGroup(group) {
			reason, message = ReasonInvalidGroup, fmt.Sprintf(
				"donor pod %s/%s on node %s has no valid %s label (%q), so it does not share the node",
				pod.Namespace, pod.Name, h.Host, LabelGroup, group)
		}
		fx.Warn(pod, reason, message)
		fx.Warn(h.Node, reason, message)
	}
}

// hostNodeExists reports whether the key is a real host whose Node exists.
func (h *hostState) hostNodeExists() bool {
	return !h.KeyIsVirtualNode && h.Node != nil
}

// nodeGroup returns the host's time-slicing group: the group label already
// on the Node, whoever set it, or else the group of the oldest donor pod
// with a valid group. Empty if there is neither.
func (h *hostState) nodeGroup() string {
	if h.Node != nil {
		if group := h.Node.Labels[LabelGroup]; group != "" {
			return group
		}
	}
	for _, pod := range h.DonorPods { // oldest first
		if group := pod.Labels[LabelGroup]; isValidGroup(group) {
			return group
		}
	}
	return ""
}

// donorPodsInNodeGroup returns the donor pods bound to the host that belong
// to the host's group.
func (h *hostState) donorPodsInNodeGroup() []*corev1.Pod {
	group := h.nodeGroup()
	var pods []*corev1.Pod
	for _, pod := range h.DonorPods {
		if group != "" && pod.Labels[LabelGroup] == group {
			pods = append(pods, pod)
		}
	}
	return pods
}

// donorPodsOutsideNodeGroup returns the donor pods bound to the host that
// belong to another group or to no valid group.
func (h *hostState) donorPodsOutsideNodeGroup() []*corev1.Pod {
	group := h.nodeGroup()
	var pods []*corev1.Pod
	for _, pod := range h.DonorPods {
		if group == "" || pod.Labels[LabelGroup] != group {
			pods = append(pods, pod)
		}
	}
	return pods
}

// ownsDonorLabel reports whether the controller currently owns the donor
// label on the host, i.e. whether the host is shared.
func (h *hostState) ownsDonorLabel() bool {
	return h.OwnedLabels[LabelDonor] == LabelValueTrue
}

// ownedGroup returns the group label value the controller owns on the host.
func (h *hostState) ownedGroup() string {
	return h.OwnedLabels[LabelGroup]
}

// isIdleSharedHost reports whether the host is shared but has no donor pods
// of its group.
func (h *hostState) isIdleSharedHost() bool {
	return h.hostNodeExists() && h.ownsDonorLabel() && len(h.donorPodsInNodeGroup()) == 0
}

// idleSinceOrNow returns the owned idle-since annotation, or the snapshot
// time truncated to seconds if it is missing or unparseable.
func (h *hostState) idleSinceOrNow() time.Time {
	return h.ownedTimeAnnotationOrNow(AnnotationIdleSince)
}

// ownedTimeAnnotationOrNow returns the RFC 3339 time in the owned annotation
// key, or the snapshot time truncated to seconds if it is missing or
// unparseable.
func (h *hostState) ownedTimeAnnotationOrNow(key string) time.Time {
	if t, err := time.Parse(time.RFC3339, h.OwnedAnnotations[key]); err == nil {
		return t
	}
	return h.Now.Truncate(time.Second)
}

// idleGracePeriodElapsed reports whether gracePeriod has passed since the
// host became idle. A non-positive gracePeriod has always elapsed.
func (h *hostState) idleGracePeriodElapsed(gracePeriod time.Duration) bool {
	if gracePeriod <= 0 {
		return true
	}
	return !h.Now.Truncate(time.Second).Before(h.idleSinceOrNow().Add(gracePeriod))
}

// hasStaleFence reports whether the host carries a GPU fence whose virtual
// node is gone, with no mirror pod left on the host.
func (h *hostState) hasStaleFence() bool {
	return h.hostNodeExists() && h.Fence != nil && !h.FenceVirtualNodeExists && len(h.MirrorPods) == 0
}

// staleFenceGracePeriodElapsed reports whether gracePeriod has passed since
// the host's fence was first seen stale. A non-positive gracePeriod has
// always elapsed.
func (h *hostState) staleFenceGracePeriodElapsed(gracePeriod time.Duration) bool {
	if gracePeriod <= 0 {
		return true
	}
	staleSince := h.ownedTimeAnnotationOrNow(AnnotationStaleFenceSince)
	return !h.Now.Truncate(time.Second).Before(staleSince.Add(gracePeriod))
}

// virtualNodesOfDeadHost returns the host's virtual nodes whose owner
// reference names a host Node that no longer exists, or that was recreated
// with a new UID.
func (h *hostState) virtualNodesOfDeadHost() []*corev1.Node {
	var dead []*corev1.Node
	for _, vk := range h.VirtualNodes {
		if h.Node == nil || h.Node.UID != virtualNodeHostReference(vk).UID {
			dead = append(dead, vk)
		}
	}
	return dead
}

// noDonorPodsOfGroupBoundToNode requires, against the live API server, that
// no non-terminal donor pod of group is bound to node.
func noDonorPodsOfGroupBoundToNode(node, group string) rulekit.Precondition {
	return rulekit.Precondition{
		Description: fmt.Sprintf("no donor pods of group %q bound to node %s", group, node),
		Check: func(ctx context.Context, live client.Reader) error {
			var pods corev1.PodList
			if err := live.List(ctx, &pods,
				client.MatchingLabels{LabelDonor: LabelValueTrue, LabelGroup: group},
				client.MatchingFields{indexPodNodeName: node},
			); err != nil {
				return err
			}
			for i := range pods.Items {
				// Filter client-side too: fake clients ignore field selectors.
				if pod := &pods.Items[i]; isNonTerminalDonorPodOn(pod, node) && pod.Labels[LabelGroup] == group {
					return fmt.Errorf("%w: donor pod %s/%s of group %q is bound to node %s",
						rulekit.ErrPreconditionFailed, pod.Namespace, pod.Name, group, node)
				}
			}
			return nil
		},
	}
}

// groupLockIsIdle requires, against the lock source, that group's lock is
// idle.
func groupLockIsIdle(locks GroupLockSource, group string) rulekit.Precondition {
	return rulekit.Precondition{
		Description: fmt.Sprintf("lock of group %q is idle", group),
		Check: func(ctx context.Context, _ client.Reader) error {
			state, err := locks.GroupLock(ctx, group)
			switch {
			case err != nil:
				return fmt.Errorf("%w: lock state of group %q is unknown: %w", rulekit.ErrPreconditionFailed, group, err)
			case state != GroupLockIdle:
				return fmt.Errorf("%w: lock of group %q is %s", rulekit.ErrPreconditionFailed, group, state)
			}
			return nil
		},
	}
}

// nodeIsGoneOrRecreated requires, against the live API server, that the Node
// name does not exist, or exists with a UID other than uid.
func nodeIsGoneOrRecreated(name string, uid types.UID) rulekit.Precondition {
	return rulekit.Precondition{
		Description: fmt.Sprintf("node %s with uid %s is gone", name, uid),
		Check: func(ctx context.Context, live client.Reader) error {
			var node corev1.Node
			err := live.Get(ctx, client.ObjectKey{Name: name}, &node)
			switch {
			case apierrors.IsNotFound(err):
				return nil
			case err != nil:
				return err
			case node.UID != uid:
				return nil
			}
			return fmt.Errorf("%w: node %s with uid %s still exists", rulekit.ErrPreconditionFailed, name, uid)
		},
	}
}

// virtualNodeIsGone requires, against the live API server, that no Node
// named name exists.
func virtualNodeIsGone(name string) rulekit.Precondition {
	return rulekit.Precondition{
		Description: fmt.Sprintf("virtual node %s is gone", name),
		Check: func(ctx context.Context, live client.Reader) error {
			var node corev1.Node
			err := live.Get(ctx, client.ObjectKey{Name: name}, &node)
			switch {
			case apierrors.IsNotFound(err):
				return nil
			case err != nil:
				return err
			}
			return fmt.Errorf("%w: node %s exists", rulekit.ErrPreconditionFailed, name)
		},
	}
}

// noMirrorPodsBoundToNode requires, against the live API server, that no
// non-terminal mirror pod is bound to node.
func noMirrorPodsBoundToNode(node string) rulekit.Precondition {
	return rulekit.Precondition{
		Description: fmt.Sprintf("no mirror pods bound to node %s", node),
		Check: func(ctx context.Context, live client.Reader) error {
			var pods corev1.PodList
			if err := live.List(ctx, &pods, client.MatchingFields{indexPodNodeName: node}); err != nil {
				return err
			}
			for i := range pods.Items {
				// Filter client-side too: fake clients ignore field selectors.
				if pod := &pods.Items[i]; isNonTerminalMirrorPodOn(pod, node) {
					return fmt.Errorf("%w: mirror pod %s/%s is bound to node %s",
						rulekit.ErrPreconditionFailed, pod.Namespace, pod.Name, node)
				}
			}
			return nil
		},
	}
}
