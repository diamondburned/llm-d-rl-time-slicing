package donorcontroller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// LabelE2E is the run-specific isolation label placed on all test nodes and pods.
const LabelE2E = "timeslice.io/e2e"

// VirtualNodeName returns the stable virtual node name for a host.
func VirtualNodeName(host string) string {
	sum := sha256.Sum256([]byte(host))
	return fmt.Sprintf("vk-%x", sum[:5])
}

// VirtualNodeProviderID returns the provider ID for a virtual node.
func VirtualNodeProviderID(name string) string {
	return "virtual-kubelet://" + name
}

// Harness coordinates the cluster connection, fake hosts, and fixture lifecycle.
type Harness struct {
	Client         kubernetes.Interface
	Config         *rest.Config
	Namespace      string
	RunID          string
	FakeHosts      []string
	Controller     *ControllerHandle
	IdleTTL        time.Duration
	IsolationTaint *corev1.Taint
	// StaleFenceGrace is the controller's --stale-fence-grace.
	StaleFenceGrace time.Duration
	DaemonSetName   string
}

// NewHarness initializes the integration test environment and registers cleanups.
func NewHarness(t *testing.T) *Harness {
	t.Helper()
	ctx := context.Background()

	cfg, err := LoadEnv()
	if err != nil {
		t.Fatalf("loading env: %v", err)
	}

	client, restCfg, err := NewKubeClient()
	if err != nil {
		t.Fatalf("building k8s client: %v", err)
	}

	ctrl, err := NewControllerHandle(ctx, client, cfg)
	if err != nil {
		t.Fatalf("finding donor-controller: %v", err)
	}

	testNS := fmt.Sprintf("dc-e2e-%s", cfg.RunID)
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNS,
			Labels: map[string]string{
				LabelE2E: cfg.RunID,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating test namespace %s: %v", testNS, err)
	}

	harness := &Harness{
		Client:    client,
		Config:    restCfg,
		Namespace: testNS,
		RunID:     cfg.RunID,
		FakeHosts: []string{
			fmt.Sprintf("dc-e2e-%s-h1", cfg.RunID),
			fmt.Sprintf("dc-e2e-%s-h2", cfg.RunID),
			fmt.Sprintf("dc-e2e-%s-h3", cfg.RunID),
		},
		Controller:      ctrl,
		IdleTTL:         cfg.IdleTTL,
		StaleFenceGrace: cfg.StaleFenceGrace,
		IsolationTaint:  cfg.IsolationTaint,
		DaemonSetName:   "fake-guest-kubelet",
	}

	t.Cleanup(func() {
		cleanupCtx := context.Background()
		t.Logf("Cleaning up test resources for run %s...", harness.RunID)

		// 1. Delete test namespace and any remaining pods
		zero := int64(0)
		if pods, err := harness.Client.CoreV1().Pods(harness.Namespace).List(cleanupCtx, metav1.ListOptions{}); err == nil {
			for i := range pods.Items {
				p := pods.Items[i]
				if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(cleanupCtx, p.Name, metav1.DeleteOptions{
					GracePeriodSeconds: &zero,
				}); err != nil {
					t.Logf("operation failed: %v", err)
				}
			}
		}
		if err := harness.Client.AppsV1().DaemonSets(harness.Namespace).Delete(cleanupCtx, harness.DaemonSetName, metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
		}); err != nil {
			t.Logf("operation failed: %v", err)
		}
		if err := harness.Client.CoreV1().Namespaces().Delete(cleanupCtx, harness.Namespace, metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
		}); err != nil {
			t.Logf("operation failed: %v", err)
		}

		// 2. Delete any fake host or virtual nodes created for this run
		nodeList, err := harness.Client.CoreV1().Nodes().List(cleanupCtx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", LabelE2E, harness.RunID),
		})
		if err == nil {
			for i := range nodeList.Items {
				n := nodeList.Items[i]
				if err := harness.Client.CoreV1().Nodes().Delete(cleanupCtx, n.Name, metav1.DeleteOptions{
					GracePeriodSeconds: &zero,
				}); err != nil {
					t.Logf("operation failed: %v", err)
				}
			}
		}

		// Clean up known names explicitly
		extraNodes := []string{
			harness.FakeHosts[0], harness.FakeHosts[1], harness.FakeHosts[2],
			VirtualNodeName(harness.FakeHosts[0]),
			VirtualNodeName(harness.FakeHosts[1]),
			VirtualNodeName(harness.FakeHosts[2]),
			fmt.Sprintf("vk-ghost-%s", harness.RunID),
			fmt.Sprintf("vk-ghost2-%s", harness.RunID),
		}
		for _, name := range extraNodes {
			if err := harness.Client.CoreV1().Nodes().Delete(cleanupCtx, name, metav1.DeleteOptions{
				GracePeriodSeconds: &zero,
			}); err != nil {
				t.Logf("operation failed: %v", err)
			}
		}
	})

	// Create fake hosts
	for i, host := range harness.FakeHosts {
		labels := map[string]string{
			LabelE2E: harness.RunID,
		}
		if i == 0 {
			// S1 requirement: pre-existing hand label team=foo set before
			labels["team"] = "foo"
		}
		_, err := harness.CreateFakeHostNode(ctx, host, labels)
		if err != nil {
			t.Fatalf("creating fake host %s: %v", host, err)
		}
	}

	return harness
}

// CreateFakeHostNode creates a fake real host Node object with spec.providerID and allocatable capacity.
func (harness *Harness) CreateFakeHostNode(ctx context.Context, name string, labels map[string]string) (*corev1.Node, error) {
	nodeLabels := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		nodeLabels[k] = v
	}
	nodeLabels[LabelE2E] = harness.RunID

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: nodeLabels,
		},
		Spec: corev1.NodeSpec{
			ProviderID: VirtualNodeProviderID(name),
		},
	}

	_, err := harness.Client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	// Patch status with capacity and allocatable so default-scheduler can bind DaemonSet pods
	statusPatch := []byte(
		`{"status":{"capacity":{"pods":"110","cpu":"4","memory":"16Gi"},"allocatable":{"pods":"110","cpu":"4","memory":"16Gi"}}}`,
	)
	patched, err := harness.Client.CoreV1().
		Nodes().
		Patch(ctx, name, types.MergePatchType, statusPatch, metav1.PatchOptions{}, "status")
	if err != nil {
		return nil, fmt.Errorf("patching fake node %s status: %w", name, err)
	}
	return patched, nil
}

// CreateVirtualNode creates a fake virtual node.
func (harness *Harness) CreateVirtualNode(
	ctx context.Context,
	name string,
	hostNode *corev1.Node,
	labels map[string]string,
	withFinalizer bool,
) (*corev1.Node, error) {
	nodeLabels := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		nodeLabels[k] = v
	}
	nodeLabels[LabelE2E] = harness.RunID

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: nodeLabels,
		},
		Spec: corev1.NodeSpec{
			ProviderID: VirtualNodeProviderID(name),
		},
	}
	if withFinalizer {
		node.Finalizers = []string{donorcontroller.VirtualNodeFinalizer}
	}
	if hostNode != nil {
		node.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       hostNode.Name,
				UID:        hostNode.UID,
			},
		}
	}
	created, err := harness.Client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// CreateDonorPod creates a donor pod bound to host (staying Pending) with LabelDonor=true and optional group.
func (harness *Harness) CreateDonorPod(
	ctx context.Context,
	name, host, group string,
	labels map[string]string,
) (*corev1.Pod, error) {
	podLabels := make(map[string]string, len(labels)+3)
	for k, v := range labels {
		podLabels[k] = v
	}
	podLabels[donorcontroller.LabelDonor] = donorcontroller.LabelValueTrue
	podLabels[LabelE2E] = harness.RunID
	if group != "" {
		podLabels[donorcontroller.LabelGroup] = group
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: harness.Namespace,
			Labels:    podLabels,
		},
		Spec: corev1.PodSpec{
			NodeName: host,
			Tolerations: []corev1.Toleration{
				{Operator: corev1.TolerationOpExists},
			},
			Containers: []corev1.Container{
				{
					Name:  "pause",
					Image: "registry.k8s.io/pause:3.10",
				},
			},
		},
	}
	return harness.Client.CoreV1().Pods(harness.Namespace).Create(ctx, pod, metav1.CreateOptions{})
}

// CreateDaemonSet creates the test DaemonSet with nodeSelector LabelDonor=true and toleration Exists.
func (harness *Harness) CreateDaemonSet(ctx context.Context, name string) (*appsv1.DaemonSet, error) {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: harness.Namespace,
			Labels: map[string]string{
				"app":    name,
				LabelE2E: harness.RunID,
			},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":    name,
						LabelE2E: harness.RunID,
					},
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						donorcontroller.LabelDonor: donorcontroller.LabelValueTrue,
						LabelE2E:                   harness.RunID,
					},
					Tolerations: []corev1.Toleration{
						{Operator: corev1.TolerationOpExists},
					},
					Containers: []corev1.Container{
						{
							Name:  "pause",
							Image: "registry.k8s.io/pause:3.10",
						},
					},
				},
			},
		},
	}
	return harness.Client.AppsV1().DaemonSets(harness.Namespace).Create(ctx, ds, metav1.CreateOptions{})
}

func isNodeShared(node *corev1.Node, group string, isolationTaint *corev1.Taint) bool {
	labels := node.Labels
	if labels == nil || labels[donorcontroller.LabelDonor] != donorcontroller.LabelValueTrue {
		return false
	}
	if labels[donorcontroller.LabelGroup] != group {
		return false
	}
	taintPresent := true
	if isolationTaint != nil {
		taintPresent = false
		for _, tnt := range node.Spec.Taints {
			if tnt.Key == isolationTaint.Key && tnt.Value == isolationTaint.Value && tnt.Effect == isolationTaint.Effect {
				taintPresent = true
				break
			}
		}
	}
	return taintPresent
}

// WaitForNodeShared waits until host has LabelDonor, all group labels, and the isolation taint.
func (harness *Harness) WaitForNodeShared(
	ctx context.Context,
	t *testing.T,
	host, group string,
	timeout time.Duration,
) time.Duration {
	t.Helper()
	t0 := time.Now()
	deadline := t0.Add(timeout)

	for time.Now().Before(deadline) {
		node, err := harness.Client.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err == nil && isNodeShared(node, group, harness.IsolationTaint) {
			elapsed := time.Since(t0)
			t.Logf("Node %s shared in %v (group: %s)", host, elapsed, group)
			return elapsed
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timeout (%v) waiting for node %s to become shared (group: %v)", timeout, host, group)
	return 0
}

func isNodeUnshared(node *corev1.Node, isolationTaint *corev1.Taint) bool {
	labels := node.Labels
	ann := node.Annotations

	hasDonor := labels != nil && labels[donorcontroller.LabelDonor] == donorcontroller.LabelValueTrue
	hasGroup := false
	if labels != nil {
		if _, ok := labels[donorcontroller.LabelGroup]; ok {
			hasGroup = true
		}
	}

	hasTaint := false
	if isolationTaint != nil {
		for _, tnt := range node.Spec.Taints {
			if tnt.Key == isolationTaint.Key && tnt.Effect == isolationTaint.Effect {
				hasTaint = true
				break
			}
		}
	}

	hasIdle := ann != nil && ann[donorcontroller.AnnotationIdleSince] != ""

	return !hasDonor && !hasGroup && !hasTaint && !hasIdle
}

// WaitForNodeUnshared waits until host has no LabelDonor, no group labels, no isolation taint, and no idle annotation.
func (harness *Harness) WaitForNodeUnshared(ctx context.Context, t *testing.T, host string, timeout time.Duration) time.Duration {
	t.Helper()
	t0 := time.Now()
	deadline := t0.Add(timeout)

	for time.Now().Before(deadline) {
		node, err := harness.Client.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err == nil && isNodeUnshared(node, harness.IsolationTaint) {
			elapsed := time.Since(t0)
			t.Logf("Node %s unshared in %v", host, elapsed)
			return elapsed
		}
		time.Sleep(150 * time.Millisecond)
	}

	t.Fatalf("timeout (%v) waiting for node %s to unshare", timeout, host)
	return 0
}

// WaitForIdleSince waits until host has AnnotationIdleSince present.
func (harness *Harness) WaitForIdleSince(
	ctx context.Context,
	t *testing.T,
	host string,
	timeout time.Duration,
) (string, time.Duration) {
	t.Helper()
	t0 := time.Now()
	deadline := t0.Add(timeout)

	for time.Now().Before(deadline) {
		node, err := harness.Client.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err == nil && node.Annotations != nil {
			if idleVal, ok := node.Annotations[donorcontroller.AnnotationIdleSince]; ok && idleVal != "" {
				elapsed := time.Since(t0)
				t.Logf("Node %s idle-since annotation appeared in %v: %s", host, elapsed, idleVal)
				return idleVal, elapsed
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timeout (%v) waiting for %s annotation on node %s", timeout, donorcontroller.AnnotationIdleSince, host)
	return "", 0
}

// WaitForDaemonSetPodOnNode waits for a DaemonSet pod with spec.nodeName=host.
func (harness *Harness) WaitForDaemonSetPodOnNode(
	ctx context.Context,
	t *testing.T,
	host string,
	timeout time.Duration,
) (*corev1.Pod, time.Duration) {
	t.Helper()
	t0 := time.Now()
	deadline := t0.Add(timeout)

	for time.Now().Before(deadline) {
		pods, err := harness.Client.CoreV1().Pods(harness.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", harness.DaemonSetName),
			FieldSelector: fmt.Sprintf("spec.nodeName=%s", host),
		})
		if err == nil && len(pods.Items) > 0 {
			elapsed := time.Since(t0)
			t.Logf("DaemonSet pod on %s scheduled in %v: %s", host, elapsed, pods.Items[0].Name)
			return &pods.Items[0], elapsed
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("timeout (%v) waiting for DaemonSet pod on node %s", timeout, host)
	return nil, 0
}

// WaitForDaemonSetPodGoneFromNode waits until no active (non-terminating) DaemonSet pods exist with spec.nodeName=host.
func (harness *Harness) WaitForDaemonSetPodGoneFromNode(
	ctx context.Context,
	t *testing.T,
	host string,
	timeout time.Duration,
) time.Duration {
	t.Helper()
	t0 := time.Now()
	deadline := t0.Add(timeout)

	for time.Now().Before(deadline) {
		pods, err := harness.Client.CoreV1().Pods(harness.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", harness.DaemonSetName),
			FieldSelector: fmt.Sprintf("spec.nodeName=%s", host),
		})
		if err == nil {
			activePods := 0
			for i := range pods.Items {
				p := pods.Items[i]
				if p.DeletionTimestamp == nil {
					activePods++
				}
			}
			if activePods == 0 {
				// Force delete any terminating pods that lack a real kubelet to finalize them
				for i := range pods.Items {
					p := pods.Items[i]
					zero := int64(0)
					if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, p.Name,
						metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
						t.Logf("operation failed: %v", err)
					}
				}
				elapsed := time.Since(t0)
				t.Logf("DaemonSet pod removed from %s in %v", host, elapsed)
				return elapsed
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("timeout (%v) waiting for DaemonSet pod to leave node %s", timeout, host)
	return 0
}

// WaitForNodeDeleted waits until the node is gone (returns NotFound).
func (harness *Harness) WaitForNodeDeleted(ctx context.Context, t *testing.T, name string, timeout time.Duration) time.Duration {
	t.Helper()
	t0 := time.Now()
	deadline := t0.Add(timeout)

	for time.Now().Before(deadline) {
		_, err := harness.Client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			elapsed := time.Since(t0)
			t.Logf("Node %s deleted in %v", name, elapsed)
			return elapsed
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timeout (%v) waiting for node %s to be deleted", timeout, name)
	return 0
}

// AssertNodeUntouched verifies that the node receives no timeslice labels/annotations or managedFields entry.
func (harness *Harness) AssertNodeUntouched(ctx context.Context, t *testing.T, host string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)

	for time.Now().Before(deadline) {
		node, err := harness.Client.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting node %s: %v", host, err)
		}

		for k := range node.Labels {
			if strings.Contains(k, "timeslice") && k != "group.timeslice.io/manual" && k != LabelE2E {
				t.Fatalf("unexpected timeslice label %s on node %s", k, host)
			}
		}
		for k := range node.Annotations {
			if strings.Contains(k, "timeslice") {
				t.Fatalf("unexpected timeslice annotation %s on node %s", k, host)
			}
		}
		for _, mf := range node.ManagedFields {
			if mf.Manager == donorcontroller.DonorControllerFieldManager {
				t.Fatalf("unexpected managedFields entry for %s on node %s", donorcontroller.DonorControllerFieldManager, host)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// AddTaintToNode adds a taint to the node spec via Update.
func AddTaintToNode(ctx context.Context, client kubernetes.Interface, host string, taint corev1.Taint) error {
	for attempt := 0; attempt < 5; attempt++ {
		node, err := client.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err != nil {
			return err
		}
		for _, t := range node.Spec.Taints {
			if t.Key == taint.Key && t.Effect == taint.Effect {
				return nil // already present
			}
		}
		node.Spec.Taints = append(node.Spec.Taints, taint)
		_, err = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("conflict adding taint to %s after 5 attempts", host)
}

// RemoveTaintFromNode removes a taint by key from the node spec.
func RemoveTaintFromNode(ctx context.Context, client kubernetes.Interface, host, taintKey string) error {
	for attempt := 0; attempt < 5; attempt++ {
		node, err := client.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		var filtered []corev1.Taint
		for _, t := range node.Spec.Taints {
			if t.Key != taintKey {
				filtered = append(filtered, t)
			}
		}
		if len(filtered) == len(node.Spec.Taints) {
			return nil // already absent
		}
		node.Spec.Taints = filtered
		_, err = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("conflict removing taint %s from %s after 5 attempts", taintKey, host)
}

// WaitForEvent waits for a Kubernetes event matching kind, name, reason, and optional message substring.
func (harness *Harness) WaitForEvent(
	ctx context.Context,
	namespace, kind, name, reason, msgSubstr string,
	timeout time.Duration,
) (*corev1.Event, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, err := harness.Client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
		if err == nil {
			for i := range events.Items {
				e := &events.Items[i]
				if e.InvolvedObject.Kind == kind && e.InvolvedObject.Name == name && e.Reason == reason {
					if msgSubstr == "" || strings.Contains(e.Message, msgSubstr) {
						return e, nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf(
		"timeout waiting for event in ns=%s on %s/%s with reason=%s msgSubstr=%q",
		namespace,
		kind,
		name,
		reason,
		msgSubstr,
	)
}
