package donorcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
)

// RunS0Baseline tests that fake hosts get no timeslice labels/annotations and no managedFields
// entry for DonorControllerFieldManager within 15 seconds.
func RunS0Baseline(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	t.Log("Verifying baseline state for fake hosts (15s quiet period)...")
	for _, host := range harness.FakeHosts {
		harness.AssertNodeUntouched(ctx, t, host, 5*time.Second)
	}

	// Verify team=foo pre-existing label on h1 survived
	h1 := harness.FakeHosts[0]
	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting fake host %s: %v", h1, err)
	}
	if node.Labels["team"] != "foo" {
		t.Fatalf("expected pre-existing hand label team=foo on %s, got %v", h1, node.Labels)
	}
}

// RunS1Share tests sharing h1 via donor pod (g1): LabelDonor, NodeGroupLabel(g1), isolation taint,
// team=foo survival, DaemonSet pod scheduled to h1, and h2 remaining untouched.
func RunS1Share(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h1 := harness.FakeHosts[0]
	h2 := harness.FakeHosts[1]

	// 1. Create DaemonSet in test namespace
	_, err := harness.CreateDaemonSet(ctx, harness.DaemonSetName)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating daemonset: %v", err)
	}

	// 2. Create donor pod bound to h1 with group g1
	_, err = harness.CreateDonorPod(ctx, "donor-s1-a", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}

	// 3. Wait for h1 to become shared with g1 and isolation taint
	harness.WaitForNodeShared(ctx, t, h1, "g1", 30*time.Second)

	// 4. Verify team=foo survived
	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h1, err)
	}
	if node.Labels["team"] != "foo" {
		t.Fatalf("pre-existing label team=foo was overwritten or lost on %s: %v", h1, node.Labels)
	}

	// 5. Verify DaemonSet gets a pod on h1
	_, _ = harness.WaitForDaemonSetPodOnNode(ctx, t, h1, 30*time.Second)

	// 6. Verify h2 is untouched
	node2, err := harness.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h2, err)
	}
	if node2.Labels != nil {
		if node2.Labels[donorcontroller.LabelDonor] != "" || node2.Labels[donorcontroller.LabelGroup] != "" {
			t.Fatalf("node %s was unexpectedly labeled: %v", h2, node2.Labels)
		}
	}
	for _, tnt := range node2.Spec.Taints {
		if tnt.Key == "timeslice.io/shared" {
			t.Fatalf("node %s unexpectedly got isolation taint: %v", h2, node2.Spec.Taints)
		}
	}
}

// RunS2GroupConflict tests that a second donor of g2 on h1 (which is g1) results in GroupConflict warnings.
func RunS2GroupConflict(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h1 := harness.FakeHosts[0]
	podName := "donor-s2-b"
	_, err := harness.CreateDonorPod(ctx, podName, h1, "g2", nil)
	if err != nil {
		t.Fatalf("creating second donor pod on %s: %v", h1, err)
	}

	harness.WaitForNodeShared(ctx, t, h1, "g1", 30*time.Second)

	if _, err := harness.WaitForEvent(ctx, harness.Namespace, "Pod", podName, donorcontroller.ReasonGroupConflict, "",
		15*time.Second); err != nil {
		t.Fatalf("waiting for GroupConflict event on pod %s: %v", podName, err)
	}
	if _, err := harness.WaitForEvent(ctx, "default", "Node", h1, donorcontroller.ReasonGroupConflict, "",
		15*time.Second); err != nil {
		t.Fatalf("waiting for GroupConflict event on node %s: %v", h1, err)
	}

	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h1, err)
	}
	if node.Labels[donorcontroller.LabelGroup] != "g1" {
		t.Fatalf("expected node to keep group=g1, got %v", node.Labels[donorcontroller.LabelGroup])
	}
}

// RunS3VirtualNodeDoesNotHold tests that deleting donors causes h1 to unshare after TTL+15s even if a virtual node exists.
func RunS3VirtualNodeDoesNotHold(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h1 := harness.FakeHosts[0]
	vnName := VirtualNodeName(h1)

	h1Node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting %s: %v", h1, err)
	}

	_, err = harness.CreateVirtualNode(ctx, vnName, h1Node, map[string]string{
		donorcontroller.VirtualNodeLabel: donorcontroller.LabelValueTrue,
	}, true)
	if err != nil {
		t.Fatalf("creating virtual node %s: %v", vnName, err)
	}

	zero := int64(0)
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s1-a",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s2-b",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}

	_, _ = harness.WaitForIdleSince(ctx, t, h1, 15*time.Second)
	harness.WaitForNodeUnshared(ctx, t, h1, harness.IdleTTL+15*time.Second)

	// virtual node should still exist
	if _, err := harness.Client.CoreV1().Nodes().Get(ctx, vnName, metav1.GetOptions{}); err != nil {
		t.Fatalf("virtual node %s should still exist: %v", vnName, err)
	}

	// cleanup: remove finalizer and delete
	vn, err := harness.Client.CoreV1().Nodes().Get(ctx, vnName, metav1.GetOptions{})
	if err == nil {
		vn.Finalizers = nil
		if _, err := harness.Client.CoreV1().Nodes().Update(ctx, vn, metav1.UpdateOptions{}); err != nil {
			t.Logf("Update failed: %v", err)
		}
		if err := harness.Client.CoreV1().Nodes().Delete(ctx, vnName,
			metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Logf("operation failed: %v", err)
		}
	}
}

// RunS4Unshare tests that deleting vk-h1 causes idle-since to appear, followed by unshare on TTL;
// team=foo survives and DaemonSet pod is removed.
// RunS4Unshare tests that team=foo survives and DaemonSet pod is removed.
// The unshare already happened in S3, so we just check the final state.
func RunS4Unshare(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h1 := harness.FakeHosts[0]

	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h1, err)
	}
	if node.Labels["team"] != "foo" {
		t.Fatalf("pre-existing label team=foo was lost after unshare on %s: %v", h1, node.Labels)
	}

	_ = harness.WaitForDaemonSetPodGoneFromNode(ctx, t, h1, 20*time.Second)
}

func RunS5E7(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h1 := harness.FakeHosts[0]

	// 1. Share h1 with donor-c
	_, err := harness.CreateDonorPod(ctx, "donor-s5-c", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	harness.WaitForNodeShared(ctx, t, h1, "g1", 30*time.Second)

	// 2. Delete donor-c to start idle clock
	zero := int64(0)
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s5-c",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}

	// 3. Wait for idle-since annotation
	_, _ = harness.WaitForIdleSince(ctx, t, h1, 15*time.Second)

	// 4. Return immediately with donor-d
	_, err = harness.CreateDonorPod(ctx, "donor-s5-d", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}

	// 5. Poll every 1s for TTL+10s: donor label must be present in every sample; idle-since cleared
	sampleDuration := harness.IdleTTL + 10*time.Second
	t.Logf("Sampling node %s every 1s for %v to verify E7 convergence...", h1, sampleDuration)
	deadline := time.Now().Add(sampleDuration)
	idleCleared := false

	for time.Now().Before(deadline) {
		node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting node %s: %v", h1, err)
		}
		if node.Labels[donorcontroller.LabelDonor] != donorcontroller.LabelValueTrue {
			t.Fatalf("donor label was dropped during idle-return convergence on %s", h1)
		}
		if node.Annotations == nil || node.Annotations[donorcontroller.AnnotationIdleSince] == "" {
			idleCleared = true
		}
		time.Sleep(1 * time.Second)
	}

	if !idleCleared {
		t.Fatalf("idle-since annotation was never cleared after donor returned to %s", h1)
	}

	// 6. Delete donor-d and let it cleanly unshare
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s5-d",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}
	harness.WaitForNodeUnshared(ctx, t, h1, harness.IdleTTL+15*time.Second)
}

// RunS6Restart tests that restarting the controller during the idle clock leaves the annotation
// value unchanged, and unshare happens ~TTL after the original idle-since (tolerance +15s).
func RunS6Restart(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h1 := harness.FakeHosts[0]

	// 1. Share h1 with donor-e
	_, err := harness.CreateDonorPod(ctx, "donor-s6-e", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	harness.WaitForNodeShared(ctx, t, h1, "g1", 30*time.Second)

	// 2. Delete donor-e
	zero := int64(0)
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s6-e",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}

	// 3. Wait for idle-since annotation and capture timestamp
	idleStr, _ := harness.WaitForIdleSince(ctx, t, h1, 15*time.Second)
	tIdle, err := time.Parse(time.RFC3339, idleStr)
	if err != nil {
		t.Fatalf("parsing idle-since timestamp %q: %v", idleStr, err)
	}

	// 4. Restart controller during idle clock
	t.Logf("Restarting donor-controller during idle clock (original idle-since: %s)...", idleStr)
	err = harness.Controller.Restart(ctx)
	if err != nil {
		t.Fatalf("restarting controller: %v", err)
	}

	// 5. Verify annotation value on node is unchanged after restart
	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s after restart: %v", h1, err)
	}
	annAfter := node.Annotations[donorcontroller.AnnotationIdleSince]
	if annAfter != idleStr {
		t.Fatalf("idle-since annotation changed across restart: before=%q, after=%q", idleStr, annAfter)
	}

	// 6. Unshare happens ~TTL after original idle-since (tolerance +15s)
	harness.WaitForNodeUnshared(ctx, t, h1, harness.IdleTTL+20*time.Second)
	tUnshare := time.Now()
	delta := tUnshare.Sub(tIdle)
	t.Logf("Unshare completed in %v from original idle-since (TTL: %v)", delta, harness.IdleTTL)

	// Assert within tolerance: must not unshare earlier than TTL-3s (clock jitter) and not later than TTL+15s
	if delta < harness.IdleTTL-3*time.Second {
		t.Fatalf("unshare occurred too early (%v < TTL %v)", delta, harness.IdleTTL)
	}
	if delta > harness.IdleTTL+15*time.Second {
		t.Fatalf("unshare occurred too late (%v > TTL+15s %v)", delta, harness.IdleTTL+15*time.Second)
	}
}

// RunS7W4Guards tests that (a) virtual node with label+finalizer+ownerRef to nonexistent host disappears within 20s
// and emits FinalizerRemoved; (b) Node with ownerRef+finalizer but NO virtual-node label still exists after 15s;
// (c) virtual node owned by live h2 is kept for 15s.
func RunS7W4Guards(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h2 := harness.FakeHosts[1]

	// (a) virtual node with nonexistent host
	ghostName := fmt.Sprintf("vk-ghost-%s", harness.RunID)
	ghostHost := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "nonexistent-host",
			UID:  "12345-67890",
		},
	}
	_, err := harness.CreateVirtualNode(ctx, ghostName, ghostHost, map[string]string{
		donorcontroller.VirtualNodeLabel: donorcontroller.LabelValueTrue,
	}, true)
	if err != nil {
		t.Fatalf("creating ghost virtual node: %v", err)
	}

	if _, err := harness.WaitForEvent(ctx, "default", "Node", ghostName, "FinalizerRemoved", "release virtual nodes of dead host",
		20*time.Second); err != nil {
		t.Fatalf("waiting for FinalizerRemoved on %s: %v", ghostName, err)
	}
	harness.WaitForNodeDeleted(ctx, t, ghostName, 20*time.Second)

	// (b) Node with finalizer but no virtual-node label
	ghostName2 := fmt.Sprintf("vk-ghost2-%s", harness.RunID)
	_, err = harness.CreateVirtualNode(ctx, ghostName2, ghostHost, map[string]string{}, true)
	if err != nil {
		t.Fatalf("creating ghost node 2: %v", err)
	}

	time.Sleep(15 * time.Second)
	if _, err := harness.Client.CoreV1().Nodes().Get(ctx, ghostName2, metav1.GetOptions{}); err != nil {
		t.Fatalf("node %s should still exist: %v", ghostName2, err)
	}

	// cleanup b
	zero := int64(0)
	vn, err := harness.Client.CoreV1().Nodes().Get(ctx, ghostName2, metav1.GetOptions{})
	if err == nil {
		vn.Finalizers = nil
		if _, err := harness.Client.CoreV1().Nodes().Update(ctx, vn, metav1.UpdateOptions{}); err != nil {
			t.Logf("Update failed: %v", err)
		}
		if err := harness.Client.CoreV1().Nodes().Delete(ctx, ghostName2,
			metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Logf("operation failed: %v", err)
		}
	}

	// (c) virtual node owned by live h2
	h2Node, err := harness.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting %s: %v", h2, err)
	}
	vnH2 := VirtualNodeName(h2)
	_, err = harness.CreateVirtualNode(ctx, vnH2, h2Node, map[string]string{
		donorcontroller.VirtualNodeLabel: donorcontroller.LabelValueTrue,
	}, true)
	if err != nil {
		t.Fatalf("creating virtual node %s: %v", vnH2, err)
	}

	time.Sleep(15 * time.Second)
	if _, err := harness.Client.CoreV1().Nodes().Get(ctx, vnH2, metav1.GetOptions{}); err != nil {
		t.Fatalf("virtual node %s should still exist: %v", vnH2, err)
	}
}

// RunS8HostDeath tests that virtual nodes are released when their host dies.
func RunS8HostDeath(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h3 := harness.FakeHosts[2]
	vnH3 := VirtualNodeName(h3)

	h3Node, err := harness.Client.CoreV1().Nodes().Get(ctx, h3, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting %s: %v", h3, err)
	}

	_, err = harness.CreateVirtualNode(ctx, vnH3, h3Node, map[string]string{
		donorcontroller.VirtualNodeLabel: donorcontroller.LabelValueTrue,
	}, true)
	if err != nil {
		t.Fatalf("creating %s: %v", vnH3, err)
	}

	zero := int64(0)
	if err := harness.Client.CoreV1().Nodes().Delete(ctx, h3,
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}

	harness.WaitForNodeDeleted(ctx, t, vnH3, 20*time.Second)
}

// RunS8bHostRecreated tests that virtual nodes are released if their host is recreated with a new UID.
func RunS8bHostRecreated(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h4 := fmt.Sprintf("dc-e2e-%s-h4", harness.RunID)
	h4Node, err := harness.CreateFakeHostNode(ctx, h4, nil)
	if err != nil {
		t.Fatalf("creating %s: %v", h4, err)
	}

	vnH4 := VirtualNodeName(h4)
	_, err = harness.CreateVirtualNode(ctx, vnH4, h4Node, map[string]string{
		donorcontroller.VirtualNodeLabel: donorcontroller.LabelValueTrue,
	}, true)
	if err != nil {
		t.Fatalf("creating %s: %v", vnH4, err)
	}

	// Delete and immediately recreate h4
	zero := int64(0)
	if err := harness.Client.CoreV1().Nodes().Delete(ctx, h4,
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}

	_, err = harness.CreateFakeHostNode(ctx, h4, nil)
	if err != nil {
		t.Fatalf("recreating %s: %v", h4, err)
	}

	// The virtual node should be released because its owner UID doesn't match h4NodeNew.UID
	harness.WaitForNodeDeleted(ctx, t, vnH4, 20*time.Second)

	if err := harness.Client.CoreV1().Nodes().Delete(ctx, h4,
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}
}

// RunS9SSACreateGuard tests that server-side apply carrying metadata.uid on a nonexistent node
// fails with uid mismatch and the node is not created.
func RunS9SSACreateGuard(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	name := fmt.Sprintf("nonexistent-%s", harness.RunID)
	uid := types.UID("0b0e9a52-1111-4222-8333-944455556666")

	nodeAC := corev1ac.Node(name).WithUID(uid)
	_, err := harness.Client.CoreV1().Nodes().Apply(ctx, nodeAC, metav1.ApplyOptions{
		FieldManager: "e2e-probe",
	})
	if err == nil {
		t.Fatalf("expected server-side apply with metadata.uid on nonexistent node to fail, got nil")
	}
	t.Logf("Observed expected SSA error: %v", err)

	if !strings.Contains(strings.ToLower(err.Error()), "uid") {
		t.Fatalf("expected error to mention 'uid mismatch', got: %v", err)
	}

	// Verify node was not created
	_, err = harness.Client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected node %s to not exist, got: %v", name, err)
	}
}

// RunS10DryRun tests that with SetDryRun(true), donor pods do not cause node sharing;
// toggling back to SetDryRun(false) shares the node.
func RunS10DryRun(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h2 := harness.FakeHosts[1]

	// 1. Enable dry-run
	t.Log("Setting controller dryRun=true...")
	err := harness.Controller.SetDryRun(ctx, true)
	if err != nil {
		t.Fatalf("setting dry-run true: %v", err)
	}
	defer func(ctx context.Context) {
		if err := harness.Controller.SetDryRun(context.WithoutCancel(ctx), false); err != nil {
			t.Logf("operation failed: %v", err)
		}
	}(ctx)

	// 2. Create donor on h2
	_, err = harness.CreateDonorPod(ctx, "donor-dry", h2, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod: %v", err)
	}

	// 3. Wait 15s, verify no LabelDonor on h2
	t.Logf("Waiting 15s to verify no mutations occur on %s during dry-run...", h2)
	time.Sleep(15 * time.Second)
	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h2, err)
	}
	if node.Labels != nil && node.Labels[donorcontroller.LabelDonor] != "" {
		t.Fatalf("node %s was shared despite dry-run being active: %v", h2, node.Labels)
	}

	// 4. Disable dry-run
	t.Log("Setting controller dryRun=false...")
	err = harness.Controller.SetDryRun(ctx, false)
	if err != nil {
		t.Fatalf("setting dry-run false: %v", err)
	}

	// 5. Confirm h2 gets shared
	harness.WaitForNodeShared(ctx, t, h2, "g1", 30*time.Second)

	// 6. Cleanup donor and unshare
	zero := int64(0)
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-dry",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}
	harness.WaitForNodeUnshared(ctx, t, h2, harness.IdleTTL+15*time.Second)
}

// RunS11Ownership tests that managedFields for DonorControllerFieldManager owns exactly
// f:metadata.f:labels {donor, group(s)}.
func RunS11Ownership(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h1 := harness.FakeHosts[0]

	// Ensure h1 is shared with g1 and g2
	_, err := harness.CreateDonorPod(ctx, "donor-s11-a", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	_, err = harness.CreateDonorPod(ctx, "donor-s11-b", h1, "g2", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	harness.WaitForNodeShared(ctx, t, h1, "g1", 30*time.Second)

	// Fetch node with managedFields
	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h1, err)
	}

	var ctrlEntry *metav1.ManagedFieldsEntry
	for i := range node.ManagedFields {
		// Taints are merge-patched under the same manager (an Update entry);
		// this asserts the server-side apply entry.
		if node.ManagedFields[i].Manager == donorcontroller.DonorControllerFieldManager &&
			node.ManagedFields[i].Operation == metav1.ManagedFieldsOperationApply {
			ctrlEntry = &node.ManagedFields[i]
			break
		}
	}
	if ctrlEntry == nil {
		t.Fatalf("managedFields entry for %s not found on %s", donorcontroller.DonorControllerFieldManager, h1)
	}

	var fieldsMap map[string]any
	if err := json.Unmarshal(ctrlEntry.FieldsV1.Raw, &fieldsMap); err != nil {
		t.Fatalf("unmarshaling FieldsV1: %v", err)
	}

	// Verify owned labels
	fMeta, ok := fieldsMap["f:metadata"].(map[string]any)
	if !ok || fMeta == nil {
		t.Fatalf("expected f:metadata in fieldsV1, got %v", fieldsMap)
	}
	fLabels, ok := fMeta["f:labels"].(map[string]any)
	if !ok || fLabels == nil {
		t.Fatalf("expected f:labels in fieldsV1, got %v", fMeta)
	}

	ownedLabelKeys := make(map[string]bool)
	for k := range fLabels {
		ownedLabelKeys[strings.TrimPrefix(k, "f:")] = true
	}

	expectedKeys := map[string]bool{
		donorcontroller.LabelDonor: true,
		donorcontroller.LabelGroup: true,
	}

	if len(ownedLabelKeys) != len(expectedKeys) {
		t.Fatalf("owned labels mismatch: got %v, expected %v", ownedLabelKeys, expectedKeys)
	}
	for k := range expectedKeys {
		if !ownedLabelKeys[k] {
			t.Fatalf("expected owned label %s missing from %v", k, ownedLabelKeys)
		}
	}

	// Verify no unexpected fields owned under f:metadata
	for k := range fMeta {
		if k == "f:labels" {
			continue
		}
		if k == "f:annotations" {
			// Tolerate idle-since only if node is idle
			fAnn, ok := fMeta["f:annotations"].(map[string]any)
			if !ok {
				t.Fatalf("expected f:annotations in metadata")
			}
			for ak := range fAnn {
				if strings.TrimPrefix(ak, "f:") != donorcontroller.AnnotationIdleSince {
					t.Fatalf("unexpected annotation owned: %s", ak)
				}
			}
			continue
		}
		t.Fatalf("unexpected field owned under f:metadata: %s", k)
	}

	// Clean up donors and unshare
	zero := int64(0)
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s11-a",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s11-b",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}
	harness.WaitForNodeUnshared(ctx, t, h1, harness.IdleTTL+15*time.Second)
}

// RunS12ForeignTaintPreservation tests that foreign taints on fake nodes survive both sharing and unsharing.
func RunS12ForeignTaintPreservation(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h2 := harness.FakeHosts[1]
	foreignTaint := corev1.Taint{
		Key:    "e2e/foreign",
		Value:  "x",
		Effect: corev1.TaintEffectNoSchedule,
	}

	// 1. Add foreign taint before sharing
	err := AddTaintToNode(ctx, harness.Client, h2, foreignTaint)
	if err != nil {
		t.Fatalf("adding foreign taint to %s: %v", h2, err)
	}
	defer func() {
		if err := RemoveTaintFromNode(context.WithoutCancel(ctx), harness.Client, h2, foreignTaint.Key); err != nil {
			t.Logf("RemoveTaintFromNode failed: %v", err)
		}
	}()

	// 2. Share h2
	_, err = harness.CreateDonorPod(ctx, "donor-s12", h2, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h2, err)
	}
	harness.WaitForNodeShared(ctx, t, h2, "g1", 30*time.Second)

	// 3. Verify foreign taint survived sharing
	node, err := harness.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h2, err)
	}
	hasForeign := false
	for _, tnt := range node.Spec.Taints {
		if tnt.Key == foreignTaint.Key && tnt.Value == foreignTaint.Value && tnt.Effect == foreignTaint.Effect {
			hasForeign = true
			break
		}
	}
	if !hasForeign {
		t.Fatalf("foreign taint was clobbered during share: %v", node.Spec.Taints)
	}

	// 4. Delete donor and wait for unshare
	zero := int64(0)
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, "donor-s12",
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Logf("operation failed: %v", err)
	}
	harness.WaitForNodeUnshared(ctx, t, h2, harness.IdleTTL+15*time.Second)

	// 5. Verify foreign taint survived unsharing
	node, err = harness.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s after unshare: %v", h2, err)
	}
	hasForeign = false
	for _, tnt := range node.Spec.Taints {
		if tnt.Key == foreignTaint.Key && tnt.Value == foreignTaint.Value && tnt.Effect == foreignTaint.Effect {
			hasForeign = true
			break
		}
	}
	if !hasForeign {
		t.Fatalf("foreign taint was clobbered during unshare: %v", node.Spec.Taints)
	}
}

// RunS13Events verifies that the controller emits Kubernetes Events on every write:
// Applied with rules [share node while donor pods are bound] on node share, Tainted/Untainted when isolation taint is configured,
// and Deleted with rule delete virtual node of deleted host on virtual node deletion.
// RunS13Events verifies that the controller emits Kubernetes Events on every write:
// Applied with rules [share node while donor pods are bound] on node share, Tainted/Untainted when isolation taint is configured,
// FinalizerRemoved with rule release virtual nodes of dead host, and GroupConflict Warning from S2.
func RunS13Events(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	host := harness.FakeHosts[0]
	vkH3 := VirtualNodeName(harness.FakeHosts[2])

	// 1. Verify Applied event on fake host Node (events for cluster-scoped objects land in namespace "default")
	appliedEv, err := harness.WaitForEvent(
		ctx,
		"default",
		"Node",
		host,
		"Applied",
		"share node while donor pods are bound",
		15*time.Second,
	)
	if err != nil {
		t.Fatalf("waiting for Applied event on %s: %v", host, err)
	}
	t.Logf("Found Applied event on %s: %s", host, appliedEv.Message)

	// 2. If isolation taint is configured, verify Tainted and Untainted events
	if harness.IsolationTaint != nil {
		taintedEv, err := harness.WaitForEvent(ctx, "default", "Node", host, "Tainted", "", 15*time.Second)
		if err != nil {
			t.Fatalf("waiting for Tainted event on %s: %v", host, err)
		}
		t.Logf("Found Tainted event on %s: %s", host, taintedEv.Message)

		untaintedEv, err := harness.WaitForEvent(ctx, "default", "Node", host, "Untainted", "", 30*time.Second)
		if err != nil {
			t.Fatalf("waiting for Untainted event on %s: %v", host, err)
		}
		t.Logf("Found Untainted event on %s: %s", host, untaintedEv.Message)
	}

	// 3. Verify FinalizerRemoved event for release virtual nodes of dead host on virtual node vk-h3
	deletedEv, err := harness.WaitForEvent(
		ctx,
		"default",
		"Node",
		vkH3,
		"FinalizerRemoved",
		"release virtual nodes of dead host",
		15*time.Second,
	)
	if err != nil {
		t.Fatalf("waiting for FinalizerRemoved event on %s: %v", vkH3, err)
	}
	t.Logf("Found FinalizerRemoved event on %s: %s", vkH3, deletedEv.Message)

	// 4. Verify GroupConflict Warning from S2
	conflictEv, err := harness.WaitForEvent(ctx, "default", "Node", host, donorcontroller.ReasonGroupConflict, "", 15*time.Second)
	if err != nil {
		t.Fatalf("waiting for GroupConflict event on %s: %v", host, err)
	}
	t.Logf("Found GroupConflict event on %s: %s", host, conflictEv.Message)
}

func RunS14LeaderElection(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	leaseName := "donor-controller.timeslice.io"
	lease, err := harness.Client.CoordinationV1().Leases(harness.Controller.Namespace()).Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting leader election lease %s in %s: %v", leaseName, harness.Controller.Namespace(), err)
	}

	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		t.Fatalf("lease %s in %s has nil or empty holderIdentity", leaseName, harness.Controller.Namespace())
	}
	holder := *lease.Spec.HolderIdentity
	t.Logf("Leader election lease %s holderIdentity: %s", leaseName, holder)

	pods, err := harness.Client.CoreV1().Pods(harness.Controller.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=donor-controller",
	})
	if err != nil {
		t.Fatalf("listing donor-controller pods in %s: %v", harness.Controller.Namespace(), err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("no donor-controller pods found in namespace %s", harness.Controller.Namespace())
	}

	var matchedPod *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if holder == p.Name || strings.HasPrefix(holder, p.Name) {
			matchedPod = p
			break
		}
	}
	if matchedPod == nil {
		podNames := make([]string, 0, len(pods.Items))
		for i := range pods.Items {
			p := pods.Items[i]
			podNames = append(podNames, p.Name)
		}
		t.Fatalf(
			"lease holderIdentity %q does not match any controller pod in %s: %v",
			holder,
			harness.Controller.Namespace(),
			podNames,
		)
	}

	if matchedPod.Status.Phase != corev1.PodRunning {
		t.Fatalf("matched leader pod %s is not Running (phase: %s)", matchedPod.Name, matchedPod.Status.Phase)
	}
	t.Logf("Leader election verified: lease held by running controller pod %s", matchedPod.Name)
}

// RunS15InvalidGroup tests that a donor pod without a group label on h2
// results in an InvalidGroup Warning on the pod and h2, and h2 is not shared.
func RunS15InvalidGroup(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	h2 := harness.FakeHosts[1]
	podName := "donor-s15"
	_, err := harness.CreateDonorPod(ctx, podName, h2, "", nil)
	if err != nil {
		t.Fatalf("creating invalid donor pod: %v", err)
	}

	if _, err := harness.WaitForEvent(ctx, harness.Namespace, "Pod", podName, donorcontroller.ReasonInvalidGroup, "",
		15*time.Second); err != nil {
		t.Fatalf("waiting for InvalidGroup event on pod: %v", err)
	}
	if _, err := harness.WaitForEvent(ctx, "default", "Node", h2, donorcontroller.ReasonInvalidGroup, "",
		15*time.Second); err != nil {
		t.Fatalf("waiting for InvalidGroup event on node: %v", err)
	}

	harness.AssertNodeUntouched(ctx, t, h2, 15*time.Second)
}

// RunFinalCheck asserts that controller logs have no 'forbidden' lines (RBAC check),
// checks for reconcile hot loops (> 50/minute per host), and reports error-level and
// Reconciler error line counts.
func RunFinalCheck(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	logs, err := harness.Controller.Logs(ctx, false)
	if err != nil {
		t.Fatalf("reading controller logs: %v", err)
	}

	// Check forbidden lines and count errors
	var errorLines []string
	var reconcilerErrors []string
	for _, line := range strings.Split(logs, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.Contains(strings.ToLower(trimmed), "forbidden") {
			t.Fatalf("RBAC failure: found 'forbidden' in controller logs: %s", line)
		}
		if strings.Contains(line, `"level":"ERROR"`) || strings.Contains(line, `"level":"error"`) {
			errorLines = append(errorLines, line)
		}
		if strings.Contains(line, "Reconciler error") {
			reconcilerErrors = append(reconcilerErrors, line)
		}
	}

	t.Logf("Controller log check: %d error-level lines, %d Reconciler error occurrences", len(errorLines), len(reconcilerErrors))
	if len(reconcilerErrors) > 0 {
		t.Errorf("Unexpected Reconciler error occurrences in controller logs:\n%s", strings.Join(reconcilerErrors, "\n"))
	}

	// Check hot loops if structured logs are present
	hostTimestamps := make(map[string][]time.Time)
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry struct {
			Time string `json:"time"`
			Host string `json:"host"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err == nil {
			if entry.Host != "" && entry.Time != "" {
				if parsedTime, err := time.Parse(time.RFC3339, entry.Time); err == nil {
					hostTimestamps[entry.Host] = append(hostTimestamps[entry.Host], parsedTime)
				}
			}
		}
	}

	if len(hostTimestamps) == 0 {
		t.Log("Note: skipping hot loop rate calculation because log entries did not include structured host/time fields")
		return
	}

	for host, times := range hostTimestamps {
		for i := range times {
			windowStart := times[i]
			count := 0
			for _, t2 := range times[i:] {
				if t2.Sub(windowStart) <= time.Minute {
					count++
				}
			}
			if count > 50 {
				t.Fatalf("hot loop detected: host %s reconciled %d times (> 50) within one minute", host, count)
			}
		}
	}
	t.Logf(
		"Controller log check passed (0 forbidden lines, max reconciliation rate within limits across %d hosts)",
		len(hostTimestamps),
	)
}

// RunS16StaleFence tests the stale GPU fence cleanup on a fresh fake host hf
// carrying the guest kubelet's fence taint (timeslice.io/gpu-fence=<vk>):
// (a) kept while the virtual node exists; (b) kept while a mirror pod is
// bound to hf, even with the virtual node gone; (c) once the mirror is gone
// too, stale-fence-since appears, and after the grace period the fence (and
// only the fence) is removed, the annotation released, and an Untainted
// Event names the rule.
func RunS16StaleFence(ctx context.Context, t *testing.T, harness *Harness) {
	t.Helper()
	hostName := fmt.Sprintf("dc-e2e-%s-hf", harness.RunID)
	vkName := VirtualNodeName(hostName)
	fence := corev1.Taint{Key: donorcontroller.FenceTaintKey, Value: vkName, Effect: corev1.TaintEffectNoSchedule}
	foreign := corev1.Taint{Key: "e2e/foreign", Value: "x", Effect: corev1.TaintEffectNoSchedule}
	hold := harness.StaleFenceGrace + 10*time.Second

	host, err := harness.CreateFakeHostNode(ctx, hostName, nil)
	if err != nil {
		t.Fatalf("creating fake host %s: %v", hostName, err)
	}
	if _, err := harness.CreateVirtualNode(ctx, vkName, host,
		map[string]string{donorcontroller.VirtualNodeLabel: donorcontroller.LabelValueTrue}, false); err != nil {
		t.Fatalf("creating virtual node %s: %v", vkName, err)
	}
	for _, taint := range []corev1.Taint{foreign, fence} {
		if err := AddTaintToNode(ctx, harness.Client, hostName, taint); err != nil {
			t.Fatalf("adding taint %s to %s: %v", taint.ToString(), hostName, err)
		}
	}

	// (a) The virtual node exists: the guest kubelet owns the fence.
	assertFenceHeld(ctx, t, harness, hostName, fence, hold, "virtual node exists")

	// (b) Virtual node gone, but a mirror pod is still bound to the host.
	mirror := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mirror-s16",
			Namespace: harness.Namespace,
			Labels:    map[string]string{donorcontroller.MirrorNodeLabel: vkName, LabelE2E: harness.RunID},
		},
		Spec: corev1.PodSpec{
			NodeName:    hostName,
			Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers:  []corev1.Container{{Name: "pause", Image: "registry.k8s.io/pause:3.10"}},
		},
	}
	if _, err := harness.Client.CoreV1().Pods(harness.Namespace).Create(ctx, mirror, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating mirror pod: %v", err)
	}
	if err := harness.Client.CoreV1().Nodes().Delete(ctx, vkName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting virtual node %s: %v", vkName, err)
	}
	harness.WaitForNodeDeleted(ctx, t, vkName, 20*time.Second)
	assertFenceHeld(ctx, t, harness, hostName, fence, hold, "mirror pod still bound")

	// (c) Mirror gone too: the fence is stale.
	zero := int64(0)
	if err := harness.Client.CoreV1().Pods(harness.Namespace).Delete(ctx, mirror.Name,
		metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatalf("deleting mirror pod: %v", err)
	}
	start := time.Now()
	var staleSince string
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		node, err := harness.Client.CoreV1().Nodes().Get(ctx, hostName, metav1.GetOptions{})
		if err != nil {
			return false, nil //nolint:nilerr // wait.Poll retries on false,nil
		}
		staleSince = node.Annotations[donorcontroller.AnnotationStaleFenceSince]
		return staleSince != "", nil
	}); err != nil {
		t.Fatalf("waiting for %s on %s: %v", donorcontroller.AnnotationStaleFenceSince, hostName, err)
	}
	t.Logf("%s=%s on %s after %v", donorcontroller.AnnotationStaleFenceSince, staleSince, hostName, time.Since(start))

	var removedAfter time.Duration
	if err := wait.PollUntilContextTimeout(ctx, time.Second,
		harness.StaleFenceGrace+20*time.Second, true,
		func(ctx context.Context) (bool, error) {
			node, err := harness.Client.CoreV1().Nodes().Get(ctx, hostName, metav1.GetOptions{})
			if err != nil {
				return false, nil //nolint:nilerr // wait.Poll retries on false,nil
			}
			if nodeHasTaint(node, fence) {
				return false, nil
			}
			removedAfter = time.Since(start)
			if !nodeHasTaint(node, foreign) {
				return false, fmt.Errorf("foreign taint removed along with the fence: %v", node.Spec.Taints)
			}
			if v, ok := node.Annotations[donorcontroller.AnnotationStaleFenceSince]; ok {
				return false, fmt.Errorf("%s=%s still set after the fence was removed", donorcontroller.AnnotationStaleFenceSince, v)
			}
			return true, nil
		}); err != nil {
		t.Fatalf("waiting for the stale fence to be removed from %s: %v", hostName, err)
	}
	t.Logf(
		"stale fence removed from %s %v after the mirror pod was deleted (grace %v)",
		hostName,
		removedAfter,
		harness.StaleFenceGrace,
	)
	if removedAfter < harness.StaleFenceGrace-3*time.Second {
		t.Errorf("fence removed after %v, before the %v grace period", removedAfter, harness.StaleFenceGrace)
	}
	if _, err := harness.WaitForEvent(ctx, "default", "Node", hostName, "Untainted", "remove stale gpu fence",
		20*time.Second); err != nil {
		t.Errorf("waiting for the Untainted Event on %s: %v", hostName, err)
	}
}

// assertFenceHeld checks every second for duration that host keeps fence and
// gets no stale-fence-since annotation.
func assertFenceHeld(
	ctx context.Context,
	t *testing.T,
	harness *Harness,
	host string,
	fence corev1.Taint,
	duration time.Duration,
	why string,
) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		node, err := harness.Client.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting %s: %v", host, err)
		}
		if !nodeHasTaint(node, fence) {
			t.Fatalf("fence removed from %s although %s: %v", host, why, node.Spec.Taints)
		}
		if v, ok := node.Annotations[donorcontroller.AnnotationStaleFenceSince]; ok {
			t.Fatalf("%s=%s set on %s although %s", donorcontroller.AnnotationStaleFenceSince, v, host, why)
		}
		time.Sleep(time.Second)
	}
	t.Logf("fence kept on %s for %v (%s)", host, duration, why)
}

func nodeHasTaint(node *corev1.Node, taint corev1.Taint) bool {
	for _, tnt := range node.Spec.Taints {
		if tnt.Key == taint.Key && tnt.Value == taint.Value && tnt.Effect == taint.Effect {
			return true
		}
	}
	return false
}
