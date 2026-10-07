//go:build integration

package donorcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
)

// RunS0Baseline tests that fake hosts get no timeslice labels/annotations and no managedFields
// entry for DonorControllerFieldManager within 15 seconds.
func RunS0Baseline(ctx context.Context, t *testing.T, h *Harness) {
	t.Log("Verifying baseline state for fake hosts (15s quiet period)...")
	for _, host := range h.FakeHosts {
		h.AssertNodeUntouched(ctx, t, host, 5*time.Second)
	}

	// Verify team=foo pre-existing label on h1 survived
	h1 := h.FakeHosts[0]
	node, err := h.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting fake host %s: %v", h1, err)
	}
	if node.Labels["team"] != "foo" {
		t.Fatalf("expected pre-existing hand label team=foo on %s, got %v", h1, node.Labels)
	}
}

// RunS1Share tests sharing h1 via donor pod (g1): LabelDonor, NodeGroupLabel(g1), isolation taint,
// team=foo survival, DaemonSet pod scheduled to h1, and h2 remaining untouched.
func RunS1Share(ctx context.Context, t *testing.T, h *Harness) {
	h1 := h.FakeHosts[0]
	h2 := h.FakeHosts[1]

	// 1. Create DaemonSet in test namespace
	_, err := h.CreateDaemonSet(ctx, h.DaemonSetName)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating daemonset: %v", err)
	}

	// 2. Create donor pod bound to h1 with group g1
	_, err = h.CreateDonorPod(ctx, "donor-s1-a", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}

	// 3. Wait for h1 to become shared with g1 and isolation taint
	h.WaitForNodeShared(ctx, t, h1, []string{"g1"}, 30*time.Second)

	// 4. Verify team=foo survived
	node, err := h.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h1, err)
	}
	if node.Labels["team"] != "foo" {
		t.Fatalf("pre-existing label team=foo was overwritten or lost on %s: %v", h1, node.Labels)
	}

	// 5. Verify DaemonSet gets a pod on h1
	_, _ = h.WaitForDaemonSetPodOnNode(ctx, t, h1, 30*time.Second)

	// 6. Verify h2 is untouched
	node2, err := h.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h2, err)
	}
	if node2.Labels != nil {
		if node2.Labels[wellknown.LabelDonor] != "" || node2.Labels[wellknown.NodeGroupLabel("g1")] != "" {
			t.Fatalf("node %s was unexpectedly labeled: %v", h2, node2.Labels)
		}
	}
	for _, tnt := range node2.Spec.Taints {
		if tnt.Key == "timeslice.io/shared" {
			t.Fatalf("node %s unexpectedly got isolation taint: %v", h2, node2.Spec.Taints)
		}
	}
}

// RunS2MultiGroup tests that binding a second donor pod with group g2 accumulates both group labels on h1.
func RunS2MultiGroup(ctx context.Context, t *testing.T, h *Harness) {
	h1 := h.FakeHosts[0]

	_, err := h.CreateDonorPod(ctx, "donor-s2-b", h1, "g2", nil)
	if err != nil {
		t.Fatalf("creating second donor pod on %s: %v", h1, err)
	}

	h.WaitForNodeShared(ctx, t, h1, []string{"g1", "g2"}, 30*time.Second)
}

// RunS3Hold tests that creating vk-h1 and deleting all donor pods holds labels+taint without idle-since.
func RunS3Hold(ctx context.Context, t *testing.T, h *Harness) {
	h1 := h.FakeHosts[0]
	vnName := wellknown.VirtualNodeName(h1)

	// 1. Create virtual node vk-h1
	_, err := h.CreateVirtualNode(ctx, vnName, map[string]string{
		wellknown.LabelGuest: wellknown.LabelValueTrue,
	})
	if err != nil {
		t.Fatalf("creating virtual node %s: %v", vnName, err)
	}

	// 2. Delete donors on h1 (force)
	zero := int64(0)
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s1-a", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s2-b", metav1.DeleteOptions{GracePeriodSeconds: &zero})

	// 3. Wait > TTL + 10s (e.g. 20s + 10s = 30s)
	holdDuration := h.IdleTTL + 10*time.Second
	t.Logf("Waiting %v (> TTL+10s) to verify hold state on %s while virtual node %s exists...", holdDuration, h1, vnName)

	deadline := time.Now().Add(holdDuration)
	for time.Now().Before(deadline) {
		// Verify vk-h1 still exists
		if _, err := h.Client.CoreV1().Nodes().Get(ctx, vnName, metav1.GetOptions{}); err != nil {
			t.Fatalf("virtual node %s disappeared during hold: %v", vnName, err)
		}

		// Verify h1 labels + taint kept and no idle-since annotation
		node, err := h.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting node %s during hold: %v", h1, err)
		}
		if node.Labels[wellknown.LabelDonor] != wellknown.LabelValueTrue {
			t.Fatalf("node %s lost donor label during hold", h1)
		}
		if node.Labels[wellknown.NodeGroupLabel("g1")] != wellknown.LabelValueTrue ||
			node.Labels[wellknown.NodeGroupLabel("g2")] != wellknown.LabelValueTrue {
			t.Fatalf("node %s lost group labels during hold: %v", h1, node.Labels)
		}
		if node.Annotations != nil && node.Annotations[wellknown.AnnotationIdleSince] != "" {
			t.Fatalf("node %s unexpectedly received idle-since annotation during hold: %s", h1, node.Annotations[wellknown.AnnotationIdleSince])
		}

		time.Sleep(3 * time.Second)
	}
}

// RunS4Unshare tests that deleting vk-h1 causes idle-since to appear, followed by unshare on TTL;
// team=foo survives and DaemonSet pod is removed.
func RunS4Unshare(ctx context.Context, t *testing.T, h *Harness) {
	h1 := h.FakeHosts[0]
	vnName := wellknown.VirtualNodeName(h1)

	// 1. Delete virtual node vk-h1
	zero := int64(0)
	err := h.Client.CoreV1().Nodes().Delete(ctx, vnName, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("deleting virtual node %s: %v", vnName, err)
	}

	// 2. idle-since appears within 15s
	_, _ = h.WaitForIdleSince(ctx, t, h1, 15*time.Second)

	// 3. Within TTL+15s: labels, taint, annotation removed
	unshareTimeout := h.IdleTTL + 15*time.Second
	h.WaitForNodeUnshared(ctx, t, h1, unshareTimeout)

	// 4. team=foo survives
	node, err := h.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h1, err)
	}
	if node.Labels["team"] != "foo" {
		t.Fatalf("pre-existing label team=foo was lost after unshare on %s: %v", h1, node.Labels)
	}

	// 5. DaemonSet pod removed from h1
	_ = h.WaitForDaemonSetPodGoneFromNode(ctx, t, h1, 20*time.Second)
}

// RunS5E7 tests that when a donor returns during the idle clock, the donor label remains present
// in every 1s sample for TTL+10s and idle annotation is cleared.
func RunS5E7(ctx context.Context, t *testing.T, h *Harness) {
	h1 := h.FakeHosts[0]

	// 1. Share h1 with donor-c
	_, err := h.CreateDonorPod(ctx, "donor-s5-c", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	h.WaitForNodeShared(ctx, t, h1, []string{"g1"}, 30*time.Second)

	// 2. Delete donor-c to start idle clock
	zero := int64(0)
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s5-c", metav1.DeleteOptions{GracePeriodSeconds: &zero})

	// 3. Wait for idle-since annotation
	_, _ = h.WaitForIdleSince(ctx, t, h1, 15*time.Second)

	// 4. Return immediately with donor-d
	_, err = h.CreateDonorPod(ctx, "donor-s5-d", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}

	// 5. Poll every 1s for TTL+10s: donor label must be present in every sample; idle-since cleared
	sampleDuration := h.IdleTTL + 10*time.Second
	t.Logf("Sampling node %s every 1s for %v to verify E7 convergence...", h1, sampleDuration)
	deadline := time.Now().Add(sampleDuration)
	idleCleared := false

	for time.Now().Before(deadline) {
		node, err := h.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting node %s: %v", h1, err)
		}
		if node.Labels[wellknown.LabelDonor] != wellknown.LabelValueTrue {
			t.Fatalf("donor label was dropped during idle-return convergence on %s", h1)
		}
		if node.Annotations == nil || node.Annotations[wellknown.AnnotationIdleSince] == "" {
			idleCleared = true
		}
		time.Sleep(1 * time.Second)
	}

	if !idleCleared {
		t.Fatalf("idle-since annotation was never cleared after donor returned to %s", h1)
	}

	// 6. Delete donor-d and let it cleanly unshare
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s5-d", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	h.WaitForNodeUnshared(ctx, t, h1, h.IdleTTL+15*time.Second)
}

// RunS6Restart tests that restarting the controller during the idle clock leaves the annotation
// value unchanged, and unshare happens ~TTL after the original idle-since (tolerance +15s).
func RunS6Restart(ctx context.Context, t *testing.T, h *Harness) {
	h1 := h.FakeHosts[0]

	// 1. Share h1 with donor-e
	_, err := h.CreateDonorPod(ctx, "donor-s6-e", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	h.WaitForNodeShared(ctx, t, h1, []string{"g1"}, 30*time.Second)

	// 2. Delete donor-e
	zero := int64(0)
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s6-e", metav1.DeleteOptions{GracePeriodSeconds: &zero})

	// 3. Wait for idle-since annotation and capture timestamp
	idleStr, _ := h.WaitForIdleSince(ctx, t, h1, 15*time.Second)
	tIdle, err := time.Parse(time.RFC3339, idleStr)
	if err != nil {
		t.Fatalf("parsing idle-since timestamp %q: %v", idleStr, err)
	}

	// 4. Restart controller during idle clock
	t.Logf("Restarting donor-controller during idle clock (original idle-since: %s)...", idleStr)
	err = h.Controller.Restart(ctx)
	if err != nil {
		t.Fatalf("restarting controller: %v", err)
	}

	// 5. Verify annotation value on node is unchanged after restart
	node, err := h.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s after restart: %v", h1, err)
	}
	annAfter := node.Annotations[wellknown.AnnotationIdleSince]
	if annAfter != idleStr {
		t.Fatalf("idle-since annotation changed across restart: before=%q, after=%q", idleStr, annAfter)
	}

	// 6. Unshare happens ~TTL after original idle-since (tolerance +15s)
	h.WaitForNodeUnshared(ctx, t, h1, h.IdleTTL+20*time.Second)
	tUnshare := time.Now()
	delta := tUnshare.Sub(tIdle)
	t.Logf("Unshare completed in %v from original idle-since (TTL: %v)", delta, h.IdleTTL)

	// Assert within tolerance: must not unshare earlier than TTL-3s (clock jitter) and not later than TTL+15s
	if delta < h.IdleTTL-3*time.Second {
		t.Fatalf("unshare occurred too early (%v < TTL %v)", delta, h.IdleTTL)
	}
	if delta > h.IdleTTL+15*time.Second {
		t.Fatalf("unshare occurred too late (%v > TTL+15s %v)", delta, h.IdleTTL+15*time.Second)
	}
}

// RunS7W4Guards tests that orphan vk-ghost-<runid> is deleted by controller, but vk-ghost2 (no guest label)
// and vk-h2 (h2 exists) are preserved for at least 15s.
func RunS7W4Guards(ctx context.Context, t *testing.T, h *Harness) {
	// 1. vk-ghost-<runid> with guest label, no host -> deleted
	ghostName := fmt.Sprintf("vk-ghost-%s", h.RunID)
	_, err := h.CreateVirtualNode(ctx, ghostName, map[string]string{
		wellknown.LabelGuest: wellknown.LabelValueTrue,
	})
	if err != nil {
		t.Fatalf("creating %s: %v", ghostName, err)
	}

	h.WaitForNodeDeleted(ctx, t, ghostName, 15*time.Second)

	// 2. vk-ghost2-<runid> with NO guest label -> kept for 15s
	ghost2Name := fmt.Sprintf("vk-ghost2-%s", h.RunID)
	_, err = h.CreateVirtualNode(ctx, ghost2Name, map[string]string{
		"other-label": "true",
	})
	if err != nil {
		t.Fatalf("creating %s: %v", ghost2Name, err)
	}

	t.Logf("Verifying %s is retained for 15s (no guest label)...", ghost2Name)
	time.Sleep(15 * time.Second)
	if _, err := h.Client.CoreV1().Nodes().Get(ctx, ghost2Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("node %s was unexpectedly deleted: %v", ghost2Name, err)
	}
	zero := int64(0)
	_ = h.Client.CoreV1().Nodes().Delete(ctx, ghost2Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})

	// 3. vk-h2 with guest label, but host h2 exists -> kept for 15s
	h2 := h.FakeHosts[1]
	vnH2 := wellknown.VirtualNodeName(h2)
	_, err = h.CreateVirtualNode(ctx, vnH2, map[string]string{
		wellknown.LabelGuest: wellknown.LabelValueTrue,
	})
	if err != nil {
		t.Fatalf("creating %s: %v", vnH2, err)
	}

	t.Logf("Verifying %s is retained for 15s (host %s exists)...", vnH2, h2)
	time.Sleep(15 * time.Second)
	if _, err := h.Client.CoreV1().Nodes().Get(ctx, vnH2, metav1.GetOptions{}); err != nil {
		t.Fatalf("node %s was unexpectedly deleted when host %s exists: %v", vnH2, h2, err)
	}
	_ = h.Client.CoreV1().Nodes().Delete(ctx, vnH2, metav1.DeleteOptions{GracePeriodSeconds: &zero})
}

// RunS8HostDeath tests W4: vk-h3 exists, deleting Node h3 causes the controller to delete vk-h3.
func RunS8HostDeath(ctx context.Context, t *testing.T, h *Harness) {
	h3 := h.FakeHosts[2]
	vnH3 := wellknown.VirtualNodeName(h3)

	// 1. Create vk-h3
	_, err := h.CreateVirtualNode(ctx, vnH3, map[string]string{
		wellknown.LabelGuest: wellknown.LabelValueTrue,
	})
	if err != nil {
		t.Fatalf("creating %s: %v", vnH3, err)
	}

	// 2. Delete fake host Node h3
	zero := int64(0)
	err = h.Client.CoreV1().Nodes().Delete(ctx, h3, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if err != nil {
		t.Fatalf("deleting host %s: %v", h3, err)
	}

	// 3. Controller must delete vk-h3
	h.WaitForNodeDeleted(ctx, t, vnH3, 20*time.Second)
}

// RunS9SSACreateGuard tests that server-side apply carrying metadata.uid on a nonexistent node
// fails with uid mismatch and the node is not created.
func RunS9SSACreateGuard(ctx context.Context, t *testing.T, h *Harness) {
	name := fmt.Sprintf("nonexistent-%s", h.RunID)
	uid := types.UID("0b0e9a52-1111-4222-8333-944455556666")

	nodeAC := corev1ac.Node(name).WithUID(uid)
	_, err := h.Client.CoreV1().Nodes().Apply(ctx, nodeAC, metav1.ApplyOptions{
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
	_, err = h.Client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected node %s to not exist, got: %v", name, err)
	}
}

// RunS10DryRun tests that with SetDryRun(true), donor pods do not cause node sharing;
// toggling back to SetDryRun(false) shares the node.
func RunS10DryRun(ctx context.Context, t *testing.T, h *Harness) {
	h2 := h.FakeHosts[1]

	// 1. Enable dry-run
	t.Log("Setting controller dryRun=true...")
	err := h.Controller.SetDryRun(ctx, true)
	if err != nil {
		t.Fatalf("setting dry-run true: %v", err)
	}
	defer func() {
		_ = h.Controller.SetDryRun(context.Background(), false)
	}()

	// 2. Create donor on h2
	_, err = h.CreateDonorPod(ctx, "donor-dry", h2, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod: %v", err)
	}

	// 3. Wait 15s, verify no LabelDonor on h2
	t.Logf("Waiting 15s to verify no mutations occur on %s during dry-run...", h2)
	time.Sleep(15 * time.Second)
	node, err := h.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h2, err)
	}
	if node.Labels != nil && node.Labels[wellknown.LabelDonor] != "" {
		t.Fatalf("node %s was shared despite dry-run being active: %v", h2, node.Labels)
	}

	// 4. Disable dry-run
	t.Log("Setting controller dryRun=false...")
	err = h.Controller.SetDryRun(ctx, false)
	if err != nil {
		t.Fatalf("setting dry-run false: %v", err)
	}

	// 5. Confirm h2 gets shared
	h.WaitForNodeShared(ctx, t, h2, []string{"g1"}, 30*time.Second)

	// 6. Cleanup donor and unshare
	zero := int64(0)
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-dry", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	h.WaitForNodeUnshared(ctx, t, h2, h.IdleTTL+15*time.Second)
}

// RunS11Ownership tests that managedFields for DonorControllerFieldManager owns exactly
// f:metadata.f:labels {donor, group(s)}.
func RunS11Ownership(ctx context.Context, t *testing.T, h *Harness) {
	h1 := h.FakeHosts[0]

	// Ensure h1 is shared with g1 and g2
	_, err := h.CreateDonorPod(ctx, "donor-s11-a", h1, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	_, err = h.CreateDonorPod(ctx, "donor-s11-b", h1, "g2", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h1, err)
	}
	h.WaitForNodeShared(ctx, t, h1, []string{"g1", "g2"}, 30*time.Second)

	// Fetch node with managedFields
	node, err := h.Client.CoreV1().Nodes().Get(ctx, h1, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", h1, err)
	}

	var ctrlEntry *metav1.ManagedFieldsEntry
	for i := range node.ManagedFields {
		if node.ManagedFields[i].Manager == wellknown.DonorControllerFieldManager {
			ctrlEntry = &node.ManagedFields[i]
			break
		}
	}
	if ctrlEntry == nil {
		t.Fatalf("managedFields entry for %s not found on %s", wellknown.DonorControllerFieldManager, h1)
	}

	var fieldsMap map[string]any
	if err := json.Unmarshal(ctrlEntry.FieldsV1.Raw, &fieldsMap); err != nil {
		t.Fatalf("unmarshaling FieldsV1: %v", err)
	}

	// Verify owned labels
	fMeta, _ := fieldsMap["f:metadata"].(map[string]any)
	if fMeta == nil {
		t.Fatalf("expected f:metadata in fieldsV1, got %v", fieldsMap)
	}
	fLabels, _ := fMeta["f:labels"].(map[string]any)
	if fLabels == nil {
		t.Fatalf("expected f:labels in fieldsV1, got %v", fMeta)
	}

	ownedLabelKeys := make(map[string]bool)
	for k := range fLabels {
		ownedLabelKeys[strings.TrimPrefix(k, "f:")] = true
	}

	expectedKeys := map[string]bool{
		wellknown.LabelDonor:           true,
		wellknown.NodeGroupLabel("g1"): true,
		wellknown.NodeGroupLabel("g2"): true,
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
			fAnn, _ := fMeta["f:annotations"].(map[string]any)
			for ak := range fAnn {
				if strings.TrimPrefix(ak, "f:") != wellknown.AnnotationIdleSince {
					t.Fatalf("unexpected annotation owned: %s", ak)
				}
			}
			continue
		}
		t.Fatalf("unexpected field owned under f:metadata: %s", k)
	}

	// Clean up donors and unshare
	zero := int64(0)
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s11-a", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s11-b", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	h.WaitForNodeUnshared(ctx, t, h1, h.IdleTTL+15*time.Second)
}

// RunS12ForeignTaintPreservation tests that foreign taints on fake nodes survive both sharing and unsharing.
func RunS12ForeignTaintPreservation(ctx context.Context, t *testing.T, h *Harness) {
	h2 := h.FakeHosts[1]
	foreignTaint := corev1.Taint{
		Key:    "e2e/foreign",
		Value:  "x",
		Effect: corev1.TaintEffectNoSchedule,
	}

	// 1. Add foreign taint before sharing
	err := AddTaintToNode(ctx, h.Client, h2, foreignTaint)
	if err != nil {
		t.Fatalf("adding foreign taint to %s: %v", h2, err)
	}
	defer func() {
		_ = RemoveTaintFromNode(context.Background(), h.Client, h2, foreignTaint.Key)
	}()

	// 2. Share h2
	_, err = h.CreateDonorPod(ctx, "donor-s12", h2, "g1", nil)
	if err != nil {
		t.Fatalf("creating donor pod on %s: %v", h2, err)
	}
	h.WaitForNodeShared(ctx, t, h2, []string{"g1"}, 30*time.Second)

	// 3. Verify foreign taint survived sharing
	node, err := h.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
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
	_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, "donor-s12", metav1.DeleteOptions{GracePeriodSeconds: &zero})
	h.WaitForNodeUnshared(ctx, t, h2, h.IdleTTL+15*time.Second)

	// 5. Verify foreign taint survived unsharing
	node, err = h.Client.CoreV1().Nodes().Get(ctx, h2, metav1.GetOptions{})
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
// Applied with rules [W1/W2] on node share, Tainted/Untainted when isolation taint is configured,
// and Deleted with rule W4 on virtual node deletion.
func RunS13Events(ctx context.Context, t *testing.T, h *Harness) {
	host := h.FakeHosts[0]
	vkH3 := wellknown.VirtualNodeName(h.FakeHosts[2])

	// 1. Verify Applied event on fake host Node (events for cluster-scoped objects land in namespace "default")
	appliedEv, err := h.WaitForEvent(ctx, "default", "Node", host, "Applied", "W1/W2", 15*time.Second)
	if err != nil {
		// If run standalone where S1 has not run yet, trigger a share/unshare cycle on host
		t.Logf("Applied event not yet found for %s (%v); triggering share to generate events...", host, err)
		pod, err := h.CreateDonorPod(ctx, "s13-trigger", host, "g1", nil)
		if err != nil {
			t.Fatalf("creating donor pod: %v", err)
		}
		_ = h.WaitForNodeShared(ctx, t, host, []string{"g1"}, 15*time.Second)
		zero := int64(0)
		_ = h.Client.CoreV1().Pods(h.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		_ = h.Client.CoreV1().Nodes().Delete(ctx, wellknown.VirtualNodeName(host), metav1.DeleteOptions{GracePeriodSeconds: &zero})
		appliedEv, err = h.WaitForEvent(ctx, "default", "Node", host, "Applied", "W1/W2", 15*time.Second)
		if err != nil {
			t.Fatalf("waiting for Applied event on %s: %v", host, err)
		}
	}
	t.Logf("Found Applied event on %s: %s", host, appliedEv.Message)

	// 2. If isolation taint is configured, verify Tainted and Untainted events
	if h.IsolationTaint != nil {
		taintedEv, err := h.WaitForEvent(ctx, "default", "Node", host, "Tainted", "", 15*time.Second)
		if err != nil {
			t.Fatalf("waiting for Tainted event on %s: %v", host, err)
		}
		t.Logf("Found Tainted event on %s: %s", host, taintedEv.Message)

		untaintedEv, err := h.WaitForEvent(ctx, "default", "Node", host, "Untainted", "", 30*time.Second)
		if err != nil {
			// In standalone mode if unshare hasn't completed yet, wait for unshare
			_ = h.WaitForNodeUnshared(ctx, t, host, 35*time.Second)
			untaintedEv, err = h.WaitForEvent(ctx, "default", "Node", host, "Untainted", "", 15*time.Second)
			if err != nil {
				t.Fatalf("waiting for Untainted event on %s: %v", host, err)
			}
		}
		t.Logf("Found Untainted event on %s: %s", host, untaintedEv.Message)
	}

	// 3. Verify Deleted event for W4 on virtual node vk-h3
	deletedEv, err := h.WaitForEvent(ctx, "default", "Node", vkH3, "Deleted", "W4", 10*time.Second)
	if err != nil {
		// If run standalone where S8 has not run yet, trigger host death / orphan virtual node
		t.Logf("Deleted event not yet found for %s (%v); triggering orphan cleanup...", vkH3, err)
		zero := int64(0)
		_ = h.Client.CoreV1().Nodes().Delete(ctx, h.FakeHosts[2], metav1.DeleteOptions{GracePeriodSeconds: &zero})
		_, _ = h.CreateVirtualNode(ctx, vkH3, map[string]string{LabelE2E: h.RunID})
		deletedEv, err = h.WaitForEvent(ctx, "default", "Node", vkH3, "Deleted", "W4", 15*time.Second)
		if err != nil {
			t.Fatalf("waiting for Deleted event on %s: %v", vkH3, err)
		}
	}
	t.Logf("Found Deleted event on %s: %s", vkH3, deletedEv.Message)
}

// RunS14LeaderElection verifies that the leader election lease donor-controller.timeslice.io
// exists in DONOR_NAMESPACE and its holderIdentity matches a running controller pod.
func RunS14LeaderElection(ctx context.Context, t *testing.T, h *Harness) {
	leaseName := "donor-controller.timeslice.io"
	lease, err := h.Client.CoordinationV1().Leases(h.Controller.Namespace()).Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting leader election lease %s in %s: %v", leaseName, h.Controller.Namespace(), err)
	}

	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		t.Fatalf("lease %s in %s has nil or empty holderIdentity", leaseName, h.Controller.Namespace())
	}
	holder := *lease.Spec.HolderIdentity
	t.Logf("Leader election lease %s holderIdentity: %s", leaseName, holder)

	pods, err := h.Client.CoreV1().Pods(h.Controller.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=donor-controller",
	})
	if err != nil {
		t.Fatalf("listing donor-controller pods in %s: %v", h.Controller.Namespace(), err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("no donor-controller pods found in namespace %s", h.Controller.Namespace())
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
		var podNames []string
		for _, p := range pods.Items {
			podNames = append(podNames, p.Name)
		}
		t.Fatalf("lease holderIdentity %q does not match any controller pod in %s: %v", holder, h.Controller.Namespace(), podNames)
	}

	if matchedPod.Status.Phase != corev1.PodRunning {
		t.Fatalf("matched leader pod %s is not Running (phase: %s)", matchedPod.Name, matchedPod.Status.Phase)
	}
	t.Logf("Leader election verified: lease held by running controller pod %s", matchedPod.Name)
}

// RunFinalCheck asserts that controller logs have no 'forbidden' lines (RBAC check),
// checks for reconcile hot loops (> 50/minute per host), and reports error-level and
// Reconciler error line counts.
func RunFinalCheck(ctx context.Context, t *testing.T, h *Harness) {
	logs, err := h.Controller.Logs(ctx, false)
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
	t.Logf("Controller log check passed (0 forbidden lines, max reconciliation rate within limits across %d hosts)", len(hostTimestamps))
}
