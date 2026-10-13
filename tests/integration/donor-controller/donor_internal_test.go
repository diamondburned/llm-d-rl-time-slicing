package donorcontroller

import (
	"context"
	"os"
	"testing"
)

// TestDonorController is the comprehensive integration test suite for the donor controller.
// It executes scenarios S0 through S16 against hermetic fake hosts.
//
// It skips unless DONOR_CHART_DEPLOYED is set, so plain `go test ./...` never
// creates Node objects in whatever cluster KUBECONFIG points at.
func TestDonorController(t *testing.T) {
	if os.Getenv("DONOR_CHART_DEPLOYED") == "" {
		t.Skip("requires chart-deployed donor controller (run.sh --phase donor, or set DONOR_CHART_DEPLOYED=1)")
	}

	ctx := context.Background()
	harness := NewHarness(t)

	var failedSubtest string
	runSubtest := func(name string, f func(ctx context.Context, t *testing.T, harness *Harness)) {
		t.Run(name, func(t *testing.T) {
			if failedSubtest != "" {
				t.Skipf("skipping %s because earlier subtest %s failed", name, failedSubtest)
			}
			f(ctx, t, harness)
			if t.Failed() {
				failedSubtest = name
			}
		})
	}

	runSubtest("S0_Baseline", RunS0Baseline)
	runSubtest("S1_Share", RunS1Share)
	runSubtest("S2_GroupConflict", RunS2GroupConflict)
	runSubtest("S3_VirtualNodeDoesNotHold", RunS3VirtualNodeDoesNotHold)
	runSubtest("S4_Unshare", RunS4Unshare)
	runSubtest("S5_E7", RunS5E7)
	runSubtest("S6_Restart", RunS6Restart)
	runSubtest("S7_W4Guards", RunS7W4Guards)
	runSubtest("S8_HostDeath", RunS8HostDeath)
	runSubtest("S8b_HostRecreated", RunS8bHostRecreated)
	runSubtest("S9_SSACreateGuard", RunS9SSACreateGuard)
	runSubtest("S10_DryRun", RunS10DryRun)
	runSubtest("S11_Ownership", RunS11Ownership)
	runSubtest("S12_ForeignTaintPreservation", RunS12ForeignTaintPreservation)
	runSubtest("S13_Events", RunS13Events)
	runSubtest("S14_LeaderElection", RunS14LeaderElection)
	runSubtest("S15_InvalidGroup", RunS15InvalidGroup)
	runSubtest("S16_StaleFence", RunS16StaleFence)
	runSubtest("FinalCheck", RunFinalCheck)
}
