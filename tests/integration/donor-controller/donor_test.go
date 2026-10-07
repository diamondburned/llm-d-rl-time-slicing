//go:build integration

package donorcontroller

import (
	"context"
	"testing"
)

// TestDonorController is the comprehensive integration test suite for the donor controller.
// It executes scenarios S0 through S12 against hermetic fake hosts.
func TestDonorController(t *testing.T) {
	ctx := context.Background()
	h := NewHarness(t)

	var failedSubtest string
	runSubtest := func(name string, f func(ctx context.Context, t *testing.T, h *Harness)) {
		t.Run(name, func(t *testing.T) {
			if failedSubtest != "" {
				t.Skipf("skipping %s because earlier subtest %s failed", name, failedSubtest)
			}
			f(ctx, t, h)
			if t.Failed() {
				failedSubtest = name
			}
		})
	}

	runSubtest("S0_Baseline", RunS0Baseline)
	runSubtest("S1_Share", RunS1Share)
	runSubtest("S2_MultiGroup", RunS2MultiGroup)
	runSubtest("S3_Hold", RunS3Hold)
	runSubtest("S4_Unshare", RunS4Unshare)
	runSubtest("S5_E7", RunS5E7)
	runSubtest("S6_Restart", RunS6Restart)
	runSubtest("S7_W4Guards", RunS7W4Guards)
	runSubtest("S8_HostDeath", RunS8HostDeath)
	runSubtest("S9_SSACreateGuard", RunS9SSACreateGuard)
	runSubtest("S10_DryRun", RunS10DryRun)
	runSubtest("S11_Ownership", RunS11Ownership)
	runSubtest("S12_ForeignTaintPreservation", RunS12ForeignTaintPreservation)
	runSubtest("S13_Events", RunS13Events)
	runSubtest("S14_LeaderElection", RunS14LeaderElection)
	runSubtest("FinalCheck", RunFinalCheck)
}
