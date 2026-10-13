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

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	rtcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	if err := run(); err != nil {
		slog.Error("Failed to run donor-controller", "error", err)
		os.Exit(1)
	}
}

func run() error {
	jsonHandler := slog.NewJSONHandler(os.Stdout, nil)
	ctxHandler := logging.NewContextHandler(jsonHandler)
	logger := slog.New(ctxHandler)
	slog.SetDefault(logger)
	ctrl.SetLogger(logr.FromSlogHandler(ctxHandler))

	workers := flag.Int("workers", 2, "The number of worker goroutines for the controller")
	idleTTL := flag.Duration("idle-ttl", 5*time.Minute, "Idle TTL before unsharing a node")
	resyncPeriod := flag.Duration("resync-period", 10*time.Minute, "Informer resync period")
	dryRun := flag.Bool("dry-run", false, "Log intended changes without making live API calls")
	isolationTaintFlag := flag.String("isolation-taint", "",
		"Isolation taint for shared nodes (format: key=value:Effect or key:Effect; "+
			"valid effects: NoSchedule, PreferNoSchedule, NoExecute)")
	leaderElect := flag.Bool("leader-elect", false, "Enable leader election for controller manager")
	leaderElectionNamespace := flag.String("leader-election-namespace", "",
		"Namespace in which the leader election resource will be created (default is in-cluster namespace)")
	metricsBindAddress := flag.String("metrics-bind-address", ":8080", "The address the metrics endpoint binds to (\"0\" disables)")
	healthProbeBindAddress := flag.String("health-probe-bind-address", ":8081", "The address the probe endpoint binds to")
	orchestratorAddr := flag.String("orchestrator-addr", "",
		"host:port of the orchestrator's gRPC API; a shared host stays shared while its group's lock is active. "+
			"Empty: donor pods alone decide")
	groupLockPollInterval := flag.Duration("group-lock-poll-interval", 2*time.Second,
		"How often an idle shared host re-reads its group's lock state")
	clearStaleFences := flag.Bool("clear-stale-fences", true,
		"Remove a host's timeslice.io/gpu-fence taint once the virtual node it names has been gone for "+
			"--stale-fence-grace and no mirror pod is left")
	staleFenceGrace := flag.Duration("stale-fence-grace", time.Minute,
		"How long a GPU fence must have been stale before it is removed")
	flag.Parse()

	taint, err := parseIsolationTaint(*isolationTaintFlag)
	if err != nil {
		return fmt.Errorf("invalid --isolation-taint flag: %w", err)
	}

	restConfig, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("unable to load kubernetes config: %w", err)
	}

	metricsOpts := metricsserver.Options{
		BindAddress: *metricsBindAddress,
	}

	cacheOpts := rtcache.Options{
		SyncPeriod: resyncPeriod,
		ByObject: map[client.Object]rtcache.ByObject{
			&corev1.Pod{}: {
				// The controller reads donor and mirror pods, and no label
				// selector matches both, so cache every pod, trimmed.
				Transform: donorcontroller.TrimPod,
			},
		},
	}

	mgrOpts := ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsOpts,
		HealthProbeBindAddress:        *healthProbeBindAddress,
		LeaderElection:                *leaderElect,
		LeaderElectionID:              "donor-controller.timeslice.io",
		LeaderElectionReleaseOnCancel: true,
		LeaderElectionNamespace:       *leaderElectionNamespace,
		Cache:                         cacheOpts,
	}

	mgr, err := ctrl.NewManager(restConfig, mgrOpts)
	if err != nil {
		return fmt.Errorf("unable to create manager: %w", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	cfg := donorcontroller.Config{
		IdleGracePeriod:       *idleTTL,
		IsolationTaint:        taint,
		GroupLockPollInterval: *groupLockPollInterval,
		ClearStaleFences:      *clearStaleFences,
		StaleFenceGracePeriod: *staleFenceGrace,
	}
	if *orchestratorAddr != "" {
		conn, err := grpc.NewClient(*orchestratorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("orchestrator client for %s: %w", *orchestratorAddr, err)
		}
		defer conn.Close()
		cfg.GroupLocks = orchestratorGroupLocks{v1alpha1.NewTimeSliceOrchestratorServiceClient(conn)}
	}

	if err := donorcontroller.Setup(mgr, cfg, donorcontroller.Options{
		DryRun:  *dryRun,
		Workers: *workers,
	}); err != nil {
		return fmt.Errorf("unable to set up donor-controller: %w", err)
	}

	ctx := ctrl.SetupSignalHandler()

	slog.InfoContext(ctx, "Starting donor-controller",
		"workers", *workers,
		"idleTTL", *idleTTL,
		"resyncPeriod", *resyncPeriod,
		"dryRun", *dryRun,
		"isolationTaint", *isolationTaintFlag,
		"leaderElect", *leaderElect,
		"leaderElectionNamespace", *leaderElectionNamespace,
		"metricsBindAddress", *metricsBindAddress,
		"healthProbeBindAddress", *healthProbeBindAddress,
		"orchestratorAddr", *orchestratorAddr,
		"groupLockPollInterval", *groupLockPollInterval,
		"clearStaleFences", *clearStaleFences,
		"staleFenceGrace", *staleFenceGrace,
	)

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}
	return nil
}

// orchestratorGroupLocks reads group lock states from the orchestrator's
// GetGroupStatus.
type orchestratorGroupLocks struct {
	client v1alpha1.TimeSliceOrchestratorServiceClient
}

// orchestratorRPCTimeout bounds one GetGroupStatus call.
const orchestratorRPCTimeout = time.Second

// groupStatusStateVacating is STATE_VACATING from #208's orchestrator proto: a
// foreground job wants the accelerator and the background guests are being
// told to leave. This tree's generated code predates it, but proto3 enums are
// open, so the value still arrives as GroupStatus_State(7). Replace it with
// v1alpha1.GroupStatus_STATE_VACATING once that proto is on main.
const groupStatusStateVacating v1alpha1.GroupStatus_State = 7

func (o orchestratorGroupLocks) GroupLock(ctx context.Context, group string) (donorcontroller.GroupLock, error) {
	ctx, cancel := context.WithTimeout(ctx, orchestratorRPCTimeout)
	defer cancel()
	resp, err := o.client.GetGroupStatus(ctx, &v1alpha1.GetGroupStatusRequest{GroupId: group})
	switch {
	case status.Code(err) == codes.NotFound:
		return donorcontroller.GroupLockIdle, nil // the orchestrator does not know the group
	case err != nil:
		return donorcontroller.GroupLockUnknown, err
	}
	switch state := resp.GetGroup().GetGroupState(); state {
	case v1alpha1.GroupStatus_STATE_LOCKED, v1alpha1.GroupStatus_STATE_SWITCHING, groupStatusStateVacating:
		return donorcontroller.GroupLockActive, nil
	case v1alpha1.GroupStatus_STATE_UNKNOWN:
		return donorcontroller.GroupLockUnknown, fmt.Errorf("orchestrator reports group %q as %s", group, state)
	default:
		return donorcontroller.GroupLockIdle, nil
	}
}

func parseIsolationTaint(taintStr string) (*corev1.Taint, error) {
	if taintStr == "" {
		//nolint:nilnil // returning nil taint and nil error indicates no taint configured
		return nil, nil
	}

	parts := strings.Split(taintStr, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid taint format %q: expected key=value:Effect or key:Effect", taintStr)
	}

	keyVal := parts[0]
	effectStr := parts[1]

	var effect corev1.TaintEffect
	switch effectStr {
	case string(corev1.TaintEffectNoSchedule):
		effect = corev1.TaintEffectNoSchedule
	case string(corev1.TaintEffectPreferNoSchedule):
		effect = corev1.TaintEffectPreferNoSchedule
	case string(corev1.TaintEffectNoExecute):
		effect = corev1.TaintEffectNoExecute
	default:
		return nil, fmt.Errorf("invalid taint effect %q: must be NoSchedule, PreferNoSchedule, or NoExecute", effectStr)
	}

	var key, value string
	if idx := strings.Index(keyVal, "="); idx >= 0 {
		key = keyVal[:idx]
		value = keyVal[idx+1:]
	} else {
		key = keyVal
	}

	if key == "" {
		return nil, fmt.Errorf("taint key cannot be empty in %q", taintStr)
	}

	return &corev1.Taint{
		Key:    key,
		Value:  value,
		Effect: effect,
	}, nil
}
