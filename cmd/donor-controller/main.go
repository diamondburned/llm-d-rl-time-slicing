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
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	rtcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	scheme = runtime.NewScheme()
)

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
	isolationTaintFlag := flag.String("isolation-taint", "", "Isolation taint for shared nodes (format: key=value:Effect or key:Effect; valid effects: NoSchedule, PreferNoSchedule, NoExecute)")
	leaderElect := flag.Bool("leader-elect", false, "Enable leader election for controller manager")
	leaderElectionNamespace := flag.String("leader-election-namespace", "", "Namespace in which the leader election resource will be created (default is in-cluster namespace)")
	metricsBindAddress := flag.String("metrics-bind-address", ":8080", "The address the metrics endpoint binds to (\"0\" disables)")
	healthProbeBindAddress := flag.String("health-probe-bind-address", ":8081", "The address the probe endpoint binds to")
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
				Transform: policy.TrimPod,
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

	cfg := policy.Config{
		IdleTTL:        *idleTTL,
		IsolationTaint: taint,
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
	)

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}
	return nil
}

func parseIsolationTaint(taintStr string) (*corev1.Taint, error) {
	if taintStr == "" {
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
