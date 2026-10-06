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
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Failed to run donor-controller", "error", err)
		os.Exit(1)
	}
}

func run() error {
	jsonHandler := slog.NewJSONHandler(os.Stdout, nil)
	ctxHandler := logging.NewContextHandler(jsonHandler)
	slog.SetDefault(slog.New(ctxHandler))

	kubeconfig := flag.String("kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	workers := flag.Int("workers", 2, "The number of worker goroutines for the controller")
	idleTTL := flag.Duration("idle-ttl", 5*time.Minute, "Idle TTL before unsharing a node")
	resyncPeriod := flag.Duration("resync-period", 10*time.Minute, "Informer resync period")
	dryRun := flag.Bool("dry-run", false, "Log intended changes without making live API calls")
	isolationTaintFlag := flag.String("isolation-taint", "", "Isolation taint for shared nodes (format: key=value:Effect or key:Effect; valid effects: NoSchedule, PreferNoSchedule, NoExecute)")
	flag.Parse()

	taint, err := parseIsolationTaint(*isolationTaintFlag)
	if err != nil {
		return fmt.Errorf("invalid --isolation-taint flag: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := buildKubeConfig(*kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to load kubernetes config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	cfg := policy.Config{
		IdleTTL:        *idleTTL,
		IsolationTaint: taint,
	}

	opts := controller.Options{
		Workers:      *workers,
		ResyncPeriod: *resyncPeriod,
		DryRun:       *dryRun,
	}

	slog.InfoContext(ctx, "Starting donor-controller",
		"workers", *workers,
		"idleTTL", *idleTTL,
		"resyncPeriod", *resyncPeriod,
		"dryRun", *dryRun,
		"isolationTaint", *isolationTaintFlag,
	)

	rules := policy.Rules(cfg)
	ctrl, err := controller.New(clientset, cfg, rules, opts)
	if err != nil {
		return fmt.Errorf("failed to create donor-controller: %w", err)
	}

	return ctrl.Run(ctx)
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

func buildKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}

	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	slog.Info("In-cluster config failed, trying default local kubeconfig", "error", err)
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	return kubeConfig.ClientConfig()
}
