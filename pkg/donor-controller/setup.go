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

// Package donorcontroller wires the donor policy onto a controller-runtime manager; see README.md.
package donorcontroller

import (
	"time"

	rulectrl "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit/ctrl"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Options configures donor controller execution.
type Options struct {
	DryRun  bool
	Workers int
}

// Setup registers the donor controller with the controller-runtime manager.
func Setup(mgr manager.Manager, cfg policy.Config, opts Options) error {
	return rulectrl.Setup(mgr, policy.NewController(cfg), rulectrl.Options{
		DryRun:                  opts.DryRun,
		MaxConcurrentReconciles: opts.Workers,
		Clock:                   time.Now,
	})
}
