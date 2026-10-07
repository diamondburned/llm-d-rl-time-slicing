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

package policy

import (
	"context"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1 "k8s.io/api/core/v1"
)

// NewController returns the complete declarative donor-controller specification for rulekit.
func NewController(cfg Config) rulekit.Controller {
	return rulekit.Controller{
		Name:         "donor-controller",
		FieldManager: wellknown.DonorControllerFieldManager,
		Rules:        Rules(cfg),
		Indexes:      []rulekit.Index{PodNodeNameIndex()},
		Owns: func(ctx context.Context, rd rulekit.Reader, key rulekit.Key) ([]rulekit.Ref, error) {
			return []rulekit.Ref{
				{
					GVK:  corev1.SchemeGroupVersion.WithKind("Node"),
					Name: key.Name,
				},
			}, nil
		},
		ManagedTaints: func(key rulekit.Key) []rulekit.ManagedTaint {
			if cfg.IsolationTaint != nil {
				return []rulekit.ManagedTaint{
					{
						Node:   key.Name,
						Key:    cfg.IsolationTaint.Key,
						Effect: cfg.IsolationTaint.Effect,
					},
				}
			}
			return nil
		},
	}
}
