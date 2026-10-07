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
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
)

// fetchHost returns the shared fetch closure for hostData rules.
func fetchHost(cfg Config) func(context.Context, rulekit.Reader, rulekit.Key) (*hostData, error) {
	return func(ctx context.Context, rd rulekit.Reader, key rulekit.Key) (*hostData, error) {
		return gatherHost(ctx, rd, key, cfg)
	}
}

// Rules returns the donor controller's rule table, in evaluation order:
//
//	| ID            | When                                                                           | Then                                                                                                     |
//	|---------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
//	| W1/W2         | !IsVirtual && Node != nil && donorsPresent                                     | keepShared (labels {donor:true, groups...}, isolation taint if configured)                              |
//	| W3/hold       | !IsVirtual && Node != nil && !donorsPresent && eraActive && vnBusy             | keepShared (clock reset: no idle annotation)                                                            |
//	| W3/idle-clock | !IsVirtual && Node != nil && eraActive && empty && now < idleSince+TTL         | keepShared + Apply(Node.WithAnnotations({idleSince})) + WakeAt(idleSince+TTL)                          |
//	| W3/unshare    | !IsVirtual && Node != nil && eraActive && empty && now >= idleSince+TTL        | Require(NoDonorPodsOn{host}); release labels, annotation, taint (implicit)                              |
//	| W4            | !IsVirtual && Node == nil && VNode != nil                                      | Delete(VNode); Require(NodeAbsent{host})                                                                |
func Rules(cfg Config) []rulekit.Rule {
	fetch := fetchHost(cfg)
	watches := hostSources()

	return []rulekit.Rule{
		rulekit.Func[*hostData]{
			Name:    "W1/W2",
			Watches: watches,
			Fetch:   fetch,
			When: func(d *hostData) bool {
				return !d.IsVirtual && d.Node != nil && d.donorsPresent()
			},
			Then: func(d *hostData, fx *rulekit.Effects) {
				keepShared(d, fx, cfg)
			},
		},
		rulekit.Func[*hostData]{
			Name:    "W3/hold",
			Watches: watches,
			Fetch:   fetch,
			When: func(d *hostData) bool {
				return !d.IsVirtual && d.Node != nil && !d.donorsPresent() && d.eraActive() && d.vnBusy()
			},
			Then: func(d *hostData, fx *rulekit.Effects) {
				keepShared(d, fx, cfg)
			},
		},
		rulekit.Func[*hostData]{
			Name:    "W3/idle-clock",
			Watches: watches,
			Fetch:   fetch,
			When: func(d *hostData) bool {
				if d.IsVirtual || d.Node == nil || !d.eraActive() || !d.empty() || cfg.IdleTTL <= 0 {
					return false
				}
				now := d.Now.Truncate(time.Second)
				idle := d.idleSince()
				return now.Before(idle.Add(cfg.IdleTTL))
			},
			Then: func(d *hostData, fx *rulekit.Effects) {
				keepShared(d, fx, cfg)
				idle := d.idleSince()
				fx.Apply(corev1ac.Node(d.Host).WithAnnotations(map[string]string{
					wellknown.AnnotationIdleSince: idle.Format(time.RFC3339),
				}))
				fx.WakeAt(idle.Add(cfg.IdleTTL))
			},
		},
		rulekit.Func[*hostData]{
			Name:    "W3/unshare",
			Watches: watches,
			Fetch:   fetch,
			When: func(d *hostData) bool {
				if d.IsVirtual || d.Node == nil || !d.eraActive() || !d.empty() {
					return false
				}
				if cfg.IdleTTL <= 0 {
					return true
				}
				now := d.Now.Truncate(time.Second)
				idle := d.idleSince()
				return !now.Before(idle.Add(cfg.IdleTTL))
			},
			Then: func(d *hostData, fx *rulekit.Effects) {
				fx.Require(NoDonorPodsOn{NodeName: d.Host})
			},
		},
		rulekit.Func[*hostData]{
			Name:    "W4",
			Watches: watches,
			Fetch:   fetch,
			When: func(d *hostData) bool {
				return !d.IsVirtual && d.Node == nil && d.VNode != nil
			},
			Then: func(d *hostData, fx *rulekit.Effects) {
				fx.Delete(d.VNode)
				fx.Require(NodeAbsent{Name: d.Host})
			},
		},
	}
}
