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
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

// donorsPresent reports whether any donor pods are bound to the node.
func donorsPresent(obs *Observed) bool {
	return len(obs.DonorPods) > 0
}

// eraActive reports whether the controller currently owns the donor label on the node.
func eraActive(obs *Observed) bool {
	return obs.Owned.Labels[DonorLabel] == LabelTrue
}

// vnBusy reports whether a virtual node exists or guest pods remain.
func vnBusy(obs *Observed) bool {
	return obs.VirtualNode != nil || len(obs.GuestPods) > 0
}

// ownedGroups returns all group label keys currently owned on the node.
func ownedGroups(obs *Observed) []string {
	if obs.Owned.Labels == nil {
		return nil
	}
	var groups []string
	for k := range obs.Owned.Labels {
		if strings.HasPrefix(k, NodeGroupLabelPrefix) {
			groups = append(groups, k)
		}
	}
	return groups
}

// donorGroups returns qualified group label keys derived from donor pods bound to the node.
func donorGroups(obs *Observed) []string {
	var groups []string
	for _, pod := range obs.DonorPods {
		if pod == nil || pod.Labels == nil {
			continue
		}
		g := pod.Labels[PodGroupLabel]
		if g == "" {
			continue
		}
		key := NodeGroupLabelPrefix + g
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			continue
		}
		groups = append(groups, key)
	}
	return groups
}

// keepShared asserts the donor label, all accumulated group labels, and isolation taint if configured.
func keepShared(obs *Observed, b *Builder, cfg Config) {
	b.SetLabel(DonorLabel, LabelTrue)
	groups := make(map[string]struct{})
	for _, k := range ownedGroups(obs) {
		groups[k] = struct{}{}
	}
	for _, k := range donorGroups(obs) {
		groups[k] = struct{}{}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		b.SetLabel(k, LabelTrue)
	}
	if cfg.IsolationTaint != nil {
		b.SetIsolationTaint(*cfg.IsolationTaint)
	}
}

// idleSince returns the parsed idle timestamp from owned annotations, or obs.Now truncated to seconds.
func idleSince(obs *Observed) time.Time {
	if obs.Owned.Annotations != nil {
		if raw, ok := obs.Owned.Annotations[IdleSinceAnnotation]; ok {
			if t, err := time.Parse(time.RFC3339, raw); err == nil {
				return t
			}
		}
	}
	return obs.Now.Truncate(time.Second)
}

// Rules returns the donor controller's rule table, in evaluation order:
//
//	| ID            | When                                                                           | Then                                                                                                        |
//	|---------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
//	| W1/W2         | obs.Node != nil && donorsPresent                                                | keepShared (labels {donor:true, groups...}, isolation taint if configured)                                  |
//	| W3/hold       | obs.Node != nil && !donorsPresent && eraActive && vnBusy                       | keepShared (clock reset: no idle annotation)                                                                |
//	| W3/idle-clock | obs.Node != nil && !donorsPresent && eraActive && !vnBusy && now < idleSince+TTL | keepShared, SetAnnotation(IdleSinceAnnotation, idleSince), RequeueAfter(idleSince+TTL-now)                 |
//	| W3/unshare    | obs.Node != nil && !donorsPresent && eraActive && !vnBusy && now >= idleSince+TTL| Require(NoDonorPodsOn{obs.Host}); release labels, annotation, taint (implicit)                      |
//	| W4            | obs.Node == nil && obs.VirtualNode != nil                                       | DeleteVirtualNode(obs.VirtualNode), Require(NodeAbsent{obs.Host})                                           |
func Rules(cfg Config) []Rule {
	return []Rule{
		{
			ID:  "W1/W2",
			Doc: "donor pods bound: share node and join their groups",
			When: func(obs *Observed) bool {
				return obs.Node != nil && donorsPresent(obs)
			},
			Then: func(obs *Observed, b *Builder) {
				keepShared(obs, b, cfg)
			},
		},
		{
			ID:  "W3/hold",
			Doc: "era active, no donors, but virtual node or guests remain: stay shared",
			When: func(obs *Observed) bool {
				return obs.Node != nil && !donorsPresent(obs) && eraActive(obs) && vnBusy(obs)
			},
			Then: func(obs *Observed, b *Builder) {
				keepShared(obs, b, cfg)
			},
		},
		{
			ID:  "W3/idle-clock",
			Doc: "era active and node empty, TTL not yet elapsed: stay shared, run clock",
			When: func(obs *Observed) bool {
				if obs.Node == nil || donorsPresent(obs) || !eraActive(obs) || vnBusy(obs) {
					return false
				}
				if cfg.IdleTTL <= 0 {
					return false
				}
				now := obs.Now.Truncate(time.Second)
				idle := idleSince(obs)
				return now.Before(idle.Add(cfg.IdleTTL))
			},
			Then: func(obs *Observed, b *Builder) {
				keepShared(obs, b, cfg)
				idle := idleSince(obs)
				b.SetAnnotation(IdleSinceAnnotation, idle.Format(time.RFC3339))
				now := obs.Now.Truncate(time.Second)
				rem := idle.Add(cfg.IdleTTL).Sub(now)
				b.RequeueAfter(rem)
			},
		},
		{
			ID:  "W3/unshare",
			Doc: "era active and node empty for TTL: release labels, annotation, taint",
			When: func(obs *Observed) bool {
				if obs.Node == nil || donorsPresent(obs) || !eraActive(obs) || vnBusy(obs) {
					return false
				}
				if cfg.IdleTTL <= 0 {
					return true
				}
				now := obs.Now.Truncate(time.Second)
				idle := idleSince(obs)
				return !now.Before(idle.Add(cfg.IdleTTL))
			},
			Then: func(obs *Observed, b *Builder) {
				b.Require(NoDonorPodsOn{NodeName: obs.Host})
			},
		},
		{
			ID:  "W4",
			Doc: "real node gone but virtual node remains: delete orphan virtual node",
			When: func(obs *Observed) bool {
				return obs.Node == nil && obs.VirtualNode != nil
			},
			Then: func(obs *Observed, b *Builder) {
				b.DeleteVirtualNode(NodeRef{Name: obs.VirtualNode.Name, UID: obs.VirtualNode.UID})
				b.Require(NodeAbsent{Name: obs.Host})
			},
		},
	}
}
