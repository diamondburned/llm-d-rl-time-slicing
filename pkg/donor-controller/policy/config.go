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
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Config holds the donor controller policy knobs.
type Config struct {
	// IdleTTL is how long a shared node must stay empty (no donors, no guests,
	// no virtual node) before the controller unshares it (W3).
	IdleTTL time.Duration
	// IsolationTaint, if non-nil, is kept on shared nodes (W5). The controller
	// owns exactly the taint with this key; other taints are never touched.
	IsolationTaint *corev1.Taint
}
