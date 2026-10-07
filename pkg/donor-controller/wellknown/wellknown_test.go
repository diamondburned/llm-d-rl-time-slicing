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

package wellknown_test

import (
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	snapshotutils "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/snapshot-agent/utils"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/infrastructure"
)

func TestWellknownDriftGuard(t *testing.T) {
	if wellknown.NodeGroupLabelPrefix != infrastructure.NodeLabelPrefix {
		t.Errorf("NodeGroupLabelPrefix (%q) != infrastructure.NodeLabelPrefix (%q)",
			wellknown.NodeGroupLabelPrefix, infrastructure.NodeLabelPrefix)
	}
	if wellknown.LabelGroup != infrastructure.PodLabelKey {
		t.Errorf("LabelGroup (%q) != infrastructure.PodLabelKey (%q)",
			wellknown.LabelGroup, infrastructure.PodLabelKey)
	}
	if wellknown.LabelJobID != infrastructure.JobLabelKey {
		t.Errorf("LabelJobID (%q) != infrastructure.JobLabelKey (%q)",
			wellknown.LabelJobID, infrastructure.JobLabelKey)
	}

	if wellknown.LabelJobID != snapshotutils.JobIDLabel {
		t.Errorf("LabelJobID (%q) != snapshotutils.JobIDLabel (%q)",
			wellknown.LabelJobID, snapshotutils.JobIDLabel)
	}
	if wellknown.LabelGroup != snapshotutils.GroupLabel {
		t.Errorf("LabelGroup (%q) != snapshotutils.GroupLabel (%q)",
			wellknown.LabelGroup, snapshotutils.GroupLabel)
	}
}
