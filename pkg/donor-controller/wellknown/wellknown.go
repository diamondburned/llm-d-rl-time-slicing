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

// Package wellknown is the single source of truth for the label, annotation
// and naming contract shared by every time-slicing component (webhook,
// orchestrator, snapshot agent, donor controller, guest kubelet, tests).
//
// It is a leaf package: constants and trivial helpers only, no imports beyond
// the standard library, so anything can depend on it.
package wellknown

const (
	// LabelValueTrue is the value used for boolean labels.
	LabelValueTrue = "true"

	// LabelDonor marks donor pods (set by users) and shared real nodes (set
	// by the donor controller). The snapshot-agent and guest-kubelet
	// DaemonSets select on it.
	LabelDonor = "timeslice.io/donor"
	// LabelGuest marks guest pods (set by users) and virtual nodes (set by
	// the guest kubelet).
	LabelGuest = "timeslice.io/guest"
	// LabelGroup carries a pod's time-slicing group (set by the webhook).
	// Mirrors infrastructure.PodLabelKey and utils.GroupLabel.
	LabelGroup = "timeslice.io/group"
	// LabelJobID carries a pod's job identity (set by the webhook).
	// Mirrors infrastructure.JobLabelKey and utils.JobIDLabel.
	LabelJobID = "timeslice.io/job-id"

	// NodeGroupLabelPrefix + <group> = "true" puts a real node in a group.
	// The orchestrator's node watch selects on it. Use NodeGroupLabel.
	// Mirrors infrastructure.NodeLabelPrefix.
	NodeGroupLabelPrefix = "group.timeslice.io/"

	// AnnotationIdleSince records, in RFC 3339, when a shared node was first
	// observed empty. Owned by the donor controller.
	AnnotationIdleSince = "timeslice.io/idle-since"

	// VirtualNodePrefix + <host> names host's virtual node. Use
	// VirtualNodeName.
	VirtualNodePrefix = "vk-"
	// VirtualKubeletProviderIDPrefix + <node name> must be the
	// spec.providerID of every virtual node. Without a providerID, GKE's
	// cloud controller manager finds no backing VM and deletes the node
	// within seconds.
	VirtualKubeletProviderIDPrefix = "virtual-kubelet://"

	// DonorControllerFieldManager is the donor controller's server-side
	// apply field manager; it defines which node fields the controller owns.
	DonorControllerFieldManager = "timeslice-donor-controller"
)

// NodeGroupLabel returns the node label key that puts a node in group.
func NodeGroupLabel(group string) string { return NodeGroupLabelPrefix + group }

// VirtualNodeName returns the name of host's virtual node.
func VirtualNodeName(host string) string { return VirtualNodePrefix + host }

// VirtualNodeProviderID returns the spec.providerID for a virtual node.
func VirtualNodeProviderID(nodeName string) string {
	return VirtualKubeletProviderIDPrefix + nodeName
}
