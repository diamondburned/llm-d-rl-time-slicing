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
	"fmt"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NoDonorPodsOn holds if a live LIST finds no non-terminal pods labeled
// LabelDonor=true bound to NodeName.
type NoDonorPodsOn struct {
	NodeName string
}

func (p NoDonorPodsOn) String() string {
	return "no donor pods on " + p.NodeName
}

// Check verifies that no non-terminal donor pods remain bound to NodeName on the live API.
func (p NoDonorPodsOn) Check(ctx context.Context, live client.Reader) error {
	var podList corev1.PodList
	err := live.List(ctx, &podList,
		client.MatchingLabels{wellknown.LabelDonor: wellknown.LabelValueTrue},
		client.MatchingFields{IndexPodNodeName: p.NodeName},
	)
	if err != nil {
		return err
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		// Filter client-side by Spec.NodeName and non-terminal (fakes may ignore field selectors).
		if pod.Spec.NodeName != p.NodeName {
			continue
		}
		if pod.Labels == nil || pod.Labels[wellknown.LabelDonor] != wellknown.LabelValueTrue {
			continue
		}
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return fmt.Errorf("%w: donor pod %s/%s still present on node %s",
				rulekit.ErrPreconditionFailed, pod.Namespace, pod.Name, p.NodeName)
		}
	}
	return nil
}

// NodeAbsent holds if a live GET of the node returns NotFound.
type NodeAbsent struct {
	Name string
}

func (p NodeAbsent) String() string {
	return "node " + p.Name + " absent"
}

// Check verifies that the node does not exist on the live API.
func (p NodeAbsent) Check(ctx context.Context, live client.Reader) error {
	var n corev1.Node
	err := live.Get(ctx, client.ObjectKey{Name: p.Name}, &n)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return fmt.Errorf("%w: node %s still exists", rulekit.ErrPreconditionFailed, p.Name)
}
