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

// Package controller: executor.go is THE ONLY API WRITER in the donor controller.
//
// No other component in this package or the policy package may write to the Kubernetes API.
// All mutations, creations, server-side applies, patches, and deletions MUST originate here.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/policy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
)

// ErrPreconditionFailed indicates that one of the plan's live preconditions failed.
var ErrPreconditionFailed = errors.New("precondition failed")

// Executor executes policy plans against the Kubernetes API.
// It is THE ONLY API WRITER in the donor controller.
type Executor struct {
	Client         kubernetes.Interface
	IsolationTaint *corev1.Taint // same as policy.Config.IsolationTaint
	DryRun         bool
	Log            *slog.Logger
}

// Execute applies the given plan to the cluster, observing live preconditions.
func (e *Executor) Execute(ctx context.Context, obs *policy.Observed, plan policy.Plan) error {
	log := e.Log
	if log == nil {
		log = slog.Default()
	}

	// 1. If plan is a no-op, perform zero API calls.
	if plan.IsNoop(obs) {
		return nil
	}

	// 5 (DryRun): in dry run, skip ALL API calls (including precondition checks)
	// and instead log each intended effect at Info, then return nil.
	if e.DryRun {
		if plan.Node != nil && obs.Node != nil {
			if !maps.Equal(plan.Node.Labels, obs.Owned.Labels) || !maps.Equal(plan.Node.Annotations, obs.Owned.Annotations) {
				log.Info("DryRun: would apply node metadata",
					"host", obs.Host,
					"fired", plan.Fired,
					"labels", plan.Node.Labels,
					"annotations", plan.Node.Annotations,
				)
			}
			if e.IsolationTaint != nil && (plan.Node.IsolationTaint != nil) != obs.HasIsolationTaint {
				log.Info("DryRun: would patch node taints",
					"host", obs.Host,
					"fired", plan.Fired,
					"isolationTaint", plan.Node.IsolationTaint,
				)
			}
		}
		if plan.DeleteVirtualNode != nil {
			log.Info("DryRun: would delete virtual node",
				"host", obs.Host,
				"fired", plan.Fired,
				"virtualNode", plan.DeleteVirtualNode.Name,
				"uid", plan.DeleteVirtualNode.UID,
			)
		}
		return nil
	}

	// 2. Check every precondition with LIVE (uncached) calls.
	// If any fails, return ErrPreconditionFailed and apply NOTHING.
	for _, p := range plan.Preconditions {
		switch pre := p.(type) {
		case policy.NoDonorPodsOn:
			podList, err := e.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
				LabelSelector: policy.DonorLabel + "=true",
				FieldSelector: "spec.nodeName=" + pre.NodeName,
			})
			if err != nil {
				return fmt.Errorf("list donor pods for precondition %s: %w", pre, err)
			}
			remaining := 0
			for _, pod := range podList.Items {
				if pod.Spec.NodeName == pre.NodeName &&
					pod.Status.Phase != corev1.PodSucceeded &&
					pod.Status.Phase != corev1.PodFailed {
					remaining++
				}
			}
			if remaining > 0 {
				return fmt.Errorf("%w: %s (%d donor pods remain)", ErrPreconditionFailed, pre, remaining)
			}
		case policy.NodeAbsent:
			_, err := e.Client.CoreV1().Nodes().Get(ctx, pre.Name, metav1.GetOptions{})
			if err == nil {
				return fmt.Errorf("%w: %s (node still exists)", ErrPreconditionFailed, pre)
			}
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("get node for precondition %s: %w", pre, err)
			}
		default:
			return fmt.Errorf("unknown precondition type: %T", p)
		}
	}

	// 3. Mutate real node state if requested.
	currentNode := obs.Node
	var latestRV string
	if obs.Node != nil {
		latestRV = obs.Node.ResourceVersion
	}

	if plan.Node != nil && obs.Node != nil {
		// 3a. Metadata via server-side apply, only if labels/annotations differ from obs.Owned.
		if !maps.Equal(plan.Node.Labels, obs.Owned.Labels) || !maps.Equal(plan.Node.Annotations, obs.Owned.Annotations) {
			// Two preconditions ride along in the apply config:
			//   - uid: SSA is an upsert, so without it a stale cache could make us
			//     re-create a Node that was just deleted (host death), which would
			//     also block W4. With a uid set, the apiserver rejects the create
			//     ("uid mismatch ... no existing object was found") and rejects a
			//     different incarnation of the same name. A resourceVersion alone
			//     does NOT prevent the create (verified on a real apiserver).
			//   - resourceVersion: optimistic concurrency against the snapshot.
			ac := corev1ac.Node(obs.Host).
				WithUID(obs.Node.UID).
				WithResourceVersion(latestRV)
			// Only specify WithLabels / WithAnnotations when non-empty. An empty apply config
			// releases all previously owned fields for this fieldManager.
			if len(plan.Node.Labels) > 0 {
				ac = ac.WithLabels(plan.Node.Labels)
			}
			if len(plan.Node.Annotations) > 0 {
				ac = ac.WithAnnotations(plan.Node.Annotations)
			}
			applied, err := e.Client.CoreV1().Nodes().Apply(ctx, ac, metav1.ApplyOptions{
				FieldManager: policy.FieldManager,
				Force:        true,
			})
			if err != nil {
				return fmt.Errorf("apply node metadata for %s: %w", obs.Host, err)
			}
			log.Info("Applied node metadata",
				"host", obs.Host,
				"fired", plan.Fired,
				"labels", plan.Node.Labels,
				"annotations", plan.Node.Annotations,
				"resourceVersion", applied.ResourceVersion,
			)
			currentNode = applied
			latestRV = applied.ResourceVersion
		}

		// 3b. Taint merge patch.
		// Taints are listType=atomic in Kubernetes, meaning server-side apply cannot manage
		// individual taints without claiming or overwriting the entire list. If we used SSA here,
		// we would wipe out or conflict with taints managed by other actors (e.g., cloud-provider,
		// kubelet, or user-applied taints). Therefore, we read-modify-write the taint list using
		// a JSON merge-patch with a resourceVersion optimistic concurrency precondition.
		if e.IsolationTaint != nil && (plan.Node.IsolationTaint != nil) != obs.HasIsolationTaint {
			var newTaints []corev1.Taint
			if currentNode != nil {
				for _, t := range currentNode.Spec.Taints {
					if t.Key == e.IsolationTaint.Key && t.Effect == e.IsolationTaint.Effect {
						continue
					}
					newTaints = append(newTaints, t)
				}
			}
			if plan.Node.IsolationTaint != nil {
				newTaints = append(newTaints, *plan.Node.IsolationTaint)
			}

			patchData := map[string]any{
				"metadata": map[string]any{
					"resourceVersion": latestRV,
				},
			}
			if len(newTaints) == 0 {
				patchData["spec"] = map[string]any{
					"taints": nil,
				}
			} else {
				patchData["spec"] = map[string]any{
					"taints": newTaints,
				}
			}

			patchBytes, err := json.Marshal(patchData)
			if err != nil {
				return fmt.Errorf("marshal taint patch for %s: %w", obs.Host, err)
			}

			patched, err := e.Client.CoreV1().Nodes().Patch(ctx, obs.Host, types.MergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("patch node taints for %s: %w", obs.Host, err)
			}
			log.Info("Patched node taints",
				"host", obs.Host,
				"fired", plan.Fired,
				"isolationTaint", plan.Node.IsolationTaint,
				"resourceVersion", patched.ResourceVersion,
			)
			currentNode = patched
			latestRV = patched.ResourceVersion
		}
	}

	// 4. Delete virtual node if requested.
	if plan.DeleteVirtualNode != nil {
		name := plan.DeleteVirtualNode.Name
		uid := plan.DeleteVirtualNode.UID
		err := e.Client.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID: &uid,
			},
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete virtual node %s: %w", name, err)
		}
		log.Info("Deleted virtual node",
			"host", obs.Host,
			"fired", plan.Fired,
			"virtualNode", name,
			"uid", uid,
		)
	}

	return nil
}
