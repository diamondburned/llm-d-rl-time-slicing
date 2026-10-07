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
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/internal/rulekit"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller/wellknown"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type testReader struct {
	client.Reader
	now time.Time
}

func (r testReader) Now() time.Time {
	return r.now
}

func buildTestClient(objs ...client.Object) client.Reader {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithReturnManagedFields().
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, IndexPodNodeName, func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		Build()
}

func TestGatherHost(t *testing.T) {
	fixedNow := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	cfg := Config{IdleTTL: 5 * time.Minute}

	t.Run("terminal pods excluded", func(t *testing.T) {
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1"},
		}
		vnNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: wellknown.VirtualNodeName("host1"),
				Labels: map[string]string{
					wellknown.LabelGuest: wellknown.LabelValueTrue,
				},
			},
		}

		runningDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "donor-running",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		succeededDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "donor-succeeded",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}
		failedDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "donor-failed",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		}

		runningGuest := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "guest-running",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelGuest: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: wellknown.VirtualNodeName("host1")},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		succeededGuest := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "guest-succeeded",
				Namespace: "default",
				Labels:    map[string]string{wellknown.LabelGuest: wellknown.LabelValueTrue},
			},
			Spec:   corev1.PodSpec{NodeName: wellknown.VirtualNodeName("host1")},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}

		cl := buildTestClient(hostNode, vnNode, runningDonor, succeededDonor, failedDonor, runningGuest, succeededGuest)
		rd := testReader{Reader: cl, now: fixedNow}

		d, err := gatherHost(ctx, rd, rulekit.Key{Name: "host1"}, cfg)
		if err != nil {
			t.Fatalf("gatherHost failed: %v", err)
		}

		if len(d.Donors) != 1 || d.Donors[0].Name != "donor-running" {
			t.Errorf("Donors = %v, want only donor-running", d.Donors)
		}
		if len(d.Guests) != 1 || d.Guests[0].Name != "guest-running" {
			t.Errorf("Guests = %v, want only guest-running", d.Guests)
		}
	})

	t.Run("terminating kept", func(t *testing.T) {
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1"},
		}
		delTime := metav1.NewTime(fixedNow)
		terminatingDonor := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "donor-terminating",
				Namespace:         "default",
				Labels:            map[string]string{wellknown.LabelDonor: wellknown.LabelValueTrue},
				Finalizers:        []string{"test.finalizer"},
				DeletionTimestamp: &delTime,
			},
			Spec:   corev1.PodSpec{NodeName: "host1"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}

		cl := buildTestClient(hostNode, terminatingDonor)
		rd := testReader{Reader: cl, now: fixedNow}

		d, err := gatherHost(ctx, rd, rulekit.Key{Name: "host1"}, cfg)
		if err != nil {
			t.Fatalf("gatherHost failed: %v", err)
		}

		if len(d.Donors) != 1 || d.Donors[0].Name != "donor-terminating" {
			t.Errorf("Donors = %v, want terminating donor kept", d.Donors)
		}
	})

	t.Run("VN without guest label ignored", func(t *testing.T) {
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "host1"},
		}
		vnNodeWithoutLabel := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   wellknown.VirtualNodeName("host1"),
				Labels: map[string]string{"unrelated": "true"},
			},
		}

		cl := buildTestClient(hostNode, vnNodeWithoutLabel)
		rd := testReader{Reader: cl, now: fixedNow}

		d, err := gatherHost(ctx, rd, rulekit.Key{Name: "host1"}, cfg)
		if err != nil {
			t.Fatalf("gatherHost failed: %v", err)
		}

		if d.VNode != nil {
			t.Errorf("VNode = %v, want nil because guest label is missing", d.VNode)
		}
	})

	t.Run("key that is a VN -> IsVirtual", func(t *testing.T) {
		vnNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: wellknown.VirtualNodeName("host1"),
				Labels: map[string]string{
					wellknown.LabelGuest: wellknown.LabelValueTrue,
				},
			},
		}

		cl := buildTestClient(vnNode)
		rd := testReader{Reader: cl, now: fixedNow}

		d, err := gatherHost(ctx, rd, rulekit.Key{Name: wellknown.VirtualNodeName("host1")}, cfg)
		if err != nil {
			t.Fatalf("gatherHost failed: %v", err)
		}

		if !d.IsVirtual {
			t.Errorf("IsVirtual = false, want true for virtual node key")
		}
	})

	t.Run("owned labels extracted from managedFields", func(t *testing.T) {
		hostNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "host1",
				Labels: map[string]string{
					wellknown.LabelDonor: wellknown.LabelValueTrue,
					"user-added-label":   "unowned",
				},
				ManagedFields: []metav1.ManagedFieldsEntry{
					{
						Manager:    wellknown.DonorControllerFieldManager,
						Operation:  metav1.ManagedFieldsOperationApply,
						APIVersion: "v1",
						FieldsType: "FieldsV1",
						FieldsV1: &metav1.FieldsV1{
							Raw: []byte(`{"f:metadata":{"f:labels":{"f:timeslice.io/donor":{}}}}`),
						},
					},
				},
			},
		}

		cl := buildTestClient(hostNode)
		rd := testReader{Reader: cl, now: fixedNow}

		d, err := gatherHost(ctx, rd, rulekit.Key{Name: "host1"}, cfg)
		if err != nil {
			t.Fatalf("gatherHost failed: %v", err)
		}

		if d.OwnedLabels == nil || d.OwnedLabels[wellknown.LabelDonor] != wellknown.LabelValueTrue {
			t.Errorf("OwnedLabels = %v, want %s=true", d.OwnedLabels, wellknown.LabelDonor)
		}
		if _, ok := d.OwnedLabels["user-added-label"]; ok {
			t.Errorf("OwnedLabels contains unowned label user-added-label: %v", d.OwnedLabels)
		}
	})
}
