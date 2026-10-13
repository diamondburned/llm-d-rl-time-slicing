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

package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"

	donorcontroller "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/donor-controller"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
)

func TestParseIsolationTaint(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *corev1.Taint
		wantErr bool
	}{
		{
			name:  "empty",
			input: "",
			want:  nil,
		},
		{
			name:  "key=value:NoSchedule",
			input: "foo=bar:NoSchedule",
			want: &corev1.Taint{
				Key:    "foo",
				Value:  "bar",
				Effect: corev1.TaintEffectNoSchedule,
			},
		},
		{
			name:  "key:NoSchedule",
			input: "foo:NoSchedule",
			want: &corev1.Taint{
				Key:    "foo",
				Value:  "",
				Effect: corev1.TaintEffectNoSchedule,
			},
		},
		{
			name:  "key=value:PreferNoSchedule",
			input: "foo=bar:PreferNoSchedule",
			want: &corev1.Taint{
				Key:    "foo",
				Value:  "bar",
				Effect: corev1.TaintEffectPreferNoSchedule,
			},
		},
		{
			name:  "key=value:NoExecute",
			input: "foo=bar:NoExecute",
			want: &corev1.Taint{
				Key:    "foo",
				Value:  "bar",
				Effect: corev1.TaintEffectNoExecute,
			},
		},
		{
			name:    "invalid effect",
			input:   "foo=bar:InvalidEffect",
			wantErr: true,
		},
		{
			name:    "missing effect",
			input:   "foo=bar",
			wantErr: true,
		},
		{
			name:    "empty key",
			input:   "=bar:NoSchedule",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseIsolationTaint(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseIsolationTaint(%q) err = %v, wantErr = %v", tc.input, err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseIsolationTaint(%q) = %+v, want %+v", tc.input, got, tc.want)
			}
		})
	}
}

func TestOrchestratorGroupLocks(t *testing.T) {
	tests := []struct {
		name string
		resp *v1alpha1.GetGroupStatusResponse
		err  error
		want donorcontroller.GroupLock
	}{
		{name: "locked -> active", resp: groupStatusResponse(v1alpha1.GroupStatus_STATE_LOCKED), want: donorcontroller.GroupLockActive},
		{
			name: "switching -> active",
			resp: groupStatusResponse(v1alpha1.GroupStatus_STATE_SWITCHING),
			want: donorcontroller.GroupLockActive,
		},
		{
			name: "vacating (#208 state 7) -> active",
			resp: groupStatusResponse(groupStatusStateVacating),
			want: donorcontroller.GroupLockActive,
		},
		{name: "background (#208 state 6) -> idle", resp: groupStatusResponse(6), want: donorcontroller.GroupLockIdle},
		{name: "idle -> idle", resp: groupStatusResponse(v1alpha1.GroupStatus_STATE_IDLE), want: donorcontroller.GroupLockIdle},
		{
			name: "idle yielded -> idle",
			resp: groupStatusResponse(v1alpha1.GroupStatus_STATE_IDLE_YIELDED),
			want: donorcontroller.GroupLockIdle,
		},
		{
			name: "unknown -> unknown",
			resp: groupStatusResponse(v1alpha1.GroupStatus_STATE_UNKNOWN),
			want: donorcontroller.GroupLockUnknown,
		},
		{name: "NotFound -> idle", err: status.Error(codes.NotFound, "no such group"), want: donorcontroller.GroupLockIdle},
		{name: "unavailable -> unknown", err: status.Error(codes.Unavailable, "down"), want: donorcontroller.GroupLockUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			locks := orchestratorGroupLocks{stubOrchestratorClient{resp: tc.resp, err: tc.err}}
			got, err := locks.GroupLock(context.Background(), "ns.job.group")
			if got != tc.want {
				t.Errorf("GroupLock() = %v, want %v", got, tc.want)
			}
			if (got == donorcontroller.GroupLockUnknown) != (err != nil) {
				t.Errorf("GroupLock() error = %v; want an error exactly when the lock is unknown", err)
			}
		})
	}
}

// TestGroupStatusStateVacatingSurvivesTheWire guards the assumption behind
// groupStatusStateVacating: proto3 enums are open, so a state this tree's
// generated code does not know still decodes to its number.
func TestGroupStatusStateVacatingSurvivesTheWire(t *testing.T) {
	b, err := proto.Marshal(groupStatusResponse(groupStatusStateVacating))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded v1alpha1.GetGroupStatusResponse
	if err := proto.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := decoded.GetGroup().GetGroupState(); got != groupStatusStateVacating {
		t.Errorf("decoded group_state = %v, want %d", got, groupStatusStateVacating)
	}
}

func groupStatusResponse(state v1alpha1.GroupStatus_State) *v1alpha1.GetGroupStatusResponse {
	return &v1alpha1.GetGroupStatusResponse{Group: &v1alpha1.GroupStatus{GroupId: "ns.job.group", GroupState: state}}
}

// stubOrchestratorClient answers GetGroupStatus; every other method panics
// through the nil embedded interface.
type stubOrchestratorClient struct {
	v1alpha1.TimeSliceOrchestratorServiceClient
	resp *v1alpha1.GetGroupStatusResponse
	err  error
}

func (s stubOrchestratorClient) GetGroupStatus(
	_ context.Context, _ *v1alpha1.GetGroupStatusRequest, _ ...grpc.CallOption,
) (*v1alpha1.GetGroupStatusResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.resp == nil {
		return nil, errors.New("stub: no response")
	}
	return s.resp, nil
}
