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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
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
