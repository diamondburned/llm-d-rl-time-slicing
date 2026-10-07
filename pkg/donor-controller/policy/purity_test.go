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
	"bufio"
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

func TestPurity(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go binary not found on PATH")
	}

	banned := []string{
		"k8s.io/client-go/kubernetes",
		"k8s.io/client-go/rest",
		"k8s.io/client-go/dynamic",
		"sigs.k8s.io/controller-runtime/pkg/manager",
		"sigs.k8s.io/controller-runtime/pkg/cache",
		"sigs.k8s.io/controller-runtime/pkg/builder",
		"sigs.k8s.io/controller-runtime/pkg/controller",
		"sigs.k8s.io/controller-runtime/pkg/client/config",
	}

	pkgs := []string{".", "../internal/rulekit"}
	for _, pkg := range pkgs {
		cmd := exec.Command(goBin, "list", "-f", `{{join .Imports "\n"}}`, pkg)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go list for %s failed: %v, output: %s", pkg, err, string(out))
		}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for scanner.Scan() {
			imp := strings.TrimSpace(scanner.Text())
			if imp == "" {
				continue
			}
			for _, b := range banned {
				if imp == b || strings.HasPrefix(imp, b+"/") {
					t.Errorf("package %s directly imports banned package: %s", pkg, imp)
				}
			}
			if strings.Contains(imp, "pkg/donor-controller/internal/rulekit/ctrl") {
				t.Errorf("package %s directly imports anything under pkg/donor-controller/internal/rulekit/ctrl: %s", pkg, imp)
			}
		}
	}
}
