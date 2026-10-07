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

// Package policy implements the pure donor controller policy as a table of
// [rulekit.Func] rules over gathered host data.
//
// Rules may only read cluster state through [rulekit.Reader] in Fetch; the
// resulting When and Then functions are pure functions over the gathered
// [*hostData]. No network I/O, clocks, or mutations occur during evaluation.
package policy
