//go:build e2e

// /*
// Copyright 2026 The Grove Authors.
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
// */

// Package koordinator contains end-to-end tests for the koord-scheduler backend.
// These tests verify gang scheduling, validation, and scheduling behaviour when
// Grove is configured with the Koordinator backend.
//
// Prerequisites:
//   - A running koordinator-grove cluster (set up via test/cross-project/koordinator-grove/setup.sh).
//   - Grove operator deployed with the koord-scheduler profile enabled
//     (test/cross-project/koordinator-grove/deploy-grove.sh).
//   - KUBECONFIG pointing at the koordinator-grove cluster (e.g., via the kcross alias).
//
// Run with:
//
//	cd grove/operator
//	KUBECONFIG=/path/to/koordinator-grove/kubeconfig go test -tags=e2e ./e2e/tests/koordinator/... -v
package koordinator

import (
	"flag"

	"github.com/ai-dynamo/grove/operator/e2e/log"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	"k8s.io/klog/v2"
)

var logger *log.Logger

func init() {
	// Suppress klog noise (same pattern as the main tests package).
	klog.InitFlags(nil)
	if err := flag.Set("logtostderr", "false"); err != nil {
		panic("Failed to set logtostderr flag")
	}
	if err := flag.Set("alsologtostderr", "false"); err != nil {
		panic("Failed to set alsologtostderr flag")
	}

	logger = log.NewTestLogger(log.InfoLevel)
	testctx.Logger = logger
}
