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

package koordinator

import (
	"context"
	"os"
	"testing"

	"github.com/ai-dynamo/grove/operator/e2e/setup"
)

// TestMain manages the lifecycle of the cluster for all Koordinator backend E2E tests.
// These tests expect an existing cluster created by:
//
//	test/cross-project/koordinator-grove/setup.sh
//
// Grove must be deployed with the koord-scheduler profile enabled:
//
//	test/cross-project/koordinator-grove/deploy-grove.sh
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Connect to the existing cluster (no cluster creation, no image push).
	sharedCluster := setup.SharedCluster(logger)
	if err := sharedCluster.Setup(ctx, nil); err != nil {
		logger.Errorf("failed to connect to cluster: %s", err)
		os.Exit(1)
	}

	code := m.Run()

	// No teardown — cluster is managed externally.
	os.Exit(code)
}
