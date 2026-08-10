//go:build e2e

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

// Package koordinator contains end-to-end tests for the koord-scheduler backend.
// These tests verify gang scheduling, validation, and scheduling behaviour when
// Grove is configured with the Koordinator backend.
//
// Prerequisites:
//   - An existing cluster with Koordinator installed:
//     helm repo add koordinator-sh https://koordinator-sh.github.io/charts/
//     helm upgrade --install koordinator koordinator-sh/koordinator -n koordinator-system --create-namespace
//   - Grove deployed with the koord-scheduler profile enabled (from operator/):
//     hack/prepare-charts.sh && PACKAGE_PATH="github.com/ai-dynamo/grove/operator/internal" \
//     PROGRAM_NAME="grove-operator" hack/deploy.sh run -m grove-operator -n default -p koord-test
//   - A cluster created with `hack/kind-up.sh --fake-nodes 10` (or equivalent): KGS1/KGS2
//     require at least 10 Ready worker nodes (KWOK fake nodes) and the local registry the
//     script provisions.
//   - For KGS5 (topology): koord-scheduler started with --enable-network-topology-manager=true
//     (the official Helm chart sets it since v1.7.0); without it the topology gang never schedules.
//   - KUBECONFIG pointing at that cluster.
//
// On clusters without Koordinator (e.g. the standard `make run-e2e` cluster) every test in
// this suite skips itself via skipUnlessKoordinator rather than failing, provided the cluster
// satisfies the worker-node requirement above (PrepareTest runs before the skip check).
//
// Run with:
//
//	cd operator
//	KUBECONFIG=/path/to/kubeconfig go test -tags=e2e ./e2e/tests/koordinator/... -v
package koordinator

import (
	"flag"
	"sync"
	"testing"

	"github.com/ai-dynamo/grove/operator/e2e/log"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

var (
	koordDetectOnce sync.Once
	koordAvailable  bool
	koordDetectMsg  string
	koordDetectErr  error
)

// skipUnlessKoordinator skips the test when the target cluster has no Koordinator
// installation, detected via the sig-scheduling PodGroup CRD that Koordinator ships.
// This keeps the suite green under the default `make run-e2e` wildcard on standard e2e
// clusters while running fully on Koordinator-enabled clusters (same guard pattern as
// the auto-mnnvl suite). Only a NotFound lookup means "not installed"; any other error
// (network, RBAC) fails the test instead of silently skipping the whole suite on a
// cluster that does have Koordinator.
func skipUnlessKoordinator(t *testing.T, tc *testctx.TestContext) {
	t.Helper()
	koordDetectOnce.Do(func() {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "apiextensions.k8s.io",
			Version: "v1",
			Kind:    "CustomResourceDefinition",
		})
		err := tc.Client.Get(tc.Ctx, client.ObjectKey{Name: "podgroups.scheduling.sigs.k8s.io"}, crd)
		switch {
		case err == nil:
			koordAvailable = true
		case apierrors.IsNotFound(err):
			koordAvailable = false
			koordDetectMsg = err.Error()
		default:
			koordDetectErr = err
		}
	})
	if koordDetectErr != nil {
		t.Fatalf("failed to detect whether Koordinator is installed (PodGroup CRD lookup): %v", koordDetectErr)
	}
	if !koordAvailable {
		t.Skipf("Skipping: Koordinator is not installed on the target cluster (PodGroup CRD lookup: %s); "+
			"see the package documentation for prerequisites", koordDetectMsg)
	}
}
