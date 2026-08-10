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

package koordinator

import (
	"context"
	"fmt"
	"sort"
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/grove/topology"
	"github.com/ai-dynamo/grove/operator/e2e/setup"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	"github.com/ai-dynamo/grove/operator/e2e/waiter"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	workloadKoordTopo   = "workload-koord-topo"
	workloadKoordAtomic = "workload-koord-atomic"
	koordTopologyName   = "koord-topology"

	// KGS5 lays the KWOK nodes out as racks of 4/3/3; only the first rack can host a
	// 4-pod gang that needs one node per pod, so MustGather has exactly one legal answer.
	topoRackANodes = 4
	topoRackBNodes = 3
	topoPodCount   = 4

	atomicPodCount = 3
)

var cntGVK = schema.GroupVersionKind{Group: "scheduling.koordinator.sh", Version: "v1alpha1", Kind: "ClusterNetworkTopology"}

// Test_KGS5_TopologyMustGatherSameRack verifies the topology path end to end: a
// ClusterTopologyBinding is synced into Koordinator's "default" ClusterNetworkTopology, the
// generated PodGroups carry a MustGather network-topology-spec with the automatically derived
// layer name, and koord-scheduler places the whole gang inside the only rack that can hold it.
//
// Requires koord-scheduler to run with --enable-network-topology-manager=true; without it the
// gang never leaves PreFilter and the pod wait below times out.
func Test_KGS5_TopologyMustGatherSameRack(t *testing.T) {
	ctx := context.Background()
	logger.Info("KGS-5: Topology-aware gang placement (MustGather within one rack)")

	tc, cleanup := testctx.PrepareTest(ctx, t, 10,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name: workloadKoordTopo, YAMLPath: "../../yaml/workload-koord-topo.yaml",
			Namespace: "default", ExpectedPods: topoPodCount,
		}),
	)
	defer cleanup()
	skipUnlessKoordinator(t, tc)
	skipUnlessClusterNetworkTopologyCRD(t, tc)

	kwokNodes := listKwokNodeNames(t, tc)
	require.GreaterOrEqual(t, len(kwokNodes), topoRackANodes+topoRackBNodes+1,
		"KGS5 needs at least %d KWOK nodes for a 4/3/3 rack layout", topoRackANodes+topoRackBNodes+1)

	// 1. Lay the nodes out as racks; the cleanup restores the original labels.
	tv := topology.NewTopologyVerifier(tc.Client, logger)
	changes := make([]topology.NodeLabelChange, 0, len(kwokNodes))
	for i, n := range kwokNodes {
		changes = append(changes, topology.NodeLabelChange{
			NodeName:  n,
			AddLabels: map[string]string{setup.TopologyLabelRack: rackForIndex(i)},
		})
	}
	labelCleanup, err := tv.MutateNodeLabels(ctx, t, changes)
	require.NoError(t, err, "failed to label KWOK nodes with racks")
	t.Cleanup(labelCleanup) // runs after the deferred workload cleanup

	// 2. Create the ClusterTopologyBinding and wait for the backend to sync it into the
	//    "default" ClusterNetworkTopology (layer name == node label key).
	levels := []grovecorev1alpha1.TopologyLevel{
		{Domain: grovecorev1alpha1.TopologyDomainRack, Key: setup.TopologyLabelRack},
		{Domain: grovecorev1alpha1.TopologyDomainHost, Key: setup.TopologyLabelHostname},
	}
	require.NoError(t, tv.CreateClusterTopology(ctx, koordTopologyName, levels))
	t.Cleanup(func() {
		if err := tv.DeleteClusterTopology(ctx, koordTopologyName); err != nil {
			logger.Errorf("failed to delete ClusterTopologyBinding %s: %v", koordTopologyName, err)
		}
	})
	logger.Info("1. Waiting for the default ClusterNetworkTopology to be synced from the binding")
	err = waiter.New[*unstructured.Unstructured]().
		WithTimeout(tc.Timeout).WithInterval(tc.Interval).WithLogger(logger).WithRetryOnError().
		WaitUntil(tc.Ctx, func(ctx context.Context) (*unstructured.Unstructured, error) {
			cnt := &unstructured.Unstructured{}
			cnt.SetGroupVersionKind(cntGVK)
			return cnt, tc.Client.Get(ctx, client.ObjectKey{Name: "default"}, cnt)
		}, func(cnt *unstructured.Unstructured) bool {
			layers, _, _ := unstructured.NestedSlice(cnt.Object, "spec", "networkTopologySpec")
			for _, l := range layers {
				if m, ok := l.(map[string]interface{}); ok && m["topologyLayer"] == setup.TopologyLabelRack {
					return true
				}
			}
			return false
		})
	require.NoError(t, err, "ClusterNetworkTopology 'default' was not synced with the rack layer")

	// 3. Deploy the 4-pod gang with pack.required=rack and wait for it to run.
	logger.Info("2. Deploying workload-koord-topo (4 pods, one per node, MustGather at rack)")
	_, err = tc.DeployAndVerifyWorkload()
	require.NoError(t, err, "failed to deploy workload-koord-topo")
	require.NoError(t, tc.WaitForPods(topoPodCount), "topology-constrained gang did not reach Running")

	// 4. All pods in one rack, and it must be the only rack with 4 nodes.
	pods, err := tc.ListPods()
	require.NoError(t, err)
	require.NoError(t, tv.VerifyPodsInSameTopologyDomain(tc.Ctx, pods.Items, setup.TopologyLabelRack),
		"gang pods were not gathered into one rack")
	require.Equal(t, rackForIndex(0), nodeLabel(t, tc, pods.Items[0].Spec.NodeName, setup.TopologyLabelRack),
		"only the first rack has enough nodes for the gang; MustGather must have chosen it")
	tc.ListPodsAndAssertDistinctNodes()

	// 5. The PodGroup carries the MustGather spec with the automatically derived layer name.
	pgs, err := listPodGroupsForReplica(tc.Ctx, tc, workloadKoordTopo, 0)
	require.NoError(t, err)
	require.Len(t, pgs.Items, 1)
	spec := pgs.Items[0].GetAnnotations()["gang.scheduling.koordinator.sh/network-topology-spec"]
	require.Contains(t, spec, `"strategy":"MustGather"`)
	require.Contains(t, spec, fmt.Sprintf(`"layer":"%s"`, setup.TopologyLabelRack),
		"layer name must be derived from the ClusterTopologyBinding without topologyKeyMappings")

	logger.Info("KGS-5: Topology-aware gang placement test completed successfully!")
}

// Test_KGS6_GangAtomicityPartialCapacity verifies all-or-nothing semantics when only part of
// the gang could be placed: with a single schedulable node and pods that each need a node of
// their own, no pod may be bound until enough capacity exists for the whole gang. KGS2 covers
// the zero-capacity case; this covers the partial one that a non-gang scheduler would get wrong.
func Test_KGS6_GangAtomicityPartialCapacity(t *testing.T) {
	ctx := context.Background()
	logger.Info("KGS-6: Gang atomicity with partial capacity")

	tc, cleanup := testctx.PrepareTest(ctx, t, 10,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name: workloadKoordAtomic, YAMLPath: "../../yaml/workload-koord-atomic.yaml",
			Namespace: "default", ExpectedPods: atomicPodCount,
		}),
	)
	defer cleanup()
	skipUnlessKoordinator(t, tc)

	workerNodes, err := tc.GetWorkerNodes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(workerNodes), atomicPodCount, "need at least %d worker nodes", atomicPodCount)

	// 1. Leave exactly one node schedulable: one pod fits, the gang of three does not.
	cordoned := workerNodes[1:]
	tc.CordonNodes(cordoned)
	logger.Infof("1. Cordoned %d of %d nodes; only %s remains schedulable", len(cordoned), len(workerNodes), workerNodes[0])

	logger.Info("2. Deploying workload-koord-atomic (3 pods, one node each, minAvailable 3)")
	_, err = tc.DeployAndVerifyWorkload()
	require.NoError(t, err, "failed to deploy workload-koord-atomic")

	// 2. Hold for a stable window: nothing may bind even though one pod would fit.
	logger.Info("3. Verifying no pod is bound while the gang cannot be satisfied")
	tc.VerifyAllPodsArePendingWithSleep()
	pods, err := tc.ListPods()
	require.NoError(t, err)
	for _, p := range pods.Items {
		require.Empty(t, p.Spec.NodeName, "pod %s was bound although the gang could not be satisfied", p.Name)
	}

	// 3. Restore capacity and expect the whole gang to run on distinct nodes.
	logger.Info("4. Uncordoning nodes and waiting for the gang to run")
	tc.UncordonNodesAndWaitForPods(cordoned, atomicPodCount)
	tc.ListPodsAndAssertDistinctNodes()
	verifyPodGroupsCreated(t, tc, workloadKoordAtomic, 0, 1)

	logger.Info("KGS-6: Gang atomicity test completed successfully!")
}

// --- helpers ---

func rackForIndex(i int) string {
	switch {
	case i < topoRackANodes:
		return "rack-a"
	case i < topoRackANodes+topoRackBNodes:
		return "rack-b"
	default:
		return "rack-c"
	}
}

func listKwokNodeNames(t *testing.T, tc *testctx.TestContext) []string {
	t.Helper()
	nodes := &corev1.NodeList{}
	require.NoError(t, tc.Client.List(tc.Ctx, nodes, client.MatchingLabels{"type": "kwok"}))
	names := make([]string, 0, len(nodes.Items))
	for _, n := range nodes.Items {
		names = append(names, n.Name)
	}
	sort.Strings(names)
	return names
}

func nodeLabel(t *testing.T, tc *testctx.TestContext, nodeName, key string) string {
	t.Helper()
	node := &corev1.Node{}
	require.NoError(t, tc.Client.Get(tc.Ctx, client.ObjectKey{Name: nodeName}, node))
	return node.Labels[key]
}

// skipUnlessClusterNetworkTopologyCRD skips when Koordinator is installed without the
// ClusterNetworkTopology CRD (older releases); any other lookup error fails the test.
func skipUnlessClusterNetworkTopologyCRD(t *testing.T, tc *testctx.TestContext) {
	t.Helper()
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	err := tc.Client.Get(tc.Ctx, client.ObjectKey{Name: "clusternetworktopologies.scheduling.koordinator.sh"}, crd)
	if apierrors.IsNotFound(err) {
		t.Skip("Skipping: the ClusterNetworkTopology CRD is not installed")
	}
	require.NoError(t, err, "failed to look up the ClusterNetworkTopology CRD")
}
