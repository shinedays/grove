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
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/grove/gvk"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	"github.com/ai-dynamo/grove/operator/e2e/waiter"
	"github.com/ai-dynamo/grove/operator/internal/mnnvl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// workloadKoord is the name of the test workload for Koordinator E2E tests.
	workloadKoord = "workload-koord"

	// expectedPodCount is the total number of pods created by workload-koord.yaml:
	// pc-a: 2 replicas + pc-b: 1 replica = 3 pods.
	expectedPodCount = 3

	// expectedPodGroupCount is the number of Koordinator PodGroup CRs expected per PodGang:
	// one per clique (pc-a, pc-b).
	expectedPodGroupCount = 2
)

var podGroupListGVK = gvk.KoordinatorPodGroup.GroupVersion().WithKind(gvk.KoordinatorPodGroup.Kind + "List")

// Test_KGS1_BasicGangScheduling verifies basic gang scheduling with koord-scheduler:
// 1. Deploy a workload using koord-scheduler on KWOK fake nodes.
// 2. Verify all 3 pods reach Running state (KWOK simulates this).
// 3. Verify Koordinator PodGroup CRs are created with correct gang annotations.
func Test_KGS1_BasicGangScheduling(t *testing.T) {
	ctx := context.Background()

	logger.Info("KGS-1: Basic gang scheduling with koord-scheduler")

	tc, cleanup := testctx.PrepareTest(ctx, t, 10,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name:         workloadKoord,
			YAMLPath:     "../../yaml/workload-koord.yaml",
			Namespace:    "default",
			ExpectedPods: expectedPodCount,
		}),
	)
	defer cleanup()
	skipUnlessKoordinator(t, tc)

	logger.Info("1. Deploying workload-koord workload")
	_, err := tc.DeployAndVerifyWorkload()
	require.NoError(t, err, "failed to deploy workload-koord")

	logger.Infof("2. Waiting for %d pods to reach Running state", expectedPodCount)
	require.NoError(t, tc.WaitForPods(expectedPodCount), "pods did not reach Running state")

	logger.Infof("3. Verifying %d Koordinator PodGroup CRs are created", expectedPodGroupCount)
	verifyPodGroupsCreated(t, tc, workloadKoord, 0, expectedPodGroupCount)

	logger.Info("KGS-1: Basic gang scheduling test completed successfully!")
}

// Test_KGS2_GangBlockingBehavior verifies that Koordinator blocks all pods when resources are
// insufficient for the gang, and releases them once resources become available:
// 1. Cordon all nodes so no scheduling is possible.
// 2. Deploy workload-koord and verify all pods are Pending.
// 3. Uncordon nodes and verify all pods reach Running state.
func Test_KGS2_GangBlockingBehavior(t *testing.T) {
	ctx := context.Background()

	logger.Info("KGS-2: Gang blocking behavior with koord-scheduler")

	tc, cleanup := testctx.PrepareTest(ctx, t, 10,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name:         workloadKoord,
			YAMLPath:     "../../yaml/workload-koord.yaml",
			Namespace:    "default",
			ExpectedPods: expectedPodCount,
		}),
	)
	defer cleanup()
	skipUnlessKoordinator(t, tc)

	logger.Info("1. Cordoning all worker nodes")
	workerNodes, err := tc.GetWorkerNodes()
	require.NoError(t, err, "failed to get worker nodes")
	require.NotEmpty(t, workerNodes, "no worker nodes found")

	tc.CordonNodes(workerNodes)
	logger.Infof("   Cordoned %d nodes", len(workerNodes))

	logger.Info("2. Deploying workload-koord workload")
	_, err = tc.DeployAndVerifyWorkload()
	require.NoError(t, err, "failed to deploy workload-koord")

	logger.Info("3. Verifying all pods are Pending (gang blocked by unavailable nodes)")
	// Intentionally NOT VerifyPodsArePendingWithUnschedulableEvents: that helper hardcodes
	// KAI event sources ("kai-scheduler"/"pod-grouper") and never matches koord-scheduler events.
	require.NoError(t, tc.VerifyAllPodsArePending(), "expected all pods to be Pending")

	logger.Info("4. Uncordoning nodes and waiting for pods to become Running")
	tc.UncordonNodesAndWaitForPods(workerNodes, expectedPodCount)

	logger.Info("KGS-2: Gang blocking behavior test completed successfully!")
}

// Test_KGS3_MNNVLValidationRejected verifies that the Grove admission webhook rejects a
// PodCliqueSet that enrolls an MNNVL group when the koord-scheduler backend is in use.
// MNNVL relies on NVIDIA DRA, which is unvalidated with the koord-scheduler backend (fail-closed).
func Test_KGS3_MNNVLValidationRejected(t *testing.T) {
	ctx := context.Background()

	logger.Info("KGS-3: MNNVL group enrollment rejection by admission webhook")

	tc, cleanup := testctx.PrepareTest(ctx, t, 0)
	defer cleanup()
	skipUnlessKoordinator(t, tc)

	// Build a PodCliqueSet with an enrolled MNNVL group and koord-scheduler.
	pcs := buildMNNVLPCS("mnnvl-koord-test")
	logger.Infof("   Attempting to create PCS %q with %s annotation", pcs.Name, mnnvl.AnnotationMNNVLGroup)

	err := tc.Client.Create(ctx, pcs)

	// Cleanup in case the PCS was somehow created despite the expected rejection.
	defer func() {
		if err == nil {
			_ = tc.Client.Delete(ctx, pcs)
		}
	}()

	require.Error(t, err, "expected admission webhook to reject PCS with an enrolled MNNVL group")

	// Verify the error is a webhook rejection (4xx status), not a connection error.
	statusErr, isStatusErr := err.(*errors.StatusError)
	if !isStatusErr || statusErr.Status().Code >= 500 {
		t.Fatalf("expected a webhook rejection (4xx) but got: %v", err)
	}

	// The rejection must come from the koord backend's own validation, not from a generic
	// MNNVL check (e.g. "auto MNNVL is not enabled"), so assert the backend-specific wording.
	assert.Contains(t, err.Error(), "not supported with the koord-scheduler backend",
		"expected the koord-scheduler backend validation to reject the MNNVL group enrollment")

	logger.Info("KGS-3: Admission webhook correctly rejected MNNVL group enrollment!")
}

// Test_KGS4_QuotaValidationRejected verifies that the admission webhook rejects a
// PodCliqueSet referencing a non-existent ElasticQuota: koord-scheduler would silently
// fall back to the default quota, so a typo must be caught at admission.
func Test_KGS4_QuotaValidationRejected(t *testing.T) {
	ctx := context.Background()

	logger.Info("KGS-4: nonexistent ElasticQuota rejection by admission webhook")

	tc, cleanup := testctx.PrepareTest(ctx, t, 0)
	defer cleanup()
	skipUnlessKoordinator(t, tc)

	pcs := buildMNNVLPCS("quota-koord-test")
	// Reuse the PCS shape but strip the MNNVL enrollment and reference a missing quota.
	pcs.Annotations = map[string]string{
		"scheduling.grove.io/koordinator-quota": "no-such-quota",
	}

	err := tc.Client.Create(ctx, pcs)
	defer func() {
		if err == nil {
			_ = tc.Client.Delete(ctx, pcs)
		}
	}()

	require.Error(t, err, "expected admission webhook to reject the PCS referencing a missing ElasticQuota")
	statusErr, isStatusErr := err.(*errors.StatusError)
	if !isStatusErr || statusErr.Status().Code >= 500 {
		t.Fatalf("expected a webhook rejection (4xx) but got: %v", err)
	}
	assert.Contains(t, err.Error(), "does not exist",
		"expected the quota existence validation to reject the PCS")

	logger.Info("KGS-4: Admission webhook correctly rejected the missing ElasticQuota!")
}

// --- helpers ---

// verifyPodGroupsCreated polls until the expected number of Koordinator PodGroup CRs
// exist for the given PodCliqueSet replica, then asserts their gang annotations are correct.
//
// pcsName is the PodCliqueSet name; replicaIdx is the zero-based replica index. PodGang
// names embed a runtime-minted epoch ("{pcsName}-{replicaIdx}-{epoch}") and cannot be
// reconstructed, so the PodGroups are located via the labels the backend clones from the
// PodGang: app.kubernetes.io/part-of=<pcsName> plus
// grove.io/podcliqueset-replica-index=<replicaIdx>. To verify all replicas of a
// multi-replica workload, call this function once per replica index.
func verifyPodGroupsCreated(t *testing.T, tc *testctx.TestContext, pcsName string, replicaIdx int, expectedCount int) {
	t.Helper()

	fetchPodGroups := func(ctx context.Context) (*unstructured.UnstructuredList, error) {
		return listPodGroupsForReplica(ctx, tc, pcsName, replicaIdx)
	}
	err := waiter.New[*unstructured.UnstructuredList]().
		WithTimeout(tc.Timeout).
		WithInterval(tc.Interval).
		WithLogger(logger).
		WithRetryOnError().
		WaitUntil(tc.Ctx, fetchPodGroups, func(list *unstructured.UnstructuredList) bool {
			return len(list.Items) >= expectedCount
		})
	require.NoError(t, err, "Koordinator PodGroup CRs not created in time")

	// Re-fetch for annotation assertions.
	list, err := listPodGroupsForReplica(tc.Ctx, tc, pcsName, replicaIdx)
	require.NoError(t, err, "failed to list PodGroups for annotation check")
	require.Len(t, list.Items, expectedCount,
		"expected exactly %d Koordinator PodGroup CRs", expectedCount)

	// All PodGroups should share the same GangGroups annotation value, belong to the
	// same (anchor) PodGang, and carry the labels cloned from it.
	var firstGangGroups, firstPodGang string
	for i, pg := range list.Items {
		annotations := pg.GetAnnotations()
		labels := pg.GetLabels()

		require.NotEmpty(t, labels[common.LabelPodGang],
			"PodGroup %d missing the %s label", i, common.LabelPodGang)
		assert.Equal(t, string(grovecorev1alpha1.PodGangEntryRoleAnchor), labels[common.LabelPodGangRole],
			"PodGroup %d should carry the PodGang role label cloned by the backend", i)
		if i == 0 {
			firstPodGang = labels[common.LabelPodGang]
		} else {
			assert.Equal(t, firstPodGang, labels[common.LabelPodGang],
				"all PodGroups of one replica must reference the same PodGang", i)
		}
		minMember, found, nestedErr := unstructured.NestedInt64(pg.Object, "spec", "minMember")
		require.NoError(t, nestedErr, "PodGroup %d has malformed minMember", i)
		require.True(t, found, "PodGroup %d missing minMember", i)

		assert.NotEmpty(t, annotations["gang.scheduling.koordinator.sh/mode"],
			"PodGroup %d missing GangMode annotation", i)
		assert.NotEmpty(t, annotations["gang.scheduling.koordinator.sh/groups"],
			"PodGroup %d missing GangGroups annotation", i)
		assert.Equal(t, fmt.Sprintf("%d", minMember), annotations["gang.scheduling.koordinator.sh/total-number"],
			"PodGroup %d GangTotalNum should match this PodGroup's total child count", i)

		if i == 0 {
			firstGangGroups = annotations["gang.scheduling.koordinator.sh/groups"]
		} else {
			assert.Equal(t, firstGangGroups, annotations["gang.scheduling.koordinator.sh/groups"],
				"GangGroups annotation must be identical across all PodGroups in the gang")
		}
	}
}

// listPodGroupsForReplica lists the Koordinator PodGroup CRs of one PodCliqueSet replica
// via the labels the backend clones from the owning PodGang (part-of + replica index) —
// PodGang names embed a runtime-minted epoch, so name-based lookup is not possible.
func listPodGroupsForReplica(ctx context.Context, tc *testctx.TestContext, pcsName string, replicaIdx int) (*unstructured.UnstructuredList, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(podGroupListGVK)
	if err := tc.Client.List(ctx, list,
		client.InNamespace(tc.Namespace),
		client.MatchingLabels{
			common.LabelPartOfKey:                pcsName,
			common.LabelPodCliqueSetReplicaIndex: fmt.Sprintf("%d", replicaIdx),
		},
	); err != nil {
		return nil, fmt.Errorf("failed to list PodGroups: %w", err)
	}
	return list, nil
}

// buildMNNVLPCS constructs a PodCliqueSet that enrolls a GPU clique into an MNNVL group
// (grove.io/mnnvl-group) with schedulerName set to koord-scheduler, which should be
// rejected by the admission webhook. The clique must request GPUs because MNNVL group
// resolution only considers GPU cliques.
func buildMNNVLPCS(name string) *grovecorev1alpha1.PodCliqueSet {
	return &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Annotations: map[string]string{
				mnnvl.AnnotationMNNVLGroup: "koord-test-group",
			},
		},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{
			Replicas: 1,
			Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{
					{
						Name: "pc-a",
						Spec: grovecorev1alpha1.PodCliqueSpec{
							RoleName:     "role-a",
							Replicas:     1,
							MinAvailable: ptr.To[int32](1),
							PodSpec: corev1.PodSpec{
								SchedulerName: "koord-scheduler",
								Containers: []corev1.Container{
									{
										Name:    "container-a",
										Image:   "busybox:latest",
										Command: []string{"sleep", "infinity"},
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceMemory: resource.MustParse("100Mi"),
												"nvidia.com/gpu":      resource.MustParse("1"),
											},
											Limits: corev1.ResourceList{
												"nvidia.com/gpu": resource.MustParse("1"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
