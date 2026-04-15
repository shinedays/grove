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
	"fmt"
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/api/common"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	"github.com/ai-dynamo/grove/operator/internal/mnnvl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
)

const (
	// podGroupGroup/Version/Resource define the GVR for Koordinator PodGroup CRs.
	podGroupGroup    = "scheduling.sigs.k8s.io"
	podGroupVersion  = "v1alpha1"
	podGroupResource = "podgroups"

	// workloadKoord is the name of the test workload for Koordinator E2E tests.
	workloadKoord = "workload-koord"

	// expectedPodCount is the total number of pods created by workload-koord.yaml:
	// pc-a: 2 replicas + pc-b: 1 replica = 3 pods.
	expectedPodCount = 3

	// expectedPodGroupCount is the number of Koordinator PodGroup CRs expected per PodGang:
	// one per clique (pc-a, pc-b).
	expectedPodGroupCount = 2
)

var podGroupGVR = schema.GroupVersionResource{
	Group:    podGroupGroup,
	Version:  podGroupVersion,
	Resource: podGroupResource,
}

// Test_KGS1_BasicGangScheduling verifies basic gang scheduling with koord-scheduler:
// 1. Deploy a workload using koord-scheduler on KWOK fake nodes.
// 2. Verify all 3 pods reach Running state (KWOK simulates this).
// 3. Verify Koordinator PodGroup CRs are created with correct gang annotations.
func Test_KGS1_BasicGangScheduling(t *testing.T) {
	ctx := context.Background()

	logger.Info("KGS-1: Basic gang scheduling with koord-scheduler")

	tc, cleanup := testctx.PrepareTest(ctx, t, 0,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name:         workloadKoord,
			YAMLPath:     "../../yaml/workload-koord.yaml",
			Namespace:    "default",
			ExpectedPods: expectedPodCount,
		}),
	)
	defer cleanup()

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
	require.NoError(t, tc.VerifyAllPodsArePending(), "expected all pods to be Pending")

	logger.Info("4. Uncordoning nodes and waiting for pods to become Running")
	tc.UncordonNodesAndWaitForPods(workerNodes, expectedPodCount)

	logger.Info("KGS-2: Gang blocking behavior test completed successfully!")
}

// Test_KGS3_MNNVLValidationRejected verifies that the Grove admission webhook rejects a
// PodCliqueSet with the MNNVL annotation when the koord-scheduler backend is in use.
// MNNVL requires NVIDIA DRA, which is incompatible with the koord-scheduler backend.
func Test_KGS3_MNNVLValidationRejected(t *testing.T) {
	ctx := context.Background()

	logger.Info("KGS-3: MNNVL annotation rejection by admission webhook")

	tc, cleanup := testctx.PrepareTest(ctx, t, 0)
	defer cleanup()

	// Build a PodCliqueSet with the MNNVL annotation and koord-scheduler.
	pcs := buildMNNVLPCS("mnnvl-koord-test")
	logger.Infof("   Attempting to create PCS %q with grove.io/auto-mnnvl=enabled", pcs.Name)

	err := tc.Clients.CRClient.Create(ctx, pcs)

	// Cleanup in case the PCS was somehow created despite the expected rejection.
	defer func() {
		if err == nil {
			_ = tc.Clients.CRClient.Delete(ctx, pcs)
		}
	}()

	require.Error(t, err, "expected admission webhook to reject PCS with MNNVL annotation")

	// Verify the error is a webhook rejection (4xx status), not a connection error.
	statusErr, isStatusErr := err.(*errors.StatusError)
	if !isStatusErr || statusErr.Status().Code >= 500 {
		t.Fatalf("expected a webhook rejection (4xx) but got: %v", err)
	}

	// The rejection should mention MNNVL.
	assert.Contains(t, err.Error(), "MNNVL",
		"expected error to mention MNNVL incompatibility")

	logger.Info("KGS-3: Admission webhook correctly rejected MNNVL annotation!")
}

// --- helpers ---

// verifyPodGroupsCreated polls until the expected number of Koordinator PodGroup CRs
// exist for the given PodCliqueSet replica, then asserts their gang annotations are correct.
//
// pcsName is the PodCliqueSet name; replicaIdx is the zero-based replica index. Grove names
// PodGangs as "{pcsName}-{replicaIdx}", so passing (workloadKoord, 0, 2) targets the PodGang
// "workload-koord-0" and its 2 PodGroup CRs. To verify all replicas of a multi-replica
// workload, call this function once per replica index.
//
// Filtering by grove.io/podgang=<podGangName> (key=value) isolates exactly the PodGroups
// owned by this replica, preventing false positives from other replicas or other workloads.
func verifyPodGroupsCreated(t *testing.T, tc *testctx.TestContext, pcsName string, replicaIdx int, expectedCount int) {
	t.Helper()

	podGangName := fmt.Sprintf("%s-%d", pcsName, replicaIdx)
	// Filter by grove.io/podgang=<podGangName> so we count only PodGroups for this
	// specific replica, not other replicas or unrelated Grove workloads in the namespace.
	groveOpts := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", common.LabelPodGang, podGangName)}

	err := tc.PollForCondition(func() (bool, error) {
		list, listErr := tc.Clients.DynamicClient.
			Resource(podGroupGVR).
			Namespace(tc.Namespace).
			List(tc.Ctx, groveOpts)
		if listErr != nil {
			return false, fmt.Errorf("failed to list PodGroups: %w", listErr)
		}
		return len(list.Items) >= expectedCount, nil
	})
	require.NoError(t, err, "Koordinator PodGroup CRs not created in time")

	// Re-fetch for annotation assertions.
	list, err := tc.Clients.DynamicClient.
		Resource(podGroupGVR).
		Namespace(tc.Namespace).
		List(tc.Ctx, groveOpts)
	require.NoError(t, err, "failed to list PodGroups for annotation check")
	require.Len(t, list.Items, expectedCount,
		"expected exactly %d Koordinator PodGroup CRs", expectedCount)

	// All PodGroups should share the same GangGroups annotation value.
	var firstGangGroups string
	for i, pg := range list.Items {
		annotations := pg.GetAnnotations()
		assert.NotEmpty(t, annotations["gang.scheduling.koordinator.sh/mode"],
			"PodGroup %d missing GangMode annotation", i)
		assert.NotEmpty(t, annotations["gang.scheduling.koordinator.sh/groups"],
			"PodGroup %d missing GangGroups annotation", i)
		assert.Equal(t, "2", annotations["gang.scheduling.koordinator.sh/total-number"],
			"PodGroup %d GangTotalNum should be 2 (number of cliques)", i)

		if i == 0 {
			firstGangGroups = annotations["gang.scheduling.koordinator.sh/groups"]
		} else {
			assert.Equal(t, firstGangGroups, annotations["gang.scheduling.koordinator.sh/groups"],
				"GangGroups annotation must be identical across all PodGroups in the gang")
		}
	}
}

// buildMNNVLPCS constructs a PodCliqueSet with the MNNVL annotation enabled and
// schedulerName set to koord-scheduler, which should be rejected by the admission webhook.
func buildMNNVLPCS(name string) *grovecorev1alpha1.PodCliqueSet {
	return &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Annotations: map[string]string{
				mnnvl.AnnotationAutoMNNVL: mnnvl.AnnotationAutoMNNVLEnabled,
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
