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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// newTestPodGang creates a PodGang with one or more PodGroups for testing.
func newTestPodGang(name string, podGroups []groveschedulerv1alpha1.PodGroup) *groveschedulerv1alpha1.PodGang {
	const namespace = "default"
	return &groveschedulerv1alpha1.PodGang{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "grove.io/v1alpha1",
			Kind:       "PodGang",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       "test-uid-123",
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: podGroups,
		},
	}
}

func newPodGroup(name string, minReplicas int32) groveschedulerv1alpha1.PodGroup {
	return groveschedulerv1alpha1.PodGroup{
		Name:        name,
		MinReplicas: minReplicas,
	}
}

func newPodGroupWithRefs(name string, minReplicas int32, totalReplicas int) groveschedulerv1alpha1.PodGroup {
	pg := newPodGroup(name, minReplicas)
	for i := 0; i < totalReplicas; i++ {
		pg.PodReferences = append(pg.PodReferences, groveschedulerv1alpha1.NamespacedName{
			Namespace: "default",
			Name:      fmt.Sprintf("%s-%d", name, i),
		})
	}
	return pg
}

func defaultTestCfg() backendConfig {
	return backendConfig{
		GangMode:               DefaultGangMode,
		MatchPolicy:            DefaultMatchPolicy,
		ScheduleTimeoutSeconds: DefaultTimeoutSecs,
	}
}

// getPodGroup fetches a Koordinator PodGroup from the fake client by name.
func getPodGroup(t *testing.T, cl client.Client, name string) *unstructured.Unstructured {
	const namespace = "default"
	t.Helper()
	pg := &unstructured.Unstructured{}
	pg.SetGroupVersionKind(podGroupGVK)
	err := cl.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, pg)
	require.NoError(t, err)
	return pg
}

func TestSyncPodGang_SingleGroup(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 2, 2),
	})

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.NoError(t, err)

	pg := getPodGroup(t, cl, "mygang-pg-a")
	assert.Equal(t, "mygang-pg-a", pg.GetName())
	assert.Equal(t, "default", pg.GetNamespace())

	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.EqualValues(t, 2, spec["minMember"])
	assert.EqualValues(t, DefaultTimeoutSecs, spec["scheduleTimeoutSeconds"])

	annotations := pg.GetAnnotations()
	assert.Equal(t, DefaultGangMode, annotations[AnnotationGangMode])
	assert.Equal(t, DefaultMatchPolicy, annotations[AnnotationGangMatchPolicy])
	assert.Equal(t, "2", annotations[AnnotationGangTotalNum])
	assert.Contains(t, annotations[AnnotationGangGroups], "default/mygang-pg-a")
}

// TestSyncPodGang_ClonesPodGangLabels verifies that PodGang labels are cloned onto the PodGroups
// and that the backend's own grove.io/podgang label wins over a same-named PodGang label.
func TestSyncPodGang_ClonesPodGangLabels(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 2, 2),
	})
	podGang.Labels = map[string]string{
		"grove.io/epoch":                      "1756100000000000000",
		"grove.io/podgang-role":               "Anchor",
		"grove.io/podcliqueset-replica-index": "0",
		"app.kubernetes.io/part-of":           "mypcs",
		common.LabelPodGang:                   "should-be-overwritten",
	}

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.NoError(t, err)

	pg := getPodGroup(t, cl, "mygang-pg-a")
	labels := pg.GetLabels()
	assert.Equal(t, "1756100000000000000", labels["grove.io/epoch"])
	assert.Equal(t, "Anchor", labels["grove.io/podgang-role"])
	assert.Equal(t, "0", labels["grove.io/podcliqueset-replica-index"])
	assert.Equal(t, "mypcs", labels["app.kubernetes.io/part-of"])
	assert.Equal(t, "mygang", labels[common.LabelPodGang],
		"the backend-owned podgang label must reference the owning PodGang, not the cloned value")
}

func TestSyncPodGang_WithoutPodReferences_OmitsGangTotalNum(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 2),
	})

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.NoError(t, err)

	pg := getPodGroup(t, cl, "mygang-pg-a")
	assert.NotContains(t, pg.GetAnnotations(), AnnotationGangTotalNum,
		"total-number must not be derived from the number of PodGroups")
}

func TestSyncPodGang_MultiGroup_GangGroupAnnotationsLinked(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("multigang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 1, 1),
		newPodGroupWithRefs("pg-b", 3, 3),
	})

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.NoError(t, err)

	// Both PodGroups should be created.
	pgA := getPodGroup(t, cl, "multigang-pg-a")
	pgB := getPodGroup(t, cl, "multigang-pg-b")

	// Both should reference the same GangGroup.
	gangGroupA := pgA.GetAnnotations()[AnnotationGangGroups]
	gangGroupB := pgB.GetAnnotations()[AnnotationGangGroups]
	assert.Equal(t, gangGroupA, gangGroupB, "GangGroup annotations must be identical across PodGroups")

	var groupsA []string
	require.NoError(t, json.Unmarshal([]byte(gangGroupA), &groupsA))
	assert.Len(t, groupsA, 2)
	assert.Contains(t, groupsA, "default/multigang-pg-a")
	assert.Contains(t, groupsA, "default/multigang-pg-b")

	// totalNumber is Koordinator's total children count for each individual gang.
	assert.Equal(t, "1", pgA.GetAnnotations()[AnnotationGangTotalNum])
	assert.Equal(t, "3", pgB.GetAnnotations()[AnnotationGangTotalNum])

	// Verify MinReplicas mapping.
	specA, _, _ := unstructured.NestedMap(pgA.Object, "spec")
	specB, _, _ := unstructured.NestedMap(pgB.Object, "spec")
	assert.EqualValues(t, 1, specA["minMember"])
	assert.EqualValues(t, 3, specB["minMember"])
}

func TestSyncPodGang_Idempotent_Update(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 2),
	})

	// First sync creates the PodGroup.
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))
	// Second sync updates the existing PodGroup (idempotent).
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// Still only one PodGroup should exist.
	pgList := &unstructured.UnstructuredList{}
	pgList.SetGroupVersionKind(podGroupGVK)
	err := cl.List(context.Background(), pgList, client.InNamespace("default"))
	require.NoError(t, err)
	assert.Len(t, pgList.Items, 1)
}

func TestSyncPodGang_UpdatePreservesExternalMetadata(t *testing.T) {
	scheme := testutils.NewTestClientBuilder().Build().Scheme()
	existingPG := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": podGroupGVK.Group + "/" + podGroupGVK.Version,
			"kind":       podGroupGVK.Kind,
			"metadata": map[string]interface{}{
				"name":      "mygang-pg-a",
				"namespace": "default",
				"labels": map[string]interface{}{
					"external.example/label": "keep",
				},
				"annotations": map[string]interface{}{
					"external.example/annotation": "keep",
					AnnotationGangTotalNum:        "99",
					AnnotationNetworkTopologySpec: "stale",
				},
				"finalizers": []interface{}{"external.example/finalizer"},
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "grove.io/v1alpha1",
						"kind":       "PodGang",
						"name":       "mygang",
						"uid":        "test-uid-123",
						"controller": true,
					},
				},
			},
			"spec": map[string]interface{}{
				"minMember": int64(1),
			},
		},
	}

	cl := testutils.CreateDefaultFakeClient([]client.Object{existingPG})
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 2, 2),
	})

	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	pg := getPodGroup(t, cl, "mygang-pg-a")
	assert.Equal(t, "keep", pg.GetLabels()["external.example/label"])
	assert.Equal(t, "keep", pg.GetAnnotations()["external.example/annotation"])
	assert.Contains(t, pg.GetFinalizers(), "external.example/finalizer")
	assert.Equal(t, DefaultGangMode, pg.GetAnnotations()[AnnotationGangMode])
	assert.Equal(t, "2", pg.GetAnnotations()[AnnotationGangTotalNum])
	assert.NotContains(t, pg.GetAnnotations(), AnnotationNetworkTopologySpec)

	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.EqualValues(t, 2, spec["minMember"])
}

func TestSyncPodGang_WithHostTopology(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	requiredKey := "kubernetes.io/hostname"
	podGang := newTestPodGang("topogang", []groveschedulerv1alpha1.PodGroup{
		{
			Name:        "pg-a",
			MinReplicas: 1,
			TopologyConstraint: &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{
					Required: &requiredKey,
				},
			},
		},
	})

	require.NoError(t, syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang))

	pg := getPodGroup(t, cl, "topogang-pg-a")
	topoAnnotation := pg.GetAnnotations()[AnnotationNetworkTopologySpec]
	assert.NotEmpty(t, topoAnnotation, "topology annotation should be set for host-level key")
	assert.Contains(t, topoAnnotation, "NodeTopologyLayer")
	assert.Contains(t, topoAnnotation, "MustGather")
}

func TestSyncPodGang_WithRackTopology_UserMapping(t *testing.T) {
	// Rack has no built-in Koordinator layer; the administrator maps it to the layer name
	// defined in the cluster's ClusterNetworkTopology CR via TopologyKeyMappings.
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	cfg.topologyKeyMappings = map[string]string{"topology.kubernetes.io/rack": "rackLayer"}

	preferredKey := "topology.kubernetes.io/rack"
	podGang := newTestPodGang("rackgang", []groveschedulerv1alpha1.PodGroup{
		{
			Name:        "pg-a",
			MinReplicas: 2,
			TopologyConstraint: &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{
					Preferred: &preferredKey,
				},
			},
		},
	})

	require.NoError(t, syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang))

	pg := getPodGroup(t, cl, "rackgang-pg-a")
	topoAnnotation := pg.GetAnnotations()[AnnotationNetworkTopologySpec]
	assert.NotEmpty(t, topoAnnotation)
	assert.Contains(t, topoAnnotation, "rackLayer")
	assert.Contains(t, topoAnnotation, "PreferGather")
}

func TestSyncPodGang_DivergentPerPodGroupTopology_ReturnsError(t *testing.T) {
	// Koordinator applies a single network-topology-spec (taken from whichever member pod
	// enters PreFilter first) to the whole GangGroup. Divergent effective constraints across
	// member PodGroups must therefore be rejected rather than synced non-deterministically.
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	requiredKey := "kubernetes.io/hostname"
	podGang := newTestPodGang("divergentgang", []groveschedulerv1alpha1.PodGroup{
		{
			Name:        "pg-a",
			MinReplicas: 1,
			TopologyConstraint: &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{
					Required: &requiredKey,
				},
			},
		},
		newPodGroup("pg-b", 1), // no constraint, no global fallback → diverges from pg-a
	})

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "divergent topology constraints")
}

func TestSyncPodGang_UniformPerPodGroupTopology_Succeeds(t *testing.T) {
	// Identical per-PodGroup constraints (or a global constraint applied to all) are fine.
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	requiredKey := "kubernetes.io/hostname"
	constraint := &groveschedulerv1alpha1.TopologyConstraint{
		PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{
			Required: &requiredKey,
		},
	}
	podGang := newTestPodGang("uniformgang", []groveschedulerv1alpha1.PodGroup{
		{Name: "pg-a", MinReplicas: 1, TopologyConstraint: constraint.DeepCopy()},
		{Name: "pg-b", MinReplicas: 1, TopologyConstraint: constraint.DeepCopy()},
	})

	require.NoError(t, syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang))

	for _, name := range []string{"uniformgang-pg-a", "uniformgang-pg-b"} {
		pg := getPodGroup(t, cl, name)
		assert.Contains(t, pg.GetAnnotations()[AnnotationNetworkTopologySpec], "NodeTopologyLayer")
	}
}

func TestSyncPodGang_UnsupportedRequiredTopologyKey_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	// "topology.kubernetes.io/region" has no Koordinator NetworkTopologySpec equivalent.
	// When used as a Required constraint it must be a hard error, not a silent skip —
	// dropping a mandatory placement constraint would violate the user's scheduling intent.
	regionKey := "topology.kubernetes.io/region"
	podGang := newTestPodGang("regiongang", []groveschedulerv1alpha1.PodGroup{
		{
			Name:        "pg-a",
			MinReplicas: 1,
			TopologyConstraint: &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{
					Required: &regionKey,
				},
			},
		},
	})

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.Error(t, err, "an unmappable Required topology key must cause syncPodGang to fail")
	assert.Contains(t, err.Error(), regionKey)
}

func TestSyncPodGang_UnsupportedPreferredTopologyKey_Succeeds(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	// "topology.kubernetes.io/region" has no Koordinator equivalent.
	// When used as a Preferred (best-effort) constraint it should be silently skipped
	// without failing the sync — advisory constraints must not block scheduling.
	regionKey := "topology.kubernetes.io/region"
	podGang := newTestPodGang("regiongang", []groveschedulerv1alpha1.PodGroup{
		{
			Name:        "pg-a",
			MinReplicas: 1,
			TopologyConstraint: &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{
					Preferred: &regionKey,
				},
			},
		},
	})

	require.NoError(t, syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang))

	// PodGroup should have been created, but without a topology annotation.
	pg := getPodGroup(t, cl, "regiongang-pg-a")
	_, hasAnnotation := pg.GetAnnotations()[AnnotationNetworkTopologySpec]
	assert.False(t, hasAnnotation, "unsupported Preferred key should not produce a topology annotation")
}

func TestSyncPodGang_WithReuseReservationRef_WarningEmitted(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})
	podGang.Spec.ReuseReservationRef = &groveschedulerv1alpha1.NamespacedName{
		Namespace: "default",
		Name:      "old-podgang",
	}

	// Should succeed (ReuseReservationRef skipped with warning, not error).
	require.NoError(t, syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang))

	// Warning event should be emitted.
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, corev1.EventTypeWarning)
		assert.Contains(t, event, "UnsupportedFeature")
	default:
		t.Fatal("expected a warning event for ReuseReservationRef but none was emitted")
	}
}

func TestSyncPodGang_PriorityClassName_NotWrittenToSpec(t *testing.T) {
	// The sig-scheduling PodGroup CRD has no priorityClassName field; writing it would be
	// silently pruned by the structural schema. It must not appear in the built object.
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("priogang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})
	podGang.Spec.PriorityClassName = "koord-prod"

	require.NoError(t, syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang))

	pg := getPodGroup(t, cl, "priogang-pg-a")
	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.NotContains(t, spec, "priorityClassName")
}

// -- prune stale PodGroups --

func TestSyncPodGang_PruneRemovedPodGroups(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	// First sync: two PodGroups.
	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
		newPodGroup("pg-b", 2),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))
	getPodGroup(t, cl, "mygang-pg-a")
	getPodGroup(t, cl, "mygang-pg-b")

	// Shrink: remove pg-b from spec.
	podGang.Spec.PodGroups = []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	}
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// pg-a must still exist.
	_ = getPodGroup(t, cl, "mygang-pg-a")

	// pg-b must have been pruned.
	orphaned := &unstructured.Unstructured{}
	orphaned.SetGroupVersionKind(podGroupGVK)
	err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "mygang-pg-b"}, orphaned)
	require.Error(t, err, "stale PodGroup should have been deleted")
	assert.True(t, apierrors.IsNotFound(err), "expected NotFound for pruned PodGroup, got: %v", err)
}

func TestSyncPodGang_PruneRespectsOwnerReference(t *testing.T) {
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	// Pre-create a PodGroup named "mygang-pg-b" owned by a DIFFERENT UID.
	// This simulates a PodGroup created by another PodGang (e.g. after UID rotation).
	differentOwnerPG := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": podGroupGVK.Group + "/" + podGroupGVK.Version,
			"kind":       podGroupGVK.Kind,
			"metadata": map[string]interface{}{
				"name":      "mygang-pg-b",
				"namespace": "default",
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "grove.io/v1alpha1",
						"kind":       "PodGang",
						"name":       "mygang",
						"uid":        "different-uid-999", // NOT the PodGang's UID
						"controller": true,
					},
				},
			},
			"spec": map[string]interface{}{
				"minMember": int64(1),
			},
		},
	}

	cl := testutils.CreateDefaultFakeClient([]client.Object{differentOwnerPG})
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	// PodGang has UID "test-uid-123" (from newTestPodGang) and only pg-a.
	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})

	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// "mygang-pg-b" is owned by a DIFFERENT UID — must NOT be pruned.
	pg := getPodGroup(t, cl, "mygang-pg-b")
	assert.Equal(t, "mygang-pg-b", pg.GetName(), "PodGroup with different owner UID must not be deleted")

	// "mygang-pg-a" should have been created by the sync.
	_ = getPodGroup(t, cl, "mygang-pg-a")
}

// -- topology key exact-match --

func TestTopologyKeyToKoordinatorLayer_ExactMatch(t *testing.T) {
	// The only built-in mapping targets Koordinator's built-in leaf layer; rack/block layer
	// names are administrator-defined in the ClusterNetworkTopology CR and therefore require
	// an explicit TopologyKeyMappings entry.
	tests := []struct {
		key      string
		expected string
	}{
		{"kubernetes.io/hostname", "NodeTopologyLayer"},
		{"topology.kubernetes.io/rack", ""},
		{"topology.kubernetes.io/block", ""},
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			got := topologyKeyToKoordinatorLayer(tc.key, nil)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestTopologyKeyToKoordinatorLayer_UnknownKeySkipped(t *testing.T) {
	got := topologyKeyToKoordinatorLayer("topology.kubernetes.io/region", nil)
	assert.Empty(t, got, "unsupported topology key should return empty string")
}

func TestTopologyKeyToKoordinatorLayer_NoFalsePositiveForHostSubstring(t *testing.T) {
	// Keys containing "host" as a substring must not map to NodeTopologyLayer.
	falsePositives := []string{
		"my.company.com/nfs-hostpath",
		"ghost-node-label",
		"most-preferred-host",
		"host",
		"hostlabel",
		"hostname.custom.io",
	}
	for _, key := range falsePositives {
		t.Run(key, func(t *testing.T) {
			got := topologyKeyToKoordinatorLayer(key, nil)
			assert.Empty(t, got, "key %q must not map to the node layer", key)
		})
	}
}

// -- user topology key mapping --

func TestTopologyKeyToKoordinatorLayer_UserMappingTakesPrecedence(t *testing.T) {
	// Custom key mapped to an administrator-defined layer name (free-form string that must
	// match a layer of the cluster's ClusterNetworkTopology CR).
	userMappings := map[string]string{
		"mycompany.com/custom-block": "spineLayer",
	}
	got := topologyKeyToKoordinatorLayer("mycompany.com/custom-block", userMappings)
	assert.Equal(t, "spineLayer", got)
}

func TestTopologyKeyToKoordinatorLayer_UserMappingOverridesBuiltin(t *testing.T) {
	// User remaps the canonical hostname key to a custom layer instead of NodeTopologyLayer.
	userMappings := map[string]string{
		"kubernetes.io/hostname": "rackLayer",
	}
	got := topologyKeyToKoordinatorLayer("kubernetes.io/hostname", userMappings)
	assert.Equal(t, "rackLayer", got, "user mapping should override built-in")
}

func TestTopologyKeyToKoordinatorLayer_NilUserMappingsFallsBackToBuiltin(t *testing.T) {
	got := topologyKeyToKoordinatorLayer("kubernetes.io/hostname", nil)
	assert.Equal(t, "NodeTopologyLayer", got)
}

// -- historical unlabeled PodGroup prune --

// TestSyncPodGang_PruneUnlabeledHistoricalPodGroup verifies that PodGroups created before the
// grove.io/podgang label was introduced are still pruned when no longer desired.
func TestSyncPodGang_PruneUnlabeledHistoricalPodGroup(t *testing.T) {
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	// Simulate a stale PodGroup created by an older version of Grove (no grove.io/podgang label),
	// but owned (via OwnerReference) by our PodGang UID.
	stalePG := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": podGroupGVK.Group + "/" + podGroupGVK.Version,
			"kind":       podGroupGVK.Kind,
			"metadata": map[string]interface{}{
				"name":      "mygang-pg-stale",
				"namespace": "default",
				// Intentionally NO grove.io/podgang label — this is the historical object.
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion":         "grove.io/v1alpha1",
						"kind":               "PodGang",
						"name":               "mygang",
						"uid":                "test-uid-123", // matches newTestPodGang UID
						"controller":         true,
						"blockOwnerDeletion": true,
					},
				},
			},
			"spec": map[string]interface{}{
				"minMember": int64(1),
			},
		},
	}

	cl := testutils.CreateDefaultFakeClient([]client.Object{stalePG})
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	// PodGang now only has pg-a — pg-stale is no longer desired.
	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})

	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// pg-a should be created.
	_ = getPodGroup(t, cl, "mygang-pg-a")

	// pg-stale (unlabeled historical) must have been pruned via the fallback list pass.
	orphaned := &unstructured.Unstructured{}
	orphaned.SetGroupVersionKind(podGroupGVK)
	err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "mygang-pg-stale"}, orphaned)
	require.Error(t, err, "unlabeled historical PodGroup should have been pruned")
	assert.True(t, apierrors.IsNotFound(err), "expected NotFound for unlabeled historical PodGroup, got: %v", err)
}

// TestSyncPodGang_ForeignPodGroupNotOverwritten verifies that createOrUpdatePodGroup refuses to
// overwrite a same-named PodGroup owned by a different controller UID.
func TestSyncPodGang_ForeignPodGroupNotOverwritten(t *testing.T) {
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	// Pre-create a PodGroup with the name Grove would derive, but owned by a foreign UID.
	foreignOwnerUID := "foreign-owner-uid-999"
	foreignPG := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": podGroupGVK.Group + "/" + podGroupGVK.Version,
			"kind":       podGroupGVK.Kind,
			"metadata": map[string]interface{}{
				"name":      "mygang-pg-a", // same name Grove would generate
				"namespace": "default",
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "grove.io/v1alpha1",
						"kind":       "PodGang",
						"name":       "other-gang",
						"uid":        foreignOwnerUID,
						"controller": true,
					},
				},
			},
			"spec": map[string]interface{}{
				"minMember": int64(5),
			},
		},
	}

	cl := testutils.CreateDefaultFakeClient([]client.Object{foreignPG})
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	// Our PodGang has UID "test-uid-123" (from newTestPodGang), different from foreignOwnerUID.
	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 2),
	})

	err := syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang)
	require.Error(t, err, "syncPodGang must fail when the derived PodGroup name is owned by a different controller")
	assert.Contains(t, err.Error(), "different owner", "error should mention the ownership conflict")

	// The foreign PodGroup's spec must be unchanged (minMember still 5, not 2).
	existing := getPodGroup(t, cl, "mygang-pg-a")
	spec, _, _ := unstructured.NestedMap(existing.Object, "spec")
	assert.EqualValues(t, int64(5), spec["minMember"], "foreign PodGroup must not have been overwritten")
}

// TestSyncPodGang_ScheduleTimeoutAnnotation_OverridesConfig verifies that the workload-level
// schedule-timeout annotation (mirrored from the PCS onto the PodGang) overrides the
// profile-level default for every PodGroup of the gang.
func TestSyncPodGang_ScheduleTimeoutAnnotation_OverridesConfig(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	podGang := newTestPodGang("timeoutgang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})
	podGang.Annotations = map[string]string{AnnotationScheduleTimeoutSeconds: "600"}

	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	pg := getPodGroup(t, cl, "timeoutgang-pg-a")
	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.EqualValues(t, 600, spec["scheduleTimeoutSeconds"])
}

// TestSyncPodGang_InvalidScheduleTimeoutAnnotation_ReturnsError verifies that a malformed
// timeout annotation fails the sync instead of being silently ignored.
func TestSyncPodGang_InvalidScheduleTimeoutAnnotation_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	podGang := newTestPodGang("timeoutgang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})
	podGang.Annotations = map[string]string{AnnotationScheduleTimeoutSeconds: "zero"}

	err := syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang)
	require.Error(t, err)
	assert.Contains(t, err.Error(), AnnotationScheduleTimeoutSeconds)
}

// TestSyncPodGang_UpdatePreservesStatus verifies that a re-sync does not wipe the status
// maintained by Koordinator's PodGroupController: the PodGroup CRD has no status subresource,
// so status is part of the main resource and would be lost on a whole-object Update.
func TestSyncPodGang_UpdatePreservesStatus(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	podGang := newTestPodGang("statusgang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 2, 2),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// Simulate koord-scheduler's PodGroupController writing status to the main resource.
	pg := getPodGroup(t, cl, "statusgang-pg-a")
	require.NoError(t, unstructured.SetNestedMap(pg.Object, map[string]interface{}{
		"phase":     "Running",
		"scheduled": int64(2),
	}, "status"))
	require.NoError(t, cl.Update(context.Background(), pg))

	// A second sync (e.g. triggered by a PodGang update) must carry the status over.
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	pg = getPodGroup(t, cl, "statusgang-pg-a")
	status, found, err := unstructured.NestedMap(pg.Object, "status")
	require.NoError(t, err)
	require.True(t, found, "status must survive the operator's update")
	assert.Equal(t, "Running", status["phase"])
}

// TestSyncPodGang_CreateFailureSkipsPrune verifies the sync-order safety guarantee: PodGroups
// are created/updated BEFORE stale ones are pruned, so a mid-loop create failure must leave
// existing (now-stale) PodGroups untouched — the gang stays consistent until the retry.
func TestSyncPodGang_CreateFailureSkipsPrune(t *testing.T) {
	scheme := testutils.NewTestClientBuilder().Build().Scheme()
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	// An existing PodGroup owned by the PodGang but absent from the new desired spec:
	// prune would delete it if it ran.
	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-new", 1),
	})
	stale := &unstructured.Unstructured{}
	stale.SetGroupVersionKind(podGroupGVK)
	stale.SetName("mygang-pg-old")
	stale.SetNamespace("default")
	stale.SetLabels(map[string]string{common.LabelPodGang: "mygang"})
	require.NoError(t, controllerutil.SetControllerReference(podGang, stale, scheme))

	cl := testutils.NewTestClientBuilder().
		WithObjects(stale).
		RecordErrorForObjects(testutils.ClientMethodCreate,
			apierrors.NewInternalError(fmt.Errorf("injected create failure")),
			client.ObjectKey{Namespace: "default", Name: "mygang-pg-new"}).
		Build()

	err := syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang)
	require.Error(t, err, "a create failure must fail the sync")

	// The stale PodGroup must still exist: prune runs only after the desired set is durable.
	_ = getPodGroup(t, cl, "mygang-pg-old")
}

// -- Coherent-update constraint release regression tests --

// TestSyncPodGang_ConstraintsReleased_PreservesExistingGangConstraints verifies that when a
// coherent update releases Grove constraints (MinReplicas 0), the existing PodGroup keeps its
// recorded minMember and total-number instead of being rewritten to 0.
func TestSyncPodGang_ConstraintsReleased_PreservesExistingGangConstraints(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	// First sync with real constraints.
	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 2, 3),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// Second sync simulating a coherent update: all MinReplicas released to 0.
	released := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 0, 3),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, released))

	pg := getPodGroup(t, cl, "mygang-pg-a")
	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.EqualValues(t, 2, spec["minMember"], "minMember must be preserved during constraint release")
	assert.Equal(t, "3", pg.GetAnnotations()[AnnotationGangTotalNum], "total-number must be preserved during constraint release")
}

// TestSyncPodGang_ConstraintsReleased_DropsTotalNumWhenExistingHasNone pins down the delete branch
// of carryOverGangConstraints: a released sync must not introduce a total-number annotation the
// pre-release PodGroup never had, even though desired would now compute one from PodReferences.
func TestSyncPodGang_ConstraintsReleased_DropsTotalNumWhenExistingHasNone(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	// First sync without PodReferences: total-number is omitted.
	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 2),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))
	pg := getPodGroup(t, cl, "mygang-pg-a")
	_, hasTotal := pg.GetAnnotations()[AnnotationGangTotalNum]
	require.False(t, hasTotal, "precondition: first sync without refs must omit total-number")

	// Released sync (all MinReplicas 0) whose desired object WOULD compute total-number=3.
	released := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 0, 3),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, released))

	pg = getPodGroup(t, cl, "mygang-pg-a")
	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.EqualValues(t, 2, spec["minMember"], "minMember must be preserved during constraint release")
	_, hasTotal = pg.GetAnnotations()[AnnotationGangTotalNum]
	assert.False(t, hasTotal,
		"released sync must mirror the existing PodGroup's total-number state (absent), not the released desired's computation")
}

// TestSyncPodGang_ConstraintsReleased_NewPodGroupGetsCurrentValues verifies that a PodGroup first
// created while constraints are released is written with the released values (as in the volcano backend).
func TestSyncPodGang_ConstraintsReleased_NewPodGroupGetsCurrentValues(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	released := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 0, 3),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, released))

	pg := getPodGroup(t, cl, "mygang-pg-a")
	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.EqualValues(t, 0, spec["minMember"])
}

// TestSyncPodGang_PartialRelease_PreservesReleasedEntryOnly verifies that a release covering only
// part of the gang is handled per PodGroup: the released entry keeps its recorded constraint while
// the untouched entry follows its spec. This mixed state (standalone PodCliques released,
// PCSG-backed ones untouched) is what a real release looks like.
func TestSyncPodGang_PartialRelease_PreservesReleasedEntryOnly(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	podGang := newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 2, 3),
		newPodGroupWithRefs("pg-b", 1, 1),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// pg-a is released to 0 while pg-b keeps its constraint — a partial release.
	podGang = newTestPodGang("mygang", []groveschedulerv1alpha1.PodGroup{
		newPodGroupWithRefs("pg-a", 0, 3),
		newPodGroupWithRefs("pg-b", 1, 1),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	pgA := getPodGroup(t, cl, "mygang-pg-a")
	specA, _, _ := unstructured.NestedMap(pgA.Object, "spec")
	assert.EqualValues(t, 2, specA["minMember"],
		"the released entry must keep the gang constraint recorded on its existing PodGroup")

	pgB := getPodGroup(t, cl, "mygang-pg-b")
	specB, _, _ := unstructured.NestedMap(pgB.Object, "spec")
	assert.EqualValues(t, 1, specB["minMember"],
		"an entry that was not released must keep following its spec")
}

// -- TopologyConstraintGroupConfigs rejection regression test --

// TestSyncPodGang_TopologyConstraintGroupConfigs_ReturnsError verifies that syncPodGang fails when
// PodGang.Spec.TopologyConstraintGroupConfigs is non-empty: per-subset topology cannot be represented
// with independent PodGroup CRs and must not be dropped silently.
func TestSyncPodGang_TopologyConstraintGroupConfigs_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	podGang := newTestPodGang("pcsg-gang", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 2),
		newPodGroup("pg-b", 1),
	})
	// Simulate the PCSG controller populating per-subset topology constraints.
	podGang.Spec.TopologyConstraintGroupConfigs = []groveschedulerv1alpha1.TopologyConstraintGroupConfig{
		{
			Name:          "gpu-group",
			PodGroupNames: []string{"pg-a"},
		},
	}

	err := syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang)
	require.Error(t, err, "syncPodGang must fail when TopologyConstraintGroupConfigs is non-empty")
	assert.Contains(t, err.Error(), "TopologyConstraintGroupConfigs")
}
