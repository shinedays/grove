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
	"encoding/json"
	"testing"

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
)

// newTestPodGang creates a PodGang with one or more PodGroups for testing.
func newTestPodGang(name, namespace string, podGroups []groveschedulerv1alpha1.PodGroup) *groveschedulerv1alpha1.PodGang {
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

func defaultTestCfg() backendConfig {
	return backendConfig{
		GangMode:               DefaultGangMode,
		MatchPolicy:            DefaultMatchPolicy,
		ScheduleTimeoutSeconds: DefaultTimeoutSecs,
	}
}

// getPodGroup fetches a Koordinator PodGroup from the fake client by name.
func getPodGroup(t *testing.T, cl client.Client, namespace, name string) *unstructured.Unstructured {
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

	podGang := newTestPodGang("mygang", "default", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 2),
	})

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.NoError(t, err)

	pg := getPodGroup(t, cl, "default", "mygang-pg-a")
	assert.Equal(t, "mygang-pg-a", pg.GetName())
	assert.Equal(t, "default", pg.GetNamespace())

	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.EqualValues(t, 2, spec["minMember"])
	assert.EqualValues(t, DefaultTimeoutSecs, spec["scheduleTimeoutSeconds"])

	annotations := pg.GetAnnotations()
	assert.Equal(t, DefaultGangMode, annotations[AnnotationGangMode])
	assert.Equal(t, DefaultMatchPolicy, annotations[AnnotationGangMatchPolicy])
	assert.Equal(t, "1", annotations[AnnotationGangTotalNum])
	assert.Contains(t, annotations[AnnotationGangGroups], "default/mygang-pg-a")
}

func TestSyncPodGang_MultiGroup_GangGroupAnnotationsLinked(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("multigang", "default", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
		newPodGroup("pg-b", 3),
	})

	err := syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang)
	require.NoError(t, err)

	// Both PodGroups should be created.
	pgA := getPodGroup(t, cl, "default", "multigang-pg-a")
	pgB := getPodGroup(t, cl, "default", "multigang-pg-b")

	// Both should reference the same GangGroup.
	gangGroupA := pgA.GetAnnotations()[AnnotationGangGroups]
	gangGroupB := pgB.GetAnnotations()[AnnotationGangGroups]
	assert.Equal(t, gangGroupA, gangGroupB, "GangGroup annotations must be identical across PodGroups")

	var groupsA []string
	require.NoError(t, json.Unmarshal([]byte(gangGroupA), &groupsA))
	assert.Len(t, groupsA, 2)
	assert.Contains(t, groupsA, "default/multigang-pg-a")
	assert.Contains(t, groupsA, "default/multigang-pg-b")

	// totalNumber should reflect the number of PodGroups.
	assert.Equal(t, "2", pgA.GetAnnotations()[AnnotationGangTotalNum])
	assert.Equal(t, "2", pgB.GetAnnotations()[AnnotationGangTotalNum])

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

	podGang := newTestPodGang("mygang", "default", []groveschedulerv1alpha1.PodGroup{
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

func TestSyncPodGang_WithHostTopology(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	requiredKey := "kubernetes.io/hostname"
	podGang := newTestPodGang("topogang", "default", []groveschedulerv1alpha1.PodGroup{
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

	pg := getPodGroup(t, cl, "default", "topogang-pg-a")
	topoAnnotation := pg.GetAnnotations()[AnnotationNetworkTopologySpec]
	assert.NotEmpty(t, topoAnnotation, "topology annotation should be set for host-level key")
	assert.Contains(t, topoAnnotation, "hostLayer")
	assert.Contains(t, topoAnnotation, "MustGather")
}

func TestSyncPodGang_WithRackTopology(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	preferredKey := "topology.kubernetes.io/rack"
	podGang := newTestPodGang("rackgang", "default", []groveschedulerv1alpha1.PodGroup{
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

	pg := getPodGroup(t, cl, "default", "rackgang-pg-a")
	topoAnnotation := pg.GetAnnotations()[AnnotationNetworkTopologySpec]
	assert.NotEmpty(t, topoAnnotation)
	assert.Contains(t, topoAnnotation, "rackLayer")
	assert.Contains(t, topoAnnotation, "PreferGather")
}

func TestSyncPodGang_UnsupportedRequiredTopologyKey_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	// "topology.kubernetes.io/region" has no Koordinator NetworkTopologySpec equivalent.
	// When used as a Required constraint it must be a hard error, not a silent skip —
	// dropping a mandatory placement constraint would violate the user's scheduling intent.
	regionKey := "topology.kubernetes.io/region"
	podGang := newTestPodGang("regiongang", "default", []groveschedulerv1alpha1.PodGroup{
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
	podGang := newTestPodGang("regiongang", "default", []groveschedulerv1alpha1.PodGroup{
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
	pg := getPodGroup(t, cl, "default", "regiongang-pg-a")
	_, hasAnnotation := pg.GetAnnotations()[AnnotationNetworkTopologySpec]
	assert.False(t, hasAnnotation, "unsupported Preferred key should not produce a topology annotation")
}

func TestSyncPodGang_WithReuseReservationRef_WarningEmitted(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("mygang", "default", []groveschedulerv1alpha1.PodGroup{
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

func TestSyncPodGang_PriorityClassName_SetInSpec(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()

	podGang := newTestPodGang("priogang", "default", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})
	podGang.Spec.PriorityClassName = "koord-prod"

	require.NoError(t, syncPodGang(context.Background(), cl, testutils.NewTestClientBuilder().Build().Scheme(), recorder, cfg, podGang))

	pg := getPodGroup(t, cl, "default", "priogang-pg-a")
	spec, _, _ := unstructured.NestedMap(pg.Object, "spec")
	assert.Equal(t, "koord-prod", spec["priorityClassName"])
}

// -- P0: prune stale PodGroups regression tests --

func TestSyncPodGang_PruneRemovedPodGroups(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	// First sync: two PodGroups.
	podGang := newTestPodGang("mygang", "default", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
		newPodGroup("pg-b", 2),
	})
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))
	getPodGroup(t, cl, "default", "mygang-pg-a")
	getPodGroup(t, cl, "default", "mygang-pg-b")

	// Shrink: remove pg-b from spec.
	podGang.Spec.PodGroups = []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	}
	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// pg-a must still exist.
	_ = getPodGroup(t, cl, "default", "mygang-pg-a")

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
	podGang := newTestPodGang("mygang", "default", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})

	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// "mygang-pg-b" is owned by a DIFFERENT UID — must NOT be pruned.
	pg := getPodGroup(t, cl, "default", "mygang-pg-b")
	assert.Equal(t, "mygang-pg-b", pg.GetName(), "PodGroup with different owner UID must not be deleted")

	// "mygang-pg-a" should have been created by the sync.
	_ = getPodGroup(t, cl, "default", "mygang-pg-a")
}

// -- P1: topology key exact-match regression tests --

func TestTopologyKeyToKoordinatorLayer_ExactMatch(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"kubernetes.io/hostname", "hostLayer"},
		{"topology.kubernetes.io/rack", "rackLayer"},
		{"topology.kubernetes.io/block", "blockLayer"},
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
	// Keys that contain "host" as a substring but are NOT "kubernetes.io/hostname".
	// The old strings.Contains("host") heuristic would have incorrectly mapped these.
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
			assert.Empty(t, got, "key %q must not map to hostLayer", key)
		})
	}
}

// -- A6: user topology key mapping tests --

func TestTopologyKeyToKoordinatorLayer_UserMappingTakesPrecedence(t *testing.T) {
	// Custom key that has no built-in mapping.
	userMappings := map[string]string{
		"mycompany.com/custom-block": "blockLayer",
	}
	got := topologyKeyToKoordinatorLayer("mycompany.com/custom-block", userMappings)
	assert.Equal(t, "blockLayer", got)
}

func TestTopologyKeyToKoordinatorLayer_UserMappingOverridesBuiltin(t *testing.T) {
	// User remaps the canonical hostname key to rackLayer instead of hostLayer.
	userMappings := map[string]string{
		"kubernetes.io/hostname": "rackLayer",
	}
	got := topologyKeyToKoordinatorLayer("kubernetes.io/hostname", userMappings)
	assert.Equal(t, "rackLayer", got, "user mapping should override built-in")
}

func TestTopologyKeyToKoordinatorLayer_NilUserMappingsFallsBackToBuiltin(t *testing.T) {
	got := topologyKeyToKoordinatorLayer("kubernetes.io/hostname", nil)
	assert.Equal(t, "hostLayer", got)
}

// -- F2: historical unlabeled PodGroup prune regression test --

// TestSyncPodGang_PruneUnlabeledHistoricalPodGroup verifies that PodGroups created before the
// grove.io/podgang label was introduced (i.e. missing the label) are still pruned when
// they are no longer part of the desired spec. This is the migration-window regression test.
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
	podGang := newTestPodGang("mygang", "default", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 1),
	})

	require.NoError(t, syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang))

	// pg-a should be created.
	_ = getPodGroup(t, cl, "default", "mygang-pg-a")

	// pg-stale (unlabeled historical) must have been pruned via the fallback list pass.
	orphaned := &unstructured.Unstructured{}
	orphaned.SetGroupVersionKind(podGroupGVK)
	err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "mygang-pg-stale"}, orphaned)
	require.Error(t, err, "unlabeled historical PodGroup should have been pruned")
	assert.True(t, apierrors.IsNotFound(err), "expected NotFound for unlabeled historical PodGroup, got: %v", err)
}

// TestSyncPodGang_ForeignPodGroupNotOverwritten verifies that createOrUpdatePodGroup
// refuses to overwrite a PodGroup that is owned by a different controller UID.
// This prevents Grove from silently corrupting scheduling state of unrelated workloads
// when a same-named PodGroup happens to exist in the namespace.
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
	podGang := newTestPodGang("mygang", "default", []groveschedulerv1alpha1.PodGroup{
		newPodGroup("pg-a", 2),
	})

	err := syncPodGang(context.Background(), cl, scheme, recorder, cfg, podGang)
	require.Error(t, err, "syncPodGang must fail when the derived PodGroup name is owned by a different controller")
	assert.Contains(t, err.Error(), "different owner", "error should mention the ownership conflict")

	// The foreign PodGroup's spec must be unchanged (minMember still 5, not 2).
	existing := getPodGroup(t, cl, "default", "mygang-pg-a")
	spec, _, _ := unstructured.NestedMap(existing.Object, "spec")
	assert.EqualValues(t, int64(5), spec["minMember"], "foreign PodGroup must not have been overwritten")
}

func TestOnPodGangDelete_Noop(t *testing.T) {
	// OnPodGangDelete is a no-op: PodGroups are garbage-collected via OwnerReference.
	// This test verifies the method returns nil without error.
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	backend := &schedulerBackend{
		client:        cl,
		eventRecorder: recorder,
		cfg:           defaultTestCfg(),
	}
	podGang := newTestPodGang("mygang", "default", nil)
	err := backend.OnPodGangDelete(context.Background(), podGang)
	assert.NoError(t, err)
}

// -- TopologyConstraintGroupConfigs rejection regression test --

// TestSyncPodGang_TopologyConstraintGroupConfigs_ReturnsError verifies that syncPodGang
// returns an error when PodGang.Spec.TopologyConstraintGroupConfigs is non-empty.
//
// The koord backend cannot represent per-subset topology constraints within a GangGroup
// (each Koordinator PodGroup CR is independent and does not share topology state with
// sibling PodGroups). Silently dropping these constraints would violate the user's
// placement requirements, so we fail fast instead.
func TestSyncPodGang_TopologyConstraintGroupConfigs_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	cfg := defaultTestCfg()
	scheme := testutils.NewTestClientBuilder().Build().Scheme()

	podGang := newTestPodGang("pcsg-gang", "default", []groveschedulerv1alpha1.PodGroup{
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
