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
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newCTB(name string, levels []grovecorev1alpha1.TopologyLevel) *grovecorev1alpha1.ClusterTopologyBinding {
	return &grovecorev1alpha1.ClusterTopologyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID("ctb-uid-" + name),
		},
		Spec: grovecorev1alpha1.ClusterTopologyBindingSpec{
			Levels: levels,
		},
	}
}

func rackHostLevels() []grovecorev1alpha1.TopologyLevel {
	return []grovecorev1alpha1.TopologyLevel{
		{Domain: grovecorev1alpha1.TopologyDomainBlock, Key: "topology.kubernetes.io/block"},
		{Domain: grovecorev1alpha1.TopologyDomainRack, Key: "topology.kubernetes.io/rack"},
		{Domain: grovecorev1alpha1.TopologyDomainHost, Key: "kubernetes.io/hostname"},
	}
}

func newTopologyBackend(t *testing.T, objects ...client.Object) *schedulerBackend {
	t.Helper()
	cl := testutils.CreateDefaultFakeClient(objects)
	return &schedulerBackend{
		client: cl,
		scheme: cl.Scheme(),
		cfg:    defaultCfg(),
	}
}

func getCNT(t *testing.T, cl client.Client) *unstructured.Unstructured {
	t.Helper()
	cnt := &unstructured.Unstructured{}
	cnt.SetGroupVersionKind(cntGVK)
	require.NoError(t, cl.Get(context.Background(), client.ObjectKey{Name: cntDefaultName}, cnt))
	return cnt
}

func TestBuildCNTNetworkTopologySpec(t *testing.T) {
	entries, err := buildCNTNetworkTopologySpec(rackHostLevels())
	require.NoError(t, err)
	require.Len(t, entries, 3, "block + rack + NodeTopologyLayer leaf")

	block := entries[0].(map[string]interface{})
	assert.Equal(t, "topology.kubernetes.io/block", block["topologyLayer"])
	assert.Nil(t, block["parentTopologyLayer"])

	rack := entries[1].(map[string]interface{})
	assert.Equal(t, "topology.kubernetes.io/rack", rack["topologyLayer"])
	assert.Equal(t, "topology.kubernetes.io/block", rack["parentTopologyLayer"])
	assert.Equal(t, []interface{}{"topology.kubernetes.io/rack"}, rack["labelKey"])

	leaf := entries[2].(map[string]interface{})
	assert.Equal(t, cntLayerNode, leaf["topologyLayer"])
	assert.Equal(t, "topology.kubernetes.io/rack", leaf["parentTopologyLayer"])
	assert.Nil(t, leaf["labelKey"], "the node leaf attaches by node name, not by label")
}

func TestBuildCNTNetworkTopologySpec_NumaRejected(t *testing.T) {
	levels := append(rackHostLevels(), grovecorev1alpha1.TopologyLevel{
		Domain: grovecorev1alpha1.TopologyDomainNuma, Key: "numa.kubernetes.io/id",
	})
	_, err := buildCNTNetworkTopologySpec(levels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "numa")
}

func TestBuildCNTNetworkTopologySpec_HostNotNarrowestRejected(t *testing.T) {
	levels := []grovecorev1alpha1.TopologyLevel{
		{Domain: grovecorev1alpha1.TopologyDomainHost, Key: "kubernetes.io/hostname"},
		{Domain: grovecorev1alpha1.TopologyDomainRack, Key: "topology.kubernetes.io/rack"},
	}
	_, err := buildCNTNetworkTopologySpec(levels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "narrowest")
}

func TestSyncTopology_CreatesDefaultCNT(t *testing.T) {
	b := newTopologyBackend(t)
	ctb := newCTB("cluster-topo", rackHostLevels())

	require.NoError(t, b.SyncTopology(context.Background(), nil, ctb))

	cnt := getCNT(t, b.client)
	assert.Equal(t, cntDefaultName, cnt.GetName())
	require.True(t, metav1.IsControlledBy(cnt, ctb), "CNT must be owned by the CTB for GC")
	entries, _, err := unstructured.NestedSlice(cnt.Object, "spec", "networkTopologySpec")
	require.NoError(t, err)
	assert.Len(t, entries, 3)

	// The derived key→layer mapping must feed gang topology annotation translation.
	merged := b.mergedTopologyKeyMappings()
	assert.Equal(t, "topology.kubernetes.io/rack", merged["topology.kubernetes.io/rack"])
	assert.Equal(t, cntLayerNode, merged["kubernetes.io/hostname"])
}

func TestSyncTopology_UpdatesInPlaceAndPreservesStatus(t *testing.T) {
	b := newTopologyBackend(t)
	ctb := newCTB("cluster-topo", rackHostLevels())
	require.NoError(t, b.SyncTopology(context.Background(), nil, ctb))

	// Simulate koord-scheduler writing status to the CNT.
	cnt := getCNT(t, b.client)
	require.NoError(t, unstructured.SetNestedField(cnt.Object, int64(12), "status", "detailStatusCount"))
	require.NoError(t, b.client.Update(context.Background(), cnt))

	// Change the CTB levels and re-sync: the CNT is updated in place (no delete/recreate).
	ctb.Spec.Levels = []grovecorev1alpha1.TopologyLevel{
		{Domain: grovecorev1alpha1.TopologyDomainRack, Key: "topology.kubernetes.io/rack"},
		{Domain: grovecorev1alpha1.TopologyDomainHost, Key: "kubernetes.io/hostname"},
	}
	require.NoError(t, b.SyncTopology(context.Background(), nil, ctb))

	cnt = getCNT(t, b.client)
	entries, _, err := unstructured.NestedSlice(cnt.Object, "spec", "networkTopologySpec")
	require.NoError(t, err)
	assert.Len(t, entries, 2, "rack + leaf after the level was removed")
	statusVal, found, err := unstructured.NestedInt64(cnt.Object, "status", "detailStatusCount")
	require.NoError(t, err)
	assert.True(t, found, "status written by koord-scheduler must survive the spec update")
	assert.EqualValues(t, 12, statusVal)
}

func TestSyncTopology_RefusesForeignCNT(t *testing.T) {
	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(cntGVK)
	foreign.SetName(cntDefaultName)
	b := newTopologyBackend(t, foreign)

	err := b.SyncTopology(context.Background(), nil, newCTB("cluster-topo", rackHostLevels()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not owned by")
}

func TestCheckTopologyDrift_InSyncWithAdminNamedLayers(t *testing.T) {
	// Administrator-managed CNT with custom layer names but a structurally matching chain:
	// drift check must pass and record the admin's layer names for annotation translation.
	adminCNT := &unstructured.Unstructured{}
	adminCNT.SetGroupVersionKind(cntGVK)
	adminCNT.SetName(cntDefaultName)
	require.NoError(t, unstructured.SetNestedSlice(adminCNT.Object, []interface{}{
		map[string]interface{}{
			"topologyLayer": "blockLayer",
			"labelKey":      []interface{}{"topology.kubernetes.io/block"},
		},
		map[string]interface{}{
			"topologyLayer":       "rackLayer",
			"parentTopologyLayer": "blockLayer",
			"labelKey":            []interface{}{"topology.kubernetes.io/rack"},
		},
		map[string]interface{}{
			"topologyLayer":       cntLayerNode,
			"parentTopologyLayer": "rackLayer",
		},
	}, "spec", "networkTopologySpec"))

	b := newTopologyBackend(t, adminCNT)
	ctb := newCTB("cluster-topo", rackHostLevels())
	ref := grovecorev1alpha1.SchedulerTopologyBinding{
		SchedulerName:     "koord-scheduler",
		TopologyReference: cntDefaultName,
	}

	inSync, msg, _, err := b.CheckTopologyDrift(context.Background(), nil, ctb, ref)
	require.NoError(t, err)
	assert.True(t, inSync, "structurally matching CNT must be in sync regardless of layer names: %s", msg)

	merged := b.mergedTopologyKeyMappings()
	assert.Equal(t, "rackLayer", merged["topology.kubernetes.io/rack"],
		"admin layer names must be recorded for annotation translation")
	assert.Equal(t, "blockLayer", merged["topology.kubernetes.io/block"])
	assert.Equal(t, cntLayerNode, merged["kubernetes.io/hostname"])
}

func TestCheckTopologyDrift_MismatchedKeysReported(t *testing.T) {
	adminCNT := &unstructured.Unstructured{}
	adminCNT.SetGroupVersionKind(cntGVK)
	adminCNT.SetName(cntDefaultName)
	require.NoError(t, unstructured.SetNestedSlice(adminCNT.Object, []interface{}{
		map[string]interface{}{
			"topologyLayer": "spineLayer",
			"labelKey":      []interface{}{"mycompany.com/spine"},
		},
		map[string]interface{}{
			"topologyLayer":       cntLayerNode,
			"parentTopologyLayer": "spineLayer",
		},
	}, "spec", "networkTopologySpec"))

	b := newTopologyBackend(t, adminCNT)
	ctb := newCTB("cluster-topo", rackHostLevels())
	ref := grovecorev1alpha1.SchedulerTopologyBinding{
		SchedulerName:     "koord-scheduler",
		TopologyReference: cntDefaultName,
	}

	inSync, msg, _, err := b.CheckTopologyDrift(context.Background(), nil, ctb, ref)
	require.NoError(t, err)
	assert.False(t, inSync)
	assert.NotEmpty(t, msg)
}

func TestCheckTopologyDrift_NonDefaultReference_Rejected(t *testing.T) {
	// koord-scheduler only reads the CNT named "default"; a structurally matching CNT under
	// another name must be reported as out of sync rather than blessing a topology the
	// scheduler never consumes.
	b := newTopologyBackend(t)
	ref := grovecorev1alpha1.SchedulerTopologyBinding{
		SchedulerName:     "koord-scheduler",
		TopologyReference: "my-topology",
	}
	inSync, msg, _, err := b.CheckTopologyDrift(context.Background(), nil, newCTB("x", rackHostLevels()), ref)
	require.NoError(t, err)
	assert.False(t, inSync)
	assert.Contains(t, msg, "default")
}

func TestOrderedNonLeafCNTLayers_DuplicateLayerRejected(t *testing.T) {
	// koordinator's tree builder rejects duplicate layer names; the drift check must not
	// silently collapse them into an in-sync verdict.
	entries := []interface{}{
		map[string]interface{}{
			"topologyLayer": "rackLayer",
			"labelKey":      []interface{}{"topology.kubernetes.io/rack"},
		},
		map[string]interface{}{
			"topologyLayer": "rackLayer",
			"labelKey":      []interface{}{"topology.kubernetes.io/rack"},
		},
		map[string]interface{}{
			"topologyLayer":       cntLayerNode,
			"parentTopologyLayer": "rackLayer",
		},
	}
	_, msg := orderedNonLeafCNTLayers(entries)
	assert.Contains(t, msg, "more than once")
}

func TestCheckTopologyDrift_NotFound(t *testing.T) {
	b := newTopologyBackend(t)
	ref := grovecorev1alpha1.SchedulerTopologyBinding{
		SchedulerName:     "koord-scheduler",
		TopologyReference: cntDefaultName,
	}
	inSync, msg, gen, err := b.CheckTopologyDrift(context.Background(), nil, newCTB("x", rackHostLevels()), ref)
	require.NoError(t, err)
	assert.False(t, inSync)
	assert.Contains(t, msg, "not found")
	assert.Zero(t, gen)
}

func TestMergedTopologyKeyMappings_UserOverridesWin(t *testing.T) {
	b := newTopologyBackend(t)
	b.cfg.topologyKeyMappings = map[string]string{"topology.kubernetes.io/rack": "userRack"}
	require.NoError(t, b.SyncTopology(context.Background(), nil, newCTB("cluster-topo", rackHostLevels())))

	merged := b.mergedTopologyKeyMappings()
	assert.Equal(t, "userRack", merged["topology.kubernetes.io/rack"], "explicit user mapping must win")
	assert.Equal(t, "topology.kubernetes.io/block", merged["topology.kubernetes.io/block"])
}
