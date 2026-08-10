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
	"reflect"
	"slices"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ scheduler.TopologyAwareBackend = (*schedulerBackend)(nil)

// Koordinator ClusterNetworkTopology (CNT) constants. Koordinator only ever writes the CNT
// status, never its spec, which is what makes Grove-managed spec synchronisation safe.
const (
	// cntDefaultName is the only CNT instance koord-scheduler reads (hardcoded upstream).
	cntDefaultName = "default"
	// cntLayerNode is Koordinator's built-in leaf layer; nodes attach to it by node name.
	cntLayerNode = "NodeTopologyLayer"
)

var cntGVK = schema.GroupVersionKind{
	Group:   "scheduling.koordinator.sh",
	Version: "v1alpha1",
	Kind:    "ClusterNetworkTopology",
}

// TopologyGVR returns the GroupVersionResource of Koordinator's ClusterNetworkTopology CRD.
func (b *schedulerBackend) TopologyGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    cntGVK.Group,
		Version:  cntGVK.Version,
		Resource: "clusternetworktopologies",
	}
}

// TopologyResourceName returns "default": koord-scheduler only reads the CNT of that name, so a
// cluster can host at most one Koordinator-synced ClusterTopologyBinding.
func (b *schedulerBackend) TopologyResourceName(_ *grovecorev1alpha1.ClusterTopologyBinding) string {
	return cntDefaultName
}

// SyncTopology creates or updates the "default" ClusterNetworkTopology spec for the given
// ClusterTopologyBinding (auto-managed path); status is owned by koord-scheduler. The CNT spec
// is mutable, so updates happen in place.
func (b *schedulerBackend) SyncTopology(ctx context.Context, k8sClient client.Client, ct *grovecorev1alpha1.ClusterTopologyBinding) error {
	if k8sClient == nil {
		k8sClient = b.client
	}
	logger := log.FromContext(ctx)

	desiredSpec, err := buildCNTNetworkTopologySpec(ct.Spec.Levels)
	if err != nil {
		return fmt.Errorf("cannot map ClusterTopologyBinding %q to a ClusterNetworkTopology: %w", ct.Name, err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(cntGVK)
	if err = k8sClient.Get(ctx, client.ObjectKey{Name: cntDefaultName}, existing); err != nil {
		if apimeta.IsNoMatchError(err) {
			return fmt.Errorf("the ClusterNetworkTopology CRD (%s) is not installed; "+
				"install Koordinator (or remove the ClusterTopologyBinding) to use topology-aware scheduling with koord-scheduler: %w",
				cntGVK.GroupVersion().String(), err)
		}
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get ClusterNetworkTopology %q: %w", cntDefaultName, err)
		}
		desired := &unstructured.Unstructured{}
		desired.SetGroupVersionKind(cntGVK)
		desired.SetName(cntDefaultName)
		if err = unstructured.SetNestedSlice(desired.Object, desiredSpec, "spec", "networkTopologySpec"); err != nil {
			return fmt.Errorf("failed to set CNT spec: %w", err)
		}
		if err = controllerutil.SetControllerReference(ct, desired, b.scheme); err != nil {
			return fmt.Errorf("failed to set owner reference on ClusterNetworkTopology: %w", err)
		}
		if err = k8sClient.Create(ctx, desired); err != nil {
			return fmt.Errorf("failed to create ClusterNetworkTopology %q: %w", cntDefaultName, err)
		}
		logger.Info("Created Koordinator ClusterNetworkTopology", "name", cntDefaultName, "clusterTopologyBinding", ct.Name)
		b.rememberTopologyLayers(ct.Spec.Levels, nil)
		return nil
	}

	// Refuse to take over a CNT the operator did not create; administrator-managed CNTs must be
	// bound via schedulerTopologyBindings instead.
	if !metav1.IsControlledBy(existing, ct) {
		return fmt.Errorf("ClusterNetworkTopology %q exists but is not owned by ClusterTopologyBinding %q; "+
			"if it is administrator-managed, reference it via schedulerTopologyBindings instead of auto-managing it",
			cntDefaultName, ct.Name)
	}

	currentSpec, _, err := unstructured.NestedSlice(existing.Object, "spec", "networkTopologySpec")
	if err != nil {
		return fmt.Errorf("failed to read CNT spec: %w", err)
	}
	if !reflect.DeepEqual(currentSpec, desiredSpec) {
		if err = unstructured.SetNestedSlice(existing.Object, desiredSpec, "spec", "networkTopologySpec"); err != nil {
			return fmt.Errorf("failed to set CNT spec: %w", err)
		}
		if err = k8sClient.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update ClusterNetworkTopology %q: %w", cntDefaultName, err)
		}
		logger.Info("Updated Koordinator ClusterNetworkTopology", "name", cntDefaultName, "clusterTopologyBinding", ct.Name)
	}
	b.rememberTopologyLayers(ct.Spec.Levels, nil)
	return nil
}

// OnTopologyDelete is a no-op for Koordinator; the OwnerReference cascade handles deletion
// (both ClusterTopologyBinding and ClusterNetworkTopology are cluster-scoped).
func (b *schedulerBackend) OnTopologyDelete(_ context.Context, _ client.Client, _ *grovecorev1alpha1.ClusterTopologyBinding) error {
	return nil
}

// CheckTopologyDrift compares the referenced CNT against the ClusterTopologyBinding levels
// (externally-managed path). Layer names are administrator-chosen, so the comparison is
// structural (root-to-leaf label keys in order); on success the actual layer names are recorded.
func (b *schedulerBackend) CheckTopologyDrift(ctx context.Context, k8sClient client.Client, ct *grovecorev1alpha1.ClusterTopologyBinding, ref grovecorev1alpha1.SchedulerTopologyBinding) (bool, string, int64, error) {
	if k8sClient == nil {
		k8sClient = b.client
	}
	// koord-scheduler only reads the CNT named "default"; a matching CNT under another name would
	// pass the comparison while never being consumed.
	if ref.TopologyReference != cntDefaultName {
		return false, fmt.Sprintf("koord-scheduler only consumes the ClusterNetworkTopology named %q; topologyReference %q is never read by the scheduler",
			cntDefaultName, ref.TopologyReference), 0, nil
	}
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(cntGVK)
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: ref.TopologyReference}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Sprintf("ClusterNetworkTopology %q not found", ref.TopologyReference), 0, nil
		}
		return false, "", 0, fmt.Errorf("failed to get ClusterNetworkTopology %q: %w", ref.TopologyReference, err)
	}

	nonLeafKeys, err := nonLeafLevelKeys(ct.Spec.Levels)
	if err != nil {
		// An unrepresentable CTB is a drift condition to report in status, not a reconcile error.
		//nolint:nilerr // intentional: the message conveys the mismatch, retrying cannot fix it.
		return false, err.Error(), existing.GetGeneration(), nil
	}

	entries, _, err := unstructured.NestedSlice(existing.Object, "spec", "networkTopologySpec")
	if err != nil {
		return false, "", existing.GetGeneration(), fmt.Errorf("failed to read CNT spec: %w", err)
	}
	chain, msg := orderedNonLeafCNTLayers(entries)
	if msg != "" {
		return false, msg, existing.GetGeneration(), nil
	}
	if len(chain) != len(nonLeafKeys) {
		return false, fmt.Sprintf("ClusterNetworkTopology has %d non-leaf layers, ClusterTopologyBinding defines %d",
			len(chain), len(nonLeafKeys)), existing.GetGeneration(), nil
	}
	layerByKey := make(map[string]string, len(ct.Spec.Levels))
	for i, layer := range chain {
		if !slices.Contains(layer.labelKeys, nonLeafKeys[i]) {
			return false, fmt.Sprintf("CNT layer %q does not match node label key %q at position %d",
				layer.name, nonLeafKeys[i], i), existing.GetGeneration(), nil
		}
		layerByKey[nonLeafKeys[i]] = layer.name
	}
	b.rememberTopologyLayers(ct.Spec.Levels, layerByKey)
	return true, "", existing.GetGeneration(), nil
}

// buildCNTNetworkTopologySpec converts CTB levels (widest to narrowest) into CNT networkTopologySpec
// entries: non-leaf layer names equal the level's label key; a host level collapses into the
// built-in NodeTopologyLayer leaf; numa levels cannot be represented.
func buildCNTNetworkTopologySpec(levels []grovecorev1alpha1.TopologyLevel) ([]interface{}, error) {
	if len(levels) == 0 {
		return nil, fmt.Errorf("ClusterTopologyBinding has no levels")
	}
	nonLeafKeys, err := nonLeafLevelKeys(levels)
	if err != nil {
		return nil, err
	}

	entries := make([]interface{}, 0, len(nonLeafKeys)+1)
	parent := ""
	for _, key := range nonLeafKeys {
		entry := map[string]interface{}{
			"topologyLayer": key,
			"labelKey":      []interface{}{key},
		}
		if parent != "" {
			entry["parentTopologyLayer"] = parent
		}
		entries = append(entries, entry)
		parent = key
	}
	leaf := map[string]interface{}{
		"topologyLayer": cntLayerNode,
	}
	if parent != "" {
		leaf["parentTopologyLayer"] = parent
	}
	entries = append(entries, leaf)
	return entries, nil
}

// nonLeafLevelKeys returns the label keys of all levels above the host level, validating
// that the hierarchy is representable in a CNT: a host level may only appear as the
// narrowest level (it collapses into NodeTopologyLayer) and numa levels are unsupported.
func nonLeafLevelKeys(levels []grovecorev1alpha1.TopologyLevel) ([]string, error) {
	for _, level := range levels {
		if level.Domain == grovecorev1alpha1.TopologyDomainNuma {
			return nil, fmt.Errorf("level %q (domain numa) cannot be represented: the ClusterNetworkTopology leaf is the node", level.Key)
		}
	}
	keys := make([]string, 0, len(levels))
	for i, level := range levels {
		if level.Domain == grovecorev1alpha1.TopologyDomainHost {
			if i != len(levels)-1 {
				return nil, fmt.Errorf("host level %q must be the narrowest level", level.Key)
			}
			continue
		}
		keys = append(keys, level.Key)
	}
	return keys, nil
}

// cntLayer is a parsed non-leaf CNT spec entry.
type cntLayer struct {
	name      string
	labelKeys []string
}

// orderedNonLeafCNTLayers orders the CNT spec entries from root to leaf by following
// parentTopologyLayer pointers upwards from NodeTopologyLayer, returning the non-leaf
// layers. A malformed spec (no NodeTopologyLayer entry, broken or cyclic parent chain,
// or entries outside the chain) yields a human-readable message instead.
func orderedNonLeafCNTLayers(entries []interface{}) ([]cntLayer, string) {
	byName := make(map[string]map[string]interface{}, len(entries))
	for _, e := range entries {
		entry, ok := e.(map[string]interface{})
		if !ok {
			return nil, "malformed networkTopologySpec entry"
		}
		name, _, _ := unstructured.NestedString(entry, "topologyLayer")
		if name == "" {
			return nil, "networkTopologySpec entry without topologyLayer"
		}
		if _, duplicate := byName[name]; duplicate {
			// koordinator's tree builder rejects duplicate layer names outright.
			return nil, fmt.Sprintf("networkTopologySpec defines layer %q more than once", name)
		}
		byName[name] = entry
	}
	leaf, ok := byName[cntLayerNode]
	if !ok {
		return nil, fmt.Sprintf("networkTopologySpec has no %s leaf entry", cntLayerNode)
	}

	var reversed []cntLayer
	current := leaf
	visited := map[string]struct{}{cntLayerNode: {}}
	for {
		parentName, _, _ := unstructured.NestedString(current, "parentTopologyLayer")
		if parentName == "" || parentName == "ClusterTopologyLayer" {
			break
		}
		if _, seen := visited[parentName]; seen {
			return nil, "networkTopologySpec parent chain contains a cycle"
		}
		parent, okParent := byName[parentName]
		if !okParent {
			return nil, fmt.Sprintf("networkTopologySpec references undefined parent layer %q", parentName)
		}
		visited[parentName] = struct{}{}
		labelKeys, _, _ := unstructured.NestedStringSlice(parent, "labelKey")
		reversed = append(reversed, cntLayer{name: parentName, labelKeys: labelKeys})
		current = parent
	}
	if len(visited) != len(byName) {
		return nil, "networkTopologySpec contains layers outside the leaf's parent chain"
	}

	chain := make([]cntLayer, 0, len(reversed))
	for i := len(reversed) - 1; i >= 0; i-- {
		chain = append(chain, reversed[i])
	}
	return chain, ""
}

// rememberTopologyLayers records the node-label-key → CNT-layer mapping derived from a successful
// sync or drift check, so gang topology annotations translate without manual TopologyKeyMappings.
// layerByKey overrides the sync-path naming (layer == label key) on the externally-managed path.
func (b *schedulerBackend) rememberTopologyLayers(levels []grovecorev1alpha1.TopologyLevel, layerByKey map[string]string) {
	derived := make(map[string]string, len(levels))
	for _, level := range levels {
		switch level.Domain {
		case grovecorev1alpha1.TopologyDomainNuma:
			continue
		case grovecorev1alpha1.TopologyDomainHost:
			derived[level.Key] = cntLayerNode
		default:
			if name, ok := layerByKey[level.Key]; ok {
				derived[level.Key] = name
			} else {
				derived[level.Key] = level.Key
			}
		}
	}
	b.topologyMu.Lock()
	defer b.topologyMu.Unlock()
	b.syncedTopologyLayerByKey = derived
}

// mergedTopologyKeyMappings combines the CTB-derived layer mapping with the user-configured
// TopologyKeyMappings; explicit user entries win.
func (b *schedulerBackend) mergedTopologyKeyMappings() map[string]string {
	b.topologyMu.RLock()
	defer b.topologyMu.RUnlock()
	if len(b.syncedTopologyLayerByKey) == 0 {
		return b.cfg.topologyKeyMappings
	}
	merged := make(map[string]string, len(b.syncedTopologyLayerByKey)+len(b.cfg.topologyKeyMappings))
	for k, v := range b.syncedTopologyLayerByKey {
		merged[k] = v
	}
	for k, v := range b.cfg.topologyKeyMappings {
		merged[k] = v
	}
	return merged
}
