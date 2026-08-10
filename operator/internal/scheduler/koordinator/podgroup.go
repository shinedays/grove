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
	"strconv"

	"github.com/ai-dynamo/grove/operator/api/common"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// podGroupName returns the Koordinator PodGroup name "{podgangName}-{podgroupName}".
func podGroupName(podGangName, podGroupName string) string {
	return podGangName + "-" + podGroupName
}

// syncPodGang creates or updates one Koordinator PodGroup CR per PodGang.Spec.PodGroups entry,
// linked into a single GangGroup via annotations, and prunes stale ones.
func syncPodGang(
	ctx context.Context,
	cl client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	cfg backendConfig,
	podGang *groveschedulerv1alpha1.PodGang,
) error {
	logger := log.FromContext(ctx)

	// cfg is a by-value copy, so the per-workload timeout override stays local to this sync.
	timeout, err := effectiveScheduleTimeout(podGang.Annotations, cfg.ScheduleTimeoutSeconds)
	if err != nil {
		return fmt.Errorf("PodGang %q/%q has an invalid %s annotation: %w",
			podGang.Namespace, podGang.Name, AnnotationScheduleTimeoutSeconds, err)
	}
	cfg.ScheduleTimeoutSeconds = timeout

	if podGang.Spec.ReuseReservationRef != nil {
		recorder.Eventf(podGang, corev1.EventTypeWarning, "UnsupportedFeature",
			"koord-scheduler backend does not support ReuseReservationRef; "+
				"Koordinator Reservation integration is not implemented in this version")
		logger.Info("ReuseReservationRef is set but not supported by koord-scheduler backend; skipping",
			"podGang", podGang.Name, "namespace", podGang.Namespace,
			"reuseReservationRef", podGang.Spec.ReuseReservationRef)
	}

	// Per-subset (PCSG) topology constraints cannot be represented with independent PodGroup CRs;
	// fail rather than silently drop a mandatory placement requirement.
	// TODO: consume TopologyConstraintGroupConfigs once the koord backend supports per-subset topology.
	if len(podGang.Spec.TopologyConstraintGroupConfigs) > 0 {
		return fmt.Errorf("PodGang %q/%q uses TopologyConstraintGroupConfigs which is not yet supported by the koord-scheduler backend; "+
			"use a PodCliqueSet-level topologyConstraint instead or remove the PCSG topology constraint group configuration",
			podGang.Namespace, podGang.Name)
	}

	gangGroupNames := lo.Map(podGang.Spec.PodGroups, func(pg groveschedulerv1alpha1.PodGroup, _ int) string {
		return fmt.Sprintf("%s/%s", podGang.Namespace, podGroupName(podGang.Name, pg.Name))
	})
	gangGroupJSON, err := json.Marshal(gangGroupNames)
	if err != nil {
		return fmt.Errorf("failed to marshal gang group names: %w", err)
	}

	desiredNames := make(map[string]struct{}, len(podGang.Spec.PodGroups))
	for _, pg := range podGang.Spec.PodGroups {
		desiredNames[podGroupName(podGang.Name, pg.Name)] = struct{}{}
	}

	// Gang-level constraint; an unmappable Required key is a hard error (see buildTopologyAnnotation).
	globalTopoAnnotation, err := buildTopologyAnnotation(ctx, podGang.Spec.TopologyConstraint, cfg.topologyKeyMappings)
	if err != nil {
		return fmt.Errorf("PodGang %q has an unsatisfiable topology constraint: %w", podGang.Name, err)
	}

	// The effective annotation must be identical across the gang: koord-scheduler applies the
	// NetworkTopologySpec of whichever member pod enters PreFilter first to the whole GangGroup.
	topoAnnotations := make(map[string]string, len(podGang.Spec.PodGroups))
	effectiveTopo := ""
	for i, pg := range podGang.Spec.PodGroups {
		perPG, err := buildTopologyAnnotation(ctx, pg.TopologyConstraint, cfg.topologyKeyMappings)
		if err != nil {
			return fmt.Errorf("PodGroup %q has an unsatisfiable topology constraint: %w", pg.Name, err)
		}
		if perPG == "" {
			perPG = globalTopoAnnotation
		}
		if i == 0 {
			effectiveTopo = perPG
		} else if perPG != effectiveTopo {
			return fmt.Errorf("PodGang %q/%q resolves to divergent topology constraints across its PodGroups; "+
				"koord-scheduler applies a single network-topology-spec to the whole GangGroup, so all cliques "+
				"must share the same effective constraint — set the topologyConstraint at the PodCliqueSet level instead",
				podGang.Namespace, podGang.Name)
		}
		topoAnnotations[pg.Name] = perPG
	}

	// Create/update the desired set before pruning so a mid-loop failure never deletes a
	// still-valid PodGroup.
	for _, pg := range podGang.Spec.PodGroups {
		desired, err := buildPodGroupObject(podGang, pg, cfg, gangGroupJSON, topoAnnotations[pg.Name], scheme)
		if err != nil {
			return fmt.Errorf("failed to build PodGroup for %q: %w", pg.Name, err)
		}
		if err := createOrUpdatePodGroup(ctx, cl, desired); err != nil {
			return fmt.Errorf("failed to sync PodGroup %q for PodGang %q: %w", pg.Name, podGang.Name, err)
		}
		logger.Info("Synced Koordinator PodGroup", "podGroup", desired.GetName(), "namespace", desired.GetNamespace())
	}

	if err := pruneOrphanedPodGroups(ctx, cl, podGang, desiredNames); err != nil {
		return fmt.Errorf("failed to prune stale PodGroups for PodGang %q: %w", podGang.Name, err)
	}
	return nil
}

// buildPodGroupObject constructs the Koordinator PodGroup unstructured object for the given grove PodGroup.
// topoAnnotation is the gang-wide network-topology-spec value; empty injects nothing.
func buildPodGroupObject(
	podGang *groveschedulerv1alpha1.PodGang,
	pg groveschedulerv1alpha1.PodGroup,
	cfg backendConfig,
	gangGroupJSON []byte,
	topoAnnotation string,
	scheme *runtime.Scheme,
) (*unstructured.Unstructured, error) {
	pgName := podGroupName(podGang.Name, pg.Name)

	annotations := map[string]interface{}{
		AnnotationGangGroups:      string(gangGroupJSON),
		AnnotationGangMode:        cfg.GangMode,
		AnnotationGangMatchPolicy: cfg.MatchPolicy,
	}
	// total-number is informational for koord-scheduler (gang decisions read minMember; a
	// missing value falls back to it), so omit it while PodReferences is still being filled.
	if totalChildren := len(pg.PodReferences); totalChildren >= int(pg.MinReplicas) && totalChildren > 0 {
		annotations[AnnotationGangTotalNum] = strconv.Itoa(totalChildren)
	}

	if topoAnnotation != "" {
		annotations[AnnotationNetworkTopologySpec] = topoAnnotation
	}

	// Clone the PodGang labels (as the KAI backend does): PodGang names embed a runtime-minted
	// epoch, so the cloned role/replica-index/epoch labels are the only stable lookup handle.
	labels := make(map[string]interface{}, len(podGang.Labels)+1)
	for k, v := range podGang.Labels {
		labels[k] = v
	}
	// Stable label used by pruneOrphanedPodGroups to narrow the List scope.
	labels[common.LabelPodGang] = podGang.Name

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": podGroupGVK.Group + "/" + podGroupGVK.Version,
			"kind":       podGroupGVK.Kind,
			"metadata": map[string]interface{}{
				"name":        pgName,
				"namespace":   podGang.Namespace,
				"labels":      labels,
				"annotations": annotations,
			},
			// Numeric values in unstructured content must be int64: apimachinery's
			// DeepCopyJSONValue panics on int32.
			"spec": map[string]interface{}{
				"minMember":              int64(pg.MinReplicas),
				"scheduleTimeoutSeconds": int64(cfg.ScheduleTimeoutSeconds),
			},
		},
	}

	// PriorityClassName is intentionally not written: the sig-scheduling PodGroup CRD has no such
	// field (the structural schema would prune it); priority applies via each Pod's own spec.

	if err := controllerutil.SetControllerReference(podGang, obj, scheme); err != nil {
		return nil, fmt.Errorf("failed to set OwnerReference on PodGroup %q: %w", pgName, err)
	}

	return obj, nil
}

// createOrUpdatePodGroup creates the PodGroup or updates the existing one. It refuses to overwrite
// a PodGroup controlled by a different owner UID, and preserves external metadata, the status
// written by Koordinator's PodGroupController, and released gang constraints.
func createOrUpdatePodGroup(ctx context.Context, cl client.Client, desired *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(podGroupGVK)

	err := cl.Get(ctx, client.ObjectKey{Name: desired.GetName(), Namespace: desired.GetNamespace()}, existing)
	if apierrors.IsNotFound(err) {
		return cl.Create(ctx, desired)
	}
	if apimeta.IsNoMatchError(err) {
		return fmt.Errorf("the PodGroup CRD (%s) is not installed; "+
			"install Koordinator (or disable the koord-scheduler profile) to schedule gangs with koord-scheduler: %w",
			podGroupGVK.GroupVersion().String(), err)
	}
	if err != nil {
		return fmt.Errorf("failed to get PodGroup %q: %w", desired.GetName(), err)
	}

	// Owner-UID gate: never adopt a PodGroup controlled by another PodGang (e.g. after UID rotation).
	desiredUID := controllerUID(desired)
	existingUID := controllerUID(existing)
	if desiredUID != "" && existingUID != desiredUID {
		return fmt.Errorf("PodGroup %q/%q already exists and is controlled by a different owner (existing UID: %q, expected: %q); "+
			"refusing to overwrite to avoid corrupting another workload",
			desired.GetNamespace(), desired.GetName(), existingUID, desiredUID)
	}

	if gangConstraintsReleased(existing, desired) {
		carryOverGangConstraints(existing, desired)
	}
	preserveExternalMetadata(existing, desired)
	// The PodGroup CRD has no status subresource, so a whole-object Update would wipe the status
	// written by Koordinator's PodGroupController; carry it over.
	if status, found, err := unstructured.NestedMap(existing.Object, "status"); err == nil && found {
		_ = unstructured.SetNestedMap(desired.Object, status, "status")
	}
	desired.SetResourceVersion(existing.GetResourceVersion())
	return cl.Update(ctx, desired)
}

// gangConstraintsReleased reports whether a Grove coherent update (GREP-393) released this
// PodGroup's gang constraint: desired minMember is 0 (admission rejects MinAvailable 0, so a
// zero can only be a release) while the existing PodGroup still records a positive value.
// Decided per PodGroup, not per gang: a release may cover only the standalone PodCliques,
// and an "all entries zero" test would write minMember 0 for the released ones.
func gangConstraintsReleased(existing, desired *unstructured.Unstructured) bool {
	desiredMin, found, err := unstructured.NestedInt64(desired.Object, "spec", "minMember")
	if err != nil || !found || desiredMin != 0 {
		return false
	}
	existingMin, found, err := unstructured.NestedInt64(existing.Object, "spec", "minMember")
	return err == nil && found && existingMin > 0
}

// carryOverGangConstraints copies spec.minMember and the total-number annotation from the
// existing PodGroup into desired so a constraint release does not rewrite them to zero.
func carryOverGangConstraints(existing, desired *unstructured.Unstructured) {
	if minMember, found, err := unstructured.NestedInt64(existing.Object, "spec", "minMember"); err == nil && found {
		_ = unstructured.SetNestedField(desired.Object, minMember, "spec", "minMember")
	}
	annotations := desired.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if existingTotal, ok := existing.GetAnnotations()[AnnotationGangTotalNum]; ok {
		annotations[AnnotationGangTotalNum] = existingTotal
	} else {
		delete(annotations, AnnotationGangTotalNum)
	}
	desired.SetAnnotations(annotations)
}

func preserveExternalMetadata(existing, desired *unstructured.Unstructured) {
	labels := existing.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	for k, v := range desired.GetLabels() {
		labels[k] = v
	}
	desired.SetLabels(labels)

	annotations := existing.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	for _, key := range []string{
		AnnotationGangGroups,
		AnnotationGangMode,
		AnnotationGangMatchPolicy,
		AnnotationGangTotalNum,
		AnnotationNetworkTopologySpec,
	} {
		delete(annotations, key)
	}
	for k, v := range desired.GetAnnotations() {
		annotations[k] = v
	}
	desired.SetAnnotations(annotations)

	desired.SetFinalizers(existing.GetFinalizers())
}

// controllerUID returns the UID of the controller OwnerReference on obj, or "" if none is set.
func controllerUID(obj *unstructured.Unstructured) string {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return string(ref.UID)
		}
	}
	return ""
}

// pruneOrphanedPodGroups deletes PodGroups owned by podGang (OwnerReference UID match) whose
// names are absent from desiredNames. Two list passes: label-filtered (normal path) plus a full
// namespace list to catch historical PodGroups that pre-date the grove.io/podgang label. Both
// passes apply the owner-UID gate; PodGroups owned by another PodGang are never touched.
func pruneOrphanedPodGroups(
	ctx context.Context,
	cl client.Client,
	podGang *groveschedulerv1alpha1.PodGang,
	desiredNames map[string]struct{},
) error {
	logger := log.FromContext(ctx)

	// Pass 1: label-filtered.
	labeled := &unstructured.UnstructuredList{}
	labeled.SetGroupVersionKind(podGroupGVK)
	if err := cl.List(ctx, labeled,
		client.InNamespace(podGang.Namespace),
		client.MatchingLabels{common.LabelPodGang: podGang.Name},
	); err != nil {
		return fmt.Errorf("failed to list PodGroups for prune: %w", err)
	}

	// Pass 2: full namespace list for unlabeled historical objects. Costs one extra List per
	// reconcile; if that matters at scale, index on owner UID rather than adding a migration flag.
	allInNS := &unstructured.UnstructuredList{}
	allInNS.SetGroupVersionKind(podGroupGVK)
	if err := cl.List(ctx, allInNS, client.InNamespace(podGang.Namespace)); err != nil {
		return fmt.Errorf("failed to list all PodGroups for legacy prune: %w", err)
	}

	seen := make(map[string]struct{}, len(labeled.Items))
	candidates := make([]*unstructured.Unstructured, 0, len(labeled.Items))
	for i := range labeled.Items {
		pg := &labeled.Items[i]
		seen[pg.GetName()] = struct{}{}
		candidates = append(candidates, pg)
	}
	for i := range allInNS.Items {
		pg := &allInNS.Items[i]
		if _, already := seen[pg.GetName()]; already {
			continue
		}
		// Labeled items belong to another PodGang or were already covered by pass 1.
		if _, hasLabel := pg.GetLabels()[common.LabelPodGang]; hasLabel {
			continue
		}
		candidates = append(candidates, pg)
	}

	for _, pg := range candidates {
		owned := false
		for _, ref := range pg.GetOwnerReferences() {
			if ref.UID == podGang.UID {
				owned = true
				break
			}
		}
		if !owned {
			continue
		}
		if _, ok := desiredNames[pg.GetName()]; ok {
			continue
		}
		logger.Info("Pruning stale Koordinator PodGroup", "podGroup", pg.GetName(), "namespace", pg.GetNamespace())
		if err := cl.Delete(ctx, pg); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete stale PodGroup %q: %w", pg.GetName(), err)
		}
	}
	return nil
}

// buildTopologyAnnotation converts a Grove TopologyConstraint into the Koordinator
// network-topology-spec annotation value. An unmappable Required key is a hard error (a mandatory
// constraint must not be silently dropped); an unmappable Preferred key is logged and skipped.
func buildTopologyAnnotation(ctx context.Context, tc *groveschedulerv1alpha1.TopologyConstraint, userMappings map[string]string) (string, error) {
	if tc == nil || tc.PackConstraint == nil {
		return "", nil
	}
	logger := log.FromContext(ctx)

	type gatherStrategyEntry struct {
		Layer    string `json:"layer"`
		Strategy string `json:"strategy"`
	}

	var gatherStrategies []gatherStrategyEntry

	if tc.PackConstraint.Required != nil {
		layer := topologyKeyToKoordinatorLayer(*tc.PackConstraint.Required, userMappings)
		if layer == "" {
			return "", fmt.Errorf("topology key %q has no Koordinator layer equivalent and cannot be used as a Required constraint; "+
				"the only built-in mapping is kubernetes.io/hostname → NodeTopologyLayer; "+
				"map other keys to layers of your ClusterNetworkTopology CR via KoordinatorSchedulerConfiguration.TopologyKeyMappings",
				*tc.PackConstraint.Required)
		}
		gatherStrategies = append(gatherStrategies, gatherStrategyEntry{
			Layer:    layer,
			Strategy: topologyStrategyMustGather,
		})
	}
	if tc.PackConstraint.Preferred != nil {
		layer := topologyKeyToKoordinatorLayer(*tc.PackConstraint.Preferred, userMappings)
		if layer == "" {
			logger.Info("Topology key has no Koordinator equivalent; Preferred constraint skipped",
				"topologyKey", *tc.PackConstraint.Preferred,
				"hint", "Built-in mapping: kubernetes.io/hostname → NodeTopologyLayer; map other keys to layers of your ClusterNetworkTopology CR via KoordinatorSchedulerConfiguration.TopologyKeyMappings")
		} else {
			gatherStrategies = append(gatherStrategies, gatherStrategyEntry{
				Layer:    layer,
				Strategy: topologyStrategyPreferGather,
			})
		}
	}

	if len(gatherStrategies) == 0 {
		return "", nil
	}

	spec := map[string]interface{}{
		"gatherStrategy": gatherStrategies,
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("failed to marshal network-topology-spec annotation: %w", err)
	}
	return string(raw), nil
}

// topologyKeyToKoordinatorLayer maps a Grove topology key (node label key) to a Koordinator
// network-topology-spec layer name: userMappings first, then the built-in
// "kubernetes.io/hostname" → "NodeTopologyLayer". Returns "" for unknown keys. Matching is exact:
// substring heuristics (e.g. Contains("host")) would mis-map keys such as "nfs-hostpath".
func topologyKeyToKoordinatorLayer(topologyKey string, userMappings map[string]string) string {
	if layer, ok := userMappings[topologyKey]; ok {
		return layer
	}
	if topologyKey == "kubernetes.io/hostname" {
		return "NodeTopologyLayer"
	}
	return ""
}
