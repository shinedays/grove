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
	"fmt"
	"strconv"

	"github.com/ai-dynamo/grove/operator/api/common"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// podGroupName returns the Koordinator PodGroup name for a given PodGang and PodGroup.
// The format is "{podgangName}-{podgroupName}".
func podGroupName(podGangName, podGroupName string) string {
	return podGangName + "-" + podGroupName
}

// syncPodGang converts a PodGang into Koordinator PodGroup CRs and keeps them in sync.
// For each PodGroup in the PodGang, one Koordinator PodGroup CR is created or updated.
// All PodGroups are linked as a GangGroup via annotations so that Koordinator treats them
// as a single atomic scheduling unit.
func syncPodGang(
	ctx context.Context,
	cl client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	cfg backendConfig,
	podGang *groveschedulerv1alpha1.PodGang,
) error {
	logger := log.FromContext(ctx)

	if podGang.Spec.ReuseReservationRef != nil {
		// Koordinator Reservation integration is deferred. Emit a warning so operators
		// know that reuse-reservation semantics will not be honoured by this backend.
		recorder.Eventf(podGang, corev1.EventTypeWarning, "UnsupportedFeature",
			"koord-scheduler backend does not support ReuseReservationRef; "+
				"Koordinator Reservation integration is not implemented in this version")
		logger.Info("ReuseReservationRef is set but not supported by koord-scheduler backend; skipping",
			"podGang", podGang.Name, "namespace", podGang.Namespace,
			"reuseReservationRef", podGang.Spec.ReuseReservationRef)
	}

	// TopologyConstraintGroupConfigs defines per-subset topology constraints (populated by
	// the PCSG controller path). The koord backend does not yet consume this field — the
	// N PodGroup CRs it creates cannot represent independent subset constraints within a
	// GangGroup. Rather than silently dropping these mandatory placement requirements, we
	// return an error so the reconciler retries and surfaces the issue.
	// TODO: consume TopologyConstraintGroupConfigs once the koord backend supports per-subset topology.
	if len(podGang.Spec.TopologyConstraintGroupConfigs) > 0 {
		return fmt.Errorf("PodGang %q/%q uses TopologyConstraintGroupConfigs which is not yet supported by the koord-scheduler backend; "+
			"use per-PodGroup TopologyConstraints instead or remove the PCSG topology constraint group configuration",
			podGang.Namespace, podGang.Name)
	}

	// Build the list of all Koordinator PodGroup names that form the GangGroup.
	// Format: ["namespace/podgroupname", ...]
	gangGroupNames := lo.Map(podGang.Spec.PodGroups, func(pg groveschedulerv1alpha1.PodGroup, _ int) string {
		return fmt.Sprintf("%s/%s", podGang.Namespace, podGroupName(podGang.Name, pg.Name))
	})
	gangGroupJSON, err := json.Marshal(gangGroupNames)
	if err != nil {
		return fmt.Errorf("failed to marshal gang group names: %w", err)
	}

	// Build desired PodGroup name set.
	desiredNames := make(map[string]struct{}, len(podGang.Spec.PodGroups))
	for _, pg := range podGang.Spec.PodGroups {
		desiredNames[podGroupName(podGang.Name, pg.Name)] = struct{}{}
	}

	// Build the global network-topology-spec annotation from the PodGang-level topology constraint.
	// A Required topology key with no Koordinator equivalent is a hard error: we must not
	// silently drop a constraint the user declared as mandatory.
	globalTopoAnnotation, err := buildTopologyAnnotation(ctx, podGang.Spec.TopologyConstraint, cfg.topologyKeyMappings)
	if err != nil {
		return fmt.Errorf("PodGang %q has an unsatisfiable topology constraint: %w", podGang.Name, err)
	}

	// Create or update the desired set of Koordinator PodGroup CRs first, then prune
	// stale ones. This ordering ensures that if a create/update fails mid-loop, no
	// currently-valid PodGroup has been deleted yet — the gang stays in a consistent
	// (if possibly incomplete) state until the next reconcile retry.
	for _, pg := range podGang.Spec.PodGroups {
		desired, err := buildPodGroupObject(ctx, podGang, pg, cfg, gangGroupJSON, globalTopoAnnotation, scheme)
		if err != nil {
			return fmt.Errorf("failed to build PodGroup for %q: %w", pg.Name, err)
		}
		if err := createOrUpdatePodGroup(ctx, cl, desired); err != nil {
			return fmt.Errorf("failed to sync PodGroup %q for PodGang %q: %w", pg.Name, podGang.Name, err)
		}
		logger.Info("Synced Koordinator PodGroup", "podGroup", desired.GetName(), "namespace", desired.GetNamespace())
	}

	// Prune only after the desired set is durable.
	if err := pruneOrphanedPodGroups(ctx, cl, podGang, desiredNames); err != nil {
		return fmt.Errorf("failed to prune stale PodGroups for PodGang %q: %w", podGang.Name, err)
	}
	return nil
}

// buildPodGroupObject constructs the Koordinator PodGroup unstructured object for the given grove PodGroup.
func buildPodGroupObject(
	ctx context.Context,
	podGang *groveschedulerv1alpha1.PodGang,
	pg groveschedulerv1alpha1.PodGroup,
	cfg backendConfig,
	gangGroupJSON []byte,
	globalTopoAnnotation string,
	scheme *runtime.Scheme,
) (*unstructured.Unstructured, error) {
	pgName := podGroupName(podGang.Name, pg.Name)

	annotations := map[string]interface{}{
		AnnotationGangGroups:      string(gangGroupJSON),
		AnnotationGangMode:        cfg.GangMode,
		AnnotationGangMatchPolicy: cfg.MatchPolicy,
	}
	if totalChildren := len(pg.PodReferences); totalChildren >= int(pg.MinReplicas) && totalChildren > 0 {
		annotations[AnnotationGangTotalNum] = strconv.Itoa(totalChildren)
	}

	// Per-PodGroup topology takes precedence over the global topology.
	// If neither is set, no topology annotation is injected.
	topoAnnotation, err := buildTopologyAnnotation(ctx, pg.TopologyConstraint, cfg.topologyKeyMappings)
	if err != nil {
		return nil, fmt.Errorf("PodGroup %q has an unsatisfiable topology constraint: %w", pgName, err)
	}
	if topoAnnotation == "" {
		topoAnnotation = globalTopoAnnotation
	}
	if topoAnnotation != "" {
		annotations[AnnotationNetworkTopologySpec] = topoAnnotation
	}

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": podGroupGVK.Group + "/" + podGroupGVK.Version,
			"kind":       podGroupGVK.Kind,
			"metadata": map[string]interface{}{
				"name":      pgName,
				"namespace": podGang.Namespace,
				"labels": map[string]interface{}{
					// Stable label used by pruneOrphanedPodGroups to narrow the List scope.
					common.LabelPodGang: podGang.Name,
				},
				"annotations": annotations,
			},
			"spec": map[string]interface{}{
				"minMember":              int64(pg.MinReplicas),
				"scheduleTimeoutSeconds": int64(cfg.ScheduleTimeoutSeconds),
			},
		},
	}

	// Set PriorityClassName if provided.
	if podGang.Spec.PriorityClassName != "" {
		spec := obj.Object["spec"].(map[string]interface{})
		spec["priorityClassName"] = podGang.Spec.PriorityClassName
	}

	// Set OwnerReference so the PodGroup is garbage-collected when the PodGang is deleted.
	if err := controllerutil.SetControllerReference(podGang, obj, scheme); err != nil {
		return nil, fmt.Errorf("failed to set OwnerReference on PodGroup %q: %w", pgName, err)
	}

	return obj, nil
}

// createOrUpdatePodGroup creates the PodGroup if it does not exist, or updates it if it does.
// Before updating an existing object, it verifies that the object is owned by the same
// controller as the desired object (by comparing controller OwnerReference UIDs). If the
// existing object is owned by a different controller, an error is returned rather than
// silently overwriting a PodGroup that belongs to another PodGang or third-party workload.
func createOrUpdatePodGroup(ctx context.Context, cl client.Client, desired *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(podGroupGVK)

	err := cl.Get(ctx, client.ObjectKey{Name: desired.GetName(), Namespace: desired.GetNamespace()}, existing)
	if apierrors.IsNotFound(err) {
		return cl.Create(ctx, desired)
	}
	if err != nil {
		return fmt.Errorf("failed to get PodGroup %q: %w", desired.GetName(), err)
	}

	// Ownership check: refuse to overwrite a PodGroup that is controlled by a different owner.
	desiredUID := controllerUID(desired)
	existingUID := controllerUID(existing)
	if desiredUID != "" && existingUID != desiredUID {
		return fmt.Errorf("PodGroup %q/%q already exists and is controlled by a different owner (existing UID: %q, expected: %q); "+
			"refusing to overwrite to avoid corrupting another workload",
			desired.GetNamespace(), desired.GetName(), existingUID, desiredUID)
	}

	preserveExternalMetadata(existing, desired)
	// Preserve the ResourceVersion for the update.
	desired.SetResourceVersion(existing.GetResourceVersion())
	return cl.Update(ctx, desired)
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

// pruneOrphanedPodGroups deletes Koordinator PodGroup CRs that are owned by podGang
// (identified by OwnerReference UID) but whose names are absent from desiredNames.
// This handles the case where PodGang.Spec.PodGroups shrinks between reconcile cycles.
// PodGroups owned by a different PodGang (different UID) are never touched.
//
// Two list passes are performed:
//  1. Label-filtered list (O(k)): finds PodGroups with grove.io/podgang=<name> — the normal
//     operating path once all objects carry the label.
//  2. Full namespace list (fallback): catches historical PodGroups that pre-date the
//     grove.io/podgang label. Once the create/update path relabels all surviving PodGroups,
//     the second pass will return only non-matching items and become a no-op.
//
// Both passes apply the owner-UID safety gate; a PodGroup is only deleted when its
// OwnerReference UID matches podGang.UID.
func pruneOrphanedPodGroups(
	ctx context.Context,
	cl client.Client,
	podGang *groveschedulerv1alpha1.PodGang,
	desiredNames map[string]struct{},
) error {
	logger := log.FromContext(ctx)

	// Pass 1: label-filtered — O(k) in normal operation.
	labeled := &unstructured.UnstructuredList{}
	labeled.SetGroupVersionKind(podGroupGVK)
	if err := cl.List(ctx, labeled,
		client.InNamespace(podGang.Namespace),
		client.MatchingLabels{common.LabelPodGang: podGang.Name},
	); err != nil {
		return fmt.Errorf("failed to list PodGroups for prune: %w", err)
	}

	// Pass 2: full namespace list — catches unlabeled historical objects (created before the
	// grove.io/podgang label was introduced). This incurs one additional API List call per
	// reconcile. Once the create/update path has relabeled all surviving PodGroups, the
	// inner loop below finds no unlabeled owned objects and performs no deletions; the List
	// call itself still occurs every reconcile (it is not skipped). If this becomes a
	// bottleneck at scale, the correct optimization is to build a controller-runtime field
	// index on the owner UID rather than adding a stateful migration flag here.
	allInNS := &unstructured.UnstructuredList{}
	allInNS.SetGroupVersionKind(podGroupGVK)
	if err := cl.List(ctx, allInNS, client.InNamespace(podGang.Namespace)); err != nil {
		return fmt.Errorf("failed to list all PodGroups for legacy prune: %w", err)
	}

	// Build a deduplicated candidate set: labeled items first, then unlabeled items
	// from the full namespace scan (items that have the label are already in pass 1).
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
		// Skip items that carry the label (any value) — they are either from a
		// different PodGang (correct to skip) or already covered by pass 1.
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
// network-topology-spec JSON annotation value.
//
// Required topology key with no Koordinator equivalent is a hard error: silently dropping
// a mandatory placement constraint would violate the user's scheduling intent and could
// schedule the workload in a topology the user explicitly prohibited.
//
// Preferred topology key with no Koordinator equivalent is logged at Info level and skipped
// (best-effort semantics — the constraint is advisory, not binding).
//
// If tc is nil or has no PackConstraint, ("", nil) is returned.
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
			// Required is a hard constraint. An unmappable key must not be silently dropped
			// into unconstrained scheduling — return an error so the caller can surface it.
			return "", fmt.Errorf("topology key %q has no Koordinator layer equivalent and cannot be used as a Required constraint; "+
				"supported built-in keys: kubernetes.io/hostname, topology.kubernetes.io/rack, topology.kubernetes.io/block; "+
				"or add a custom mapping via KoordinatorSchedulerConfiguration.TopologyKeyMappings",
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
			// Preferred is advisory — log and skip rather than hard-fail.
			logger.Info("Topology key has no Koordinator equivalent; Preferred constraint skipped",
				"topologyKey", *tc.PackConstraint.Preferred,
				"hint", "Supported keys: kubernetes.io/hostname, topology.kubernetes.io/rack, topology.kubernetes.io/block; or add a custom mapping via KoordinatorSchedulerConfiguration.TopologyKeyMappings")
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
		logger.Error(err, "failed to marshal network-topology-spec annotation; topology constraint will not be applied")
		return "", nil
	}
	return string(raw), nil
}

// topologyKeyToKoordinatorLayer maps a Grove topology key (node label key) to the
// Koordinator layer name used in network-topology-spec.
//
// Lookup order:
//  1. userMappings (from KoordinatorSchedulerConfiguration.TopologyKeyMappings) — takes precedence.
//  2. Built-in canonical Kubernetes node label keys.
//
// Supported built-in mappings:
//   - "kubernetes.io/hostname"         → "hostLayer"
//   - "topology.kubernetes.io/rack"    → "rackLayer"
//   - "topology.kubernetes.io/block"   → "blockLayer"
//
// Returns "" for unknown keys. Exact key matching is intentional: substring heuristics
// (e.g. Contains("host")) would silently mis-map unrelated keys such as "nfs-hostpath".
// Callers are responsible for deciding how to handle an empty return value — see
// buildTopologyAnnotation for the Required vs Preferred policy.
func topologyKeyToKoordinatorLayer(topologyKey string, userMappings map[string]string) string {
	// Check user-defined mappings first; they override the built-in list.
	if layer, ok := userMappings[topologyKey]; ok {
		return layer
	}
	switch topologyKey {
	case "kubernetes.io/hostname":
		return "hostLayer"
	case "topology.kubernetes.io/rack":
		return "rackLayer"
	case "topology.kubernetes.io/block":
		return "blockLayer"
	default:
		return ""
	}
}
