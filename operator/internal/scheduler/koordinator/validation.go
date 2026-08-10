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

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/mnnvl"
)

// validatePodCliqueSet enforces Koordinator-specific constraints on a PodCliqueSet at admission.
func validatePodCliqueSet(_ context.Context, pcs *grovecorev1alpha1.PodCliqueSet) error {
	// MNNVL depends on DRA (ComputeDomain + ResourceClaims), which is unvalidated with Koordinator
	// gang scheduling/Reservation/DeviceShare: fail closed.
	if group, ok := firstMNNVLGroup(pcs); ok {
		return newMNNVLUnsupportedError(pcs, group)
	}
	if pcsg, ok := firstPCSGWithTopologyConstraint(pcs); ok {
		return newPCSGTopologyUnsupportedError(pcs, pcsg.Name)
	}
	if hasDivergentCliqueTopology(pcs) {
		return newDivergentTopologyError(pcs)
	}
	if cliqueName, value, ok := firstInvalidQoSClique(pcs, nil); ok {
		return newInvalidQoSError(pcs, cliqueName, value)
	}
	if err := validateScheduleTimeoutAnnotation(pcs, nil); err != nil {
		return err
	}
	return nil
}

// validatePodCliqueSetUpdate is migration-aware: it rejects newly requested Koordinator-incompatible
// features while allowing legacy objects with unchanged incompatible fields to be updated or deleted.
// MNNVL tolerance is group-granular: only activating a group name absent from the old object is rejected.
func validatePodCliqueSetUpdate(_ context.Context, oldPCS, newPCS *grovecorev1alpha1.PodCliqueSet) error {
	oldGroups := collectMNNVLGroups(oldPCS)
	for _, group := range sortedKeys(collectMNNVLGroups(newPCS)) {
		if _, existed := oldGroups[group]; !existed {
			return newMNNVLUnsupportedOnUpdateError(newPCS, group)
		}
	}
	if pcsg, ok := firstNewOrChangedPCSGTopologyConstraint(oldPCS, newPCS); ok {
		return newPCSGTopologyUnsupportedError(newPCS, pcsg.Name)
	}
	if !hasDivergentCliqueTopology(oldPCS) && hasDivergentCliqueTopology(newPCS) {
		return newDivergentTopologyError(newPCS)
	}
	if cliqueName, value, ok := firstInvalidQoSClique(newPCS, oldPCS); ok {
		return newInvalidQoSError(newPCS, cliqueName, value)
	}
	if err := validateScheduleTimeoutAnnotation(newPCS, oldPCS); err != nil {
		return err
	}
	return nil
}

// validateScheduleTimeoutAnnotation rejects an invalid schedule-timeout override; on update an
// unchanged invalid value is tolerated. Clique-level placement is rejected: the timeout is gang-wide
// and the PodGang mirroring path only propagates PCS-level annotations.
func validateScheduleTimeoutAnnotation(pcs, oldPCS *grovecorev1alpha1.PodCliqueSet) error {
	if pcs == nil {
		return nil
	}
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		if _, exists := clique.Annotations[AnnotationScheduleTimeoutSeconds]; exists {
			if oldCliqueHasTimeoutAnnotation(oldPCS, clique.Name) {
				continue
			}
			return fmt.Errorf("PodCliqueSet %q sets %s on PodClique %q; "+
				"the schedule timeout applies to the whole gang and must be set as a PodCliqueSet-level annotation",
				pcs.Name, AnnotationScheduleTimeoutSeconds, clique.Name)
		}
	}
	raw, ok := pcs.Annotations[AnnotationScheduleTimeoutSeconds]
	if !ok {
		return nil
	}
	if _, err := effectiveScheduleTimeout(pcs.Annotations, 0); err != nil {
		if oldPCS != nil && oldPCS.Annotations[AnnotationScheduleTimeoutSeconds] == raw {
			return nil
		}
		return fmt.Errorf("PodCliqueSet %q has an invalid %s annotation: %v",
			pcs.Name, AnnotationScheduleTimeoutSeconds, err)
	}
	return nil
}

func oldCliqueHasTimeoutAnnotation(oldPCS *grovecorev1alpha1.PodCliqueSet, cliqueName string) bool {
	if oldPCS == nil {
		return false
	}
	for _, clique := range oldPCS.Spec.Template.Cliques {
		if clique == nil || clique.Name != cliqueName {
			continue
		}
		_, exists := clique.Annotations[AnnotationScheduleTimeoutSeconds]
		return exists
	}
	return false
}

// firstInvalidQoSClique returns the first clique whose koordinator.sh/qosClass template label is
// outside the Koordinator QoS enum; on update (oldPCS non-nil) an unchanged invalid value is
// tolerated (Koordinator treats unknown values as QoSNone).
func firstInvalidQoSClique(pcs, oldPCS *grovecorev1alpha1.PodCliqueSet) (string, string, bool) {
	if pcs == nil {
		return "", "", false
	}
	oldQoSByClique := map[string]string{}
	if oldPCS != nil {
		for _, clique := range oldPCS.Spec.Template.Cliques {
			if clique == nil {
				continue
			}
			if v, exists := clique.Labels[LabelKoordinatorQoSClass]; exists {
				oldQoSByClique[clique.Name] = v
			}
		}
	}
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		v, exists := clique.Labels[LabelKoordinatorQoSClass]
		if !exists {
			continue
		}
		switch v {
		case "LSE", "LSR", "LS", "BE":
			continue
		}
		if oldV, existed := oldQoSByClique[clique.Name]; existed && oldV == v {
			continue
		}
		return clique.Name, v, true
	}
	return "", "", false
}

// hasDivergentCliqueTopology reports whether cliques resolve to differing effective topology
// constraints (clique-level falls back to the template level). koord-scheduler applies a single
// network-topology-spec to the whole GangGroup, so divergence would be resolved by pod arrival order.
func hasDivergentCliqueTopology(pcs *grovecorev1alpha1.PodCliqueSet) bool {
	if pcs == nil || len(pcs.Spec.Template.Cliques) == 0 {
		return false
	}
	effective := func(clique *grovecorev1alpha1.PodCliqueTemplateSpec) *grovecorev1alpha1.TopologyConstraint {
		if clique != nil && clique.TopologyConstraint != nil {
			return clique.TopologyConstraint
		}
		return pcs.Spec.Template.TopologyConstraint
	}
	inheritedName := ""
	if tpl := pcs.Spec.Template.TopologyConstraint; tpl != nil {
		inheritedName = tpl.TopologyName
	}
	first := effectiveTopology(effective(pcs.Spec.Template.Cliques[0]), inheritedName)
	for _, clique := range pcs.Spec.Template.Cliques[1:] {
		if effectiveTopology(effective(clique), inheritedName) != first {
			return true
		}
	}
	return false
}

// topologyIdentity is the scheduler-relevant projection of a TopologyConstraint. Comparing
// it instead of the raw struct treats the deprecated packDomain and pack.required forms
// of the same domain as equal (as the PodGang translation does), so a partial
// packDomain→pack migration is not rejected as divergence.
type topologyIdentity struct {
	present             bool
	name                string
	required, preferred grovecorev1alpha1.TopologyDomain
}

func effectiveTopology(tc *grovecorev1alpha1.TopologyConstraint, inheritedName string) topologyIdentity {
	if tc == nil {
		return topologyIdentity{}
	}
	name := tc.TopologyName
	if name == "" {
		name = inheritedName
	}
	return topologyIdentity{present: true, name: name, required: tc.RequiredDomain(), preferred: tc.PreferredDomain()}
}

// firstMNNVLGroup returns the first MNNVL group activated for pcs (a GPU clique resolving an
// enrolled grove.io/mnnvl-group annotation through the clique → PCSG → PCS hierarchy), if any.
func firstMNNVLGroup(pcs *grovecorev1alpha1.PodCliqueSet) (string, bool) {
	groups := sortedKeys(collectMNNVLGroups(pcs))
	if len(groups) == 0 {
		return "", false
	}
	return groups[0], true
}

// collectMNNVLGroups returns the set of MNNVL groups that would be activated for pcs.
func collectMNNVLGroups(pcs *grovecorev1alpha1.PodCliqueSet) map[string]struct{} {
	groups := make(map[string]struct{})
	if pcs == nil {
		return groups
	}
	pcsgByClique := make(map[string]grovecorev1alpha1.PodCliqueScalingGroupConfig)
	for _, pcsg := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		for _, cliqueName := range pcsg.CliqueNames {
			pcsgByClique[cliqueName] = pcsg
		}
	}
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique == nil || !mnnvl.HasGPUInPodSpec(&clique.Spec.PodSpec) {
			continue
		}
		var pcsgAnnotations map[string]string
		if pcsgCfg, ok := pcsgByClique[clique.Name]; ok {
			pcsgAnnotations = pcsgCfg.Annotations
		}
		if group, ok := mnnvl.ResolveGroupNameHierarchically(clique.Annotations, pcsgAnnotations, pcs.Annotations); ok {
			groups[group] = struct{}{}
		}
	}
	return groups
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func firstPCSGWithTopologyConstraint(pcs *grovecorev1alpha1.PodCliqueSet) (grovecorev1alpha1.PodCliqueScalingGroupConfig, bool) {
	if pcs == nil {
		return grovecorev1alpha1.PodCliqueScalingGroupConfig{}, false
	}
	for _, pcsg := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		if pcsg.TopologyConstraint != nil {
			return pcsg, true
		}
	}
	return grovecorev1alpha1.PodCliqueScalingGroupConfig{}, false
}

func firstNewOrChangedPCSGTopologyConstraint(oldPCS, newPCS *grovecorev1alpha1.PodCliqueSet) (grovecorev1alpha1.PodCliqueScalingGroupConfig, bool) {
	oldByName := map[string]grovecorev1alpha1.PodCliqueScalingGroupConfig{}
	if oldPCS != nil {
		for _, pcsg := range oldPCS.Spec.Template.PodCliqueScalingGroupConfigs {
			oldByName[pcsg.Name] = pcsg
		}
	}
	if newPCS == nil {
		return grovecorev1alpha1.PodCliqueScalingGroupConfig{}, false
	}
	for _, pcsg := range newPCS.Spec.Template.PodCliqueScalingGroupConfigs {
		if pcsg.TopologyConstraint == nil {
			continue
		}
		oldPCSG, ok := oldByName[pcsg.Name]
		if !ok || effectiveTopology(oldPCSG.TopologyConstraint, templateTopologyName(oldPCS)) !=
			effectiveTopology(pcsg.TopologyConstraint, templateTopologyName(newPCS)) {
			return pcsg, true
		}
	}
	return grovecorev1alpha1.PodCliqueScalingGroupConfig{}, false
}

func templateTopologyName(pcs *grovecorev1alpha1.PodCliqueSet) string {
	if pcs == nil || pcs.Spec.Template.TopologyConstraint == nil {
		return ""
	}
	return pcs.Spec.Template.TopologyConstraint.TopologyName
}

func newMNNVLUnsupportedError(pcs *grovecorev1alpha1.PodCliqueSet, group string) error {
	return fmt.Errorf(
		"PodCliqueSet %q enables MNNVL group %q via the %s annotation; "+
			"MNNVL is not supported with the koord-scheduler backend. "+
			"Use the kai-scheduler backend for MNNVL workloads, or remove the annotation",
		pcs.Name, group, mnnvl.AnnotationMNNVLGroup,
	)
}

// newMNNVLUnsupportedOnUpdateError words the rejection for the update path: the mnnvl-group
// annotation is immutable on update, so the actionable fix is reverting the activating change.
func newMNNVLUnsupportedOnUpdateError(pcs *grovecorev1alpha1.PodCliqueSet, group string) error {
	return fmt.Errorf(
		"this update would newly activate MNNVL group %q on PodCliqueSet %q; "+
			"MNNVL is not supported with the koord-scheduler backend. "+
			"Revert the change that activates the group, or use the kai-scheduler backend for MNNVL workloads",
		group, pcs.Name,
	)
}

func newPCSGTopologyUnsupportedError(pcs *grovecorev1alpha1.PodCliqueSet, pcsgName string) error {
	return fmt.Errorf(
		"PodCliqueSet %q uses topologyConstraint on PodCliqueScalingGroup %q; "+
			"PCSG topology constraints are not supported with the koord-scheduler backend. "+
			"Move the topologyConstraint to the PodCliqueSet level, or use a backend that supports per-scaling-group topology",
		pcs.Name, pcsgName,
	)
}

func newDivergentTopologyError(pcs *grovecorev1alpha1.PodCliqueSet) error {
	return fmt.Errorf(
		"PodCliqueSet %q defines differing effective topologyConstraints across its PodCliques; "+
			"koord-scheduler applies a single network-topology-spec to the whole gang, so all cliques "+
			"must share the same constraint — set the topologyConstraint at the PodCliqueSet level instead",
		pcs.Name,
	)
}

func newInvalidQoSError(pcs *grovecorev1alpha1.PodCliqueSet, cliqueName, value string) error {
	// Koordinator's SYSTEM QoS class is reserved for system daemons and intentionally not accepted.
	return fmt.Errorf(
		"PodCliqueSet %q sets label %s=%q on PodClique %q; "+
			"QoS classes supported by this backend are LSE, LSR, LS, BE",
		pcs.Name, LabelKoordinatorQoSClass, value, cliqueName,
	)
}
