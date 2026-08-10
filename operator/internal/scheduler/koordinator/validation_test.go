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
	"github.com/ai-dynamo/grove/operator/internal/mnnvl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newPCS(annotations map[string]string) *grovecorev1alpha1.PodCliqueSet {
	return &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pcs",
			Namespace:   "default",
			Annotations: annotations,
		},
	}
}

// newGPUClique returns a clique template whose pod spec requests one GPU, so that the
// MNNVL hierarchical group resolution treats it as a candidate clique.
func newGPUClique(name string, annotations map[string]string) *grovecorev1alpha1.PodCliqueTemplateSpec {
	return &grovecorev1alpha1.PodCliqueTemplateSpec{
		Name:        name,
		Annotations: annotations,
		Spec: grovecorev1alpha1.PodCliqueSpec{
			PodSpec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "main",
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								"nvidia.com/gpu": resource.MustParse("1"),
							},
						},
					},
				},
			},
		},
	}
}

func TestValidatePodCliqueSet_MNNVLGroupOnPCS_Rejected(t *testing.T) {
	pcs := newPCS(map[string]string{
		mnnvl.AnnotationMNNVLGroup: "group-a",
	})
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", nil),
	}
	err := validatePodCliqueSet(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MNNVL is not supported with the koord-scheduler backend")
	assert.Contains(t, err.Error(), "group-a")
}

func TestValidatePodCliqueSet_MNNVLGroupOnClique_Rejected(t *testing.T) {
	pcs := newPCS(nil)
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", map[string]string{mnnvl.AnnotationMNNVLGroup: "group-b"}),
	}
	err := validatePodCliqueSet(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "group-b")
}

func TestValidatePodCliqueSet_MNNVLOptOut_Accepted(t *testing.T) {
	// PCS-level group with an explicit clique-level opt-out ("none") resolves to no
	// active MNNVL group, so the PCS is accepted.
	pcs := newPCS(map[string]string{
		mnnvl.AnnotationMNNVLGroup: "group-a",
	})
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", map[string]string{mnnvl.AnnotationMNNVLGroup: mnnvl.AnnotationMNNVLGroupOptOut}),
	}
	err := validatePodCliqueSet(context.Background(), pcs)
	assert.NoError(t, err)
}

func TestValidatePodCliqueSet_MNNVLGroupWithoutGPUClique_Accepted(t *testing.T) {
	// The mnnvl-group annotation only activates for GPU cliques; a PCS whose cliques
	// request no GPUs never enrolls, so it is accepted.
	pcs := newPCS(map[string]string{
		mnnvl.AnnotationMNNVLGroup: "group-a",
	})
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{Name: "cpu-only"},
	}
	err := validatePodCliqueSet(context.Background(), pcs)
	assert.NoError(t, err)
}

func TestValidatePodCliqueSet_NoMNNVLAnnotation_Accepted(t *testing.T) {
	pcs := newPCS(nil)
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", nil),
	}
	err := validatePodCliqueSet(context.Background(), pcs)
	assert.NoError(t, err)
}

func TestValidatePodCliqueSet_PCSGTopologyConstraint_Rejected(t *testing.T) {
	pcs := newPCS(nil)
	pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{
		{
			Name: "pcsg-a",
			TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{
				PackDomain: grovecorev1alpha1.TopologyDomainRack,
			},
		},
	}

	err := validatePodCliqueSet(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PCSG topology constraints are not supported")
	assert.Contains(t, err.Error(), "pcsg-a")
}

func TestValidatePodCliqueSet_TemplateAndCliqueTopologyConstraints_Accepted(t *testing.T) {
	pcs := newPCS(nil)
	pcs.Spec.Template.TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
		PackDomain: grovecorev1alpha1.TopologyDomainRack,
	}
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name: "worker",
			TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{
				PackDomain: grovecorev1alpha1.TopologyDomainHost,
			},
		},
	}

	err := validatePodCliqueSet(context.Background(), pcs)
	assert.NoError(t, err)
}

func TestValidatePodCliqueSetUpdate_MNNVLAlreadyEnabled_Accepted(t *testing.T) {
	// Legacy object: MNNVL group enrolled before the workload selected this backend.
	// Updates that keep the group unchanged must not be rejected.
	oldPCS := newPCS(map[string]string{
		mnnvl.AnnotationMNNVLGroup: "group-a",
	})
	oldPCS.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", nil),
	}
	newPCS := oldPCS.DeepCopy()

	err := validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS)
	assert.NoError(t, err)
}

func TestValidatePodCliqueSetUpdate_MNNVLNewlyEnabled_Rejected(t *testing.T) {
	oldPCS := newPCS(nil)
	oldPCS.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", nil),
	}
	newPCS := oldPCS.DeepCopy()
	newPCS.Annotations = map[string]string{
		mnnvl.AnnotationMNNVLGroup: "group-a",
	}

	err := validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MNNVL is not supported with the koord-scheduler backend")
}

func TestValidatePodCliqueSetUpdate_MNNVLGroupChanged_Rejected(t *testing.T) {
	// Changing the group name is a new enrollment for the new group — rejected.
	oldPCS := newPCS(map[string]string{
		mnnvl.AnnotationMNNVLGroup: "group-a",
	})
	oldPCS.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", nil),
	}
	newPCS := oldPCS.DeepCopy()
	newPCS.Annotations[mnnvl.AnnotationMNNVLGroup] = "group-b"

	err := validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "group-b")
}

func TestValidatePodCliqueSet_DivergentCliqueTopology_Rejected(t *testing.T) {
	// koord-scheduler applies a single network-topology-spec to the whole gang, so cliques
	// resolving to differing effective constraints must be rejected at admission.
	pcs := newPCS(nil)
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name: "worker",
			TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{
				PackDomain: grovecorev1alpha1.TopologyDomainHost,
			},
		},
		{Name: "leader"}, // inherits no template-level constraint → diverges from worker
	}

	err := validatePodCliqueSet(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "differing effective topologyConstraints")
}

func TestValidatePodCliqueSet_UniformCliqueTopologyViaTemplate_Accepted(t *testing.T) {
	// A template-level constraint inherited by all cliques is uniform and accepted.
	pcs := newPCS(nil)
	pcs.Spec.Template.TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
		PackDomain: grovecorev1alpha1.TopologyDomainRack,
	}
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{Name: "worker"},
		{Name: "leader"},
	}

	assert.NoError(t, validatePodCliqueSet(context.Background(), pcs))
}

func TestValidatePodCliqueSetUpdate_DivergentTopologyUnchanged_Accepted(t *testing.T) {
	// Legacy object already divergent before selecting this backend: tolerated on update.
	oldPCS := newPCS(nil)
	oldPCS.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name: "worker",
			TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{
				PackDomain: grovecorev1alpha1.TopologyDomainHost,
			},
		},
		{Name: "leader"},
	}
	newPCS := oldPCS.DeepCopy()

	assert.NoError(t, validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS))
}

func TestValidatePodCliqueSetUpdate_DivergentTopologyNewlyIntroduced_Rejected(t *testing.T) {
	oldPCS := newPCS(nil)
	oldPCS.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{Name: "worker"},
		{Name: "leader"},
	}
	newPCS := oldPCS.DeepCopy()
	newPCS.Spec.Template.Cliques[0].TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
		PackDomain: grovecorev1alpha1.TopologyDomainHost,
	}

	err := validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "differing effective topologyConstraints")
}

func TestValidatePodCliqueSetUpdate_NewGPUCliqueUnderExistingGroup_Accepted(t *testing.T) {
	// Group-granular tolerance: a clique that newly starts requesting GPUs under an
	// already-active MNNVL group is tolerated — only activating a NEW group is rejected.
	oldPCS := newPCS(map[string]string{
		mnnvl.AnnotationMNNVLGroup: "group-a",
	})
	oldPCS.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		newGPUClique("worker", nil),
		{Name: "cpu-only"},
	}
	newPCS := oldPCS.DeepCopy()
	newPCS.Spec.Template.Cliques[1] = newGPUClique("cpu-only", nil) // flips to GPU, same group

	assert.NoError(t, validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS))
}

func TestValidatePodCliqueSet_InvalidQoSLabel_Rejected(t *testing.T) {
	pcs := newPCS(nil)
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name:   "worker",
			Labels: map[string]string{LabelKoordinatorQoSClass: "GUARANTEED"},
		},
	}
	err := validatePodCliqueSet(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "QoS classes supported by this backend")
	assert.Contains(t, err.Error(), "GUARANTEED")
}

func TestValidatePodCliqueSet_ValidQoSLabel_Accepted(t *testing.T) {
	pcs := newPCS(nil)
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name:   "worker",
			Labels: map[string]string{LabelKoordinatorQoSClass: "LSR"},
		},
	}
	assert.NoError(t, validatePodCliqueSet(context.Background(), pcs))
}

func TestValidatePodCliqueSetUpdate_InvalidQoSLabelUnchanged_Accepted(t *testing.T) {
	// Legacy object with a bad QoS value: updates that keep it unchanged are tolerated.
	oldPCS := newPCS(nil)
	oldPCS.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name:   "worker",
			Labels: map[string]string{LabelKoordinatorQoSClass: "bogus"},
		},
	}
	newPCS := oldPCS.DeepCopy()
	assert.NoError(t, validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS))

	// Changing it to another invalid value is rejected.
	newPCS.Spec.Template.Cliques[0].Labels[LabelKoordinatorQoSClass] = "bogus2"
	err := validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus2")
}

func TestValidatePodCliqueSet_ScheduleTimeoutAnnotation(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "valid positive integer", value: "300", wantErr: false},
		{name: "not an integer", value: "5m", wantErr: true},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-5", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pcs := newPCS(map[string]string{AnnotationScheduleTimeoutSeconds: tt.value})
			err := validatePodCliqueSet(context.Background(), pcs)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), AnnotationScheduleTimeoutSeconds)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePodCliqueSet_TimeoutAnnotationOnClique_Rejected(t *testing.T) {
	// The schedule timeout applies to the whole gang; a clique-level annotation would be
	// silently ignored by the PodGang mirroring path, so it is rejected with guidance.
	pcs := newPCS(nil)
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name:        "worker",
			Annotations: map[string]string{AnnotationScheduleTimeoutSeconds: "300"},
		},
	}
	err := validatePodCliqueSet(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PodCliqueSet-level annotation")
}

func TestValidatePodCliqueSetUpdate_InvalidTimeoutUnchanged_Accepted(t *testing.T) {
	oldPCS := newPCS(map[string]string{AnnotationScheduleTimeoutSeconds: "bogus"})
	newPCS := oldPCS.DeepCopy()
	assert.NoError(t, validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS))

	newPCS.Annotations[AnnotationScheduleTimeoutSeconds] = "still-bogus"
	require.Error(t, validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS))
}

func TestValidatePodCliqueSetUpdate_PCSGTopologyConstraintUnchanged_Accepted(t *testing.T) {
	oldPCS := newPCS(nil)
	oldPCS.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{
		{
			Name: "pcsg-a",
			TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{
				PackDomain: grovecorev1alpha1.TopologyDomainRack,
			},
		},
	}
	newPCS := oldPCS.DeepCopy()

	err := validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS)
	assert.NoError(t, err)
}

func TestValidatePodCliqueSetUpdate_PCSGTopologyConstraintNewlyAdded_Rejected(t *testing.T) {
	oldPCS := newPCS(nil)
	oldPCS.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{
		{Name: "pcsg-a"},
	}
	newPCS := oldPCS.DeepCopy()
	newPCS.Spec.Template.PodCliqueScalingGroupConfigs[0].TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
		PackDomain: grovecorev1alpha1.TopologyDomainRack,
	}

	err := validatePodCliqueSetUpdate(context.Background(), oldPCS, newPCS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PCSG topology constraints are not supported")
	assert.Contains(t, err.Error(), "pcsg-a")
}

// TestHasDivergentCliqueTopology_EquivalentRepresentationsNotDivergent verifies that the
// deprecated packDomain form and the pack.required form of the same domain, as well as an
// inherited vs. explicit topologyName, are treated as the same effective constraint.
func TestHasDivergentCliqueTopology_EquivalentRepresentationsNotDivergent(t *testing.T) {
	pcs := &grovecorev1alpha1.PodCliqueSet{}
	pcs.Spec.Template.TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
		TopologyName: "ctb",
		Pack:         &grovecorev1alpha1.TopologyPackConstraint{RequiredDomain: "rack"},
	}
	pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{Name: "a", TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{PackDomain: "rack"}},
		{Name: "b", TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{
			TopologyName: "ctb", Pack: &grovecorev1alpha1.TopologyPackConstraint{RequiredDomain: "rack"}}},
		{Name: "c"}, // inherits the template-level constraint
	}
	assert.False(t, hasDivergentCliqueTopology(pcs), "same effective domain in different forms must not be divergent")

	// A genuinely different domain is still divergent.
	pcs.Spec.Template.Cliques[0].TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{PackDomain: "zone"}
	assert.True(t, hasDivergentCliqueTopology(pcs))

	// A different effective topologyName is divergent even with the same domain.
	pcs.Spec.Template.Cliques[0].TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
		TopologyName: "other", PackDomain: "rack"}
	assert.True(t, hasDivergentCliqueTopology(pcs))
}
