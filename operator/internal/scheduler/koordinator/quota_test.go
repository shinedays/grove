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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newQuotaPCS(pcsAnnotation string, cliqueAnnotations ...map[string]string) *grovecorev1alpha1.PodCliqueSet {
	var annotations map[string]string
	if pcsAnnotation != "" {
		annotations = map[string]string{AnnotationQuotaName: pcsAnnotation}
	}
	pcs := newPCS(annotations)
	for i, ca := range cliqueAnnotations {
		pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, &grovecorev1alpha1.PodCliqueTemplateSpec{
			Name:        "clique-" + string(rune('a'+i)),
			Annotations: ca,
		})
	}
	return pcs
}

func newElasticQuota(name string, labels map[string]string) *unstructured.Unstructured {
	const namespace = "ns1"
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(elasticQuotaGVK)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetLabels(labels)
	return obj
}

func TestResolveQuotaForPodCliqueSet(t *testing.T) {
	tests := []struct {
		name    string
		pcs     *grovecorev1alpha1.PodCliqueSet
		want    string
		wantErr string
	}{
		{
			name: "no annotation anywhere",
			pcs:  newQuotaPCS("", nil, nil),
			want: "",
		},
		{
			name: "PCS-level applies to all cliques",
			pcs:  newQuotaPCS("team-a", nil, nil),
			want: "team-a",
		},
		{
			name: "clique-level uniform",
			pcs:  newQuotaPCS("", map[string]string{AnnotationQuotaName: "team-b"}, map[string]string{AnnotationQuotaName: "team-b"}),
			want: "team-b",
		},
		{
			name: "clique agrees with PCS",
			pcs:  newQuotaPCS("team-a", map[string]string{AnnotationQuotaName: "team-a"}, nil),
			want: "team-a",
		},
		{
			name:    "clique conflicts with PCS",
			pcs:     newQuotaPCS("team-a", map[string]string{AnnotationQuotaName: "team-b"}, nil),
			wantErr: "conflicts with the PodCliqueSet-level value",
		},
		{
			name:    "cliques disagree",
			pcs:     newQuotaPCS("", map[string]string{AnnotationQuotaName: "team-a"}, map[string]string{AnnotationQuotaName: "team-b"}),
			wantErr: "same",
		},
		{
			name:    "one clique annotated one not without PCS default",
			pcs:     newQuotaPCS("", map[string]string{AnnotationQuotaName: "team-a"}, nil),
			wantErr: "same",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveQuotaForPodCliqueSet(tt.pcs)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateQuotaAnnotations(t *testing.T) {
	tests := []struct {
		name    string
		quota   string
		objects []client.Object
		wantErr string
	}{
		{
			name:  "no annotation skips validation",
			quota: "",
		},
		{
			name:    "quota exists",
			quota:   "team-a",
			objects: []client.Object{newElasticQuota("team-a", nil)},
		},
		{
			name:    "quota missing",
			quota:   "team-a",
			wantErr: "does not exist",
		},
		{
			name:    "parent quota rejected",
			quota:   "team-parent",
			objects: []client.Object{newElasticQuota("team-parent", map[string]string{labelQuotaIsParent: "true"})},
			wantErr: "parent quota",
		},
		{
			name:    "invalid label value",
			quota:   "Team/A",
			wantErr: "not a valid label value",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := testutils.CreateDefaultFakeClient(tt.objects)
			pcs := newQuotaPCS(tt.quota, nil)
			err := validateQuotaAnnotations(context.Background(), cl, pcs)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestPreparePodForPodCliqueSet_InjectsQuotaLabel(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	b := &schedulerBackend{client: cl, cfg: defaultCfg()}

	pcs := newQuotaPCS("team-a", nil)
	pod := newTestPod(gangLabels())
	require.NoError(t, b.PreparePodForPodCliqueSet(pcs, pod))

	assert.Equal(t, "team-a", pod.Labels[LabelQuotaName])
	assert.Equal(t, "koord-scheduler", pod.Spec.SchedulerName, "regular PreparePod work must still happen")
	assert.NotEmpty(t, pod.Annotations[AnnotationGangName])
}

func TestPreparePodForPodCliqueSet_NoAnnotation_NoLabel(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	b := &schedulerBackend{client: cl, cfg: defaultCfg()}

	pcs := newQuotaPCS("", nil)
	pod := newTestPod(gangLabels())
	require.NoError(t, b.PreparePodForPodCliqueSet(pcs, pod))

	_, ok := pod.Labels[LabelQuotaName]
	assert.False(t, ok, "no quota label must be injected when the annotation is absent")
}

func TestPreparePodForPodCliqueSet_InvalidLabelValue_ReturnsError(t *testing.T) {
	// Defensive check for objects that bypassed the admission webhook: an invalid label
	// value must fail preparation instead of producing a pod the API server rejects.
	cl := testutils.CreateDefaultFakeClient(nil)
	b := &schedulerBackend{client: cl, cfg: defaultCfg()}

	pcs := newQuotaPCS("Bad/Quota", nil)
	pod := newTestPod(gangLabels())
	err := b.PreparePodForPodCliqueSet(pcs, pod)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid label value")
}

func TestValidatePodCliqueSetUpdate_QuotaImmutable(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient([]client.Object{
		newElasticQuota("team-a", nil),
		newElasticQuota("team-b", nil),
	})
	b := &schedulerBackend{client: cl, cfg: defaultCfg()}
	ctx := context.Background()

	// Changing the bound quota is rejected: the binding lives on pods created at different
	// times, so a change would split the gang across quotas.
	oldPCS := newQuotaPCS("team-a", nil)
	newPCS := oldPCS.DeepCopy()
	newPCS.Annotations[AnnotationQuotaName] = "team-b"
	err := b.ValidatePodCliqueSetUpdate(ctx, oldPCS, newPCS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable")

	// Unchanged binding is tolerated even when the quota no longer exists.
	gone := newQuotaPCS("team-gone", nil)
	assert.NoError(t, b.ValidatePodCliqueSetUpdate(ctx, gone, gone.DeepCopy()))

	// Adding a binding to a previously unbound workload is also rejected: the label is only
	// set on pods at creation, so existing pods would stay in the default quota.
	oldEmpty := newQuotaPCS("", nil)
	err = b.ValidatePodCliqueSetUpdate(ctx, oldEmpty, newQuotaPCS("team-a", nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable")

	// Removing a binding is rejected for the same reason.
	err = b.ValidatePodCliqueSetUpdate(ctx, newQuotaPCS("team-a", nil), oldEmpty)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable")

	// Repairing legacy conflicting annotations into a resolvable state is fully validated.
	conflicted := newQuotaPCS("team-a", map[string]string{AnnotationQuotaName: "team-b"})
	err = b.ValidatePodCliqueSetUpdate(ctx, conflicted, newQuotaPCS("no-such-quota", nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
	assert.NoError(t, b.ValidatePodCliqueSetUpdate(ctx, conflicted, newQuotaPCS("team-a", nil)))
}

func TestValidatePodCliqueSetUpdate_LegacyConflictingQuotaAnnotations_Tolerated(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	b := &schedulerBackend{client: cl, cfg: defaultCfg()}
	ctx := context.Background()

	// Legacy object with conflicting quota annotations (unresolvable): unchanged → tolerated.
	oldPCS := newQuotaPCS("team-a", map[string]string{AnnotationQuotaName: "team-b"})
	newPCS := oldPCS.DeepCopy()
	assert.NoError(t, b.ValidatePodCliqueSetUpdate(ctx, oldPCS, newPCS))

	// Introducing a conflict on a previously clean object is rejected.
	cleanOld := newQuotaPCS("team-a", nil)
	conflictNew := newQuotaPCS("team-a", map[string]string{AnnotationQuotaName: "team-b"})
	err := b.ValidatePodCliqueSetUpdate(ctx, cleanOld, conflictNew)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting quota annotations")
}
