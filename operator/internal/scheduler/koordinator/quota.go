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

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// elasticQuotaGVK identifies Koordinator's ElasticQuota CRD. Quota names are cluster-unique
// (enforced by Koordinator's webhook), so lookups list cluster-wide and filter by name.
var elasticQuotaGVK = schema.GroupVersionKind{
	Group:   "scheduling.sigs.k8s.io",
	Version: "v1alpha1",
	Kind:    "ElasticQuota",
}

// labelQuotaIsParent marks an inner node of the quota tree; pods may only bind to leaf quotas.
const labelQuotaIsParent = "quota.scheduling.koordinator.sh/is-parent"

// resolveQuotaForPodCliqueSet resolves the effective ElasticQuota name from the PCS-level and
// clique-level scheduling.grove.io/koordinator-quota annotations. All cliques must resolve to the
// same quota (a gang split across quotas blocks whenever one lacks headroom); "" when unset.
func resolveQuotaForPodCliqueSet(pcs *grovecorev1alpha1.PodCliqueSet) (string, error) {
	if pcs == nil {
		return "", nil
	}
	pcsQuota := pcs.Annotations[AnnotationQuotaName]
	effective := ""
	seen := false
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		cliqueQuota := clique.Annotations[AnnotationQuotaName]
		resolved := cliqueQuota
		if resolved == "" {
			resolved = pcsQuota
		} else if pcsQuota != "" && cliqueQuota != pcsQuota {
			return "", fmt.Errorf("PodClique %q sets %s=%q which conflicts with the PodCliqueSet-level value %q",
				clique.Name, AnnotationQuotaName, cliqueQuota, pcsQuota)
		}
		if !seen {
			effective = resolved
			seen = true
			continue
		}
		if resolved != effective {
			return "", fmt.Errorf("all PodCliques must resolve to the same %s value; got %q and %q",
				AnnotationQuotaName, effective, resolved)
		}
	}
	if !seen {
		effective = pcsQuota
	}
	return effective, nil
}

// validateQuotaAnnotations checks at admission that the bound ElasticQuota name is a valid label
// value, exists, and is a leaf. Koordinator silently falls back to koordinator-default-quota for
// an unknown quota, so a typo would otherwise go unnoticed.
func validateQuotaAnnotations(ctx context.Context, cl client.Client, pcs *grovecorev1alpha1.PodCliqueSet) error {
	quota, err := resolveQuotaForPodCliqueSet(pcs)
	if err != nil {
		return fmt.Errorf("PodCliqueSet %q has conflicting quota annotations: %w", pcs.Name, err)
	}
	if quota == "" {
		return nil
	}
	if errs := validation.IsValidLabelValue(quota); len(errs) > 0 {
		return fmt.Errorf("PodCliqueSet %q references ElasticQuota %q which is not a valid label value: %v",
			pcs.Name, quota, errs)
	}
	obj, err := findElasticQuotaByName(ctx, cl, quota)
	if err != nil {
		return fmt.Errorf("failed to look up ElasticQuota %q for PodCliqueSet %q: %w", quota, pcs.Name, err)
	}
	if obj == nil {
		return fmt.Errorf("PodCliqueSet %q references ElasticQuota %q which does not exist; "+
			"koord-scheduler would silently fall back to the default quota — create the ElasticQuota or fix the %s annotation",
			pcs.Name, quota, AnnotationQuotaName)
	}
	if obj.GetLabels()[labelQuotaIsParent] == "true" {
		return fmt.Errorf("PodCliqueSet %q references ElasticQuota %q which is a parent quota; "+
			"pods can only be bound to leaf quotas", pcs.Name, quota)
	}
	return nil
}

// findElasticQuotaByName returns the ElasticQuota with the given name, or nil. The list is a live
// API call (unstructured objects bypass the manager cache); duplicates yield the first match.
func findElasticQuotaByName(ctx context.Context, cl client.Client, name string) (*unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(elasticQuotaGVK.GroupVersion().WithKind(elasticQuotaGVK.Kind + "List"))
	if err := cl.List(ctx, list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].GetName() == name {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}
