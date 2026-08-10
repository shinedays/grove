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

// Package koordinator implements the Grove scheduler.Backend interface for Koordinator's koord-scheduler.
//
// # Supported capabilities
//
//   - Gang scheduling: PodGang → one Koordinator PodGroup per PodGroup, linked by GangGroup annotations.
//   - minMember semantics: PodGroup[].MinReplicas → PodGroup.Spec.MinMember.
//   - Priority: via each Pod's spec.priorityClassName (the sig-scheduling PodGroup CRD has no such field).
//   - PodGroup association: PreparePod sets the gang.scheduling.koordinator.sh/name annotation.
//   - ClusterTopologyBinding → ClusterNetworkTopology sync (TopologyAwareBackend): spec of the single
//     "default" CNT only; at most one CTB per cluster; numa levels cannot be represented.
//   - ElasticQuota binding via the scheduling.grove.io/koordinator-quota annotation (immutable on update).
//   - Per-workload schedule timeout via the scheduling.grove.io/koordinator-schedule-timeout-seconds annotation.
//
// # Partially supported capabilities
//
//   - Topology constraints: Required→MustGather, Preferred→PreferGather with exact key→layer matching;
//     unknown Required keys fail reconciliation, unknown Preferred keys are skipped.
//   - QoS: per-clique koordinator.sh/qosClass labels or the DefaultQoSClass fallback; no semantic mapping.
//   - Schedule timeout: gang-level only; annotation changes on a live PCS apply on the next spec change.
//   - ReuseReservationRef: logged and skipped; Koordinator Reservation integration is deferred.
//
// # Not supported capabilities
//
//   - MNNVL / ComputeDomain: DRA claims with Koordinator gang/Reservation/DeviceShare are unvalidated; rejected at admission.
//   - PodCliqueScalingGroup topology constraints: not representable with one PodGroup per clique; rejected at admission.
//   - GPU fine-grained sharing (gpu-core, gpu-memory): requires explicit koordinator.sh/gpu-* requests; whole-GPU works.
//   - Pod migration, NUMA scheduling: outside backend scope (the CNT leaf is the node).
package koordinator

import (
	"context"
	"fmt"
	"sync"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// schedulerBackend implements the scheduler.Backend interface for Koordinator's koord-scheduler.
type schedulerBackend struct {
	client        client.Client
	scheme        *runtime.Scheme
	name          string
	eventRecorder record.EventRecorder
	profile       configv1alpha1.SchedulerProfile
	cfg           backendConfig

	// topologyMu guards syncedTopologyLayerByKey: written by the clustertopology controller
	// (SyncTopology/CheckTopologyDrift), read by the podgang reconciler (SyncPodGang).
	topologyMu               sync.RWMutex
	syncedTopologyLayerByKey map[string]string
}

var _ scheduler.Backend = (*schedulerBackend)(nil)

var _ scheduler.PodCliqueSetUpdateValidator = (*schedulerBackend)(nil)

var _ scheduler.PodCliqueSetAwarePodPreparer = (*schedulerBackend)(nil)

// New creates a new Koordinator backend instance. profile.Config is unmarshalled into
// KoordinatorSchedulerConfiguration by Init.
func New(cl client.Client, scheme *runtime.Scheme, eventRecorder record.EventRecorder, profile configv1alpha1.SchedulerProfile) scheduler.Backend {
	return &schedulerBackend{
		client:        cl,
		scheme:        scheme,
		name:          string(configv1alpha1.SchedulerNameKoordinator),
		eventRecorder: eventRecorder,
		profile:       profile,
	}
}

// Name returns "koord-scheduler", the value that will be set in Pod.Spec.SchedulerName.
func (b *schedulerBackend) Name() string {
	return b.name
}

// Init parses and validates the backend profile config; a parse error is fatal at operator
// startup so a misconfigured profile is never silently ignored. PodGroups are handled as
// unstructured objects, so no scheme registration is needed.
func (b *schedulerBackend) Init(_ client.Client) error {
	cfg, err := parseConfig(b.profile)
	if err != nil {
		return fmt.Errorf("invalid koord-scheduler profile config: %w", err)
	}
	b.cfg = cfg
	return nil
}

// SyncPodGang converts a Grove PodGang into one Koordinator PodGroup CR per PodGroup linked as a
// GangGroup, using topology-key mappings derived from the synced ClusterTopologyBinding.
func (b *schedulerBackend) SyncPodGang(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) error {
	cfg := b.cfg
	cfg.topologyKeyMappings = b.mergedTopologyKeyMappings()
	return syncPodGang(ctx, b.client, b.scheme, b.eventRecorder, cfg, podGang)
}

// PreparePod adds koord-scheduler-specific configuration to a Pod before it is created.
// It sets Pod.Spec.SchedulerName and injects the PodGroup association annotation.
func (b *schedulerBackend) PreparePod(pod *corev1.Pod) error {
	return preparePod(pod, b.cfg)
}

// PreparePodForPodCliqueSet performs the regular PreparePod work and additionally binds the pod
// to the ElasticQuota named by the scheduling.grove.io/koordinator-quota PCS/clique annotation.
func (b *schedulerBackend) PreparePodForPodCliqueSet(pcs *grovecorev1alpha1.PodCliqueSet, pod *corev1.Pod) error {
	if err := preparePod(pod, b.cfg); err != nil {
		return err
	}
	quota, err := resolveQuotaForPodCliqueSet(pcs)
	if err != nil {
		return fmt.Errorf("failed to resolve ElasticQuota for PodCliqueSet %q: %w", pcs.Name, err)
	}
	if quota != "" {
		// Defensive re-check: admission validates this, but an object that bypassed the
		// webhook would otherwise produce a pod the API server rejects on every attempt.
		if errs := validation.IsValidLabelValue(quota); len(errs) > 0 {
			return fmt.Errorf("ElasticQuota name %q for PodCliqueSet %q is not a valid label value: %v",
				quota, pcs.Name, errs)
		}
		pod.Labels[LabelQuotaName] = quota
	}
	return nil
}

// ValidatePodCliqueSet enforces Koordinator backend constraints on the PodCliqueSet at admission time.
func (b *schedulerBackend) ValidatePodCliqueSet(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet) error {
	if err := validatePodCliqueSet(ctx, pcs); err != nil {
		return err
	}
	return validateQuotaAnnotations(ctx, b.client, pcs)
}

// ValidatePodCliqueSetUpdate enforces Koordinator backend constraints on PodCliqueSet updates.
// The quota binding is immutable (it is a pod label set at creation, so a change would split the
// live gang across quotas); unchanged bindings are tolerated so a deleted quota cannot wedge updates.
func (b *schedulerBackend) ValidatePodCliqueSetUpdate(ctx context.Context, oldPCS, newPCS *grovecorev1alpha1.PodCliqueSet) error {
	if err := validatePodCliqueSetUpdate(ctx, oldPCS, newPCS); err != nil {
		return err
	}
	oldQuota, oldErr := resolveQuotaForPodCliqueSet(oldPCS)
	newQuota, newErr := resolveQuotaForPodCliqueSet(newPCS)
	switch {
	case newErr != nil:
		if oldErr != nil {
			//nolint:nilerr // migration-aware: an unchanged unresolvable legacy annotation must not wedge updates.
			return nil
		}
		return fmt.Errorf("PodCliqueSet %q has conflicting quota annotations: %w", newPCS.Name, newErr)
	case oldErr != nil:
		// Repairing legacy unresolvable annotations: never consistently bound, so validate fully.
		return validateQuotaAnnotations(ctx, b.client, newPCS)
	case newQuota == oldQuota:
		// Unchanged binding: skip re-validation so a deleted quota does not wedge the object.
		return nil
	default:
		// Any effective change (add, remove, switch) is rejected: existing pods are never relabelled.
		return fmt.Errorf("the %s annotation is immutable on PodCliqueSet %q (%q → %q): "+
			"the quota is bound via a label on each pod at creation, so changing it would split "+
			"the gang across quotas — recreate the workload to change its quota binding",
			AnnotationQuotaName, newPCS.Name, oldQuota, newQuota)
	}
}
