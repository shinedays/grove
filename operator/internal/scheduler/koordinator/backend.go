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

// Package koordinator implements the Grove scheduler.Backend interface for Koordinator's koord-scheduler.
//
// # Supported capabilities
//
// The following Grove features are fully supported with this backend:
//   - Gang scheduling: PodGang → multiple Koordinator PodGroups linked by GangGroup annotations.
//   - minMember semantics: PodGroup[].MinReplicas → PodGroup.Spec.MinMember.
//   - Priority: PodGang.Spec.PriorityClassName passed through to PodGroup.
//   - PodGroup label injection: PreparePod sets the pod-group.scheduling.sigs.k8s.io label.
//
// # Partially supported capabilities
//
// The following features have known limitations:
//   - Topology constraints (block/rack/host level only, exact key matching): Required→MustGather,
//     Preferred→PreferGather are injected as Koordinator network-topology-spec annotation on PodGroup CRs.
//     Only three canonical keys are supported: "kubernetes.io/hostname" (hostLayer),
//     "topology.kubernetes.io/rack" (rackLayer), "topology.kubernetes.io/block" (blockLayer).
//     All other keys are skipped with a warning log; substring matching is not used.
//   - QoS label: can be injected via KoordinatorSchedulerConfiguration.DefaultQoSClass, but there is
//     no automatic semantic mapping from Grove concepts to Koordinator QoS classes.
//   - PodGroup scheduling timeout: controlled via KoordinatorSchedulerConfiguration.ScheduleTimeoutSeconds
//     (default 30s); Grove PodGang itself has no timeout field.
//   - ReuseReservationRef: first-version support logs a warning and skips; full Koordinator Reservation
//     integration is deferred.
//
// # Not supported capabilities
//
// The following features cannot be supported with this backend:
//   - MNNVL / ComputeDomain: depends on NVIDIA DRA, incompatible with Koordinator DeviceShare.
//     PodCliqueSet with grove.io/auto-mnnvl enabled is rejected at admission.
//   - GPU fine-grained sharing (gpu-core, gpu-memory): requires koordinator.sh/gpu-* resource names;
//     Grove Pods use standard nvidia.com/gpu.
//   - ClusterTopology → ClusterNetworkTopology CRD synchronisation: Koordinator's ClusterNetworkTopology
//     is managed by the Koordinator operator independently. This backend does not implement TopologyAwareSchedBackend.
//   - ElasticQuota, Pod migration, NUMA scheduling: outside Grove scheduler backend scope.
package koordinator

import (
	"context"
	"fmt"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// schedulerBackend implements the scheduler.Backend interface for Koordinator's koord-scheduler.
type schedulerBackend struct {
	client        client.Client
	scheme        *runtime.Scheme
	name          string
	eventRecorder record.EventRecorder
	cfg           backendConfig
	// initErr captures a config parse error from New() so that Init() can surface it
	// as a fatal startup failure. Storing it here avoids changing the New() signature,
	// which does not return an error per the existing factory convention.
	initErr error
}

var _ scheduler.Backend = (*schedulerBackend)(nil)

// New creates a new Koordinator backend instance.
// profile is the scheduler profile for koord-scheduler; its Config field is unmarshalled
// into KoordinatorSchedulerConfiguration to supply backend-specific options.
//
// A config parse error is stored in initErr and surfaced by Init(), which causes the
// operator to fail at startup rather than silently running with default settings.
func New(cl client.Client, scheme *runtime.Scheme, eventRecorder record.EventRecorder, profile configv1alpha1.SchedulerProfile) scheduler.Backend {
	cfg, err := parseConfig(profile)
	if err != nil {
		// Log at construction time so the error is visible even before Init() is called.
		log.Log.Error(err, "invalid koord-scheduler profile config; operator startup will fail",
			"profile", profile.Name)
	}
	return &schedulerBackend{
		client:        cl,
		scheme:        scheme,
		name:          string(configv1alpha1.SchedulerNameKoordinator),
		eventRecorder: eventRecorder,
		cfg:           cfg,
		initErr:       err,
	}
}

// Name returns "koord-scheduler", the value that will be set in Pod.Spec.SchedulerName.
func (b *schedulerBackend) Name() string {
	return b.name
}

// Init validates that the backend is ready to use.
// It surfaces any config parse error captured by New(), making misconfigured profiles
// fatal at operator startup (the scheduler manager propagates Init() errors).
func (b *schedulerBackend) Init() error {
	if b.initErr != nil {
		return fmt.Errorf("invalid koord-scheduler profile config: %w", b.initErr)
	}
	return nil
}

// SyncPodGang converts a Grove PodGang into one Koordinator PodGroup CR per PodGroup,
// linked together as a GangGroup via annotations.
func (b *schedulerBackend) SyncPodGang(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) error {
	return syncPodGang(ctx, b.client, b.scheme, b.eventRecorder, b.cfg, podGang)
}

// OnPodGangDelete is a no-op: PodGroup CRs are owned by the PodGang via OwnerReference
// and are garbage-collected automatically by Kubernetes when the PodGang is deleted.
func (b *schedulerBackend) OnPodGangDelete(_ context.Context, _ *groveschedulerv1alpha1.PodGang) error {
	return nil
}

// PreparePod adds koord-scheduler-specific configuration to a Pod before it is created.
// It sets Pod.Spec.SchedulerName and injects the PodGroup association label.
func (b *schedulerBackend) PreparePod(pod *corev1.Pod) {
	preparePod(pod, b.cfg)
}

// ValidatePodCliqueSet enforces Koordinator backend constraints on the PodCliqueSet at admission time.
// It rejects configurations that are fundamentally incompatible with koord-scheduler.
func (b *schedulerBackend) ValidatePodCliqueSet(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet) error {
	return validatePodCliqueSet(ctx, pcs)
}
