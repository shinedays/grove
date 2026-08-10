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

import "k8s.io/apimachinery/pkg/runtime/schema"

// Koordinator PodGroup CRD identity.
var podGroupGVK = schema.GroupVersionKind{
	Group:   "scheduling.sigs.k8s.io",
	Version: "v1alpha1",
	Kind:    "PodGroup",
}

// AnnotationGangName is the pod annotation associating a pod with its Koordinator PodGroup (same
// namespace). Koordinator also accepts the pod-group.scheduling.sigs.k8s.io label, but Grove-derived
// names ("{podgang}-{podclique}") can exceed the 63-character label value limit; annotations have none.
const AnnotationGangName = "gang.scheduling.koordinator.sh/name"

// Koordinator gang scheduling annotation keys on PodGroup CRs.
// See: https://koordinator.sh/docs/user-manuals/gang-scheduling
const (
	// AnnotationGangGroups declares which PodGroups form a GangGroup.
	// Value is a JSON array of "namespace/podgroup-name" strings, e.g. ["ns/pg1","ns/pg2"].
	AnnotationGangGroups = "gang.scheduling.koordinator.sh/groups"

	// AnnotationGangMode controls the failure handling policy for the gang.
	// Valid values: "Strict" (default), "NonStrict".
	AnnotationGangMode = "gang.scheduling.koordinator.sh/mode"

	// AnnotationGangMatchPolicy controls when a GangGroup is considered satisfied.
	// Valid values: "once-satisfied" (default), "only-waiting", "waiting-and-running".
	AnnotationGangMatchPolicy = "gang.scheduling.koordinator.sh/match-policy"

	// AnnotationGangTotalNum specifies the total children number of a Koordinator gang.
	AnnotationGangTotalNum = "gang.scheduling.koordinator.sh/total-number"
)

// LabelKoordinatorQoSClass is the Koordinator QoS label key on pods.
const LabelKoordinatorQoSClass = "koordinator.sh/qosClass"

// Grove workload-level annotations consumed by the koord-scheduler backend.
const (
	// AnnotationScheduleTimeoutSeconds overrides KoordinatorSchedulerConfiguration.ScheduleTimeoutSeconds
	// for one workload (positive integer seconds, gang-wide). Set on the PodCliqueSet; the podgang
	// component mirrors it onto the PodGang, where syncPodGang reads it.
	AnnotationScheduleTimeoutSeconds = "scheduling.grove.io/koordinator-schedule-timeout-seconds"

	// AnnotationQuotaName binds the workload's pods to a Koordinator ElasticQuota by injecting the
	// quota.scheduling.koordinator.sh/name label. Allowed on the PCS or on clique templates (values
	// must agree). Unlike the timeout annotation it is resolved from the PodCliqueSet, not the PodGang.
	AnnotationQuotaName = "scheduling.grove.io/koordinator-quota"
)

// LabelQuotaName is the Koordinator pod label that binds a pod to an ElasticQuota.
const LabelQuotaName = "quota.scheduling.koordinator.sh/name"

// Default values for optional gang scheduling configuration.
const (
	DefaultGangMode    = "Strict"
	DefaultMatchPolicy = "once-satisfied"
	DefaultTimeoutSecs = int32(30)
)

// AnnotationNetworkTopologySpec is the Koordinator network-topology-spec annotation key on PodGroup CRs.
const AnnotationNetworkTopologySpec = "gang.scheduling.koordinator.sh/network-topology-spec"

// topologyStrategyMustGather maps to Grove's Required topology constraint.
const topologyStrategyMustGather = "MustGather"

// topologyStrategyPreferGather maps to Grove's Preferred topology constraint.
const topologyStrategyPreferGather = "PreferGather"
