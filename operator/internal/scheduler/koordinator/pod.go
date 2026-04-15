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
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	"github.com/ai-dynamo/grove/operator/api/common"

	corev1 "k8s.io/api/core/v1"
)

// preparePod adds koord-scheduler-specific configuration to a pod before it is created.
//
// It performs three tasks:
//  1. Sets Pod.Spec.SchedulerName to "koord-scheduler" so the pod is picked up by koord-scheduler.
//  2. Injects the Koordinator PodGroup association label (pod-group.scheduling.sigs.k8s.io) so
//     koord-scheduler's coscheduling plugin can associate the pod with its PodGroup CR.
//     The PodGroup name is derived from the pod's grove.io/podgang and grove.io/podclique labels
//     using the same naming convention as syncPodGang.
//  3. Optionally injects the koordinator.sh/qosClass label when cfg.DefaultQoSClass is set.
func preparePod(pod *corev1.Pod, cfg backendConfig) {
	// 1. Set the scheduler name.
	pod.Spec.SchedulerName = string(configv1alpha1.SchedulerNameKoordinator)

	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}

	// 2. Inject the PodGroup association label.
	// The Koordinator PodGroup name is "{podgang}-{podclique}" (same convention used in SyncPodGang).
	gangName := pod.Labels[common.LabelPodGang]
	cliqueName := pod.Labels[common.LabelPodClique]
	if gangName != "" && cliqueName != "" {
		pod.Labels[LabelPodGroup] = podGroupName(gangName, cliqueName)
	}

	// 3. Optionally inject the QoS class label.
	if cfg.DefaultQoSClass != "" {
		pod.Labels[LabelKoordinatorQoSClass] = cfg.DefaultQoSClass
	}
}

