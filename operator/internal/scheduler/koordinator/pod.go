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
	"fmt"

	"github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"

	corev1 "k8s.io/api/core/v1"
)

// preparePod sets Pod.Spec.SchedulerName, injects the PodGroup association annotation and
// applies cfg.DefaultQoSClass as a fallback QoS label. The annotation value is derived from the
// grove.io/podgang and grove.io/podclique labels via podGroupName so it always matches the
// PodGroup CR created by syncPodGang (see AnnotationGangName for annotation vs label).
func preparePod(pod *corev1.Pod, cfg backendConfig) error {
	gangName := pod.Labels[common.LabelPodGang]
	if gangName == "" {
		return fmt.Errorf("koord-scheduler requires pod label %q", common.LabelPodGang)
	}
	cliqueName := pod.Labels[common.LabelPodClique]
	if cliqueName == "" {
		return fmt.Errorf("koord-scheduler requires pod label %q", common.LabelPodClique)
	}

	pod.Spec.SchedulerName = string(configv1alpha1.SchedulerNameKoordinator)

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[AnnotationGangName] = podGroupName(gangName, cliqueName)

	// DefaultQoSClass is a fallback: a template-level koordinator.sh/qosClass label wins.
	if cfg.DefaultQoSClass != "" {
		if _, ok := pod.Labels[LabelKoordinatorQoSClass]; !ok {
			pod.Labels[LabelKoordinatorQoSClass] = cfg.DefaultQoSClass
		}
	}
	return nil
}
