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
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestPod(labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels:    labels,
		},
	}
}

func defaultCfg() backendConfig {
	return backendConfig{
		GangMode:               DefaultGangMode,
		MatchPolicy:            DefaultMatchPolicy,
		ScheduleTimeoutSeconds: DefaultTimeoutSecs,
	}
}

func TestPreparePod_SchedulerName(t *testing.T) {
	pod := newTestPod(nil)
	preparePod(pod, defaultCfg())
	assert.Equal(t, "koord-scheduler", pod.Spec.SchedulerName)
}

func TestPreparePod_PodGroupLabel_Set(t *testing.T) {
	// When both grove.io/podgang and grove.io/podclique labels are present,
	// the Koordinator PodGroup label should be set to "{podgang}-{podclique}".
	pod := newTestPod(map[string]string{
		common.LabelPodGang:   "mypodgang",
		common.LabelPodClique: "pc-a",
	})
	preparePod(pod, defaultCfg())
	assert.Equal(t, "mypodgang-pc-a", pod.Labels[LabelPodGroup])
}

func TestPreparePod_PodGroupLabel_MissingGang(t *testing.T) {
	// If grove.io/podgang label is missing, PodGroup label should not be set.
	pod := newTestPod(map[string]string{
		common.LabelPodClique: "pc-a",
	})
	preparePod(pod, defaultCfg())
	_, ok := pod.Labels[LabelPodGroup]
	assert.False(t, ok, "PodGroup label should not be set when podgang label is missing")
}

func TestPreparePod_PodGroupLabel_MissingClique(t *testing.T) {
	// If grove.io/podclique label is missing, PodGroup label should not be set.
	pod := newTestPod(map[string]string{
		common.LabelPodGang: "mypodgang",
	})
	preparePod(pod, defaultCfg())
	_, ok := pod.Labels[LabelPodGroup]
	assert.False(t, ok, "PodGroup label should not be set when podclique label is missing")
}

func TestPreparePod_QoSLabel_Set(t *testing.T) {
	cfg := defaultCfg()
	cfg.DefaultQoSClass = "LSE"
	pod := newTestPod(nil)
	preparePod(pod, cfg)
	assert.Equal(t, "LSE", pod.Labels[LabelKoordinatorQoSClass])
}

func TestPreparePod_QoSLabel_NotSet_WhenEmpty(t *testing.T) {
	cfg := defaultCfg()
	cfg.DefaultQoSClass = ""
	pod := newTestPod(nil)
	preparePod(pod, cfg)
	_, ok := pod.Labels[LabelKoordinatorQoSClass]
	assert.False(t, ok, "QoS label should not be set when DefaultQoSClass is empty")
}

func TestPreparePod_NilLabels_InitialisedBeforeWrite(t *testing.T) {
	// Pod with nil Labels map should not panic; labels should be initialised.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "no-labels"}}
	assert.NotPanics(t, func() { preparePod(pod, defaultCfg()) })
	assert.NotNil(t, pod.Labels)
}
