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
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func gangLabels() map[string]string {
	return map[string]string{
		common.LabelPodGang:   "mypodgang",
		common.LabelPodClique: "pc-a",
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
	pod := newTestPod(gangLabels())
	require.NoError(t, preparePod(pod, defaultCfg()))
	assert.Equal(t, "koord-scheduler", pod.Spec.SchedulerName)
}

func TestPreparePod_GangNameAnnotation_Set(t *testing.T) {
	// When both grove.io/podgang and grove.io/podclique labels are present, the
	// Koordinator gang-name annotation should be set to "{podgang}-{podclique}".
	pod := newTestPod(gangLabels())
	require.NoError(t, preparePod(pod, defaultCfg()))
	assert.Equal(t, "mypodgang-pc-a", pod.Annotations[AnnotationGangName])
}

func TestPreparePod_GangNameAnnotation_LongNamesStayValid(t *testing.T) {
	// PodGroup names concatenate two FQNs and can exceed the 63-character label value
	// limit; the annotation-based association must accept them without truncation.
	pod := newTestPod(map[string]string{
		common.LabelPodGang:   "my-inference-workload-0-decode-workers-0",
		common.LabelPodClique: "my-inference-workload-0-decode-workers-1-worker",
	})
	require.NoError(t, preparePod(pod, defaultCfg()))
	want := "my-inference-workload-0-decode-workers-0-my-inference-workload-0-decode-workers-1-worker"
	assert.Equal(t, want, pod.Annotations[AnnotationGangName])
	assert.Greater(t, len(want), 63)
}

func TestPreparePod_MissingGang_ReturnsError(t *testing.T) {
	// If grove.io/podgang label is missing, the PodGroup association label cannot be
	// derived — preparePod must fail rather than silently bypass gang scheduling.
	pod := newTestPod(map[string]string{
		common.LabelPodClique: "pc-a",
	})
	err := preparePod(pod, defaultCfg())
	require.Error(t, err)
	assert.Contains(t, err.Error(), common.LabelPodGang)
}

func TestPreparePod_MissingClique_ReturnsError(t *testing.T) {
	// If grove.io/podclique label is missing, preparePod must fail.
	pod := newTestPod(map[string]string{
		common.LabelPodGang: "mypodgang",
	})
	err := preparePod(pod, defaultCfg())
	require.Error(t, err)
	assert.Contains(t, err.Error(), common.LabelPodClique)
}

func TestPreparePod_QoSLabel_Set(t *testing.T) {
	cfg := defaultCfg()
	cfg.DefaultQoSClass = "LSE"
	pod := newTestPod(gangLabels())
	require.NoError(t, preparePod(pod, cfg))
	assert.Equal(t, "LSE", pod.Labels[LabelKoordinatorQoSClass])
}

func TestPreparePod_QoSLabel_NotSet_WhenEmpty(t *testing.T) {
	cfg := defaultCfg()
	cfg.DefaultQoSClass = ""
	pod := newTestPod(gangLabels())
	require.NoError(t, preparePod(pod, cfg))
	_, ok := pod.Labels[LabelKoordinatorQoSClass]
	assert.False(t, ok, "QoS label should not be set when DefaultQoSClass is empty")
}

func TestPreparePod_QoSLabel_UserLabelTakesPrecedence(t *testing.T) {
	// A QoS label set on the clique template reaches the pod through the PodClique
	// metadata inheritance chain; the configured default must not override it.
	cfg := defaultCfg()
	cfg.DefaultQoSClass = "BE"
	labels := gangLabels()
	labels[LabelKoordinatorQoSClass] = "LSR"
	pod := newTestPod(labels)
	require.NoError(t, preparePod(pod, cfg))
	assert.Equal(t, "LSR", pod.Labels[LabelKoordinatorQoSClass],
		"per-clique QoS label must take precedence over DefaultQoSClass")
}

func TestPreparePod_NilLabels_ReturnsError(t *testing.T) {
	// A pod with a nil Labels map cannot carry the required gang labels — preparePod
	// must return an error and must not panic.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "no-labels"}}
	assert.NotPanics(t, func() {
		assert.Error(t, preparePod(pod, defaultCfg()))
	})
}
