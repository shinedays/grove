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
	"encoding/json"
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// TestNew_ValidConfig verifies that New() returns a backend whose Name() matches
// the koord-scheduler profile name.
func TestNew_ValidConfig(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}

	b := New(cl, cl.Scheme(), recorder, profile)
	assert.Equal(t, "koord-scheduler", b.Name())
}

// TestBackend_Init_InvalidConfig_ReturnsError verifies that Init() surfaces a config parse
// error, making a misconfigured koord-scheduler profile fatal at operator startup.
func TestBackend_Init_InvalidConfig_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)

	// Inject an invalid GangMode to trigger a parseConfig error.
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		GangMode: "InvalidMode",
	}
	rawBytes, err := json.Marshal(raw)
	require.NoError(t, err)

	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}

	b := New(cl, cl.Scheme(), recorder, profile)
	require.NotNil(t, b)

	initErr := b.Init(cl)
	require.Error(t, initErr, "Init() must return an error for an invalid config")
	assert.Contains(t, initErr.Error(), "invalid koord-scheduler profile config")
}

// TestBackend_Init_ValidConfig_ReturnsNoError verifies that Init() returns nil when the
// config is valid, allowing the operator to start normally.
func TestBackend_Init_ValidConfig_ReturnsNoError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}

	b := New(cl, cl.Scheme(), recorder, profile)
	assert.NoError(t, b.Init(cl))
}

// TestBackend_PreparePod verifies the Backend.PreparePod interface method delegates
// correctly and sets the scheduler name on the Pod.
func TestBackend_PreparePod(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}

	b := New(cl, cl.Scheme(), recorder, profile)
	require.NoError(t, b.Init(cl))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				common.LabelPodGang:   "gang-a",
				common.LabelPodClique: "clique-b",
			},
		},
	}
	require.NoError(t, b.PreparePod(pod))

	assert.Equal(t, "koord-scheduler", pod.Spec.SchedulerName)
	assert.Equal(t, "gang-a-clique-b", pod.Annotations[AnnotationGangName])
}

// TestBackend_PreparePod_MissingLabels_ReturnsError verifies that PreparePod refuses a pod
// lacking the grove.io/podgang or grove.io/podclique labels: without them the PodGroup
// association label cannot be derived and the pod would bypass gang scheduling entirely.
func TestBackend_PreparePod_MissingLabels_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}

	b := New(cl, cl.Scheme(), recorder, profile)
	require.NoError(t, b.Init(cl))

	tests := []struct {
		name   string
		labels map[string]string
	}{
		{name: "no labels", labels: nil},
		{name: "missing podclique label", labels: map[string]string{common.LabelPodGang: "gang-a"}},
		{name: "missing podgang label", labels: map[string]string{common.LabelPodClique: "clique-b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: tt.labels}}
			assert.Error(t, b.PreparePod(pod))
		})
	}
}
