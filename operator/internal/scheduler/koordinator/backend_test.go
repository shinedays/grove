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
	"encoding/json"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// TestNew_ValidConfig verifies that New() succeeds with a valid config and returns
// a backend whose Name() matches the koord-scheduler profile name.
func TestNew_ValidConfig(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}

	b := New(cl, cl.Scheme(), recorder, profile)
	assert.Equal(t, "koord-scheduler", b.Name())
}

// TestNew_InvalidConfig_StoresError verifies that New() does not panic when the config
// cannot be parsed — it stores the error in initErr so that Init() can surface it as a
// fatal startup failure. The backend object is constructed (not nil) and its Name()
// works correctly, but Init() must return an error.
func TestNew_InvalidConfig_StoresError(t *testing.T) {
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

	// Backend is constructed (not nil) so the factory pattern is preserved.
	assert.NotNil(t, b)
	assert.Equal(t, "koord-scheduler", b.Name())

	// No Kubernetes Event should be emitted — the error is logged via log.Log.Error instead.
	select {
	case unexpected := <-recorder.Events:
		t.Fatalf("unexpected event emitted: %s", unexpected)
	default:
		// correct: no event
	}
}

// TestBackend_Init_InvalidConfig_ReturnsError verifies that Init() surfaces the config
// parse error captured at New() time. This makes a misconfigured koord-scheduler profile
// fatal at operator startup (the scheduler manager propagates Init() errors) rather than
// silently scheduling with default settings.
func TestBackend_Init_InvalidConfig_ReturnsError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)

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
	initErr := b.Init()
	require.Error(t, initErr, "Init() must return an error for an invalid config")
	assert.Contains(t, initErr.Error(), "invalid koord-scheduler profile config")
}

// TestBackend_PreparePod verifies the Backend.PreparePod interface method delegates
// correctly and sets the scheduler name on the Pod.
func TestBackend_PreparePod(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}

	b := New(cl, cl.Scheme(), recorder, profile)

	pod := &corev1.Pod{}
	b.PreparePod(pod)

	assert.Equal(t, "koord-scheduler", pod.Spec.SchedulerName)
}

// TestBackend_Init_ValidConfig_ReturnsNoError verifies that Init() returns nil when the
// config is valid, allowing the operator to start normally.
func TestBackend_Init_ValidConfig_ReturnsNoError(t *testing.T) {
	cl := testutils.CreateDefaultFakeClient(nil)
	recorder := record.NewFakeRecorder(10)
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}

	b := New(cl, cl.Scheme(), recorder, profile)
	assert.NoError(t, b.Init())
}
