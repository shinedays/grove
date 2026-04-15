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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestParseConfig_Defaults(t *testing.T) {
	// Profile with no Config → all defaults applied.
	profile := configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKoordinator}
	cfg, err := parseConfig(profile)
	require.NoError(t, err)
	assert.Equal(t, DefaultGangMode, cfg.GangMode)
	assert.Equal(t, DefaultMatchPolicy, cfg.MatchPolicy)
	assert.Equal(t, DefaultTimeoutSecs, cfg.ScheduleTimeoutSeconds)
	assert.Empty(t, cfg.DefaultQoSClass)
}

func TestParseConfig_CustomValues(t *testing.T) {
	timeout := int32(60)
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		GangMode:               "NonStrict",
		MatchPolicy:            "only-waiting",
		ScheduleTimeoutSeconds: &timeout,
		DefaultQoSClass:        "LSE",
	}
	rawBytes, err := json.Marshal(raw)
	require.NoError(t, err)

	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	cfg, err := parseConfig(profile)
	require.NoError(t, err)
	assert.Equal(t, "NonStrict", cfg.GangMode)
	assert.Equal(t, "only-waiting", cfg.MatchPolicy)
	assert.Equal(t, int32(60), cfg.ScheduleTimeoutSeconds)
	assert.Equal(t, "LSE", cfg.DefaultQoSClass)
}

func TestParseConfig_InvalidJSON(t *testing.T) {
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: []byte(`{invalid json}`)},
	}
	cfg, err := parseConfig(profile)
	// Expect error, defaults still returned.
	require.Error(t, err)
	assert.Equal(t, DefaultGangMode, cfg.GangMode)
}

func TestParseConfig_InvalidGangMode(t *testing.T) {
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		GangMode: "InvalidMode",
	}
	rawBytes, _ := json.Marshal(raw)
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	cfg, err := parseConfig(profile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GangMode")
	// Defaults must still be returned so callers can fall back gracefully.
	assert.Equal(t, DefaultGangMode, cfg.GangMode)
}

func TestParseConfig_InvalidMatchPolicy(t *testing.T) {
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		MatchPolicy: "least-satisfied", // removed invalid value
	}
	rawBytes, _ := json.Marshal(raw)
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	cfg, err := parseConfig(profile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid MatchPolicy")
	assert.Equal(t, DefaultMatchPolicy, cfg.MatchPolicy)
}

func TestParseConfig_InvalidScheduleTimeoutSeconds(t *testing.T) {
	for _, badVal := range []int32{0, -1, -100} {
		v := badVal
		raw := configv1alpha1.KoordinatorSchedulerConfiguration{ScheduleTimeoutSeconds: &v}
		rawBytes, _ := json.Marshal(raw)
		profile := configv1alpha1.SchedulerProfile{
			Name:   configv1alpha1.SchedulerNameKoordinator,
			Config: &runtime.RawExtension{Raw: rawBytes},
		}
		cfg, err := parseConfig(profile)
		require.Errorf(t, err, "expected error for ScheduleTimeoutSeconds=%d", badVal)
		assert.Contains(t, err.Error(), "invalid ScheduleTimeoutSeconds")
		assert.Equal(t, DefaultTimeoutSecs, cfg.ScheduleTimeoutSeconds, "fallback default should be returned")
	}
}

func TestParseConfig_InvalidDefaultQoSClass(t *testing.T) {
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{DefaultQoSClass: "GOLD"}
	rawBytes, _ := json.Marshal(raw)
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	cfg, err := parseConfig(profile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid DefaultQoSClass")
	assert.Empty(t, cfg.DefaultQoSClass, "fallback default (empty) should be returned")
}

func TestParseConfig_ValidDefaultQoSClass_AllValues(t *testing.T) {
	for _, qos := range []string{"LSE", "LSR", "LS", "BE", ""} {
		raw := configv1alpha1.KoordinatorSchedulerConfiguration{DefaultQoSClass: qos}
		rawBytes, _ := json.Marshal(raw)
		profile := configv1alpha1.SchedulerProfile{
			Name:   configv1alpha1.SchedulerNameKoordinator,
			Config: &runtime.RawExtension{Raw: rawBytes},
		}
		cfg, err := parseConfig(profile)
		require.NoErrorf(t, err, "QoSClass=%q should be valid", qos)
		assert.Equal(t, qos, cfg.DefaultQoSClass)
	}
}

func TestParseConfig_TopologyKeyMappings_ValidLayers(t *testing.T) {
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		TopologyKeyMappings: map[string]string{
			"mycompany.com/rack":  "rackLayer",
			"mycompany.com/block": "blockLayer",
		},
	}
	rawBytes, _ := json.Marshal(raw)
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	cfg, err := parseConfig(profile)
	require.NoError(t, err)
	assert.Equal(t, "rackLayer", cfg.topologyKeyMappings["mycompany.com/rack"])
	assert.Equal(t, "blockLayer", cfg.topologyKeyMappings["mycompany.com/block"])
}

func TestParseConfig_TopologyKeyMappings_InvalidLayer(t *testing.T) {
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		TopologyKeyMappings: map[string]string{
			"mycompany.com/dc": "datacenterLayer", // not a valid Koordinator layer
		},
	}
	rawBytes, _ := json.Marshal(raw)
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	_, err := parseConfig(profile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid topology layer")
}

func TestParseConfig_PartialOverride(t *testing.T) {
	// Only override GangMode; other fields use defaults.
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		GangMode: "NonStrict",
	}
	rawBytes, _ := json.Marshal(raw)
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	cfg, err := parseConfig(profile)
	require.NoError(t, err)
	assert.Equal(t, "NonStrict", cfg.GangMode)
	assert.Equal(t, DefaultMatchPolicy, cfg.MatchPolicy)
	assert.Equal(t, DefaultTimeoutSecs, cfg.ScheduleTimeoutSeconds)
}

// TestParseConfig_ErrorOnLaterFieldReturnsStrictDefaults verifies that parseConfig returns
// pure defaults when a later field fails validation — not a partially-applied mix.
// Regression test for the "fail-open partial state" scenario (N1 fix).
func TestParseConfig_ErrorOnLaterFieldReturnsStrictDefaults(t *testing.T) {
	// GangMode is valid and would be applied to cfg before DefaultQoSClass is checked.
	// Without the N1 fix, cfg would return with GangMode="NonStrict" (user value) despite
	// the error — making "using defaults" semantics inaccurate.
	timeout := int32(90)
	raw := configv1alpha1.KoordinatorSchedulerConfiguration{
		GangMode:               "NonStrict",       // valid; applied before the error
		ScheduleTimeoutSeconds: &timeout,           // valid; applied before the error
		DefaultQoSClass:        "GOLD",             // invalid; triggers error
	}
	rawBytes, _ := json.Marshal(raw)
	profile := configv1alpha1.SchedulerProfile{
		Name:   configv1alpha1.SchedulerNameKoordinator,
		Config: &runtime.RawExtension{Raw: rawBytes},
	}
	cfg, err := parseConfig(profile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid DefaultQoSClass")
	// All fields must be pure defaults — no partial user values must bleed through.
	assert.Equal(t, DefaultGangMode, cfg.GangMode, "GangMode must revert to default on error")
	assert.Equal(t, DefaultMatchPolicy, cfg.MatchPolicy)
	assert.Equal(t, DefaultTimeoutSecs, cfg.ScheduleTimeoutSeconds, "ScheduleTimeoutSeconds must revert to default on error")
	assert.Empty(t, cfg.DefaultQoSClass)
	assert.Nil(t, cfg.topologyKeyMappings)
}
