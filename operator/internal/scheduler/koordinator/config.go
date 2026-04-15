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
	"fmt"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
)

// backendConfig holds the resolved, defaulted configuration for the koordinator backend.
type backendConfig struct {
	GangMode               string
	MatchPolicy            string
	ScheduleTimeoutSeconds int32
	DefaultQoSClass        string
	topologyKeyMappings    map[string]string // user-defined topology key → layer, may be nil
}

// parseConfig unmarshals SchedulerProfile.Config into KoordinatorSchedulerConfiguration
// and applies defaults for any unset fields.
// On any validation error, pure defaults are returned (no partial state) so that callers
// can trust the "using defaults" semantics when parseConfig reports a non-nil error.
func parseConfig(profile configv1alpha1.SchedulerProfile) (backendConfig, error) {
	defaults := backendConfig{
		GangMode:               DefaultGangMode,
		MatchPolicy:            DefaultMatchPolicy,
		ScheduleTimeoutSeconds: DefaultTimeoutSecs,
	}
	if profile.Config == nil {
		return defaults, nil
	}

	var raw configv1alpha1.KoordinatorSchedulerConfiguration
	if err := json.Unmarshal(profile.Config.Raw, &raw); err != nil {
		return defaults, fmt.Errorf("failed to unmarshal KoordinatorSchedulerConfiguration: %w", err)
	}

	// Work on a separate copy so that validation errors leave defaults untouched.
	cfg := defaults
	if raw.GangMode != "" {
		switch raw.GangMode {
		case "Strict", "NonStrict":
			cfg.GangMode = raw.GangMode
		default:
			return defaults, fmt.Errorf("invalid GangMode %q: valid values are Strict, NonStrict", raw.GangMode)
		}
	}
	if raw.MatchPolicy != "" {
		switch raw.MatchPolicy {
		case "once-satisfied", "only-waiting", "waiting-and-running":
			cfg.MatchPolicy = raw.MatchPolicy
		default:
			return defaults, fmt.Errorf("invalid MatchPolicy %q: valid values are once-satisfied, only-waiting, waiting-and-running", raw.MatchPolicy)
		}
	}
	if raw.ScheduleTimeoutSeconds != nil {
		if *raw.ScheduleTimeoutSeconds < 1 {
			return defaults, fmt.Errorf("invalid ScheduleTimeoutSeconds %d: must be >= 1", *raw.ScheduleTimeoutSeconds)
		}
		cfg.ScheduleTimeoutSeconds = *raw.ScheduleTimeoutSeconds
	}
	switch raw.DefaultQoSClass {
	case "", "LSE", "LSR", "LS", "BE":
		cfg.DefaultQoSClass = raw.DefaultQoSClass
	default:
		return defaults, fmt.Errorf("invalid DefaultQoSClass %q: valid values are LSE, LSR, LS, BE", raw.DefaultQoSClass)
	}
	if len(raw.TopologyKeyMappings) > 0 {
		cfg.topologyKeyMappings = make(map[string]string, len(raw.TopologyKeyMappings))
		for k, v := range raw.TopologyKeyMappings {
			switch v {
			case "hostLayer", "rackLayer", "blockLayer":
				cfg.topologyKeyMappings[k] = v
			default:
				return defaults, fmt.Errorf("invalid topology layer %q for key %q in topologyKeyMappings: valid layers are hostLayer, rackLayer, blockLayer", v, k)
			}
		}
	}
	return cfg, nil
}
