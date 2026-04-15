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
	"context"
	"fmt"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/mnnvl"
)

// validatePodCliqueSet enforces Koordinator-specific constraints on a PodCliqueSet.
// It is called at admission time (webhook) and should return a descriptive error if
// the configuration is fundamentally incompatible with koord-scheduler.
func validatePodCliqueSet(_ context.Context, pcs *grovecorev1alpha1.PodCliqueSet) error {
	// MNNVL (Multi-Node NVLink) relies on NVIDIA DRA ComputeDomain CRDs and is
	// incompatible with Koordinator's device management. Reject at admission.
	if pcs.Annotations[mnnvl.AnnotationAutoMNNVL] == mnnvl.AnnotationAutoMNNVLEnabled {
		return fmt.Errorf(
			"PodCliqueSet %q has annotation %s=%s which enables MNNVL; "+
				"MNNVL is not supported with the koord-scheduler backend. "+
				"Use the kai-scheduler backend for MNNVL workloads, or remove the annotation",
			pcs.Name, mnnvl.AnnotationAutoMNNVL, mnnvl.AnnotationAutoMNNVLEnabled,
		)
	}
	return nil
}
