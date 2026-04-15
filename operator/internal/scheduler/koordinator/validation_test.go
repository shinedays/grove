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
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/mnnvl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newPCS(annotations map[string]string) *grovecorev1alpha1.PodCliqueSet {
	return &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pcs",
			Namespace:   "default",
			Annotations: annotations,
		},
	}
}

func TestValidatePodCliqueSet_MNNVLEnabled_Rejected(t *testing.T) {
	pcs := newPCS(map[string]string{
		mnnvl.AnnotationAutoMNNVL: mnnvl.AnnotationAutoMNNVLEnabled,
	})
	err := validatePodCliqueSet(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MNNVL is not supported with the koord-scheduler backend")
}

func TestValidatePodCliqueSet_MNNVLDisabled_Accepted(t *testing.T) {
	pcs := newPCS(map[string]string{
		mnnvl.AnnotationAutoMNNVL: "disabled",
	})
	err := validatePodCliqueSet(context.Background(), pcs)
	assert.NoError(t, err)
}

func TestValidatePodCliqueSet_NoMNNVLAnnotation_Accepted(t *testing.T) {
	pcs := newPCS(nil)
	err := validatePodCliqueSet(context.Background(), pcs)
	assert.NoError(t, err)
}
