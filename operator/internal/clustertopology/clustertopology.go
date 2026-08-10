// Copyright 2025 The Grove Authors.
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

package clustertopology

import (
	"context"
	"fmt"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SynchronizeTopology synchronizes scheduler-specific topology resources at operator startup.
// Lists all existing ClusterTopologyBinding resources and ensures backend topologies exist for each.
// Called before controllers start to avoid races with PCS reconciliation.
//
// A sync failure for one ClusterTopologyBinding is logged and skipped rather than aborting startup:
// the controller retries and reports it in status, and crashing would take every controller and
// webhook down over one misconfigured binding. Only list failures are fatal.
func SynchronizeTopology(ctx context.Context, cl client.Client, logger logr.Logger, backends map[string]scheduler.TopologyAwareBackend) error {
	ctList := &grovecorev1alpha1.ClusterTopologyBindingList{}
	if err := cl.List(ctx, ctList); err != nil {
		return fmt.Errorf("failed to list ClusterTopologyBinding resources: %w", err)
	}
	for i := range ctList.Items {
		ct := &ctList.Items[i]
		schedulerRefMap := BuildSchedulerReferenceMap(ct.Spec.SchedulerTopologyBindings)

		for backendName, tasBackend := range backends {
			if ref, isExternallyManaged := schedulerRefMap[backendName]; isExternallyManaged {
				// Externally-managed resources are administrator-owned; run the drift check at startup
				// too so backends that derive state from it (koordinator topology-key mapping) are primed.
				if _, _, _, err := tasBackend.CheckTopologyDrift(ctx, cl, ct, *ref); err != nil {
					logger.Error(err, "Startup topology drift check failed; the ClusterTopologyBinding controller will retry",
						"clusterTopologyBinding", ct.Name, "backend", backendName)
				}
				continue
			}
			if err := tasBackend.SyncTopology(ctx, cl, ct); err != nil {
				logger.Error(err, "Startup topology sync failed; the ClusterTopologyBinding controller will retry and report status",
					"clusterTopologyBinding", ct.Name, "backend", backendName)
			}
		}
		logger.Info("Synchronized backend topologies for ClusterTopologyBinding", "name", ct.Name)
	}
	return nil
}

// GetClusterTopologyLevels retrieves the TopologyLevels from the specified ClusterTopologyBinding resource.
func GetClusterTopologyLevels(ctx context.Context, cl client.Client, name string) ([]grovecorev1alpha1.TopologyLevel, error) {
	clusterTopology := &grovecorev1alpha1.ClusterTopologyBinding{}
	if err := cl.Get(ctx, client.ObjectKey{Name: name}, clusterTopology); err != nil {
		return nil, err
	}
	return clusterTopology.Spec.Levels, nil
}

// BuildSchedulerReferenceMap builds a map from scheduler name to SchedulerTopologyReference pointer.
func BuildSchedulerReferenceMap(refs []grovecorev1alpha1.SchedulerTopologyBinding) map[string]*grovecorev1alpha1.SchedulerTopologyBinding {
	m := make(map[string]*grovecorev1alpha1.SchedulerTopologyBinding, len(refs))
	for i := range refs {
		m[refs[i].SchedulerName] = &refs[i]
	}
	return m
}
