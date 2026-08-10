# Koordinator Scheduler Backend

<!-- toc -->
- [Overview](#overview)
- [Enabling the backend](#enabling-the-backend)
- [Workload-level annotations](#workload-level-annotations)
  - [<code>scheduling.grove.io/koordinator-quota</code> — ElasticQuota binding](#schedulinggroveiokoordinator-quota--elasticquota-binding)
  - [<code>scheduling.grove.io/koordinator-schedule-timeout-seconds</code> — per-workload gang timeout](#schedulinggroveiokoordinator-schedule-timeout-seconds--per-workload-gang-timeout)
- [Topology-aware scheduling](#topology-aware-scheduling)
- [Not supported](#not-supported)
- [E2E](#e2e)
<!-- /toc -->

This document describes the `koord-scheduler` backend: how Grove workloads map onto
[Koordinator](https://github.com/koordinator-sh/koordinator) gang scheduling, how to
configure the backend, and the workload-level annotations it consumes.

Validated with Koordinator v1.8.0: gang scheduling, gang atomicity, topology-aware placement,
quota and admission paths are exercised by the KGS e2e suite on a kind + KWOK cluster; the
upstream contracts the backend relies on were cross-checked against koordinator `main`.

## Overview

The backend converts each Grove `PodGang` into one Koordinator `PodGroup` CR
(`scheduling.sigs.k8s.io/v1alpha1`) per clique, linked into a single atomic gang via the
`gang.scheduling.koordinator.sh/groups` (GangGroup) annotation. Pods are associated with
their PodGroup through the `gang.scheduling.koordinator.sh/name` annotation and scheduled
by `koord-scheduler`.

| Grove | Koordinator |
|---|---|
| `PodGang.Spec.PodGroups[i]` (one per clique) | one `PodGroup` CR named `{podgang}-{podclique}` |
| `MinReplicas` | `PodGroup.Spec.MinMember` |
| all cliques of a PodGang | one GangGroup (`gang.scheduling.koordinator.sh/groups`) |
| `TopologyConstraint` (PCS level) | `gang.scheduling.koordinator.sh/network-topology-spec` annotation |
| `ClusterTopologyBinding` | Koordinator `ClusterNetworkTopology` named `default` |

PodGroups are owned by the PodGang (garbage-collected on deletion); updates preserve
externally managed metadata and the status written by Koordinator's PodGroupController.
When a coherent rolling update releases a PodGroup's `MinReplicas` to 0, the gang constraints
already recorded on that PodGroup are preserved (decided per PodGroup, so a partial release
is handled correctly).

## Enabling the backend

```yaml
config:
  scheduler:
    defaultProfileName: koord-scheduler   # optional; per-clique schedulerName also works
    profiles:
      - name: default-scheduler
      - name: koord-scheduler
        config:                            # optional KoordinatorSchedulerConfiguration
          gangMode: Strict                 # Strict (default) | NonStrict — failure handling only;
                                           # minMember gates binding in both modes
          matchPolicy: once-satisfied      # once-satisfied (default) | only-waiting | waiting-and-running
          scheduleTimeoutSeconds: 30       # gang Permit-stage wait, >= 1
          defaultQoSClass: ""              # LSE|LSR|LS|BE — injected only when the pod has no
                                           # koordinator.sh/qosClass label of its own
          topologyKeyMappings:             # node-label-key → ClusterNetworkTopology layer name;
            mycompany.com/spine: spineLayer  # usually unnecessary, see Topology below
```

Koordinator must be installed in the cluster (`helm repo add koordinator-sh
https://koordinator-sh.github.io/charts/ && helm upgrade --install koordinator
koordinator-sh/koordinator -n koordinator-system --create-namespace`). The Helm chart
grants the operator RBAC for PodGroups, ElasticQuotas, and ClusterNetworkTopologies only
when the profile is enabled.

## Workload-level annotations

### `scheduling.grove.io/koordinator-quota` — ElasticQuota binding

Binds all pods of the workload to a Koordinator ElasticQuota by injecting the
`quota.scheduling.koordinator.sh/name` pod label:

```yaml
apiVersion: grove.io/v1alpha1
kind: PodCliqueSet
metadata:
  name: my-workload
  annotations:
    scheduling.grove.io/koordinator-quota: team-a
```

- May be set on the PCS (applies to all cliques) or on individual clique templates; when
  both are set they must agree, and all cliques must resolve to the same quota (a gang
  split across quotas would block as a whole).
- Admission validates that the quota exists and is a leaf (not a parent) quota —
  koord-scheduler silently falls back to `koordinator-default-quota` for unknown names,
  so typos would otherwise go unnoticed.
- **The binding is immutable**: adding, removing, or switching it on a live workload is
  rejected, because the label is set at pod creation and existing pods are never
  relabelled — a change would permanently split the gang across quotas. Recreate the
  workload to change its binding.
- Without the annotation, Koordinator's native fallback chain applies (namespace-bound
  quota → `koordinator-default-quota`).

### `scheduling.grove.io/koordinator-schedule-timeout-seconds` — per-workload gang timeout

Overrides the profile-level `scheduleTimeoutSeconds` for one workload (PCS-level only;
the value applies to every PodGroup of the gang):

```yaml
metadata:
  annotations:
    scheduling.grove.io/koordinator-schedule-timeout-seconds: "120"
```

Note: changing the annotation on a live PCS takes effect on the next spec change or
operator restart (metadata-only updates are filtered by the reconcile predicates, the
same limitation as the volcano queue annotation).

## Topology-aware scheduling

With `topologyAwareScheduling.enabled`, the backend implements the grove
`TopologyAwareBackend` interface:

- A `ClusterTopologyBinding` is synced into Koordinator's single `ClusterNetworkTopology`
  named `default` (spec only — status stays owned by koord-scheduler). Non-leaf layer
  names equal the level's node label key; the leaf is Koordinator's built-in
  `NodeTopologyLayer`. Levels below host (numa) cannot be represented and are rejected.
- An administrator-managed `default` CNT is supported instead via
  `schedulerTopologyBindings` (`schedulerName: koord-scheduler`, `topologyReference:
  default`): the backend performs a structural drift check that tolerates custom layer
  names.
- After a successful sync or drift check, the node-label-key → layer-name mapping is
  derived automatically, so `topologyKeyMappings` is only needed for keys outside the
  ClusterTopologyBinding.
- PCS-level `topologyConstraint` maps to `MustGather` (required) / `PreferGather`
  (preferred). koord-scheduler applies a **single** network-topology-spec to the whole
  GangGroup (taken from whichever member pod enters scheduling first), so divergent
  effective constraints across cliques — and PCSG-level constraints — are rejected at
  admission.
- koord-scheduler must run with `--enable-network-topology-manager=true` (off by default in
  the binary; set by the official Helm chart since v1.7.0). Without it every gang carrying a
  network-topology-spec fails PreFilter with "no cluster network topology".
- Only one ClusterTopologyBinding can be synced per cluster (koord-scheduler hardcodes
  the `default` CNT name).

## Not supported

- **MNNVL / ComputeDomain** (requires the NVIDIA DRA driver: ComputeDomain CRs plus
  injected ResourceClaims): koord-scheduler does inherit the upstream DynamicResources
  plugin (vendored kube-scheduler v1.35, DRA GA), but DRA claim allocation combined with
  Koordinator's gang scheduling, Reservation, and DeviceShare machinery is unvalidated, so
  PodCliqueSets that enroll a `grove.io/mnnvl-group` are rejected at admission
  (fail-closed). Update validation is migration-aware — legacy objects with unchanged
  incompatible fields can still be updated or deleted.
- **PCSG (per-scaling-group) topology constraints**: Koordinator has no sub-gang topology
  structure.
- **GPU fine-grained sharing** requires `koordinator.sh/gpu-*` resource names declared
  explicitly in the PodSpec plus Koordinator's device-management components. Whole-GPU
  `nvidia.com/gpu` requests work natively.
- `ReuseReservationRef` emits a warning and is skipped (no producer in Grove today).

## E2E

`operator/e2e/tests/koordinator/` (KGS1–6) runs against an existing cluster with
Koordinator installed and Grove deployed with the `koord-test` skaffold profile; on
clusters without Koordinator the suite skips itself. KGS1–4 cover basic gang scheduling,
gang blocking, and the MNNVL / unknown-quota admission rejections. KGS5 covers the topology
path end to end: it labels the KWOK nodes into racks of 4/3/3, creates a
`ClusterTopologyBinding`, waits for the `default` ClusterNetworkTopology to be synced, and
verifies that a 4-pod `pack.required: rack` gang (one pod per node) lands in the only
4-node rack with a `MustGather` PodGroup spec carrying the derived layer name. KGS6 covers
gang atomicity: with 9 of 10 nodes cordoned, a 3-pod gang that needs a node per pod stays
entirely unbound for a stable window and runs on distinct nodes once capacity returns. See
the package documentation in `operator/e2e/tests/koordinator/setup.go` for prerequisites
(KGS5 additionally needs `--enable-network-topology-manager=true` on koord-scheduler).
