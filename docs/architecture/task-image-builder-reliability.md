# Task-image builder reliability

## Status and scope

This design hardens the active Phase 1 native task-image builders without
activating the Phase 2 rootless provider. It replaces the pod-lifecycle-bound
global execution witness transport, separates runtime Kubernetes authority
from rollout authority, and makes the documented local-storage floor a hard
pre-claim admission requirement.

The existing durable materialization identity, lease fencing, immutable
registry publication, exclusive Slurm reservations, and registry retention
model remain unchanged.

## Invariants

1. A missing, stale, malformed, incorrectly signed, or wrongly scoped global
   execution witness must prevent new trial and builder capacity and drain any
   capacity that the legacy supervisor owns.
2. Witness readers must never receive `pods/exec` authority and supervisor
   processes must never receive the protected rollout kubeconfig.
3. A builder with an unavailable storage probe or less than the configured
   free-space floor must exit before claiming a materialization.
4. Automatic cleanup may remove only resources proven to be Loom-managed.
   Broad Docker pruning and inference from an unlabelled image name are not
   permitted.
5. A failed builder allocation must not cause a 30-second resubmission loop.
6. Existing ready materializations, immutable registry digests, and historical
   publication projections remain valid. Historical append-only evidence is
   never fabricated.

## Stable witness publication

The capacity-manager pod gains a dedicated `witness-publisher` sidecar. The
sidecar uses the existing database-backed Ed25519 exporter to create fresh
`gb10` and `oldlab` exports together every ten seconds. It atomically patches
the data keys `gb10.json` and `oldlab.json` in the stable ConfigMap
`loom-global-execution-witness-v1` in `loom-dev`.

The ConfigMap is not a trust authority. Consumers continue to validate the
pinned public-key fingerprint, signature, canonical digest, authority, pool,
epoch, execution state, ceiling, and expiry. The object is merely a durable
transport name. If publication stops, its 30-second witnesses expire and the
existing fail-closed behavior takes effect.

Only the sidecar receives a projected Kubernetes service-account token. The
main manager container retains `automountServiceAccountToken: false` and does
not mount that token. A namespace Role permits the publisher service account
to `get` and `patch` only the named ConfigMap.

External supervisors replace `kubectl exec deployment/loom-capacity-manager`
with a bounded, shell-free `kubectl get configmap ... -o json`. They extract
only the architecture-specific data key and pass the bytes through the
existing cryptographic parser. The old deployment-exec source remains
accepted for a transition release but is absent from every active profile and
is removed after convergence evidence proves the stable source.

## Runtime Kubernetes authority

Both Slurm controllers use a dedicated kubeconfig at
`/var/lib/loom-staging-rollout/external-supervisor.kubeconfig`. The existing
publisher installs the same scoped service-account identity on OLDLAB and
GB10. Its permissions are limited to the dedicated staging database Secret,
the objects required for the database port-forward, and `get` on the exact
`loom-dev/loom-global-execution-witness-v1` ConfigMap.

The protected rollout kubeconfig remains at
`/var/lib/loom-staging-rollout/kubeconfig` and is never referenced by a
supervisor unit. The temporary exact-pod `pods/exec` Role and RoleBinding are
deleted during convergence after both supervisors prove successful reads.

## Storage admission and owned cleanup

Managed-image cleanup returns structured evidence containing the Docker root,
final free bytes, required free bytes, probe availability, and cleanup error
count. Before TTL and pressure eviction, it removes stopped containers only
when the container itself or its referenced image carries one of Loom's
managed labels:

- `loom.task-image=true`
- `loom.task-sidecar=true`
- `loom.trial-cache=true`

Running containers and unlabelled resources are never removed. Container
volumes are retained. Existing TTL pruning and oldest-first pressure eviction
then operate on managed images, followed by a fresh filesystem probe.

The exclusive builder runs this preparation before constructing its control
plane client or asking for a claim. Probe failure or free space below
`LOOM_WORKER_TASK_IMAGE_MIN_FREE_GB` raises a safe fatal storage-admission
error and exits nonzero. No materialization lease is consumed, so task state
remains queued.

The builder autoscaler applies a five-minute cooldown after the latest failed
allocation in the same environment and pool. Demand remains durable and is
retried automatically after the cooldown, while a persistent storage or
runtime failure cannot submit one job every supervisor tick.

Unlabelled historical Docker objects are outside automatic authority. Site
convergence inventories them and removes only operator-verified Loom objects.
The code never broadens ownership based solely on repository or tag spelling.

## Rollout order

1. Merge and release one immutable candidate containing publisher, reader,
   storage admission, and cooldown behavior.
2. Apply the capacity-control-plane manifests and prove that both ConfigMap
   keys refresh and validate for longer than two witness TTLs.
3. Publish the scoped external-supervisor kubeconfig to both Slurm controllers
   at the dedicated path and verify its effective permissions.
4. Apply staging environment state so all four supervisors use the ConfigMap
   source and dedicated kubeconfig.
5. Verify OLDLAB and GB10 trial and builder supervisors reconcile successfully
   with an empty queue.
6. Perform owned GB10 legacy cleanup until storage admission passes, without
   submitting a canary task.
7. Delete the temporary pod-name-bound witness RBAC and prove that
   `pods/exec` is denied to the external supervisor identity.

Any failed step leaves scale-up closed. The registry and existing ready
digests continue serving trials independently of builder availability.

## Verification

Automated tests cover ConfigMap rendering and publisher RBAC, bounded atomic
publication, ConfigMap reader validation and malformed input, absence of
`pods/exec` from active staging policy, dedicated kubeconfig paths, stopped
managed-container cleanup, hard pre-claim storage rejection, successful
admission, and failed-allocation cooldown.

Live acceptance requires fresh signed witnesses for both pools, successful
supervisor results on both controllers, exact Slurm reservations and QoS,
storage at or above the configured floor, zero active builder jobs with an
empty queue, and denial of external-supervisor `pods/exec` authority.
