# Task image materialization

## Purpose

Loom must never discover a missing Dockerfile-backed task image after a
contained trial has started. Docker build steps launched through the host daemon
cannot be placed in a packed Slurm job's cgroup, so non-exclusive workers may
pull images but must not build them.

Task image materialization is therefore a control-plane prerequisite, separate
from trial execution. It covers the task's primary Dockerfile and every
Dockerfile-backed sidecar. A declared Dockerfile is authoritative when a legacy
config also contains `docker_image`; otherwise prebuilt `docker_image`
references remain pull-only inputs and do not create build work.

## Architecture

The control plane owns a durable, content-addressed materialization row for each
`(task_id, task_checksum, cpu_arch)` tuple. `task_checksum` is the
NUL-delimited relative-path plus file-bytes digest from
`loom_benchmarks.util.sha256_of_dir` (not `dirhash`). `cpu_arch = "any"` expands to native
`x86_64` and `arm64` variants. The row snapshots the task config and bundle
source so a later task update cannot change an in-flight build.

Benchmark registration enqueues the rows asynchronously. Trial submission is
the correctness backstop: it idempotently ensures the current task's rows exist
and links the trial to every applicable architecture variant. The scheduler may
claim a trial on an architecture only when that architecture's materialization
is ready. Tasks containing no Dockerfiles need no materialization row.

An independently autoscaled task-image builder pool consumes the queue. Builder
jobs run with exclusive Slurm allocations, one Docker build at a time per host
daemon. A builder materializes the immutable bundle, verifies its checksum and
native architecture, builds every Dockerfile-backed component, pushes
architecture-qualified content-addressed tags to the shared registry, records
the returned immutable manifest digests, and marks the row ready. Builders use
lease epochs and heartbeats so a
stale process cannot publish readiness after its lease has been reclaimed.

The exclusive host-daemon pool is the active Phase 1 implementation and remains
the rollback path. The Phase 2 allocation-scoped rootless provider is a separate
contract: it renders a native-architecture Slurm job held for durable grant
binding, without a reservation, node pin, `--exclusive`, host runtime socket,
credential, or broad environment export. Its checked-in policy is inert input;
no route, timer, supervisor, or autoscaler constructs the provider from it.

Ordinary Loom workers never wait for or perform a task-authored Dockerfile
build. After the scheduler observes readiness, the trial claim carries the
frozen task snapshot and exact per-component registry digests. The execution
worker verifies the bundle and pulls those digest references—not re-derived
tags—before starting the sandbox. A registry miss after readiness is an
infrastructure consistency failure rather than permission to rebuild on a
packed node.

Agent-adapter `install_script` layering is the pre-existing trial-image cache
subsystem, not a task-image component: its identity depends on the selected
trial agent as well as the resolved task-image digest. Containment-required
service workers may only pull those cached layers; moving that separate cache
to its own durable materialization queue is outside this task-image lifecycle.

## State and failure handling

Materializations move through `queued`, `claimed`, `running`, `ready`, `failed`,
`retiring`, and `retired`. Expired `claimed` or `running` leases return to
`queued`. Retryable
failures use bounded backoff and a maximum attempt count; deterministic build
failures become `failed` and keep dependent trials queued with an explicit
image-materialization reason. An operator retry can enqueue a new attempt
without changing the content key; registering a new task checksum creates a
new immutable materialization identity rather than resetting bounded attempts.

Registry publication cannot be atomic across multiple component manifests.
When a component is pushed, Loom records the immutable digest immediately in an
append-only `task_image_publication_evidence` row bound to the exact
materialization attempt, lease epoch, component, registry image, and builder
before readiness can be committed. The materialization row's JSON publication
history is a compatibility projection, not the audit authority. An exact replay
within one attempt is idempotent, while an identical OCI digest from a later
lease remains distinct evidence. Automatic retries reuse current verified
component publications. Stale builders may add cleanup evidence but cannot
publish readiness, so lease loss and later retries cannot turn earlier
manifests into untracked registry leaks.

The builder autoscaler scales from zero using queued build demand. It dispatches
only exclusive Slurm jobs and never converts the existing non-exclusive GB10
trial pool into builders. One allocation may process several jobs sequentially
before exiting after an idle grace period, amortizing image and worker startup.
Each enabled builder policy requires an explicit Slurm reservation; the nodes
may remain part of shared inventory, but a builder receives the whole selected
node temporarily and never overlaps packed trial work. A missing global
execution witness disables scale-up and drains both pending and running builder
allocations.

## Reliable witness transport and supervisor authority

The capacity-manager `witness-publisher` sidecar makes a database-backed,
Ed25519-signed export for `gb10` and `oldlab` together every ten seconds. It
atomically patches `gb10.json` and `oldlab.json` in the stable
`loom-dev/loom-global-execution-witness-v1` ConfigMap. The ConfigMap is only a
durable transport name, never a trust authority: each reader still checks the
pinned public-key fingerprint, signature, canonical digest, authority, pool,
epoch, execution state, ceiling, and expiry. A missing, malformed, stale, or
wrongly scoped export therefore fails closed; the 30-second expiry closes
scale-up and drains capacity owned by the legacy supervisor.

The manager container does not receive the publisher token. Only the sidecar
mounts a projected token, and its `loom-capacity-witness-publisher` service
account can only `get` and `patch` that one ConfigMap in `loom-dev`. External
trial and builder supervisors read the architecture-specific key through a bounded,
shell-free `kubectl get configmap ... -o json`, then give those bytes to the
existing cryptographic parser. Their dedicated runtime credential is
`/var/lib/loom-staging-rollout/external-supervisor.kubeconfig`; it may read the
dedicated staging database Secret, perform the scoped database port-forward,
and `get` that exact ConfigMap. It has no `pods/exec` authority. The protected
rollout credential at `/var/lib/loom-staging-rollout/kubeconfig` is used only
by the rollout publisher and is never referenced by a supervisor unit.

The old `deployment/loom-capacity-manager` exec source is transition-only. It
is absent from every active profile and the temporary exact-pod `pods/exec`
Role and RoleBinding are removed only after both controllers have demonstrated
successful ConfigMap reads. A later failure remains closed rather than falling
back to exec.

Staging profile and supervisor activation is a protected, broker-owned rollout:
`loom-staging-rollout --env staging start` creates the required rollout
envelope and converges both controllers. Direct `loom admin environment-state
apply` is not a valid staging mutation path. Rollback first closes supervisor
scale-up, then uses only recorded immutable prior profile, credential, manifest,
and RBAC artifacts; it never restores exec automatically.

## Hard local-storage admission

Before it constructs a control-plane client or requests a materialization
claim, an exclusive builder performs owned cleanup and records structured
evidence: Docker root, final free bytes, required free bytes, probe
availability, and cleanup error count. A missing probe or free space below
`LOOM_WORKER_TASK_IMAGE_MIN_FREE_GB` is a fatal storage-admission error. The
allocation exits nonzero without consuming a lease, so the materialization
remains queued. The same preparation and admission check runs again after each
claim is processed, before another claim can be requested.

Cleanup first removes only stopped (`created`, `dead`, or `exited`) containers
where the container itself or its referenced image carries one of
`loom.task-image=true`, `loom.task-sidecar=true`, or
`loom.trial-cache=true`. It never removes running containers, container
volumes, or any unlabelled resource. TTL pruning and oldest-first pressure
eviction then apply only to managed images, followed by a fresh filesystem
probe. In particular, Docker system prune, broad container prune, and ownership
inference from a repository or tag spelling are outside Loom's authority.

The autoscaler retains queued demand, but after the latest failed allocation in
the same `(environment, pool_name)` it waits exactly five minutes before
submitting again. The cooldown is neither global nor per architecture: a
failure in one environment or builder pool does not delay another. This avoids
one failed storage/runtime allocation every supervisor tick while allowing an
automatic retry when the cooldown ends.

## Retention

Local builder and execution-node copies are caches. Loom labels all managed task
images and evicts them by creation-age TTL with a free-space backstop. Registry
artifacts remain pinned while referenced by the current registered task version
or a nonterminal trial. Unreferenced materializations receive a grace
period before registry deletion and garbage collection. Content-addressed tags
make deletion safe: a later reference recreates the same queue key, rebuilds
the components, and records fresh immutable digest evidence before scheduling.

## Rollout safety

The schema migration backfills materializations and links for existing
nonterminal Dockerfile-backed trials. Claim SQL also treats an unlinked trial as
claimable only when its current task definition contains no Dockerfile, closing
mixed-version rollout races. The queue and builder endpoints remain inert until
the builder policy and registry are configured. Registration may enqueue while
the policy is disabled, but trials requiring an unbuilt image remain queued
rather than failing after claim. Existing prebuilt-image tasks are unaffected.
The old task-Dockerfile build fallback remains available to explicit local/CLI
workflows (`loom run`); service trial workers are pull-only for task-authored
Dockerfile components. `loom service up --environment local` runs a loopback
registry plus a laptop Docker builder sidecar so native-arch Dockerfile
materializations can become `ready` without Slurm. That sidecar is not the
production exclusive-builder design. Until it is present and has published a
digest, Dockerfile-backed local batches stay queued.

The rootless provider remains disabled until the allocation executor, node
guard, renewable registry-credential broker, and publication acceptance path
have each passed their activation evidence. Removing those blockers is a later
release step that must wire the provider deliberately; loading the Phase 2
policy in this increment cannot submit or release a Slurm job.

## Verification

Tests cover deterministic per-architecture keys, idempotent enqueue, lease
fencing and recovery, trial architecture gating, registry publication, contained
worker pull-only behavior, exclusive Slurm request rendering, autoscaler demand,
and bounded local eviction. They additionally cover bounded atomic ConfigMap
publication, reader validation, the absence of active `pods/exec` authority,
the dedicated supervisor kubeconfig, stopped managed-container cleanup, hard
pre-claim rejection and successful storage admission, and the exact
five-minute failed-allocation cooldown. Integration tests prove that a trial
cannot be claimed before readiness and becomes claimable immediately after the
matching architecture variant is committed.
