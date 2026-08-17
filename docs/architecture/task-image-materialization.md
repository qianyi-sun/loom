# Task image materialization

## Purpose

Loom must never discover a missing Dockerfile-backed task image after a
contained trial has started. Docker build steps launched through the host daemon
cannot be placed in a packed Slurm job's cgroup, so non-exclusive workers may
pull images but must not build them.

Task image materialization is therefore a control-plane prerequisite, separate
from trial execution. It covers the task's primary Dockerfile and every
Dockerfile-backed sidecar. Prebuilt `docker_image` references remain pull-only
inputs and do not create build work.

## Architecture

The control plane owns a durable, content-addressed materialization row for each
`(task_id, task_checksum, cpu_arch)` tuple. `cpu_arch = "any"` expands to native
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

Ordinary Loom workers never wait for or perform a build. After the scheduler
observes readiness, the trial claim carries the frozen task snapshot and exact
per-component registry digests. The execution worker verifies the bundle and
pulls those digest references—not re-derived tags—before starting the sandbox.
A registry miss after readiness is an infrastructure consistency failure rather
than permission to rebuild on a packed node.

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
append-only publication history before readiness can be committed. Automatic
retries reuse current verified component publications. Stale builders may add
cleanup evidence but cannot publish readiness, so lease loss and later retries
cannot turn earlier manifests into untracked registry leaks.

The builder autoscaler scales from zero using queued build demand. It dispatches
only exclusive Slurm jobs and never converts the existing non-exclusive GB10
trial pool into builders. One allocation may process several jobs sequentially
before exiting after an idle grace period, amortizing image and worker startup.
Each enabled builder policy requires an explicit Slurm reservation; the nodes
may remain part of shared inventory, but a builder receives the whole selected
node temporarily and never overlaps packed trial work. A missing global
execution witness disables scale-up and drains both pending and running builder
allocations.

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
The old worker-side build
fallback remains available to explicit local/CLI workflows; service trial
workers are pull-only once builder readiness gating is active.

## Verification

Tests cover deterministic per-architecture keys, idempotent enqueue, lease
fencing and recovery, trial architecture gating, registry publication, contained
worker pull-only behavior, exclusive Slurm request rendering, autoscaler demand,
and bounded local eviction. Integration tests prove that a trial cannot be
claimed before readiness and becomes claimable immediately after the matching
architecture variant is committed.
