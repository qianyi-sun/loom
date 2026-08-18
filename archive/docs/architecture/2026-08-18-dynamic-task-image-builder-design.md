# Dynamic, allocation-contained task-image builders

**Status:** Proposed permanent design  
**Date:** 2026-08-18  
**Target branch:** `dev`

## Decision

Loom will keep task-image materialization as a durable control-plane
prerequisite, but it will replace host-Docker, exclusive-node builders with an
allocation-scoped rootless BuildKit executor.

Builder capacity will use ordinary, dynamically placed Slurm allocations. It
will not require a permanent node reservation, a fixed node, or `--exclusive`.
Builds may queue while the cluster is fully occupied. Scheduling policy will
prevent new trial work from starving a queued builder, but it will not promise
an immediate or fixed-latency build start.

The existing exclusive backend and its provisioned capacity are a transitional
safety mechanism. They remain a rollback candidate until the replacement
passes real-cluster acceptance on both native architectures, but may serve as a
rollback only after their own protected activation has been proven. Their
reservations are removed only as a separate, explicitly approved operational
action after that acceptance.

## Context

Loom must materialize every Dockerfile-backed primary task image and sidecar
before a contained trial starts. Registration normally creates this work;
trial submission is the idempotent correctness backstop. Trial workers receive
immutable registry digests and remain pull-only.

The current builder starts a Docker client in a Slurm allocation but mounts the
host's rootful Docker socket. Dockerfile `RUN` containers are therefore created
by a daemon that already lives outside the allocation cgroup:

```text
Slurm allocation cgroup: builder worker -> Docker client
Host service cgroup:      rootful dockerd -> BuildKit/RUN containers
```

Slurm bounds the client, not the actual build processes. `--exclusive` prevents
those unaccounted processes from overlapping other Slurm jobs, but it does not
contain them and does not protect non-Slurm host services. The permanent
reservation added for that exclusive backend also contradicts the intended
scale-to-zero, dynamically allocated capacity model.

The root cause is not that image building inherently requires a dedicated or
exclusive machine. The root cause is delegating builds to a pre-existing
privileged host daemon outside the allocation's process tree.

## Goals

- Put the builder daemon, every Dockerfile process, and every helper beneath
  the requesting Slurm allocation cgroup.
- Share eligible nodes with trial jobs without permanent idle capacity.
- Preserve registration-time prebuilding, submission-time backstop creation,
  native `x86_64` and `arm64` materialization, lease fencing, pull-by-digest
  execution, and retained publication evidence.
- Prevent continuous trial arrivals from starving a queued builder.
- Treat task Dockerfiles and build contexts as hostile user-space input.
- Bound CPU, memory, PIDs, devices, I/O, temporary storage, wall time, network,
  and credential exposure.
- Leave no builder processes, mounts, credentials, or unbounded disk cache
  after an allocation exits.
- Fail before claiming work when containment cannot be proven.
- Keep deployment authority host-local and make release rehearsal validation
  independent of artifacts that the rollout has not created yet.
- Provide a reversible migration and objective real-cluster acceptance.

## Non-goals

- A guaranteed image-build start latency. Such a guarantee requires reserved
  idle capacity or preemption and conflicts with the selected utilization
  policy.
- Emulated cross-architecture builds. Production materializations are built on
  the matching native architecture.
- Moving the separate agent-install trial-image cache into this lifecycle.
- Protecting against an unknown host-kernel exploit. The rootless executor
  removes host-root authority and hardens the shared-kernel boundary; a threat
  model that includes kernel compromise requires a microVM environment
  provider.
- Replacing Slurm or the registry.
- Treating a source key as proof of reproducibility when a Dockerfile downloads
  mutable external content. The published image digest is the execution
  authority.

## Alternatives considered

### Allocation-scoped rootless BuildKit — selected

Start a fresh, unprivileged BuildKit daemon directly in every builder
allocation. User, mount, PID, and network namespaces isolate the build; the
Slurm cgroup and job-scoped storage enforce resource limits. This is closest to
Loom's existing Dockerfile behavior while removing the host Docker daemon from
the build path.

### Rootless Podman or Buildah

A daemonless engine can also inherit the allocation cgroup, but it introduces
more Dockerfile-compatibility, caching, metadata, and publication differences.
It is a viable later executor, not the lowest-risk permanent migration.

### Rootful allocation-local BuildKit

Explicit cgroup-parent propagation can improve containment, but the daemon and
its helpers retain host privilege. Correctness then depends on more privileged
daemon and storage configuration, with a larger blast radius.

### Dedicated builder service or nodes

This provides strong operational separation and predictable latency, but holds
capacity away from trials and adds a second compute fleet. It is appropriate
only if Loom later adopts a build-start SLA that justifies that cost.

## Trust model

Trusted components are:

- the pinned Loom builder release and its installed launcher;
- the pinned rootless BuildKit, RootlessKit, snapshotter, and network helpers;
- the Slurm controller, cgroup/prolog/epilog configuration, and the narrow
  build-environment provider;
- the Loom control plane and its materialization state machine; and
- the registry authentication, immutability, and retention controls.

Untrusted inputs are:

- Dockerfiles, build contexts, build arguments, and task-supplied files;
- processes created by Dockerfile instructions; and
- public registries and package endpoints contacted during a build.

The design assumes untrusted build code may consume all available resources,
fork or daemonize, ignore termination, inspect `/proc`, probe mounted paths,
scan networks, attempt to reach node-local services, and search for
credentials. It must remain bounded even when a trial is running concurrently.

Rootless means that apparent root inside the build's user namespace maps to an
unprivileged identity on the host. Rootless is one layer, not the complete
security contract. Cgroups, namespaces, seccomp, `no_new_privileges`, mount
policy, network policy, storage quotas, and cleanup are all mandatory.

## Architecture

```text
Task registration / trial submission
                |
                v
    Durable materialization service
                |
                v
    Builder capacity reconciler
                |
                | ordinary architecture-constrained sbatch
                v
+---------- shared Slurm node / allocation cgroup ----------+
| allocation supervisor                                     |
|  +-- containment and release validation                   |
|  +-- rootless BuildKit daemon                             |
|  +-- Dockerfile RUN processes and helpers                 |
|  +-- job-scoped storage, network, and credentials         |
+----------------------------+-------------------------------+
                             | staged push
                             v
                  immutable registry digest
                             |
                             v
                   materialization ready
                             |
                             v
              trial worker pulls exact digest
```

### Materialization service

The control plane owns the durable build intent, attempt leases, component
publication history, readiness transition, retirement, and retry policy. A
trial is eligible for worker claim only when every required component for the
worker's native architecture is ready.

Registration asynchronously ensures materializations. Trial submission repeats
the same operation idempotently so missed or mixed-version registration cannot
allow a trial to bypass the gate. A failed build is represented as a
materialization failure, not as a trial that ran without LLM calls or valid
evidence.

### Build environment provider

Capacity management is separated from image-building mechanics by a narrow
provider contract. The provider owns only:

- capability and containment validation;
- allocation submission, inspection, cancellation, and observed state;
- architecture and resource request rendering; and
- delivery of a one-use job grant without exposing it in Slurm arguments or
  exported environment metadata.

The initial `SlurmBuildEnvironmentProvider` submits ordinary jobs. A future
microVM or Kubernetes provider can implement the same contract without changing
materialization semantics. Loom does not need to adopt a broad external
"environment provider" abstraction for trial execution to gain this boundary.

### Allocation supervisor and BuildKit executor

The installed supervisor runs directly under `sbatch`/`srun`; it is not started
with the host Docker socket and is not itself launched by a host container
daemon. It validates the allocation, creates job-private runtime state, obtains
short-lived credentials, starts rootless BuildKit, claims work, and supervises
cleanup.

The BuildKit executor accepts a frozen task snapshot, native platform, locked
build policy, and component set. It returns immutable component digests and
provenance. It does not own retry state, scheduling, or trial eligibility.

### Artifact publisher and retention controller

Each pushed component is recorded immediately against the fenced attempt.
Readiness is committed only after the registry independently confirms every
immutable digest. The retention controller keeps referenced publications,
retires unreferenced ones after a grace period, and garbage-collects partial or
abandoned attempts.

### Trial scheduler and workers

The scheduler carries the frozen task snapshot and exact component digests in
the trial execution grant. Service workers verify the bundle and pull those
digests. A missing artifact after readiness is an infrastructure consistency
failure; a trial worker never rebuilds it.

## Materialization identity and provenance

The permanent identity is derived from the image-affecting input rather than
from scheduling details:

```text
SHA256(
  canonical task snapshot and component build specifications,
  task bundle checksum,
  native CPU architecture,
  explicit build-policy epoch
)
```

The build-policy epoch is bumped deliberately when builder semantics or a
security policy requires rematerialization. The exact BuildKit and helper
binary digests are recorded as provenance but do not silently invalidate the
entire catalog after every maintenance release.

The first fenced resolver for a materialization records base-image references
as immutable digests. Retries reuse that locked base set. Mutable downloads
performed by arbitrary `RUN` instructions cannot generally be made
reproducible; their consequence is captured in the immutable output digest and
provenance rather than hidden behind a claim of source reproducibility.

Materialization identity may be shared by tasks with identical canonical build
input. Task versions and trials link to the materialization instead of owning
its artifact. Existing task-scoped rows can migrate without changing current
execution grants: new rows use the new key version, and old rows remain readable
until their references retire.

Provenance records at least:

- materialization key version and build-policy epoch;
- task snapshot and bundle digests;
- native platform and all resolved base-image digests;
- builder release and BuildKit/helper binary digests;
- Slurm cluster and job identifiers, without secrets;
- network-policy version;
- timestamps, attempt/lease epoch, and component output digests; and
- containment-evidence version and result.

The output digest, not a mutable tag, is the only execution reference.

## End-to-end lifecycle

1. Registration validates and freezes the task bundle, then ensures one
   materialization per required native architecture. `cpu_arch = "any"`
   creates `x86_64` and `arm64` intents.
2. Trial submission idempotently ensures and links the same intents. The trial
   remains visibly blocked on `task_image_materialization`, not failed.
3. The capacity reconciler observes queued demand and submits at most the
   policy's bounded number of ordinary Slurm builder jobs.
4. The allocation supervisor proves containment before it asks for a claim.
5. The supervisor exchanges its one-use job grant for a short-lived
   `task-image:build` control-plane credential and an attempt-scoped registry
   push credential.
6. The builder claims a matching architecture row with a lease epoch, fetches
   the frozen bundle through a time-limited object URL, and verifies its digest.
7. Base references are locked if this is the first attempt. BuildKit builds
   every Dockerfile-backed component using the native platform and restricted
   policy.
8. Each component is pushed to an attempt-specific staging reference. The
   returned manifest digest and provenance are appended immediately.
9. The control plane verifies the registry manifests and atomically marks the
   complete component set ready. A stale lease cannot perform this transition.
10. A matching trial worker receives exact digests, pulls them, verifies the
    frozen bundle, and only then starts trial execution and evidence creation.
11. One allocation may claim further rows sequentially. It exits after a short
    idle grace period and leaves no durable node-local state.

## Dynamic Slurm scheduling and starvation policy

Builder jobs request a fixed, reviewed resource profile and render:

- one node and one task;
- native architecture and `loom_rootless_buildkit` capability constraints;
- positive CPU, memory, PID, I/O, temporary-storage, and wall-time limits; and
- a dedicated builder QoS with bounded submitted and running job counts.

They do not render:

- `--exclusive`;
- `--reservation`;
- `--nodelist` or a permanent `allowed_nodes` pin; or
- access to `/var/run/docker.sock` or another host runtime.

The builder QoS gives pending builders priority over newly submitted trial
capacity but does not preempt running trials. Slurm's scheduler may make a
temporary earliest-start plan for a pending builder; that is normal dynamic
scheduling, not a permanent named reservation.

Long-lived trial worker jobs require an application-level starvation rule:

1. New builder demand immediately suppresses further trial-pool scale-up on
   that architecture.
2. When oldest queue age crosses a soft threshold, the capacity arbiter marks
   enough reusable trial workers to drain after their current trial to satisfy
   the builder's resource profile.
3. Draining workers receive no new trial claim and exit normally; active trials
   are not cancelled.
4. Drain pressure is released when the builder starts or demand disappears.

Therefore continuous new trial arrivals cannot keep a long-lived worker alive
forever ahead of the builder. If every eligible resource is occupied by active
trials, the builder still waits for one to finish. A hard start-time bound would
require a later, explicit preemption or reservation policy.

Builder concurrency is capped in the other direction so a registration burst
cannot starve trials. The initial production policy is one builder allocation
per architecture, one materialization at a time per allocation.

## Eligible-node contract

A node advertises `loom_rootless_buildkit` only while all of these properties
are true:

- cgroup v2 is active;
- Slurm uses `task/cgroup` and `proctrack/cgroup`;
- CPU, memory, device, and swap constraints are enabled and proven;
- delegated `pids` and `io` controllers can enforce limits beneath the job
  cgroup without permitting movement into its parent;
- unprivileged user namespaces and approved subordinate UID/GID mappings are
  available to a dedicated builder operating-system identity;
- pinned RootlessKit, BuildKit, snapshotter, and network helpers are installed
  from the trusted release;
- node-local scratch supports a hard per-job quota and deterministic cleanup;
  and
- the restricted build-egress path is healthy.

Missing or changed prerequisites remove the feature. The allocation also
revalidates its observed cgroup and runtime state before claiming work. A stale
feature label therefore fails closed rather than weakening containment.

The current OLDLAB `ConstrainRAMSpace=no` and GB10 `ConstrainCores=no` states
are incompatible with non-exclusive activation. They must be corrected and
read back successfully before either cluster is eligible.

## Allocation containment contract

Before claiming work, the supervisor must prove:

1. its process belongs to the expected Slurm job cgroup;
2. requested CPU, memory, device, PID, and I/O limits are effective;
3. its writable delegated subtree cannot move a process to the allocation
   parent or another job;
4. job storage is empty, local, quota-limited, and inaccessible to the build
   mount namespace except where explicitly projected;
5. no host Docker/containerd socket, host network, cluster credential, or
   unsafe host mount is visible;
6. installed executables match the pinned release manifest; and
7. a probe build places BuildKit, its executor, every `RUN` process, the
   snapshotter, and network helpers beneath the allocation cgroup.

BuildKit runs with user, mount, PID, and network namespaces, a restrictive
seccomp profile, `no_new_privileges`, and no privileged entitlements. Loom
forbids host networking, `security.insecure`, device passthrough, SSH-agent
forwarding, arbitrary host binds, and unpinned remote Dockerfile frontends.

The trusted supervisor remains outside BuildKit's PID namespace but inside the
same allocation cgroup so it can monitor the whole delegated subtree. It
continuously verifies that expected processes remain in that subtree. Kernel
cgroup membership, not parent-PID inspection alone, is authoritative.

BuildKit may parallelize layers internally, but the supervisor submits only one
materialization at a time. An OOM, PID exhaustion, disk-quota event, I/O limit,
or wall-time expiry is contained to the allocation and reported with a typed
failure reason.

On normal exit, cancellation, or signal, the supervisor stops claims, revokes
credentials, terminates BuildKit, unmounts rootless filesystems, and requests
job-storage cleanup. Slurm's epilog verifies that the allocation cgroup is
empty, kills any survivor, removes the quota assignment and directory, and
records cleanup evidence. Cleanup failure quarantines the node capability.

## Storage and cache

Node-local BuildKit state is disposable and has a hard per-job byte and inode
quota. It is always deleted at allocation exit. Loom never relies on a local
image or layer surviving for correctness.

Cross-job acceleration uses an optional, content-addressed registry cache in a
separate namespace. Cache import is verified by digest and scoped to the build
policy; cache data never grants publication readiness. Cache objects have a
short TTL and free-space backstop. Disabling the cache changes performance, not
correctness.

Ready component manifests remain pinned while referenced by a current task
version or nonterminal trial. Unreferenced materializations enter `retiring`
after a grace period. A fenced GC claimant rechecks references immediately
before deletion, deletes registry manifests, invokes registry garbage
collection as appropriate, and records retirement. Partial attempt references
have a shorter grace period. Provenance and lifecycle evidence remain durable
after registry deletion.

Images therefore do not stay on builder disks forever, and registry artifacts
do not grow without a reference-aware retention policy.

## Network and credentials

Dockerfile `RUN` traffic uses a job-specific network namespace and controlled
egress proxy. The policy blocks host, RFC1918/cluster, link-local, metadata,
control-plane, Slurm-controller, and credential endpoints. Approved public
package and source traffic can be allowed and audited by policy. Host networking
is never an escape hatch.

The supervisor receives only a one-use bootstrap grant. Raw secrets must not be
placed in `sbatch` arguments, Slurm-exported environment metadata, build
arguments, labels, logs, or a shared Docker config directory. An
authority-installed grant projector or equivalent workload-identity mechanism
delivers the bootstrap secret through a job-private channel.

The control plane exchanges the grant for:

- a short-lived token limited to task-image claim, heartbeat, publication, and
  failure endpoints; and
- a short-lived registry credential limited to the attempt's staging and cache
  namespaces.

Credentials live in job-private memory-backed state. The trusted BuildKit
client may use registry authentication through its session, but Dockerfile
processes receive neither the credential files nor environment variables.
Every credential is revoked or expires when the allocation/lease ends.

## State, fencing, and failure handling

The durable lifecycle remains:

```text
queued -> claimed -> running -> ready -> retiring -> retired
  ^         |          |
  +---------+----------+  expired lease or retryable failure
            |
            +------------> failed  deterministic or exhausted failure
```

Every mutation after claim includes the materialization ID, attempt ID, lease
epoch, and expected current state. Heartbeats extend only the matching lease.
A stale builder may record cleanup evidence but cannot replace publications or
mark readiness.

Failures are typed:

- **Deterministic input failures:** invalid Dockerfile, missing build context,
  forbidden entitlement, unsupported native platform. Mark failed and expose a
  user-actionable reason.
- **Transient infrastructure failures:** allocation loss, object-store or
  registry outage, public dependency timeout, temporary disk failure. Return to
  queued with bounded exponential backoff and jitter.
- **Resource-profile failures:** OOM, PID or disk-quota exhaustion, wall-time
  expiry. Retry only according to an explicit bounded policy; repeated failures
  require operator/user action rather than silently requesting an unbounded
  allocation.
- **Containment failures:** missing controller, escaped process, forbidden mount
  or network path, cleanup survivor. Fail before claim where possible,
  quarantine the node capability, and page operations. Do not charge the task's
  deterministic-attempt budget.
- **Publication uncertainty:** record every observed digest before retry. Never
  infer readiness from the existence of a tag.

Dependent trials remain in a nonterminal `waiting_for_task_image` condition
while retries remain. A terminal materialization failure is reported against
the trial as a prerequisite failure without manufacturing a trial attempt,
trajectory, LLM usage record, or invalid execution evidence.

## Observability and operator interface

Metrics and structured events include:

- queued count and oldest queue age by architecture;
- desired, pending, running, draining, and idle-exit builder allocations;
- Slurm pending reason and earliest-start estimate when available;
- time from registration to ready and time in each lifecycle state;
- build duration, bytes, cache hit/miss, and component count;
- resource high-water marks and CPU/memory/PID/I/O/storage limit events;
- containment preflight, runtime watcher, and cleanup results;
- lease loss, retry class, final failure reason, and partial publications;
- credential age/revocation and registry verification/GC outcomes; and
- trials waiting on, released by, or terminally blocked by materialization.

Alerts fire on excessive oldest queue age, no eligible nodes, repeated
containment failure, cleanup residue, registry inconsistency, expiring
credentials in use, or retention backlog. The operator view joins a task/trial
to its materialization, attempt, Slurm job, publication digests, and typed
failure without exposing secrets.

## Rollout and rollback

### Phase 0: inert implementation

Add the provider/executor boundary, versioned schema fields, rootless builder
release, tests, and disabled policies. The existing exclusive backend,
prerequisites, and reservations remain unchanged; this phase does not assume
that its protected rollout has completed or that it is serving demand. No
reservation is removed.

### Phase 1: cluster prerequisites and certification

Provision cgroup enforcement, builder OS identity, rootless runtime, storage
quota, egress, workload credential projection, and ordinary builder QoS. Run
read-only conformance first, then certify eligible shared nodes with the Slurm
feature. Certification is architecture-independent but recorded per node and
release.

### Phase 2: shadow canaries

Submit rootless non-exclusive builds for controlled canary task sets on each
architecture without making their publications authoritative for production
trials. Verify functionality and provenance; do not require byte-identical
digests from Dockerfiles with mutable external downloads. Run adversarial builds
beside representative trials and host services.

### Phase 3: gated production activation

Enable one architecture at a time with one builder allocation. New claims use
the rootless backend; the exclusive backend stops new claims but remains
available as a rollback candidate. Verify real registrations, submission
backstops, registry retention, drain behavior, and scale-to-zero.

Locate the original run store for task/run `4139e767`, enqueue or retry its
materialization through the production path, and run an end-to-end trial. The
incident is not considered unblocked until the rerun performs LLM calls and
produces valid evidence.

### Phase 4: soak and retire compensation

After both architectures pass acceptance and a production soak, retire the
legacy `--exclusive`, fixed-node, host-Docker, and reservation policy and code
paths. Deleting the existing OLDLAB or GB10 reservation is a separate
destructive operation requiring explicit approval and exact readback of the
target reservation.

Rollback disables new rootless claims and drains its allocations. It may
re-enable the exclusive backend and reservation only after that path has been
separately activated and proven. Otherwise materialization-dependent trials
remain safely queued while operators repair the rootless path. Ready immutable
digests remain valid; no database rollback or trial rebuild is required.

## Implementation decomposition

This architecture is intentionally broader than one reviewable change. It is
delivered as separately planned, tested, and merged increments:

1. **Rollout validation correction:** split rehearsal validation from
   post-materialization runtime validation and remove the misleading generic
   hosted-runner deployment path. This is the first increment because it also
   removes the current protected-rollout bootstrap cycle.
2. **Containment prerequisites and evidence:** define the node conformance
   contract, evidence schema, rootless runtime release, storage quota, network
   boundary, and workload-grant projection. This remains inert.
3. **Rootless executor and provider:** add the narrow environment-provider
   boundary, allocation supervisor, BuildKit executor, publication provenance,
   and disabled policies.
4. **Dynamic scheduling and starvation control:** render ordinary non-exclusive
   requests and integrate builder pressure with trial-worker draining.
5. **Cluster canary and activation:** certify nodes, run adversarial acceptance,
   activate one architecture at a time, and perform the incident rerun.
6. **Legacy retirement:** after soak and explicit approval, remove the
   host-Docker exclusive path and then the exact named reservations.

Each increment uses its own implementation plan and PR, passes CI, and is
squash-merged before the next protected rollout step. An increment must not
silently activate behavior introduced by a later increment.

## Rollout validation correction

Release rehearsal and live runtime validation are different phases and must not
have a bootstrap cycle.

- **Rehearsal validation** is side-effect-free. It validates candidate policy,
  templates, paths, pinned release manifests, authority bindings, and intended
  operations using candidate inputs. It must not require release-specific worker
  environment or repository paths that materialization has not created.
- **Materialization** creates the release-specific repository, environment,
  and credential projections under the protected host-local lock.
- **Post-materialization validation** checks the actual files, permissions,
  installed binaries, service configuration, and non-mutating Slurm request.
- **Activation** mutates supervisors only after both validation phases have
  produced evidence.

The generic GitHub staging deployment path must not pretend to own host-local
rollout authority or locks. Hosted CI validates code and release artifacts;
the installed host-local authority performs protected staging mutation. A
deployment workflow without the required secrets and shared lock is removed or
made explicitly non-deploying.

## Verification and acceptance

### Unit and contract tests

- versioned materialization identity, native architecture expansion, and
  idempotent registration/submission linkage;
- lease fencing, heartbeat recovery, retry budgets, partial publication, and
  reference-aware GC;
- Slurm rendering contains resource/capability/QoS constraints and omits
  `--exclusive`, `--reservation`, `--nodelist`, Docker socket, and secret values;
- starvation signals suppress scale-up, drain reusable workers after the soft
  threshold, and release pressure after builder start;
- execution grants remain digest-only and trial claims remain gated; and
- rehearsal validation succeeds without future runtime artifacts, while
  post-materialization validation requires and verifies them.

### Integration tests

- direct rootless BuildKit startup under a test allocation cgroup;
- primary and sidecar builds, base locking, native platform verification,
  staged publication, registry `HEAD` verification, and atomic readiness;
- lease loss during build and publication cannot mark ready;
- cancellation and timeout remove processes, mounts, runtime files, and
  credentials; and
- expired or partial artifacts are collected without deleting a live digest.

### Real-cluster adversarial acceptance

Run on certified OLDLAB and GB10 shared nodes beside a representative trial.
Evidence must prove that:

- every supervisor, BuildKit, `RUN`, snapshotter, and network-helper PID remains
  in the allocation cgroup;
- CPU saturation, memory exhaustion, a fork bomb, excessive I/O, and disk/inode
  exhaustion cannot exceed the allocation limits;
- daemonization, double-forking, ignored signals, and nested namespaces leave no
  surviving process after cancellation;
- forbidden host paths, container sockets, node-local/cluster endpoints, and
  registry/control-plane credentials are inaccessible;
- the concurrent trial remains inside its own limits and host services remain
  healthy;
- local state and mounts are absent after epilog, partial publications are
  retained only for the configured grace period, and the builder returns to
  scale zero;
- under continuous synthetic trial demand, no later trial allocation jumps
  ahead of the pending builder and reusable workers drain as designed; and
- rollback to the exclusive backend succeeds without changing ready trial
  digests.

Non-exclusive activation remains fail-closed until the signed acceptance record
for the exact cluster configuration, kernel, Slurm configuration, builder
release, and BuildKit/helper release verifies successfully.

### End-to-end incident acceptance

For the original affected task/run, acceptance requires all of the following:

- the correct run store and task materialization are located;
- registration/submission creates or reuses the expected native build;
- the dynamic rootless builder publishes and the registry verifies every
  component digest;
- the trial transitions from waiting to claimable without a manual image copy;
- execution pulls the exact recorded digest;
- at least one expected LLM call is recorded; and
- verifier/evidence output is valid rather than a no-call invalid-evidence
  artifact.

## Final invariants

1. A service trial never builds a task Dockerfile.
2. A builder never claims work before proving its containment contract.
3. Every build process is a descendant of the allocation cgroup; no pre-existing
   host daemon performs the build.
4. Dynamic builders use no permanent node reservation or exclusive allocation.
5. Builders can wait behind active work but cannot be starved by newly admitted
   trial work.
6. Only verified immutable digests make a materialization ready.
7. Lease loss prevents stale readiness, regardless of registry side effects.
8. Build input never receives control-plane or registry credentials.
9. Local builder state is bounded and disposable; registry retention is
   reference-aware and fenced.
10. Missing containment, credentials, registry consistency, or rollout
    prerequisites fails closed before trial execution.
