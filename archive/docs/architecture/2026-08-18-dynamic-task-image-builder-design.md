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
- Cross-task artifact deduplication or a normalized artifact/reference schema.
  Those are independent storage optimizations, not containment prerequisites.
- Treating a source key as proof of reproducibility when a Dockerfile downloads
  mutable external content. The published image digest is the execution
  authority.

## Alternatives considered

### Allocation-scoped rootless BuildKit — selected

Start a fresh, unprivileged BuildKit daemon directly in every builder
allocation. User, mount, PID, and network namespaces isolate the build; the
Slurm cgroup and job-scoped storage enforce resource limits. This is closest to
Loom's existing Dockerfile behavior while removing the host Docker daemon from
the build path. Because the rootless OCI worker's `network.host` is relative to
its enclosing RootlessKit namespace, that namespace and its egress controls are
part of the required executor rather than optional hardening.

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
- the Slurm controller, cgroup/epilog configuration, root-owned node guard, and
  the narrow build-environment provider;
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
+---------------------- shared Slurm node ----------------------+
| root-owned node guard: peer/cgroup auth + BPF attachment      |
|                    | Unix socket + sealed memfd                |
|  +-----------------v-- Slurm allocation cgroup -------------+ |
|  | allocation supervisor                                   | |
|  |  +-- containment and release validation                 | |
|  |  +-- rootless BuildKit daemon                           | |
|  |  +-- Dockerfile RUN processes and helpers               | |
|  |  +-- job-scoped storage, network, and credentials       | |
|  +-----------------------------+----------------------------+ |
+--------------------------------+------------------------------+
                                 | per-attempt repository push
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
- journaling each submission intent, reconciling ambiguous submissions, binding
  a nonsecret grant ID to exactly one held Slurm job, and releasing the job only
  after that binding is durable.

The initial `SlurmBuildEnvironmentProvider` submits ordinary jobs. A future
microVM or Kubernetes provider can implement the same contract without changing
materialization semantics. Loom does not need to adopt a broad external
"environment provider" abstraction for trial execution to gain this boundary.

The installed provider executor runs as a dedicated non-root builder Unix
principal and submits only its own Slurm jobs. Its Slurm association permits the
builder partition and capped QoS but not administrator operations. Trial-worker
jobs run under a different Unix principal.

### Node-local guard

Each eligible node runs a small root-owned `loom-task-builder-node-guard`
daemon installed by the cluster authority. It is not a Slurm Prolog, and its
availability does not control `slurmd`'s Prolog result or drain a node. Its
narrow responsibilities are to authenticate a local builder allocation, attach
the reviewed network programs, project a one-use bootstrap credential, and
quarantine the builder capability when containment or cleanup fails.

The guard listens on a root-owned Unix `SOCK_SEQPACKET` socket. It authenticates
the connecting supervisor with `SO_PEERCRED`, immediately pins it with a pidfd,
and verifies that it is the live batch-step leader, runs the pinned supervisor
executable, has the dedicated Unix identity, and occupies the expected Slurm
job cgroup. The submitted batch script contains only `exec` of that supervisor.
The guard compares the cgroup-derived job ID, node-configured cluster ID, and
live job attributes with the durable grant while holding the pidfd through the
exchange. It passes the credential only as an
`MFD_CLOEXEC|MFD_ALLOW_SEALING` memfd over `SCM_RIGHTS`. The memfd is sealed
against write, growth, shrink, and further seal changes before transfer; no
bootstrap secret is written to a path. The guard's node-specific mTLS key
remains root-only and cannot be requested over the local protocol.

The guard has no remote listener and accepts only a bounded versioned request
containing a grant ID; it never parses a task bundle or Dockerfile. Its systemd
unit pins the executable and policy digests, limits memory/PIDs/requests,
restricts filesystem writes to its runtime state, and limits network access to
the exact control-plane projection endpoint. It retains only the host authority
needed for peer/cgroup inspection, approved Loom-subtree management, and BPF
attachment. Malformed or excessive local requests are rate-limited and audited
without affecting `slurmd`.

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

Each pushed component digest is recorded immediately as cleanup evidence
against the fenced attempt. Cleanup evidence is not readiness evidence. The
control-plane publisher fetches and validates the manifest and config by digest,
then writes a canonical, signed publication statement bound to the task,
component, architecture, build policy, attempt, and lease. Readiness is
committed only after every component has that verified statement. The retention
controller keeps referenced publications, retires unreferenced ones after a
grace period, and garbage-collects partial or abandoned attempts.

### Trial scheduler and workers

The scheduler carries the frozen task snapshot, exact component digests, and
publication-statement digests in the trial execution grant. Service workers
verify the bundle and statements and pull those image digests. A missing
artifact after readiness is an infrastructure consistency failure; a trial
worker never rebuilds it.

## Materialization identity and provenance

The containment migration preserves Loom's current task-scoped v1 identity:

```text
SHA256(
  v1 domain,
  task ID,
  canonical task checksum,
  native CPU architecture
)
```

There remains one materialization row per `(task_id, task_checksum, cpu_arch)`.
The existing task and trial reference model, uniqueness constraints, scheduler
queries, execution grants, and retention logic remain authoritative. The
rootless executor does not require a materialization-key migration or
cross-task sharing.

The builder records a reviewed build-policy version and the exact BuildKit and
helper binary digests in publication provenance. When a security or policy
change requires rebuilding already-ready materializations, an operator starts a
fenced rematerialization campaign using the existing ready-to-queued retry
transition. Previous output digests remain in publication history; active trial
execution grants already bound to a digest do not change.

Resolved base-image digests are recorded as observed provenance, but this design
does not add a separate base-locking resolver. Mutable base tags and downloads
performed by arbitrary `RUN` instructions mean a later retry can legitimately
produce a different output digest. That digest is recorded as a new publication
generation rather than hidden behind a claim of source reproducibility.

Provenance records at least:

- v1 materialization key and reviewed build-policy version;
- task ID, task checksum, and component identity;
- task snapshot and bundle digests;
- native platform and observed resolved base-image digests;
- builder release and BuildKit/helper binary digests;
- Slurm cluster and job identifiers, without secrets;
- network-policy version;
- timestamps, attempt/lease epoch, and component output digests;
- containment-evidence version and result; and
- the control-plane-signed publication-statement digest.

The output digest, not a mutable tag, is the only execution reference.

## End-to-end lifecycle

1. Registration validates and freezes the task bundle, then ensures one
   materialization per required native architecture. `cpu_arch = "any"`
   creates `x86_64` and `arm64` intents.
2. Trial submission idempotently ensures and links the same intents. The trial
   remains visibly blocked on `task_image_materialization`, not failed.
3. The capacity reconciler observes queued demand, journals a grant and
   submission intent, submits an ordinary Slurm builder job in held state with
   the grant ID in its versioned comment, reconciles zero/one/multiple matches,
   binds exactly one returned or discovered job ID, and then releases the job.
4. The allocation supervisor proves containment before it asks for a claim.
5. The node-local guard authenticates the supervisor and its job cgroup,
   installs the job's network policy, and passes a sealed one-use memfd. The
   supervisor exchanges it for a short-lived `task-image:build` session.
6. The builder claims a matching architecture row with a lease epoch.
7. Only after claim, the builder obtains registry credentials scoped to the
   exact per-component attempt repositories and lease, fetches the frozen bundle
   through a time-limited object URL, and verifies its digest. Any cache
   credential is separate.
8. BuildKit builds every Dockerfile-backed component using the native platform
   and restricted policy, recording resolved base digests as provenance.
9. Each component is pushed to its own attempt-specific repository. The
   returned digest is appended immediately as cleanup evidence.
10. The control-plane publisher fetches the manifest and config by digest,
    verifies the publication contract, signs its statement, and atomically marks
    the complete component set ready. A stale lease cannot perform this
    transition.
11. A matching trial worker receives exact digests, pulls them, verifies the
    frozen bundle, and only then starts trial execution and evidence creation.
12. One allocation may claim further rows sequentially. It exits after a short
    idle grace period and leaves no durable node-local state.

## Dynamic Slurm scheduling and starvation policy

Builder jobs request a fixed, reviewed resource profile and render:

- one node and one task;
- native architecture and `loom_rootless_buildkit` capability constraints;
- positive CPU, memory, PID, I/O, temporary-storage, and wall-time limits; and
- zero builder-cgroup swap (`memory.swap.max=0`); and
- an overlapping shared-node builder partition with a higher `PriorityTier`
  than Loom trial-worker partitions; and
- a dedicated builder QoS with bounded submitted/running job counts and a hard
  aggregate TRES ceiling.

They do not render:

- `--exclusive`;
- `--reservation`;
- `--nodelist` or a permanent `allowed_nodes` pin; or
- access to `/var/run/docker.sock` or another host runtime.

They render `--hold` for grant binding, `--no-requeue`, and the exact nonsecret
comment `loom-task-builder-v1:grant=<grant-id>`. An allocation lost before or
after projection terminates; replacement demand receives a new Slurm job and a
new grant.

Submission is recoverable even when `sbatch` commits a job but its response is
lost. Before invoking `sbatch`, the reconciler durably moves the grant from
`issued` to `submitting` and records the cluster, submitting Unix identity,
account, comment, and expected request digest. It never retries merely because
the command result is ambiguous. Instead it inventories live jobs with `squeue`
and recent terminal jobs with accounting by exact cluster, submitting identity,
and versioned comment, then validates every candidate's immutable request
fields:

- zero matches after both inventories return an authoritative result moves the
  same grant back to `issued`, from which one new held submission is allowed;
- one match binds that job, regardless of whether the original `sbatch` response
  was observed; and
- multiple matches are never guessed between: all remain held, are cancelled
  and confirmed terminal, the grant is revoked, and replacement demand receives
  a new grant.

An unavailable or incomplete inventory leaves the grant in `submitting` and
raises an operator-visible reconciliation error; it does not submit again. A
job with the comment but any mismatched request field is cancelled and audited,
never adopted. These rules make a network timeout unable to create a running
unbound builder.

QoS priority is additive under Slurm's multifactor plugin and is therefore not
the starvation fence. Strict ordering between Loom job classes comes from the
overlapping builder partition's higher `PriorityTier`, certified scheduler
configuration, and Loom admission control. The partition contains the same
shared nodes as the trial partitions; it reserves no node. The builder QoS
limits resource consumption but does not provide ordering.

Slurm may make a temporary earliest-start plan for a pending higher-tier
builder and backfill lower-tier work only when it will not delay that start.
That is dynamic scheduler state, not a permanent named reservation. Running
trials are never preempted by this policy.

Long-lived trial worker jobs require an application-level starvation rule:

1. The reconciler submits and durably binds the held builder job before it
   releases any new trial-capacity decision for that architecture.
2. New builder demand immediately suppresses further trial-pool scale-up and
   cancels enough pending, not-yet-running Loom trial-worker allocations to
   remove conflicting queued capacity. No active trial is affected.
3. When oldest queue age crosses a configured soft threshold, the capacity
   arbiter marks enough reusable running trial workers to drain after their
   current trial to satisfy the builder's resource profile.
4. The claim service refuses new work to a draining worker. It exits normally
   after its active trial or immediately when idle.
5. Drain and admission pressure are released when the builder starts, its demand
   disappears, or the builder job terminates without replacement demand.

Therefore continuous new Loom trial arrivals and previously pending Loom
trial-worker submissions cannot jump ahead of the builder. If every eligible
resource is occupied by active trials, the builder still waits for one to
finish. Jobs outside Loom's admission authority, or jobs in an equal/higher
operator-defined partition tier, may also delay it. A cluster-wide hard
start-time bound would require a later, explicit preemption or reservation
policy.

Builder concurrency is capped in the other direction so a registration burst
cannot starve trials. The initial production policy is one builder allocation
per architecture, one materialization at a time per allocation.

## Eligible-node contract

A node advertises `loom_rootless_buildkit` only while all of these properties
are true:

- cgroup v2 is active;
- Slurm uses `task/cgroup` and `proctrack/cgroup`;
- CPU, memory, device, and swap constraints are enabled and proven;
- the builder profile can enforce `memory.swap.max=0`, disable core dumps, and
  provide the small locked-memory allowance required for credentials;
- delegated `pids` and `io` controllers can enforce limits beneath the job
  cgroup without permitting movement into its parent;
- a conformance job can create the Loom subtree beneath the Slurm batch-task
  cgroup, move only its batch leader into it, and still be fully observed by
  Slurm accounting, cancellation, and epilog cleanup;
- unprivileged user namespaces and approved subordinate UID/GID mappings are
  available to a dedicated builder operating-system identity;
- pinned RootlessKit, BuildKit, snapshotter, and network helpers are installed
  from the trusted release;
- the cluster policy names an exact snapshotter rather than `auto`: either
  unprivileged kernel `overlayfs` after a successful probe, or
  `fuse-overlayfs`; when FUSE is selected, the allocation may open `/dev/fuse`
  but the Dockerfile execution mount namespace cannot see that device;
- the cluster policy pins `slirp4netns` and a RootlessKit release that supports
  `--disable-host-loopback`, IPv6, sandbox mode, and seccomp mode; all four are
  enabled and probed rather than accepted through `auto` fallback;
- the kernel and libc expose the pinned pidfd, sealed-memfd, and
  `clone3(CLONE_INTO_CGROUP)` behavior required by the launcher and guard;
- the pinned node guard and its cgroup-v2 BPF programs are installed, the guard
  socket is root-owned, and both IPv4 and IPv6 fail-closed probes pass;
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
2. requested CPU, memory, device, PID, and I/O limits and zero-swap policy are
   effective;
3. its writable delegated subtree cannot move a process to the allocation
   parent or another job;
4. job storage is empty, local, quota-limited, and inaccessible to the build
   mount namespace except where explicitly projected;
5. no host Docker/containerd socket, host network, cluster credential, or
   unsafe host mount is visible;
6. installed executables match the pinned release manifest; and
7. a probe build places BuildKit, its executor, every `RUN` process, the
   snapshotter, and network helpers beneath the allocation cgroup; and
8. the node guard has authenticated this peer and attached the exact reviewed
   network-policy program and map digests before releasing a credential.

The trusted launcher first uses the pinned `newuidmap` and `newgidmap` helpers
to establish the approved subordinate-ID mapping. Only after that mapping
exists does the OCI executor apply `no_new_privileges` to Dockerfile execution
processes. The launcher and helpers have their own narrow AppArmor/seccomp
policy; a node-wide unconfined profile is not an acceptable substitute.

RootlessKit starts with pinned `slirp4netns`, `--disable-host-loopback`,
`--ipv6`, `--slirp4netns-sandbox=true`, and
`--slirp4netns-seccomp=true`. BuildKit uses only the rootless OCI worker with
process sandboxing enabled. Its insecure-entitlement list is empty. Loom
forbids `security.insecure`, real host networking, CDI/device injection,
SSH-agent forwarding, arbitrary host binds, and unpinned remote Dockerfile
frontends. If `fuse-overlayfs` is selected, only the trusted snapshotter mount
namespace receives `/dev/fuse`; Dockerfile `RUN` processes do not.

The current `gvisor-tap-vsock` RootlessKit driver is not a production fallback:
it is experimental, does not support IPv6 routing, and does not implement the
required host-loopback disablement. `pasta` is also experimental in the pinned
RootlessKit line. Adding or changing a network driver requires a new reviewed
policy version and full two-architecture acceptance; runtime auto-selection is
forbidden.

The trusted supervisor remains outside BuildKit's PID namespace but inside the
same allocation cgroup so it can monitor the whole delegated subtree. It
continuously verifies that expected processes remain in that subtree. Kernel
cgroup membership, not parent-PID inspection alone, is authoritative.

Within the exact Slurm batch-task cgroup, the node guard creates a root-owned
`loom-builder` containment root with `trusted-service` and `build-egress`
children. It attaches policy while all three are empty, then moves only the
pidfd-pinned supervisor into `trusted-service`. It never moves `slurmstepd` or
attaches Loom policy to a Slurm daemon cgroup. The whole Loom subtree remains a
descendant of the Slurm task hierarchy, so its CPU, memory, PIDs, I/O, device,
and accounting controls are inherited and Slurm cleanup still observes it.

The guard delegates only the process-creation and controller files needed below
`loom-builder`; no allocation process can move to an ancestor or sibling. The
trusted supervisor uses `clone3(CLONE_INTO_CGROUP)` to start the pinned
RootlessKit launcher directly in `build-egress`; RootlessKit starts BuildKit and
its pinned `slirp4netns` helper as normal descendants. No process can open an
egress socket in that subtree before inheriting the attached policy, and the
guard never executes a Dockerfile process, network helper, or BuildKit daemon.
Dockerfile traffic therefore cannot race policy installation.

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

Cross-job registry caching is disabled by default. A later explicit policy may
enable an optional content-addressed cache in a separate repository namespace.
Cache import is verified by digest and scoped to the build policy; cache data
never grants publication readiness. Cache read and write credentials are
separate from publication credentials and from each other. Cache objects have a
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

Dockerfile `RUN` traffic uses the job-specific RootlessKit network namespace.
Although BuildKit calls this OCI-worker mode `network.host`, the "host" is that
isolated namespace, never the node namespace. Loom uses the exact certified
`slirp4netns` release with sandbox, seccomp, host-loopback disablement, and IPv6
enabled. No alternate driver is selected at runtime.

A root-owned node guard enforces this policy with pinned cgroup-v2 BPF programs,
not proxy environment variables alone. Before enabling the RootlessKit
interface, it attaches `BPF_CGROUP_INET4_CONNECT`,
`BPF_CGROUP_INET6_CONNECT`, `BPF_CGROUP_UDP4_SENDMSG`,
`BPF_CGROUP_UDP6_SENDMSG`, and a `BPF_CGROUP_INET_EGRESS` packet backstop. The
packet program covers non-connect, raw, forwarded, and translated traffic. The
The `loom-builder` containment-root policy permits only the union of exact
trusted-service and build-egress destinations; the inherited `build-egress`
child policy narrows that to the audited package/DNS gateway and exact registry
endpoints needed for base-image reads and attempt publication. The
`trusted-service` child permits only the exact control-plane, object-store,
registry, and revocation endpoints. No program is attached to a Slurm daemon's
cgroup.

Both IPv4 and IPv6 default to deny. Loopback outside the namespace,
RFC1918/ULA cluster ranges except explicit endpoint addresses, link-local,
metadata, Slurm-controller, credential, and node-management destinations are
denied. Program, map-schema, and policy-input digests are release evidence;
maps are root-writable only. Attach or probe failure prevents credential
projection and terminates the job. A missing, replaced, or detached program
during the job terminates the allocation and quarantines the node capability.
Consequently a Dockerfile cannot bypass the policy by ignoring proxy variables,
using IPv6 or UDP, or requesting real host networking.

The bootstrap protocol is exact rather than provider-defined:

1. Before submission, the capacity reconciler creates a `BuilderJobGrant`
   containing a random nonsecret ID, purpose (`production` or `shadow`), optional
   shadow campaign ID, pool, architecture, builder release, resource profile,
   expiry, and state `issued`.
2. The provider journals `submitting`, then calls `sbatch --hold --no-requeue`
   with `--comment=loom-task-builder-v1:grant=<grant-id>`. Only the grant ID is
   carried in job metadata; no bearer secret is passed to `sbatch` or
   `--export`. Ambiguous results follow the inventory procedure in the
   scheduling section; only one matching held job can become bound.
3. The reconciler atomically binds the grant to the exact Slurm cluster and job
   ID, then releases that job. An unbound held job is cancelled by
   reconciliation and can never obtain a credential.
4. Once running, the supervisor connects to the node guard's Unix socket and
   sends the nonsecret grant ID. The guard uses `SO_PEERCRED` and the peer's
   live cgroup to derive the job identity; it does not trust job-script
   environment variables. With its node-specific mTLS identity it submits the
   derived cluster/job ID, Unix identity, account, QoS, partition, architecture,
   resource shape, and builder release. The control plane compares every field
   with the durable binding and returns a one-use attachment challenge without
   changing the grant state or releasing a credential.
5. The guard creates the empty Loom cgroup subtree, attaches and probes the
   exact BPF policy, moves the pidfd-pinned supervisor into `trusted-service`,
   and returns the challenge with the observed program, map, policy, and cgroup
   digests. The control plane rechecks the binding and atomically moves
   `bound -> projected`, returning a one-use random bootstrap secret over mTLS.
   The guard places it in a sealed memfd and transfers only that descriptor to
   the still-authenticated peer. Trial workers use a different Unix identity
   and cannot connect successfully.
6. Before starting RootlessKit, the supervisor verifies all required memfd
   seals with `F_GET_SEALS`, copies the bounded payload exactly once into an
   `mlock`ed, non-dumpable buffer, closes the descriptor, and exchanges the
   secret. The guard retains its own locked mapping until that exchange is
   acknowledged, then closes it. The control plane atomically moves
   `projected -> exchanged` and returns a short-lived session limited to claim,
   start, heartbeat, publication, and failure operations for that pool and
   architecture and the grant's exact purpose/campaign. A shadow session cannot
   call the production claim or publication transitions.
7. After a materialization claim creates an attempt and lease epoch, the
   supervisor requests a publication credential. For production, the broker
   derives the exact set of repositories as
   `loom-task-image-attempts/<architecture>/<attempt-id>/<component>`; for
   shadow, it derives
   `loom-task-image-shadow/<campaign-id>/<architecture>/<attempt-id>/<component>`.
   The caller never supplies an arbitrary repository root. Credentials contain
   only the push/pull actions required for that attempt. Component names are
   normalized from the frozen snapshot before authorization. Registry
   authorization is repository-scoped: tag prefixes are never treated as a
   security boundary. Expiry is no later than either the job/session expiry or
   lease expiry. A different purpose, campaign, attempt, lease epoch,
   repository, or architecture is rejected. If registry caching is later
   enabled, cache import and export use separate credentials for a separate
   cache repository; neither credential authorizes an attempt repository.
8. Job cancellation, epilog, or session expiry revokes the session and registry
   credentials. Closing an unconsumed memfd destroys its only allocation-side
   copy.

Projection and exchange use canonical idempotency keys and stored encrypted
response receipts. An exact retry after an ambiguous transport failure returns
the same still-valid response. A changed field, different node/job, different
idempotency key, expired grant, second semantic exchange, or attempt/lease
rebinding is rejected and audited. Because builder jobs use `--no-requeue`, a
replacement allocation never inherits a projected grant.

The node guard is a cluster-administrator-installed prerequisite with only
grant-projection, network-attachment, and capability-quarantine authority; it
cannot claim materializations or push images. Its mTLS key is root-owned and
never enters the allocation. This concrete guard is the initial
`SlurmBuildEnvironmentProvider` mechanism; replacing it later requires a
separately reviewed provider contract.

Raw secrets must not appear in Slurm arguments or exported metadata, build
arguments, labels, logs, a shared Docker config directory, or persistent disk.
The short-lived session and repository credentials live in job-private
memory. The supervisor's credential agent holds authoritative copies in locked
buffers; all trusted processes are non-dumpable and non-ptraceable, and the
builder cgroup cannot swap. Buffers are zeroized on expiry or revocation; any
unavoidable transient copy inside the pinned BuildKit registry client dies with
the allocation. The trusted BuildKit client may use registry authentication
through its session, but Dockerfile processes receive neither credential files
nor environment variables. Registry and cache clients use distinct in-memory
credential helpers, so a cache challenge cannot receive a publication token.

## Publication verification contract

A registry `HEAD` request is a liveness probe only. It cannot grant readiness.
For each reported component digest, the control-plane publisher:

1. performs an authenticated `GET` by digest and recomputes the digest over the
   exact returned manifest bytes;
2. accepts only reviewed OCI image or Docker schema-2 media types and enforces
   configured manifest/config/layer count and byte limits;
3. if the top level is an index, requires exactly one runnable image descriptor
   for the expected `linux/<native-architecture>` platform and permits only
   explicitly recognized non-runnable provenance descriptors;
4. fetches the runnable manifest and config by digest, verifies every descriptor
   binding, recomputes the config digest, checks each layer's registry existence
   and declared size, and requires the config OS and architecture to match the
   materialization;
5. checks that the registry repository is the attempt-scoped destination and
   that the component name is one expected by the frozen task snapshot;
6. creates a canonical publication statement containing the materialization
   key, task checksum, component, platform, purpose (`production` or `shadow`),
   attempt ID, lease epoch, Slurm job, build-policy version, builder release,
   observed base digests, containment evidence digest, output digest, and
   signer-issued `issued_at` timestamp; and
7. signs and stores that statement with the control plane's publication key.

The statement schema is `loom.task-image-publication/v1`. It contains only
integers, booleans, strings, arrays, and objects; timestamps are UTC RFC 3339
strings and digests use normalized lowercase algorithm/hex form. The exact
statement object is encoded with RFC 8785 JSON Canonicalization Scheme. The
signature preimage is the ASCII domain separator
`loom-task-image-publication-v1`, one NUL byte, and those canonical bytes. The
publisher computes SHA-256 over the canonical bytes and asks the publication
signer for an Ed25519 signature.

The immutable signature envelope stores the schema, `key_id`, fixed algorithm
`Ed25519`, statement digest, canonical statement bytes, and base64url signature.
It is inserted in the control-plane publication store in the same transaction
that associates it with the fenced component generation; an optional OCI
referrer is only a replica, never the database authority. The publication key
is distinct from bootstrap, registry, and execution-grant keys. Its private key
is non-exportable from the configured KMS/HSM or equivalent host signing
service, and the signer accepts only this domain and schema.
The signing service also requires the statement's `issued_at` to be within the
configured clock-skew window of its own clock and within the selected key's
active interval; the publisher cannot backdate a statement into a retired key.

Key records have `active`, `verify_only`, or `revoked` status and activation and
retirement timestamps. Routine rotation creates and distributes a new active
public key before use, moves the previous key to `verify_only`, and never
invalidates an existing statement. A verification key cannot be deleted while
any publication statement or retained audit record names it; because lifecycle
evidence is durable, those public keys are archived for the same duration.
Workers and the readiness transaction reject an unknown key, noncanonical
statement, digest mismatch, wrong domain/algorithm, bad signature, or a key used
outside its activation interval.

`revoked` is reserved for compromise, not routine rotation. Revocation blocks
new readiness and new trial grants for every affected statement, revokes
unclaimed grants, records a `publication_quarantine` overlay on affected ready
materializations, and starts a fenced rematerialization campaign with a
nonrevoked key. The production eligibility query excludes that overlay while
the underlying lifecycle row remains available for forensics. Running trials
are not silently rewritten; they are recorded for incident handling. The
compromised private key remains disabled while its public key and revocation
record remain available to explain historical evidence.

Image labels and mutable tags are diagnostic only; an untrusted Dockerfile may
set them. The verified descriptor plus the signed control-plane statement is the
readiness authority. Completion rechecks the current lease and requires one
verified statement for every expected component in the same attempt. Observed
but unverified digests remain only in cleanup history.

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

### Phase 0: rollout validation correction

Split side-effect-free rehearsal validation from post-materialization runtime
validation and remove or make explicitly non-deploying the misleading generic
hosted-runner staging path. This phase removes the current protected-rollout
bootstrap cycle but does not activate a builder or mutate a reservation.

### Phase 1: cluster prerequisites and certification

Provision cgroup enforcement, the dedicated builder OS identity and Slurm
association, rootless runtime and exact snapshotter, storage quota, egress
enforcement, the root-owned node guard and Unix socket, the repository-scoped
registry credential broker, the publication-signing key lifecycle, the
overlapping higher-tier builder partition, and the capped builder QoS. Add the
evidence schema and run read-only conformance. The policies remain disabled and
no node is certified for production claims yet.

### Phase 2: inert provider and executor

Add recoverable held-job/grant submission, the narrow Slurm environment
provider, allocation supervisor, BuildKit executor, signed publication
statements, tests, and disabled policies. Preserve the current task-scoped
materialization identity and reference model. Certify eligible shared nodes only
after the exact installed release passes conformance. The existing exclusive
backend, prerequisites, and reservations remain unchanged.

### Phase 3: shadow canaries

Shadow work uses a separate `TaskImageShadowCampaign` and shadow-attempt queue,
not production `TaskImageMaterialization` rows. Its jobs carry a shadow campaign
ID, its credentials authorize only
`loom-task-image-shadow/<campaign-id>/<architecture>/<attempt-id>/<component>`,
and its statements carry `purpose=shadow`. Production publishers reject that
repository root and purpose; the production readiness transaction, scheduler,
execution-grant query, and retention roots never read shadow rows. Conversely,
shadow credentials cannot write production attempt repositories.

Submit rootless non-exclusive builds for controlled canary task snapshots on
each architecture through that isolated path. Verify functionality and
provenance; do not require byte-identical digests from Dockerfiles with mutable
external downloads. Run adversarial builds beside the concrete trial and host
service fixtures. Shadow cache remains disabled so cache state cannot bridge
the two purposes.

### Phase 4: dynamic scheduling and gated production activation

Enable strict builder/trial ordering, pending-trial cancellation, and worker
draining under disabled/shadow evidence first. Then enable one architecture at
a time with one builder allocation. New claims use the rootless backend; the
exclusive backend stops new claims but remains available as a rollback
candidate. Verify real registrations, submission backstops, registry retention,
drain behavior, and scale-to-zero.

Locate the original run store for task/run `4139e767`, enqueue or retry its
materialization through the production path, and run an end-to-end trial. The
incident is not considered unblocked until the rerun performs LLM calls and
produces valid evidence.

### Phase 5: soak and retire compensation

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

1. **Rollout validation correction:** Phase 0.
2. **Containment prerequisites and evidence:** Phase 1.
3. **Inert rootless provider and executor:** Phase 2.
4. **Shadow canary and adversarial acceptance:** Phase 3.
5. **Dynamic scheduling and gated activation:** Phase 4, including the incident
   rerun after both native paths are proven.
6. **Legacy retirement:** Phase 5, including separate approval before deleting
   either exact named reservation.

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

- preserved v1 task-scoped materialization identity, native architecture
  expansion, and
  idempotent registration/submission linkage;
- lease fencing, heartbeat recovery, retry budgets, partial publication, and
  reference-aware GC;
- held Slurm rendering contains partition/resource/capability/QoS constraints,
  `--hold`, `--no-requeue`, and the exact versioned grant comment, while
  omitting `--exclusive`, `--reservation`, `--nodelist`, Docker socket, and
  bearer secrets;
- journal-before-submit and authoritative zero/one/multiple inventory recovery,
  held-job binding, release, mismatched/orphan cancellation, and refusal to
  retry while inventory is incomplete;
- node-guard `SO_PEERCRED`, Unix-identity, live-cgroup, job-field, executable,
  and memfd-seal validation, exact transport replay, semantic replay rejection,
  expiry, and revocation;
- BPF rendering and probes cover IPv4, IPv6, TCP, UDP, and packet egress, attach
  before interface/credential release, and fail closed on missing programs or
  changed policy digests;
- starvation control suppresses scale-up, cancels pending trial capacity,
  verifies higher partition tier, drains reusable workers after the threshold,
  prevents draining-worker claims, and releases pressure after builder start;
- publication validation rejects digest, size, media-type, platform, component,
  attempt, lease, or statement-binding mismatches, and a `HEAD` result alone
  cannot mark readiness;
- repository authorization rejects tag-prefix scoping and cross-attempt,
  cross-component, cache/publication, and shadow/production access;
- RFC 8785 and Ed25519 golden vectors cover canonicalization, domain separation,
  envelope validation, rotation, verification-key retention, and compromise
  revocation;
- shadow queue, campaign, repository, publisher, readiness, scheduler, and
  retention queries cannot cross into production;
- execution grants remain digest-only, include the matching publication
  statement digests, and trial claims remain gated; and
- rehearsal validation succeeds without future runtime artifacts, while
  post-materialization validation requires and verifies them.

### Integration tests

- direct rootless BuildKit startup under a test allocation cgroup with the
  certified network driver, process sandbox, and snapshotter;
- primary and sidecar builds, observed-base provenance, staged publication,
  authenticated manifest/config retrieval, signed publication statements, and
  atomic readiness;
- killed `sbatch` response and accounting-delay tests prove that one committed
  held job is adopted, zero is safely retried, multiple are all cancelled, and
  incomplete inventory never causes a second submission;
- a held job cannot receive a projected grant before exact job binding; a peer
  outside the bound cgroup cannot use the guard socket; the valid peer receives
  only a sealed memfd; exact transport retries return the recorded response
  while changed or semantic replays fail;
- direct IPv4/IPv6 TCP, UDP, raw-packet, loopback, metadata, and cluster-endpoint
  bypass attempts fail while the audited egress gateway and exact publication
  repositories remain usable;
- shadow publications cannot satisfy a production materialization even when
  task checksum, architecture, component, and output digest match;
- lease loss during build and publication cannot mark ready;
- cancellation and timeout remove processes, mounts, runtime files, and
  credentials; and
- expired or partial artifacts are collected without deleting a live digest.

### Real-cluster adversarial acceptance

Run on certified OLDLAB and GB10 shared nodes with these versioned fixtures,
whose source and image digests are part of the acceptance record:

- `task-image-adversary-v1`: primary and sidecar Dockerfiles exercise CPU,
  memory, PIDs, I/O, bytes/inodes, daemonization, namespaces, forbidden mounts,
  FUSE visibility, and IPv4/IPv6 TCP/UDP egress bypasses;
- `terminus2-neighbor-v1`: a normal Loom worker claim for a pinned Terminus-2
  benchmark/task snapshot, using the production execution path and controlled
  provider, must record at least three expected LLM calls and valid ATIF and
  verifier evidence; and
- `host-services-neighbor-v1`: continuous probes cover `slurmd.service`,
  `munge.service`, the certified SSH unit (`sshd.service` or `ssh.service`), and
  `nvidia-persistenced.service` where it is in the node baseline, plus controller
  `scontrol ping`, control-plane `/healthz`, and an authenticated registry
  `/v2/`. The certificate records the exact unit set so a missing expected unit
  is a failure rather than a skipped probe.

Each architecture runs five warm `terminus2-neighbor-v1` trials alone and five
with the adversarial build. All ten must succeed. The colocated median trial
wall time may be at most 120% of the solo median and no colocated trial may
exceed 150% of the solo maximum. Host probes must have zero failed samples,
restarts, OOM kills outside the builder cgroup, or node `DRAIN`/`DOWN`
transitions; their p95 latency must remain below both 500 ms and twice the solo
baseline. `MemAvailable` must remain above the certified reserve (at least 2048
MiB), I/O `full avg10` below 50, and D-state processes at or below 32; any
stricter cluster profile wins.

Evidence must additionally prove that:

- every supervisor, BuildKit, `RUN`, snapshotter, and network-helper PID remains
  in the allocation cgroup;
- CPU saturation, memory exhaustion, a fork bomb, excessive I/O, and disk/inode
  exhaustion cannot exceed the allocation limits;
- daemonization, double-forking, ignored signals, and nested namespaces leave no
  surviving process after cancellation;
- forbidden host paths, container sockets, node-local/cluster endpoints, and
  registry/control-plane credentials are inaccessible;
- direct IPv4/IPv6 and TCP/UDP/raw egress cannot bypass BPF policy by ignoring
  proxy variables, host loopback remains unreachable, and `/dev/fuse` is absent
  from Dockerfile processes even when the trusted snapshotter uses it;
- the concurrent trial remains inside its own limits and meets the fixture
  thresholds above;
- local state and mounts are absent after epilog, partial publications are
  retained only for the configured grace period, and the builder returns to
  scale zero;
- under continuous synthetic Loom trial demand, conflicting pending trial
  allocations are cancelled, no later Loom trial allocation jumps ahead of the
  pending higher-tier builder, and reusable workers drain as designed; and
- rollback to the exclusive backend succeeds without changing ready trial
  digests.

Non-exclusive activation remains fail-closed until the signed acceptance record
for the exact cluster configuration, kernel, Slurm configuration, builder
release, and BuildKit/helper release verifies successfully.

### End-to-end incident acceptance

For the original affected task/run, acceptance requires all of the following:

- the correct run store and task materialization are located;
- registration/submission creates or reuses the expected native build;
- the dynamic rootless builder publishes every component and the control-plane
  publisher validates its manifest/config and signs the exact publication
  statement;
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
5. Builders can wait behind active or external work but cannot be starved by
   newly admitted or pending Loom trial-worker capacity.
6. Only validated immutable digests with signed publication statements make a
   materialization ready.
7. Lease loss prevents stale readiness, regardless of registry side effects.
8. Build input never receives control-plane or registry credentials.
9. Local builder state is bounded and disposable; registry retention is
   reference-aware and fenced.
10. Missing containment, credentials, registry consistency, or rollout
    prerequisites fails closed before trial execution.
11. No synchronous Slurm Prolog depends on Loom control-plane availability.
12. One grant can bind at most one Slurm job; ambiguous submission never causes
    an unobserved retry.
13. Publication authority is repository-scoped, and cache, shadow, and
    production credentials are mutually unusable.
14. A shadow row, repository, or statement can never make a production
    materialization ready.
