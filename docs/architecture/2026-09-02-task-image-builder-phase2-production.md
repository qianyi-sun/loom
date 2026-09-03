# Task-image builder Phase 2 production design

**Status:** Approved for incremental implementation; production activation remains closed

**Date:** 2026-09-02

**Target branch:** `dev`

**Predecessor:** `archive/docs/architecture/2026-08-18-dynamic-task-image-builder-design.md`

## Decision

Loom will complete the permanent task-image builder as an allocation-contained,
rootless BuildKit provider. Builder work will run in ordinary, dynamically placed
Slurm allocations on shared eligible nodes. It will use neither a fixed node nor
`--exclusive`, a permanent reservation, or the host Docker/containerd socket.

This document narrows the approved predecessor design into production-sized
increments and fixes the interfaces between them. It does not relax any of the
predecessor's security or acceptance requirements. When the two documents differ
in implementation sequencing or component naming, this document controls.

Phase 1 remains the active rollback path throughout Phase 2 implementation,
shadow acceptance, architecture-by-architecture activation, and soak. Removing
the Phase 1 reservation or host-Docker path is a later, separately authorized
operation.

## Current state

PR #1517 supplied an inert foundation:

- strict disabled OLDLAB/x86_64 and GB10/arm64 provider policies;
- ordinary held Slurm request rendering without reservation, node pin, broad
  environment export, or host-runtime socket;
- durable one-invocation grants and authoritative ambiguous-submission recovery;
- immutable attempt/lease publication evidence; and
- migration `0108`.

The checked-in provider remains disabled. The prerequisite policy still has
`production_certification_allowed=false`, no certified nodes, and the
`phase2_guard_provider_release_missing` blocker. Existing tests prove that no
production composition constructs or submits the rootless provider.

Production activation is correctly blocked because the following authority is
not yet implemented:

- allocation and node identity projection;
- the root-owned node guard and pinned network policy;
- the allocation supervisor and allocation-local rootless BuildKit executor;
- short-lived repository-scoped registry credentials;
- digest-by-digest publication validation and signing;
- worker verification and one-use start authorization;
- shadow campaigns and adversarial two-architecture acceptance; and
- the architecture capacity fence that prevents trial arrivals from starving a
  queued builder.

## Non-negotiable invariants

1. A service trial worker never builds a task Dockerfile.
2. No task-authored process runs before containment and network policy are
   attached to its allocation cgroup.
3. BuildKit, every `RUN` process, RootlessKit, the snapshotter, and network
   helpers remain descendants of the exact Slurm allocation cgroup.
4. The root-owned guard never parses a task bundle, Dockerfile, registry
   response, or build argument.
5. The allocation receives no Slurm, database, cluster, node, or long-lived
   control-plane credential.
6. Raw secrets never enter Slurm arguments, comments, exported environment,
   build arguments, labels, logs, or persistent allocation storage.
7. One build grant authorizes one `sbatch` invocation and at most one exact held
   job. Binding commits before release.
8. A projected bootstrap credential is one-use, job-bound, short-lived, and
   delivered only as a sealed file descriptor.
9. Registry publication credentials name exact attempt/component repositories;
   a caller cannot request its own repository scope.
10. A registry `HEAD`, mutable tag, or builder report cannot make a
    materialization ready. Readiness requires verified immutable bytes and a
    signed control-plane publication statement.
11. Lease loss, guard-attestation loss, or job termination prevents credential
    renewal and readiness even when registry side effects already occurred.
12. Trial runtime creation requires a fresh one-use authorization serialized
    with publication-key revocation.
13. Dynamic builders use no permanent reservation, fixed node, or exclusive
    allocation.
14. Continuous Loom trial arrivals cannot jump ahead of an already-pending
    builder, while running trials are not preempted.
15. Local build state is quota-bounded and disposable; registry retention is
    reference-aware and fenced.
16. Phase 1 remains independently operable until both architectures pass soak.

## Non-goals

- Guaranteeing a fixed build-start latency. That would require reserved idle
  capacity or preemption and is a separate utilization/SLA decision.
- Emulating either production architecture. OLDLAB builds `linux/amd64` and
  GB10 builds `linux/arm64` natively.
- Protecting against an unknown host-kernel exploit. Rootless namespaces and
  BPF reduce authority and exposure but share the host kernel; that threat model
  requires a separately designed microVM provider.
- Moving the agent-install trial-image cache into this lifecycle, enabling
  cross-job BuildKit cache, or adding cross-task artifact deduplication.
- Replacing Slurm, the registry, or the existing materialization identity.
- Claiming reproducible output from mutable base tags or arbitrary network
  downloads. The verified publication digest is the execution authority.
- Removing Phase 1 capacity as part of Phase 2 implementation or activation.

## Trust and process boundaries

The control plane remains the authority for materialization, attempt/lease,
publication, trial eligibility, and retention state. Slurm remains the authority
for placement and the outer resource cgroup. The registry remains an artifact
store, not an execution authority.

Phase 2 adds two narrowly scoped processes:

- `loom-task-image-authority` is a dedicated internal service. It terminates
  mutual TLS, verifies a separately hashed node bearer principal, and owns
  projection, containment-attestation, session, registry-credential,
  publication-signing, and start-authorization transitions. Running this
  surface separately from the public control-plane API makes its client CA,
  routes, request limits, metrics, and network policy independently auditable.
- `loom-task-builder-node-guard` is a root-owned node daemon. It authenticates a
  local supervisor, verifies the live Slurm job/cgroup, installs the exact
  containment and BPF policy, transfers one sealed bootstrap descriptor, sends
  attestations, and quarantines only the builder capability when evidence
  becomes ambiguous.

Mutual TLS and the node bearer are defense in depth. TLS rejects clients outside
the node-guard CA before HTTP parsing. The application principal registry binds
one token digest to one cluster, node name, and allowed projection/attestation
scopes. Possession of only a certificate or only a bearer token grants nothing.
The certificate private key and bearer file are root-owned and never cross the
Unix socket.

The inert example registry at
`deploy/task-image-builder/authority-principals-v1.example.json` demonstrates
the on-disk digest format using the public, non-secret bearer strings
`example-only-oldlab-node-bearer-do-not-use` and
`example-only-gb10-node-bearer-do-not-use`. The JSON deliberately contains only
their SHA-256 digests, never these raw example strings. Production activation
must provision independently generated node credentials through the later
credential ceremony; neither example value is usable production authority.

The guard is implemented as an isolated CPython program installed from the
verified Loom host release and started with `/usr/bin/python3 -I -B`. Its local
protocol is bounded and uses only fixed system calls and precompiled,
digest-pinned BPF objects. It does not load plugins, import from writable paths,
invoke a shell, or accept executable paths or commands from a request. This
matches Loom's existing root Slurm guard operational model without introducing a
second compiler/toolchain trust chain. The release and adversarial suite treats
the interpreter, guard file, BPF object, and loader as one pinned unit.

## End-to-end flow

1. Registration freezes the task bundle and idempotently ensures native
   materializations. Trial submission repeats the ensure operation as the
   correctness backstop.
2. The capacity reconciler issues a durable build authority and held-job grant,
   consumes its one submission authority, reconciles the exact Slurm inventory,
   commits one job binding, and releases that held job.
3. The supervisor starts by `exec` directly under the Slurm batch step and sends
   only the nonsecret grant UUID to the local guard.
4. The guard obtains `SO_PEERCRED`, opens a pidfd immediately, and verifies the
   still-live peer, installed supervisor digest, dedicated UID/GID, batch-step
   leadership, exact Slurm job, and cgroup inode. It submits those derived facts
   through the node-authenticated authority API.
5. The authority locks the released grant, requires an unexpired exact job and
   node binding, and returns a one-use attachment challenge. It does not return
   a credential yet.
6. The guard creates the empty allocation-descendant `loom-builder` containment
   root and its `trusted-service` and `build-egress` children, programs positive
   PID/I/O/network limits, attaches and pins the exact BPF links/maps, moves only
   the pidfd-pinned supervisor, probes policy, and submits the canonical proof.
7. The authority atomically records the proof and encrypted replay receipt, then
   returns a short-lived random bootstrap secret. The guard places it in an
   `MFD_CLOEXEC|MFD_ALLOW_SEALING` memfd, adds every required seal, passes it over
   `SCM_RIGHTS`, and closes its allocation-side copy after acknowledgement.
8. The supervisor verifies seals, reads the payload once into locked non-dumpable
   memory, closes the descriptor, and exchanges it for a short-lived build
   session. The exact transport retry returns the same still-valid encrypted
   receipt; a second semantic exchange or changed idempotency body is rejected.
9. A fresh session with a matching guard attestation may claim one matching
   materialization. After claim, it receives a frozen bundle capability and
   independently renewable base-read and publication credentials. Publication
   repositories are derived from durable attempt/component state.
10. The supervisor starts RootlessKit directly in `build-egress` with
    `clone3(CLONE_INTO_CGROUP)`, then starts the pinned rootless OCI BuildKit
    worker. It builds one materialization at a time and records every observed
    component digest as cleanup evidence.
11. The publication service fetches registry bytes by digest, verifies the full
    OCI descriptor graph and platform, canonicalizes a
    `loom.task-image-publication/v1` statement, and signs it through the
    publication key. Only a complete same-attempt statement set can commit
    readiness.
12. A native trial worker receives immutable digests, full publication
    envelopes, and a signed keyset snapshot. Immediately before creating the
    first runtime, it obtains and consumes a one-use start authorization bound
    to the trial claim epoch and publication-revocation epoch.
13. An idle builder exits after its bounded grace period. Slurm epilog and the
    guard prove empty cgroups, mounts, storage, credentials, and pinned state.

## Projection authority and durable state

Submission state and credential state are deliberately separate. The existing
`TaskImageBuildGrant` continues to record `issued -> submitting -> bound ->
released|revoked`. A new immutable authority binding adds:

- purpose `production` or `shadow` and optional shadow campaign;
- environment, pool, native architecture, and Slurm request digest;
- builder-release, build-policy, containment-policy, and resource-profile
  digests; and
- issue and expiry times.

The migration refuses to infer those fields for existing rows. Because the
provider is disabled and no production composition can issue a grant, a nonempty
grant table at migration time is unexpected authority and fails the migration
closed for operator investigation.

`TaskImageBuildProjection` has one row per grant and a state of
`challenged`, `projected`, `exchanged`, `revoked`, or `expired`. It binds the
node principal, node boot ID, Slurm job, peer PID/executable, cgroup path/inode,
canonical request/proof/exchange digests, deadlines, secret hashes, encrypted
secret-store references, and the latest valid attestation generation. A
  separate append-only event table records every accepted state transition and
the first exact replay of each phase. A uniqueness constraint makes replay
auditing bounded rather than allowing a retry storm to grow the journal.

`TaskImageBuildContainmentAttestation` is append-only and unique by
`(grant_id, generation)`. It binds the cgroup inode, link/program/map IDs,
policy digest, resource-limit digest, issue time, and expiry. A newer generation
may extend liveness but cannot change the immutable attachment identity. Exact
replay is idempotent; a changed document at an existing generation is an
equivocation that revokes the projection and quarantines the capability.

The authority stores raw bootstrap/session response material only through the
existing authenticated-encryption `SecretStore`, in the same database
transaction as the row that references it. Database rows otherwise hold only
SHA-256 hashes. Expired or revoked tokens never authenticate even if encrypted
retention has not yet removed the replay receipt.

All transition methods lock the grant, projection, and relevant parent state in
one consistent order. They accept an explicit `now` for deterministic tests,
use canonical request digests as idempotency bindings, and return intentionally
indistinguishable authorization failures at the HTTP boundary.

## Node guard and containment

The guard listens only on a root-owned `SOCK_SEQPACKET` Unix socket. The maximum
message size, field count, pending-peer count, and request rate are fixed. The
only supervisor-supplied authority is a grant UUID; every security-relevant job,
identity, executable, and cgroup fact is derived locally.

The guard opens the peer pidfd before slow work and checks it at every boundary.
It reads the process executable through `/proc/<pid>/exe` without accepting a
path, hashes the opened file, verifies `SO_PEERCRED`, reads cgroup membership,
and compares Slurm controller/accounting facts with the grant. A dead peer,
changed inode, re-exec, job mismatch, supplementary privileged group, or stale
controller response aborts before projection.

The containment root is a descendant of the exact Slurm batch-task cgroup. The
guard never moves `slurmstepd`, writes an ancestor/sibling cgroup, or attaches a
program to a Slurm daemon. It applies the grant's positive PID and I/O ceilings
and verifies inherited CPU, memory, swap, and device limits. Failure to delegate
the required controllers removes the node feature and prevents claims.

Network attachment uses pinned cgroup `bpf_link` objects and root-only maps.
Each independently loaded scope has a singleton subject map; the pinned link
readback, rather than the currently executing task, binds that map instance to
the exact attachment cgroup. This keeps ingress attribution and nested
BuildKit descendants correct.
The endpoint and limiter inputs come only from a canonical root-owned policy
artifact whose byte digest is the grant's containment-policy identity; they are
never supplied by the allocation or an authority response.
Both IPv4 and IPv6 default deny. Connect, UDP send, socket lifecycle, ingress,
and egress hooks cover destination policy plus byte, packet, flow, and DNS
limits. `trusted-service` receives only authority/object-store/registry
endpoints; `build-egress` receives only audited DNS/package/base-image and exact
attempt publication endpoints. Attachment happens while cgroups are empty.
The guard crash policy stays pinned and deny-capable; a missing fresh
attestation stops renewal and new work.

Guard cleanup requires both terminal Slurm evidence and an empty allocation
cgroup. Ambiguous cleanup leaves links pinned and marks only
`loom_rootless_buildkit` unavailable on that node. It never drains the Slurm
node or changes unrelated jobs automatically.

## Allocation supervisor and BuildKit

The supervisor is a dedicated non-root executable and the sole process in the
submitted batch script. It validates the installed release and allocation,
obtains the sealed bootstrap, maintains the short-lived session, claims work,
starts/stops BuildKit, and reports typed evidence. It never calls Slurm
administration APIs.

RootlessKit uses only the pinned `slirp4netns` driver with host-loopback
disabled, IPv6 enabled, sandbox enabled, and seccomp enabled. BuildKit uses only
the rootless OCI worker with process sandboxing and the exact certified
snapshotter. Insecure entitlements, host networking, CDI/device injection,
SSH-agent forwarding, arbitrary host binds, and unpinned remote Dockerfile
frontends are rejected.

Job storage is local, empty at start, protected by byte and inode quotas, and
deleted after the allocation. Cross-job BuildKit cache remains disabled during
Phase 2 and shadow acceptance. Correctness therefore never depends on node-local
state.

## Credentials, publication, and trial start

Bootstrap, build session, object read, base-registry read, publication, cache,
and trial-start capabilities are distinct types and scopes. Renewal creates a
new bounded generation after a matching lease heartbeat and fresh guard
attestation; it never extends or broadens the old capability.

Production publication paths are exactly
`loom-task-image-attempts/<architecture>/<attempt-id>/<component>`. Shadow paths
are exactly
`loom-task-image-shadow/<campaign-id>/<architecture>/<attempt-id>/<component>`.
Registry repository authorization, not a tag prefix, enforces this separation.

Publication statements use RFC 8785 canonical JSON and Ed25519 domain-separated
signatures. Publication keys are distinct from execution-grant, bootstrap, and
registry keys. Routine rotation preserves verify-only keys. Compromise
revocation increments a durable epoch, quarantines affected materializations,
blocks new grants/starts, and races the start-authorization transaction on the
same locks.

Workers pin the execution-grant trust root and verify the full statement
envelopes and signed `PublicationVerificationKeysetV1`. The registry cannot
supply missing authority through an OCI referrer. An expired start receipt is
not renewable under the same trial claim epoch; the worker abandons/requeues and
must obtain a higher epoch.

## Scheduling and starvation

The overlapping builder partition has a strictly higher Slurm `PriorityTier`
than Loom's trial partition and the builder QoS caps one allocation per native
architecture. This does not reserve a node and does not preempt running work.

One durable `ArchitectureCapacityFence` per environment/cluster/architecture is
the serialization authority for both builder demand and trial-capacity writes.
Creating buildable demand moves the fence to `builder_pending`, advances its
epoch, sets the trial admission ceiling to zero, and marks conflicting pending
trial submissions for cancellation before any provider call. Trial-capacity
submission rechecks a short-lived signed witness immediately before `sbatch`.

After the configured soft wait threshold, reusable trial workers are marked to
drain after their current trial and cannot claim another. When the builder
starts, the fence records `builder_running` and the measured residual trial TRES
ceiling. On builder termination it moves atomically either back to
`builder_pending` if demand remains or to `open`. Stale or ambiguous state has no
valid witness and suppresses new trial capacity.

This prevents new Loom trials from starving a builder. A builder may still wait
for already-running trials or external/equal-higher-tier jobs. A hard start-time
SLA would require an explicit later reservation or preemption decision.

## Failure handling and observability

Deterministic Dockerfile, context, entitlement, or platform failures consume the
task attempt policy and expose a bounded user-actionable reason. Allocation,
registry, object-store, and public-dependency failures retry with a bounded
backoff. Repeated OOM, PID, I/O, storage, inode, or wall-time exhaustion does not
silently enlarge the resource profile. Containment, attachment, attestation, or
cleanup failures do not consume a task's deterministic budget; they revoke the
session, quarantine the node's builder capability, and page operations.

Metrics and structured events bind environment, architecture, grant, Slurm job,
attempt, and lease without secrets. They cover queue age, provider state, Slurm
pending reason, build phase duration, resource high-water marks, limit events,
network counters/drops, attestation age, credential generation/expiry,
publication verification, retention backlog, cleanup, and trials waiting on or
released by materialization. Alerts fire for no eligible node, stale capacity
fence, excessive queue age, repeated containment failure, link/map drift,
credential-renewal failure, registry inconsistency, and cleanup residue.

## Protected implementation sequence

Every increment is inert or shadow-only when merged, has its own branch and PR,
passes current-head protected CI, receives code review, and is squash-merged
before its successor starts.

1. **Phase 2A — projection authority core.** Extend grants with immutable
   purpose/release/policy/expiry authority; add canonical projection,
   attachment-proof, exchange, session, and attestation contracts; add locked
   durable state transitions with encrypted exact-replay receipts. No HTTP
   route, daemon, secret provisioning, or provider activation is included.
2. **Phase 2B — authority service and node guard release.** Deliver this as two
   protected, inert sub-increments so the network authority and root/kernel
   authority are reviewed independently. **Phase 2B1** adds the dedicated mTLS
   service, node principal registry, bounded guard-mediated APIs, and disabled
   deployment composition. **Phase 2B2** adds the root-owned guard, Unix
   peer/pidfd/cgroup verification, pinned BPF policy/ledger, sealed-memfd
   transfer, and systemd/install/conformance artifacts. The node guard is the
   only mTLS client and mediates bootstrap exchange; no client private key or
   node bearer enters the allocation.
3. **Phase 2C — allocation supervisor and rootless executor.** Add session-bound
   claim/heartbeat, release validation, quota-backed storage, RootlessKit and
   BuildKit startup, native component builds, typed cleanup, and no-cache
   behavior. It remains production-disabled.
4. **Phase 2D — publication and execution authority.** Add renewable exact
   registry credentials, OCI graph validation, signed publication statements,
   keyset rotation/revocation, reference-aware partial retention, immutable
   execution-grant bindings, and one-use trial-start authorization.
5. **Phase 3 — shadow campaigns.** Add an isolated shadow queue/repository and
   run functional and adversarial canaries on OLDLAB first and GB10 second.
   Shadow output cannot satisfy production readiness.
6. **Phase 4 — architecture fence and cutover.** Add the starvation fence and
   signed executor witness, prove scheduling races under shadow, then activate
   OLDLAB/x86_64 and GB10/arm64 separately with one builder each.
7. **Incident acceptance and soak.** Locate the authoritative store for
   `4139e767`, rematerialize through the rootless path, and require real LLM
   calls plus valid execution evidence. Preserve Phase 1 until the two-arch soak
   and rollback drill pass.

## Verification gates

Each code increment requires unit, integration, migration, lint, strict type,
and package-boundary tests. Security tests must mutate every identity, digest,
time, generation, scope, and replay binding independently.

Before a node may advertise `loom_rootless_buildkit`, host conformance must
prove cgroup v2 controller enforcement, subordinate IDs, pinned runtime hashes,
sealed memfd and pidfd behavior, exact snapshotter/network flags, quota cleanup,
BPF attach/probe persistence, and a fail-closed guard restart. A stale Slurm
feature label is insufficient; allocation preflight repeats the effective
checks.

Shadow acceptance then runs the versioned adversarial builder beside both a
normal Terminus-2 trial and host-service probes on each architecture. It must
prove resource ceilings, direct IPv4/IPv6/TCP/UDP/raw bypass denial, flow/DNS
rate limits, no credential visibility, no process/mount/storage residue, no
foreign OOM/restart/drain, expected LLM calls, and valid ATIF/verifier evidence.
The quantitative latency and resource thresholds remain those in the
predecessor design.

Production activation requires a signed record binding the exact Git release,
host release, kernel, Slurm/cgroup configuration, BuildKit/RootlessKit/helper
digests, BPF program/map policy, registry policy, keyset, and acceptance
fixtures. Drift closes certification rather than selecting a fallback.

## Rollback

Before production cutover, rollback is simply continued Phase 1 operation.
After one architecture cuts over, rollback stops new rootless claims, revokes
its sessions and credentials, drains only its exact allocations, and restores
new claims to the already-proven Phase 1 backend. Ready immutable digests remain
valid.

Rollback never rewrites the database, manually copies an image, prunes a shared
runtime, cancels a foreign Slurm job, or removes a reservation. Removing the
named Phase 1 reservation is deferred until after both architectures complete
soak and requires a separate exact-target approval and readback.
