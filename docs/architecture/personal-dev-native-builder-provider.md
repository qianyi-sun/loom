# Native GB10 provider for personal candidate builds

## Status and scope

This design replaces emulated `linux/arm64` personal-candidate builds with a
native GB10 provider. The existing OLDLAB Kubernetes/gVisor path remains the
native `linux/amd64` provider. One candidate attempt still produces both
platform bundles, the existing trusted management-side exporter still verifies,
scans, and publishes every image, and the final candidate remains
`personal-dev-only` and non-promotable.

The change is deliberately separate from Loom task execution and task-image
materialization. The GB10 agent has no Slurm command, worker token, task-image
token, capacity-manager credential, database credential, registry credential,
or Kubernetes credential. It cannot raise executable capacity or submit a Loom
task. It receives only one signed personal-build grant at a time per advertised
free slot and the same source-read/artifact-write capability class already used
by the Kubernetes builder.

The immediate cause is the protected issue #1280 acceptance result. Native
amd64 completed, while arm64 under QEMU in KVM gVisor exited `139` during an
ordinary Node/npm step. A separate `QEMU_CPU=cortex-a72` probe failed identically.
Further QEMU tuning is not an accepted production path.

This document covers management-plane state, the agent protocol, native host
containment, architecture dispatch, failure recovery, rollout, and acceptance.
It does not activate Slurm or task capacity.

## Host feasibility observation

A read-only observation on 2026-08-30 established that the GB10 controller
`gx10-01c7` is `aarch64`, has `/dev/kvm`, cgroup v2, Docker 28.3.3 with the
systemd cgroup driver and overlay2, 20 CPUs, and approximately 120 GiB RAM.
`slurmctld` and the existing Docker daemon were active. `runsc` was absent.

These observations select the host but do not authorize activation. The
installer repeats every prerequisite check against the protected release and
stops if the host, Docker, KVM, cgroup, route, disk, or service inventory has
changed. The provider uses a separate Docker daemon and data root so it never
reconfigures or restarts the controller's existing daemon.

## Approaches considered

### Selected: signed pull agent plus dedicated gVisor Docker runtime

Run a release-pinned agent on the native arm64 controller. The agent polls the
management API with an Ed25519-signed, replay-fenced status request. It executes
each grant in two separate KVM-gVisor containers on a dedicated Docker daemon:
an authority-free rootless BuildKit container and a restricted client container
holding the attempt capabilities. A per-grant bridge is the only connection
between them.

This preserves the existing split between untrusted Dockerfile execution and
the client holding upload authority. Separate gVisor sandboxes are at least as
strong as the current separate-container gVisor Pod boundary. The dedicated
daemon gives the agent no access to existing controller containers.

### Rejected: continue QEMU on OLDLAB

The exact trusted image failed twice at the same native executable boundary.
More CPU-model or binfmt tuning would retain emulation as a release-critical
dependency without evidence that it can build the repository.

### Rejected: submit personal builds through the task-image Slurm provider

That path couples arbitrary personal source, task-image materialization,
exclusive Slurm grants, and another authority's autoscaler lifecycle. It also
prevents independent cancellation and accounting. Personal candidate builds
must remain separately identifiable and revocable.

### Rejected: use rootless runc or the host Docker daemon directly

Rootless namespaces reduce ordinary privilege but do not provide the KVM gVisor
kernel boundary already required for personal source. Reusing the host daemon
would also give the agent visibility into unrelated controller containers and
would require a daemon restart to add the runtime.

## Security and authority invariants

The implementation must preserve all of these invariants:

1. Candidate-controlled bytes execute only inside KVM gVisor.
2. The BuildKit sandbox receives no source URL, artifact upload capability,
   agent key, Docker socket, host path, service token, or registry credential.
3. The restricted client and BuildKit run in different gVisor sandboxes. The
   client opens no listener. BuildKit listens only on its per-grant bridge.
4. The agent receives no MinIO, database, registry, Kubernetes, Slurm, task,
   worker, capacity-manager, or owner credential.
5. The agent can reach only its dedicated Docker socket. The agent container
   never receives the controller's existing Docker socket.
6. Every grant is bound to candidate, build attempt, whole-attempt lease epoch,
   platform, builder image digest, runtime profile digest, contract digest,
   agent key, and stable agent instance.
7. A grant is never reassigned within one whole-attempt lease epoch. A restarted
   agent may resume only its own exact grant. Reassignment requires the central
   coordinator to acquire a higher whole-attempt lease epoch, which also changes
   the artifact key.
8. A late or duplicated agent response cannot make a superseded attempt ready.
9. Management, not the agent, verifies the uploaded object binding and performs
   artifact verification, scanning, registry publication, and candidate-ready
   commit.
10. Provider installation and agent readiness do not alter task capacity,
    Slurm state, or the executable-new-capacity ceiling.

## Components

### Management-side platform dispatcher

`PersonalDevBuildCoordinator` remains the owner of the whole candidate-attempt
lease. Its executor becomes a composite of two platform executors:

- `linux/amd64`: the existing Kubernetes/gVisor executor, restricted to the
  OLDLAB native job;
- `linux/arm64`: a native-agent executor backed by durable provider grants.

Both platform operations start concurrently. The composite calls the existing
trusted exporter only after both exact artifact objects are present. Failure of
either platform cancels the sibling, and the coordinator's existing `finally`
cleanup removes the amd64 namespace and cancels the arm64 grant before the
whole attempt is finished.

The legacy all-Kubernetes executor remains available only when the native
provider is explicitly disabled, preserving local tests and rollback. An
operational personal-dev plan must require the native provider; it may not
silently fall back to QEMU.

### Native-build grant store

Migration `0123` adds two tables.

`personal_dev_native_builder_agents` records one current agent identity:

- stable `instance_id` and Ed25519 `key_id`;
- platform (`linux/arm64`) and provider (`gb10-gvisor-docker-v1`);
- protocol version, host architecture, host boot ID;
- exact agent image, builder image, and runtime profile digests;
- configured maximum concurrency and observed active grant IDs;
- last request nonce, strictly monotonic signed-request timestamp, signed status
  digest, readiness evidence digest, and freshness timestamp.

`personal_dev_native_build_grants` records one platform execution:

- UUID grant ID;
- candidate and build-attempt IDs;
- whole-attempt lease epoch and platform;
- provider, required agent instance/key, builder image, runtime profile, and
  contract SHA-256;
- deterministic source and artifact object bindings;
- state `queued`, `running`, `succeeded`, `failed`, or `cancelled`;
- running agent, started/heartbeat/finished timestamps;
- bounded failure reason;
- canonical signed completion evidence and its SHA-256.

There is exactly one grant for `(attempt_id, attempt_lease_epoch, platform)`.
The artifact key remains the existing key, which already contains the whole
attempt lease epoch. Because a grant is not reassigned within that epoch, no
two writers can hold capabilities for the same key. A new coordinator lease
uses a different key; the candidate GC contract therefore remains unchanged.

All state transitions lock the grant and its parent build attempt. Issuance and
claim require the parent attempt to be `running`, currently leased, and at the
same lease epoch. Heartbeat and completion repeat that predicate. Cleanup may
cancel a grant after the parent lease is lost, but cannot revive it.

### Signed pull protocol

The internal API exposes three signature-gated operations:

1. `POST /api/v1/internal/personal-dev/native-builder/poll`
2. `POST /api/v1/internal/personal-dev/native-builder/grants/{id}/heartbeat`
3. `POST /api/v1/internal/personal-dev/native-builder/grants/{id}/complete`

Every request is canonical JSON signed by the configured Ed25519 agent key and
contains a UUID nonce plus a UTC timestamp. For each agent or running grant,
the service requires that timestamp to be strictly newer than the last accepted
message and records it with the nonce under the same row lock. The service
therefore rejects both an exact replay and an older captured request still
inside the freshness window. It also rejects an unknown key, invalid signature,
stale/future timestamp, identity drift, or non-canonical payload before reading
or mutating a grant.

The poll describes the agent's exact runtime identity, capacity, and managed
grant inventory. It updates the agent freshness row, returns grants the agent
must cancel, and returns at most one queued grant when the reported active count
is below the approved maximum. Replaying a poll is rejected by the persisted
nonce. A running grant is returned again only to its same stable agent instance,
allowing restart recovery.

The secret response contains:

- canonical build contract;
- short-lived source GET URL;
- bounded artifact POST URL and fields;
- exact maximum bytes and expiry;
- builder image and runtime profile digests.

The service creates remote capabilities with a separate S3 client that signs
against `LOOM_SVC_MINIO_PUBLIC_ENDPOINT`. Native-provider startup requires a
non-userinfo HTTPS origin. The internal MinIO client remains authoritative for
intake, HEAD verification, export, and GC.

Heartbeat reports the exact grant and whole-attempt lease. The response is
`continue=true` only while all central predicates still hold. The agent begins
cancellation immediately on any other response or on an inability to refresh
within the bounded heartbeat grace period.

Completion is success or failure, never both. Success includes signed canonical
runtime evidence. Before committing `succeeded`, management HEADs the expected
artifact object and independently requires its content type, size, attempt ID,
whole-attempt lease epoch, candidate SHA, and platform metadata. It does not
trust an agent-supplied object digest or registry claim.

### Native agent

The agent is a minimal multi-architecture OCI image, separate from the sandbox
image. A root-owned systemd unit starts it through the controller's existing
Docker daemon with:

- an immutable image reference;
- read-only root filesystem, all capabilities dropped, and no-new-privileges;
- the agent private key and CA mounted read-only;
- only the dedicated builder-daemon Unix socket;
- no host PID, IPC, network or device namespace; no primary Docker socket; and
  no host filesystem mount other than the exact key, CA, and dedicated socket.

The agent uses the dedicated Docker API to create exact resources labeled with
grant ID, attempt ID, whole-attempt lease epoch, platform, agent instance,
builder image, runtime profile, and contract digest. It never evaluates a
Dockerfile or extracts source itself.

For each grant it:

1. creates a private bridge from the reserved provider address pool;
2. creates the BuildKit container and the restricted client container without
   starting either;
3. copies the canonical contract and capabilities into the client's private
   read-only container layer with UID/GID 1000 and mode 0400;
4. starts BuildKit, waits for its fixed health probe, then starts the client;
5. heartbeats centrally while waiting;
6. inspects exact image, runtime, security, network, restart, exit, and OOM
   evidence;
7. submits signed completion and waits for acknowledgement;
8. stops/removes the two containers and their bridge.

At startup, the agent inventories every managed object. It resumes an exact
running grant, removes an object only after the service declares the grant
non-current, and stops on duplicate or shape-drifted managed objects. It never
prunes foreign objects.

The agent supports two simultaneous grants by default. This is configurable but
must equal the management-approved value. The existing global and per-owner
candidate-build limits remain the outer admission authority.

### Builder containers

The existing personal builder image remains the only sandbox image.

The BuildKit container:

- uses the dedicated `runsc-personal-dev-native` runtime;
- runs UID/GID 1000;
- has only `SETUID` and `SETGID` in its bounding set, unconfined OCI seccomp
  inside gVisor, and no initial `no_new_privs`;
- invokes the existing fail-closed RootlessKit launcher in a new fixed TCP mode;
- sets `no_new_privs=1` before BuildKit and every Dockerfile descendant;
- mounts only private tmpfs state, home, run, and temporary directories;
- receives no contract, capability, source, output, socket, or host mount.

The client container:

- uses a separate gVisor sandbox on the same per-grant bridge;
- runs UID/GID 1000, drops all capabilities, uses the default seccomp profile,
  has `no_new_privs=1`, and has a read-only root filesystem;
- receives only the contract and two bounded capabilities;
- uses a memory-accounted tmpfs workspace;
- connects to the one fixed BuildKit TCP port on the grant-local bridge;
- verifies source and every OCI output before uploading the canonical artifact.

BuildKit is authority-free and per-grant, so candidate code may corrupt or
terminate only its own build. It cannot reach another grant's bridge or client.

### Dedicated Docker and network runtime

The host installer adds, without changing the existing daemon:

- the pinned arm64 gVisor release under a versioned root;
- an exact runsc configuration using KVM, sandbox networking, exclusive file
  access, no host UDS/FIFO bridge, no raw networking, OCI seccomp enforcement,
  no set-ID elevation, release enforcement, and the gVisor marker;
- `loom-personal-dev-builder-dockerd.service` with its own socket, exec root,
  data root, default runtime, bridge address pool, logs, and systemd slice;
- an exact nftables table for the provider bridges;
- the separately activated agent unit.

The dedicated daemon has `iptables=false`. The Loom-owned nftables table:

- allows traffic within one grant bridge;
- drops routing between different grant bridges;
- denies IPv4 private, loopback, link-local, carrier-grade, benchmark,
  documentation, multicast, and reserved destinations;
- supplies an operator-pinned public recursive DNS pair and allows only those
  public DNS endpoints plus public TCP 80/443 egress;
- performs NAT only for the reserved provider source range;
- admits established return traffic; and
- enables no IPv6 or inbound publication.

The installer rejects an address-pool or route conflict. It never edits an
unrelated nftables table.

The daemon slice is bounded to 900% CPU and 72 GiB memory. Each grant is bounded
to one 1-CPU/16-GiB client and one 3-CPU/16-GiB BuildKit container, finite PIDs,
and finite tmpfs mounts. At the approved concurrency of two, at least 11 host
CPUs and approximately 48 GiB RAM remain outside the provider slice for
`slurmctld` and controller services. Startup also requires a finite disk-space
reserve.

BuildKit state is per-attempt tmpfs, so candidate/base image caches disappear
with the grant. The dedicated daemon persistently stores only trusted builder
release images; the agent image remains in the controller's existing daemon.
Convergence on each daemon retains the current and immediately previous
applicable trusted release and removes only unreferenced older managed images
after exact digest and zero-container checks.

## End-to-end data flow

1. An owner uploads a deterministic source archive through the existing API.
2. The existing build coordinator claims the candidate attempt and starts its
   whole-attempt lease heartbeat.
3. The composite executor starts the OLDLAB amd64 Job and durably issues the
   GB10 arm64 grant.
4. The signed GB10 agent poll claims the arm64 grant and receives short-lived
   public S3 capabilities.
5. The two native providers independently upload their platform bundles under
   the same attempt and lease binding.
6. Management independently verifies both object bindings, OCI bundles, image
   architectures, scanner results, and registry publications.
7. Management publishes one multi-platform index per Loom component and commits
   candidate readiness under the whole-attempt lease.
8. Personal environment reconciliation and activation continue unchanged.

No source, capability, or candidate result enters the task/capacity manager.

## Failure, cancellation, and recovery

- **Coordinator restart:** the agent may continue while the parent lease is
  current. If a new coordinator acquires a higher lease epoch, the old grant is
  cancelled and its artifact key is no longer authoritative.
- **Agent restart:** the same stable agent inventories and resumes its exact
  containers/grant. No new writer is created.
- **Agent loss:** the coordinator's deadline expires and fails the attempt.
  Reapplying the owner environment creates the existing higher build-attempt
  sequence; a grant is not stolen mid-epoch.
- **Heartbeat loss:** after the grace period the agent stops both containers and
  reports no success.
- **One platform fails:** the sibling platform is cancelled, all ephemeral
  resources are cleaned, and the candidate attempt fails with a bounded phase.
- **Late upload or completion:** a stale whole-attempt lease cannot be exported
  or committed. A completion with a mismatched signature, nonce, object, agent,
  image, runtime, or contract is rejected.
- **Management exporter failure:** the existing whole-attempt lease and cleanup
  behavior applies. Registry publication remains solely management-owned.
- **Host or runtime drift:** the agent reports unavailable and claims no grant.
  There is no runc, QEMU, Slurm, or host-daemon fallback.
- **Cleanup failure:** candidate completion fails closed. The agent retains
  labeled resources for exact startup reconciliation; it does not broaden
  deletion scope.

## Configuration and readiness

New service settings are inert by default and include native-provider
enablement, approved agent instance/key, public key file and digest, agent and
runtime freshness, protocol version, runtime profile digest, agent image,
approved concurrency, and public object-store origin.

Operational render requires all of the following before owner apply is exposed:

- native provider enabled;
- trusted release contains immutable arm64 agent and builder images;
- management has the exact agent public key and runtime profile digest;
- public object-store origin is HTTPS and passes a bounded capability probe;
- one fresh signed agent row matches the release, host, protocol, runtime, and
  concurrency contract;
- the agent reports zero drift and no unknown managed resources;
- OLDLAB Kubernetes runtime remains ready;
- workers remain unavailable and executable-new-capacity remains zero during
  personal-environment acceptance.

Shadow mode may render and inspect this contract while leaving both the native
provider and agent inactive.

## Rollout and rollback

1. Merge the protocol, migration, composite executor, inert agent, installer,
   status, and tests behind disabled configuration.
2. Produce a trusted multi-architecture release including the agent and builder
   image digests.
3. Download and verify the pinned gVisor arm64 archive; stage and verify the
   dedicated daemon/runtime without starting the agent.
4. Run a protected gVisor two-container conformance on the GB10 controller,
   including native arm64 repository build steps, capability separation,
   network denials, cgroup limits, and cleanup.
5. Install the agent key and exact release-bound unit, start the agent, and
   require fresh signed readiness with zero grants.
6. Render and apply a new zero-capacity personal operational plan.
7. Run two concurrent owners, requiring two simultaneous arm64 grants and two
   simultaneous native amd64 Jobs, exact multi-platform publications, owner
   isolation, routes, and authenticated API teardown.
8. Return to the exact approved operational zero-capacity state.

Rollback disables native-provider claim first, waits for or cancels exact
grants, stops/removes only labeled agent resources, stops the agent and
dedicated daemon, removes its exact nftables table, and then removes only
byte-identical managed runtime files. It never restarts the existing Docker
daemon, invokes Slurm, deletes a personal namespace directly, or changes task
capacity.

## Verification requirements

Unit and integration tests must cover:

- canonical signed protocol parsing, freshness, nonce replay, key and identity
  drift;
- database constraints, one-writer issuance, same-agent resume, parent-lease
  fencing, cancellation, idempotent exact completion, and stale completion;
- architecture dispatch and no operational QEMU fallback;
- concurrent success, sibling cancellation, coordinator restart, agent restart,
  and cleanup failure;
- public/internal S3 client separation and exact artifact HEAD binding;
- Docker create/inspect contracts for both containers, distinct sandbox/network
  identity, resource limits, mounts, security options, and labels;
- no Docker socket, Slurm, registry, database, task, worker, or capacity
  credential in either sandbox or agent configuration;
- strict host profile parsing, safe install/remove, dedicated-daemon isolation,
  nftables exactness, address conflict rejection, and image retention;
- release/render/status contracts and inert defaults; and
- migrations at head plus downgrade behavior.

Protected host verification must additionally prove:

- exact arm64 gVisor bytes and version under KVM;
- client and BuildKit are separate gVisor sandboxes;
- client capability and BuildKit PID/mount isolation;
- RootlessKit helper bounding capabilities and `no_new_privs` transition;
- native `linux/arm64` build and OCI metadata;
- public HTTP(S)/DNS allow and private/cross-grant denial;
- per-container and aggregate cgroup ceilings;
- agent restart resume and central cancellation;
- two-owner concurrency; and
- zero Slurm mutations, zero Loom task submissions, worker unavailability, and
  executable-new-capacity ceiling zero throughout acceptance.

## Non-goals

This provider does not build task images, execute trials, choose worker pools,
change Slurm reservations, configure the global capacity manager, cache
candidate build layers across attempts, or make personal candidates promotable.
It does not authorize nonzero development capacity.
