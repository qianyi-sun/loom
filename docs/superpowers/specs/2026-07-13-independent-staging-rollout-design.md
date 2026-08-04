# Independent Staging Rollout Design

## Status and scope

Status: approved for implementation on 2026-07-13. Repository implementation
does not authorize shared-staging use before merge and live acceptance.

Membership amendment (2026-07-31): #822 restores the complete
`trt-gb10-1..15` inventory as the only canonical topology for trust, heartbeat,
health, scheduling, rollout, and acceptance. A healthy busy host remains visible
and reports reduced or zero capacity; a health failure is temporarily
unschedulable but is not removed from inventory. The former active-14 / 140-slot
exception is historical implementation debt and is not an acceptable steady
state.

This design implements the operator-independence portion of #803. Hongjian and
Devansh must each be able to update Loom staging after code has merged without
asking Qianyi to provide credentials, start a process, refresh a backup, or
recover a disconnected terminal. The normal path must remain attributable,
restartable, mutually exclusive, and subject to every existing protected
rollout gate.

The owner decision is strict: a new staging rollout may deploy only the exact
head of `origin/dev` fetched at request time. It may not accept a pull-request
ref, feature branch, tag, historical SHA, local commit, user-selected image tag,
or alternate remote. A resume continues the SHA already pinned by the original
request even if `origin/dev` advances later.

This slice does not grant production promotion authority, change GitHub
Environment policy, or weaken the merged active-GB10, backup, protected
preflight, environment-state, release-gate, or smoke acceptance. Under #822
that means all 15 hosts remain represented in heartbeat, health, scheduling,
and acceptance evidence, with allocatable capacity derived dynamically.
Production authority remains governed by #757.

## Root cause

The existing `loom cluster rollout staging` driver is restartable, but its
operator boundary is still a manual Qianyi-owned procedure:

1. The runbook launches a Qianyi user unit from `/home/qianyi/dev/loom` and
   relies on Qianyi's lingering user manager.
2. The fixed runner checkout can have a stale cached `origin/dev`. The driver
   resolves a local Git ref but neither fetches nor verifies the configured
   remote before pinning the candidate.
3. The staging preset references Qianyi-owned token, catalog, worker-env, and
   GB10 deploy-identity paths. The GB10 preflight correctly rejects a private
   key with group or world permission, so adding a shared ACL to that key is
   not a valid multi-user design.
4. Driver ownership in `state.json` records only PID, hostname, boot ID, and
   timestamps. It does not record an authenticated human initiator or request
   identity. Existing smoke actor strings are fixed labels rather than caller
   authentication.
5. Single-writer protection covers one evidence directory and one image tag.
   Two different tags can start two drivers. Protected subcommands take short
   mutation locks, but those locks are released between steps, so full
   rollouts can interleave.
6. The driver checks a pre-existing backup manifest but does not create the
   Postgres dump, MinIO snapshot, and protected Secret backup on which the
   manifest depends. Once the current backup expires, another operator still
   needs Qianyi's manual preparation.
7. `status`, `logs`, `resume`, and cancellation are shell recipes involving
   Qianyi's systemd user bus and evidence paths rather than a supported
   operator interface.

Hongjian and Devansh currently have Docker access and `NOPASSWD: ALL` on
`platform-dev`. Those grants are root-equivalent. A broker on the same host can
prevent accidental misuse, serialize rollouts, and provide a consistent audit
trail, but it cannot claim secret confidentiality or tamper resistance against
a deliberately privileged administrator. This design treats the three humans
as trusted host administrators, as explicitly requested. A future strict
confidentiality boundary would require a separate runner host without their
interactive root/Docker access, followed by credential rotation.

## Goals

- Hongjian and Devansh can independently start, inspect, resume, and, with an
  audited reason, cancel a staging rollout.
- A start always fresh-fetches and pins the current merged `origin/dev` head.
- A stale backup is refreshed before any rollout mutation.
- Exactly one full staging driver may be pending or running at a time,
  regardless of image tag.
- A request is bound to the authenticated OS caller and persisted through
  request, unit, rollout, mutation-lock, and summary evidence.
- The protected credentials and deploy key are consumed by a dedicated service
  identity and never placed in argv, environment dumps, or evidence.
- Disconnects, service restarts, failed steps, and cancellation preserve the
  existing verify-and-resume behavior; no operator edits `state.json`.
- Installation, revocation, preflight, and rollback are reproducible rather
  than host-local oral procedure.

## Non-goals

- Unmerged branch or pull-request deployment.
- User-selected refs, SHAs, image tags, cluster configs, token sources,
  concurrency outside the configured bound, or force-lock flags.
- Simultaneous or queued staging rollouts. A second request fails visibly while
  another request owns staging.
- Production deployment or production Environment approval.
- Direct token, private-key, worker `.env`, Postgres, MinIO, or Kubernetes
  Secret viewing as part of the rollout interface.
- Removal of Hongjian's or Devansh's existing host-admin/Docker access in this
  slice. The broker is the supported normal path, not a security sandbox from
  root-equivalent users.
- OLDLAB-2 UID normalization, which remains tracked by #803.

## Chosen architecture

### Dedicated service identity and trusted installation

Create a non-login `loom-rollout` account on `platform-dev` and enable linger
only for that account. Its root-managed installation consists of:

- `/opt/loom-staging-runner/repo`: a clean runner repository whose only allowed
  `origin` is `https://github.com/qianyi-sun/loom.git`;
- `/opt/loom-staging-runner/venv`: the rollout/cluster dependency environment;
- `/etc/loom/staging-rollout.toml`: root-owned non-secret paths and bounded
  policy, including the remote URL, `dev` ref, checkout, state root, rollout
  root, kubeconfig, maximum GB10 prep concurrency, smoke team identity, and
  expected redacted token fingerprints;
- `/var/lib/loom-staging-rollout`: service-owned request ledger, active pointer,
  attempt events, and a dedicated GB10 deploy identity;
- `/run/loom-staging-rollout`: runtime locks and transient metadata;
- a root-owned client at `/usr/local/bin/loom-staging-rollout` and broker entry
  point that operators cannot modify.

Generate a new service-owned GB10 deploy key instead of copying Qianyi's
private key. A fresh install bootstraps its public key on the complete 15-host
inventory, which is also the authority for validation and legacy-key
revocation. The private file remains owner-only so
the existing GB10 identity preflight continues to fail closed on group/world
exposure. A broker-owned cluster-config materialization supplies that key
path; users cannot override it.

The service account receives the Docker, kubeconfig, staging data-root, token,
catalog, and worker-env access needed by the existing driver. Values remain in
protected file sources. The migration does not print or copy values into the
repository, request ledger, process arguments, or journal. The existing
Qianyi-named shared directory may remain as a transitional source path; the
runtime dependency is the `loom-rollout` service account and its declared
access, not a Qianyi login session.

### Operator group and command surface

Create `loom-staging-operators` with Qianyi, Hongjian, and Devansh as members.
The supported interface is:

```text
loom-staging-rollout start [--dry-run]
loom-staging-rollout status [REQUEST_ID]
loom-staging-rollout logs REQUEST_ID [--follow]
loom-staging-rollout resume REQUEST_ID
loom-staging-rollout cancel REQUEST_ID --reason TEXT
loom-staging-rollout cleanup-incomplete-backup REQUEST_ID
```

`--dry-run` is the only `start` option. It performs caller, installation,
remote, fresh-fetch, candidate-binding, singleton-availability, and redaction
checks, then writes a non-active preview request without creating a backup,
unit, rollout evidence directory, or mutation. `start` accepts no ref, SHA,
tag, path, environment, config, secret, smoke, skip, force, or free-form command
option. `resume` accepts only an existing request ID. `status` and `logs`
return redacted broker, systemd, and rollout state. `cancel` requires a
non-empty bounded reason and records both the original initiator and cancelling
operator.
`cleanup-incomplete-backup` is an explicit, audited recovery command for one
failed pre-launch request; it accepts no path or retention selector.

The root-owned client invokes one fixed broker command as `loom-rollout`
through a `NOSETENV` sudoers rule. The broker derives caller username and UID
from the authenticated sudo/PAM context and validates membership itself; it
does not accept an actor field from argv or an inherited environment variable.
The broker uses a fixed `PATH`, clears `PYTHONPATH` and unsafe Git/Python
variables, and rejects unknown arguments. Because the current operators retain
broader sudo by owner decision, this is an operational attribution boundary,
not protection from a malicious root administrator.

### Candidate binding

For every new request, while holding the broker launch mutex:

1. Verify the runner checkout and configured `origin` are root/service-owned,
   clean, and match the single allowed repository URL.
2. Fetch exactly `refs/heads/dev` into `refs/remotes/origin/dev`, with pruning
   and no user-supplied refspec.
3. Resolve the fetched commit to one 40-character SHA and derive
   `staging-<sha7>`.
4. Record the remote URL, fetch timestamp, ref, SHA, and image tag in a private
   request envelope.
5. Pass that envelope to the driver. Non-dry-run protected staging invocation
   without a valid service-owned envelope fails before evidence creation or
   mutation.

A new request never accepts a historical merged SHA. Operational rollback is a
new merged revert on `dev`, followed by another normal request. Resume is the
only path allowed to run a SHA that is no longer the current `origin/dev` head,
and it must match the original rollout's persisted inputs.

### Pre-roll backup ownership

A non-dry `start` performs a broker-owned backup phase before launching the
rollout driver. It creates a new timestamped backup root, then uses the
existing documented mechanisms to produce:

- a Postgres `loom` database dump;
- MinIO snapshots of the required staging buckets;
- a protected Kubernetes/runtime Secret backup;
- a metadata-only `backup-manifest.json` containing paths, sizes, hashes, and
  verification state but no raw secret values.

The broker runs `loom cluster backup check` with the driver's required
remaining freshness window and passes that exact immutable manifest path into
the rollout. A failed or incomplete component backup removes no previous
backup, never advances the active pointer, and prevents all rollout mutation.
Retention cleanup remains a separate bounded policy and never runs
automatically on the failure path. The explicit
`cleanup-incomplete-backup REQUEST_ID` recovery runs only after the broker has
recorded (or safely recovered) a pre-launch backup failure, while the launch
mutex proves no concurrent broker publication. It refuses any manifest-backed
or `latest`-selected root and retains the request ledger.

### Full-lifecycle singleton

Add a staging-driver singleton separate from the existing protected mutation
lease. Reusing the mutation lease for the whole driver would deadlock when a
child `cluster up` or environment-state command acquires the same lease.

The broker serializes active-pointer creation under a short launch mutex. It
atomically writes `active.json` in `pending` state before starting a transient
`loom-rollout` user unit named from the request ID. The unit obtains and holds
the full-lifecycle lock until the driver exits. If unit creation fails, the
broker records the failed launch and clears the pointer. `status` reconciles the
pointer against the unit, driver PID/boot ID, request ledger, and `state.json`
instead of trusting one source alone.

A second `start` or `resume` while the singleton is owned fails with safe
details: request ID, initiator, pinned SHA, image tag, unit, current step, and
start time. It never queues or preempts the active request. Existing per-step
mutation leases remain unchanged and continue to protect direct lower-level
commands.

### Detached execution, cancellation, and resume

The dedicated account's lingering user manager owns transient units named
`loom-staging-rollout-<request-id>-<attempt>.service`. The unit uses a fixed
working directory, fixed venv, `UMask=0077`, no interactive terminal, bounded
environment, and explicit credential/config paths. It injects required
non-secret smoke identity and expected redacted fingerprints from the protected
broker config.

Cancellation sends the normal termination signal to the unit and records a
cancel event; it does not edit rollout state or delete partial evidence. Any
operator may cancel an abandoned shared-staging request so recovery never
depends on the original initiator, but the reason and cancelling identity are
mandatory. Resume launches a new attempt for the same request, image tag,
target ref, pinned SHA, backup manifest, and evidence directory with
`--resume`. The existing driver verifies partial steps and refuses mismatched
inputs.

### Attribution and evidence

Each request envelope and attempt event records only safe metadata:

- schema version and request/attempt IDs;
- caller username and numeric UID derived by the broker;
- operation (`start`, `resume`, or `cancel`) and bounded reason;
- allowed remote, `origin/dev`, resolved SHA, and image tag;
- fetched-at, requested-at, started-at, finished-at, and result timestamps;
- systemd unit, rollout ID, evidence path, current step, and terminal status;
- runner version/config hashes and redacted credential fingerprints.

`RolloutContext`, `inputs.json`, `state.json`, mutation-lock evidence, and the
final summary carry request ID and initiating operator. Resumes add events
rather than overwriting original attribution. Raw tokens, private keys,
credential file bodies, full environments, private endpoints, and arbitrary
subprocess errors are excluded.

The service account owns the request ledger and writes atomically with private
temporary files, `fsync`, rename-without-replacement where required, and
directory `fsync`. Normal operators use `status` and `logs`; they do not need to
edit evidence. Existing host-admin privileges mean this is not tamper-proof
against a deliberate root user, which is documented rather than obscured.

## Data flow

```text
Hongjian or Devansh
  -> root-owned client / authenticated sudo context
  -> broker validates operator and fixed command schema
  -> fresh-fetch exact origin/dev HEAD
  -> create request envelope + acquire staging singleton
  -> create and verify protected backup
  -> detached loom-rollout systemd user unit
  -> existing rollout driver with broker envelope
  -> all protected steps, complete 15-host GB10 inventory, release gate, smoke
  -> request/attempt/state/summary evidence
  -> status and redacted logs available to every operator
```

## Failure behavior

- Unauthorized or revoked caller: reject before Git, backup, evidence, or
  systemd activity.
- Dirty/writable-by-operator checkout, wrong remote, fetch failure, or malformed
  SHA: reject before backup or mutation.
- Active request: reject with redacted owner/status; do not queue.
- Backup component or freshness failure: record request failure and leave the
  last valid backup untouched; do not start the rollout.
- Broker power loss after `backup_started`: with no envelope, attempt, active
  pointer, or unit, recover a generic backup failure under the launch mutex and
  allow only the request-bound incomplete-backup cleanup command.
- Credential missing, too permissive, or fingerprint-mismatched: fail before
  the first network mutation.
- Broker/unit restart: reconcile the active pointer, unit, PID/boot ID, and
  driver state; never assume success from a stale pointer.
- Driver step failure: preserve normal evidence and release the lifecycle lock
  only after terminal bookkeeping; allow explicit resume.
- Cancellation: preserve resumable state and record the cancelling operator and
  reason.
- Any GB10 host missing, unreachable, or unconverged, or any drift in the fixed
  15-host topology authority: retain the existing fail-closed
  prep/release-gate result. Report health separately from current allocatable
  capacity; do not silently remove a host from inventory.

## Alternatives rejected

### Share Qianyi's deploy key and remaining env files

This is fast but creates one unrevocable human identity, conflicts with the
owner-only GB10 key preflight, and cannot attribute or independently revoke an
operator. It also leaves stale checkout, backup, concurrency, and resume
problems unsolved.

### Give each operator a complete personal runner and GB10 key

Distinct keys improve GB10 SSH attribution but do not solve shared Control
Plane credentials, backup generation, whole-rollout locking, evidence schema,
or shared staging conflicts. Three independently drifting checkouts, venvs,
kubeconfigs, and 15-host key distributions enlarge the failure surface.

### Continue `sudo -u qianyi systemd-run --user`

This is technically possible today because both users have broad sudo. It
still records Qianyi as the unit/driver owner, depends on Qianyi's checkout and
linger, permits stale refs and unbounded arguments, and has no whole-driver
singleton. It is rejected for both normal operation and break-glass. Emergency
recovery must preserve the service-owned envelope and exact pinned SHA.

### Reuse the existing mutation lock for the full driver

The driver invokes child commands that acquire the same environment lease.
Holding it around the parent would self-deadlock. The lifecycle singleton must
be a separate lock and evidence namespace.

## Implementation boundaries

The implementation should add focused modules under `src/loom_cli/rollout/`
for broker policy, request/attempt persistence, candidate fetch/binding,
lifecycle locking, and client/status formatting. Existing driver, context,
state, evidence, and mutation-lock modules should gain only the fields and
hooks required to carry the broker request identity. Host convergence should be
repo-owned through checked-in templates/install/preflight commands for the
service account, sudoers rule, systemd units, directories, modes, runner
checkout, dependencies, kubeconfig, credential references, and GB10 public-key
trust.

Do not turn `scripts/ops/deploy_environment.sh` into this runner. That script
owns GitHub workflow partial deployment, while this design owns the physical
full staging rollout lifecycle.

## Verification and acceptance

Repository tests must prove:

- `start` accepts no candidate override and pins only a freshly fetched
  allowed `origin/dev` head;
- wrong remote, stale cached ref, feature ref, tag, historical SHA, dirty or
  operator-writable runner, and unsafe environment injection fail closed;
- a valid service-owned request envelope is mandatory for non-dry-run protected
  staging;
- caller identity cannot come from argv and persists through context, state,
  lock evidence, attempts, and summary;
- two different tags cannot own staging concurrently;
- backup failure prevents unit launch and mutation, while a verified fresh
  backup is pinned for resume;
- cancel preserves resumable state and records actor/reason;
- status/log output is redacted and request-to-rollout correlation is complete;
- token and private-key values never appear in process arguments, request
  evidence, driver logs, or summaries;
- existing rollout, resume, mutation-lock, all-host, release-gate, and smoke
  tests remain green.

Live acceptance on `platform-dev` must prove:

1. The service preflight passes for checkout ownership, clean/fresh remote,
   dependencies, Docker, kube context, data paths, credentials, backup tools,
   all 15 GB10 connections, and exact full-15 topology validation.
2. Hongjian and Devansh each run `start --dry-run`; evidence records the correct
   distinct OS user and the same fresh merged `origin/dev` SHA.
3. An unauthorized user is rejected, and an intentional simultaneous request
   is rejected with the active request's safe status.
4. One operator starts a real detached rollout, disconnects, reconnects, and
   reads status/logs without Qianyi involvement.
5. The other operator can inspect the request and is authorized to resume it if
   a real failure or cancellation occurs. Repository and isolated integration
   tests prove the resume path when the live rollout completes normally; the
   live acceptance does not manufacture a driver/pod failure or edit state.
6. The rollout completes every existing step, all 15 GB10 hosts appear in
   heartbeat/health evidence, healthy hosts converge, the release gate and
   smoke pass, and request/attempt/rollout correlation is complete. A busy host
   may advertise zero allocatable capacity without being removed from inventory.
7. No raw secret appears in argv, journald, request evidence, rollout evidence,
   or the final summary.

No broad host-admin or Docker permission is revoked by this slice. If a later
owner decision requires strong secret isolation or unforgeable audit against
administrators, migrate the same broker to a dedicated host, revoke interactive
root/Docker access there, rotate all staging tokens/catalog credentials and the
GB10 deploy identity, and rerun the full acceptance gate.

## Rollback

Stop new broker requests, remove operators from `loom-staging-operators`, and
retain the request/rollout evidence. Do not delete or edit an in-progress
driver state. Emergency recovery disables admission, repairs or reinstalls the
broker from clean merged `dev`, and then uses
`loom-staging-rollout resume REQUEST_ID`. The broker reuses the existing
service-owned envelope and exact pinned SHA while preserving identity checks,
lifecycle locking, and attempt attribution; no operator invokes the lower-level
driver directly. Removing the service GB10 public key and credential ACLs after
the rollout reaches a safe terminal state revokes the new runner without
changing Qianyi's existing emergency key.

Rollback does not weaken cluster state, restore an older image directly, or
permit an arbitrary ref. A code rollback is a merged revert on `dev` followed
by another normal staging request.
> **Superseded GB10 membership policy (2026-07-31):** This archived design
> preserves historical active-14/140-slot assumptions for provenance; they are
> not operative architecture or acceptance requirements. The current contract
> keeps `trt-gb10-1..15` in heartbeat, health, scheduling, and acceptance, with
> dynamic resource availability and no static node-7 exclusion. See
> `docs/architecture/gb10-dynamic-capacity.md` and #822.
>
