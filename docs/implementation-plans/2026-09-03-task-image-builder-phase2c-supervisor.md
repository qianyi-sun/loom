# Task-image builder Phase 2C supervisor and rootless executor implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Every
> production behavior follows a red/green/refactor cycle, and every completion
> or merge claim requires fresh verification.

**Goal:** Deliver the production-disabled allocation supervisor, renewable
session-bound materialization lifecycle, quota-backed disposable storage, and
native rootless BuildKit executor for both supported architectures.

**Architecture:** The root node guard remains the only mTLS/node-bearer client
and proxies a bounded local descriptor protocol for session renewal and build
lease operations. A static non-root Go supervisor consumes all secret-bearing
responses through sealed memfds, launches the pinned RootlessKit process
directly into the already-attached `build-egress` cgroup with
`clone3(CLONE_INTO_CGROUP)`, and serially produces validated OCI-layout output
under a guard-created project-quota directory. A deterministic composite
release binds the guard, supervisor, patched rootless runtime, BPF policy, and
authority contracts without installing any activation surface.

**Tech stack:** Python 3.11, FastAPI, SQLAlchemy/PostgreSQL, RFC 8785, encrypted
`SecretStore`, Linux cgroup v2/BPF/pidfd/memfd/project quota, Go 1.23.4 static
binaries, RootlessKit 3.1.0, BuildKit 0.32.2, `slirp4netns` 1.3.4,
`fuse-overlayfs` 1.17, Docker Buildx, pytest, Ruff, strict mypy, `go test`,
`go vet`, and protected GitHub CI.

**Spec:** `docs/architecture/2026-09-02-task-image-builder-phase2-production.md`

## Global constraints

- Base every change on verified squash merge
  `8620eb3b340954db0bdb0b9107f01884a89c6dab` in the dedicated Phase 2C
  worktree.
- Keep both rootless provider policies disabled,
  `production_certification_allowed=false`, `certified_nodes=[]`, and
  `phase2_guard_provider_release_missing` present throughout this increment.
- Do not create a live guard or supervisor config, activation marker,
  `current` link, systemd enablement, service/socket, BPF pin, node feature, or
  positive production replica count.
- The guard is the only process that holds the authority client key and node
  bearer. The supervisor receives no Slurm, database, Kubernetes, node,
  object-store root, registry root, or long-lived control-plane credential.
- Secret-bearing bootstrap, session, and bundle-capability payloads cross the
  local boundary only in fully sealed anonymous memfds and never appear in
  arguments, environment, paths, ledgers, metrics, journals, or error text.
- No task-authored process starts until the allocation cgroups and default-deny
  BPF links are attached. RootlessKit and every descendant start directly in
  the exact `build-egress` cgroup.
- Runtime execution paths are content-addressed under
  `/opt/loom-task-image-builder-provider/releases/<release-sha256>/`; no
  executable lookup uses `/opt/.../current`, `$PATH`, or a caller-supplied
  path.
- Keep the composite provider-release SHA-256 and the native supervisor ELF
  SHA-256 as separate authority fields. The grant binds both; the release
  digest selects the directory and the executable digest authenticates
  `/proc/<pid>/exe`. Never compare an ELF digest to a manifest digest.
- Replace the upstream `rootless-runtime-v1` BuildKit and RootlessKit binaries
  with checksum-bound source builds containing `golang.org/x/crypto v0.55.0`;
  do not certify binaries exposed to CVE-2026-56854.
- Job storage is empty at attachment, local, quota-bounded by bytes and inodes,
  assigned to the fixed site project ID, and disposable. Cross-job cache,
  import cache, and export cache are disabled.
- Phase 2C never marks a materialization ready and never publishes registry
  bytes. A successful local build returns a typed in-process publication
  handoff; until Phase 2D supplies that handoff, the real main program releases
  the lease as retryable infrastructure work without consuming the
  deterministic task-failure budget.
- Preserve Phase 1 files, units, reservations, and runtime behavior exactly.
- Do not modify or push `docs/superpowers/**` or `.superpowers/**`.

---

### Task 1: Append-only session generations and atomic attestation renewal

**Files:**

- Create: `migrations/versions/0129_task_image_builder_session_generations.py`
- Modify: `src/loom/db/schema.py`
- Modify: `src/loom_task_image_authority/contracts.py`
- Modify: `src/loom_task_image_authority/store.py`
- Modify: `src/loom_control_plane/task_image_build_environment.py`
- Test: `tests/integration/test_task_image_session_generation_migration.py`
- Test: `tests/unit/test_task_image_projection_contracts.py`
- Test: `tests/unit/test_task_image_build_environment.py`
- Test: `tests/integration/test_task_image_projection_store.py`

**Interfaces:**

- `TaskImageBuildSessionGeneration` is append-only and unique by
  `(grant_id, generation)` and by `session_id`. It stores the token hash,
  encrypted secret reference, canonical public session JSON/SHA-256, the bound
  attestation generation/SHA-256, and issue/expiry times. Generations after the
  first additionally store a renewal ID, canonical public renewal SHA-256, and
  predecessor session ID; exact generation `1` has all three fields null. It
  never stores a raw token, and no generation row is updated or deleted by a
  runtime transition.
- `TaskImageBuildProjection.session_generation` and the existing session
  columns mirror only the current generation. Migration `0129` backfills an
  exact generation `1` row for every structurally valid exchanged projection
  and aborts on an incomplete or contradictory legacy session.
- `TaskImageBuildSessionV2` adds positive `generation` and a distinct nonzero
  `session_id`. Its maximum lifetime remains 15 minutes but its actual expiry
  is the minimum of that lifetime, the grant expiry, and the new containment
  attestation expiry; renewal is no longer bounded by the consumed bootstrap
  expiry.
- `TaskImageSessionRenewalV1` contains `renewal_id`, `grant_id`, current
  `session_id`, current positive `session_generation`, current `session_token`,
  the complete next `TaskImageContainmentAttestationV1`, and `observed_at`.
- `renew_task_image_build_session(session, *, principal, request, now,
  secret_store, session_token_factory, session_id_factory) ->
  TaskImageBuildSessionV2` locks grant, projection, current generation, and
  attestation in that order; validates the current token before changing the
  attestation; atomically appends the next attestation and session generation
  and advances only the projection's current pointer; and returns an encrypted
  exact replay for one identical `renewal_id`. Reuse of that ID with a
  different canonical public request is a conflict and does not change either
  current pointer.
- `authorize_task_image_build_session(..., session_id, session_generation,
  raw_session_token, now)` accepts only the projection's exact current session
  and current attestation. Superseded, skipped, expired, replay-mutated, or
  revoked generations are indistinguishable authorization failures.
- `TaskImageBuildGrantAuthorityV2` separates
  `builder_release_sha256` (the canonical composite provider manifest) from
  `supervisor_executable_sha256` (the native ELF). Projection compares the
  observed executable only with the latter. V1 authority remains parseable for
  migration and historical replay, where its `builder_release_sha256` retains
  its original meaning as the supervisor ELF digest, but a V1 grant cannot
  enter any new Phase 2C claim transition. New issuance produces only V2.

- [ ] **Step 1: Write failing migration tests**

  Create an exchanged projection under both a legacy V1 grant and a V2 grant,
  each with a complete legacy session, and assert
  upgrade produces generation `1`, the current projection pointer, both unique
  constraints, foreign keys, time/hash/reference checks, and downgrade removes
  only the new table/column. Insert each incomplete legacy shape and assert the
  upgrade aborts rather than inferring authority.

- [ ] **Step 2: Run the migration test and confirm RED**

  Run: `.venv/bin/pytest -q tests/integration/test_task_image_session_generation_migration.py`

  Expected: collection fails because migration `0129` and the generation model
  do not exist.

- [ ] **Step 3: Implement migration and ORM state**

  Add the exact checks and indexes described above and update new grant
  issuance to the V2 release/ELF binding. Preserve V1 parsing only for
  historical state and reject it at the Phase 2C claim boundary. Use a single
  `INSERT ... SELECT` backfill guarded by a preceding contradiction query; do
  not synthesize tokens, IDs, timestamps, or digests.

- [ ] **Step 4: Run migration tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/integration/test_task_image_session_generation_migration.py`

- [ ] **Step 5: Write failing V2 contract and renewal-store tests**

  Mutate independently the grant, principal, node, current session ID,
  generation, token, token hash, attestation generation/digest/attachment,
  renewal ID/body, observation time, and each expiry. Assert exact replay
  returns the same token and adds at most one replay event; changed replay
  revokes nothing but returns conflict; attestation equivocation durably
  revokes the projection.

- [ ] **Step 6: Run contract/store tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_projection_contracts.py tests/integration/test_task_image_projection_store.py`

- [ ] **Step 7: Implement atomic renewal and current-generation authorization**

  Reuse the existing canonicalization and encrypted-secret helpers. Store only
  token SHA-256 outside `SecretStore`, compare tokens with
  `hmac.compare_digest`, and keep the initial bootstrap exchange as generation
  `1` of `TaskImageBuildSessionV2`.

- [ ] **Step 8: Run all Task 1 tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/integration/test_task_image_session_generation_migration.py tests/unit/test_task_image_projection_contracts.py tests/integration/test_task_image_projection_store.py`

- [ ] **Step 9: Commit Task 1**

  Commit message: `feat(builder): add renewable session generations`

---

### Task 2: Session-bound materialization lease and frozen bundle capabilities

**Files:**

- Modify: `migrations/versions/0129_task_image_builder_session_generations.py`
- Modify: `src/loom/db/schema.py`
- Create: `src/loom/task_image_build_plan.py`
- Create: `src/loom_task_image_authority/materializations.py`
- Create: `src/loom_task_image_authority/bundle_capability.py`
- Modify: `src/loom_task_image_authority/contracts.py`
- Modify: `src/loom_task_image_authority/config.py`
- Modify: `src/loom_task_image_authority/api.py`
- Modify: `src/loom_control_plane/task_image_materializations.py`
- Test: `tests/unit/test_task_image_build_plan.py`
- Test: `tests/unit/test_task_image_bundle_capability.py`
- Test: `tests/integration/test_task_image_authority_materializations.py`
- Test: `tests/integration/test_task_image_materialization_store.py`
- Test: `tests/integration/test_task_image_authority_api.py`

**Interfaces:**

- Migration `0129` adds nullable legacy-compatible `grant_id`, `session_id`,
  `session_generation`, and nonzero `claim_id` columns to
  `TaskImageMaterializationAttempt`, with an all-null-or-all-present check and
  exact foreign keys to the session-generation authority. Rootless claims must
  populate all four; Phase 1 claims remain null and unchanged.
- `derive_task_image_build_plan(row, authorization) ->
  TaskImageBuildPlanV1` derives, rather than accepts, `builder_id`, native
  platform, task/checksum, metadata checksum, component names, Dockerfile and
  context paths, timeout, byte/file ceilings, and OCI output names. Paths are
  canonical relative POSIX paths below the bundle, components are exactly
  `task` and sorted `sidecar:<name>` entries declared by the frozen
  `TaskConfig`, and the plan contains no URL or credential.
- `claim_session_materialization(session, *, authorization, claim_id, now,
  lease_seconds) -> (TaskImageMaterialization, TaskImageBuildPlanV1)` derives
  architecture and builder identity from the authorized current session,
  requires a V2 release/ELF grant binding, performs an exact-idempotent claim,
  and rejects another grant/session trying to reuse `claim_id`.
- Start, heartbeat, infrastructure release, and deterministic failure methods
  accept only `(authorization, materialization_id, attempt_id, lease_epoch,
  operation_id, now)`. Caller-supplied builder IDs, architectures, attempt
  counts, task paths, and repository scopes are absent.
- `TaskImageBundleCapabilityProvider.issue(plan, *, now) ->
  TaskImageBundleCapabilityV1` accepts only a canonical `s3://bucket/prefix/`
  source, lists at most 2,000 exact objects and 512 MiB, rejects traversal,
  duplicates, empty bundles, redirects, and non-HTTPS public origins, and
  returns sorted single-object presigned GET URLs expiring in at most 15
  minutes. The supervisor must still verify the full bundle content and mode
  digests after download.
- The guard-authenticated authority routes are:
  `PUT /v1/projections/{grant}/sessions/{generation}/renew`,
  `POST /v1/projections/{grant}/materializations/claim`, and
  `PUT /v1/projections/{grant}/materializations/{id}/{start|heartbeat|release|fail|bundle}`.
  Every mutation body is strict/canonical and contains the current short-lived
  session secret; all responses are bounded. The bundle response is marked
  secret-bearing and is never emitted by metrics or exception bodies.

- [ ] **Step 1: Write failing build-plan tests**

  Assert exact primary/sidecar ordering and paths. Mutate absolute/traversing
  Dockerfile/context paths, missing metadata SHA-256, nonnative architecture,
  no Dockerfile-backed component, duplicate sidecar name, invalid timeout, and
  oversized limits; each must fail before a lease is claimed.

- [ ] **Step 2: Run build-plan tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_build_plan.py`

- [ ] **Step 3: Implement the pure server-derived build plan**

  Keep this module free of FastAPI, SQLAlchemy sessions, object-store clients,
  subprocesses, Docker, and registry code. Validate with the existing frozen
  `TaskConfig` model and emit one strict frozen Pydantic contract.

- [ ] **Step 4: Write failing session-bound lease tests**

  Cover claim exact replay, concurrent claims, wrong/stale/superseded session,
  wrong attempt/epoch/materialization, lease expiry, start replay, heartbeat
  replay, deterministic failure, infrastructure release, and containment
  failure. Prove only deterministic failures advance the deterministic failure
  budget and Phase 1 methods retain their prior behavior.

- [ ] **Step 5: Run lease tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/integration/test_task_image_authority_materializations.py tests/integration/test_task_image_materialization_store.py`

- [ ] **Step 6: Implement session-owned lease transitions**

  Lock current session authority before the materialization row. Bind the
  immutable attempt row to the exact claim/session; derive
  `builder_id = "rootless:" + session_id.hex`; use explicit `now`; and append
  one bounded operation event per exact idempotency key. Infrastructure release
  returns the row to `queued` without advancing its deterministic failure
  counter.

- [ ] **Step 7: Write failing capability and HTTP tests**

  Use a fake S3 lister/presigner. Mutate source bucket/prefix, key ordering,
  relative path, object count/size, URL scheme/origin, expiry, session fields,
  route grant/materialization IDs, node principal, body length, and response
  length. Assert public validation errors never include a token, URL, source
  key, task config, or exception text.

- [ ] **Step 8: Run capability/API tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_bundle_capability.py tests/integration/test_task_image_authority_api.py`

- [ ] **Step 9: Implement bounded capability provider and routes**

  Add explicit authority settings for the public HTTPS S3 origin, expected
  bucket, maximum objects/bytes, and expiry. Keep deployment defaults absent so
  startup remains unavailable until a later credential ceremony. Inject the
  provider in tests; do not add a production object-store secret in Phase 2C.

- [ ] **Step 10: Run all Task 2 tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_build_plan.py tests/unit/test_task_image_bundle_capability.py tests/integration/test_task_image_authority_materializations.py tests/integration/test_task_image_materialization_store.py tests/integration/test_task_image_authority_api.py`

- [ ] **Step 11: Commit Task 2**

  Commit message: `feat(builder): bind materializations to build sessions`

---

### Task 3: Guard-mediated renewal, project-quota storage, and terminal cleanup

**Files:**

- Modify: `src/loom_task_image_builder_guard/models.py`
- Modify: `src/loom_task_image_builder_guard/config.py`
- Modify: `src/loom_task_image_builder_guard/protocol.py`
- Modify: `src/loom_task_image_builder_guard/authority.py`
- Modify: `src/loom_task_image_builder_guard/ledger.py`
- Modify: `src/loom_task_image_builder_guard/slurm.py`
- Modify: `src/loom_task_image_builder_guard/service.py`
- Create: `src/loom_task_image_builder_guard/storage.py`
- Modify: `deploy/task-image-builder/guard-config-oldlab-v1.example.json`
- Modify: `deploy/task-image-builder/guard-config-gb10-v1.example.json`
- Test: `tests/unit/test_task_image_builder_guard_protocol.py`
- Test: `tests/unit/test_task_image_builder_guard_authority.py`
- Test: `tests/unit/test_task_image_builder_guard_storage.py`
- Test: `tests/unit/test_task_image_builder_guard_ledger.py`
- Test: `tests/unit/test_task_image_builder_guard_service.py`
- Test: `tests/integration/test_task_image_builder_guard_local_flow.py`

**Interfaces:**

- The strict local operations become `project`, `exchange`, `renew`, `claim`,
  `start`, `heartbeat`, `bundle`, `release`, `fail`, `finish`, and `ack`.
  Secret-input operations require exactly one sealed memfd containing the
  current session wire document; no JSON field can carry a token, URL, task
  path, build argument, or credential.
- `AuthorityClient` owns fixed route methods for those operations. It parses
  only session envelopes and nonsecret lease acknowledgements. It treats a
  bundle capability body as opaque bounded bytes, immediately seals it in a
  memfd, and never deserializes task config, bundle entries, Dockerfiles,
  registry responses, or build arguments.
- `ProjectQuotaStorage.prepare(grant_id, *, byte_limit, inode_limit) ->
  JobStorage` safely opens the pre-provisioned root, requires its exact mount
  device/project-quota mode/owner/mode, creates only
  `jobs/<canonical-grant-id>`, assigns the configured numeric project ID with
  `FS_IOC_FSSETXATTR`, applies hard byte/inode limits with `quotactl`, verifies
  readback, then changes the exact job directory to UID 993/GID 980 mode 0700.
  The returned path/inode/device/project/quota digest is included in the
  attachment proof and every later attestation.
- The fixed project ID is safe because the site QoS admits at most one active
  rootless builder allocation per architecture. `prepare` fails if another
  nonempty project directory or quota usage exists; it never clears foreign
  usage to make admission pass.
- `finish` stores a nonsecret typed supervisor cleanup report and moves the
  ledger to `finishing`; it does not remove anything while the job is live.
  Guard reconciliation requires peer death, exact terminal Slurm controller
  and accounting evidence, empty descendant cgroups, no remaining mount, an
  empty job directory, and quota usage equal to the one project-owned job-root
  inode before deleting that directory. It then requires zero quota usage,
  clears and verifies the quota record, and deletes BPF pins and cgroups in
  that order. Ambiguity preserves deny pins and storage evidence and withdraws
  only `loom_rootless_buildkit`.
- The successful `projected` response transfers exactly three ordered
  capabilities with `SCM_RIGHTS`: the sealed bootstrap memfd, the open quota
  job-directory FD, and the open `build-egress` cgroup-directory FD. The JSON
  response fixes their roles and expected device/inode identities. No later
  path lookup is used for bundle writes or `clone3`; the guard retains its own
  independent descriptors for attestation and cleanup.
- After exchange, the guard no longer advances an attestation independently.
  The supervisor returns its current sealed session before the renewal
  deadline; the guard creates the next attestation and performs the atomic
  authority renewal. A missed renewal revokes/quarantines the session rather
  than extending it without possession proof.

- [ ] **Step 1: Write failing strict config/storage tests**

  Cover symlink/root escape, wrong mount/device/owner/mode, absent project quota,
  reused/nonempty project, project-ID mismatch, byte/inode limit mismatch,
  partial ioctl/quotactl write, readback drift, and cleanup with nonzero usage,
  a mount, a symlink, a changed inode, or another device.

- [ ] **Step 2: Run storage tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_storage.py`

- [ ] **Step 3: Implement direct project-quota storage operations**

  Use `O_NOFOLLOW|O_DIRECTORY|O_CLOEXEC`, fd-relative operations, stable
  `fstat`, `FS_IOC_FSGETXATTR/FS_IOC_FSSETXATTR`, and `quotactl` through fixed
  ctypes structures. Do not invoke a shell, `xfs_quota`, `setquota`, `rm`, or a
  caller-selected command. Cleanup walks descriptor-relative and refuses
  symlinks or mount-device changes. Treat the empty project-owned root inode as
  the only permitted pre-delete quota usage; delete it first, prove usage is
  zero, then clear and verify its quota limits.

- [ ] **Step 4: Run storage tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_storage.py`

- [ ] **Step 5: Write failing local-protocol/authority/ledger tests**

  For each new operation mutate field sets, descriptor count/order/role,
  seals/content and directory device/inode/type,
  grant/session/attempt/epoch/operation IDs, current session hash, response
  size, response operation, and exact replay binding. Prove the ledger stores
  only SHA-256/public IDs and never raw session/capability bytes.

- [ ] **Step 6: Run guard protocol tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_protocol.py tests/unit/test_task_image_builder_guard_authority.py tests/unit/test_task_image_builder_guard_ledger.py`

- [ ] **Step 7: Implement bounded proxy operations and ledger states**

  Add fixed request encoders/response validators with absolute monotonic
  deadlines. Keep the node bearer in the existing Authorization header and the
  current session only in the mTLS request body; redact all `GuardError`
  messages to fixed codes.

- [ ] **Step 8: Write failing lifecycle/reconciliation tests**

  Exercise exact project/exchange/renew/claim/start/heartbeat/bundle/release/
  fail/finish sequences over real `SOCK_SEQPACKET` sockets and memfds. Inject
  guard crash/restart before and after every storage, ledger, authority, cgroup,
  and pin boundary. Prove live/ambiguous/terminal Slurm observations select
  recover, quarantine, or exact cleanup without draining/cancelling a node or
  touching a foreign path.

- [ ] **Step 9: Run lifecycle tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_service.py tests/integration/test_task_image_builder_guard_local_flow.py`

- [ ] **Step 10: Implement renewal and terminal reconciliation**

  Prepare storage before the attachment proof, keep its descriptor identity in
  the live allocation, require current-session possession for each renewal and
  lease operation, and perform terminal cleanup only after exact terminal
  Slurm evidence plus empty readback.

- [ ] **Step 11: Run all Task 3 tests and confirm GREEN**

  Run: `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_protocol.py tests/unit/test_task_image_builder_guard_authority.py tests/unit/test_task_image_builder_guard_storage.py tests/unit/test_task_image_builder_guard_ledger.py tests/unit/test_task_image_builder_guard_service.py tests/integration/test_task_image_builder_guard_local_flow.py`

- [ ] **Step 12: Commit Task 3**

  Commit message: `feat(builder): mediate quota-backed build sessions`

---

### Task 4: Static supervisor bootstrap, secret memory, and fixed local client

**Files:**

- Create: `cmd/loom-task-image-builder-supervisor/main.go`
- Create: `cmd/loom-task-image-builder-supervisor/config.go`
- Create: `cmd/loom-task-image-builder-supervisor/protocol.go`
- Create: `cmd/loom-task-image-builder-supervisor/secret.go`
- Create: `cmd/loom-task-image-builder-supervisor/session.go`
- Create: `cmd/loom-task-image-builder-supervisor/config_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/protocol_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/secret_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/session_test.go`
- Create: `deploy/task-image-builder/supervisor-config-oldlab-v1.example.json`
- Create: `deploy/task-image-builder/supervisor-config-gb10-v1.example.json`

**Interfaces:**

- The binary accepts exactly `--grant-id <canonical-nonzero-uuid>`; release,
  socket and runtime paths come only from its compiled release root plus one
  root-owned mode-0444 config at the compiled path. Dynamic cgroup and storage
  authority comes only from the two guard-transferred directory descriptors.
  It rejects unknown arguments and inherited environment keys outside a fixed
  Slurm identity allowlist; it clears the inherited environment and constructs
  locale, timezone, HOME, and TMPDIR itself below the quota directory.
- `LoadConfig(path string, expectedRelease string) (Config, error)` uses strict
  duplicate-key rejecting JSON, `Lstat`/`Open`/`Fstat` identity checks, exact
  owner/mode, native architecture, absolute content-addressed paths, and
  SHA-256 verification for every executable/config member.
- `SecretBuffer` sets `PR_SET_DUMPABLE=0`, disables core dumps, verifies a
  received fd is anonymous, regular, link-count zero, CLOEXEC, and has exactly
  `F_SEAL_SEAL|SHRINK|GROW|WRITE`; reads once into `mlock`ed memory; closes the
  fd; never implements `String`/JSON/text marshaling; and zeroes plus `munlock`s
  on `Close`.
- `GuardClient` uses one `AF_UNIX/SOCK_SEQPACKET|SOCK_CLOEXEC` connection per
  operation, exact bounded packets, `SCM_RIGHTS`, kernel credentials, response
  ID acknowledgement, and absolute deadlines. It exposes typed
  `Project`, `Exchange`, `Renew`, `Claim`, `Start`, `Heartbeat`, `Bundle`,
  `Release`, `Fail`, and `Finish` methods without a generic operation or route
  selector.
- `GuardClient.Project` returns one owned `AllocationCapabilities` object that
  contains the validated bootstrap secret and the exact workspace/cgroup
  directory descriptors. Every partial receive closes every right.
- `SessionManager.WithCurrent(func(*SecretBuffer) error)` lends the session
  only for the callback lifetime and never returns a copyable byte slice;
  `Renew(ctx)` sends a newly sealed copy to the guard, atomically swaps only a
  fully validated next generation, and destroys the superseded token.

- [ ] **Step 1: Write failing config and argument tests**

  Cover unknown/duplicate fields, symlinks, writable files/directories, changed
  inode, wrong release hash, native-architecture mismatch, non-content-addressed
  path, manifest/ELF digest confusion, extra argument/environment authority,
  inherited HOME/TMPDIR, and zero/noncanonical grant IDs.

- [ ] **Step 2: Run config tests and confirm RED**

  Run in the pinned Go toolchain container:
  `docker run --rm -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Config|Arguments)'`

  Expected: package or symbol-not-found failure.

- [ ] **Step 3: Implement strict startup and config loading**

  Use only the Go standard library. Compile default paths and the expected
  release digest with `-ldflags`; no environment variable or `$PATH` lookup may
  override them.

- [ ] **Step 4: Write failing memfd and seqpacket tests**

  Use real socketpairs/memfds. Mutate seals, CLOEXEC, file type/link count,
  descriptor count/order/type/inode, packet truncation, credentials,
  schema/field set,
  response/ack UUID, and timeouts. Assert `%v`, `%s`, JSON, panic, and logs
  cannot reveal sentinel secret text.

- [ ] **Step 5: Run protocol tests and confirm RED**

  Run: `docker run --rm -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Secret|Protocol|Session)'`

- [ ] **Step 6: Implement secret memory, local client, and renewal manager**

  Use `syscall.Recvmsg`, `ParseSocketControlMessage`, `ParseUnixRights`,
  `F_GET_SEALS`, `Mlock`, `Munlock`, `Prctl`, and fixed JSON structs. Close all
  received rights on every error and copy current session bytes only into a new
  sealed memfd immediately before a guard request.

- [ ] **Step 7: Run the complete supervisor foundation tests and confirm GREEN**

  Run: `docker run --rm -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor`

- [ ] **Step 8: Commit Task 4**

  Commit message: `feat(builder): add static allocation supervisor`

---

### Task 5: Patched rootless runtime and cgroup-native OCI build executor

**Files:**

- Create: `deploy/task-image-builder/Dockerfile.rootless-runtime-v2`
- Create: `deploy/task-image-builder/rootless-runtime-v2.json`
- Create: `cmd/loom-task-image-builder-supervisor/process_linux.go`
- Create: `cmd/loom-task-image-builder-supervisor/process_other.go`
- Create: `cmd/loom-task-image-builder-supervisor/download.go`
- Create: `cmd/loom-task-image-builder-supervisor/checksum.go`
- Create: `cmd/loom-task-image-builder-supervisor/executor.go`
- Create: `cmd/loom-task-image-builder-supervisor/oci.go`
- Create: `cmd/loom-task-image-builder-supervisor/process_linux_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/download_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/checksum_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/executor_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/oci_test.go`
- Test: `tests/ops/test_task_image_builder_rootless_runtime_v2.py`

**Interfaces:**

- The Dockerfile builds BuildKit `v0.32.2` at signed-tag commit
  `991535e0973488b6a429096d21fa13f81f2d89d8` and RootlessKit `v3.1.0` at
  signed-tag commit `62d2101fbbe4f79bc845a337c4e868d27ff602c9`
  from checksum-bound archives with Go `1.26.7`, changes only
  `golang.org/x/crypto` to `v0.55.0`, uses `-trimpath -buildvcs=false`, and
  proves both module metadata and final amd64/arm64 binary SHA-256 values.
- `LaunchInCgroup(ctx, executable, argv, env, cgroupDirFD) (*Process, error)`
  opens/hashes the fixed executable, sets `SysProcAttr.UseCgroupFD=true` and
  `CgroupFD` to the open directory (never `cgroup.procs`), requires the child
  cgroup inode before release, and treats kernels
  without `clone3(CLONE_INTO_CGROUP)` as unsupported. It never writes the child
  PID to `cgroup.procs` after fork and never falls back to ordinary `fork/exec`.
- `DownloadBundle` accepts only the sealed `TaskImageBundleCapabilityV1` and
  the guard-transferred workspace directory FD, uses
  an exact TLS minimum/CA/server name and no proxy/redirect, creates files with
  `openat2(RESOLVE_BENEATH|NO_SYMLINKS|NO_MAGICLINKS)`, enforces per-file and
  aggregate counts/bytes, fsyncs, and verifies Loom's content and file-mode
  metadata SHA-256 values before returning.
- `Executor.Start` launches exactly RootlessKit with `slirp4netns`,
  host-loopback disabled, IPv6/sandbox/seccomp enabled, then exact rootless OCI
  BuildKit with process sandboxing and `fuse-overlayfs`. It rejects insecure
  entitlements, host networking, CDI/devices, SSH forwarding, arbitrary binds,
  remote frontends, cache imports/exports, and any architecture other than the
  native `linux/amd64` or `linux/arm64` plan.
- `Executor.Build(component) -> OCIOutput` calls pinned `buildctl` with builtin
  `dockerfile.v0`, the server-derived context/Dockerfile, no cache import/export,
  and `type=oci` output below the job directory. `ValidateOCIOutput` streams the
  tar/layout, rejects links/devices/path escape/duplicates, validates every
  descriptor digest and size, requires one manifest/config with exact OS and
  architecture, and returns the immutable top-level digest plus file SHA-256.

- [ ] **Step 1: Write failing runtime supply-chain tests**

  Assert exact base/toolchain/source checksums, signed commits, x/crypto
  override, reproducibility flags, allowed build tags, version/module checks,
  both architecture hashes, and absence of mutable tags/downloads or unrelated
  dependency upgrades.

- [ ] **Step 2: Run runtime tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_rootless_runtime_v2.py`

- [ ] **Step 3: Add the patched reproducible runtime definition**

  Base the source-build stages on the already-reviewed personal-development
  remediation, but emit only the seven host runtime members: `buildctl`,
  `buildkitd`, `buildkit-runc`, `rootlesskit`, `rootlessctl`, `slirp4netns`, and
  `fuse-overlayfs`. Record exact per-architecture digests in the v2 manifest.

- [ ] **Step 4: Write failing launch/download/checksum tests**

  Inject clone3 unsupported/fallback, changed executable, wrong cgroup inode,
  inherited extra fd/env, redirect/proxy/TLS downgrade, traversal/symlink,
  changed file count/size/mode/content, partial download, and quota exhaustion.
  Require cleanup of every partial file/process.

- [ ] **Step 5: Run foundation executor tests and confirm RED**

  Run: `docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Launch|Download|Checksum)'`

- [ ] **Step 6: Implement cgroup launch and safe bundle materialization**

  Add Linux-only raw syscall wrappers where the standard library cannot expose
  `openat2` or seal constants. The non-Linux file must return a fixed
  unsupported-platform error and never emulate containment.

- [ ] **Step 7: Write failing BuildKit/OCI tests**

  Use fake fixed executables for argv/env/cgroup tests and a real privileged
  rootless BuildKit fixture for one native no-cache build. Mutate every required
  RootlessKit/BuildKit flag and OCI descriptor field independently; prove
  daemon, helpers, `RUN` descendants, and cleanup remain under the exact test
  cgroup and that the host Docker/containerd sockets are never opened.

- [ ] **Step 8: Run executor tests and confirm RED**

  Run: `docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Executor|OCI|NativeBuild)'`

- [ ] **Step 9: Implement fixed RootlessKit/BuildKit execution and OCI validation**

  Start one daemon per claim, build components serially, keep cache directories
  inside the quota root, capture only bounded log tails, and terminate with
  SIGTERM then a bounded SIGKILL escalation. Treat any surviving process,
  mount, socket, or storage use as cleanup failure.

- [ ] **Step 10: Run all Task 5 tests and confirm GREEN**

  Run the Python runtime test and the complete Go package test in the pinned
  privileged toolchain/runtime fixture.

- [ ] **Step 11: Commit Task 5**

  Commit message: `feat(builder): execute native rootless OCI builds`

---

### Task 6: Supervisor claim, heartbeat, signal, and cleanup orchestration

**Files:**

- Create: `cmd/loom-task-image-builder-supervisor/orchestrator.go`
- Create: `cmd/loom-task-image-builder-supervisor/outcome.go`
- Create: `cmd/loom-task-image-builder-supervisor/orchestrator_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/integration_test.go`
- Modify: `cmd/loom-task-image-builder-supervisor/main.go`
- Test: `tests/integration/test_task_image_builder_phase2c_flow.py`

**Interfaces:**

- `Orchestrator.Run(ctx) error` performs project, exchange, immediate renewal if
  needed, claim, bundle acquisition, start, heartbeat/renewal, serial component
  builds, typed publication handoff, release/failure, executor cleanup, and
  guard `finish` in one structured lifetime. It processes one claim at a time
  and exits after a bounded idle grace.
- Renewal occurs before one-third of the remaining attestation/session lifetime
  or 15 seconds before expiry, whichever is earlier. Lease heartbeat occurs at
  one-third of its duration. A failed renewal cancels the build immediately;
  later local output cannot be reported as successful.
- `BuildOutcome` is exactly one of `built`, `deterministic_failure`,
  `transient_failure`, `containment_failure`, `lease_lost`, or `cancelled`, with
  a bounded fixed reason code, component, OCI digest/file hash/size evidence,
  and resource/cleanup counters. It carries no raw BuildKit log, URL, token,
  Dockerfile text, or environment.
- `PublicationHandoff.Accept(ctx, BuiltComponentSet) error` is a concrete typed
  boundary. Phase 2C's production main installs `DisabledPublicationHandoff`,
  which returns `publication_phase_unavailable`; the orchestrator then invokes
  infrastructure release, preserving deterministic budget. Tests install an
  in-memory accepting handoff to prove the complete build lifecycle. Phase 2D
  replaces this implementation without changing the executor or lease model.
- SIGINT/SIGTERM cancel claiming, terminate the active executor, preserve the
  current session long enough for one bounded release/failure attempt, issue
  `finish`, zero secrets, and exit nonzero on any cleanup ambiguity.

- [ ] **Step 1: Write failing state-machine tests**

  Use deterministic fake clock, guard client, executor, and handoff. Cover idle
  exit, happy built handoff, every failure phase, renewal/heartbeat races,
  superseded session, lease loss after output, signal at every boundary,
  partial component output, repeated cleanup, and cleanup ambiguity. Assert the
  exact call order and that no later phase runs after its authority is lost.

- [ ] **Step 2: Run orchestrator tests and confirm RED**

  Run: `docker run --rm -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'TestOrchestrator'`

- [ ] **Step 3: Implement the cancellation-safe orchestrator**

  Use one owner goroutine for session/lease state, `context` cancellation,
  explicit timers, and idempotent LIFO cleanup. Never let a heartbeat goroutine
  mutate state after the owner has begun terminal cleanup.

- [ ] **Step 4: Write failing cross-language flow tests**

  Start the real Python authority app and guard local socket with a test
  PostgreSQL database, then run the compiled Go supervisor against a fake
  cgroup/runtime executor. Prove sealed descriptor transfer, atomic renewal,
  session-derived claim identity, exact bundle capability, heartbeat, typed
  release, and redaction. Mutate one field at every Python/Go boundary.

- [ ] **Step 5: Run cross-language tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/integration/test_task_image_builder_phase2c_flow.py`

- [ ] **Step 6: Complete main wiring and cross-language adapters**

  Main validates the content-addressed release before connecting, installs
  signal handlers before projection, and always calls `Finish` after a
  projected allocation. Keep disabled publication as the only compiled
  production handoff in this increment.

- [ ] **Step 7: Run all Task 6 tests and confirm GREEN**

  Run the complete Go package and the Phase 2C integration flow test.

- [ ] **Step 8: Commit Task 6**

  Commit message: `feat(builder): orchestrate contained build leases`

---

### Task 7: Composite provider release, inert installer, and conformance

**Files:**

- Create: `deploy/task-image-builder/provider-release-v1.json`
- Modify: `deploy/task-image-builder/guard-release-v1.json`
- Modify: `deploy/task-image-builder/host-release-v2.json`
- Modify: `deploy/task-image-builder/prerequisites-v1.toml`
- Modify: `deploy/task-image-builder/rootless-provider-v1.toml`
- Modify: `deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh`
- Create: `scripts/ops/task_image_builder_provider_release.py`
- Create: `scripts/ops/install_task_image_builder_provider_release.py`
- Create: `scripts/ops/task_image_builder_provider_conformance.py`
- Create: `docs/runbooks/task-image-builder-phase2c-supervisor.md`
- Test: `tests/ops/test_task_image_builder_provider_release.py`
- Test: `tests/ops/test_task_image_builder_provider_install.py`
- Test: `tests/ops/test_task_image_builder_provider_conformance.py`
- Modify: `tests/ops/test_task_image_builder_prerequisite_profile.py`
- Modify: `tests/ops/test_task_image_rootless_provider_policy.py`
- Modify: `tests/ops/test_task_image_builder_node_prerequisites_install.py`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**

- `provider-release-v1.json` binds source and artifact SHA-256 values for the
  guard zipapp, BPF ELF/map schema, static supervisor, runtime-v2 binaries,
  fixed configs, systemd template, authority contract version, and installer/
  conformance scripts. `task_image_builder_provider_release.py` builds each
  architecture twice in clean pinned containers and refuses non-byte-identical
  results.
- The release directory is named by the SHA-256 of its canonical manifest and
  contains no self-referential `current` link. Provider policy stores the fixed
  install root and supervisor relative path; the submitted executable is
  derived as `/opt/loom-task-image-builder-provider/releases/<grant builder-release-sha256>/bin/loom-task-builder-supervisor`.
- The staging installer accepts only a complete digest-named directory and
  installs it root-owned/read-only plus a mode-0600 staging receipt. It never
  writes live config/credentials, renders a unit, runs `systemctl`, advertises a
  feature, changes provider policy, or creates an activation/current link.
- Conformance verifies native static ELF identity, compiled release path,
  patched Go module metadata, subuid/subgid ranges, project-quota readback in an
  isolated empty directory, clone3-into-cgroup, RootlessKit/BuildKit flags,
  no-cache OCI fixture build, full process ancestry, network-denial probes,
  cleanup, and a fail-closed guard restart. Its Phase 2C result always has
  `production_ready=false` and blocker
  `phase2_guard_provider_release_missing`.

- [ ] **Step 1: Write failing release-policy tests**

  Assert exact member inventory/digests/modes/architectures, runtime v2 and
  x/crypto binding, derived content-addressed supervisor path, changed guard
  digest, deterministic rebuild, and rejection of symlink, writable, extra,
  missing, reordered, or self-referential input.

- [ ] **Step 2: Run release tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_provider_release.py tests/ops/test_task_image_builder_prerequisite_profile.py tests/ops/test_task_image_rootless_provider_policy.py`

- [ ] **Step 3: Implement deterministic composite release assembly**

  Reuse the guard release's safe snapshot/canonical-manifest patterns. Build the
  Go supervisor with `CGO_ENABLED=0`, `-trimpath`, empty build ID, and fixed
  ldflags. Do not commit built ELF binaries to Git; CI artifacts are derived
  only from the reviewed source/spec.

- [ ] **Step 4: Write failing installer tests**

  Cover wrong digest/architecture/owner/mode, collision, partial copy, changed
  source, cross-device rename, interruption at each fsync/rename, idempotent
  exact replay, and forbidden activation writes/calls.

- [ ] **Step 5: Run installer tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_provider_install.py tests/ops/test_task_image_builder_node_prerequisites_install.py`

- [ ] **Step 6: Implement the stage-only installer and update runtime prerequisites**

  Install into a hidden sibling, fsync every member/directory, atomically rename
  only after full verification, and preserve same-name drift as a hidden
  conflict directory. Keep live state entirely absent.

- [ ] **Step 7: Write failing conformance tests**

  Inject every missing kernel/controller/quota/subid/runtime/flag/process/
  network/cleanup condition, stale Slurm feature, disabled-policy drift, and an
  attempted production-ready result. Assert conformance never attaches to a
  Slurm/foreign cgroup and removes its isolated scratch state.

- [ ] **Step 8: Run conformance tests and confirm RED**

  Run: `.venv/bin/pytest -q tests/ops/test_task_image_builder_provider_conformance.py`

- [ ] **Step 9: Implement offline/live conformance and CI Go checks**

  Add the supervisor package to existing `gofmt`, `go vet`, and `go test`
  protected jobs. Live mode takes an explicit staged digest directory and an
  explicit empty scratch cgroup/storage root; it never selects a production
  target automatically.

- [ ] **Step 10: Run all Task 7 tests and confirm GREEN**

  Run all release, installer, conformance, prerequisite, and provider policy
  tests plus deterministic double-build comparison.

- [ ] **Step 11: Commit Task 7**

  Commit message: `build(builder): assemble inert Phase 2C provider release`

---

### Task 8: Full verification, adversarial review, PR, merge, and inertness proof

**Files:**

- Modify only files from Tasks 1-7 when fixing a verified defect.

- [ ] **Step 1: Run focused Python and Go suites**

  Run every new Phase 2C test plus the complete Phase 2A/2B projection,
  authority, guard, materialization, provider-policy, prerequisite, host-release,
  deployment, and Phase 1 regression suites. Run the complete supervisor Go
  package in the pinned Go 1.23.4 container.

- [ ] **Step 2: Run migration, static, and deterministic-release gates**

  Require one head in `migrations`, `capacity_migrations`, and
  `capacity_guard_migrations`; run Ruff on every changed Python file, strict
  mypy on changed Python packages, `gofmt -d`, `go vet`, `go test`, two clean
  provider-release builds with byte comparison, `git diff --check`, and a
  forbidden-path/secret-prefix scan.

- [ ] **Step 3: Perform an adversarial self-review against the controlling spec**

  Review the full base-to-head diff for session gaps/equivocation/replay,
  lock-order deadlocks, token/URL leakage, root parsing of task data, descriptor
  leaks, TOCTOU/path escape, project-quota reuse, clone3 fallback, processes
  outside Slurm, insecure BuildKit features, unbounded logs/responses, OCI graph
  confusion, signal races, unsafe cleanup, broad Slurm/node mutation,
  nondeterminism, vulnerable runtime members, activation drift, and Phase 1
  drift. Add a failing regression test before each correction and repeat until
  no unresolved finding remains.

- [ ] **Step 4: Commit verified review corrections**

  Use narrow commit messages naming the proven defect. Do not rewrite or
  force-push published history.

- [ ] **Step 5: Push and open a non-draft PR to `dev`**

  Include the approved spec, exact base/head, threat boundaries, migration and
  runtime supply-chain notes, TDD counts, deterministic release evidence,
  inertness proof, and explicit Phase 2D handoff. Confirm the diff contains no
  `docs/superpowers/**` or `.superpowers/**` path.

- [ ] **Step 6: Resolve review and current-head protected CI**

  Reproduce valid feedback with a failing test, fix it, rerun focused/full
  verification, and push normally. If `dev` advances, use the forge's update
  branch operation and reverify. Require `repository-checks`, `images-gate`,
  `cluster-smoke-gate`, and `staging-smoke-gate` on the exact final head; never
  bypass, admin-merge, force-push, or push directly to `dev`.

- [ ] **Step 7: Complete ordinary squash merge and prove the merged tree**

  Enable ordinary squash auto-merge only after current-head checks exist. After
  merge, fetch `origin/dev`, prove the PR-head tree equals the squash tree, and
  record the exact merge SHA/tree.

- [ ] **Step 8: Verify post-merge production inertness**

  Recheck the three migration heads; authority replicas zero/default-deny; both
  providers disabled; certification false; certified nodes empty; blocker
  present; no live config/credentials/activation marker/current link/unit/
  socket/BPF pins/feature; and Phase 1 timer/reservation/runtime unchanged. A
  staged content-addressed Phase 2C release is allowed, but no staged artifact
  is activation authority.

- [ ] **Step 9: Record the exact Phase 2D handoff**

  Report the remaining protected increment: renewable repository-scoped
  registry credentials, digest-by-digest OCI graph verification, signed
  publication statements/keyset rotation and revocation, reference-aware
  partial retention, immutable execution grants, and one-use trial-start
  authorization. Do not call the builder production-active.
