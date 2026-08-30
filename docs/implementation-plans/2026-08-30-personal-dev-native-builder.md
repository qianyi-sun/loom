# Personal Dev Native Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not dispatch subagents for this plan.

**Goal:** Build and prove a native arm64 GB10 provider that completes multi-platform personal candidates concurrently for multiple owners while retaining native OLDLAB amd64 builds and zero task/Slurm authority.

**Architecture:** The existing candidate coordinator keeps its whole-attempt lease and dispatches two platform executors. OLDLAB remains a Kubernetes/KVM-gVisor executor; GB10 uses a durable, signed pull grant consumed by a release-pinned agent that creates separate client and rootless-BuildKit KVM-gVisor containers on a dedicated Docker daemon. Management independently verifies, scans, publishes, and commits both platform artifacts.

**Tech Stack:** Python 3.11, asyncio, FastAPI, Pydantic, SQLAlchemy/PostgreSQL, Alembic, boto3/MinIO, Ed25519, Docker Engine API, systemd, gVisor/runsc, nftables, GitHub Actions, pytest, ruff, mypy.

**Spec:** `docs/architecture/personal-dev-native-builder-provider.md`

## Global Constraints

- Candidate-controlled bytes execute only under KVM gVisor; there is no runc or QEMU operational fallback.
- The native agent has no Slurm, task-image, task, worker, capacity-manager, Kubernetes, database, registry, or MinIO credential.
- The agent may access only the dedicated builder Docker socket; neither sandbox receives any Docker socket.
- A grant is never reassigned within one whole-attempt lease epoch; same-agent restart is the only in-epoch resume.
- The existing artifact key remains authoritative and safe because whole-attempt lease epoch changes create a different key.
- Only management verifies artifacts, scans images, publishes registry manifests, and commits candidate readiness.
- Defaults remain inert. Operational render must require a fresh exact native agent and must never silently select QEMU.
- Approved GB10 agent concurrency is two. Existing global/per-owner candidate limits remain the outer admission authority.
- Dedicated provider aggregate limits are 900% CPU and 72 GiB; each grant has a 1-CPU/16-GiB client and a 3-CPU/16-GiB BuildKit container.
- No task submission, Slurm mutation, direct personal namespace deletion, or nonzero executable-capacity mutation is part of implementation or acceptance.
- Preserve unrelated user changes and the detached operational acceptance worktree.

## File and responsibility map

- `src/loom/personal_dev_native_builder_protocol.py`: canonical signed request/evidence types and Ed25519 verification.
- `src/loom/personal_dev_native_builder_store.py`: durable agent/grant transitions with parent-attempt fencing.
- `src/loom/personal_dev_native_builder_executor.py`: remote platform executor used by the management coordinator.
- `src/loom_service/routes/personal_dev_native_builder.py`: signature-gated internal agent API.
- `src/loom/personal_dev_builder_runtime.py`: one-platform Kubernetes executor and two-platform composite.
- `src/loom/personal_dev_native_builder_agent.py`: reconciliation logic and exact Docker resource contract.
- `src/loom_personal_dev_native_builder_agent/__main__.py`: environment validation and long-running agent loop.
- `scripts/ops/personal_dev_native_builder_runtime_profile.py`: strict host-release/profile model.
- `scripts/ops/install_personal_dev_native_builder_runtime.py`: fail-closed stage/install/verify/remove operations.
- `scripts/ops/converge_personal_dev_native_builder_release.py`: exact current/previous image retention on the primary and dedicated daemons.
- `deploy/personal-dev-native-builder/*`: exact host profile, dedicated dockerd/runsc/nftables/systemd inputs.
- `deploy/Dockerfile.personal-dev-native-builder-agent`: minimal multi-architecture trusted agent image.
- `src/loom_personal_dev_native_builder_probe/__main__.py`: read-only, secret-free durable agent status for the external observer.
- `src/loom/personal_dev_control_plane_{config,render,status}.py`: trusted-release binding and readiness.
- `.github/workflows/images.yml` and `scripts/ci_personal_dev_trusted_release.py`: immutable agent release production and trust gate.

---

### Task 1: Canonical signed native-agent protocol

**Files:**
- Create: `src/loom/personal_dev_native_builder_protocol.py`
- Create: `tests/unit/test_personal_dev_native_builder_protocol.py`

**Interfaces:**
- Produces: `NativeBuilderAgentStatus`, `NativeBuilderPollRequest`, `NativeBuilderGrantPayload`, `NativeBuilderHeartbeatRequest`, `NativeBuilderCompletion`, `NativeBuilderRuntimeEvidence`, `PersonalDevNativeBuilderSigner`, `PersonalDevNativeBuilderVerifier`.
- Every signed type exposes `canonical_bytes() -> bytes`; signer methods return 128-character lowercase Ed25519 signatures.
- Verifier methods accept `signature: str` and `now: datetime`, enforce a 60-second past window and 15-second future skew, and return the payload SHA-256.

- [ ] **Step 1: Write failing tests for strict canonical models**

  Cover exact field sets, `linux/arm64`, provider `gb10-gvisor-docker-v1`, `aarch64`, immutable image references, 64-character digests, concurrency exactly two, sorted unique grant inventories, timezone-aware timestamps, UUID nonce, and canonical ASCII JSON.

- [ ] **Step 2: Run the protocol tests and confirm import failure**

  Run: `uv run pytest -q tests/unit/test_personal_dev_native_builder_protocol.py`

  Expected: FAIL because `loom.personal_dev_native_builder_protocol` does not exist.

- [ ] **Step 3: Implement frozen dataclasses and canonical serialization**

  Use the activation protocol's Ed25519 loading and file-metadata rules, but keep native-builder messages in their own module. Include `schema_version=1` in canonical bytes and reject unknown/duplicate collection entries before signing.

- [ ] **Step 4: Add signature, freshness, wrong-key, and tampering tests**

  Include changed agent instance, runtime profile, builder image, grant ID, attempt lease, completion outcome, evidence, reused signature on a different message, stale time, and future time.

- [ ] **Step 5: Implement signer, verifier, and exact key loaders**

  Add type-specific poll, heartbeat, and completion signing and verification methods. Load exactly 32 raw Ed25519 bytes with `O_NOFOLLOW`, stable metadata checks, owner-only `0400` for the private key, read-only `0400` or `0440` for the public key, and optional public-key SHA-256 binding. Return the verified payload SHA-256 only after freshness and signature validation.

- [ ] **Step 6: Re-run protocol tests and confirm they pass**

  Run: `uv run pytest -q tests/unit/test_personal_dev_native_builder_protocol.py`

- [ ] **Step 7: Run focused quality gates**

  Run:

  ```bash
  uv run pytest -q tests/unit/test_personal_dev_native_builder_protocol.py
  uv run ruff check src/loom/personal_dev_native_builder_protocol.py tests/unit/test_personal_dev_native_builder_protocol.py
  uv run mypy src/loom/personal_dev_native_builder_protocol.py
  ```

- [ ] **Step 8: Commit the protocol**

  ```bash
  git add src/loom/personal_dev_native_builder_protocol.py tests/unit/test_personal_dev_native_builder_protocol.py
  git commit -m "feat(personal-dev): define signed native builder protocol"
  ```

### Task 2: Durable native-agent and grant state

**Files:**
- Create: `migrations/versions/0123_personal_dev_native_builder.py`
- Modify: `src/loom/db/schema.py`
- Create: `src/loom/personal_dev_native_builder_store.py`
- Create: `tests/integration/test_personal_dev_native_builder_migration.py`
- Create: `tests/integration/test_personal_dev_native_builder_store.py`
- Modify: `tests/integration/test_alembic_migrations.py`

**Interfaces:**
- Consumes: Task 1 protocol models.
- Produces: `issue_native_build_grant(session: AsyncSession, registration: CandidateRegistration, policy: NativeBuilderGrantPolicy, now: datetime) -> PersonalDevNativeBuildGrant`, `poll_native_build_grant(session: AsyncSession, request: NativeBuilderPollRequest, now: datetime) -> NativeBuilderPollResult`, `heartbeat_native_build_grant(session: AsyncSession, request: NativeBuilderHeartbeatRequest, now: datetime) -> bool`, `complete_native_build_grant(session: AsyncSession, completion: NativeBuilderCompletion, now: datetime, artifact_head: NativeBuilderArtifactHead | None) -> PersonalDevNativeBuildGrant`, `cancel_native_build_grant(session: AsyncSession, attempt_id: UUID, attempt_lease_epoch: int, platform: PersonalDevPlatform, now: datetime) -> bool`, `get_native_build_grant(session: AsyncSession, attempt_id: UUID, attempt_lease_epoch: int, platform: PersonalDevPlatform) -> PersonalDevNativeBuildGrant | None`, and `NativeBuilderGrantFencedError`.
- Produces ORM rows `PersonalDevNativeBuilderAgent` and `PersonalDevNativeBuildGrant` with the exact states and unique key from the spec.

- [ ] **Step 1: Write failing migration inspection tests**

  Assert both tables, foreign keys, check constraints, unique `(attempt_id, attempt_lease_epoch, platform)`, active/state indexes, non-null canonical evidence rules, and a downgrade that removes only `0123` objects.

- [ ] **Step 2: Run migration tests and confirm head/schema failure**

  Run: `uv run pytest -q tests/integration/test_personal_dev_native_builder_migration.py tests/integration/test_alembic_migrations.py`

- [ ] **Step 3: Implement migration and matching ORM schema**

  Use `ondelete=CASCADE` from grant to build attempt and `ondelete=RESTRICT` from grant to candidate. Make agent identity the primary key. Store the last accepted request timestamp and nonce for monotonic replay fencing.

- [ ] **Step 4: Write failing store transition tests**

  Test one-writer issuance, exact idempotent issue, conflicting policy, parent state/lease expiry, FIFO claim, two-slot admission, no third claim, same-instance resume, foreign-instance denial, monotonic poll/heartbeat timestamp, cancellation, signed success/failure, exact idempotent completion, and stale whole-attempt completion.

- [ ] **Step 5: Implement store transitions under row locks**

  Each mutation must lock both grant and parent attempt. `poll_native_build_grant` must update signed agent readiness even when no grant exists, reconcile the reported inventory, return explicit cancellation IDs, and claim at most one queued row.

- [ ] **Step 6: Run database and focused quality gates**

  Run:

  ```bash
  uv run pytest -q tests/integration/test_personal_dev_native_builder_migration.py tests/integration/test_personal_dev_native_builder_store.py tests/integration/test_alembic_migrations.py
  uv run ruff check src/loom/db/schema.py src/loom/personal_dev_native_builder_store.py migrations/versions/0123_personal_dev_native_builder.py tests/integration/test_personal_dev_native_builder_migration.py tests/integration/test_personal_dev_native_builder_store.py
  uv run mypy src/loom/personal_dev_native_builder_store.py
  ```

- [ ] **Step 7: Commit durable state**

  ```bash
  git add migrations/versions/0123_personal_dev_native_builder.py src/loom/db/schema.py src/loom/personal_dev_native_builder_store.py tests/integration/test_personal_dev_native_builder_migration.py tests/integration/test_personal_dev_native_builder_store.py tests/integration/test_alembic_migrations.py
  git commit -m "feat(personal-dev): persist native builder grants"
  ```

### Task 3: Public capability separation and internal agent API

**Files:**
- Modify: `src/loom_service/storage.py`
- Create: `src/loom_service/routes/personal_dev_native_builder.py`
- Modify: `src/loom_service/routes/__init__.py`
- Modify: `src/loom_service/app.py`
- Modify: `src/loom/personal_dev_builder_manifest.py`
- Modify: `src/loom/personal_dev_builder_runtime.py`
- Create: `tests/unit/test_personal_dev_native_builder_routes.py`
- Modify: `tests/unit/test_personal_dev_builder_manifest.py`
- Modify: `tests/unit/test_personal_dev_builder_runtime.py`
- Modify: `tests/unit/test_minio_public_endpoint.py`

**Interfaces:**
- Consumes: Tasks 1-2 store and protocol.
- Produces: public `personal_dev_builder_contract(registration: CandidateRegistration, *, platform: PersonalDevPlatform, config: PersonalDevBuilderManifestConfig) -> str` and a public-presign `S3PersonalDevBuildCapabilityProvider` dedicated to native grants.
- Produces internal FastAPI models/routes for poll, heartbeat, and completion; secrets appear only in the successful poll response and never in logs/errors.

- [ ] **Step 1: Write failing contract extraction tests**

  Require byte-identical contract JSON from the existing Kubernetes manifest and the new public function. Retain every candidate, lifecycle, platform, size, and whole-attempt lease binding.

- [ ] **Step 2: Extract and export the contract renderer**

  Replace private `_contract` calls with `personal_dev_builder_contract` without changing existing manifest bytes.

- [ ] **Step 3: Write failing route authentication and replay tests**

  Use an in-memory store factory and fake object store. Test valid signed poll, 204/no grant semantics, one grant with public URLs, invalid/stale/replayed signature, secret-free errors, heartbeat continue/cancel, success HEAD verification, failure completion, wrong object metadata, and parent-lease fencing.

- [ ] **Step 4: Add a strict public S3 presign client**

  Require `https`, no userinfo/query/fragment, and an origin path of empty or `/`. Keep `minio_client` internal. Store a separate `personal_dev_native_builder_presign_client` in app state only when the native provider is enabled.

- [ ] **Step 5: Implement and register the internal routes**

  Verify signatures before creating a DB session transition. On success, issue capability expiry no shorter than `active_deadline_seconds + 60`. HEAD the exact deterministic artifact key before a successful completion transition.

- [ ] **Step 6: Run focused route/runtime tests**

  Run:

  ```bash
  uv run pytest -q tests/unit/test_personal_dev_native_builder_routes.py tests/unit/test_personal_dev_builder_manifest.py tests/unit/test_personal_dev_builder_runtime.py tests/unit/test_minio_public_endpoint.py
  uv run ruff check src/loom_service/storage.py src/loom_service/routes/personal_dev_native_builder.py src/loom_service/app.py src/loom/personal_dev_builder_manifest.py src/loom/personal_dev_builder_runtime.py tests/unit/test_personal_dev_native_builder_routes.py
  uv run mypy src/loom_service/routes/personal_dev_native_builder.py
  ```

- [ ] **Step 7: Commit the agent API**

  ```bash
  git add src/loom_service/storage.py src/loom_service/routes/personal_dev_native_builder.py src/loom_service/routes/__init__.py src/loom_service/app.py src/loom/personal_dev_builder_manifest.py src/loom/personal_dev_builder_runtime.py tests/unit/test_personal_dev_native_builder_routes.py tests/unit/test_personal_dev_builder_manifest.py tests/unit/test_personal_dev_builder_runtime.py tests/unit/test_minio_public_endpoint.py
  git commit -m "feat(personal-dev): expose fenced native builder grants"
  ```

### Task 4: Architecture-specific composite execution

**Files:**
- Create: `src/loom/personal_dev_native_builder_executor.py`
- Modify: `src/loom/personal_dev_builder_runtime.py`
- Modify: `src/loom_service/personal_dev_builder.py`
- Modify: `src/loom_service/app.py`
- Create: `tests/unit/test_personal_dev_native_builder_executor.py`
- Modify: `tests/unit/test_personal_dev_builder_runtime.py`
- Modify: `tests/unit/test_service_personal_dev_builder.py`
- Modify: `tests/unit/test_personal_dev_builder.py`

**Interfaces:**
- Consumes: Task 2 grant store.
- Produces: `PersonalDevPlatformBuildExecutor`, `KubectlPersonalDevPlatformBuildExecutor`, `NativeAgentPersonalDevPlatformBuildExecutor`, and `CompositePersonalDevBuildExecutor` implementing the existing `PersonalDevBuildExecutor` protocol.
- `build_platform(registration: CandidateRegistration, *, source_archive: Path) -> None` means the executor's exact platform artifact is durably present; `cleanup_platform(registration: CandidateRegistration) -> None` is idempotent and exact. `CompositePersonalDevBuildExecutor.build(registration: CandidateRegistration, *, source_archive: Path) -> Mapping[str, object]` retains the existing coordinator interface and publishes only after both platform calls succeed.

- [ ] **Step 1: Write failing single-platform Kubernetes tests**

  Assert the amd64 executor creates exactly one amd64 Job, never creates arm64, waits for exact runtime evidence, and deletes only its attempt namespace during cleanup.

- [ ] **Step 2: Refactor Kubernetes execution without changing legacy behavior**

  Move one-platform work into `KubectlPersonalDevPlatformBuildExecutor`. Keep `KubectlPersonalDevBuildExecutor` as a legacy wrapper that dispatches both platforms and exports, so disabled native-provider tests remain compatible.

- [ ] **Step 3: Write failing native grant wait/cancel tests**

  Test idempotent issue, success observation, bounded failure, timeout, whole-attempt cancellation, coroutine cancellation, and session-per-poll behavior.

- [ ] **Step 4: Implement the remote platform executor**

  Poll with short independent DB sessions. Never hold a DB transaction while awaiting the agent. On cleanup, cancel only the exact attempt/whole-lease/platform grant.

- [ ] **Step 5: Write failing composite concurrency tests**

  Prove both platforms start before either is released, exporter runs only after both, one failure cancels the sibling, exporter failure still cleans both, and returned publication remains byte-identical to the existing exporter output.

- [ ] **Step 6: Compose native mode in service startup**

  Pass `session_factory` into builder runtime construction. When native mode is enabled, bind amd64 to Kubernetes and arm64 to the native grant executor. When disabled, retain the legacy executor. Expose no QEMU fallback in operational mode.

- [ ] **Step 7: Run focused coordinator tests**

  Run:

  ```bash
  uv run pytest -q tests/unit/test_personal_dev_native_builder_executor.py tests/unit/test_personal_dev_builder_runtime.py tests/unit/test_service_personal_dev_builder.py tests/unit/test_personal_dev_builder.py
  uv run ruff check src/loom/personal_dev_native_builder_executor.py src/loom/personal_dev_builder_runtime.py src/loom_service/personal_dev_builder.py src/loom_service/app.py tests/unit/test_personal_dev_native_builder_executor.py
  uv run mypy src/loom/personal_dev_native_builder_executor.py src/loom/personal_dev_builder_runtime.py src/loom_service/personal_dev_builder.py
  ```

- [ ] **Step 8: Commit platform dispatch**

  ```bash
  git add src/loom/personal_dev_native_builder_executor.py src/loom/personal_dev_builder_runtime.py src/loom_service/personal_dev_builder.py src/loom_service/app.py tests/unit/test_personal_dev_native_builder_executor.py tests/unit/test_personal_dev_builder_runtime.py tests/unit/test_service_personal_dev_builder.py tests/unit/test_personal_dev_builder.py
  git commit -m "feat(personal-dev): dispatch native platform builders"
  ```

### Task 5: Native agent reconciliation and exact Docker contract

**Files:**
- Create: `src/loom/personal_dev_native_builder_agent.py`
- Create: `src/loom_personal_dev_native_builder_agent/__init__.py`
- Create: `src/loom_personal_dev_native_builder_agent/__main__.py`
- Create: `tests/unit/test_personal_dev_native_builder_agent.py`
- Create: `tests/unit/test_personal_dev_native_builder_agent_main.py`

**Interfaces:**
- Consumes: Task 1 signed protocol and Task 3 HTTP routes.
- Produces: `HttpPersonalDevNativeBuilderAuthority`, `DockerPersonalDevNativeBuildRuntime`, `PersonalDevNativeBuilderAgent`, and executable module `python -m loom_personal_dev_native_builder_agent`.
- Runtime methods: `inventory()`, `start(grant)`, `observe(grant)`, `cancel(grant_id)`, and `cleanup(grant_id)`; every method is limited to exact Loom labels on the dedicated daemon.

- [ ] **Step 1: Write failing HTTP adapter tests**

  Test canonical request bodies/signatures, strict response field sets, no response-body logging, 204/no grant, heartbeat continue, completion acknowledgement, TLS origin validation, and `trust_env=False`.

- [ ] **Step 2: Implement the HTTP adapter and agent loop shell**

  Use a stable agent instance ID and signer key. Bound poll and heartbeat intervals. If heartbeat cannot be refreshed within grace, signal cancellation locally without waiting for central recovery.

- [ ] **Step 3: Write failing Docker create-contract tests**

  Against a fake Docker API, assert two distinct containers, runtime `runsc-personal-dev-native`, immutable builder image, exact labels, one per-grant network, no published port, no host mount/socket/device, BuildKit-only SETUID/SETGID and unconfined seccomp, client cap-drop/default seccomp/no-new-privileges, read-only roots, tmpfs sizes, CPU/memory/PID limits, and capability files copied only into the stopped client with UID/GID 1000 mode 0400.

- [ ] **Step 4: Implement exact Docker resource creation**

  Use the Docker SDK only against `unix:///run/loom-personal-dev-builder/docker.sock`. Reject a server that is not `aarch64`, lacks the runtime, or reports a builder image/platform mismatch. Create BuildKit first, wait on the fixed health command, then start the client.

- [ ] **Step 5: Write failing resume, cancellation, and drift tests**

  Cover zero objects, same-grant stopped/running/exited objects, duplicate roles, foreign labels, shape drift, unknown managed objects, client success/failure/OOM, BuildKit early exit, signed evidence, completion retry before cleanup, central cancellation, and exact cleanup ordering.

- [ ] **Step 6: Implement reconciliation and evidence**

  Inspect rather than trust desired state. Send success only for client exit zero, no restart/OOM, exact image/runtime/network/security identity, and live expected BuildKit. Preserve objects until completion is acknowledged. Never prune an unlabeled or drifted object.

- [ ] **Step 7: Write failing environment and privilege-boundary tests**

  In `test_personal_dev_native_builder_agent_main.py`, reject an unknown or inherited environment variable, a proxy variable, root UID, wrong effective UID, missing supplemental socket GID, key ownership/mode drift, primary Docker socket, relative CA path, mutable image, zero/oversized interval, and concurrency other than two. Assert the accepted environment constructs an HTTP client with `trust_env=False` and a Docker client for only the dedicated socket.

- [ ] **Step 8: Implement strict environment startup**

  Require HTTPS service origin, readable owner-only Ed25519 key, exact agent/builder image references, runtime/profile digests, concurrency two, positive bounded intervals, dedicated socket path, the exact non-root UID and supplemental socket GID, and no proxy inheritance.

- [ ] **Step 9: Run agent quality gates**

  Run:

  ```bash
  uv run pytest -q tests/unit/test_personal_dev_native_builder_agent.py tests/unit/test_personal_dev_native_builder_agent_main.py
  uv run ruff check src/loom/personal_dev_native_builder_agent.py src/loom_personal_dev_native_builder_agent tests/unit/test_personal_dev_native_builder_agent.py tests/unit/test_personal_dev_native_builder_agent_main.py
  uv run mypy src/loom/personal_dev_native_builder_agent.py src/loom_personal_dev_native_builder_agent
  ```

- [ ] **Step 10: Commit the agent**

  ```bash
  git add src/loom/personal_dev_native_builder_agent.py src/loom_personal_dev_native_builder_agent tests/unit/test_personal_dev_native_builder_agent.py tests/unit/test_personal_dev_native_builder_agent_main.py
  git commit -m "feat(personal-dev): add native GB10 builder agent"
  ```

### Task 6: Fixed TCP BuildKit mode inside separate gVisor sandboxes

**Files:**
- Modify: `deploy/personal-dev-builder/loom-personal-dev-buildkitd`
- Modify: `src/loom/personal_dev_sandbox_builder.py`
- Modify: `deploy/Dockerfile.personal-dev-builder`
- Modify: `tests/unit/test_personal_dev_sandbox_builder.py`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**
- Consumes: Task 5 exact command contract.
- Produces: launcher flag `--native-tcp-buildkit-child` and client option `--native-buildkit-address tcp://<grant-local-name>:1234` with strict hostname validation.
- Existing no-argument Kubernetes UDS mode remains byte-for-byte compatible.

- [ ] **Step 1: Write failing launcher dispatch and preflight tests**

  Require exactly one new fixed mode, reject arbitrary addresses/ports/flags, retain UID/GID/capability/gVisor/NNP checks, and assert BuildKit child gets `no_new_privs=1` before exec.

- [ ] **Step 2: Implement fixed TCP launcher mode**

  Listen on `tcp://0.0.0.0:1234` only in native mode. Do not add TLS because the endpoint is confined to one bridge and holds no credential; never publish the port.

- [ ] **Step 3: Write failing client address validation tests**

  Accept only `tcp://buildkit-<12 lowercase hex>:1234` from the explicit native CLI option. UDS remains the only default. Reject IPs, other DNS names, userinfo, query/fragment, other ports, and environment overrides.

- [ ] **Step 4: Implement native client connection mode**

  Preserve source verification, suppressed candidate build output, OCI verification, byte limits, and artifact upload behavior.

- [ ] **Step 5: Run builder image and unit tests**

  Run:

  ```bash
  uv run pytest -q tests/unit/test_personal_dev_sandbox_builder.py tests/ops/test_personal_dev_control_plane_package_boundary.py -k 'sandbox_builder or rootless_sidecar or builder_image'
  uv run ruff check src/loom/personal_dev_sandbox_builder.py tests/unit/test_personal_dev_sandbox_builder.py
  uv run mypy src/loom/personal_dev_sandbox_builder.py
  docker buildx build --platform linux/amd64 -f deploy/Dockerfile.personal-dev-builder --load -t loom-personal-dev-builder:native-provider-test .
  ```

- [ ] **Step 6: Commit native sandbox transport**

  ```bash
  git add deploy/personal-dev-builder/loom-personal-dev-buildkitd deploy/Dockerfile.personal-dev-builder src/loom/personal_dev_sandbox_builder.py tests/unit/test_personal_dev_sandbox_builder.py tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "feat(personal-dev): support native gVisor builder transport"
  ```

### Task 7: Inert GB10 host runtime profile and installer

**Files:**
- Create: `scripts/ops/personal_dev_native_builder_runtime_profile.py`
- Create: `scripts/ops/install_personal_dev_native_builder_runtime.py`
- Create: `deploy/personal-dev-native-builder/runtime-profile-v1.json`
- Create: `deploy/personal-dev-native-builder/runsc.toml`
- Create: `deploy/personal-dev-native-builder/dockerd.json`
- Create: `deploy/personal-dev-native-builder/loom-personal-dev-builder-dockerd.service`
- Create: `deploy/personal-dev-native-builder/loom-personal-dev-native-builder-agent.service.in`
- Create: `deploy/personal-dev-native-builder/loom-personal-dev-native-builder.sysusers`
- Create: `deploy/personal-dev-native-builder/loom-personal-dev-builder.slice`
- Create: `deploy/personal-dev-native-builder/provider-network.nft`
- Create: `scripts/ops/converge_personal_dev_native_builder_release.py`
- Create: `tests/ops/test_personal_dev_native_builder_runtime_profile.py`
- Create: `tests/ops/test_install_personal_dev_native_builder_runtime.py`
- Create: `tests/ops/test_converge_personal_dev_native_builder_release.py`

**Interfaces:**
- Produces: profile loader `load_native_builder_runtime_profile(path: Path) -> NativeBuilderRuntimeProfile`; installer modes `preflight`, `install`, `stage-agent`, `verify-staged`, `verify-active`, `remove`; and release convergence modes `plan`, `apply`, and `verify`.
- Installer manages only `/opt/loom/gvisor/<release>`, `/etc/loom/personal-dev-native-builder`, `/var/lib/loom-personal-dev-builder`, `/run/loom-personal-dev-builder`, `/etc/systemd/system/loom-personal-dev-*`, `/etc/sysusers.d/loom-personal-dev-native-builder.conf`, and nft table `loom_personal_dev_builder`.
- The profile fixes a non-login agent UID and a distinct dedicated-socket GID. `stage-agent` requires immutable agent/builder images, exact release/profile/key/CA/service-origin bindings, writes an inactive unit, and never embeds secret bytes. Release convergence retains exactly current/previous trusted agent images on the primary daemon and builder images on the dedicated daemon; deletion is repository-, label-, digest-, and zero-container-scoped and never invokes daemon-wide prune.

- [ ] **Step 1: Acquire independent arm64 gVisor release evidence**

  Download `https://storage.googleapis.com/gvisor/releases/release/20260810/aarch64/gvisor.tar.bz2` into a fresh `mktemp -d`, hash the archive and every expected member, and verify `runsc --version` in an arm64 container or on the target host. Record the hand-checked literals for the failing tests, but do not write production files yet and do not check the 151-MB archive into Git.

- [ ] **Step 2: Write failing strict-profile tests**

  Start with a missing-profile failure and literal expected archive/member hashes and sizes from Step 1. Reject extra/missing keys, wrong host `gx10-01c7`, non-aarch64 release, wrong Docker 28.3.3 identity, non-KVM platform, runtime relaxation, route/address overlap, mutable paths, unsafe modes, non-public DNS, changed resource ceilings, UID/GID collision or equality, wrong key/socket modes, and malformed archive members.

- [ ] **Step 3: Implement profile parsing and generated-byte helpers**

  Write `runtime-profile-v1.json` with the literal evidence from Step 1. Parse canonical JSON only. Generate exact dockerd, runsc, nftables, slice, sysusers, daemon service, and parameterized inactive agent-service bytes from the profile and require checked-in files to match.

- [ ] **Step 4: Write failing installer transaction tests**

  Use a fake host root and command runner. Cover wrong hostname/arch, missing KVM/cgroup v2/nft/dockerd/systemd-sysusers, active foreign unit, route collision, insufficient disk/memory/CPU, UID/GID conflict, unsafe archive member, digest drift, symlink/hardlink/owner/mode drift, key/CA drift, dedicated socket group/mode drift, partial install, idempotent exact install and agent staging, fsync publication, inactive-by-default staging, active verification, busy removal, and byte-identical rollback.

- [ ] **Step 5: Implement inert installation state machine**

  `install` must stage bytes and units but leave both dockerd and agent inactive. `verify-active` may inspect only the dedicated daemon/runtime and exact nft table. `remove` must refuse while any managed container/network exists or any byte differs.

- [ ] **Step 6: Write failing exact image-retention tests**

  Against fake primary and dedicated Docker APIs, require current before activation, retain current plus optional previous, reject mutable/wrong-platform/wrong-repository/wrong-label images, refuse to remove an image used by any container, remove only an older unreferenced Loom-managed repository digest, and prove there is no daemon-wide prune or Slurm/task command.

- [ ] **Step 7: Implement release convergence**

  `plan` is read-only and canonical; `apply` imports or pulls exact trusted digests and deletes only the planned older managed digests after a second zero-container check; `verify` requires exact platform, repository, OCI source/revision labels, and current/previous retention on both daemons. Never pass registry credentials to the agent service.

- [ ] **Step 8: Run host-runtime quality gates**

  Run:

  ```bash
  uv run pytest -q tests/ops/test_personal_dev_native_builder_runtime_profile.py tests/ops/test_install_personal_dev_native_builder_runtime.py tests/ops/test_converge_personal_dev_native_builder_release.py
  uv run ruff check scripts/ops/personal_dev_native_builder_runtime_profile.py scripts/ops/install_personal_dev_native_builder_runtime.py scripts/ops/converge_personal_dev_native_builder_release.py tests/ops/test_personal_dev_native_builder_runtime_profile.py tests/ops/test_install_personal_dev_native_builder_runtime.py tests/ops/test_converge_personal_dev_native_builder_release.py
  uv run mypy scripts/ops/personal_dev_native_builder_runtime_profile.py scripts/ops/install_personal_dev_native_builder_runtime.py scripts/ops/converge_personal_dev_native_builder_release.py
  ```

- [ ] **Step 9: Commit the inert host runtime**

  ```bash
  git add scripts/ops/personal_dev_native_builder_runtime_profile.py scripts/ops/install_personal_dev_native_builder_runtime.py scripts/ops/converge_personal_dev_native_builder_release.py deploy/personal-dev-native-builder tests/ops/test_personal_dev_native_builder_runtime_profile.py tests/ops/test_install_personal_dev_native_builder_runtime.py tests/ops/test_converge_personal_dev_native_builder_release.py
  git commit -m "feat(personal-dev): package inert GB10 builder runtime"
  ```

### Task 8: Trusted agent image and release gate

**Files:**
- Create: `deploy/Dockerfile.personal-dev-native-builder-agent`
- Modify: `.github/workflows/images.yml`
- Modify: `scripts/ci_personal_dev_trusted_release.py`
- Modify: `tests/ops/test_ci_personal_dev_trusted_release.py`
- Modify: `tests/ops/test_ci_image_candidate.py`
- Modify: `scripts/validate_trivy_release_report.py`
- Modify: `src/loom/personal_dev_control_plane_config.py`
- Modify: `tests/unit/test_personal_dev_control_plane_config.py`

**Interfaces:**
- Produces release key `personal_dev_native_builder_agent` and image repository `ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent`.
- Trusted release requires both amd64 and arm64 provenance, exact Dockerfile/context digest, vulnerability result, immutable index digest, and source SHA parity.

- [ ] **Step 1: Write failing trusted-release tests for the new image**

  Require the new release key everywhere `personal_dev_builder` is required, reject a missing architecture, mutable tag, wrong repository, source mismatch, or incomplete scan evidence.

- [ ] **Step 2: Add the minimal agent Dockerfile**

  Use the repository's pinned Python 3.11 slim base pattern. Install only CA certificates and locked Python wheels needed by HTTP, Ed25519, and Docker SDK code. Copy only the agent/protocol modules. Run as non-root with a read-only-compatible home/tmp contract.

- [ ] **Step 3: Add the image to CI planning/build/provenance/promotion**

  Mirror the personal-dev activation-agent multi-architecture path, using the new Dockerfile and repository name. Include it in the trusted personal-dev release record and scan allowlist.

- [ ] **Step 4: Extend release config parsing**

  Add `personal_dev_native_builder_agent` to `PersonalDevControlPlaneImages` and repository mapping. Keep old trusted-release JSON invalid when native mode is required, while shadow/predecessor compatibility remains explicit and bounded.

- [ ] **Step 5: Run release/config tests**

  Run:

  ```bash
  uv run pytest -q tests/ops/test_ci_personal_dev_trusted_release.py tests/ops/test_ci_image_candidate.py tests/unit/test_personal_dev_control_plane_config.py
  uv run ruff check scripts/ci_personal_dev_trusted_release.py scripts/validate_trivy_release_report.py src/loom/personal_dev_control_plane_config.py tests/ops/test_ci_personal_dev_trusted_release.py tests/unit/test_personal_dev_control_plane_config.py
  uv run mypy scripts/ci_personal_dev_trusted_release.py src/loom/personal_dev_control_plane_config.py
  ```

- [ ] **Step 6: Commit release production**

  ```bash
  git add deploy/Dockerfile.personal-dev-native-builder-agent .github/workflows/images.yml scripts/ci_personal_dev_trusted_release.py scripts/validate_trivy_release_report.py src/loom/personal_dev_control_plane_config.py tests/ops/test_ci_personal_dev_trusted_release.py tests/ops/test_ci_image_candidate.py tests/unit/test_personal_dev_control_plane_config.py
  git commit -m "ci(personal-dev): release native builder agent"
  ```

### Task 9: Service configuration, render, and readiness interlocks

**Files:**
- Modify: `config/loom-schema.toml`
- Regenerate: `src/loom_service/config/_generated.py`
- Regenerate: `src/loom_cli/data/loom-schema.toml`
- Regenerate: `tests/loom_config/snapshots/loom_service.json`
- Modify: `src/loom/personal_dev_control_plane_config.py`
- Modify: `src/loom/personal_dev_control_plane_render.py`
- Modify: `src/loom/personal_dev_control_plane_status.py`
- Modify: `deploy/dev-fleet/personal-dev-control-plane.toml`
- Modify: `tests/unit/test_personal_dev_control_plane_config.py`
- Modify: `tests/unit/test_personal_dev_control_plane_render.py`
- Modify: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `tests/unit/test_service_personal_dev_builder.py`
- Create: `src/loom_personal_dev_native_builder_probe/__init__.py`
- Create: `src/loom_personal_dev_native_builder_probe/__main__.py`
- Create: `tests/unit/test_personal_dev_native_builder_probe.py`

**Interfaces:**
- Consumes: release key from Task 8 and agent freshness rows from Task 2.
- Produces inert service settings for enablement, key/profile/image/protocol/freshness/concurrency, a bounded read-only `python -m loom_personal_dev_native_builder_probe` command that emits canonical secret-free JSON from the durable agent row, and operational blockers `native_builder_disabled`, `native_builder_agent_stale`, `native_builder_identity_mismatch`, `native_builder_inventory_drift`, and `native_builder_public_store_unavailable`.

- [ ] **Step 1: Write failing config default and startup tests**

  Assert native mode defaults false, all identity strings default empty, concurrency defaults two, freshness is positive/bounded, and native enablement rejects missing HTTPS public store, public key/digest, immutable images, profile digest, or lease/deadline margin.

- [ ] **Step 2: Add schema fields and regenerate configuration**

  Run: `uv run loom config codegen`

  Then run: `uv run loom config codegen --check`

- [ ] **Step 3: Write failing render tests**

  Shadow must remain inert. Acceptance/operational render must mount only the native public key into management, set exact release/profile/protocol values, use the public MinIO origin only for signing, contain no private agent key, and require native mode without a QEMU fallback.

- [ ] **Step 4: Implement release-bound render**

  Extend the non-secret profile and acceptance/operational plan with the native provider identity. Preserve bounded predecessor parsing for the currently trusted release, but do not permit predecessor compatibility to satisfy new operational readiness.

- [ ] **Step 5: Write failing status/freshness tests**

  First test the probe with a real temporary database session: it is read-only, emits the exact bounded field set, rejects multiple configured identities, and never emits a nonce, signature, capability, URL, key path, or database value. Then cover no agent, stale/future agent, wrong boot/host/key/image/profile/protocol/concurrency, unknown managed grants, active exact grants, zero-grant ready, malformed/oversized probe output, workers unavailable, and executable ceiling zero.

- [ ] **Step 6: Implement fail-closed native readiness**

  Add a separate bounded `kubectl exec` probe command for the management container and read only its canonical secret-free agent status. A fresh exact agent removes native blockers; no agent state may remove existing Kubernetes, storage, database, DNS, capacity, or release blockers. Probe failure or ambiguity fails closed.

- [ ] **Step 7: Run config/render/status gates**

  Run:

  ```bash
  uv run loom config codegen --check
  uv run pytest -q tests/unit/test_personal_dev_control_plane_config.py tests/unit/test_personal_dev_control_plane_render.py tests/unit/test_personal_dev_control_plane_status.py tests/unit/test_personal_dev_native_builder_probe.py tests/unit/test_service_personal_dev_builder.py
  uv run ruff check src/loom/personal_dev_control_plane_config.py src/loom/personal_dev_control_plane_render.py src/loom/personal_dev_control_plane_status.py src/loom_personal_dev_native_builder_probe tests/unit/test_personal_dev_control_plane_config.py tests/unit/test_personal_dev_control_plane_render.py tests/unit/test_personal_dev_control_plane_status.py tests/unit/test_personal_dev_native_builder_probe.py
  uv run mypy src/loom/personal_dev_control_plane_config.py src/loom/personal_dev_control_plane_render.py src/loom/personal_dev_control_plane_status.py src/loom_personal_dev_native_builder_probe
  ```

- [ ] **Step 8: Commit config and readiness**

  ```bash
  git add config/loom-schema.toml src/loom_service/config/_generated.py src/loom_cli/data/loom-schema.toml tests/loom_config/snapshots/loom_service.json src/loom/personal_dev_control_plane_config.py src/loom/personal_dev_control_plane_render.py src/loom/personal_dev_control_plane_status.py src/loom_personal_dev_native_builder_probe deploy/dev-fleet/personal-dev-control-plane.toml tests/unit/test_personal_dev_control_plane_config.py tests/unit/test_personal_dev_control_plane_render.py tests/unit/test_personal_dev_control_plane_status.py tests/unit/test_personal_dev_native_builder_probe.py tests/unit/test_service_personal_dev_builder.py
  git commit -m "feat(personal-dev): gate native builder readiness"
  ```

### Task 10: Protected host convergence and acceptance runbook

**Files:**
- Create: `docs/runbooks/personal-dev-native-builder-runtime.md`
- Create: `docs/runbooks/personal-dev-native-builder-acceptance.md`
- Modify: `docs/architecture/README.md`
- Modify: `deploy/dev-fleet/README.md`
- Create: `tests/ops/test_personal_dev_native_builder_runbooks.py`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**
- Consumes: Tasks 1-9 exact release/runtime/status commands.
- Produces an inert stage/verify transaction, separately authorized activation transaction, exact rollback, sanitized evidence index, and two-owner zero-capacity acceptance sequence.

- [ ] **Step 1: Write failing runbook package-boundary tests**

  Require exact candidate/release hashes, owner-only evidence, before/after host/Slurm/capacity snapshots, no secret output, no direct personal namespace deletion, no Slurm mutation command, no task submission, agent-before-management ordering, authenticated owner API cleanup, and exact shadow/operational restoration.

- [ ] **Step 2: Write the host runtime runbook**

  Include read-only preflight, arm64 archive download/digest verification, exact sysusers collision checks, root-owned staging, installer preflight/install/verify-staged, explicit dedicated-daemon activation, current/previous release-image convergence without daemon-wide prune, two-container gVisor conformance, exact nft/cgroup/denial probes, owner-only agent key installation without printing, inactive agent-unit staging, explicit agent activation, signed readiness, and byte-identical rollback.

- [ ] **Step 3: Write the two-owner acceptance runbook**

  Require two distinct owner tokens, two different source archives, simultaneous arm64 grants and amd64 Jobs, native node/runtime evidence, immutable multi-platform image indexes, owner isolation, route smoke, no workers/tasks/Slurm mutations, capacity ceiling zero, API teardown, and final zero-grant/zero-namespace state.

- [ ] **Step 4: Run runbook policy tests**

  Run:

  ```bash
  uv run pytest -q tests/ops/test_personal_dev_native_builder_runbooks.py tests/ops/test_personal_dev_control_plane_package_boundary.py
  uv run ruff check tests/ops/test_personal_dev_native_builder_runbooks.py
  ```

- [ ] **Step 5: Commit operational documentation**

  ```bash
  git add docs/runbooks/personal-dev-native-builder-runtime.md docs/runbooks/personal-dev-native-builder-acceptance.md docs/architecture/README.md deploy/dev-fleet/README.md tests/ops/test_personal_dev_native_builder_runbooks.py tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "docs(personal-dev): add native builder rollout and acceptance"
  ```

### Task 11: Integrated verification and iterative self-review

**Files:**
- Modify only files implicated by failures or review findings.

**Interfaces:**
- Produces a review-clean branch with evidence-backed test results and no uncommitted changes.

- [ ] **Step 1: Run the complete personal-dev focused suite**

  ```bash
  uv run pytest -q \
    tests/unit/test_personal_dev_builder.py \
    tests/unit/test_personal_dev_builder_runtime.py \
    tests/unit/test_personal_dev_builder_manifest.py \
    tests/unit/test_personal_dev_sandbox_builder.py \
    tests/unit/test_personal_dev_builder_artifact.py \
    tests/unit/test_personal_dev_builder_exporter.py \
    tests/unit/test_service_personal_dev_builder.py \
    tests/unit/test_personal_dev_native_builder_protocol.py \
    tests/unit/test_personal_dev_native_builder_routes.py \
    tests/unit/test_personal_dev_native_builder_executor.py \
    tests/unit/test_personal_dev_native_builder_agent.py \
    tests/unit/test_personal_dev_native_builder_agent_main.py \
    tests/unit/test_personal_dev_native_builder_probe.py \
    tests/integration/test_personal_dev_native_builder_migration.py \
    tests/integration/test_personal_dev_native_builder_store.py \
    tests/ops/test_personal_dev_native_builder_runtime_profile.py \
    tests/ops/test_install_personal_dev_native_builder_runtime.py \
    tests/ops/test_converge_personal_dev_native_builder_release.py \
    tests/ops/test_personal_dev_native_builder_runbooks.py
  ```

- [ ] **Step 2: Run repository quality and schema gates**

  ```bash
  uv run ruff check src tests scripts migrations
  uv run mypy src/loom/personal_dev_native_builder_protocol.py src/loom/personal_dev_native_builder_store.py src/loom/personal_dev_native_builder_executor.py src/loom/personal_dev_native_builder_agent.py src/loom_service/routes/personal_dev_native_builder.py
  uv run loom config codegen --check
  uv run alembic heads
  git diff --check origin/dev..HEAD
  ```

- [ ] **Step 3: Run full unit and ops suites**

  ```bash
  uv run pytest -q tests/unit tests/ops
  ```

- [ ] **Step 4: Perform manual security review**

  Trace every secret/capability from service to client; every Docker socket and host mount; every grant transition and cancellation race; every artifact writer/key; every fallback branch; every cleanup selector; and every operational command. Record and fix each concrete finding before proceeding.

- [ ] **Step 5: Re-run affected and complete gates after every fix**

  Do not claim review completion from an earlier run. The final evidence must be from the final tree.

- [ ] **Step 6: Commit final review fixes**

  ```bash
  git add -u
  git commit -m "fix(personal-dev): close native builder review findings"
  ```

  Skip this commit only when the review produced no file changes.

### Task 12: PR, protected CI, release, and zero-capacity live acceptance

**Files:**
- No source edits unless protected CI or live evidence exposes a reproducible defect.

**Interfaces:**
- Produces a merged trusted release and sealed operational proof for the final multi-person development goal.

- [ ] **Step 1: Rebase on the current `origin/dev` and rerun final gates**

  Use a normal non-destructive rebase. Resolve only feature-overlapping changes; preserve unrelated work.

- [ ] **Step 2: Push and open a PR**

  Push `feat/personal-dev-native-builder`, open a PR summarizing authority boundaries, migrations, host runtime, tests, and explicit zero-capacity/non-Slurm scope.

- [ ] **Step 3: Monitor protected CI and fix root causes**

  Use systematic debugging for any failure. Do not merge with required checks missing, cancelled, stale, or failing.

- [ ] **Step 4: Merge and wait for a trusted image release**

  Record the exact merge commit, release run/attempt, trusted-release SHA-256, agent and builder multi-architecture digests, migration head, and host-runtime profile digest.

- [ ] **Step 5: Execute protected GB10 runtime convergence**

  Follow `docs/runbooks/personal-dev-native-builder-runtime.md`. Keep provider claims disabled until host conformance and signed zero-grant readiness pass. Do not invoke Slurm or alter executable capacity.

- [ ] **Step 6: Render and apply exact zero-capacity operational mode**

  Require server-side diff, backup/restore authority, schema transition evidence, fresh agent, OLDLAB runtime, workers unavailable, and executable-new-capacity ceiling zero.

- [ ] **Step 7: Execute two-owner acceptance**

  Follow `docs/runbooks/personal-dev-native-builder-acceptance.md`. Prove simultaneous native builds, immutable multi-platform candidates, isolated deployments/routes, and owner-only teardown.

- [ ] **Step 8: Seal and review evidence**

  Hash every sanitized artifact into an immutable evidence index. Re-read the final status, capacity, Slurm read-only snapshots, DB counts, namespaces, agents, grants, and owner resources. Fix and repeat if any blocker or unexplained residue remains.

- [ ] **Step 9: Mark the project goal complete only from final evidence**

  Completion requires: both owners passed, native arm64 and amd64 identities passed, no live owner resources after teardown, zero grants/namespaces, workers unavailable, executable ceiling zero, protected CI/release trusted, and no unresolved review finding.
