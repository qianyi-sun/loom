# Task 15C — executor runtime remediation report

## Status

DONE_WITH_CONCERNS for implementation commit `228f6bd00` pending this report/ledger metadata commit.

Task 15C closes the reviewed production-runtime gap while preserving the inert posture:

- checked-in/rendered executable ceiling remains zero;
- DryRun V1 remains non-executable;
- checked-in systemd path remains validate-only/non-installable;
- no live Kubernetes, Slurm, systemd, activation, origin, or development mutation was performed.

## Root causes closed

1. The daemon had only an inert client path and could not securely enter ordinary active/drain-only execution from an authenticated manager context plus a non-rendered activation artifact.
2. The pool executor lacked a production multi-subject admission resolver; one retained subject DB client could not safely serve all production/staging/static/personal subjects.
3. Runtime assembly was incomplete: Slurm authority, profiles, journal/state/handoff paths, executor identity, and local config were not all recomputed and compared before constructing the mutating backend.
4. Heartbeats were not durable journal-first operations with replay and post-inventory evidence.
5. Drain-only execution had no structural boundary and previously could only fail instead of safely draining retained commitments.
6. Bootstrap evidence was predictable/test-like and later found to have race/crash gaps:
   - successful worker registration could delete the handoff before returning the scoped credential, losing crash replay through the wrapper exec boundary;
   - prepare and claim publication used replace semantics that could overwrite a concurrent capability/claim.
7. Manager inventory ingestion did not advance the central journal high-water to the inventory-reported durable local head, so retirement could reject the final post-inventory heartbeat.
8. Stored launch reconstruction initially omitted the deterministic handoff reference, and heartbeat equality initially compared concrete Pydantic subclasses instead of the normalized execution context fields.

## Implementation summary

### 1. Authenticated current execution context

- Added `GET /v2/executors/{pool_id}/context`.
- Added `CapacityExecutionStore.executor_current_context`.
- Added `ExecutableCapacityExecutorClient.current_execution_context`.
- `run_daemon_once()` now fetches the authenticated current context before active/drain-only assembly.

### 2. Production multi-subject admission resolver

- Added `AdmissionBindingEntryV2`, `AdmissionBindingDirectoryV2`, `RoutedExecutableAdmissionClient`, digest loading, and canonical digest computation in `src/loom_capacity_executor/runtime.py`.
- Directory and files are owner-only (`0700` directory, `0600` entries), bounded, nonsymlink, exact subject/incarnation keyed.
- Entry publication is atomic no-replace with temp-file fsync, hard-link final publish, and directory fsync.
- Accepted subject classes: `production`, `staging`, `development`, `loom-dev`, `loom-dev-*`, `static-*`.
- Explicitly rejects `loom-dev-shared`.
- Per operation, the resolver revalidates the pinned directory digest, opens one scoped `DatabaseExecutableAdmissionClient`, and disposes it.

### 3. Complete immutable runtime assembly

- Added `ActivationRuntimeArtifactV2`, secure `load_activation_runtime_artifact()`, `resolve_runtime_profile()`, and `build_executable_runtime()`.
- Runtime artifact file must be absolute, current-UID owned, `0600`, regular, nonsymlink, bounded JSON.
- Runtime directories must be current-UID owned `0700`; journal file, if present, must be `0600`.
- Normal daemon active/drain-only entry now:
  1. loads the non-rendered runtime artifact;
  2. compares it with the authenticated current context and local immutable config;
  3. builds manager client, routed admission client, typed Slurm backend, profile set, journal, handoff store, and `ExecutablePoolExecutor`;
  4. closes the journal on exit.
- The executor snapshots the complete Slurm authority it was constructed with. `run_executor_once()` rejects later backend drift across path, hash, owner, resource ceiling, timeout, and output bounds, then calls typed Slurm authority validation before work.

### 4. Journal-first heartbeat lifecycle

- Added `ExecutableHeartbeatLoop`.
- Heartbeats are journaled before sending, replay pending requests byte-for-byte, and enforce monotonic sequence/receipt binding.
- Inert and mutating paths send a pre-work heartbeat; inventory publication sends a post-inventory heartbeat.
- Heartbeat execution context comparison normalizes away subclass-only fields (`allocation_epoch`, `executable`) to preserve exact logical fences after JSON round trips.

### 5. Structurally safe drain-only execution

- Added `ExecutablePoolExecutor.tick_drain_only()`.
- Drain-only mode replays/recovers already durable work but rejects new proposal/bootstrap/permit/submission work locally.
- `run_executor_once()` now executes this drain-only boundary instead of hard failing for retained active-era commitments.

### 6. One-time CSPRNG bootstrap handoff and wrapper exchange

- Added `BootstrapHandoffStore`, `BootstrapHandoffRecordV2`, `BootstrapHandoffClaimV2`, `BootstrapHandoffCredentialV2`, and `consume_bootstrap_handoff()`.
- Clear capability is generated with OS CSPRNG (`secrets.token_urlsafe(32)`), stored only in owner-only handoff records, and exposed to manager/journal/protected DB only by SHA-256.
- Handoff reference is deterministic from the canonical intent binding (`<sha256(binding)>.json`) and Slurm argv carries only `--bootstrap-handoff=<reference>`.
- Handoff record binds the complete intent, expiry, trusted launcher release, physical binding, bootstrap epoch, and protected admission route digest.
- Prepare and claim publication are atomic no-replace; concurrent existing records/claims are loaded and validated rather than replaced.
- Before protected worker registration, the wrapper writes a durable claim containing exact physical binding, worker registration, and scoped credential.
- After successful registration, the wrapper writes a capability-free credential receipt, then deletes/fsyncs the clear handoff claim before returning the credential.
- Crash replay after response loss or after success-before-exec reuses the exact stored registration/credential and does not mint a second worker.

### 7. Two-pool/multi-owner production-entry acceptance

- The integration harness now exercises production runtime builders, routed admission resolution, heartbeat loop, profile resolver, handoff store/consumer, and deterministic handoff reference.
- The two-pool bridge tests cover two personal owners plus static/shared subjects, both pools, restart/replay, active → drain-only → inventory/heartbeat → retirement, and zero live jobs.
- Full bridge integration ran twice stably on the final source state.

## RED/GREEN evidence

Focused RED evidence observed before fixes:

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q`
  - RED: `4 failed, 4 passed in 0.50s`
  - Failures covered missing post-success credential replay and prepare/claim replacement races.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_runtime.py -q`
  - RED: `2 failed, 11 passed in 0.22s`
  - Failures covered symlink overwrite and existing-entry replacement in admission binding publication.
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py::test_daemon_entry_fetches_current_context_loads_artifact_and_assembles_runtime tests/ops/test_global_fleet_pool_executor_once.py::test_full_slurm_authority_envelope_mismatch_is_rejected -q`
  - RED: `2 failed in 0.15s`
  - Failures covered missing daemon activation-artifact entry and missing full Slurm envelope drift rejection.
- Earlier Task 15C RED evidence from this worktree:
  - bridge harness missing protected-router registration cache;
  - heartbeat replay failed after JSON round trip due subclass equality;
  - stored launch reconstruction failed until handoff reference was included;
  - inventory ingest failed to advance central journal high-water;
  - committed protected-registration replay failed on `.used`;
  - handoff route binding was missing from prepare/consume.

Focused GREEN evidence:

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q`
  - `8 passed in 0.53s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_runtime.py -q`
  - `14 passed in 0.26s`
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py::test_daemon_entry_fetches_current_context_loads_artifact_and_assembles_runtime tests/ops/test_global_fleet_pool_executor_once.py::test_full_slurm_authority_envelope_mismatch_is_rejected tests/ops/test_global_fleet_pool_executor_once.py::test_current_drain_only_authority_executes_drain_boundary tests/ops/test_global_fleet_pool_executor_once.py::test_module_entrypoint_exposes_real_daemon_arguments -q`
  - `4 passed in 0.69s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_runtime.py tests/ops/test_global_fleet_pool_executor_once.py -q`
  - `41 passed in 1.70s`

Final verification on the exact source state before report/ledger:

- `uv run --no-sync ruff format --check ... && uv run --no-sync ruff check ... && uv run --no-sync mypy`
  - `20 files already formatted`
  - `All checks passed!`
  - `Success: no issues found in 808 source files`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py tests/unit/test_capacity_executor_config.py tests/unit/test_capacity_executor_runtime.py tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_heartbeat.py -q`
  - `170 passed in 12.65s`
- `uv run --no-sync pytest tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py -q`
  - `112 passed, 1 warning in 31.44s`
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q`
  - pass 1: `4 passed in 60.61s (0:01:00)`
  - pass 2: `4 passed in 54.75s`
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q`
  - `39 passed in 1.44s`
- `git diff --check && find docs -maxdepth 1 -type d -name superpowers -print`
  - exit 0; no output.

## Controller checklist closure

- Byte-identical crash-safe handoff replay/credential persistence: closed by durable claim + capability-free credential receipt replay tests.
- Exact admission DB route binding: closed by `protected_admission_route_sha256` in handoff records and routed admission resolver route digest.
- Full handoff failure/crash matrix: closed for random/private capability, changed route, committed-response-loss replay, post-success-before-exec replay, concurrent prepare race, concurrent claim race, argv secret exclusion, and one-time clear capability removal.
- Deterministic stored-launch handoff ref: closed by deterministic reference and stored-launch reconstruction validation.
- Normal trusted-wrapper invocation/process integration: production `consume_bootstrap_handoff()` is used by the bridge harness for create → launch → physical bind → wrapper exchange → protected worker registration; fake Slurm subprocesses remain the only allowed scheduler stand-in.
- Ordinary production runtime assembly: closed by daemon current-context fetch, secure artifact loading, `build_executable_runtime()`, and journal cleanup.
- Full Slurm path/hash/owner/resource/timeout identity: closed by immutable `expected_slurm_authority` snapshot, equality check, and typed `validate_authority()`.
- Atomic owner-only nonsymlink admission-directory publication + fsync: closed by no-replace hard-link publication and symlink/existing-entry tests.
- All subject classes while rejecting `loom-dev-shared`: closed by environment-name acceptance/rejection tests.
- Stable two-run two-pool/multi-owner acceptance: closed by final bridge pass 1 and pass 2 above.

## Commits

- Implementation: `228f6bd00` (`feat: remediate executable pool runtime bridge`)
- Report/ledger metadata: this follow-up commit; final exact hash is reported by the controller response.

## Concerns

- Non-blocking existing warning remains in manager integration tests: `StarletteDeprecationWarning` from `fastapi.testclient` importing Starlette `TestClient`.
- No live activation was performed by design; production activation remains gated on a later operator-supplied non-rendered activation artifact.

## 2026-08-14 resumed controller addendum

This addendum records the follow-up remediation performed after the first
Task 15C implementation/report commit. It closes the remaining production-entry
harness and runtime-binding gaps without performing live activation.

### Additional RED evidence

- `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py -q`
  - RED: failed in `_publish_inventory` with `UnboundLocalError: cannot access local variable 'envelope'`.
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py::test_harness_uses_public_runtime_and_trusted_process_entry -q`
  - RED: failed with `AttributeError: 'ExecutableCapacityHarness' object has no attribute 'pool_runtime_entry_components'`.
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py::test_harness_uses_public_runtime_and_trusted_process_entry -q`
  - RED after adding the production-entry assertion: failed with `RuntimeAssemblyError: current execution context differs from activation artifact`.
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q`
  - RED during second-epoch runtime assembly: failed because the activation artifact approved profile set differed from the controller-local binding.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py -q`
  - RED: the direct-permit recovery regression reached launch reconstruction without the deterministic bootstrap handoff record that production replay now requires.
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py -q`
  - RED: stale inventory heartbeat sequence expectations still asserted journal sequence `2` after the journal-first heartbeat path now correctly advances to sequence `3`.

### Additional remediation summary

- Fixed terminal inventory publication so the terminal proof envelope is
  initialized before use.
- Added `test_harness_uses_public_runtime_and_trusted_process_entry`.
- Converted the bridge harness to use `current_execution_context()`,
  `PoolExecutorConfig`, `ActivationRuntimeArtifactV2`,
  `build_executable_runtime()`, `RoutedExecutableAdmissionClient`, the public
  heartbeat loop, and `run_trusted_launcher_process()` with the exact
  `SlurmLaunchRequestV2.trusted_launcher_argv()`.
- Published owner-only per-epoch admission bindings, trusted launcher config
  files, and approved profile-set digests from the harness.
- Normalized runtime current-context comparison so a base
  `ExecutionContextV2` from the manager can match the logically equivalent
  artifact `ExecutionAuthorityV2` fields while still rejecting fence drift.
- Added complete approved profile-set digest binding through
  `ApprovedLaunchProfileSetV2`, activation artifact config, immutable pool
  manifest, runtime assembly, and inert checked-in CLI rendering. Positive
  runtime assembly rejects the zero digest.
- Updated recovery and ops expectations for deterministic handoff replay and
  journal-first heartbeat sequencing.

### Additional GREEN evidence on source state before report/ledger append

- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py::test_harness_uses_public_runtime_and_trusted_process_entry -q`
  - `1 passed in 8.69s`
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q`
  - pass 1: `5 passed in 41.95s`
  - pass 2: `5 passed in 51.36s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py tests/unit/test_capacity_executor_config.py tests/unit/test_capacity_executor_runtime.py tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_heartbeat.py tests/unit/test_capacity_executor_launch_renderer.py -q`
  - `205 passed in 10.77s`
- `uv run --no-sync pytest tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py -q`
  - `112 passed, 1 warning in 27.73s`
  - later rerun of the same suite: `112 passed, 1 warning in 31.33s`
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q`
  - `39 passed in 1.55s`
- `uv run --no-sync pytest tests/loom_cli/test_capacity_control_plane.py -q`
  - `36 passed in 0.64s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_heartbeat.py tests/unit/test_capacity_executor_launch_renderer.py -q`
  - `39 passed in 1.04s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_runtime.py -q`
  - `58 passed in 0.65s`
- `uv run --no-sync ruff format --check <changed-python-files> && uv run --no-sync ruff check <changed-python-files>`
  - `23 files already formatted`
  - `All checks passed!`
- `uv run --no-sync mypy`
  - `Success: no issues found in 809 source files`
- `git diff --check && find docs -maxdepth 1 -type d -name superpowers -print`
  - exit 0; no output.

Full-repository `ruff format --check .` remains intentionally excluded from the
Task 15C gate because this repository currently has 845 pre-existing
unformatted files outside the touched set. The scoped changed-file Ruff format
and check gates above passed.

### Final verification after report/ledger append

- `uv run --no-sync ruff format --check <23 pending Python files> && uv run --no-sync ruff check <23 pending Python files>`
  - `23 files already formatted`
  - `All checks passed!`
- `uv run --no-sync mypy`
  - `Success: no issues found in 809 source files`
- `git diff --check && find docs -maxdepth 1 -type d -name superpowers -print`
  - exit 0; no output.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py tests/unit/test_capacity_executor_config.py tests/unit/test_capacity_executor_runtime.py tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_heartbeat.py tests/unit/test_capacity_executor_launch_renderer.py -q`
  - `205 passed in 10.82s`
- `uv run --no-sync pytest tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py -q`
  - `112 passed, 1 warning in 27.35s`
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q`
  - pass 1: `5 passed in 51.50s`
  - pass 2: `5 passed in 50.33s`
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q`
  - `39 passed in 3.14s`
- `uv run --no-sync pytest tests/loom_cli/test_capacity_control_plane.py -q`
  - `36 passed in 0.68s`

## Fix Round 2 — scoped review remediation

Base: `c8085c5f4` (`fix: close executable runtime production-entry gaps`).

Scoped review status: two Important findings open:

1. Production-entry profile resolution needed to prove executor work selection
   consumes the public runtime profile resolver instead of a private duplicate.
   The lifecycle-store/API portion required a technical ruling against adding a
   new operator mutation route.
2. The shipped trusted wrapper needed to authenticate the exact candidate
   executable and image before exposing the scoped worker credential, without a
   pathname verify-then-exec race.

### Root cause

- `ExecutablePoolExecutor._profile_for()` duplicated the
  `resolve_runtime_profile()` matching logic, so daemon assembly and executor
  render/acceptance could drift as profile policy evolved.
- `TrustedLauncherConfigV2` pinned only `candidate_argv`. The process parsed
  `--image-digest` but did not compare it, and
  `exec_bootstrap_handoff_candidate()` claimed the handoff/credential before
  the shipped process authenticated the candidate executable.

### Remediation

- Added `src/loom_capacity_executor/runtime_profiles.py` as the shared public
  runtime-profile resolver module. `runtime.py` re-exports
  `RuntimeAssemblyError` and `resolve_runtime_profile()`, while
  `ExecutablePoolExecutor._profile_for()` now calls that resolver directly.
- Added `TrustedCandidateExecutableV2` and required
  `candidate_executable`/`candidate_image_digest` fields in
  `TrustedLauncherConfigV2`.
- The shipped trusted process now:
  1. verifies the config-pinned image digest against Slurm argv;
  2. opens the candidate with `O_NOFOLLOW`;
  3. checks canonical path, owner, exact mode, current-UID writable mode,
     regular-file/nonsymlink identity, and SHA-256 on the opened object;
  4. consumes/claims the bootstrap handoff only after those checks; and
  5. execs `/proc/self/fd/<fd>` with the original argv so pathname replacement
     after verification does not change the executed object.
- Updated bridge trusted-launcher config publication to include the pinned
  `/usr/bin/true` candidate identity and image digest for the offline fake
  Slurm process harness.

### Lifecycle-store/API technical ruling for finding 1

No new lifecycle or operator activation route was added.

Evidence:

- Plan Task 2 Step 5 says: `Do not add a renderable ceiling or activation CLI/API route`.
- Plan Task 4 Step 5 says strict executor API routes must not expose an
  operator activation route.
- Plan Task 12 permits exact `management_store` injection and explicitly says
  no prepare, activate, drain, retire, apply, start, enable, or ceiling-change
  route.
- Plan Task 13 Step 4 requires public/database/wire boundaries with one real
  manager store/API; it does not require a renderable or routable operator
  activation surface.
- Current API coverage already asserts
  `POST /v1/execution-activations` returns `404`.
- The harness uses existing public `CapacityManagementStore` methods for
  offline fixture epoch setup (`prepare_execution_epoch`,
  `activate_execution_epoch`, `begin_execution_drain`,
  `retire_execution_epoch`) and real API/client paths for normal manager,
  executor, heartbeat, inventory, protected admission, and lifecycle
  interactions. This round added no private helpers, direct SQL activation
  shortcut, or operator mutation route.

### RED evidence

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q -k 'candidate_hash or candidate_owner or image_digest_mismatch or replaced_candidate or already_open_verified_candidate'`
  - RED: `5 failed, 16 deselected in 0.25s`
  - Representative failure: config rejected `candidate_executable` and
    `candidate_image_digest` as extra fields, yielding generic
    `trusted launcher config is invalid` instead of authenticating candidate
    hash/owner/image and executing the verified object.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_rejects_current_uid_writable_candidate_mode -q`
  - RED: `1 failed in 0.38s`
  - Failure reached `_ExecBoundaryError` with
    `LOOM_EXECUTOR_WORKER_CREDENTIAL` in the environment, proving a
    current-UID writable candidate mode could receive the credential.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py::test_render_launch_uses_public_runtime_profile_resolver -q`
  - RED: `1 failed in 0.19s`
  - Failure: `Failed: DID NOT RAISE <class 'loom_capacity_executor.runtime.RuntimeAssemblyError'>`, proving render/acceptance bypassed the public resolver sentinel.

### GREEN evidence

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q -k 'candidate_hash or candidate_owner or current_uid_writable or image_digest_mismatch or replaced_candidate or already_open_verified_candidate or process_entry_derives_physical_binding'`
  - `7 passed, 15 deselected in 0.76s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py::test_render_launch_uses_public_runtime_profile_resolver -q`
  - `1 passed in 0.13s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q`
  - `22 passed in 1.93s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py -q`
  - `27 passed in 1.90s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_runtime.py -q`
  - `16 passed in 0.43s`
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py::test_harness_uses_public_runtime_and_trusted_process_entry -q`
  - `1 passed in 15.53s`

### Final verification on source state before report/ledger append

- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py tests/unit/test_capacity_executor_config.py tests/unit/test_capacity_executor_runtime.py tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_heartbeat.py tests/unit/test_capacity_executor_launch_renderer.py -q`
  - `212 passed in 12.71s`
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q`
  - pass 1: `5 passed in 70.29s (0:01:10)`
  - pass 2: `5 passed in 59.41s`
- `uv run --no-sync pytest tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py -q`
  - `112 passed, 1 warning in 20.37s`
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q`
  - `39 passed in 1.55s`
- `uv run --no-sync pytest tests/loom_cli/test_capacity_control_plane.py -q`
  - `36 passed in 0.66s`
- `uv run --no-sync ruff format --check <7 pending Python files> && uv run --no-sync ruff check <7 pending Python files>`
  - `7 files already formatted`
  - `All checks passed!`
- `uv run --no-sync mypy`
  - `Success: no issues found in 810 source files`
- `git diff --check && find docs -maxdepth 1 -type d -name superpowers -print`
  - exit 0; no output.

### Files changed

- `src/loom_capacity_executor/executable.py`
- `src/loom_capacity_executor/runtime.py`
- `src/loom_capacity_executor/runtime_profiles.py`
- `src/loom_capacity_executor/trusted_launcher.py`
- `tests/support/executable_capacity_harness.py`
- `tests/unit/test_capacity_executor_bootstrap_handoff.py`
- `tests/unit/test_capacity_executor_executable.py`
- this report and `progress.md`

### Self-review

- DryRun V1 contracts were not changed.
- Checked-in/rendered executable ceiling remains zero; no activation artifact,
  activation CLI, activation API, systemd install/start, Slurm, Kubernetes, or
  origin/dev mutation was performed.
- The trusted-wrapper rejection tests assert no protected registration,
  capability consumption, exec call, or worker credential exposure on rejected
  candidate hash, owner, current-UID writable mode, replacement, or image
  mismatch.
- The success test proves the exec target is `/proc/self/fd/<fd>` and still
  reads the originally verified candidate after the configured pathname is
  replaced.

## Fix Round 3 — sealed candidate snapshot remediation

Base: `edb60fabb` (`fix: close executable bridge round 2 review gaps`).

Scoped fix round 2 re-review status:

1. Addressed: executor work selection now consumes the shared public runtime
   profile resolver, and no lifecycle mutation route was added.
2. Important open: the trusted wrapper still returned an authenticated
   descriptor to a current-UID-owned source inode, so a `0555` candidate could
   be `chmod`ed and rewritten in-place after hashing and before exec.
3. Important open: the candidate descriptor was opened close-on-exec while the
   wrapper executed `/proc/self/fd/<fd>`, so real shebang candidates failed when
   the interpreter tried to reopen the now-closed descriptor.

### Root cause

- Round 2 authenticated the candidate path, owner, mode, image, and opened
  source descriptor before consuming the bootstrap handoff, but it kept the
  source inode as the object passed to exec. For same-UID ownership, `0555`
  does not make the inode immutable because the owner can `chmod` and rewrite
  the same inode after the hash is computed.
- The previous success coverage used injected `execvpe` and read
  `/proc/self/fd/<fd>` before a real exec. That masked the Linux shebang path:
  `execve("/proc/self/fd/<fd>", argv, env)` closes close-on-exec descriptors
  before `/bin/sh` reopens the script path.

### RED evidence

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_verified_candidate_descriptor_is_immutable_after_same_inode_rewrite tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_real_exec_supports_shebang_candidate_descriptor -q`
  - RED: `2 failed in 0.38s`.
  - Same-inode mutation failure: the descriptor read
    `#!/bin/sh\nprintf 'mutated\n'\n` instead of the preverified
    `#!/bin/sh\nprintf 'original\n'\n`.
  - Real shebang failure: child exited `2` and stderr contained
    `/bin/sh: 0: cannot open /proc/self/fd/15: No such file`.

### Remediation

- `_open_verified_candidate()` still verifies the configured canonical path,
  nonsymlink regular-file identity, owner, exact mode, current-UID writable
  mode, and SHA-256 before the bootstrap handoff is consumed.
- After source verification, it now streams the verified bytes into an
  anonymous Linux `memfd` snapshot, closes the source descriptor, seals the
  snapshot with write/grow/shrink/further-seal prevention, then marks only the
  sealed descriptor inheritable for the exec handoff.
- The trusted process still calls `exec_bootstrap_handoff_candidate()` only
  after the image digest and sealed candidate snapshot are authenticated, and
  still passes the original candidate argv with no shell.
- Added focused regressions for same-inode post-verification mutation and a
  real child-process shebang exec using production `os.execvpe`.
- Added libc-backed `memfd_create` and Linux fcntl seal constants because this
  Python runtime does not expose `os.memfd_create` or the seal constants even
  though the Linux kernel/libc support them.

### GREEN evidence and verification

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_verified_candidate_descriptor_is_immutable_after_same_inode_rewrite tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_real_exec_supports_shebang_candidate_descriptor -q`
  - `2 passed in 0.17s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q`
  - `24 passed in 0.34s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py tests/unit/test_capacity_executor_config.py tests/unit/test_capacity_executor_runtime.py tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_heartbeat.py tests/unit/test_capacity_executor_launch_renderer.py -q`
  - `214 passed in 9.59s`
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q`
  - pass 1: `5 passed in 28.84s`
  - pass 2: `5 passed in 39.82s`
- `uv run --no-sync pytest tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py -q`
  - `112 passed, 1 warning in 19.04s`
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q`
  - `39 passed in 0.87s`
- `uv run --no-sync pytest tests/loom_cli/test_capacity_control_plane.py -q`
  - `36 passed in 0.67s`
- `uv run --no-sync ruff format --check <2 pending Python files> && uv run --no-sync ruff check <2 pending Python files>`
  - `2 files already formatted`
  - `All checks passed!`
- `uv run --no-sync mypy`
  - `Success: no issues found in 810 source files`
- `git diff --check && find docs -maxdepth 1 -type d -name superpowers -print`
  - exit 0; no output.

The existing manager integration warning remains the known
`StarletteDeprecationWarning` from `fastapi.testclient` importing Starlette's
`TestClient`.

### Files changed

- `src/loom_capacity_executor/trusted_launcher.py`
- `tests/unit/test_capacity_executor_bootstrap_handoff.py`
- this report and `progress.md`

### Self-review

- DryRun V1 contracts were not changed.
- Checked-in/rendered executable ceiling remains zero.
- No live Kubernetes, Slurm, systemd, activation, `origin/dev`, or lifecycle
  mutation route work was performed.
- The source candidate descriptor is closed before physical binding resolution,
  admission construction, handoff consumption, claim, credential exposure, and
  exec.
- Rejection/error paths close both source and snapshot descriptors; if injected
  `execvpe` returns or raises, `run_trusted_launcher_process()` closes the
  sealed descriptor in its `finally` block.
- A successful candidate receives at most the sealed nonsecret snapshot
  descriptor needed for `/proc/self/fd/<fd>` exec and shebang interpreter
  reopen.

## Fix Round 4 — post-seal snapshot authentication

Base: `f544b847f` (`fix: seal trusted launcher candidate snapshot`).

Scoped fix round 3 re-review status:

1. Addressed: the trusted wrapper no longer executes a mutable source inode.
2. Addressed: the sealed nonsecret descriptor is inheritable, so real shebang
   candidates can be executed through `/proc/self/fd/<fd>`.
3. Important open: the candidate bytes copied into the memfd were hashed before
   the memfd was sealed, and the immutable sealed snapshot was never
   independently rehashed. A same-UID process could reopen the unsealed
   descriptor through `/proc/<pid>/fd/<fd>` and change the bytes after the
   initial copy/hash but before seals were added.

### Root cause

Round 3 correctly moved execution to a sealed anonymous memfd snapshot, but
treated the source-copy digest as proof of the eventual sealed object. That
left a narrow writable window between the final source read and
`F_ADD_SEALS`. If the memfd bytes changed in that window, the wrapper would
seal attacker-controlled bytes and proceed to physical binding, admission,
handoff claim, credential exposure, and exec without authenticating the object
actually handed to the candidate process.

### RED evidence

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_rejects_memfd_mutation_between_copy_hash_and_seal -q`
  - RED: `1 failed in 0.18s`
  - Failure reached `_ExecBoundaryError` from `fake_execvpe` with
    `LOOM_EXECUTOR_WORKER_CREDENTIAL` in the environment, proving the mutated
    memfd reached the candidate exec boundary after protected registration and
    credential exposure.

### Remediation

- Added `_candidate_snapshot_sha256()` to seek and read the sealed snapshot
  descriptor after `F_ADD_SEALS` completes.
- `_open_verified_candidate()` now independently hashes the immutable sealed
  snapshot and uses `hmac.compare_digest()` against the config-pinned
  candidate SHA-256 before marking the descriptor inheritable.
- Any post-seal digest mismatch, seek/read failure, or sealing failure raises
  the bounded `BootstrapHandoffError` while the existing error path closes the
  source and snapshot descriptors.
- No physical binding resolution, routed admission construction, bootstrap
  claim, credential exposure, or exec happens until after the post-seal
  immutable-snapshot digest matches.

### GREEN evidence and verification

- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_rejects_memfd_mutation_between_copy_hash_and_seal -q`
  - `1 passed in 0.13s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_rejects_memfd_mutation_between_copy_hash_and_seal tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_verified_candidate_descriptor_is_immutable_after_same_inode_rewrite tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_real_exec_supports_shebang_candidate_descriptor tests/unit/test_capacity_executor_bootstrap_handoff.py::test_trusted_launcher_executes_already_open_verified_candidate -q`
  - `4 passed in 0.18s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q`
  - `25 passed in 0.33s`
- After Ruff formatting the touched test file, rerun:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_bootstrap_handoff.py -q`
  - `25 passed in 1.24s`
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py tests/unit/test_capacity_executor_config.py tests/unit/test_capacity_executor_runtime.py tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_heartbeat.py tests/unit/test_capacity_executor_launch_renderer.py -q`
  - `215 passed in 8.72s`
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q`
  - pass 1: `5 passed in 22.50s`
  - pass 2: `5 passed in 23.61s`
- `uv run --no-sync pytest tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_manager_execution_store.py tests/integration/test_capacity_manager_execution_epoch.py -q`
  - `112 passed, 1 warning in 19.37s`
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q`
  - `39 passed in 1.03s`
- `uv run --no-sync pytest tests/loom_cli/test_capacity_control_plane.py -q`
  - `36 passed in 0.71s`
- `uv run --no-sync ruff format --check src/loom_capacity_executor/trusted_launcher.py tests/unit/test_capacity_executor_bootstrap_handoff.py && uv run --no-sync ruff check src/loom_capacity_executor/trusted_launcher.py tests/unit/test_capacity_executor_bootstrap_handoff.py`
  - `2 files already formatted`
  - `All checks passed!`
- `uv run --no-sync mypy`
  - `Success: no issues found in 810 source files`
- `git diff --check && find docs -maxdepth 1 -type d -name superpowers -print`
  - exit 0; no output.

The existing manager integration warning remains the known
`StarletteDeprecationWarning` from `fastapi.testclient` importing Starlette's
`TestClient`.

### Files changed

- `src/loom_capacity_executor/trusted_launcher.py`
- `tests/unit/test_capacity_executor_bootstrap_handoff.py`
- this report and `progress.md`

### Self-review

- DryRun V1 contracts were not changed.
- Checked-in/rendered executable ceiling remains zero.
- No live Kubernetes, Slurm, systemd, activation, `origin/dev`, or lifecycle
  mutation route work was performed.
- The new regression names the production mutation it catches: removing the
  post-seal digest compare lets changed memfd bytes reach credential exposure
  and candidate exec.
- The regression exercises production wrapper behavior and monkeypatches only
  the real seal boundary to deterministically simulate the same-UID `/proc`
  reopen race.
- On mismatch, the test proves no admission request, bootstrap capability use,
  worker credential exposure, launch claim, or exec call occurs, and that the
  touched memfd descriptor is closed.
- Normal sealed snapshot execution and the real shebang process path remain
  covered and passing.
