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
