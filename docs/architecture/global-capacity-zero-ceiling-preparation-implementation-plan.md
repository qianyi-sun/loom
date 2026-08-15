# Global Capacity Zero-Ceiling Preparation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one global execution epoch deployable, observable, and safely
abortable at an immutable zero executable-capacity ceiling, using real
controller-local read-only OLDLAB and GB10 inventories.

**Architecture:** The manager loads an optional digest-pinned owner policy and
exposes only prepare, exact executor registration, prepared-readiness, and
prepared-abort routes. Pool-local prepared services load separately pinned
executor and inventory policies, register against the prepared epoch, capture
Slurm through the existing fixed read-only runner, journal before publishing,
and refuse every active or drain-only context.

**Tech Stack:** Python 3.11, Pydantic v2/pydantic-settings, FastAPI, SQLAlchemy
2/PostgreSQL, httpx mTLS, typed Slurm JSON inventory, systemd, Kubernetes YAML,
pytest, Ruff, and mypy.

## Global constraints

- All checked-in and rendered executable-new-capacity ceilings remain exactly
  `0`; this package adds no `prepared -> active` route or command.
- `loom-dev` is the shared infrastructure namespace. Personal applications use
  `loom-dev-<owner>`; never create `loom-dev-shared`.
- The manager is the only allocation authority. OLDLAB and GB10 executors are
  exact pool-local credential/failure domains and contain no allocation policy.
- Pool placement remains manager-owned. Users configure no pool weights.
- Candidate identities retain their tagged `git-sha1` or `source-sha256`
  representation without translation.
- Inventory may invoke only the fixed typed read-only `scontrol show nodes
  --json` and `squeue --json` commands. It never constructs an executable
  scheduler backend in prepared mode.
- Missing, stale, partial, contradictory, foreign, ambiguous, unknown, or
  quarantined evidence keeps prepared readiness false and capacity frozen.
- This branch must not apply Kubernetes YAML, install/start/enable systemd
  units, mutate Slurm, write a registry, or change a live database.
- Preserve all unrelated worktrees, branches, untracked files, and the main
  checkout's user-owned `.codex/` directory.

---

### Task 1: Production owner-policy loading and least-privilege scopes

**Files:**

- Create: `src/loom_capacity_manager/execution_policy.py`
- Modify: `src/loom_capacity_manager/config.py`
- Modify: `src/loom_capacity_manager/api.py`
- Modify: `src/loom_capacity_manager/auth.py`
- Create: `tests/unit/test_capacity_execution_policy.py`
- Modify: `tests/unit/test_capacity_auth.py`
- Modify: `tests/integration/test_capacity_manager_api.py`

**Interfaces:**

- Produces `load_execution_preparation_policy(path: Path,
  expected_sha256: str) -> ExecutionPreparationPolicyV2`.
- Adds optional paired settings `execution_policy_file: Path | None` and
  `execution_policy_sha256: str | None`.
- Adds unbound scopes `capacity:execution:prepare` and
  `capacity:execution:abort`.
- Makes the default `create_app()` store consume the pinned policy; injected
  stores in tests retain their explicit policy.

- [ ] **Step 1: Write policy-loader and settings failure tests**

  Cover a current-UID regular file, canonical policy JSON, exact checksum, and
  successful load. Parameterize missing file, symlink, FIFO/non-regular file,
  file larger than `MAX_CONTRACT_BYTES`, changed inode/metadata, malformed JSON,
  unknown fields, uppercase/non-64-character checksum, zero checksum, checksum
  mismatch, and only one of the two settings fields. Assert failures contain no
  policy bytes or path supplied by the test.

  Use a real fixture policy serialized with:

  ```python
  payload = canonical_executable_bytes(execution_policy())
  expected = hashlib.sha256(payload).hexdigest()
  policy_file.write_bytes(payload)
  policy_file.chmod(0o600)
  assert load_execution_preparation_policy(policy_file, expected) == execution_policy()
  ```

- [ ] **Step 2: Run the new loader tests and verify the API construction test fails**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_capacity_execution_policy.py \
    tests/unit/test_capacity_auth.py \
    tests/integration/test_capacity_manager_api.py -k 'execution_policy or scope'
  ```

  Expected: failure because the loader, settings pair, and scopes do not exist.

- [ ] **Step 3: Implement the bounded stable-file loader**

  In `execution_policy.py`, open with `O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, validate a
  regular current-UID-owned file with no group/other write bits, cap bytes at
  `MAX_CONTRACT_BYTES`, compare pre/post `fstat` identity and size/timestamps,
  hash the exact bytes with `hashlib.sha256`, and parse only with
  `ExecutionPreparationPolicyV2.model_validate_json`. Expose one generic
  `ExecutionPolicyError` for all unsafe-input failures.

- [ ] **Step 4: Add paired settings and least-privilege scopes**

  Add a `model_validator(mode="after")` to `CapacityManagerSettings` requiring
  both policy settings or neither. Validate the digest against
  `^[0-9a-f]{64}$` and reject the all-zero digest. Add the two new scopes to
  `CapacityScope`. Extend principal validation so any principal with either
  scope is unbound (`subject_id`, `pool_id`, and executor fields all absent),
  while the two scopes need not be granted together.

- [ ] **Step 5: Wire policy loading into the production store construction**

  Resolve the optional policy before constructing the default
  `CapacityManagementStore`:

  ```python
  execution_policy = (
      None
      if settings.execution_policy_file is None
      else load_execution_preparation_policy(
          settings.execution_policy_file,
          cast(str, settings.execution_policy_sha256),
      )
  )
  resolved_management_store = management_store or CapacityManagementStore(
      freshness_seconds=settings.freshness_seconds,
      execution_policy=execution_policy,
  )
  ```

  Do not reload the file during requests. Do not weaken injected-store tests.

- [ ] **Step 6: Run focused tests and commit**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_capacity_execution_policy.py \
    tests/unit/test_capacity_auth.py \
    tests/integration/test_capacity_manager_api.py -k 'policy or principal or startup'
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom_capacity_manager tests/unit/test_capacity_execution_policy.py \
    tests/unit/test_capacity_auth.py tests/integration/test_capacity_manager_api.py
  ```

  Commit:

  ```bash
  git add src/loom_capacity_manager tests/unit/test_capacity_execution_policy.py \
    tests/unit/test_capacity_auth.py tests/integration/test_capacity_manager_api.py
  git commit -m "feat(capacity): load exact execution preparation policy"
  ```

---

### Task 2: Exact prepared abort and manager-derived readiness

**Files:**

- Modify: `src/loom_capacity_manager/executable_contracts.py`
- Modify: `src/loom_capacity_manager/store.py`
- Create: `src/loom_capacity_manager/preparation_readiness.py`
- Modify: `src/loom_capacity_manager/api.py`
- Modify: `tests/unit/test_capacity_manager_executable_contracts.py`
- Modify: `tests/integration/test_capacity_manager_execution_epoch.py`
- Create: `tests/integration/test_capacity_preparation_readiness.py`

**Interfaces:**

- Produces strict `ExecutionPreparationAbortV2` with authority, writer,
  execution epoch, manifest digest, and `executable: Literal[True]` fences.
- Produces `CapacityManagementStore.abort_prepared_execution_epoch(...) ->
  RetiredExecutionEpoch`.
- Adds the read-only `CapacityManagementStore.execution_policy` property.
- Produces `load_prepared_execution_readiness(session, *, execution_policy,
  execution_policy_sha256, freshness_seconds) ->
  PreparedExecutionReadinessV2`; the function obtains current time from the
  database inside the read transaction.

- [ ] **Step 1: Write contract and store abort tests**

  Prove the contract rejects nil/invalid epochs, extra fields, malformed
  digests, and non-true executable values. Prepare a real epoch, register zero,
  one, and two executors in separate cases, then prove exact abort returns
  shadow at ceiling zero and appends retirement evidence. Cover replay, reused
  idempotency key, wrong actor/request, stale writer, wrong epoch/manifest,
  active/drain-only states, and any existing executable intent.

  The successful request is exact:

  ```python
  ExecutionPreparationAbortV2(
      authority_incarnation=AUTHORITY_ID,
      expected_writer_epoch=writer.writer_epoch,
      execution_epoch=prepared.execution_epoch,
      execution_manifest_sha256=prepared.execution_manifest_sha256,
  )
  ```

- [ ] **Step 2: Run abort tests and observe the missing contract/method failure**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_capacity_manager_executable_contracts.py \
    tests/integration/test_capacity_manager_execution_epoch.py -k 'abort_prepared'
  ```

- [ ] **Step 3: Implement the append-only abort transition**

  Lock the singleton authority and exact `CapacityExecutionEpoch`. Revalidate
  the current pinned preparation policy with `_validate_execution_preparation`.
  Lock and reject any `CapacityExecutableIntent` for the epoch. Set the epoch to
  `retired`, fill the existing retirement actor/idempotency/digest/payload/time
  columns, restore authority to `shadow` with epoch `0`, manifest `None`, ceiling
  `0`, `increase_freeze=True`, and reason
  `execution_preparation_aborted`. Emit
  `capacity_execution_preparation_aborted`. Replays must compare every stored
  field and must not require the epoch still to be current.

- [ ] **Step 4: Write readiness matrix tests**

  Build database fixtures for: policy disabled; shadow; prepared with no
  executors; one executor; expired lease; no inventory; stale inventory;
  heartbeat not newer than inventory; journal mismatch; invalid inventory JSON;
  foreign record; unknown record; quarantine; changed executor identity;
  incomplete subject acknowledgements; and a complete empty two-pool prepared
  inventory. Assert stable sorted blocker codes and that only the final case has
  `ready=True`.

  The result contract must contain bounded data only:

  ```python
  PreparedReadinessBlocker = Literal[
      "execution-policy-disabled",
      "manager-shadow",
      "manager-not-prepared",
      "nonzero-executable-ceiling",
      "increase-freeze-missing",
      "subject-acknowledgements-incomplete",
      "executor-registration-missing",
      "executor-binding-changed",
      "executor-lease-expired",
      "executor-inventory-missing",
      "executor-inventory-invalid",
      "executor-inventory-stale",
      "executor-post-inventory-heartbeat-missing",
      "executor-inventory-foreign",
      "executor-inventory-unknown",
      "executor-inventory-quarantined",
      "executor-inventory-ownership-missing",
  ]

  class PreparedExecutionReadinessV2(StrictV2Model):
      ready: bool
      policy_mode: Literal["disabled", "pinned"]
      policy_sha256: Digest | None
      execution: ExecutionContextV2 | None
      expected_subject_count: Quantity
      acknowledged_subject_count: Quantity
      executors: tuple[PreparedExecutorReadinessV1, ...]
      blockers: tuple[PreparedReadinessBlocker, ...]
      executable: Literal[False] = False
  ```

- [ ] **Step 5: Implement readiness without scheduler or credential access**

  Query only manager tables and obtain time from `clock_timestamp()` in the
  same transaction. Reuse the strict
  `ExecutableExecutorInventoryV2` parser and canonical digest verification.
  Require canonical pool order `("gb10", "oldlab")`, exact prepared bindings,
  lease freshness, complete inventory, inventory freshness, and a heartbeat
  strictly after the last inventory. Count record classifications and block on
  foreign, unknown, unsigned registered-Loom, or quarantined evidence. Validate
  the complete subject acknowledgement set against the current configuration
  using the same manager-store invariants as preparation.

- [ ] **Step 6: Run focused tests and commit**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_capacity_manager_executable_contracts.py \
    tests/integration/test_capacity_manager_execution_epoch.py \
    tests/integration/test_capacity_preparation_readiness.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom_capacity_manager/executable_contracts.py \
    src/loom_capacity_manager/store.py \
    src/loom_capacity_manager/preparation_readiness.py
  ```

  Commit:

  ```bash
  git add src/loom_capacity_manager tests/unit/test_capacity_manager_executable_contracts.py \
    tests/integration/test_capacity_manager_execution_epoch.py \
    tests/integration/test_capacity_preparation_readiness.py
  git commit -m "feat(capacity): fence prepared readiness and abort"
  ```

---

### Task 3: Protected preparation, registration, abort, and status HTTP routes

**Files:**

- Modify: `src/loom_capacity_manager/api.py`
- Modify: `src/loom_capacity_executor/client.py`
- Modify: `tests/integration/test_capacity_manager_api.py`
- Modify: `tests/unit/test_capacity_executor_client.py`

**Interfaces:**

- Adds the four HTTP routes defined in the design.
- Produces `ExecutableCapacityExecutorClient.register_execution_executor(*,
  idempotency_key: UUID) -> ExecutionContextV2`.
- Retains no activation, drain, retirement, apply, or ceiling-change route.

- [ ] **Step 1: Write authorization and transport tests**

  Test missing/invalid token, wrong scope, subject-bound principal, pool-bound
  mismatch, path/body pool mismatch, executor/incarnation/generation mismatch,
  missing/malformed/reused idempotency key, oversized/chunked body, invalid
  contract, store conflict redaction, and success/replay for each mutating route.
  Test the status route in disabled, shadow, incomplete-prepared, and ready
  states. The client test must assert the exact method, path, authorization,
  idempotency header, canonical request bytes, bounded response, and response
  binding.

- [ ] **Step 2: Run route/client tests and confirm 404 or missing-method failures**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/integration/test_capacity_manager_api.py -k 'execution_preparation or execution_executor_registration' \
    tests/unit/test_capacity_executor_client.py -k 'register_execution'
  ```

- [ ] **Step 3: Add strict request dependencies and routes**

  Use the existing `contract_body()` size-limited parser. The prepare route
  calls `prepare_execution_epoch`; executor registration calls
  `register_execution_executor`; abort first checks the path epoch equals the
  body epoch, then calls `abort_prepared_execution_epoch`; readiness calls the
  read-only loader. Pass `actor.principal_id` and `Idempotency-Key` without
  accepting actor/request fields from query strings.

- [ ] **Step 4: Add exact executor registration transport**

  Serialize `self.registration` with `canonical_executable_bytes`, send it to
  `/v2/executors/{pool_id}/registration`, parse `ExecutionContextV2`, and require
  it equals `self.registration.execution`. Use the caller-supplied stable
  idempotency key; do not generate a new key during retry.

- [ ] **Step 5: Prove the mutation surface remains bounded and commit**

  Assert these remain 404: `/v2/execution-activations`,
  `/v2/execution-drains`, `/v2/execution-retirements`, and any generic
  `/v2/execution-transitions`. Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/integration/test_capacity_manager_api.py \
    tests/unit/test_capacity_executor_client.py
  ```

  Commit:

  ```bash
  git add src/loom_capacity_manager/api.py src/loom_capacity_executor/client.py \
    tests/integration/test_capacity_manager_api.py tests/unit/test_capacity_executor_client.py
  git commit -m "feat(capacity): expose zero-ceiling preparation API"
  ```

---

### Task 4: Strict controller-local inventory policy and rendering

**Files:**

- Create: `src/loom_capacity_pool_executor/config.py`
- Modify: `src/loom_cli/capacity_control_plane.py`
- Modify: `src/loom_cli/capacity_control_plane_cmd.py`
- Modify: `deploy/dev-fleet/capacity-pool-executor.toml.example`
- Create: `tests/unit/test_capacity_pool_executor_config.py`
- Modify: `tests/loom_cli/test_capacity_control_plane.py`
- Modify: `tests/loom_cli/test_capacity_control_plane_cmd.py`

**Interfaces:**

- Produces `load_slurm_inventory_policy(path: Path, *, expected_sha256: str) ->
  SlurmInventoryPolicy` with the same stable-file guarantees as Task 1.
- Extends each `CapacityPoolExecutorBinding` with one strict inventory policy.
- Adds `render_capacity_pool_inventory_policies(profile) -> dict[str, str]` and
  `render-executor --output inventory-policy`.

- [ ] **Step 1: Write loader and profile validation tests**

  Cover exact JSON round-trip and digest. Reject unsafe files, wrong digest,
  wrong pool/generation, empty/duplicate/case-colliding nodes, nodes outside the
  executor pool, zero slot resources, generic resources, unsupported Slurm
  version, malformed parser, root/query UID confusion, zero visibility digest,
  duplicate partitions, mutable executable paths, and profile/inventory pool
  generation drift.

- [ ] **Step 2: Write deterministic renderer/CLI tests and observe failures**

  Assert GB10 and OLDLAB render distinct one-line canonical JSON, the rendered
  policy digest changes when any consumed node/controller/query fact changes,
  and `--pool` emits only its exact policy. Invalid inputs must produce no
  partial stdout and must not echo rejected values.

- [ ] **Step 3: Implement the bounded loader**

  Parse a strict internal document model and construct the existing frozen
  `SlurmInventoryPolicy`. Compare the document pool/generation with the caller's
  executor config during Task 5. Do not accept executable paths in the document;
  `SubprocessReadOnlySlurmCommandRunner` retains its fixed command constants and
  verifies only their digests.

- [ ] **Step 4: Extend the typed TOML model and example**

  Add nested inventory fields under each `[[pools]]` entry. The example uses
  shape-valid, non-live identities and at least one canonical node per pool.
  Keep its global executable ceiling at zero and keep GB10/OLDLAB credentials,
  state, journal, controller, and nodes distinct.

- [ ] **Step 5: Implement canonical render and CLI output**

  Render with `json.dumps(..., sort_keys=True, separators=(",", ":"),
  ensure_ascii=True, allow_nan=False) + "\n"`. Update the CLI output choices
  without adding an install/apply/activate command.

- [ ] **Step 6: Run focused tests and commit**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_capacity_pool_executor_config.py \
    tests/unit/test_capacity_pool_executor_slurm_inventory.py \
    tests/loom_cli/test_capacity_control_plane.py \
    tests/loom_cli/test_capacity_control_plane_cmd.py
  ```

  Commit:

  ```bash
  git add src/loom_capacity_pool_executor src/loom_cli/capacity_control_plane.py \
    src/loom_cli/capacity_control_plane_cmd.py \
    deploy/dev-fleet/capacity-pool-executor.toml.example \
    tests/unit/test_capacity_pool_executor_config.py \
    tests/loom_cli/test_capacity_control_plane.py \
    tests/loom_cli/test_capacity_control_plane_cmd.py
  git commit -m "feat(capacity): render exact Slurm inventory policy"
  ```

---

### Task 5: Journaled prepared physical inventory runtime

**Files:**

- Modify: `scripts/ops/global_fleet_pool_executor_once.py`
- Modify: `src/loom_capacity_executor/client.py`
- Modify: `src/loom_capacity_executor/config.py`
- Modify: `tests/ops/test_global_fleet_pool_executor_once.py`
- Modify: `tests/unit/test_capacity_executor_client.py`
- Modify: `tests/unit/test_capacity_executor_config.py`

**Interfaces:**

- Adds CLI arguments `--prepared-only`, `--inventory-policy`, and
  `--expected-inventory-policy-sha256`.
- Produces `run_prepared_inventory_once(config, policy, *, client,
  runner_factory=SubprocessReadOnlySlurmCommandRunner) -> ExecutorOnceResult`.
- Prepared-only execution returns `mode="inventory-only"`; it rejects shadow,
  active, and drain-only contexts.

- [ ] **Step 1: Write prepared runtime happy-path tests**

  Use a fake client and fixed read-only runner. Assert exact order:
  current-context read, registration, heartbeat, checkpoint, two read-only
  queries, journal request, inventory publication, journal confirmation,
  heartbeat. Assert the published inventory includes every classified physical
  record and the exact manager/journal/pool binding.

- [ ] **Step 2: Write fail-closed and restart matrix tests**

  Cover config/policy pool or generation mismatch, policy digest mismatch,
  context fence mismatch, shadow/active/drain-only context, registration
  rejection, query warning/race/timeout/oversize output, journal append failure,
  publication timeout, publication rejection, confirmation append failure, and
  termination at every await. On restart after a durable publish request,
  assert the byte-identical payload is replayed without rerunning Slurm. Assert
  no test can observe construction of `AsyncSlurmBackend`, `sbatch`, or
  `scancel`.

- [ ] **Step 3: Run tests and verify the prepared-only path is absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_global_fleet_pool_executor_once.py -k prepared
  ```

- [ ] **Step 4: Implement deterministic registration and policy binding**

  Derive the stable registration idempotency UUID with UUIDv5 over the canonical
  registration digest and a fixed package namespace. Require the loaded policy
  pool/generation/query UID to match `PoolExecutorConfig` and current EUID before
  any HTTP or Slurm query.

- [ ] **Step 5: Implement journal-first physical inventory**

  Reuse `capture_slurm_capacity_reports` and publish only its executable
  inventory through the executor credential. Preserve the paired shadow pool
  observation for the separate pool-reporter path; do not grant that scope to
  the executor. Use the manager checkpoint's next inventory sequence and the
  current journal head. A replayed pending journal event is the only source of
  the retry payload.

- [ ] **Step 6: Make production CLI mode separation explicit**

  `--validate-only` retains the synthetic no-query validation behavior.
  `--prepared-only` requires both policy arguments and refuses an activation
  artifact. The existing executable path requires an activation artifact and
  refuses `--prepared-only`. Make all combinations mutually exclusive in
  argparse and again in runtime validation.

- [ ] **Step 7: Run focused tests, strict typing, and commit**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_global_fleet_pool_executor_once.py \
    tests/unit/test_capacity_executor_client.py \
    tests/unit/test_capacity_executor_config.py \
    tests/unit/test_capacity_pool_executor_slurm_inventory.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    scripts/ops/global_fleet_pool_executor_once.py \
    src/loom_capacity_executor src/loom_capacity_pool_executor
  ```

  Commit:

  ```bash
  git add scripts/ops/global_fleet_pool_executor_once.py \
    src/loom_capacity_executor tests/ops/test_global_fleet_pool_executor_once.py \
    tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_config.py
  git commit -m "feat(capacity): publish prepared physical inventory"
  ```

---

### Task 6: Digest-addressed manager policy render and prepared systemd service

**Files:**

- Modify: `src/loom_cli/capacity_control_plane.py`
- Modify: `src/loom_cli/capacity_control_plane_cmd.py`
- Create: `deploy/dev-fleet/loom-capacity-pool-executor-prepared.service`
- Create: `deploy/dev-fleet/loom-capacity-pool-executor-prepared.timer`
- Modify: `tests/loom_cli/test_capacity_control_plane.py`
- Modify: `tests/loom_cli/test_capacity_control_plane_cmd.py`
- Modify: `tests/ops/test_capacity_pool_executor_package_boundary.py`

**Interfaces:**

- Extends `render_capacity_control_plane_manifests(...,
  execution_policy: ExecutionPreparationPolicyV2 | None = None,
  execution_policy_sha256: str | None = None)`.
- Adds paired render CLI arguments `--execution-policy-file` and
  `--execution-policy-sha256`.
- Adds a prepared-only service/timer that cannot execute active authority.

- [ ] **Step 1: Write manager policy render tests**

  Assert neither argument preserves the default policy-disabled Deployment;
  only one fails; both render exactly one immutable ConfigMap named
  `loom-capacity-execution-policy-<digest-prefix>`, a read-only fixed-path mount,
  and exact path/digest environment values. Assert the ConfigMap contains only
  canonical policy JSON and no bearer/TLS/database/ownership material. Any
  policy byte or digest change must change the ConfigMap name and Deployment
  template.

- [ ] **Step 2: Write prepared unit/timer package tests**

  Require `Type=oneshot`, the prepared-only CLI flags, owner-only config paths,
  `NoNewPrivileges`, `ProtectSystem=strict`, bounded timeout, non-overlapping
  timer behavior, and an `[Install]` section only on the timer. Reject
  `--validate-only`, activation artifact flags, `sbatch`, `scancel`, shell
  interpreters, writable credential paths, nonzero ceiling variables, or an
  environment-local autoscaler command.

- [ ] **Step 3: Implement the optional immutable ConfigMap render**

  Validate the supplied policy with the Task 1 loader before rendering. Use the
  full digest in an `immutable: true` ConfigMap annotation/data binding and a
  DNS-safe bounded prefix in its name. Mount only that named object. Do not add
  the volume/env when policy is disabled.

- [ ] **Step 4: Add paired CLI arguments without an apply surface**

  Load and validate before writing stdout. Preserve generic error messages and
  exit code `2`. Keep the only subcommands `render`, `render-executor`, and
  `status`; the new arguments modify render output but perform no external
  action.

- [ ] **Step 5: Add hardened prepared service/timer**

  The service calls the installed exact image/venv module once with
  `--prepared-only` and both inventory policy arguments. Its environment file
  supplies only paths, pool, and digests. The timer uses a bounded interval
  below the minimum executor lease and `Persistent=false`; a missed tick must
  not burst. Neither unit invokes a shell.

- [ ] **Step 6: Run render/package tests and commit**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/loom_cli/test_capacity_control_plane.py \
    tests/loom_cli/test_capacity_control_plane_cmd.py \
    tests/ops/test_capacity_pool_executor_package_boundary.py
  systemd-analyze verify \
    deploy/dev-fleet/loom-capacity-pool-executor-prepared.service \
    deploy/dev-fleet/loom-capacity-pool-executor-prepared.timer
  ```

  Commit:

  ```bash
  git add src/loom_cli/capacity_control_plane.py \
    src/loom_cli/capacity_control_plane_cmd.py deploy/dev-fleet \
    tests/loom_cli/test_capacity_control_plane.py \
    tests/loom_cli/test_capacity_control_plane_cmd.py \
    tests/ops/test_capacity_pool_executor_package_boundary.py
  git commit -m "feat(capacity): render zero-ceiling preparation services"
  ```

---

### Task 7: Runbook, governance, full verification, review, and PR

**Files:**

- Modify: `deploy/dev-fleet/README.md`
- Modify: `docs/runbooks/executable-global-capacity-bridge-rehearsal.md`
- Modify: `docs/architecture/executable-global-capacity-bridge-design.md`
- Modify: only additional files required by evidence-backed review findings

**Interfaces:**

- Documents the exact shadow-deploy, legacy-freeze input, policy render,
  prepare, executor registration/inventory, readiness, abort, and stop sequence.
- Produces a normal PR with exact-head local/CI evidence and no live mutation.

- [ ] **Step 1: Update the operational sequence and stop conditions**

  Document that the live pre-window audit remains unchanged: no `loom-dev`
  deployment exists and environment-local writers remain authoritative. Give
  exact render/status commands, owner/mode requirements, evidence paths, and
  expected canonical readiness JSON. State that preparation and abort are DB
  mutations requiring the #906 window, even though both retain ceiling zero.
  Do not provide an activation command because none exists.

- [ ] **Step 2: Run formatting, lint, and strict typing**

  Run:

  ```bash
  mapfile -t changed_python_files < <(
    git diff --name-only --diff-filter=ACMR origin/dev -- '*.py'
  )
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff format --check \
    "${changed_python_files[@]}"
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom_capacity_manager src/loom_capacity_executor \
    src/loom_capacity_pool_executor src/loom_cli tests
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom_capacity_manager src/loom_capacity_executor \
    src/loom_capacity_pool_executor src/loom_cli
  ```

- [ ] **Step 3: Run the complete focused capacity suite**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_capacity_*.py \
    tests/integration/test_capacity_*.py \
    tests/ops/test_capacity_*.py \
    tests/ops/test_global_fleet_pool_executor_once.py \
    tests/loom_cli/test_capacity_control_plane.py \
    tests/loom_cli/test_capacity_control_plane_cmd.py
  ```

  Run the migration suite against its disposable test PostgreSQL exactly as
  configured by the existing CI workflow. Do not point it at a live database.

- [ ] **Step 4: Run authoritative repository/security gates from final-head CI**

  Read `.github/workflows/ci.yml` and run the current changed-file planner,
  repository hygiene, mutation inventory, package boundary, secret scan, and
  source-tree checks. At minimum verify:

  ```bash
  test ! -e docs/superpowers
  ! git grep -n 'executable_new_capacity_ceiling[" =:]*[1-9]' -- \
    deploy/dev-fleet docs/architecture/global-capacity-zero-ceiling-preparation-design.md
  ! git grep -n 'loom-dev-shared' -- deploy config
  git diff --check origin/dev...HEAD
  git status --short --branch
  ```

- [ ] **Step 5: Iteratively self-review until no concrete finding remains**

  Review policy file races/digests, auth bindings, body bounds, idempotency,
  transaction locks, prepared abort, writer replacement, readiness freshness,
  inventory classification, journal interruption/replay, systemd sandboxing,
  rendered secret boundaries, nonzero ceilings, scheduler mutation reachability,
  foreign-work handling, and error redaction. For every finding, first add a
  failing regression test, then fix and rerun focused plus affected broad suites.

- [ ] **Step 6: Rebase on current `origin/dev` and repeat final verification**

  Run:

  ```bash
  git fetch origin dev
  git rebase origin/dev
  git diff --check origin/dev...HEAD
  git status --short --branch
  ```

  Rerun Steps 2-4 at the rebased exact head.

- [ ] **Step 7: Request independent review and resolve every evidence-backed finding**

  Use `superpowers:requesting-code-review`. Review the complete diff against
  the design, not only the last commit. Apply the receiving-review workflow for
  any finding. Repeat review after fixes until critical, important, and minor
  findings are empty.

- [ ] **Step 8: Push, open a normal PR, and require exact-head CI**

  Push `feat/capacity-cutover-evidence`, open a PR to `dev`, link #906, and state
  explicitly that no live state changed and no activation surface exists.
  Require current-head repository, images, cluster-smoke, and staging-smoke
  gates. If the head changes, disregard stale green runs and wait for the new
  exact head.

- [ ] **Step 9: Merge only after approval, prove squash-tree identity, and clean up**

  Use `superpowers:verification-before-completion` and
  `superpowers:finishing-a-development-branch`. After merge, fetch, verify the
  squash tree equals the approved PR-head tree, confirm `origin/dev` contains
  it, delete only this merged local/remote feature branch and worktree, and
  update #906 with exact commits, runs, remaining blockers, and the unchanged
  live-activation boundary.
