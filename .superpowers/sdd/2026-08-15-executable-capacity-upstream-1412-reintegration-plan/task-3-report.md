Status: DONE_WITH_CONCERNS

Base: b0943956f2d6f0c26a3f35b2aef535f0495723ea
Head: e6ca394ed

Changed files:
- src/loom_capacity_manager/api.py
- src/loom_capacity_manager/executable_contracts.py
- src/loom_capacity_manager/models.py
- src/loom_capacity_manager/reconciler.py
- tests/integration/conftest.py
- tests/integration/test_capacity_manager_api.py
- tests/integration/test_capacity_migration_reintegration.py
- tests/unit/test_capacity_auth.py
- tests/unit/test_capacity_manager_executable_contracts.py
- .superpowers/sdd/2026-08-15-executable-capacity-upstream-1412-reintegration-plan/task-3-report.md

RED commands and failure reasons:
1. Command:
   `uv run --no-sync pytest -q tests/unit/test_capacity_auth.py tests/unit/test_capacity_manager_executable_contracts.py tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_migration_reintegration.py`
   Output:
   `2 failed, 91 passed, 1 warning in 19.58s`
   Failure reasons:
   - `tests/integration/test_capacity_manager_api.py::test_v2_executor_work_route_is_exactly_pool_bound`
     failed because `/v2/executors/{pool_id}/context` called
     `CapacityExecutionStore.executor_current_context`, which does not exist
     (`AttributeError` at `src/loom_capacity_manager/api.py:1144`).
   - `tests/integration/test_capacity_manager_api.py::test_submission_recovery_response_is_explicitly_quarantined`
     failed because the recovery response omitted the required
     `"state": "quarantined"` field.

Implementation summary:
- Restored Task 3 RED coverage for exact V2 queue/auth/API boundaries:
  acceptance `pool_id` canonicality, submission recovery absence evidence,
  legacy executor generation rejection, cross-pool/body/path/generation
  mismatch rejection, and migration ORM parity assertions for
  `input_valid_until` plus executable protected-release receipts.
- Fixed `/v2/executors/{pool_id}/context` to read the current execution
  context from the management store and fail closed with HTTP 409 when the
  manager is still in shadow mode.
- Fixed `/v2/executors/{pool_id}/permits/{permit_id}/recover` to return an
  explicit recovery state of `quarantined` without changing execution-store
  state-transition ownership.
- Kept the executable ceiling/read-only status surface unchanged and did not
  add activation routes or per-intent protected-release ORM fields.
- Ran `ruff format` on the brief-scoped files required to satisfy the static
  gate; resulting non-test source diffs outside `api.py` are formatting-only.

GREEN/static commands and output:
1. `uv run --no-sync pytest -q tests/unit/test_capacity_auth.py tests/unit/test_capacity_manager_executable_contracts.py tests/integration/test_capacity_manager_api.py tests/integration/test_capacity_migration_reintegration.py`
   Output:
   `93 passed, 1 warning in 13.21s`
2. `uv run --no-sync ruff format --check src/loom_capacity_manager/auth.py src/loom_capacity_manager/executable_contracts.py src/loom_capacity_manager/models.py src/loom_capacity_manager/api.py src/loom_capacity_manager/ownership.py src/loom_capacity_manager/reconciler.py tests/integration/conftest.py tests/integration/test_capacity_manager_api.py tests/unit/test_capacity_auth.py tests/unit/test_capacity_manager_executable_contracts.py tests/integration/test_capacity_migration_reintegration.py`
   Output:
   `11 files already formatted`
3. `uv run --no-sync ruff check src/loom_capacity_manager/auth.py src/loom_capacity_manager/executable_contracts.py src/loom_capacity_manager/models.py src/loom_capacity_manager/api.py src/loom_capacity_manager/ownership.py src/loom_capacity_manager/reconciler.py tests/integration/conftest.py tests/integration/test_capacity_manager_api.py tests/unit/test_capacity_auth.py tests/unit/test_capacity_manager_executable_contracts.py tests/integration/test_capacity_migration_reintegration.py`
   Output:
   `All checks passed!`
4. `uv run --no-sync mypy src/loom_capacity_manager/auth.py src/loom_capacity_manager/executable_contracts.py src/loom_capacity_manager/models.py src/loom_capacity_manager/api.py src/loom_capacity_manager/ownership.py src/loom_capacity_manager/reconciler.py`
   Output:
   `Success: no issues found in 6 source files`
5. `git diff --check`
   Output:
   `<no output>`

Commit SHA:
- e6ca394ed

Self-review:
- Confirmed the RED cycle caught missing production behavior before the API fix.
- Limited behavior changes to the Task 3 API boundary and report/tests; did not
  broaden into execution-store concurrency or quarantine-transition ownership.
- Re-verified the exact requested pytest + static gate after formatting.

Concerns:
- The required pytest run still emits the pre-existing
  `StarletteDeprecationWarning` from `fastapi.testclient` importing `httpx`;
  this warning is non-blocking and not introduced by Task 3.

Confirmation:
- No live Kubernetes, Slurm, systemd, deployment, activation, or production
  database action occurred. All checks ran only against local code and test
  databases.
