# Task 15D final whole-branch remediation report

Date: 2026-08-14

## Scope and constraints

Addressed the four final-review Minor findings only. No activation route, live
Kubernetes/Slurm/systemd/database mutation, remote operation, positive executable
ceiling, user-pool weighting, QoS/profile expansion, or live authority expansion
was added. `DryRun*V1` remains permanently non-executable and checked-in
rendered executable ceiling remains zero.

## Root cause summary

1. Executable intent `observed_state` had wire-contract validation in typed
   payloads but no ORM/database parity. Already-upgraded capacity databases also
   needed a forward migration instead of an in-place historical edit only.
2. Pending-cancel recovery was implemented fail-closed, but direct tests did not
   cover each replay branch: running drain-only, exact terminal, no-evidence
   ambiguity, and post-`scancel` response loss.
3. The executor boundary protocols still used broad `Any`, so strict MyPy could
   not catch concrete client/backend interface drift at the manager, protected
   admission, and Slurm boundaries.
4. Reconciliation failure-persistence hard failures were indirectly covered as
   generic runtime errors, not directly as `ReconciliationFailurePersistenceError`
   with rollback/no-false-transition assertions.

## Per-item RED/GREEN evidence

### 1. Executable intent observed-state database invariant

Production break named before test: a raw writer can persist
`observed_state='impossible'` after an already-`capacity_0007` database upgrades
to head.

RED command:

```text
uv run pytest tests/integration/test_capacity_management_migrations.py::test_existing_executable_intents_upgrade_to_observed_state_database_check -q
```

RED output:

```text
FAILED ... Failed: DID NOT RAISE <class 'sqlalchemy.exc.IntegrityError'>
```

GREEN fix:

- Added ORM check `capacity_executable_intent_observed_state_check`.
- Added forward migration `capacity_0008_executable_intent_observed_state_check.py`.
- Updated packaged capacity migration head expectations from `capacity_0007` to
  `capacity_0008`.

GREEN command/output:

```text
uv run pytest tests/integration/test_capacity_management_migrations.py::test_existing_executable_intents_upgrade_to_observed_state_database_check -q
.                                                                        [100%]
1 passed in 12.32s
```

### 2. Pending-cancel crash/replay matrix

Production breaks named before tests:

- running replay must remain drain-only and never signal active work;
- exact terminal evidence must resolve idempotently without a second `scancel`;
- no live/terminal evidence must quarantine instead of inferring success;
- post-`scancel` response loss must recover from exact terminal evidence without
  issuing another scheduler cancel.

Initial RED command exposed a test-fixture construction error (Slurm contracts
are not executable-contract `StrictV2Model`s):

```text
uv run pytest tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_of_running_job_is_drain_only tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_accepts_already_terminal_evidence_without_scancel tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_without_live_or_terminal_evidence_quarantines tests/unit/test_capacity_executor_recovery.py::test_post_scancel_response_loss_recovers_idempotently_from_terminal_evidence -q
```

Initial output:

```text
FAILED ... ValueError: canonical executable encoding requires a schema-v2 model
```

After correcting the helper to use the real Slurm journal JSON format, the tests
passed without production changes, confirming this item was direct branch
coverage over already-implemented fail-closed behavior.

GREEN command/output:

```text
uv run pytest tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_of_running_job_is_drain_only tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_accepts_already_terminal_evidence_without_scancel tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_without_live_or_terminal_evidence_quarantines tests/unit/test_capacity_executor_recovery.py::test_post_scancel_response_loss_recovers_idempotently_from_terminal_evidence -q
....                                                                     [100%]
4 passed in 0.63s
```

### 3. Exact executor boundary protocol typing

Production break named before change: a concrete executable manager client,
protected-admission client, or Slurm backend can drift from the executor’s
expected protocol while the protocol still says `Any`.

Change:

- Reused existing receipt/work contracts from
  `loom_capacity_executor.client`.
- Reused existing admission result contracts from
  `loom_capacity_agent.admission`.
- Reused existing Slurm request/result contracts from
  `loom_capacity_executor.slurm_contracts`.
- Added `TYPE_CHECKING` conformance assignments for
  `ExecutableCapacityExecutorClient`, `DatabaseExecutableAdmissionClient`, and
  `AsyncSlurmBackend`.
- Narrowed internal checkpoint/work helper signatures to the same exact types.

Static GREEN command/output:

```text
uv run mypy src/loom_capacity_executor/executable.py src/loom_capacity_executor/client.py src/loom_capacity_executor/admission_client.py src/loom_capacity_executor/slurm_backend.py
Success: no issues found in 4 source files
```

Full strict MyPy output:

```text
uv run mypy
Success: no issues found in 810 source files
```

### 4. Reconciliation failure-persistence regressions

Production break named before test: the active hard-failure path can be observed
only as a generic `RuntimeError`, and rollback might leave a false executable or
failed transition behind.

Change:

- Strengthened the existing hard-failure test to assert
  `ReconciliationFailurePersistenceError` directly.
- Added post-failure assertions that no `CapacityAllocationEpoch` status was
  persisted and `increase_freeze` was not falsely committed.

Focused command/output:

```text
uv run pytest tests/integration/test_capacity_manager_api.py::test_allocation_commit_and_failure_recorder_errors_propagate_hard_failure -q
.                                                                        [100%]
1 passed, 1 warning in 14.10s
```

Warning: existing Starlette/httpx deprecation warning from `fastapi.testclient`.

## Combined focused regressions

```text
uv run pytest tests/integration/test_capacity_management_migrations.py::test_existing_executable_intents_upgrade_to_observed_state_database_check tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_of_running_job_is_drain_only tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_accepts_already_terminal_evidence_without_scancel tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_replay_without_live_or_terminal_evidence_quarantines tests/unit/test_capacity_executor_recovery.py::test_post_scancel_response_loss_recovers_idempotently_from_terminal_evidence tests/integration/test_capacity_manager_api.py::test_allocation_commit_and_failure_recorder_errors_propagate_hard_failure -q
......                                                                   [100%]
6 passed, 1 warning in 13.58s
```

## Affected broad gates

```text
uv run pytest tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_executable.py -q
....................................................                     [100%]
52 passed in 4.05s
```

```text
uv run pytest tests/integration/test_capacity_management_migrations.py -q
................                                                         [100%]
16 passed in 12.04s
```

```text
uv run pytest tests/loom_cli/test_capacity_control_plane.py::test_renderer_emits_one_inert_control_plane_in_dependency_order tests/loom_cli/test_capacity_control_plane.py::test_migration_job_name_binds_the_complete_immutable_spec tests/loom_cli/test_capacity_control_plane_packaging.py -q
.....                                                                    [100%]
5 passed in 2.00s
```

```text
uv run ruff check src/loom_capacity_executor/executable.py src/loom_capacity_manager/models.py capacity_migrations/versions/capacity_0008_executable_intent_observed_state_check.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_management_migrations.py tests/integration/test_capacity_manager_api.py tests/loom_cli/test_capacity_control_plane.py tests/loom_cli/test_capacity_control_plane_packaging.py
All checks passed!
```

```text
uv run mypy
Success: no issues found in 810 source files
```

## Diff/docs hygiene

- No live-operation code path, activation API/CLI, remotes, or external system
  mutation was added.
- New migration is forward-only from `capacity_0007` to `capacity_0008`; existing
  capacity migration packaging/head tests were updated accordingly.
- Checked-in control-plane manifest expectations remain inert and still bind the
  exact migration Job name/spec.
- No unrelated formatting churn beyond Ruff import sorting in the touched
  executor file.

## Files changed

- `capacity_migrations/versions/capacity_0008_executable_intent_observed_state_check.py`
- `src/loom_capacity_executor/executable.py`
- `src/loom_capacity_manager/models.py`
- `tests/integration/test_capacity_management_migrations.py`
- `tests/integration/test_capacity_manager_api.py`
- `tests/loom_cli/test_capacity_control_plane.py`
- `tests/loom_cli/test_capacity_control_plane_packaging.py`
- `tests/unit/test_capacity_executor_recovery.py`
- `.superpowers/sdd/executable-global-capacity-bridge-implementation-plan/progress.md`
- `.superpowers/sdd/executable-global-capacity-bridge-implementation-plan/task-15-final-remediation-report.md`

## Self-review

- Verified the new database check is present in ORM and forward migration, and
  that valid/NULL existing rows survive upgrade while invalid raw writes fail.
- Verified pending-cancel recovery branch tests assert real journal events and
  scheduler side effects, not mock behavior.
- Verified protocol typing reuses existing wire/result classes and does not
  duplicate contracts.
- Verified reconciliation hard-failure test asserts the exact error type and no
  false persisted transition.
- Verified no live activation or operational mutation authority was introduced.
