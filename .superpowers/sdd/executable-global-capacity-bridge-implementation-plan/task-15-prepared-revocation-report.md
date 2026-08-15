# Task 15 Phase A prepared revocation report

## Status

DONE_WITH_CONCERNS

Phase A implementation is committed as:

- `793dbc57c90af368f398f59e677a848fda28b14c` — protected prepared-bootstrap revocation primitive

Concern: this Phase intentionally does not implement trusted V2 reporter publication or final manager retirement; those are explicitly reserved for the next task.

## Root cause and design

The original Task 15 review finding was that an executor could close an accepted/post-prepare intent before any physical Slurm job or worker existed without revoking the protected prepared bootstrap. That left an append-only prepared capability unable to produce protected-release evidence and could block execution-epoch retirement.

The prior in-progress migration also attempted to rewrite `observe_executable_intent()` via brittle string replacement of PostgreSQL-normalized `pg_get_functiondef()` output. That caused setup failures and made downgrade fidelity fragile.

Implemented design:

- Added schema-v2 contracts:
  - `ExecutablePreparedBootstrapRevocationV2`
  - `RevokedExecutableBootstrapV2`
- Added protected admission/store/client method:
  - `revoke_prepared_bootstrap()`
  - database routine `revoke_prepared_executable_bootstrap(...)`
- Added append-only guard evidence kind:
  - `prepared-revoked`
- Revocation requires exact prepared binding, no physical binding, no worker registration, no claims, claim high-water `0`, and protected epoch greater than bootstrap epoch.
- Revocation sets protected claim state to draining and fences later physical binding and worker registration.
- Recreated `observe_executable_intent()` explicitly in guard_0017 with security definer, fixed `search_path`, owner/grant expectations, and the new `prepared_revocation` field.
- Recreated guard_0016 observation exactly on downgrade, including observer role grant and no `prepared_revocation` field.
- Added local handoff revocation:
  - removes only exact unconsumed handoff records;
  - fails closed on `.used`, `.credential`, `.ownership`, `.launched`, or symlink sidecar evidence;
  - is idempotent if the clear handoff record is already gone.
- Added executor close/recovery path:
  - journals exact protected revocation request;
  - replays lost protected response before fetching more central work;
  - records exact protected confirmation;
  - deletes clear handoff only after protected confirmation;
  - records `prepared-handoff-deleted` for crash-safe cleanup;
  - orders central close after revocation/cleanup;
  - never submits Slurm for prepared-only close.

## Changed Phase A files

- `capacity_guard_migrations/versions/guard_0017_prepared_bootstrap_revocation.py`
- `src/loom_capacity_agent/admission.py`
- `src/loom_capacity_agent/executable_admission.py`
- `src/loom_capacity_executor/admission_client.py`
- `src/loom_capacity_executor/bootstrap_handoff.py`
- `src/loom_capacity_executor/executable.py`
- `src/loom_capacity_executor/journal.py`
- `tests/integration/test_capacity_agent_executable_admission.py`
- `tests/integration/test_capacity_agent_migrations.py`
- `tests/integration/test_capacity_guard_migrations.py`
- `tests/integration/test_capacity_submission_store.py`
- `tests/unit/test_capacity_executor_admission_client.py`
- `tests/unit/test_capacity_executor_bootstrap_handoff.py`
- `tests/unit/test_capacity_executor_executable.py`
- `tests/unit/test_capacity_executor_recovery.py`

Unrelated retained-runtime/capacity_0004/model/ops files were left unstaged.

## RED evidence

- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_crash_after_prepared_revocation_replays_before_work_fetch`
  - Failed as expected: recovery fetched central work before replaying durable `protected-prepared-revocation-requested`.
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_confirmed_prepared_revocation_deletes_handoff_before_work_fetch`
  - Failed as expected: confirmed protected revocation did not delete the clear handoff before work fetch.
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_prepared_handoff_deletion_replay_is_idempotent`
  - Failed as expected: idempotent deletion replay was not resolved before work fetch.
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_prepared_handoff_deleted_replays_central_close`
  - Failed as expected after correcting the test to use the executor operation namespace: repeated close rejected `prepared-handoff-deleted` as journal drift.
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py::test_guard_0017_downgrades_to_0016_and_reupgrades_observation_faithfully`
  - Failed as expected: downgrade still exposed `prepared_revocation` in `observe_executable_intent()` instead of restoring guard_0016 behavior.

## GREEN evidence

Initial smoke of prior repairs:

- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py::test_prepared_bootstrap_revocation_fences_physical_binding_and_registration`
  - `1 passed in 16.14s`
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py::test_close_revokes_prepared_bootstrap_without_a_physical_job`
  - `1 passed in 0.20s`

Narrow TDD greens:

- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_crash_after_prepared_revocation_replays_before_work_fetch`
  - `1 passed in 0.15s`
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_confirmed_prepared_revocation_deletes_handoff_before_work_fetch tests/unit/test_capacity_executor_recovery.py::test_prepared_handoff_deletion_replay_is_idempotent`
  - `2 passed in 0.17s`
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_prepared_handoff_deleted_replays_central_close`
  - `1 passed in 0.33s`
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py::test_crash_after_prepared_revocation_replays_before_work_fetch tests/unit/test_capacity_executor_recovery.py::test_confirmed_prepared_revocation_deletes_handoff_before_work_fetch tests/unit/test_capacity_executor_recovery.py::test_prepared_handoff_deletion_replay_is_idempotent tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_prepared_handoff_deleted_replays_central_close`
  - `4 passed in 0.63s`
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_bootstrap_handoff.py::test_revoke_prepared_removes_only_the_exact_unconsumed_handoff tests/unit/test_capacity_executor_bootstrap_handoff.py::test_revoke_prepared_rejects_changed_handoff_binding tests/unit/test_capacity_executor_bootstrap_handoff.py::test_revoke_prepared_rejects_consumed_or_physical_handoff_evidence tests/unit/test_capacity_executor_bootstrap_handoff.py::test_revoke_prepared_rejects_symlink_consumed_evidence`
  - `7 passed in 0.19s`
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_admission_client.py::test_database_client_sends_exact_prepared_revocation_to_protected_transaction tests/unit/test_capacity_executor_admission_client.py::test_database_client_rejects_prepared_revocation_receipt_type_mismatch`
  - `2 passed in 0.14s`
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py::test_guard_0017_downgrades_to_0016_and_reupgrades_observation_faithfully`
  - `1 passed in 22.24s`
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py::test_guard_0017_routine_security_and_grant_inventory`
  - `1 passed in 20.92s`
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py::test_guard_0017_refuses_downgrade_with_prepared_revocation_evidence`
  - `1 passed in 20.95s`

Final focused tests:

- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py`
  - `57 passed in 6.76s`
- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_bootstrap_handoff.py tests/unit/test_capacity_executor_admission_client.py`
  - `46 passed in 3.79s`
- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py`
  - `34 passed in 31.44s`
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py::test_guard_schema_startup_returns_numeric_head tests/integration/test_capacity_guard_migrations.py::test_guard_schema_has_exact_owner_and_preserves_public_application_tables tests/integration/test_capacity_guard_migrations.py::test_guard_0014_downgrade_restores_executor_only_observation tests/integration/test_capacity_guard_migrations.py::test_guard_0017_routine_security_and_grant_inventory tests/integration/test_capacity_guard_migrations.py::test_guard_0017_downgrades_to_0016_and_reupgrades_observation_faithfully tests/integration/test_capacity_guard_migrations.py::test_guard_0017_refuses_downgrade_with_prepared_revocation_evidence`
  - `6 passed in 21.04s`
- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_migrations.py::test_executor_role_is_distinct_and_has_only_bounded_executable_procedures tests/integration/test_capacity_submission_store.py::test_atomic_submission_blocks_guard_downgrade_without_data_loss tests/integration/test_capacity_submission_store.py::test_concurrent_guard_downgrade_observes_committing_atomic_submission`
  - `3 passed in 20.12s`

## Quality gates

- `uv run --no-sync ruff format ...`
  - First run: `2 files reformatted, 13 files left unchanged`
  - Final run: `1 file left unchanged`
- `uv run --no-sync ruff check ...`
  - First run found style issues in the new migration and `__all__`; fixed.
  - Final run: `All checks passed!`
- `uv run --no-sync mypy capacity_guard_migrations/versions/guard_0017_prepared_bootstrap_revocation.py src/loom_capacity_agent/admission.py src/loom_capacity_agent/executable_admission.py src/loom_capacity_executor/admission_client.py src/loom_capacity_executor/bootstrap_handoff.py src/loom_capacity_executor/executable.py src/loom_capacity_executor/journal.py`
  - First run found one optional Alembic config typing issue; fixed.
  - Final run: `Success: no issues found in 7 source files`
- `git diff --check`
  - Passed with no output.

## Self-review

- Exact binding:
  - Request/receipt models and admission store validate subject/binding/epoch/digest invariants.
  - Executor replays exact canonical revocation bytes and rejects journal drift.
- Authority separation:
  - New guard routine is executor-only; observer can observe but cannot revoke.
  - Security definer and fixed `search_path=pg_catalog` are asserted by tests.
- Idempotency:
  - Protected revocation replays exact request.
  - Database revocation is idempotent for exact request replay.
  - Handoff deletion tolerates already-missing clear capability after protected confirmation.
- Crash safety:
  - Covered response loss after protected commit.
  - Covered restart after protected confirmation before handoff deletion.
  - Covered deletion replay and close replay after deletion before central close.
  - Central close is ordered after protected revocation/cleanup.
- Grant scope:
  - Guard tests assert executor and observer grants for observation, executor-only grant for revocation.
- Downgrade fidelity:
  - Downgrade refuses with `prepared-revoked` evidence.
  - Downgrade to guard_0016 removes revocation routine and observation field while preserving guard_0016 observer grant.
  - Re-upgrade restores guard_0017 routine and grants.

## Remaining concerns

- Trusted V2 reporter publication through the manager protected-release endpoint and final execution-epoch retirement are intentionally not implemented in Phase A per the task instructions.
- The worktree still contains unrelated unstaged retained-runtime/capacity_0004/model/ops repair files from earlier work; they were not included in the Phase A commit.
