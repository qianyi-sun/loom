# Task 15B — Executor mutation and recovery safety remediation report

## Scope

Implemented the Task 15B executor safety remediation while preserving the fixed boundaries from the brief:

- no daemon/runtime/heartbeat/CSPRNG handoff assembly;
- no live Kubernetes, Slurm, systemd, activation-state, or `origin/dev` mutation;
- no checked-in/rendered positive executable ceiling changes;
- DryRun V1 remains non-executable;
- no `docs/superpowers` artifact was created;
- manager/pool/subject/candidate/deployment/profile/resource/node/ownership fences were preserved.

Commit inventory:

- `ae5ce24a8` (`fix: harden executable executor mutation recovery`) — implementation, migration, and tests.
- Report artifact committed separately after this file was authored.

## Root cause summary

1. Pending-cancel was still too narrow: the final `scancel` path rechecked scheduler authority and basic association, but did not carry and compare the complete scheduler shape, node set, ownership token/proof digest, and resource identity needed to detect job-ID reuse or scheduler-field drift.
2. A bound physical job with no registered worker had no protected admission transition that revoked bootstrap and advanced the late-registration fence before executor cancellation, so safe pending cancellation could not be distinguished from killing a job that might still register.
3. Local protected-drain and cancellation journal requests could remain permanently unresolved after a crash between durable request and confirmation.
4. `submitting-unknown` recovery considered live inventory but did not adopt a conclusively exact terminal accounting record once accounting had observed through the signed submitted time.
5. Manager HTTP response handling checked body size after materializing the body, so an oversized response could be buffered before rejection.
6. Protected admission database operations had connect timeouts but not end-to-end pool-acquire, database lock/statement, and outer operation deadlines.

## Implementation summary

- Extended `SlurmCancelRequestV2` with partition, CPU, memory, GPU, generic TRES, node set, ownership token, and ownership evidence digest. `AsyncSlurmBackend.cancel_pending()` now reobserves the exact job immediately before `scancel` and refuses every mismatch, unknown/missing/duplicate observation, or non-`PENDING` state without mutation.
- Added protected unregistered-worker withdrawal contracts and `ExecutableAdmissionStore.withdraw_unregistered_worker()`, backed by `guard_0016_executable_unregistered_withdrawal.py`. The migration adds a `withdrawn` admission event, installs `withdraw_unregistered_executable_worker`, fences delayed registration after withdrawal, requires exact physical binding and zero protected claims, and grants the executor role only the new bounded function.
- Updated executable close flow so an exact owned pending job with no registered worker obtains the protected withdrawal receipt before journaling and attempting conditional pending cancellation. Registered/running workers remain drain-first.
- Added local replay before work fetch in `ExecutablePoolExecutor.tick()` and `recover()` for `protected-drain-requested`, `protected-withdraw-requested`, and `pending-cancel-requested` journal records.
- Added terminal-accounting adoption during ambiguous submission recovery when accounting is observed through the signed submitted time and exactly one terminal record matches the full signed scheduler/ownership shape.
- Replaced manager receipt/work fetch body reads with streamed cumulative byte-bound enforcement before JSON parsing.
- Added pool timeout, local lock/statement timeout, and outer `asyncio.timeout()` around each protected admission transaction, preserving rollback on timeout.

## RED evidence

- Pending-cancel field reuse/mismatch:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rejects_job_reuse_or_scheduler_field_mismatch tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rejects_missing_unknown_or_duplicate_evidence_before_scancel -q`
  → failed as expected: the mismatch cases still reached the old path instead of rejecting before `scancel`.
- Bound-but-unregistered pending close:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py::test_close_withdraws_bound_unregistered_pending_job_before_cancel -q`
  → failed as expected with `quarantined`.
- Protected withdrawal migration/function:
  `uv run --no-sync pytest tests/integration/test_capacity_agent_executable_admission.py::test_withdraw_unregistered_physical_binding_revokes_bootstrap_and_fences_registration -q`
  → failed as expected with `UndefinedFunction`.
- Local drain/cancel replay:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py::test_crash_after_protected_drain_request_replays_before_work_fetch tests/unit/test_capacity_executor_recovery.py::test_crash_after_pending_cancel_request_retries_exact_cancel_before_work_fetch -q`
  → failed as expected: drain replay fell through to work fetch and cancel replay used the old cancel event.
- Terminal submission adoption:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_adopts_exact_terminal_accounting_match -q`
  → failed as expected with `quarantined`.
- Manager streaming response bounds:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_client.py -q`
  → oversized streaming tests failed before the streaming-bound implementation because the client over-read/materialized bodies.
- Protected admission transaction bounds:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_admission_client.py -q`
  → failed before implementation due missing timeout knobs and missing transaction deadline/rollback behavior.

## GREEN and regression evidence

- Slurm pending-cancel focused:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rejects_job_reuse_or_scheduler_field_mismatch tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rejects_missing_unknown_or_duplicate_evidence_before_scancel tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rechecks_exact_state_and_association tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_uses_scheduler_predicates_after_exact_reobservation tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_treats_unexpected_success_output_as_uncertain -q`
  → `15 passed`.
- Close/withdrawal focused:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py::test_close_withdraws_bound_unregistered_pending_job_before_cancel tests/unit/test_capacity_executor_executable.py::test_protected_drain_precedes_conditional_pending_cancel tests/unit/test_capacity_executor_executable.py::test_ordinary_reclamation_never_signals_active_worker -q`
  → `3 passed`.
- Protected withdrawal integration:
  `uv run --no-sync pytest tests/integration/test_capacity_agent_executable_admission.py::test_withdraw_unregistered_physical_binding_revokes_bootstrap_and_fences_registration -q`
  → `1 passed`.
- Local replay focused:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py::test_crash_after_protected_drain_request_replays_before_work_fetch tests/unit/test_capacity_executor_recovery.py::test_crash_after_pending_cancel_request_retries_exact_cancel_before_work_fetch -q`
  → `2 passed`.
- Terminal recovery focused:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_adopts_exact_terminal_accounting_match tests/unit/test_capacity_executor_recovery.py::test_ambiguous_recovery_stays_quarantined_and_charged -q`
  → `5 passed`.
- Manager client streaming:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_client.py -q`
  → `5 passed`.
- Protected admission client bounds:
  `uv run --no-sync pytest tests/unit/test_capacity_executor_admission_client.py -q`
  → `11 passed`.

Final verification after formatting/static fixes:

- `uv run --no-sync ruff format --check <16 touched Python/migration files>` → `16 files already formatted`.
- `uv run --no-sync ruff check <16 touched Python/migration files>` → `All checks passed!`.
- `uv run --no-sync mypy` → `Success: no issues found in 805 source files`.
- `git diff --check` → passed.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py -q` → `119 passed in 6.23s`.
- `uv run --no-sync pytest tests/integration/test_capacity_agent_executable_admission.py -q` → `33 passed in 26.35s`.
- `uv run --no-sync pytest tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_agent_migrations.py tests/integration/test_capacity_management_migrations.py -q` → `43 passed in 32.66s`.
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q` → `4 passed in 38.40s`.
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q` → `34 passed in 0.92s`.

## Static-check note

An attempted full-repository `ruff format --check src tests capacity_guard_migrations` stopped on pre-existing unrelated format drift across 730 files. I did not reformat the repository. Task-scoped Ruff format/check was rerun on every touched Python/migration file and passed; full MyPy and `git diff --check` also passed.

## Concerns

- First fresh focused-unit rerun hit one transient fake Slurm authority-validation timeout; the isolated parametrized test immediately passed (`7 passed`) and the full focused unit suite then passed (`119 passed`).
- Full-repository Ruff format remains noisy from unrelated baseline drift, so formatting verification is scoped to the Task 15B touched files.
