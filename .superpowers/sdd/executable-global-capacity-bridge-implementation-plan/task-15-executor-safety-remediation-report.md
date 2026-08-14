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

## Fix round 1 — executor mutation/recovery review remediation

Review findings addressed:

1. Final pending cancellation was derived from a live observation instead of the protected physical binding and signed launch shape. The proof digest field was carried but not enforced.
2. Submission recovery adopted live evidence without also considering accounting, and adopted terminal evidence while ignoring conflicting live rows.
3. Pending-cancel recovery ignored same-job live conflicts before classifying an older accounting row as already terminal.
4. Terminal accounting evidence did not include partition, so partition drift could not be rejected.

Root cause:

- `_exact_matches()` ignored pending node constraints and did not anchor close-time cancellation to the durable `PhysicalJobBindingV2.slurm_job_id`.
- `_cancel_request_from_job()` copied job ID/resources/nodes from whichever live observation matched loosely, rather than building the expected cancel request from the signed launch request plus protected physical binding.
- `_recover_pending_cancel()` filtered live rows down to exact matches before checking for same-job conflicts.
- `recover()` branched on live matches before querying accounting and did not treat same-token live/terminal contradictions as ambiguous.
- `SlurmTerminalEvidenceV2` and the sacct parser omitted partition.

RED:

- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rejects_proof_digest_mismatch_before_scancel tests/unit/test_capacity_executor_slurm_backend.py::test_accounting_high_water_returns_only_exact_terminal_evidence -q` → `2 failed`: digest mismatch did not raise before `scancel`; partition-aware sacct output was rejected as a malformed field count.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_unbound_duplicate_pending_job_after_drain tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_bound_pending_job_with_node_mismatch tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_quarantines_exact_live_and_terminal_conflict tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_quarantines_changed_live_with_exact_terminal tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_rejects_terminal_partition_mismatch tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_recovery_quarantines_same_job_live_terminal_conflict -q` → `6 failed`: duplicate/mismatched pending jobs were cancelled or closed, live/terminal submission conflicts were adopted, partition-mismatched terminal evidence was adopted, and same-job cancel replay conflict was classified `pending-cancelled`.

Implementation:

- `cancel_pending()` now decodes the scheduler-safe ownership token and requires it to equal `ownership_evidence_sha256` before authority validation or mutation.
- `SlurmTerminalEvidenceV2` now carries `partition`; `sacct` queries request `Partition`, parser validates it against authority, and terminal matching includes it.
- Close-time pending cancellation now loads the durable protected physical binding, builds cancel requests from signed launch shape plus `PhysicalJobBindingV2.slurm_job_id`, and quarantines unbound duplicates or bound same-job field/node drift without `scancel`.
- `_exact_matches()` now requires nodes for pending and non-pending observations.
- Submission recovery now queries live inventory and accounting together before adoption; exact-live plus terminal, same-token changed live, terminal partition/resource drift, duplicates, and insufficient terminal evidence remain quarantined/charged.
- Pending-cancel recovery now checks same-job live conflicts before terminal classification and quarantines live/terminal contradictions without `scancel`.

GREEN/regression:

- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rejects_proof_digest_mismatch_before_scancel tests/unit/test_capacity_executor_slurm_backend.py::test_accounting_high_water_returns_only_exact_terminal_evidence -q` → `2 passed in 0.21s`.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_unbound_duplicate_pending_job_after_drain tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_bound_pending_job_with_node_mismatch tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_quarantines_exact_live_and_terminal_conflict tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_quarantines_changed_live_with_exact_terminal tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_rejects_terminal_partition_mismatch tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_recovery_quarantines_same_job_live_terminal_conflict -q` → `6 passed in 0.16s`.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py::test_cancel_pending_rejects_proof_digest_mismatch_before_scancel tests/unit/test_capacity_executor_slurm_backend.py::test_accounting_high_water_returns_only_exact_terminal_evidence tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_unbound_duplicate_pending_job_after_drain tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_bound_pending_job_with_node_mismatch tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_quarantines_exact_live_and_terminal_conflict tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_quarantines_changed_live_with_exact_terminal tests/unit/test_capacity_executor_recovery.py::test_unknown_submission_recovery_rejects_terminal_partition_mismatch tests/unit/test_capacity_executor_recovery.py::test_pending_cancel_recovery_quarantines_same_job_live_terminal_conflict -q` → `8 passed in 0.32s`.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py -q` → `126 passed in 6.69s`.
- `uv run --no-sync pytest tests/integration/test_capacity_agent_executable_admission.py -q` → `33 passed in 10.30s`.
- `uv run --no-sync pytest tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_agent_migrations.py tests/integration/test_capacity_management_migrations.py -q` → `43 passed in 19.17s`.
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q` → `4 passed in 38.00s`.
- `uv run --no-sync pytest tests/ops/test_global_fleet_pool_executor_once.py tests/ops/test_global_fleet_capacity_shadow_once.py tests/ops/test_capacity_mutation_inventory.py tests/ops/test_capacity_guard_package_boundary.py -q` → `34 passed in 0.82s`.

Static verification:

- `uv run --no-sync ruff format --check src/loom_capacity_executor/executable.py src/loom_capacity_executor/slurm_backend.py src/loom_capacity_executor/slurm_contracts.py tests/support/fake_slurm.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_slurm_backend.py` → `7 files already formatted`.
- `uv run --no-sync ruff check src/loom_capacity_executor/executable.py src/loom_capacity_executor/slurm_backend.py src/loom_capacity_executor/slurm_contracts.py tests/support/fake_slurm.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_slurm_backend.py` → `All checks passed!`.
- `uv run --no-sync mypy` → `Success: no issues found in 805 source files`.
- `git diff --check` → passed.

Concerns:

- Full-repository Ruff format still has unrelated baseline drift; this round keeps Ruff formatting/checking scoped to the seven amended files while retaining full MyPy and task-required test gates.
- Physical-bind retrieval is intentionally fail-closed from the durable current intent record available at close entry; the protected observation contract still does not expose physical job ID after later intent events.

## Fix round 2 — durable physical-bind retrieval after later intent events

Review finding addressed:

- The fix-round-1 concern was a correctness gap: once `protected-withdraw-confirmed` or `protected-drain-confirmed` overwrote the latest generic intent record, `_physical_binding()` could no longer recover the earlier `physical-bind-confirmed`. A repeated close after withdrawal could quarantine a conclusively owned pending job forever, and a repeated close after drain could report quarantine instead of stable draining for a running worker.

Root cause:

- The physical binding receipt was only written under the shared `object_kind='intent'` / intent-ID key. Later protected intent events are valid latest intent state but are not physical binding state.

RED:

- `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_withdraw_confirmed_recovers_physical_binding tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_drain_confirmed_recovers_running_physical_binding -q` → `2 failed`: both cases returned `quarantined` instead of `pending-cancelled` / `draining`.

Implementation:

- Kept existing generic intent `physical-bind-*` records for compatibility with current recovery tests and old journal state before later intent events.
- Added a distinct confirmed physical-bind journal record under `object_kind='executor'` and object ID `physical-bind:<intent_id>`.
- `_physical_binding()` now prefers the distinct confirmed key and falls back to legacy latest-intent `physical-bind-confirmed` when available.
- `_bind_physical()` validates the distinct record against the signed envelope, physical binding digest, protected job ID, and ownership evidence digest; if a crash leaves the distinct confirmed key ahead of a legacy intent requested record, recovery can finish the legacy confirmation without re-mutating protected admission.
- Test fake Slurm now allows idempotent withdrawal replay (`>= 1` protected withdrawal request) before pending cancellation.

GREEN/regression:

- `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_withdraw_confirmed_recovers_physical_binding tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_drain_confirmed_recovers_running_physical_binding -q` → `2 passed in 0.40s`.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_withdraw_confirmed_recovers_physical_binding tests/unit/test_capacity_executor_recovery.py::test_repeated_close_after_drain_confirmed_recovers_running_physical_binding tests/unit/test_capacity_executor_recovery.py::test_crash_after_pending_cancel_request_retries_exact_cancel_before_work_fetch tests/unit/test_capacity_executor_executable.py::test_close_withdraws_bound_unregistered_pending_job_before_cancel tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_unbound_duplicate_pending_job_after_drain tests/unit/test_capacity_executor_executable.py::test_close_never_cancels_bound_pending_job_with_node_mismatch -q` → `6 passed in 1.08s`.
- `uv run --no-sync pytest tests/unit/test_capacity_executor_slurm_backend.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_client.py tests/unit/test_capacity_executor_admission_client.py -q` → `128 passed in 8.49s`.
- `uv run --no-sync pytest tests/integration/test_executable_global_capacity_bridge.py -q` → `4 passed in 51.11s`.

Static verification:

- `uv run --no-sync ruff format --check src/loom_capacity_executor/executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_executable.py` → `3 files already formatted`.
- `uv run --no-sync ruff check src/loom_capacity_executor/executable.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_executable.py` → `All checks passed!`.
- `uv run --no-sync mypy` → `Success: no issues found in 805 source files`.
- `git diff --check` → passed.

Concerns:

- Full-repository Ruff format still has unrelated baseline drift, so this follow-up keeps Ruff scoped to amended files while retaining full MyPy, affected unit, bridge, and whitespace gates.
