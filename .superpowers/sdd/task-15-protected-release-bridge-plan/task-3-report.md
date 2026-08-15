# Task 3: Executor Verification for Every Protected Terminal Fence

## Status

Implemented and verified.

## Files changed

- `src/loom_capacity_agent/admission.py`
- `src/loom_capacity_executor/executable.py`
- `capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py`
- `tests/unit/test_capacity_executor_executable.py`
- `tests/unit/test_capacity_executor_recovery.py`
- `tests/integration/test_capacity_agent_executable_admission.py`

## RED

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py -k 'protected_terminal or unused_release or withdrawal_release'
```

Captured output:

```text
.FFFFFFFFFFFFF                                                           [100%]
=================================== FAILURES ===================================
____ test_withdrawal_release_digest_requires_exact_terminal_slurm_evidence _____
E       AssertionError: assert 'quarantined' == 'released'

__ test_unused_release_uses_exact_confirmed_inventory_and_prepared_revocation __
E       AssertionError: assert 'quarantined' == 'released'

FAILED tests/unit/test_capacity_executor_executable.py::test_withdrawal_release_digest_requires_exact_terminal_slurm_evidence
FAILED tests/unit/test_capacity_executor_executable.py::test_unused_release_uses_exact_confirmed_inventory_and_prepared_revocation
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[no-protected-evidence-protected terminal evidence is absent or ambiguous]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[multiple-protected-evidence-protected terminal evidence is absent or ambiguous]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[changed-protected-digest-protected terminal evidence is absent or changed]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[changed-protected-epoch-protected terminal evidence is absent or changed]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[changed-protected-binding-protected terminal evidence is absent or changed]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[changed-inventory-sequence-unused terminal inventory is absent or changed]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[changed-inventory-digest-unused terminal inventory is absent or changed]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[nonempty-owned-inventory-unused terminal inventory still owns the intent]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[retained-launch-envelope-unused terminal still has local physical ownership]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[physical-binding-unused terminal still has local physical ownership]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release[matching-owned-slurm-work-unused terminal still has local physical ownership]
13 failed, 1 passed, 29 deselected in 2.41s
```

The harness truncated the middle of the failure output, but the visible failures matched the expected missing behavior: withdrawal and unused releases quarantined, and the matrix still returned the old generic release/physical reasons.

Additional replay RED added after diff review:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py -k 'release_replay_rechecks'
```

```text
F                                                                        [100%]
E       AssertionError: assert 'released' == 'quarantined'
1 failed, 30 deselected in 0.24s
```

## GREEN

Focused terminal matrix:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py -k 'protected_terminal or unused_release or withdrawal_release'
```

```text
..............                                                           [100%]
14 passed, 29 deselected in 0.30s
```

Replay regression:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py -k 'release_replay_rechecks'
```

```text
.                                                                        [100%]
1 passed, 30 deselected in 0.20s
```

## Final gates

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_agent_executable_admission.py
```

```text
........................................................................ [ 62%]
............................................                             [100%]
116 passed in 11.99s
```

```bash
uv run --no-sync ruff format --check src/loom_capacity_agent/admission.py src/loom_capacity_executor/executable.py capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_agent_executable_admission.py
```

```text
6 files already formatted
```

```bash
uv run --no-sync ruff check src/loom_capacity_agent/admission.py src/loom_capacity_executor/executable.py capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_agent_executable_admission.py
```

```text
All checks passed!
```

```bash
uv run --no-sync mypy src/loom_capacity_agent/admission.py src/loom_capacity_executor/executable.py
```

```text
Success: no issues found in 2 source files
```

```bash
git diff --check
```

```text
<no output>
```

## Implementation notes

- Added `withdrawal` to `ProtectedIntentObservationV2`.
- Added mutual exclusion across `release`, `withdrawal`, and `prepared_revocation`.
- Recreated `observe_executable_intent()` in `guard_0018` so upgrade returns withdrawal receipts and downgrade restores the prior no-withdrawal observation shape while preserving executor/observer grants.
- Added executor terminal-fence selection requiring exactly one protected terminal receipt.
- Worker/slurm-job releases still require exact Slurm terminal evidence.
- `unused` releases require the exact latest confirmed inventory journal payload/digest/sequence, no authenticated owned inventory record for the intent, no retained launch envelope, and no retained physical binding.
- Durable central release replay now routes back through `_release()` so pending replay cannot bypass terminal verification.

## Self-review

- Protected receipt no/multiple/changed digest/changed epoch/changed binding fail closed before central release.
- Withdrawal release succeeds only with exact withdrawal digest and exact terminal Slurm evidence.
- Prepared revocation release succeeds only through `terminal_kind="unused"` and exact confirmed inventory evidence.
- Changed inventory sequence/digest, absent/incomplete inventory, owned inventory record, retained launch envelope, physical binding, and matching owned Slurm work all quarantine and do not call central release.
- Existing central replay coverage was updated with valid terminal evidence; new replay regression prevents bypassing the new release verifier.
- Existing unrelated dirty files were left unstaged and unmodified.

## Concerns

- The initial RED output was partially truncated by the tool output cap; the report includes the exact visible summary and representative failure lines.
- `_confirmed_inventory_for_release()` intentionally fails closed unless the matching confirmed inventory is the retained latest inventory journal record. The current journal API does not expose historical confirmed inventory payloads without adding a broader journal API outside the Task 3 file set.

---

# Fix round 1

## Root cause

- Protected terminal verification collapsed `release`, `withdrawal`, and `prepared_revocation` into only one digest/epoch/binding tuple. That discarded receipt kind and withdrawal `slurm_job_id`.
- `unused` inventory verification checked only the latest confirmed inventory record, so a delayed release anchored to an earlier complete inventory sequence failed closed incorrectly.
- `unused` physical absence checked the impossible journal key `("intent", "physical-bind:<intent>")`, while real physical-bind confirmations are under `("executor", "physical-bind:<intent>")`.
- `unused` did not check exact current live/terminal Slurm matches from durable launch evidence.

## Files changed in fix round 1

- `src/loom_capacity_executor/executable.py`
- `src/loom_capacity_executor/journal.py`
- `tests/unit/test_capacity_executor_executable.py`
- `tests/unit/test_capacity_executor_recovery.py`
- `task-3-report.md`

`src/loom_capacity_executor/journal.py` was added to the fix because exact historical inventory lookup needs bounded access to prior durable records for one exact object.

## RED

Executable regressions:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py -k 'prepared_revocation_rejects_slurm or changed_terminal_slurm_job_id or historical_confirmed_inventory_sequence or real_durable_physical_binding_record or exact_owned_current_slurm_match'
```

```text
FFFFFF                                                                   [100%]
FAILED tests/unit/test_capacity_executor_executable.py::test_protected_terminal_prepared_revocation_rejects_slurm_terminal_kind
FAILED tests/unit/test_capacity_executor_executable.py::test_withdrawal_release_rejects_changed_terminal_slurm_job_id
FAILED tests/unit/test_capacity_executor_executable.py::test_unused_release_accepts_exact_historical_confirmed_inventory_sequence
FAILED tests/unit/test_capacity_executor_executable.py::test_unused_release_rejects_real_durable_physical_binding_record
FAILED tests/unit/test_capacity_executor_executable.py::test_unused_release_rejects_exact_owned_current_slurm_match[live]
FAILED tests/unit/test_capacity_executor_executable.py::test_unused_release_rejects_exact_owned_current_slurm_match[terminal]
6 failed, 43 deselected in 0.83s
```

Representative failures:

```text
E       AssertionError: assert 'released' == 'quarantined'
E       AssertionError: assert 'quarantined' == 'released'
```

Replay regression:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py -k 'prepared_revocation_slurm_terminal_kind'
```

```text
F                                                                        [100%]
E       AssertionError: assert 'released' == 'quarantined'
1 failed, 31 deselected in 0.28s
```

## GREEN

Executable regressions:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py -k 'prepared_revocation_rejects_slurm or changed_terminal_slurm_job_id or historical_confirmed_inventory_sequence or real_durable_physical_binding_record or exact_owned_current_slurm_match'
```

```text
......                                                                   [100%]
6 passed, 43 deselected in 0.26s
```

Replay regression:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py -k 'prepared_revocation_slurm_terminal_kind'
```

```text
.                                                                        [100%]
1 passed, 31 deselected in 0.15s
```

## Final gates

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_agent_executable_admission.py
```

```text
........................................................................ [ 58%]
...................................................                      [100%]
123 passed in 17.40s
```

```bash
uv run --no-sync ruff format --check src/loom_capacity_agent/admission.py src/loom_capacity_executor/executable.py src/loom_capacity_executor/journal.py capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_agent_executable_admission.py
```

```text
7 files already formatted
```

```bash
uv run --no-sync ruff check src/loom_capacity_agent/admission.py src/loom_capacity_executor/executable.py src/loom_capacity_executor/journal.py capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_agent_executable_admission.py
```

```text
All checks passed!
```

```bash
uv run --no-sync mypy src/loom_capacity_agent/admission.py src/loom_capacity_executor/executable.py src/loom_capacity_executor/journal.py
```

```text
Success: no issues found in 3 source files
```

```bash
git diff --check
```

```text
<no output>
```

## Self-review

- Prepared revocation now authorizes only `terminal_kind="unused"`.
- Withdrawal now requires `terminal_kind="slurm-job"` and the exact `withdrawal.slurm_job_id`; terminal job `999` cannot satisfy withdrawal for job `101`.
- Worker-backed releases still use exact Slurm terminal evidence and reject `unused`.
- Durable replay uses the same `_release()` verification path and now preserves the receipt-kind rules.
- Historical confirmed inventories are available through a bounded exact-object journal record lookup; `unused` release anchored to inventory N remains valid after inventory N+1.
- `unused` checks real executor-scoped physical-bind confirmations.
- `unused` checks exact current live and terminal Slurm matches using durable historical launch evidence while ignoring non-exact/foreign records.

## Concerns

- The journal now retains an in-memory history of validated records already bounded by `_MAX_RECORDS` and `_MAX_JOURNAL_BYTES`; this is intentionally narrow and exposed only as exact-object `records(object_kind, object_id)`.

---

# Fix round 2

## Root cause

`unused` release checked exact scheduler matches from durable launch evidence, but did not treat same ownership-token resource mismatches as authenticated ownership conflicts. A live/terminal Slurm record with the signed Loom ownership token but changed CPUs/resources is not proof of absence and cannot be inferred released.

## Files changed in fix round 2

- `src/loom_capacity_executor/executable.py`
- `tests/unit/test_capacity_executor_executable.py`
- `task-3-report.md`

## RED

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py -k 'authenticated_ownership_conflict'
```

```text
FF                                                                       [100%]
FAILED tests/unit/test_capacity_executor_executable.py::test_unused_release_rejects_authenticated_ownership_conflict[live]
FAILED tests/unit/test_capacity_executor_executable.py::test_unused_release_rejects_authenticated_ownership_conflict[terminal]
2 failed, 49 deselected in 0.25s
```

Representative failure:

```text
E       AssertionError: assert 'released' == 'quarantined'
```

## GREEN

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py -k 'authenticated_ownership_conflict'
```

```text
..                                                                       [100%]
2 passed, 49 deselected in 0.15s
```

## Final gates

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_recovery.py tests/integration/test_capacity_agent_executable_admission.py
```

```text
........................................................................ [ 57%]
.....................................................                    [100%]
125 passed in 10.26s
```

```bash
uv run --no-sync ruff format --check src/loom_capacity_executor/executable.py tests/unit/test_capacity_executor_executable.py
```

```text
2 files already formatted
```

```bash
uv run --no-sync ruff check src/loom_capacity_executor/executable.py tests/unit/test_capacity_executor_executable.py
```

```text
All checks passed!
```

```bash
uv run --no-sync mypy src/loom_capacity_executor/executable.py
```

```text
Success: no issues found in 1 source file
```

```bash
git diff --check
```

```text
<no output>
```

## Self-review

- `unused` now quarantines on exact current live Slurm matches.
- `unused` now quarantines on exact current terminal Slurm matches.
- `unused` now quarantines on authenticated live ownership conflicts with the same signed ownership token and changed resources.
- `unused` now quarantines on authenticated terminal ownership conflicts with the same signed ownership token and changed resources.
- Scheduler state is read-only in these paths; live and terminal fake Slurm records are retained unchanged and central release is not called.
- Foreign/non-matching scheduler work still is not inferred released by these ownership-token predicates.

## Concerns

- None beyond the existing historical journal note from fix round 1.
