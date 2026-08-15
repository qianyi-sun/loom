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
