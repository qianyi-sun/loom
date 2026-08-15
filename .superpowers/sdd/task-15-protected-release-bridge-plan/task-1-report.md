## Status

Implemented Task 1 protected release outbox and exact normalization. Ready for scoped commit.

## Root cause / design

`guard_0017` produced protected executable release evidence (`released`, `withdrawn`, and `prepared-revoked`) but had no least-privilege, restart-safe outbox for the registered capacity-agent role to publish those releases to the manager contract. Task 1 adds `guard_0018` with owner-only cursor/evidence tables and two bounded SECURITY DEFINER functions:

- `read_next_executable_protected_release(uuid)`
- `acknowledge_executable_protected_release_publication(uuid,bigint,jsonb,text,text)`

The read path validates serializable isolation, exact current registered agent authority, and returns the earliest unacknowledged admission event in `released`, `withdrawn`, or `prepared-revoked`. Python validates the returned manager `ExecutableProtectedReleaseV2` and computes the canonical executable digest.

The acknowledgement path revalidates the same authority, locks the per-agent state, rederives the next manager release payload, requires exact JSON equality and lowercase SHA-256 digests, writes append-only publication evidence, and compare-and-sets `last_event_id`. Exact replay returns the stored receipt; changed replay conflicts.

## Changed files

- `capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py`
- `src/loom_capacity_agent/admission.py`
- `src/loom_capacity_agent/store.py`
- `tests/unit/test_capacity_agent_admission_contracts.py`
- `tests/integration/test_capacity_agent_executable_admission.py`
- `tests/integration/test_capacity_agent_migrations.py`
- `tests/integration/test_capacity_guard_migrations.py`
- `tests/integration/test_capacity_submission_store.py`
- `.superpowers/sdd/task-15-protected-release-bridge-plan/task-1-report.md`

Unrelated pre-existing dirty files were preserved and not staged.

## Commits

- Commit to be created with message: `feat(capacity): expose protected release outbox`

## RED commands / outcomes

- `uv run --no-sync pytest -q tests/unit/test_capacity_agent_admission_contracts.py -k publishable_release`
  - RED: failed during collection because `PublishableExecutableProtectedReleaseV2` was missing.
- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py -k release_outbox`
  - RED: failed during collection because `ProtectedReleasePublicationCheckpointV2` and the outbox wrappers/migration surface were missing.

## GREEN commands / outcomes

- `uv run --no-sync pytest -q tests/unit/test_capacity_agent_admission_contracts.py -k publishable_release`
  - GREEN: `2 passed, 7 deselected`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py -k release_outbox`
  - GREEN after implementation and explicit skip-event assertion: `5 passed, 34 deselected`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_guard_migrations.py -k '0018 or schema_has_exact_owner or schema_startup'`
  - GREEN: `4 passed, 21 deselected`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_migrations.py -k 'bounded_agent_functions or fixed_search_paths or executor_role'`
  - GREEN: `4 passed, 4 deselected`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_submission_store.py`
  - GREEN: `36 passed`.
- `uv run --no-sync pytest -q tests/integration/test_capacity_agent_executable_admission.py tests/integration/test_capacity_agent_migrations.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_submission_store.py tests/unit/test_capacity_agent_admission_contracts.py`
  - GREEN before the later explicit skip-event assertion: `117 passed`.

## Quality gates

Scoped Task 1 gates requested at checkpoint:

- `uv run --no-sync ruff format --check capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py src/loom_capacity_agent/admission.py src/loom_capacity_agent/store.py tests/unit/test_capacity_agent_admission_contracts.py tests/integration/test_capacity_agent_executable_admission.py tests/integration/test_capacity_agent_migrations.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_submission_store.py`
  - PASS: `8 files already formatted`.
- `uv run --no-sync ruff check capacity_guard_migrations/versions/guard_0018_executable_release_outbox.py src/loom_capacity_agent/admission.py src/loom_capacity_agent/store.py tests/unit/test_capacity_agent_admission_contracts.py tests/integration/test_capacity_agent_executable_admission.py tests/integration/test_capacity_agent_migrations.py tests/integration/test_capacity_guard_migrations.py tests/integration/test_capacity_submission_store.py`
  - PASS: `All checks passed!`
- `uv run --no-sync mypy src/loom_capacity_agent/admission.py src/loom_capacity_agent/store.py`
  - PASS: `Success: no issues found in 2 source files`.
- `git diff --check`
  - PASS.

Earlier exact broad Ruff format command from the original brief was also run and failed only on unrelated pre-existing files under `src/loom_capacity_agent/reporter.py` and `src/loom_capacity_agent/secret_init.py`; those files were preserved.

## Self-review

- Exact normalization:
  - `released` maps to manager `ExecutableProtectedReleaseV2.protected_release_sha256` from the release receipt.
  - `withdrawn` maps to manager `protected_release_sha256` from `withdrawal_digest` and does not invent worker-release evidence.
  - `prepared-revoked` maps to manager `protected_release_sha256` from the prepared revocation receipt.
  - All three normalize to the manager fields only: schema version, binding, reporter incarnation, bootstrap/protected epochs, bootstrap revoked, protected release digest, executable.
- Agent authority scope:
  - Functions require serializable transactions, session user equal to the registered agent role, no owner membership, and exact current registered agent/fence binding.
  - Executor and observer roles are not granted the outbox functions.
  - Application roles receive no table privileges on outbox state/evidence tables.
- Replay / CAS behavior:
  - Read without ack returns the same next event.
  - Ack must target the next event id and exact payload.
  - Exact ack replay converges to the stored receipt.
  - Changed replay conflicts.
  - State cursor update is compare-and-set under row lock.
- Bounds:
  - JSON publication payloads are objects and limited to 8 MiB.
  - Publication and acknowledgement digests are lowercase SHA-256.
  - Python wrappers use canonical executable bytes/digest and bounded v2 model validation.
- Fixed search path:
  - Both `guard_0018` functions are SECURITY DEFINER with `SET search_path = pg_catalog`; migration tests cover the agent function inventory.
- Downgrade fidelity:
  - Downgrade refuses when publication evidence exists.
  - Evidence-free downgrade drops only the outbox functions/tables and restores `guard_0017`; re-upgrade restores `guard_0018` grants.

## Concerns

- The original brief named `tests/unit/test_capacity_agent_admission.py`, but this worktree has `tests/unit/test_capacity_agent_admission_contracts.py`; tests were added to and run from the existing file.
- The exact broad Ruff format command from the original brief still fails on unrelated pre-existing formatting drift in `src/loom_capacity_agent/reporter.py` and `src/loom_capacity_agent/secret_init.py`. Per the preservation constraint, those files were not modified or staged.
- The combined 117-test suite was not rerun after adding the final explicit skip-event assertion because the checkpoint instruction limited further execution to Ruff/MyPy/diff gates. The outbox-focused test containing the skip assertion was rerun and passed.
