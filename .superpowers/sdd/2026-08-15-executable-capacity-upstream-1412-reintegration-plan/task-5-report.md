## Task 5 report

### Summary

Task 5 needed real changes.

The immediate RED in `tests/integration/test_executable_global_capacity_bridge.py::test_redeploy_delete_and_drain_require_full_epoch_turnover` came from a stale executable protected-release outbox row surviving an owner redeploy. The harness generated launched-worker protected release evidence inside the guard and then bypassed the executable release reporter by sending the manager release directly. That left the guard outbox unread. After redeploy, the next reporter run read the older unread event under the new owner registration and correctly failed with `executable protected release event binding is stale`.

While recovering that path, I also re-validated the speculative accepted->released work. `capacity_0006` was restored byte-for-byte to blob `e9133f3ac92588c7580904a64af49f66576b5850` first. Reverting the broad accepted->released store branch exposed one real required case: an `accepted` intent under active increase freeze must still release directly. That transition is now allowed only through `capacity_0007`, with upgrade/downgrade coverage. Drain-only accepted intents went back to emitting close work.

### Changes made

1. Restored `capacity_migrations/versions/capacity_0006_executable_work_queue.py` to the required official blob and kept official `capacity_0004/0005/0006` byte-identical.
2. Changed `tests/support/executable_capacity_harness.py` so launched/pending-cancel retirement paths:
   - still create exact protected release evidence in the guard;
   - then publish it through `ExecutableProtectedReleaseReporterRuntime` via `publish_next_protected_release_with_replay(...)`;
   - no longer bypass the outbox with a direct manager `PUT`;
   - therefore consume the append-only protected-release ledger in-order and avoid stale unread rows across redeploy.
3. Kept the executor/manager/root-cause fixes that survived review:
   - exact `pool_id` propagation into `ExecutableReservationAcceptanceV2`;
   - trusted launcher digest in ownership proof metadata uses `binding.execution.trusted_fleet_release_sha256`;
   - retirement-safe recomputation from stored exact inventory plus later confirmation heartbeat;
   - harness reads `CapacityExecutableProtectedReleaseReceipt` as the sole protected-release ledger;
   - launched/pending-cancel paths keep terminal inventory before central close.
4. Refactored the duplicated retirement heartbeat confirmation predicate in `src/loom_capacity_manager/execution_store.py` into `_heartbeat_confirms_final_inventory(...)`.
5. Narrowed accepted->released behavior in `src/loom_capacity_manager/execution_store.py`:
   - `accepted` + active freeze/active no-increase still releases directly;
   - `accepted` + drain-only goes through close work again.
6. Added the required queue-guard delta in `capacity_migrations/versions/capacity_0007_executable_bridge_completion.py`:
   - upgrade rewrites `capacity_executable_intent_guard()` to allow the exact `accepted -> released` transition;
   - downgrade restores the official `capacity_0006` intent guard exactly.
7. Updated tests for:
   - drain-only accepted close behavior in `tests/integration/test_capacity_manager_execution_store.py`;
   - `capacity_0007` reintegration/roundtrip guard expectations in `tests/integration/test_capacity_migration_reintegration.py`.

### RED/GREEN evidence

Initial reproduced RED:

- `uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k redeploy_delete_and_drain_require_full_epoch_turnover`

Immediate recovery regressions isolated and resolved:

- `uv run --no-sync pytest -q tests/integration/test_capacity_manager_execution_store.py -k drain_only_writer_transition_allows_retained_intent_close`
- `uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k redeploy_delete_and_drain_require_full_epoch_turnover`
- `uv run --no-sync pytest -q tests/integration/test_capacity_migration_reintegration.py -k 'capacity_0007_adds_bridge_completion_and_only_patches_accepted_release_guard or reintegrated_capacity_round_trip_restores_exact_upstream_capacity_0005_surface'`

Focused executor/store validation:

- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_executable.py tests/unit/test_capacity_executor_launch_renderer.py tests/unit/test_capacity_executor_recovery.py tests/unit/test_capacity_executor_remote.py tests/integration/test_capacity_manager_execution_store.py`

Bridge file:

- `uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py`

Repeated public boundary flows:

- `uv run --no-sync pytest -q tests/integration/test_executable_global_capacity_bridge.py -k 'prepared_unused or unregistered_withdrawn or protected_release'`
- repeated twice, both green

Repeated recovery flows:

- `uv run --no-sync pytest -q tests/unit/test_capacity_executor_recovery.py -k 'ambiguous or protected or release'`
- repeated twice, both green

Full Task 5 suite from the brief:

```bash
uv run --no-sync pytest -q \
  tests/unit/test_capacity_agent_*.py \
  tests/unit/test_capacity_executor_*.py \
  tests/integration/test_capacity_agent_*.py \
  tests/integration/test_capacity_guard_migrations.py \
  tests/integration/test_capacity_manager_execution_store.py \
  tests/integration/test_capacity_manager_execution_epoch.py \
  tests/integration/test_executable_global_capacity_bridge.py \
  tests/ops/test_global_fleet_pool_executor_once.py
```

Result: `651 passed`.

### Static checks

- `uv run --no-sync ruff format ...changed paths...`
- `uv run --no-sync ruff check ...changed paths...`
- `uv run --no-sync mypy src/loom_capacity_executor/executable.py src/loom_capacity_executor/launch_renderer.py src/loom_capacity_manager/execution_store.py`
- `git diff --check`

All passed.

### Immutable migration verification

Verified base blob vs working tree hashes:

- `capacity_0004_executable_bridge.py`: `7e91bb25e2a42b7a1562c7828036a257b380f896`
- `capacity_0005_executable_allocation.py`: `15f24e0f66e5d0d0bf3352d195fe61b129576cf7`
- `capacity_0006_executable_work_queue.py`: `e9133f3ac92588c7580904a64af49f66576b5850`

### Self-review checklist

- No live actions were introduced.
- Protected release receipt remains the sole append-only manager release ledger.
- Consumed/ambiguous work remains quarantined and charged.
- Foreign work remains untouched.
- Exact pool/generation bindings are preserved.
- Exact trusted launcher validation remains `proof.metadata.trusted_launcher_sha256 == epoch.trusted_fleet_release_sha256`.
- Integer-zero ceiling still avoids new mutating backend construction.
- No V1 widening was introduced.
- Official `capacity_0004/0005/0006` remain byte-identical.
- `capacity_0007` is the only migration changed, and only for the proved accepted->released guard delta plus exact roundtrip coverage.
