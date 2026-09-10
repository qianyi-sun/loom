"""Typed inventory is admitted only under authenticated matching V4 authority."""

import pytest

from loom_capacity_manager.executable_contracts import ExecutableExecutorHeartbeatV2
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.store import CapacityStoreError
from loom_capacity_manager.typed_inventory_contracts import ExecutableExecutorInventoryV3
from tests.capacity_build_membership_fixtures import typed_sql_execution
from tests.integration.test_capacity_typed_membership_execution import typed_management


@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
@pytest.mark.parametrize("wrong_policy", (False, True))
@pytest.mark.parametrize("activate", (False, True))
async def test_typed_inventory_admission_preserves_operator_and_executor_fences(
    capacity_session, pool, wrong_policy, activate
):
    legacy, preparation, _fleet, execution = await typed_sql_execution(
        capacity_session, activate=activate
    )
    management = legacy if wrong_policy else typed_management(preparation)
    binding = next(item for item in preparation.executors if item.pool_id == pool)
    store = CapacityExecutionStore()
    common = dict(
        execution=execution,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=pool,
        pool_generation=binding.pool_generation,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    await store.heartbeat_executor(
        capacity_session, ExecutableExecutorHeartbeatV2(**common, heartbeat_sequence=1)
    )
    value = ExecutableExecutorInventoryV3(**common, inventory_sequence=1)
    if wrong_policy:
        with pytest.raises(CapacityStoreError):
            await store.ingest_typed_executor_inventory(
                capacity_session, value, management=management
            )
        checkpoint = await store.executor_checkpoint(capacity_session, binding)
        assert checkpoint.inventory_sequence == 0
    else:
        result = await store.ingest_typed_executor_inventory(
            capacity_session, value, management=management
        )
        assert result.inventory_sequence == 1
        replay = await store.ingest_typed_executor_inventory(
            capacity_session, value, management=management
        )
        assert replay.replayed
        if not activate:
            from loom_capacity_manager.typed_membership_store import _load_typed_history

            context = await management.execution_authority(capacity_session)
            assert context.execution_state == "prepared"
            assert context.executable_new_capacity_ceiling == 0
            with pytest.raises(CapacityStoreError, match="current activated authority"):
                await _load_typed_history(capacity_session, execution.execution_epoch)
