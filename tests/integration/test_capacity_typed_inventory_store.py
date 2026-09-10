"""Typed inventory is admitted only under authenticated matching V4 authority."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

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


@pytest.mark.parametrize("version", ('2', '4', 'null', '"3"', None))
async def test_typed_inventory_sql_rejects_legacy_version_under_typed_manifest(capacity_session, version):
    _legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    binding = preparation.executors[0]
    store = CapacityExecutionStore()
    common = dict(execution=execution, executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation, pool_id=binding.pool_id,
        pool_generation=binding.pool_generation, journal_sequence=0, journal_digest="0" * 64)
    await store.heartbeat_executor(capacity_session,
        ExecutableExecutorHeartbeatV2(**common, heartbeat_sequence=1))
    await store.ingest_typed_executor_inventory(capacity_session,
        ExecutableExecutorInventoryV3(**common, inventory_sequence=1),
        management=typed_management(preparation))
    with pytest.raises(IntegrityError, match="inventory version differs from execution manifest"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(text("""
                UPDATE capacity_executable_executor_states
                   SET inventory_payload = CASE WHEN CAST(:version AS text) IS NULL THEN NULL ELSE jsonb_set(
                         jsonb_set(inventory_payload, '{schema_version}', CAST(:version AS jsonb)),
                         '{inventory_sequence}', to_jsonb(inventory_high_water + 1)) END,
                       inventory_high_water = inventory_high_water + 1
                 WHERE executor_incarnation = :executor
            """), {"executor": binding.executor_incarnation, "version": version})


@pytest.mark.parametrize("retained", (False, True))
async def test_typed_inventory_downgrade_retains_evidence_and_fences_new_writes(capacity_session, retained):
    from alembic import command

    from tests.integration.test_capacity_build_membership_sql import _config

    _legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    binding = preparation.executors[0]
    store = CapacityExecutionStore()
    common = dict(execution=execution, executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation, pool_id=binding.pool_id,
        pool_generation=binding.pool_generation, journal_sequence=0, journal_digest="0" * 64)
    management = typed_management(preparation)
    inventory = ExecutableExecutorInventoryV3(**common, inventory_sequence=1)
    await store.heartbeat_executor(capacity_session,
        ExecutableExecutorHeartbeatV2(**common, heartbeat_sequence=1))
    if retained:
        await store.ingest_typed_executor_inventory(capacity_session, inventory, management=management)
        with pytest.raises(RuntimeError, match="retained typed inventory"):
            async with capacity_session.begin_nested():
                connection = await capacity_session.connection()
                await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0018"))
        assert await capacity_session.scalar(text("SELECT version_num FROM alembic_version")) == "capacity_0022"
        assert (await store.executor_checkpoint(capacity_session, binding)).inventory_sequence == 1
    else:
        connection = await capacity_session.connection()
        await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0018"))
        with pytest.raises(IntegrityError, match="capacity_executor_legacy_inventory_check"):
            async with capacity_session.begin_nested():
                await store.ingest_typed_executor_inventory(capacity_session, inventory, management=management)
        await connection.run_sync(lambda sync: command.upgrade(_config(sync), "capacity_0019"))
        await store.ingest_typed_executor_inventory(capacity_session, inventory, management=management)
        assert (await store.executor_checkpoint(capacity_session, binding)).inventory_sequence == 1
    row = (await capacity_session.execute(text("""
        SELECT prosecdef, proconfig, EXISTS (
          SELECT 1 FROM aclexplode(coalesce(proacl, acldefault('f', proowner)))
           WHERE grantee = 0 AND privilege_type = 'EXECUTE')
          FROM pg_proc WHERE oid = 'public.capacity_executor_inventory_version_guard()'::regprocedure
    """))).one()
    assert row[0] and "search_path=pg_catalog" in row[1] and not row[2]


@pytest.mark.parametrize("retained_payload", (None, '{"schema_version":2}'))
async def test_inventory_upgrade_refuses_missing_or_mismatched_retained_version(capacity_session, retained_payload):
    from alembic import command

    from tests.integration.test_capacity_build_membership_sql import _config

    _legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    binding = preparation.executors[0]
    await CapacityExecutionStore().heartbeat_executor(capacity_session,
        ExecutableExecutorHeartbeatV2(execution=execution, executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation, pool_id=binding.pool_id,
            pool_generation=binding.pool_generation, journal_sequence=0,
            journal_digest="0" * 64, heartbeat_sequence=1))
    connection = await capacity_session.connection()
    await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0018"))
    await capacity_session.execute(text("""
        UPDATE capacity_executable_executor_states
           SET inventory_high_water = 1, last_inventory_digest = repeat('a', 64),
               inventory_payload = CAST(:payload AS jsonb)
         WHERE executor_incarnation = :executor
    """), {"executor": binding.executor_incarnation, "payload": retained_payload})
    with pytest.raises(DBAPIError, match="retained inventory version differs"):
        async with capacity_session.begin_nested():
            connection = await capacity_session.connection()
            await connection.run_sync(lambda sync: command.upgrade(_config(sync), "capacity_0019"))
    assert await capacity_session.scalar(text("SELECT version_num FROM alembic_version")) == "capacity_0018"
    assert await capacity_session.scalar(text(
        "SELECT to_regprocedure('public.capacity_executor_inventory_version_guard()')")) is None
