"""A large final inventory must retire through actual manager heartbeat guards."""

import json
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import ExecutableExecutorHeartbeatV2
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.typed_inventory_contracts import (
    ExecutableExecutorInventoryV3,
    ExecutableInventoryRecordV3,
    inventory_confirmation_journal_head,
)
from tests.capacity_build_membership_fixtures import typed_sql_execution
from tests.integration.test_capacity_manager_execution_epoch import _drain_request
from tests.integration.test_capacity_typed_membership_execution import typed_management


async def inventory_setup(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    binding = preparation.executors[0]
    store = CapacityExecutionStore()
    common = dict(execution=execution, executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation, pool_id=binding.pool_id,
        pool_generation=binding.pool_generation)
    await store.heartbeat_executor(capacity_session, ExecutableExecutorHeartbeatV2(
        **common, heartbeat_sequence=1, journal_sequence=0, journal_digest="0" * 64))
    await management.begin_execution_drain(capacity_session, _drain_request(execution),
        actor="inventory-retirement-operator", idempotency_key=UUID(int=88001))
    inventory = ExecutableExecutorInventoryV3(**common, inventory_sequence=1,
        journal_sequence=0, journal_digest="0" * 64, records=tuple(
            ExecutableInventoryRecordV3(physical_identity=f"foreign-{index:04d}",
                physical_kind="slurm-job", authority_scope="foreign", state="active",
                resources=ResourceVectorV1(slots=1), controller_evidence_sha256="a" * 64)
            for index in range(300)))
    return store, typed_management(preparation), common, inventory


@pytest.mark.parametrize("large", (False, True))
async def test_large_typed_final_inventory_confirms_retirement_in_database(capacity_session, large):
    store, management, common, inventory = await inventory_setup(capacity_session)
    if not large:
        inventory = inventory.model_copy(update={"records": ()})
    await store.ingest_typed_executor_inventory(capacity_session, inventory,
        management=management)
    sequence, digest = inventory_confirmation_journal_head(inventory)
    assert (sequence > 2) == large
    await store.heartbeat_executor(capacity_session, ExecutableExecutorHeartbeatV2(
        **common, heartbeat_sequence=2, journal_sequence=sequence, journal_digest=digest,
        journal_checkpoint_sequence=0, journal_checkpoint_digest="0" * 64))
    assert await capacity_session.scalar(text("""
        SELECT retirement_safe FROM capacity_executable_executor_states
        WHERE executor_incarnation=:executor
    """), {"executor": inventory.executor_incarnation}) is True
    with pytest.raises(IntegrityError, match="capacity_executable_executor_retirement_check"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(text("""
                UPDATE capacity_executable_executor_states SET journal_high_water=journal_high_water+1
                WHERE executor_incarnation=:executor
            """), {"executor": inventory.executor_incarnation})


@pytest.mark.parametrize("retained", ("none", "latest", "historical"))
async def test_chunked_inventory_rollback_preserves_evidence_and_fences_late_writers(capacity_session, retained):
    from alembic import command

    from tests.integration.test_capacity_build_membership_sql import _config

    store, management, _common, inventory = await inventory_setup(capacity_session)
    if retained != "none":
        await store.ingest_typed_executor_inventory(capacity_session, inventory, management=management)
        if retained == "historical":
            await store.ingest_typed_executor_inventory(capacity_session,
                inventory.model_copy(update={"inventory_sequence": 2, "records": ()}), management=management)
        await capacity_session.execute(text("""
            UPDATE capacity_executable_executor_states SET chunked_inventory_seen=false
            WHERE executor_incarnation=:executor
        """), {"executor": inventory.executor_incarnation})
        assert await capacity_session.scalar(text("""
            SELECT chunked_inventory_seen FROM capacity_executable_executor_states
            WHERE executor_incarnation=:executor
        """), {"executor": inventory.executor_incarnation}) is True
        with pytest.raises(RuntimeError, match="retained chunked inventory"):
            async with capacity_session.begin_nested():
                connection = await capacity_session.connection()
                await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0021"))
        assert await capacity_session.scalar(text("SELECT version_num FROM alembic_version")) == "capacity_0022"
    else:
        connection = await capacity_session.connection()
        await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0021"))
        with pytest.raises(IntegrityError, match="capacity_executor_inline_inventory_check"):
            async with capacity_session.begin_nested():
                # An already-entered old writer updates its original columns;
                # current ORM code intentionally requires the current schema.
                await capacity_session.execute(text("""
                    UPDATE capacity_executable_executor_states
                    SET inventory_payload=CAST(:payload AS jsonb), inventory_high_water=1,
                        last_inventory_digest=repeat('a',64)
                    WHERE executor_incarnation=:executor
                """), {"payload": json.dumps(inventory.model_dump(mode="json")),
                    "executor": inventory.executor_incarnation})
        await connection.run_sync(lambda sync: command.upgrade(_config(sync), "capacity_0022"))
        await store.ingest_typed_executor_inventory(capacity_session, inventory, management=management)
    guard = (await capacity_session.execute(text("""
        SELECT prosecdef, proconfig, EXISTS (
          SELECT 1 FROM aclexplode(coalesce(proacl,acldefault('f',proowner)))
          WHERE grantee=0 AND privilege_type='EXECUTE')
        FROM pg_proc WHERE oid='public.capacity_executor_chunked_inventory_history_guard()'::regprocedure
    """))).one()
    assert guard[0] and "search_path=pg_catalog" in guard[1] and not guard[2]
