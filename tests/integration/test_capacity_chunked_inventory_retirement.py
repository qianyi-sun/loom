"""A large final inventory must retire through actual manager heartbeat guards."""

from uuid import UUID

from sqlalchemy import text

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


async def test_large_typed_final_inventory_confirms_retirement_in_database(capacity_session):
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
    await store.ingest_typed_executor_inventory(capacity_session, inventory,
        management=typed_management(preparation))
    sequence, digest = inventory_confirmation_journal_head(inventory)
    assert sequence > 2
    await store.heartbeat_executor(capacity_session, ExecutableExecutorHeartbeatV2(
        **common, heartbeat_sequence=2, journal_sequence=sequence, journal_digest=digest,
        journal_checkpoint_sequence=0, journal_checkpoint_digest="0" * 64))
    assert await capacity_session.scalar(text("""
        SELECT retirement_safe FROM capacity_executable_executor_states
        WHERE executor_incarnation=:executor
    """), {"executor": binding.executor_incarnation}) is True
