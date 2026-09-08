"""Persisted executable allocation evidence for dynamic application membership."""

import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.membership_execution import ExecutableEpochV3, parse_executable_epoch
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import CapacityAllocationEpoch
from loom_capacity_manager.reconciler import _commit_reconciled_epoch
from tests.capacity_fixtures import demand_snapshot, pool_observation
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _request


async def test_executable_commit_pins_exact_membership_without_rewriting_base(
    isolated_capacity_postgres_url: str,
) -> None:
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            fixture, active = await _active_v3(session)
            request = _request(active)
            admitted = await CapacityMembershipStore(fixture.store).apply(
                session, request, actor=DELEGATE, idempotency_key=UUID(int=22200)
            )
            await fixture.store.ingest_demand_snapshot(
                session,
                demand_snapshot(sequence=1, pending_attempt_ids=("base-attempt",)),
                actor="development",
            )
            for pool_id in ("gb10", "oldlab"):
                await fixture.store.ingest_pool_observation(
                    session,
                    pool_observation(sequence=1, pool_id=pool_id),
                    actor=f"{pool_id}-reporter",
                )
        async with sessions() as reader, reader.begin():
            value = await fixture.store.load_allocation_input(reader, fixture.writer)
        async with sessions() as committer:
            allocation_id, committed = await _commit_reconciled_epoch(
                committer, fixture.store, fixture.writer, allocate_shadow(value)
            )
        assert isinstance(committed, ExecutableEpochV3)
        assert committed.membership == value.membership
        assert committed.membership.revision == admitted.revision
        assert committed.configuration == value.configuration
        async with sessions() as reader:
            row = await reader.scalar(
                select(CapacityAllocationEpoch).where(
                    CapacityAllocationEpoch.allocation_epoch == allocation_id,
                )
            )
            assert row is not None and row.sealed
            assert parse_executable_epoch(json.dumps(row.complete_payload)) == committed
    finally:
        await engine.dispose()
