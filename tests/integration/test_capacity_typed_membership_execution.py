"""Fresh typed demand reaches sealed allocation; runtime activation is separate."""

import json
from importlib import import_module

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.models import CapacityAllocationEpoch
from loom_capacity_manager.reconciler import _commit_reconciled_epoch
from loom_capacity_manager.store import WriterFence
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)
from tests.capacity_fixtures import pool_observation
from tests.integration.test_capacity_mixed_membership_store import apply
from tests.integration.test_capacity_typed_membership_demand import report


async def test_typed_two_owner_demand_is_sealed_without_erasing_build_membership(isolated_capacity_postgres_url):
    module = import_module("loom_capacity_manager.membership_execution")
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            management, preparation, _fleet, execution = await typed_sql_execution(session)
            builds = []
            for index, owner in enumerate((88010, 88011)):
                build = await apply(session, build_request(preparation, execution, owner=owner, revision=index * 2), key=111000 + index * 2)
                application = await apply(session, application_request(preparation, execution, owner=owner, revision=index * 2 + 1), key=111001 + index * 2)
                builds.append(build.member.configuration.subject_id)
                await management.ingest_demand_snapshot(session, report(application.member.configuration), actor="owner-agent")
            for pool in ("gb10", "oldlab"):
                await management.ingest_pool_observation(session, pool_observation(sequence=1, pool_id=pool), actor=f"{pool}-reporter")
        writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
        async with sessions() as reader, reader.begin():
            value = await management.load_allocation_input(reader, writer)
        async with sessions() as committer:
            allocation_id, sealed = await _commit_reconciled_epoch(committer, management, writer, allocate_shadow(value))
        assert sealed.schema_version == 4
        assert sealed.membership == value.membership
        assert len(sealed.membership.members) == 4
        assert {member.purpose for member in sealed.membership.members} == {"personal-application", "personal-build-worker"}
        assert sealed.configuration == value.configuration
        assert any(allocation.desired_shapes for allocation in sealed.allocations)
        assert not any(allocation.desired_shapes for allocation in sealed.allocations if allocation.subject_id in builds)
        async with sessions() as reader:
            row = await reader.get(CapacityAllocationEpoch, allocation_id)
            assert row.sealed and row.executable
            assert module.parse_executable_epoch(json.dumps(row.complete_payload)) == sealed
    finally:
        await engine.dispose()
