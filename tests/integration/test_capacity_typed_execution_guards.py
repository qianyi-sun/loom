"""Typed SQL execution reads must preserve real mixed owner allocation history."""

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom_capacity_manager.models import CapacityAllocationEpoch, CapacityAuthorityState
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import WriterFence
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)
from tests.capacity_fixtures import pool_observation
from tests.integration.test_capacity_mixed_membership_store import apply
from tests.integration.test_capacity_typed_membership_demand import report
from tests.integration.test_capacity_typed_membership_execution import typed_management


async def sealed_owners(session):
    _legacy, preparation, _fleet, execution = await typed_sql_execution(session)
    management = typed_management(preparation)
    authority = await session.get(CapacityAuthorityState, 1)
    authority.increase_freeze = False
    authority.increase_freeze_reason = None
    members = []
    for index, owner in enumerate((88010, 88011)):
        build = await apply(session, build_request(preparation, execution,
            owner=owner, revision=index * 2), key=121000 + index * 2)
        application = await apply(session, application_request(preparation, execution,
            owner=owner, revision=index * 2 + 1), key=121001 + index * 2)
        members.extend((build.member, application.member))
        await management.ingest_demand_snapshot(session, report(application.member.configuration),
            actor="owner-agent")
    for pool in ("gb10", "oldlab"):
        await management.ingest_pool_observation(session,
            pool_observation(sequence=1, pool_id=pool), actor=f"{pool}-reporter")
    sessions = async_sessionmaker(bind=session.bind, expire_on_commit=False,
        join_transaction_mode="create_savepoint")
    result = await reconcile_shadow_once(sessions,
        WriterFence(authority_incarnation=execution.authority_incarnation,
            writer_epoch=execution.writer_epoch), store=management)
    assert result.status == "committed"
    allocation = (await session.scalars(select(CapacityAllocationEpoch))).one()
    assert allocation.sealed and allocation.complete_payload["schema_version"] == 4
    return preparation, execution, allocation, members


async def test_typed_sql_pinned_and_current_reads_preserve_application_and_build_purposes(capacity_session):
    _preparation, _execution, allocation, members = await sealed_owners(capacity_session)
    for member in members:
        parameters = {"allocation": allocation.allocation_epoch,
            "subject": member.configuration.subject_id,
            "incarnation": member.configuration.subject_incarnation}
        pinned = await capacity_session.scalar(text("""
            SELECT public.capacity_membership_pinned_subject(:allocation, :subject, :incarnation)
        """), parameters)
        assert pinned["configuration"] == member.configuration.model_dump(mode="json")
        assert pinned["acknowledgement"] == member.acknowledgement.model_dump(mode="json")
        assert pinned["purpose"] == member.purpose
        query = text("SELECT public.capacity_membership_target_current(:allocation, :subject, :incarnation)")
        if member.purpose == "personal-application":
            assert await capacity_session.scalar(query, parameters) is True
        else:
            # A retained pending build service is not an executable worker.
            with pytest.raises(DBAPIError):
                async with capacity_session.begin_nested():
                    await capacity_session.scalar(query, parameters)
