"""Protected reports must never count held work as pending or forget cancellation holds."""

from importlib import import_module

import pytest
from sqlalchemy import text

from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def test_protected_report_moves_pending_to_held_without_double_counting(prepared_input):
    store_type = import_module("loom_capacity_build_guard.demand_store").BuildGuardDemandStore
    sessions, _engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        pending = await store_type(session, installation=retained).capture(configuration_generation=1,
            sources={request.id: registration})
    assert pending.sequence == 1
    assert pending.subject_id == retained.document.subject_id
    assert pending.reporter_incarnation == retained.document.reporter_incarnation
    assert pending.pending_unassigned[0].attempt_ids == (str(request.id),)
    assert pending.pending_unassigned[0].eligible_pool_ids == (proposal.shapes[0].binding.pool_id,)
    assert pending.current_assignments == pending.fixed_claims == ()
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions.begin() as session:
        held = await store_type(session, installation=retained).capture(configuration_generation=1, sources={})
    assert held.sequence == 2
    assert held.pending_unassigned == held.fixed_claims == ()
    assert held.current_assignments[0].attempt_id == str(request.id)
    assert held.current_assignments[0].allowance_epoch == proposal.shapes[0].binding.execution.allocation_epoch
    async with sessions.begin() as session:
        assert await store_type(session, installation=retained).read_latest() == held


async def test_cancelled_held_request_remains_assigned_and_report_rollback_is_exact(prepared_input):
    store_type = import_module("loom_capacity_build_guard.demand_store").BuildGuardDemandStore
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
        connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
            {"id": registration.build_attempt.id})
    async with sessions() as session:
        await session.begin()
        captured = await store_type(session, installation=retained).capture(configuration_generation=1, sources={})
        assert len(captured.current_assignments) == 1
        assert captured.pending_unassigned == ()
        await session.rollback()
    async with sessions.begin() as session:
        assert await store_type(session, installation=retained).read_latest() is None
        assert (await store_type(session, installation=retained).capture(configuration_generation=1, sources={})).sequence == 1


async def test_capture_requires_outer_transaction_and_complete_current_sources(prepared_input):
    from sqlalchemy.exc import DBAPIError

    store_type = import_module("loom_capacity_build_guard.demand_store").BuildGuardDemandStore
    sessions, _engine, retained, _proposal, registration, request = prepared_input
    async with sessions() as session:
        with pytest.raises(ValueError, match="transaction"):
            await store_type(session, installation=retained).capture(configuration_generation=1, sources={request.id: registration})
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match=r"source|complete"):
            await store_type(session, installation=retained).capture(configuration_generation=1, sources={})
        assert await store_type(session, installation=retained).read_latest() is None
