"""Manager closure is replayable after source expiry, not physical release."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from loom_capacity_manager.executable_contracts import ExecutableAdmissionPlanClosureV2
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("prepared", [False, True])
async def test_closure_replays_after_source_cancellation_without_releasing_holds(prepared_input, prepared):
    sessions, engine, retained, proposal, registration, request = prepared_input
    if prepared:
        async with sessions.begin() as session:
            await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    async with sessions.begin() as session:
        closed = await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
    async with sessions.begin() as session:
        work = await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id)
    assert work.acknowledgement.closure_id == closure.closure_id
    assert work.acknowledgement.disposition_kind == ("abandoned" if prepared else "never-converged")
    async with sessions.begin() as session:
        assert await BuildGuardPlanStore(session, installation=retained).close_plan(closure) == closed
        assert await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id) == work
        with pytest.raises(DBAPIError, match="terminal disposition"):
            await BuildGuardPlanStore(session, installation=retained).authorize_publication(proposal.plan_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='closure'")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == int(prepared)
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='release'")) == 0


async def test_closure_rejects_rebinding_and_rolls_back_with_caller(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    async with sessions() as session:
        await session.begin()
        await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
        await session.rollback()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="replay"):
            await BuildGuardPlanStore(session, installation=retained).close_plan(closure.model_copy(update={"closure_id": uuid4()}))


async def test_closure_remains_available_after_lost_publication_reply(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).authorize_publication(proposal.plan_id)
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="allocation-superseded")
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
    async with sessions.begin() as session:
        work = await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id)
    assert work.acknowledgement.disposition_kind == "abandoned"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 2
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_closure_acknowledgement_requires_committed_closure(prepared_input):
    sessions, engine, retained, proposal, _, _ = prepared_input
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    async with sessions.begin() as session:
        store = BuildGuardPlanStore(session, installation=retained)
        async with session.begin_nested():
            await store.close_plan(closure)
        with pytest.raises(DBAPIError, match="committed closure"):
            await store.authorize_closure_publication(proposal.plan_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id)
