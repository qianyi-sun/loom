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


async def test_capture_does_not_lock_finished_unheld_history(prepared_input):
    store_type = import_module("loom_capacity_build_guard.demand_store").BuildGuardDemandStore
    sessions, engine, retained, _proposal, registration, _request = prepared_input
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET state='succeeded', finished_at=now(), lease_expires_at=NULL WHERE id=:id"),
            {"id": registration.build_attempt.id})
    with engine.begin() as connection:
        connection.execute(text("SELECT id FROM personal_dev_candidate_build_attempts WHERE id=:id FOR UPDATE"),
            {"id": registration.build_attempt.id})
        async with sessions.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='100ms'"))
            result = await store_type(session, installation=retained).capture(configuration_generation=1, sources={})
            assert result.pending_unassigned == result.current_assignments == ()


async def test_capture_rejects_configuration_regression(prepared_input):
    from sqlalchemy.exc import DBAPIError

    store_type = import_module("loom_capacity_build_guard.demand_store").BuildGuardDemandStore
    sessions, _engine, retained, _proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        first = await store_type(session, installation=retained).capture(configuration_generation=2, sources={request.id: registration})
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="configuration"):
            await store_type(session, installation=retained).capture(configuration_generation=1, sources={request.id: registration})
        assert await store_type(session, installation=retained).read_latest() == first


async def test_two_owner_reports_capture_concurrently_without_cross_owner_demand(prepared_input, sessions, owner_sessions, tmp_path):
    import asyncio
    from datetime import UTC, datetime
    from uuid import uuid4

    from loom.personal_dev_build_platform_requests import stage_platform_requests
    from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
    from tests.integration.test_personal_dev_build_platform_requests import build_service
    from tests.integration.test_personal_dev_native_builder_store import _seed_running_attempt

    coordinator_type = import_module("loom_capacity_build_guard.demand_store").BuildDemandCoordinator
    agent_sessions, _engine, first_installation, _proposal, first_source, first_request = prepared_input
    now = datetime.now(UTC)
    second_source = await _seed_running_attempt(sessions, now=now)
    member, runtime = build_service(tmp_path, second_source)
    subject, incarnation, reporter = uuid4(), uuid4(), uuid4()
    member = member.model_copy(update={"configuration": member.configuration.model_copy(update={
        "subject_id": subject, "subject_incarnation": incarnation, "demand_reporter_incarnation": reporter}),
        "acknowledgement": member.acknowledgement.model_copy(update={
            "subject_id": subject, "subject_incarnation": incarnation, "reporter_incarnation": reporter})})
    async with sessions.begin() as session:
        second_request, = await stage_platform_requests(session, second_source, member=member, runtime=runtime,
            platforms=(first_request.platform,), now=now)
    owner_factory, owner = owner_sessions
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        second_installation = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(member=member, runtime=runtime)

    async def capture(installation, request, source):
        return await coordinator_type(agent_sessions, installation=installation).capture(configuration_generation=1, sources={request.id: source})

    first, second = await asyncio.gather(capture(first_installation, first_request, first_source),
        capture(second_installation, second_request, second_source))
    assert first.sequence == second.sequence == 1
    assert first.subject_id != second.subject_id
    assert first.pending_unassigned[0].attempt_ids == (str(first_request.id),)
    assert second.pending_unassigned[0].attempt_ids == (str(second_request.id),)


@pytest.mark.parametrize("signature", ["capture_demand(uuid,bigint,jsonb)", "read_demand(uuid)"])
@pytest.mark.parametrize("boundary", ["missing", "execute", "search-path", "grant-option"])
def test_migration_requires_exact_demand_entrypoints(build_guard_database, signature, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    function = f"loom_capacity_build_guard.{signature}"
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as connection:
        if boundary == "missing":
            connection.exec_driver_sql(f"DROP FUNCTION {function}")
        elif boundary == "execute":
            connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {function} FROM {quote(agent)}")
        elif boundary == "search-path":
            connection.exec_driver_sql(f"ALTER FUNCTION {function} SET search_path=public")
        else:
            connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {function} TO {quote(agent)} WITH GRANT OPTION")
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


async def test_malformed_report_response_does_not_advance_reporter_state(prepared_input, monkeypatch):
    import json

    store_type = import_module("loom_capacity_build_guard.demand_store").BuildGuardDemandStore
    sessions, _engine, retained, _proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        scalar = session.scalar

        async def corrupt(*args, **kwargs):
            payload = json.loads(await scalar(*args, **kwargs))
            payload["pending_unassigned"][0]["requested_slots"] = 2
            return json.dumps(payload, sort_keys=True, separators=(",", ":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError):
            await store_type(session, installation=retained).capture(configuration_generation=1, sources={request.id: registration})
    async with sessions.begin() as session:
        assert await store_type(session, installation=retained).read_latest() is None
