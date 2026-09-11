"""The installed guard loads real pending sources, not caller-made readiness."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_build_guard.demand_store import BuildDemandCoordinator
from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def test_capture_loads_real_sources_and_excludes_held_work(prepared_input):
    sessions, _engine, retained, proposal, registration, request = prepared_input
    coordinator = BuildDemandCoordinator(sessions, installation=retained)
    first = await coordinator.capture(configuration_generation=1)
    assert first.pending_unassigned[0].attempt_ids == (str(request.id),)
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    second = await coordinator.capture(configuration_generation=1)
    assert second.pending_unassigned == ()
    assert second.current_assignments[0].attempt_id == str(request.id)
    assert second.sequence == first.sequence+1


@pytest.mark.parametrize("terminal", ["cancelled", "expired", "finished"])
async def test_capture_does_not_load_inactive_sources(prepared_input, terminal):
    sessions, engine, retained, _proposal, registration, request = prepared_input
    with engine.begin() as connection:
        if terminal == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
        elif terminal == "expired":
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
                {"id": registration.build_attempt.id})
        else:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET state='succeeded', finished_at=now(), lease_expires_at=NULL WHERE id=:id"),
                {"id": registration.build_attempt.id})
    snapshot = await BuildDemandCoordinator(sessions, installation=retained).capture(configuration_generation=1)
    assert snapshot.pending_unassigned == snapshot.current_assignments == snapshot.fixed_claims == ()


async def test_source_loading_does_not_rebind_changed_source_to_staged_request(prepared_input):
    sessions, engine, retained, _proposal, registration, _request = prepared_input
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_candidates SET object_key=object_key || '-changed' WHERE id=:id"),
            {"id": registration.candidate.id})
    with pytest.raises(DBAPIError, match="source"):
        await BuildDemandCoordinator(sessions, installation=retained).capture(configuration_generation=1)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.reporter_state")) == 0


@pytest.mark.parametrize("boundary", ["missing", "execute", "search-path", "grant-option"])
def test_source_reader_required_privileges(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.read_pending_sources(uuid)"
    with engine.begin() as connection:
        if boundary == "missing":
            connection.exec_driver_sql(f"DROP FUNCTION {signature}")
        elif boundary == "execute":
            connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}")
        elif boundary == "search-path":
            connection.exec_driver_sql(f"ALTER FUNCTION {signature} SET search_path=public")
        else:
            connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {signature} TO {engine.dialect.identifier_preparer.quote(agent)} WITH GRANT OPTION")
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
