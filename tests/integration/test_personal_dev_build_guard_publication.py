"""Publication must derive acknowledgement authority from existing durable holds."""

from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAcknowledgementV2,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def publish(session, retained, plan_id):
    return await session.scalar(text("SELECT loom_capacity_build_guard.authorize_publication(:installation, :plan)"),
        {"installation": retained.id, "plan": plan_id})


async def test_publication_uses_durable_assignments_and_replays_exactly(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        prepared = await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions.begin() as session:
        wire = await publish(session, retained, proposal.plan_id)
    ack = ExecutableAdmissionAcknowledgementV2.model_validate_json(wire)
    assert ack.prepared_plan_digest == prepared.digest
    assert ack.proposal_digest == canonical_executable_digest(proposal)
    assert ack.assignments[0].transition_id == prepared.assignments[0].id
    assert ack.assignments[0].lifecycle_sequence == prepared.assignments[0].request_sequence
    assert ack.assignments[0].execution_generation == registration.build_attempt.lease_epoch
    assert ack.assignments[0].requirements_digest == request.source_binding_sha256
    async with sessions.begin() as session:
        assert await publish(session, retained, proposal.plan_id) == wire
        work = await BuildGuardPlanStore(session, installation=retained).authorize_publication(proposal.plan_id)
        assert work.acknowledgement == ack
        assert work.idempotency_key == uuid5(NAMESPACE_URL,
            f"loom:protected-executable-admission:{canonical_executable_digest(ack)}")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='publication'")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_publication_never_creates_a_missing_plan(prepared_input):
    sessions, engine, retained, *_ = prepared_input
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="absent"):
            await publish(session, retained, uuid4())
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0


@pytest.mark.parametrize("boundary", ["cancelled", "shortened", "source", "hold"])
async def test_publication_rechecks_current_source_and_holds_even_after_publication(prepared_input, boundary):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions.begin() as session:
        await publish(session, retained, proposal.plan_id)
    with engine.begin() as connection:
        if boundary == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
        elif boundary == "shortened":
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()+interval '10 seconds' WHERE id=:id"), {"id": registration.build_attempt.id})
        elif boundary == "source":
            connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"), {"id": registration.candidate.id})
        else:
            connection.execute(text("DELETE FROM loom_capacity_build_guard.request_holds WHERE request_id=:id"), {"id": request.id})
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match=r"source|lease|hold|assignment"):
            await publish(session, retained, proposal.plan_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 1


async def test_publication_rolls_back_with_caller(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions() as session:
        await session.begin()
        await publish(session, retained, proposal.plan_id)
        await session.rollback()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("kind", ["closure", "release"])
async def test_terminal_disposition_prevents_publication_without_freeing_hold(prepared_input, kind):
    from hashlib import sha256

    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    with engine.begin() as connection:
        connection.execute(text("""INSERT INTO loom_capacity_build_guard.dispositions
            (id,plan_id,kind,payload,wire_payload,payload_sha256) VALUES (:id,:plan,:kind,'{}',:wire,:digest)"""),
            {"id": uuid4(), "plan": proposal.plan_id, "kind": kind, "wire": b"{}", "digest": sha256(b"{}").hexdigest()})
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="terminal disposition"):
            await publish(session, retained, proposal.plan_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_publication_keeps_source_locked_until_callers_manager_receipt(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions.begin() as session:
        await publish(session, retained, proposal.plan_id)
        # A separate database actor cannot cancel while the caller is publishing.
        with engine.begin() as connection:
            connection.execute(text("SET LOCAL lock_timeout='50ms'"))
            with pytest.raises(DBAPIError, match="lock timeout"):
                connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"),
                    {"id": request.id})
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})


@pytest.mark.parametrize("nested", [False, True])
async def test_publication_requires_preparation_committed_before_outer_transaction(prepared_input, nested):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        store = BuildGuardPlanStore(session, installation=retained)
        if nested:
            async with session.begin_nested():
                await store.prepare(proposal, sources={request.id: registration})
        else:
            await store.prepare(proposal, sources={request.id: registration})
        with pytest.raises(DBAPIError, match="committed preparation"):
            await store.authorize_publication(proposal.plan_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).authorize_publication(proposal.plan_id)


async def test_publication_upgrade_preserves_preexisting_committed_plan(prepared_input, build_guard_database):
    from alembic import command

    sessions, _engine, retained, proposal, registration, request = prepared_input
    config, *_ = build_guard_database
    command.downgrade(config, "build_guard_0003")
    async with sessions.begin() as session:
        prepared = await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    command.upgrade(config, "head")
    async with sessions.begin() as session:
        work = await BuildGuardPlanStore(session, installation=retained).authorize_publication(proposal.plan_id)
        assert work.acknowledgement.prepared_plan_digest == prepared.digest
