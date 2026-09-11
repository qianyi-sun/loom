"""One manager proposal must persist one exact protected hold before acknowledgement."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.personal_dev_build_admission import bind_personal_build_admission
from loom.personal_dev_build_platform_requests import (
    canonical_build_source,
    stage_platform_requests,
)
from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_platform_requests import build_service
from tests.integration.test_personal_dev_native_builder_store import _seed_running_attempt
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_personal_dev_build_admission import admission_input


@pytest.fixture(params=["gb10", "oldlab"])
async def prepared_input(build_guard_database, owner_sessions, sessions, tmp_path, request):
    _config, engine, owner, _agent, agent_url = build_guard_database
    now = datetime.now(UTC)
    registration = await _seed_running_attempt(sessions, now=now)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        requests = await stage_platform_requests(session, registration,
            member=member, runtime=runtime,
            platforms=("linux/arm64" if request.param == "gb10" else "linux/amd64",), now=now)
    owner_factory, _ = owner_sessions
    with engine.begin() as connection:
        quote = engine.dialect.identifier_preparer.quote
        for table in ("personal_dev_candidates", "personal_dev_candidate_build_attempts", "personal_dev_build_platform_requests"):
            connection.exec_driver_sql(f"GRANT SELECT, UPDATE (id) ON public.{table} TO {quote(owner)}")
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        retained = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(member=member, runtime=runtime)
    values = admission_input(tmp_path, pool=request.param)
    proposal = values["proposal"]
    shape = proposal.shapes[0]
    shape = shape.model_copy(update={"binding": shape.binding.model_copy(update={"account_id": member.configuration.account_id})})
    proposal = proposal.model_copy(update={"shapes": (shape,), "lease_not_after": now + timedelta(minutes=2),
        "allowances": (proposal.allowances[0].model_copy(update={"protected_attempt_id": requests[0].id}),)})
    bind_personal_build_admission(member=member, runtime=runtime, execution=values["execution"],
        proposal=proposal, requests=((requests[0], registration),), now=now)
    database = create_async_engine(agent_url.set(drivername="postgresql+psycopg"), isolation_level="SERIALIZABLE")
    try:
        yield async_sessionmaker(database), engine, retained, proposal, registration, requests[0]
    finally:
        await database.dispose()


async def prepare(session, retained, proposal, registration, request):
    wire = canonical_executable_bytes(proposal)
    return await session.scalar(text("""SELECT loom_capacity_build_guard.prepare_plan(
        :installation, CAST(:payload AS jsonb), :wire, :digest, CAST(:sources AS jsonb))
    """), {"installation": retained.id, "payload": wire.decode("ascii"), "wire": wire,
        "digest": canonical_executable_digest(proposal),
        "sources": json.dumps({str(request.id): canonical_build_source(registration).decode("ascii")})})


async def test_agent_preparation_persists_exact_replay_and_one_hold(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        first = await prepare(session, retained, proposal, registration, request)
    async with sessions.begin() as session:
        assert await prepare(session, retained, proposal, registration, request) == first
    assert first["proposal_digest"] == canonical_executable_digest(proposal)
    assert len(first["assignments"]) == 1
    assert first["assignments"][0]["request_id"] == str(request.id)
    assert first["assignments"][0]["request_sequence"] == 1
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.assignments")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0


async def test_conflicting_proposal_cannot_hold_same_request(prepared_input):
    from uuid import uuid4

    from sqlalchemy.exc import DBAPIError

    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await prepare(session, retained, proposal, registration, request)
    conflict = proposal.model_copy(update={"plan_id": uuid4(), "proposal_id": uuid4()})
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match=r"hold|assignment"):
            await prepare(session, retained, conflict, registration, request)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 1


async def test_preparation_rolls_back_with_outer_transaction(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions() as session:
        await session.begin()
        await prepare(session, retained, proposal, registration, request)
        await session.rollback()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0


@pytest.mark.parametrize("boundary", ["node", "account", "reporter", "expired", "source"])
async def test_preparation_rejects_authority_drift_without_partial_writes(prepared_input, boundary):
    from uuid import uuid4

    from sqlalchemy.exc import DBAPIError

    sessions, engine, retained, proposal, registration, request = prepared_input
    if boundary in {"node", "account"}:
        shape = proposal.shapes[0]
        changes = {"node_ids": ("controller-forbidden",)} if boundary == "node" else {"account_id": "foreign-owner"}
        proposal = proposal.model_copy(update={"shapes": (shape.model_copy(update={
            "binding": shape.binding.model_copy(update=changes)}),)})
    elif boundary == "reporter":
        proposal = proposal.model_copy(update={"reporter_incarnation": uuid4()})
    elif boundary == "expired":
        proposal = proposal.model_copy(update={"lease_not_after": datetime.now(UTC) - timedelta(seconds=1)})
    else:
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"), {"id": registration.candidate.id})
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match=r"binding|installation|lease|source"):
            await prepare(session, retained, proposal, registration, request)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0


@pytest.mark.parametrize("boundary", ["cancelled", "shortened"])
async def test_current_replay_rejects_cancellation_or_shortened_lease_without_freeing_hold(prepared_input, boundary):
    from sqlalchemy.exc import DBAPIError

    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await prepare(session, retained, proposal, registration, request)
    with engine.begin() as connection:
        if boundary == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
        else:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()+interval '10 seconds' WHERE id=:id"), {"id": registration.build_attempt.id})
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match=r"lease|assignment"):
            await prepare(session, retained, proposal, registration, request)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("boundary", ["proposal_id", "admission_incarnation", "manager_input_digest", "noncanonical", "execution_epoch"])
async def test_sql_boundary_rejects_non_contract_proposals(prepared_input, boundary):
    from hashlib import sha256

    from sqlalchemy.exc import DBAPIError

    sessions, engine, retained, proposal, registration, request = prepared_input
    payload = json.loads(canonical_executable_bytes(proposal))
    if boundary == "execution_epoch":
        del payload["shapes"][0]["binding"]["execution"]["execution_epoch"]
    elif boundary != "noncanonical":
        del payload[boundary]
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    if boundary == "noncanonical":
        wire = b" " + wire
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match=r"contract|canonical|field"):
            await session.execute(text("""SELECT loom_capacity_build_guard.prepare_plan(
                :installation, CAST(:payload AS jsonb), :wire, :digest, CAST(:sources AS jsonb))
            """), {"installation": retained.id, "payload": wire.decode("ascii"), "wire": wire,
                "digest": sha256(wire).hexdigest(),
                "sources": json.dumps({str(request.id): canonical_build_source(registration).decode("ascii")})})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0


async def test_every_protocol_object_requires_its_canonical_field_set(prepared_input):
    from copy import deepcopy
    from hashlib import sha256

    from sqlalchemy.exc import DBAPIError

    sessions, engine, retained, proposal, registration, request = prepared_input
    original = json.loads(canonical_executable_bytes(proposal))
    paths = ((), ("shapes", 0), ("shapes", 0, "binding"),
        ("shapes", 0, "binding", "execution"), ("allowances", 0))
    async with sessions.begin() as session:
        for path in paths:
            target = original
            for segment in path:
                target = target[segment]
            for field in (*target, "unexpected"):
                payload = deepcopy(original)
                changed = payload
                for segment in path:
                    changed = changed[segment]
                if field == "unexpected":
                    changed[field] = "not-authority"
                else:
                    del changed[field]
                wire = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
                with pytest.raises(DBAPIError):
                    async with session.begin_nested():
                        await session.execute(text("""SELECT loom_capacity_build_guard.prepare_plan(
                            :installation, CAST(:payload AS jsonb), :wire, :digest, CAST(:sources AS jsonb))
                        """), {"installation": retained.id, "payload": wire.decode("ascii"), "wire": wire,
                            "digest": sha256(wire).hexdigest(),
                            "sources": json.dumps({str(request.id): canonical_build_source(registration).decode("ascii")})})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0
