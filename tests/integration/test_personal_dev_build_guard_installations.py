"""Protected installation facts must survive replay without granting admission."""

from importlib import import_module

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.unit.test_personal_dev_build_admission import admission_input


@pytest.fixture
async def owner_sessions(build_guard_database):
    config, engine, owner, _agent, _url = build_guard_database
    command.upgrade(config, "head")
    database = create_async_engine(engine.url.set(drivername="postgresql+psycopg"), isolation_level="SERIALIZABLE")
    try:
        yield async_sessionmaker(database), owner
    finally:
        await database.dispose()


async def test_installation_replays_across_capacity_only_updates(owner_sessions, tmp_path):
    store_type = import_module("loom_capacity_build_guard.installation_store").BuildGuardInstallationStore
    sessions, owner = owner_sessions
    values = admission_input(tmp_path)
    member, runtime = values["member"], values["runtime"]
    async with sessions.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        first = await store_type(session, expected_owner_role=owner).retain(member=member, runtime=runtime)
    updated = member.model_copy(update={"revision": member.revision + 1,
        "configuration": member.configuration.model_copy(update={"configuration_generation": 2, "max_slots": 1}),
        "acknowledgement": member.acknowledgement.model_copy(update={"configuration_generation": 2})})
    async with sessions.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        store = store_type(session, expected_owner_role=owner)
        assert await store.retain(member=updated, runtime=runtime) == first
        assert await store.read(first.id) == first
        assert await session.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.installations")) == 1
        for table in ("plans", "assignments", "request_holds", "dispositions"):
            assert await session.scalar(text(f"SELECT count(*) FROM loom_capacity_build_guard.{table}")) == 0


@pytest.mark.parametrize("boundary", ["runtime", "reporter", "owner", "candidate-generation"])
async def test_installation_rejects_same_deployment_rebinding(owner_sessions, tmp_path, boundary):
    from dataclasses import replace
    from uuid import uuid4

    store_type = import_module("loom_capacity_build_guard.installation_store").BuildGuardInstallationStore
    sessions, owner = owner_sessions
    values = admission_input(tmp_path)
    member, runtime = values["member"], values["runtime"]
    async with sessions.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        first = await store_type(session, expected_owner_role=owner).retain(member=member, runtime=runtime)
    if boundary == "runtime":
        runtime = replace(runtime, release_evidence_sha256="f" * 64)
    elif boundary == "reporter":
        identity = uuid4()
        member = member.model_copy(update={
            "configuration": member.configuration.model_copy(update={"demand_reporter_incarnation": identity}),
            "acknowledgement": member.acknowledgement.model_copy(update={"reporter_incarnation": identity})})
    elif boundary == "owner":
        identity = uuid4()
        member = member.model_copy(update={"owner_id": identity,
            "configuration": member.configuration.model_copy(update={"account_id": f"dev-owner-{identity.hex}"})})
    else:
        member = member.model_copy(update={"configuration": member.configuration.model_copy(update={"candidate_generation": 2})})
    async with sessions.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        store = store_type(session, expected_owner_role=owner)
        with pytest.raises(ValueError, match="binding|replay"):
            await store.retain(member=member, runtime=runtime)
        assert await store.read(first.id) == first


async def test_installation_requires_owner_transaction(owner_sessions, tmp_path):
    store_type = import_module("loom_capacity_build_guard.installation_store").BuildGuardInstallationStore
    sessions, owner = owner_sessions
    values = admission_input(tmp_path)
    async with sessions() as session:
        store = store_type(session, expected_owner_role=owner)
        with pytest.raises(ValueError, match="transaction"):
            await store.retain(member=values["member"], runtime=values["runtime"])
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="owner"):
            await store_type(session, expected_owner_role=owner).retain(member=values["member"], runtime=values["runtime"])


async def test_installation_readback_rejects_noncanonical_owner_evidence(owner_sessions, tmp_path):
    from hashlib import sha256

    store_type = import_module("loom_capacity_build_guard.installation_store").BuildGuardInstallationStore
    sessions, owner = owner_sessions
    values = admission_input(tmp_path)
    async with sessions.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        store = store_type(session, expected_owner_role=owner)
        first = await store.retain(member=values["member"], runtime=values["runtime"])
        # Owner corruption is not runtime authority; even valid JSON with a
        # recomputed hash must not be accepted if its canonical wire changed.
        await session.execute(text("ALTER TABLE loom_capacity_build_guard.installations DISABLE TRIGGER immutable"))
        wire = b" " + first.wire_payload
        await session.execute(text("UPDATE loom_capacity_build_guard.installations SET wire_payload=:wire, payload_sha256=:digest"),
            {"wire": wire, "digest": sha256(wire).hexdigest()})
        await session.execute(text("ALTER TABLE loom_capacity_build_guard.installations ENABLE TRIGGER immutable"))
        with pytest.raises(ValueError, match="canonical"):
            await store.read(first.id)
