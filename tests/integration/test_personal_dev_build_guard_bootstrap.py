"""Hash-only bootstrap precedes admission and cannot grant a build capability."""

from datetime import UTC, datetime, timedelta
from importlib import import_module

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapProposalV2,
    canonical_executable_bytes,
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


def bootstrap(proposal):
    return ExecutableBootstrapProposalV2(binding=proposal.shapes[0].binding,
        command_sequence=2, proposal_epoch=1, bootstrap_sha256="a"*64,
        expires_at=datetime.now(UTC)+timedelta(minutes=2))


async def test_bootstrap_is_durable_before_acknowledgement_and_needs_no_assignment(prepared_input):
    store_type = import_module("loom_capacity_build_guard.bootstrap_store").BuildGuardBootstrapStore
    sessions, engine, installation, plan, _source, _request = prepared_input
    proposal = bootstrap(plan)
    async with sessions.begin() as session:
        registered = await store_type(session, installation=installation).register(proposal)
        assert registered.proposal == proposal
        assert registered.bootstrap_registration_epoch == 1
        assert registered.executable is False
        with pytest.raises(DBAPIError, match="committed"):
            await store_type(session, installation=installation).authorize_publication(proposal.binding.intent_id)
    async with sessions.begin() as session:
        assert await store_type(session, installation=installation).register(proposal) == registered
        published = await store_type(session, installation=installation).authorize_publication(proposal.binding.intent_id)
    assert published.acknowledgement.binding == proposal.binding
    assert published.acknowledgement.bootstrap_evidence_sha256 == registered.digest
    assert published.acknowledgement.proposal_digest == canonical_executable_digest(proposal)
    async with sessions.begin() as session:
        assert await store_type(session, installation=installation).authorize_publication(proposal.binding.intent_id) == published
    with engine.connect() as connection:
        for table in ("plans", "assignments", "request_holds"):
            assert connection.scalar(text(f"SELECT count(*) FROM loom_capacity_build_guard.{table}")) == 0
        for table in ("personal_dev_native_build_grants", "personal_dev_native_builder_agents"):
            assert connection.scalar(text(f"SELECT count(*) FROM {table}")) == 0


@pytest.mark.parametrize("field,value", [("bootstrap_sha256","b"*64), ("proposal_epoch",2)])
async def test_bootstrap_rejects_same_intent_rotation(prepared_input, field, value):
    store_type = import_module("loom_capacity_build_guard.bootstrap_store").BuildGuardBootstrapStore
    sessions, _engine, installation, plan, _source, _request = prepared_input
    proposal = bootstrap(plan)
    async with sessions.begin() as session:
        registered = await store_type(session, installation=installation).register(proposal)
    async with sessions.begin() as session:
        with pytest.raises((ValueError, DBAPIError)):
            await store_type(session, installation=installation).register(proposal.model_copy(update={field:value}))
        assert await store_type(session, installation=installation).register(proposal) == registered


@pytest.mark.parametrize("boundary", ["executable", "epoch", "account", "node", "candidate", "expiry"])
async def test_direct_sql_bootstrap_rejects_invalid_native_binding(prepared_input, boundary):
    sessions, _engine, installation, plan, _source, _request = prepared_input
    proposal = bootstrap(plan)
    if boundary == "executable":
        proposal = proposal.model_copy(update={"executable":False})
    elif boundary == "epoch":
        proposal = proposal.model_copy(update={"proposal_epoch":2})
    elif boundary == "expiry":
        proposal = proposal.model_copy(update={"expires_at":datetime.now(UTC)-timedelta(seconds=1)})
    else:
        updates = {"account_id":"dev-owner-"+"0"*32} if boundary=="account" else (
            {"node_ids":("gb10-2",)} if boundary=="node" else
            {"candidate":proposal.binding.candidate.model_copy(update={"publication_sha256":"b"*64})})
        proposal = proposal.model_copy(update={"binding":proposal.binding.model_copy(update=updates)})
    wire = canonical_executable_bytes(proposal)
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="bootstrap"):
            async with session.begin_nested():
                await session.scalar(text("SELECT loom_capacity_build_guard.register_bootstrap(:id,CAST(:payload AS jsonb),:wire,:digest)"),
                    {"id":installation.id,"payload":wire.decode("ascii"),"wire":wire,"digest":canonical_executable_digest(proposal)})


async def test_expired_hash_is_retained_but_cannot_authorize_or_rotate(prepared_input):
    import asyncio

    store_type = import_module("loom_capacity_build_guard.bootstrap_store").BuildGuardBootstrapStore
    sessions, engine, installation, plan, _source, _request = prepared_input
    proposal = bootstrap(plan).model_copy(update={"expires_at":datetime.now(UTC)+timedelta(seconds=2)})
    async with sessions.begin() as session:
        original = await store_type(session, installation=installation).register(proposal)
    async with asyncio.timeout(5):
        while datetime.now(UTC) < proposal.expires_at:
            await asyncio.sleep(0.05)
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="expiry"):
            await store_type(session, installation=installation).authorize_publication(proposal.binding.intent_id)
        replacement = proposal.model_copy(update={"expires_at":datetime.now(UTC)+timedelta(minutes=2), "bootstrap_sha256":"b"*64})
        with pytest.raises(DBAPIError, match="rotation"):
            await store_type(session, installation=installation).register(replacement)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT payload_sha256 FROM loom_capacity_build_guard.bootstraps")) == original.digest


async def test_bootstrap_response_validation_rolls_back_hash_retention(prepared_input, monkeypatch):
    import json

    store_type = import_module("loom_capacity_build_guard.bootstrap_store").BuildGuardBootstrapStore
    sessions, engine, installation, plan, _source, _request = prepared_input
    async with sessions.begin() as session:
        scalar = session.scalar

        async def corrupt(*args, **kwargs):
            result = json.loads(await scalar(*args, **kwargs))
            result["proposal"]["bootstrap_sha256"] = "b"*64
            return json.dumps(result, sort_keys=True, separators=(",",":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError):
            await store_type(session, installation=installation).register(bootstrap(plan))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.bootstraps")) == 0


@pytest.mark.parametrize("signature", ["register_bootstrap(uuid,jsonb,bytea,text)", "authorize_bootstrap_publication(uuid,uuid)"])
@pytest.mark.parametrize("boundary", ["missing", "execute", "search-path", "grant-option"])
def test_bootstrap_requires_exact_entrypoint_privileges(build_guard_database, signature, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = f"loom_capacity_build_guard.{signature}"
    quoted = engine.dialect.identifier_preparer.quote(agent)
    with engine.begin() as connection:
        if boundary == "missing":
            connection.exec_driver_sql(f"DROP FUNCTION {signature}")
        elif boundary == "execute":
            connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {signature} FROM {quoted}")
        elif boundary == "search-path":
            connection.exec_driver_sql(f"ALTER FUNCTION {signature} SET search_path=public")
        else:
            connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {signature} TO {quoted} WITH GRANT OPTION")
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


@pytest.mark.parametrize("failure", ["lost", "wrong-receipt"])
async def test_coordinator_replays_committed_bootstrap_through_real_reporter_client(prepared_input, failure):
    import httpx

    from loom_capacity_agent.client import DemandPublishError, DemandReporterClient
    from loom_capacity_manager.executable_contracts import ExecutableBootstrapAcknowledgementV2
    from tests.unit.test_capacity_agent_client import _configuration

    coordinator_type = import_module("loom_capacity_build_guard.bootstrap_store").BuildBootstrapCoordinator
    sessions, engine, installation, plan, _source, _request = prepared_input
    proposal = bootstrap(plan)
    configuration = _configuration().model_copy(update={
        "subject_id":installation.document.subject_id,"subject_incarnation":installation.document.subject_incarnation,
        "deployment_generation":installation.document.deployment_generation,
        "reporter_incarnation":installation.document.reporter_incarnation,
        "protected_admission_sha256":installation.document.protected_admission_sha256})
    delivered = []

    async def handle(outgoing):
        delivered.append((outgoing.content,outgoing.headers["Idempotency-Key"]))
        acknowledgement = ExecutableBootstrapAcknowledgementV2.model_validate_json(outgoing.content)
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.bootstraps")) == 1
        if len(delivered)==1 and failure=="lost":
            raise httpx.ReadTimeout("reply lost",request=outgoing)
        return httpx.Response(200,json={"intent_id":str(proposal.binding.intent_id),"bootstrap_registration_epoch":1,
            "receipt_digest":"0"*64 if len(delivered)==1 else canonical_executable_digest(acknowledgement),
            "replayed":len(delivered)>1,"executable":True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        publisher = DemandReporterClient(configuration,manager_origin="https://manager.example",bearer_token="test-token",http_client=http)
        coordinator = coordinator_type(sessions,installation=installation,publisher=publisher)
        await coordinator.register(proposal)
        with pytest.raises(DemandPublishError):
            await coordinator.publish(proposal.binding.intent_id)
        restarted = coordinator_type(sessions,installation=installation,publisher=publisher)
        receipt = await restarted.publish(proposal.binding.intent_id)
    assert receipt.replayed
    assert delivered[0] == delivered[1]
