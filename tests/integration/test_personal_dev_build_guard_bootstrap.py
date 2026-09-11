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
