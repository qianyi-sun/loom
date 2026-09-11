"""The management adapter consumes real protected SQL receipts, not synthetic trials."""

from importlib import import_module

import pytest
from sqlalchemy import text

from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def test_agent_store_replays_typed_durable_plan_without_publication(prepared_input):
    store_type = import_module("loom_capacity_build_guard.plan_store").BuildGuardPlanStore
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        first = await store_type(session, installation=retained).prepare(proposal,
            sources={request.id: registration})
    async with sessions.begin() as session:
        replay = await store_type(session, installation=retained).prepare(proposal,
            sources={request.id: registration})
    assert replay == first
    assert len(first.digest) == 64
    assert first.proposal == proposal
    assert first.installation_id == retained.id
    assert len(first.assignments) == 1
    assert first.assignments[0].request_id == request.id
    assert first.assignments[0].execution_generation == registration.build_attempt.lease_epoch
    assert first.assignments[0].request_sequence == 1
    assert first.assignments[0].runtime_installation_sha256 == request.runtime_installation_sha256
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0


async def test_agent_store_requires_outer_transaction_and_complete_source_set(prepared_input):
    store_type = import_module("loom_capacity_build_guard.plan_store").BuildGuardPlanStore
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions() as session:
        with pytest.raises(ValueError, match="transaction"):
            await store_type(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="source"):
            await store_type(session, installation=retained).prepare(proposal, sources={})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0
