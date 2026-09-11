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


@pytest.mark.parametrize("boundary", ["proposal", "missing", "duplicate", "source", "sequence", "lease", "unknown"])
async def test_agent_store_rejects_corrupt_receipt_and_rolls_back_holds(prepared_input, monkeypatch, boundary):
    store_type = import_module("loom_capacity_build_guard.plan_store").BuildGuardPlanStore
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        scalar = session.scalar

        async def corrupt(*args, **kwargs):
            receipt = await scalar(*args, **kwargs)
            if boundary == "proposal":
                receipt["proposal_digest"] = "f" * 64
            elif boundary == "missing":
                receipt["assignments"] = []
            elif boundary == "duplicate":
                receipt["assignments"] *= 2
            elif boundary == "source":
                receipt["assignments"][0]["source_binding_sha256"] = "f" * 64
            elif boundary == "sequence":
                receipt["assignments"][0]["request_sequence"] = True
            elif boundary == "lease":
                receipt["assignments"][0]["lease_not_after_epoch_microseconds"] += 10**12
            else:
                receipt["assignments"][0]["unknown"] = "unexpected"
            return receipt

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError):
            await store_type(session, installation=retained).prepare(proposal, sources={request.id: registration})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
