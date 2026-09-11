"""A manager terminal witness settles lost work, never invents image success."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_build_guard.demand_store import BuildGuardDemandStore
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.integration import test_personal_dev_build_guard_registration as registration_module
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_outcomes import outcome_request
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_build_guard_terminal import terminal_input, terminal_store
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def interrupted_input(values, monkeypatch):
    witnesses = []

    async def retained_physical(inputs):
        evidence, physical = await terminal_input(inputs)
        witnesses.append(evidence)
        return None, None, None, physical, None

    # Reuse the real registered-worker flow with the existing exact signed job.
    monkeypatch.setattr(registration_module, "bound_input", retained_physical)
    claim = await claim_input(values, monkeypatch)
    factory, _engine, installation, *_ = values
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    return claim, witnesses[0]


async def test_terminal_interruption_closes_lost_claim_without_worker_secret_or_capacity_release(prepared_input, monkeypatch):
    factory, engine, installation, *_ = prepared_input
    claim, evidence = await interrupted_input(prepared_input, monkeypatch)
    digest = canonical_executable_digest(evidence)
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(evidence)
    async with factory.begin() as session:
        completed = await terminal_store(session, installation).settle_interrupted(claim, terminal_inventory_sha256=digest)
        assert completed.request.result == "interrupted" and not completed.executable
        assert completed.request.claim == claim and completed.request.terminal_inventory_sha256 == digest
        with pytest.raises(DBAPIError, match="committed outcome"):
            await store(session, installation).read_outcome(claim)
    async with factory.begin() as session:
        assert await terminal_store(session, installation).settle_interrupted(claim, terminal_inventory_sha256=digest) == completed
        assert await store(session, installation).read_outcome(claim) == completed
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert report.fixed_claims == report.pending_unassigned == () and len(report.current_assignments) == 1
        with pytest.raises(DBAPIError, match="replay"):
            await store(session, installation).record_outcome(outcome_request(claim), worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_candidates WHERE status='building'")) == 1


@pytest.mark.parametrize("boundary", ["absent", "uncommitted", "digest", "claim", "worker", "binding"])
async def test_interruption_requires_exact_committed_terminal_and_claim(prepared_input, monkeypatch, boundary):
    factory, engine, installation, *_ = prepared_input
    claim, evidence = await interrupted_input(prepared_input, monkeypatch)
    if boundary not in {"absent", "uncommitted"}:
        async with factory.begin() as session:
            await terminal_store(session, installation).import_evidence(evidence)
    if boundary == "claim":
        claim = claim.model_copy(update={"operation_id": uuid4()})
    elif boundary == "worker":
        claim = claim.model_copy(update={"worker_incarnation": uuid4()})
    elif boundary == "binding":
        claim = claim.model_copy(update={"binding": claim.binding.model_copy(update={"account_id": "foreign"})})
    async with factory.begin() as session:
        if boundary == "uncommitted":
            await terminal_store(session, installation).import_evidence(evidence)
        with pytest.raises((DBAPIError, ValueError)):
            await terminal_store(session, installation).settle_interrupted(claim,
                terminal_inventory_sha256="f" * 64 if boundary == "digest" else canonical_executable_digest(evidence))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 0


@pytest.mark.parametrize("result", ["artifact-ready", "failed", "cancelled"])
async def test_terminal_recovery_preserves_prior_worker_result(prepared_input, monkeypatch, result):
    factory, _engine, installation, *_ = prepared_input
    claim, evidence = await interrupted_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        reported = await store(session, installation).record_outcome(outcome_request(claim, result=result), worker_credential=CREDENTIAL)
        await terminal_store(session, installation).import_evidence(evidence)
    async with factory.begin() as session:
        recovered = await terminal_store(session, installation).settle_interrupted(claim,
            terminal_inventory_sha256=canonical_executable_digest(evidence))
        assert recovered == reported
