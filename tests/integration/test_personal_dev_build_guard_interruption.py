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


async def test_interruption_receipt_corruption_rolls_back_and_retained_settlement_blocks_downgrade(prepared_input, monkeypatch, build_guard_database):
    from alembic import command

    factory, _engine, installation, *_ = prepared_input
    claim, evidence = await interrupted_input(prepared_input, monkeypatch)
    digest = canonical_executable_digest(evidence)
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(evidence)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(statement, *args, **kwargs):
            result = await original(statement, *args, **kwargs)
            if "settle_interrupted_claim(" in str(statement):
                return result.replace(digest, "f" * 64)
            return result

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await terminal_store(session, installation).settle_interrupted(claim, terminal_inventory_sha256=digest)
    async with factory.begin() as session:
        assert await store(session, installation).read_outcome(claim) is None
        await terminal_store(session, installation).settle_interrupted(claim, terminal_inventory_sha256=digest)
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0019")


async def test_worker_result_race_with_terminal_settlement_has_one_immutable_winner(prepared_input, monkeypatch):
    import asyncio

    factory, _engine, installation, *_ = prepared_input
    claim, evidence = await interrupted_input(prepared_input, monkeypatch)
    digest = canonical_executable_digest(evidence)
    request = outcome_request(claim)
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(evidence)
    ready = [asyncio.Event(), asyncio.Event()]

    async def compete(index):
        try:
            async with factory.begin() as session:
                ready[index].set()
                await ready[1-index].wait()
                if index:
                    return await terminal_store(session, installation).settle_interrupted(claim, terminal_inventory_sha256=digest)
                return await store(session, installation).record_outcome(request, worker_credential=CREDENTIAL)
        except DBAPIError as exc:
            assert getattr(exc.orig, "sqlstate", None) in {"40001", "P0001"}
            return None

    reported, recovered = await asyncio.gather(compete(0), compete(1))
    assert reported is not None or recovered is not None
    async with factory.begin() as session:
        final = await store(session, installation).read_outcome(claim)
        assert final is not None and final.request.result in {"artifact-ready", "interrupted"}
        for committed in (reported, recovered):
            if committed is not None:
                assert final == committed
        assert await terminal_store(session, installation).settle_interrupted(claim, terminal_inventory_sha256=digest) == final


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path"])
def test_interruption_authority_privileges_are_exact(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.settle_interrupted_claim(uuid,jsonb,bytea,text)"
    statement = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}[boundary]
    with engine.begin() as connection:
        connection.execute(text(statement))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
