"""Native outcomes close exact work, never grant publication or free capacity."""

from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_build_guard.demand_store import BuildGuardDemandStore
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_drain import drain_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def outcome_request(claim, *, result="artifact-ready"):
    protocol = import_module("loom_capacity_agent.build_admission")
    return protocol.BuildOutcomeRequestV1(claim=claim, operation_id=uuid4(), result=result,
        artifact=protocol.BuildArtifactV1(archive_sha256="a" * 64, archive_size_bytes=1024)
        if result == "artifact-ready" else None)


@pytest.mark.parametrize("result", ["artifact-ready", "failed", "cancelled"])
@pytest.mark.parametrize("drain_first", [False, True])
async def test_native_outcome_closes_work_but_preserves_hold_and_historical_drain(prepared_input, monkeypatch, result, drain_first):
    factory, engine, installation, *_ = prepared_input
    drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    historical = None
    if drain_first:
        async with factory.begin() as session:
            historical = await store(session, installation).begin_drain(drain)
    request = outcome_request(claim, result=result)
    async with factory.begin() as session:
        receipt = await store(session, installation).record_outcome(request, worker_credential=CREDENTIAL)
        assert receipt.request == request and receipt.request_digest == canonical_digest(request)
        assert receipt.live_claim_count == 0 and receipt.claim_high_water == 1
        assert receipt.executable is False
        with pytest.raises(DBAPIError, match="committed outcome"):
            await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
    async with factory.begin() as session:
        assert await store(session, installation).record_outcome(request, worker_credential=CREDENTIAL) == receipt
        assert await store(session, installation).read_outcome(claim) == receipt
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert report.fixed_claims == report.pending_unassigned == ()
        assert len(report.current_assignments) == 1
        drained = await store(session, installation).begin_drain(drain)
        if historical is not None:
            assert drained == historical and drained.live_claim_count == 1
        else:
            assert drained.live_claim_count == 0 and drained.claim_high_water == 1
        assert (await store(session, installation).observe_intent(claim.binding)).claim_high_water == 1
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_candidates WHERE status='building'")) == 1


@pytest.mark.parametrize("boundary", ["credential", "claim", "worker", "binding", "replay", "uncommitted"])
async def test_native_outcome_rejects_changed_or_uncommitted_authority(prepared_input, monkeypatch, boundary):
    factory, engine, installation, *_ = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    request = outcome_request(claim)
    if boundary != "uncommitted":
        async with factory.begin() as session:
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    if boundary == "replay":
        async with factory.begin() as session:
            await store(session, installation).record_outcome(request, worker_credential=CREDENTIAL)
        request = request.model_copy(update={"operation_id": uuid4()})
    elif boundary in {"claim", "worker", "binding"}:
        changes = {"operation_id": uuid4()} if boundary == "claim" else {"worker_incarnation": uuid4()} if boundary == "worker" else {
            "binding": claim.binding.model_copy(update={"account_id": "foreign"})}
        request = request.model_copy(update={"claim": claim.model_copy(update=changes)})
    async with factory.begin() as session:
        if boundary == "uncommitted":
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).record_outcome(request,
                worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == int(boundary == "replay")


@pytest.mark.parametrize("boundary", ["cancelled", "expired"])
async def test_native_outcome_after_source_expiry_is_only_historical_evidence(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    _drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    with engine.begin() as connection:
        if boundary == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
        else:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
    async with factory.begin() as session:
        receipt = await store(session, installation).record_outcome(outcome_request(claim), worker_credential=CREDENTIAL)
        assert receipt.request.result == "artifact-ready" and not receipt.executable
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_outcome_corrupt_receipt_rollback_and_immutable_history(prepared_input, monkeypatch, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    _drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    request = outcome_request(claim)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(statement, *args, **kwargs):
            result = await original(statement, *args, **kwargs)
            return result.replace(str(request.operation_id), str(uuid4())) if "record_outcome(" in str(statement) else result

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await store(session, installation).record_outcome(request, worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        assert await store(session, installation).read_outcome(claim) is None
        await store(session, installation).record_outcome(request, worker_credential=CREDENTIAL)
        with pytest.raises(DBAPIError, match="committed outcome"):
            await store(session, installation).read_outcome(claim)
    for statement in ("UPDATE loom_capacity_build_guard.platform_outcomes SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.platform_outcomes", "TRUNCATE loom_capacity_build_guard.platform_outcomes"):
        with engine.begin() as connection, pytest.raises(DBAPIError, match="append-only"):
            connection.execute(text(statement))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0018")
