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
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
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
    async with factory.begin() as session:
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


@pytest.mark.parametrize("result", ["artifact-ready", "failed", "cancelled"])
async def test_completed_request_fences_source_admission_but_failed_work_can_retry_after_release(prepared_input, monkeypatch, owner_sessions, result):
    from loom.personal_dev_build_platform_requests import canonical_build_source

    factory, _engine, installation, _plan, source, platform = prepared_input
    _drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    async with factory.begin() as session:
        await store(session, installation).record_outcome(outcome_request(claim, result=result), worker_credential=CREDENTIAL)
    owner_factory, role = owner_sessions
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {role}"))
        finished = await session.scalar(text("SELECT loom_capacity_build_guard.native_request_finished(:request)"), {"request": platform.id})
        assert finished is (result != "failed")
        wire = canonical_build_source(source)
        statement = text("""SELECT loom_capacity_build_guard.assert_current_source(
            :installation,:request,CAST(:payload AS jsonb),:wire,:digest)""")
        parameters = {"installation": installation.id, "request": platform.id, "payload": wire.decode("ascii"),
            "wire": wire, "digest": platform.source_binding_sha256}
        if result == "failed":
            assert await session.scalar(statement, parameters) is not None
        else:
            with pytest.raises(DBAPIError, match="live lease changed"):
                await session.scalar(statement, parameters)


async def test_native_outcome_and_drain_serialize_without_changing_old_receipt(prepared_input, monkeypatch):
    import asyncio

    factory, _engine, installation, *_ = prepared_input
    drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    outcome = outcome_request(claim)
    ready = [asyncio.Event(), asyncio.Event()]

    async def compete(index):
        try:
            async with factory.begin() as session:
                ready[index].set()
                await ready[1-index].wait()
                if index:
                    return await store(session, installation).begin_drain(drain)
                return await store(session, installation).record_outcome(outcome, worker_credential=CREDENTIAL)
        except DBAPIError as exc:
            assert getattr(exc.orig, "sqlstate", None) == "40001"
            return None

    completed, drained = await asyncio.gather(compete(0), compete(1))
    assert completed is not None or drained is not None
    async with factory.begin() as session:
        final_outcome = await store(session, installation).record_outcome(outcome, worker_credential=CREDENTIAL)
        if completed is not None:
            assert completed == final_outcome
    async with factory.begin() as session:
        final_drain = await store(session, installation).begin_drain(drain)
        if drained is not None:
            assert drained == final_drain
        else:
            assert final_drain.live_claim_count == 0
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert report.fixed_claims == () and len(report.current_assignments) == 1


async def test_migration_preserves_pre_outcome_drain_receipts(prepared_input, monkeypatch, build_guard_database):
    from alembic import command

    factory, _engine, installation, *_ = prepared_input
    # No outcome exists: downgrade preserves registration and claim history.
    command.downgrade(build_guard_database[0], "build_guard_0018")
    drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    async with factory.begin() as session:
        before = await store(session, installation).begin_drain(drain)
    command.upgrade(build_guard_database[0], "head")
    async with factory.begin() as session:
        assert await store(session, installation).begin_drain(drain) == before
        await store(session, installation).record_outcome(outcome_request(claim), worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        assert await store(session, installation).begin_drain(drain) == before


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path", "helper"])
def test_native_outcome_privilege_drift_is_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.record_outcome(uuid,jsonb,bytea,text,text)"
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
        "helper": "ALTER FUNCTION loom_capacity_build_guard.native_live_claim_count(uuid) SECURITY DEFINER"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


@pytest.mark.parametrize("boundary", ["success", "missing-result", "null-artifact", "failure-artifact", "extra-artifact", "negative-size", "bool-size", "overflow-size", "bad-digest", "credential"])
async def test_raw_sql_outcome_cannot_bypass_wrapper_contract(prepared_input, monkeypatch, boundary):
    import json
    from hashlib import sha256

    factory, engine, installation, *_ = prepared_input
    _drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    payload = outcome_request(claim).model_dump(mode="json")
    if boundary == "success":
        payload["result"] = "success"
    elif boundary == "missing-result":
        del payload["result"]
    elif boundary == "null-artifact":
        payload["artifact"] = None
    elif boundary == "failure-artifact":
        payload["result"] = "failed"
    elif boundary == "extra-artifact":
        payload["artifact"]["url"] = "https://foreign.invalid/artifact"
    elif boundary == "bad-digest":
        payload["artifact"]["archive_sha256"] = "z" * 64
    elif boundary != "credential":
        payload["artifact"]["archive_size_bytes"] = {"negative-size": -1, "bool-size": True, "overflow-size": 2**63}[boundary]
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            await session.scalar(text("""SELECT loom_capacity_build_guard.record_outcome(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:credential)"""),
                {"installation": installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": sha256(wire).hexdigest(), "credential": "x" * 64 if boundary == "credential" else sha256(CREDENTIAL.encode("ascii")).hexdigest()})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
