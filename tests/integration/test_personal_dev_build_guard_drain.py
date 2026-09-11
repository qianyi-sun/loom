"""A registered native worker drains without freeing its live claim or hold."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutableDrainRequestV2
from tests.integration.test_personal_dev_build_guard_claims import claim_input
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


async def drain_input(values, monkeypatch, *, claimed):
    claim = await claim_input(values, monkeypatch)
    factory, _engine, installation, *_ = values
    if claimed:
        async with factory.begin() as session:
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    return ExecutableDrainRequestV2(operation_id=uuid4(), binding=claim.binding,
        worker_id=claim.worker_id, worker_incarnation=claim.worker_incarnation,
        expected_claim_high_water=int(claimed), drain_epoch=3), claim


@pytest.mark.parametrize("claimed", [False, True])
async def test_registered_drain_fences_new_claims_without_releasing_capacity(prepared_input, monkeypatch, claimed):
    from loom_capacity_build_guard.demand_store import BuildGuardDemandStore

    factory, engine, installation, *_ = prepared_input
    request, claim = await drain_input(prepared_input, monkeypatch, claimed=claimed)
    async with factory.begin() as session:
        receipt = await store(session, installation).begin_drain(request)
        assert receipt.worker_id == claim.worker_id and receipt.worker_incarnation == claim.worker_incarnation
        assert receipt.claim_high_water == receipt.live_claim_count == int(claimed)
        assert receipt.drain_epoch == 3
        with pytest.raises(DBAPIError, match="committed drain"):
            await store(session, installation).observe_intent(request.binding)
    async with factory.begin() as session:
        assert await store(session, installation).begin_drain(request) == receipt
        assert (await store(session, installation).observe_intent(request.binding)).drain == receipt
        if claimed:
            # Exact claim replay is evidence recovery only, never a renewed lease.
            assert (await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)).request == claim
        else:
            with pytest.raises(DBAPIError, match="drain"):
                await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert len(report.current_assignments) == 1 and len(report.fixed_claims) == int(claimed)
        assert report.pending_unassigned == ()
        if claimed:
            assert report.fixed_claims[0].state == "cancel-pending"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_drains")) == 1


@pytest.mark.parametrize("boundary", ["cancelled", "expired"])
async def test_registered_drain_does_not_require_fresh_source(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    request, _claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    with engine.begin() as connection:
        if boundary == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
        else:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
    async with factory.begin() as session:
        receipt = await store(session, installation).begin_drain(request)
        assert receipt.live_claim_count == 1


@pytest.mark.parametrize("boundary", ["worker", "binding", "water", "epoch", "replay"])
async def test_registered_drain_rejects_changed_authority(prepared_input, monkeypatch, boundary):
    factory, engine, installation, *_ = prepared_input
    request, _claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    if boundary == "replay":
        async with factory.begin() as session:
            await store(session, installation).begin_drain(request)
        request = request.model_copy(update={"operation_id": uuid4()})
    elif boundary == "worker":
        request = request.model_copy(update={"worker_incarnation": uuid4()})
    elif boundary == "binding":
        request = request.model_copy(update={"binding": request.binding.model_copy(update={"account_id": "foreign"})})
    elif boundary == "water":
        request = request.model_copy(update={"expected_claim_high_water": 0})
    else:
        request = request.model_copy(update={"drain_epoch": 2})
    async with factory.begin() as session:
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).begin_drain(request)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_drains")) == int(boundary == "replay")
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_native_claim_and_drain_serialize_without_late_work(prepared_input, monkeypatch):
    import asyncio

    factory, _engine, installation, *_ = prepared_input
    request, claim = await drain_input(prepared_input, monkeypatch, claimed=False)
    ready = [asyncio.Event(), asyncio.Event()]

    async def compete(index):
        try:
            async with factory.begin() as session:
                ready[index].set()
                await ready[1-index].wait()
                if index:
                    await store(session, installation).begin_drain(request)
                else:
                    await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
            return True
        except DBAPIError:
            return False

    claimed, drained = await asyncio.gather(compete(0), compete(1))
    assert claimed != drained
    async with factory.begin() as session:
        observed = await store(session, installation).observe_intent(request.binding)
        assert observed.claim_high_water == int(claimed)
        if not drained:
            request = request.model_copy(update={"expected_claim_high_water": observed.claim_high_water})
            await store(session, installation).begin_drain(request)
    async with factory.begin() as session:
        observed = await store(session, installation).observe_intent(request.binding)
        assert observed.drain is not None and observed.drain.live_claim_count == int(claimed)


async def test_corrupt_drain_receipt_rolls_back_fence_and_downgrade_refuses_evidence(prepared_input, monkeypatch, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    request, _claim = await drain_input(prepared_input, monkeypatch, claimed=False)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(statement, *args, **kwargs):
            result = await original(statement, *args, **kwargs)
            if "begin_drain(" in str(statement):
                return result.replace(str(request.worker_id), str(uuid4()))
            return result

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await store(session, installation).begin_drain(request)
    async with factory.begin() as session:
        assert (await store(session, installation).observe_intent(request.binding)).drain is None
        await store(session, installation).begin_drain(request)
    for statement in ("UPDATE loom_capacity_build_guard.worker_drains SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.worker_drains", "TRUNCATE loom_capacity_build_guard.worker_drains CASCADE"):
        with engine.begin() as connection, pytest.raises(DBAPIError, match="append-only"):
            connection.execute(text(statement))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0017")


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path", "helper"])
def test_native_drain_privilege_drift_is_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.begin_drain(uuid,jsonb,bytea,text)"
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
        "helper": "ALTER FUNCTION loom_capacity_build_guard.native_worker_drain(uuid) SECURITY DEFINER"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
