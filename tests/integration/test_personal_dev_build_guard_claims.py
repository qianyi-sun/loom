"""Native claims bind one allocated platform request and retain its slot charge."""

from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import (
    BOOTSTRAP,
    CREDENTIAL,
    registration_input,
)
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def claim_input(values, monkeypatch, *, expires_at=None):
    worker, _physical = await registration_input(values, monkeypatch, expires_at=expires_at)
    factory, _engine, installation, _plan, _source, platform = values
    async with factory.begin() as session:
        await store(session, installation).register_worker(worker, bootstrap_capability=BOOTSTRAP)
    return import_module("loom_capacity_agent.build_admission").BuildClaimRequestV1(
        binding=worker.binding, operation_id=uuid4(), request_id=platform.id,
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)


async def test_native_claim_is_exact_committed_fixed_demand(prepared_input, monkeypatch):
    from loom_capacity_build_guard.demand_store import BuildGuardDemandStore
    from loom_capacity_manager.contracts import canonical_digest

    factory, engine, installation, _plan, _source, platform = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        receipt = await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
        assert receipt.request == request and receipt.request_digest == canonical_digest(request)
        assert receipt.claim_high_water == 1
        with pytest.raises(DBAPIError, match="committed claim"):
            await store(session, installation).observe_intent(request.binding)
    async with factory.begin() as session:
        assert await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL) == receipt
        assert (await store(session, installation).observe_intent(request.binding)).claim_high_water == 1
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert report.pending_unassigned == ()
        assert len(report.fixed_claims) == len(report.current_assignments) == 1
        claim = report.fixed_claims[0]
        assert claim.claim_id == str(request.operation_id)
        assert claim.attempt_id == str(platform.id)
        assert claim.worker_identity == str(request.worker_incarnation)
        assert claim.pool_id == request.binding.pool_id
        assert claim.resources == request.binding.resources
        assert claim.concurrency_slots == 1 and claim.state == "live"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


@pytest.mark.parametrize("boundary", ["secret", "request", "worker", "incarnation", "binding", "cancelled", "expired"])
async def test_native_claim_rejects_changed_worker_or_current_authority(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    if boundary in {"request", "worker", "incarnation"}:
        field = {"request": "request_id", "worker": "worker_id", "incarnation": "worker_incarnation"}[boundary]
        request = request.model_copy(update={field: uuid4()})
    elif boundary == "binding":
        request = request.model_copy(update={"binding": request.binding.model_copy(update={"account_id": "foreign"})})
    elif boundary in {"cancelled", "expired"}:
        with engine.begin() as connection:
            if boundary == "cancelled":
                connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
            else:
                connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
    async with factory.begin() as session:
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).claim_platform(request,
                worker_credential="x" * 43 if boundary == "secret" else CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("boundary", ["cancelled", "expired"])
async def test_cancelled_claim_replay_keeps_charge_without_new_execution(prepared_input, monkeypatch, boundary):
    from loom_capacity_build_guard.demand_store import BuildGuardDemandStore

    factory, engine, installation, _plan, source, platform = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        receipt = await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
    with engine.begin() as connection:
        if boundary == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
        else:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
    async with factory.begin() as session:
        assert await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL) == receipt
        with pytest.raises(DBAPIError, match="replay"):
            await store(session, installation).claim_platform(request.model_copy(update={"operation_id": uuid4()}), worker_credential=CREDENTIAL)
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert report.pending_unassigned == () and len(report.fixed_claims) == 1
        assert report.fixed_claims[0].state == ("cancel-pending" if boundary == "cancelled" else "unknown")


async def test_claim_rollback_does_not_publish_worker_progress(prepared_input, monkeypatch):
    factory, _engine, installation, *_ = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    async with factory() as session:
        await session.begin()
        await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
        await session.rollback()
    async with factory.begin() as session:
        assert (await store(session, installation).observe_intent(request.binding)).claim_high_water == 0
        assert (await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)).claim_high_water == 1


async def test_native_claim_requires_prior_registration_commit(prepared_input, monkeypatch):
    from loom_capacity_agent.build_admission import BuildClaimRequestV1

    factory, _engine, installation, _plan, _source, platform = prepared_input
    worker, _physical = await registration_input(prepared_input, monkeypatch)
    request = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(), request_id=platform.id,
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)
    async with factory.begin() as session:
        await store(session, installation).register_worker(worker, bootstrap_capability=BOOTSTRAP)
        with pytest.raises(DBAPIError, match="committed worker"):
            await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)


async def test_native_worker_can_claim_after_exchange_deadline_with_valid_lease(prepared_input, monkeypatch):
    import asyncio
    from datetime import UTC, datetime, timedelta

    factory, engine, installation, *_ = prepared_input
    expiry = datetime.now(UTC) + timedelta(seconds=4)
    request = await claim_input(prepared_input, monkeypatch, expires_at=expiry)
    # The bootstrap deadline limits exchange, not the now-registered worker.
    async with asyncio.timeout(10):
        while True:
            with engine.connect() as observer:
                expired = observer.scalar(text("SELECT clock_timestamp() >= :expiry"), {"expiry": expiry})
            if expired:
                break
            await asyncio.sleep(0.02)
    async with factory.begin() as session:
        assert (await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)).claim_high_water == 1


async def test_uncommitted_claim_cannot_replace_retained_demand(prepared_input, monkeypatch):
    from loom_capacity_build_guard.demand_store import BuildGuardDemandStore

    factory, _engine, installation, *_ = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        before = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
        demand = BuildGuardDemandStore(session, installation=installation)
        with pytest.raises(DBAPIError, match="committed claim"):
            await demand.capture(configuration_generation=1)
        assert await demand.read_latest() == before
    async with factory.begin() as session:
        after = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert after.sequence == before.sequence + 1 and len(after.fixed_claims) == 1


async def test_native_claim_rejects_terminal_registered_job(prepared_input, monkeypatch):
    from tests.integration import test_personal_dev_build_guard_registration as registration_module
    from tests.integration.test_personal_dev_build_guard_terminal import (
        terminal_input,
        terminal_store,
    )

    evidence = []

    async def bind_with_terminal_proof(values):
        terminal, physical = await terminal_input(values)
        evidence.append(terminal)
        return None, None, None, physical, None

    monkeypatch.setattr(registration_module, "bound_input", bind_with_terminal_proof)
    request = await claim_input(prepared_input, monkeypatch)
    factory, _engine, installation, *_ = prepared_input
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(evidence[0])
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="already terminal"):
            await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)


async def test_corrupt_native_claim_receipt_rolls_back_inside_outer_commit(prepared_input, monkeypatch):
    factory, engine, installation, *_ = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(statement, *args, **kwargs):
            result = await original(statement, *args, **kwargs)
            if "claim_platform(" in str(statement):
                return result.replace(str(request.operation_id), str(uuid4()))
            return result

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == 0


async def test_claim_evidence_cannot_be_mutated_or_downgraded(prepared_input, monkeypatch, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
    for statement in ("UPDATE loom_capacity_build_guard.platform_claims SET request_id=request_id",
        "DELETE FROM loom_capacity_build_guard.platform_claims", "TRUNCATE loom_capacity_build_guard.platform_claims CASCADE"):
        with engine.begin() as connection, pytest.raises(DBAPIError, match="append-only"):
            connection.execute(text(statement))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0016")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) == "build_guard_0021"


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path", "helper"])
def test_native_claim_privilege_drift_is_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.claim_platform(uuid,jsonb,bytea,text,text)"
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
        "helper": "ALTER FUNCTION loom_capacity_build_guard.fixed_native_claims(uuid) SECURITY DEFINER"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
