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


async def claim_input(values, monkeypatch):
    worker, _physical = await registration_input(values, monkeypatch)
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


async def test_cancelled_claim_replay_keeps_charge_without_new_execution(prepared_input, monkeypatch):
    from loom_capacity_build_guard.demand_store import BuildGuardDemandStore

    factory, engine, installation, _plan, _source, platform = prepared_input
    request = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        receipt = await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
    async with factory.begin() as session:
        assert await store(session, installation).claim_platform(request, worker_credential=CREDENTIAL) == receipt
        with pytest.raises(DBAPIError, match="replay"):
            await store(session, installation).claim_platform(request.model_copy(update={"operation_id": uuid4()}), worker_credential=CREDENTIAL)
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert report.pending_unassigned == () and len(report.fixed_claims) == 1
        assert report.fixed_claims[0].state == "cancel-pending"


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
