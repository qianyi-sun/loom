"""An allocation-bound native credential cannot become an application worker."""

from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_withdrawal import bound_input, withdrawal
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions

BOOTSTRAP = "b" * 43
CREDENTIAL = "w" * 43


async def registration_input(values, monkeypatch):
    from tests.integration import test_personal_dev_build_guard_execution as execution

    original = execution.bootstrap
    monkeypatch.setattr(execution, "bootstrap", lambda proposal: original(proposal).model_copy(
        update={"bootstrap_sha256": sha256(BOOTSTRAP.encode("ascii")).hexdigest()}))
    _registration, _digest, _prepared, physical, _bound = await bound_input(values)
    request = ExecutableWorkerRegistrationV2(operation_id=uuid4(), binding=physical.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2, slurm_job_id=physical.slurm_job_id,
        worker_id=uuid4(), worker_incarnation=uuid4(), worker_credential_sha256=sha256(CREDENTIAL.encode("ascii")).hexdigest())
    return request, physical


async def test_native_registration_is_committed_observed_and_exactly_replayable(prepared_input, monkeypatch):
    factory, engine, installation, *_ = prepared_input
    request, physical = await registration_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        receipt = await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP)
        assert receipt.worker_id == request.worker_id
        assert receipt.worker_incarnation == request.worker_incarnation
        with pytest.raises(DBAPIError, match="committed registration"):
            await store(session, installation).observe_intent(request.binding)
    async with factory.begin() as session:
        assert await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP) == receipt
        observed = await store(session, installation).observe_intent(request.binding)
        assert observed.worker_id == request.worker_id and observed.worker_incarnation == request.worker_incarnation
        assert observed.protected_registration_epoch == 2 and observed.claim_high_water == 0
        assert observed.release is None
        with pytest.raises(DBAPIError, match="registered"):
            await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0
        wire = connection.scalar(text("SELECT convert_from(wire_payload,'UTF8') FROM loom_capacity_build_guard.worker_registrations"))
        assert BOOTSTRAP not in wire and CREDENTIAL not in wire


@pytest.mark.parametrize("boundary", ["secret", "job", "binding", "epoch", "predecessor", "cancelled", "withdrawn"])
async def test_native_registration_rejects_unowned_or_closed_bootstrap(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, _registration, platform_request = prepared_input
    request, physical = await registration_input(prepared_input, monkeypatch)
    if boundary == "job":
        request = request.model_copy(update={"slurm_job_id": "9999"})
    elif boundary == "binding":
        request = request.model_copy(update={"binding": request.binding.model_copy(update={"account_id": "foreign"})})
    elif boundary == "epoch":
        request = request.model_copy(update={"protected_registration_epoch": 3})
    elif boundary == "predecessor":
        request = request.model_copy(update={"predecessor_worker_incarnation": uuid4()})
    elif boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform_request.id})
    elif boundary == "withdrawn":
        async with factory.begin() as session:
            await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
    async with factory.begin() as session:
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).register_worker(request,
                bootstrap_capability="x" * 43 if boundary == "secret" else BOOTSTRAP)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_registration_replay_after_source_expiry_cannot_rotate_credential(prepared_input, monkeypatch):
    factory, engine, installation, _plan, registration, _request = prepared_input
    request, _physical = await registration_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        receipt = await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
            {"id": registration.build_attempt.id})
    async with factory.begin() as session:
        assert await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP) == receipt
        with pytest.raises(DBAPIError, match="replay"):
            await store(session, installation).register_worker(request.model_copy(update={"worker_credential_sha256": "f" * 64}),
                bootstrap_capability=BOOTSTRAP)
