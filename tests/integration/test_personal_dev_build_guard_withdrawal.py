"""Bound bootstrap withdrawal never proves physical release or frees a hold."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutableWorkerWithdrawalRequestV2
from loom_capacity_build_guard.bootstrap_store import BuildGuardBootstrapStore
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.integration.test_personal_dev_build_guard_execution import admitted, physical, store
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def withdrawal(binding):
    return ExecutableWorkerWithdrawalRequestV2(operation_id=uuid4(), binding=binding.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2,
        slurm_job_id=binding.slurm_job_id, ownership_evidence_sha256=binding.ownership_evidence_sha256)


async def bound_input(values):
    factory, _engine, installation, *_ = values
    registration, digest = await admitted(values)
    async with factory.begin() as session:
        prepared = await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    binding = physical(registration)
    async with factory.begin() as session:
        bound = await store(session, installation).bind_slurm_job(binding)
    return registration, digest, prepared, binding, bound


async def test_withdrawal_survives_cancellation_and_retains_capacity_charge(prepared_input):
    factory, engine, installation, _plan, _source, request = prepared_input
    registration, digest, prepared, binding, bound = await bound_input(prepared_input)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id":request.id})
    withdraw = withdrawal(binding)
    async with factory.begin() as session:
        receipt = await store(session, installation).withdraw_unregistered_worker(withdraw)
        assert receipt.protected_high_water > bound.protected_high_water
        with pytest.raises(DBAPIError, match="committed"):
            await store(session, installation).observe_intent(binding.binding)
    assert receipt.withdrawal_digest == canonical_executable_digest(withdraw)
    async with factory.begin() as session:
        guard = store(session, installation)
        assert await guard.withdraw_unregistered_worker(withdraw) == receipt
        observed = await guard.observe_intent(binding.binding)
        assert observed.withdrawal == receipt
        assert observed.release is None and observed.prepared_revocation is None
        assert observed.worker_id is None
        assert await guard.prepare_worker(registration, bootstrap_sha256=digest) == prepared
        with pytest.raises(DBAPIError, match="revoked"):
            await guard.bind_slurm_job(binding)
        with pytest.raises(DBAPIError, match="revoked"):
            await BuildGuardBootstrapStore(session, installation=installation).authorize_publication(binding.binding.intent_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


@pytest.mark.parametrize("boundary", ["binding", "job", "ownership", "epoch", "operation"])
async def test_withdrawal_rejects_changed_physical_binding_or_replay(prepared_input, boundary):
    factory, _engine, installation, *_ = prepared_input
    _registration, _digest, _prepared, binding, _bound = await bound_input(prepared_input)
    request = withdrawal(binding)
    async with factory.begin() as session:
        original = await store(session, installation).withdraw_unregistered_worker(request)
    changes = {"binding":{"binding":request.binding.model_copy(update={"account_id":"foreign"})},
        "job":{"slurm_job_id":"9999"}, "ownership":{"ownership_evidence_sha256":"f"*64},
        "epoch":{"protected_registration_epoch":3}, "operation":{"operation_id":uuid4()}}
    async with factory.begin() as session:
        with pytest.raises((DBAPIError, ValueError)):
            await store(session, installation).withdraw_unregistered_worker(request.model_copy(update=changes[boundary]))
        assert await store(session, installation).withdraw_unregistered_worker(request) == original


async def test_withdrawal_requires_prior_physical_commit(prepared_input):
    factory, _engine, installation, *_ = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    binding = physical(registration)
    async with factory.begin() as session:
        guard = store(session, installation)
        with pytest.raises(DBAPIError, match="physical"):
            await guard.withdraw_unregistered_worker(withdrawal(binding))
        await guard.bind_slurm_job(binding)
        with pytest.raises(DBAPIError, match="committed"):
            await guard.withdraw_unregistered_worker(withdrawal(binding))


async def test_withdrawal_evidence_blocks_destructive_downgrade(prepared_input, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    _registration, _digest, _prepared, binding, _bound = await bound_input(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).withdraw_unregistered_worker(withdrawal(binding))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0011")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) == "build_guard_0012"
