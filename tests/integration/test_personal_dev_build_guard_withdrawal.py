"""Bound bootstrap withdrawal never proves physical release or frees a hold."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutableWorkerWithdrawalRequestV2
from loom_capacity_build_guard.bootstrap_store import BuildGuardBootstrapStore
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.integration.test_personal_dev_build_guard_execution import admitted, physical, store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
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
        assert connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) == "build_guard_0025"


@pytest.mark.parametrize("boundary", ["schema", "extra", "executable", "noncanonical", "claims", "physical"])
async def test_withdrawal_direct_sql_rejects_forged_contract(prepared_input, boundary):
    import json
    from hashlib import sha256

    from loom_capacity_manager.executable_contracts import canonical_executable_bytes

    factory, engine, installation, *_ = prepared_input
    _registration, _digest, _prepared, binding, _bound = await bound_input(prepared_input)
    payload = json.loads(canonical_executable_bytes(withdrawal(binding)))
    if boundary == "schema":
        payload["schema_version"] = 2.0
    elif boundary == "extra":
        payload["unknown"] = "ignored"
    elif boundary == "executable":
        payload["executable"] = False
    elif boundary == "claims":
        payload["expected_claim_high_water"] = 1
    elif boundary == "physical":
        payload["ownership_evidence_sha256"] = "f"*64
    wire = json.dumps(payload, sort_keys=True, separators=(",",":"), ensure_ascii=True).encode("ascii")
    if boundary == "noncanonical":
        wire += b" "
    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.withdraw_unregistered_worker(
                    :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                    {"installation":installation.id,"payload":wire.decode("ascii"),"wire":wire,
                        "digest":sha256(wire).hexdigest()})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_withdrawals")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_withdrawal_corrupt_receipt_rolls_back_before_outer_commit(prepared_input, monkeypatch):
    import json

    factory, engine, installation, *_ = prepared_input
    _registration, _digest, _prepared, binding, _bound = await bound_input(prepared_input)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(*args, **kwargs):
            payload = json.loads(await original(*args, **kwargs))
            payload["intent_id"] = str(uuid4())
            return json.dumps(payload, sort_keys=True, separators=(",",":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await store(session, installation).withdraw_unregistered_worker(withdrawal(binding))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_withdrawals")) == 0


async def test_expired_bound_bootstrap_and_source_lease_can_be_withdrawn(prepared_input):
    import asyncio
    from datetime import UTC, datetime, timedelta

    factory, engine, installation, _plan, source, _request = prepared_input
    expiry = datetime.now(UTC) + timedelta(seconds=2)
    registration, digest = await admitted(prepared_input, expires_at=expiry)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    binding = physical(registration)
    async with factory.begin() as session:
        await store(session, installation).bind_slurm_job(binding)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
            {"id":source.build_attempt.id})
    async with asyncio.timeout(5):
        while datetime.now(UTC) < expiry:
            await asyncio.sleep(0.05)
    request = withdrawal(binding)
    async with factory.begin() as session:
        receipt = await store(session, installation).withdraw_unregistered_worker(request)
    async with factory.begin() as session:
        assert await store(session, installation).withdraw_unregistered_worker(request) == receipt
        assert (await store(session, installation).observe_intent(binding.binding)).withdrawal == receipt
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_concurrent_withdrawal_retains_one_immutable_receipt(prepared_input):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    _registration, _digest, _prepared, binding, _bound = await bound_input(prepared_input)
    request = withdrawal(binding)

    async def compete():
        try:
            async with factory.begin() as session:
                receipt = await store(session, installation).withdraw_unregistered_worker(request)
            return receipt
        except DBAPIError as exc:
            # Serializable contenders may need exact replay, never a new request.
            assert exc.orig.sqlstate == "40001"
            return None

    receipts = [item for item in await asyncio.gather(compete(), compete()) if item is not None]
    assert receipts and all(item == receipts[0] for item in receipts)
    async with factory.begin() as session:
        assert await store(session, installation).withdraw_unregistered_worker(request) == receipts[0]
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text("SELECT * FROM loom_capacity_build_guard.worker_withdrawals"))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_withdrawals")) == 1
    for statement in (
        "UPDATE loom_capacity_build_guard.worker_withdrawals SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.worker_withdrawals",
        "TRUNCATE loom_capacity_build_guard.worker_withdrawals",
    ):
        with pytest.raises(DBAPIError):
            with engine.begin() as connection:
                connection.execute(text(statement))
