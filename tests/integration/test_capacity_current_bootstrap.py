"""Current pre-registration evidence must not inherit historical replay authority."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom_capacity_agent.executable_admission import (
    ExecutableAdmissionError,
    ExecutableAdmissionStore,
)
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.integration.test_capacity_agent_executable_admission import (
    _initialize_and_register,
    _physical,
    _protect_bootstrap,
    _reconfigure_registration,
    _serializable_executor_session,
    _value,
    _withdrawal,
    _worker,
)

_CAPABILITY = "current-bootstrap-test-capability"
_DIGEST = hashlib.sha256(_CAPABILITY.encode()).hexdigest()


async def _observe(database, registration, physical, *, isolation="SERIALIZABLE"):
    engine = create_async_engine(_value(database, "executor_url"), isolation_level=isolation)
    try:
        async with AsyncSession(engine) as session:
            receipt = await ExecutableAdmissionStore(session, registration=registration).observe_current_bootstrap(physical)
            assert not session.in_transaction()
            return receipt
    finally:
        await engine.dispose()


async def _prepared(database, *, short_lived=False):
    fence, registration = await _initialize_and_register(database)
    request = await _protect_bootstrap(
        database, registration, bootstrap_sha256=_DIGEST,
        expires_at=datetime.now(UTC) + timedelta(seconds=2) if short_lived else None,
    )
    physical = _physical(request)
    async with _serializable_executor_session(database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(request, bootstrap_sha256=_DIGEST)
        bound = await store.bind_slurm_job(physical)
    return fence, registration, request, physical, bound


@pytest.mark.asyncio
async def test_current_bootstrap_is_fresh_exact_and_does_not_consume_or_record_start(
    capacity_guard_database,
):
    _, registration, request, physical, bound = await _prepared(capacity_guard_database)
    before = datetime.now(UTC)
    observed = await _observe(capacity_guard_database, registration, physical)
    assert observed.physical_binding == physical
    assert observed.request_digest == canonical_executable_digest(physical)
    assert observed.agent_incarnation == registration.agent_incarnation
    assert observed.bootstrap_sha256 == _DIGEST
    assert before <= observed.observed_at < observed.bootstrap_expires_at
    assert observed.observation_state == "current-unused-bootstrap"
    with pytest.raises(ValidationError):
        type(observed).model_validate_json(json.dumps(observed.model_dump(mode="json") | {"executable": 0}))
    again = await _observe(capacity_guard_database, registration, physical)
    assert again.observed_at >= observed.observed_at
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        # Historical physical replay remains unchanged; observation is not a new event.
        assert await store.bind_slurm_job(physical) == bound
        registered = await store.register_worker(_worker(request), bootstrap_capability=_CAPABILITY)
        assert registered.protected_high_water == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["operation_id", "slurm_job_id", "ownership_evidence_sha256"])
async def test_current_bootstrap_rejects_physical_substitution(capacity_guard_database, field):
    _, registration, _, physical, _ = await _prepared(capacity_guard_database)
    replacement = dict(operation_id=uuid4(), slurm_job_id="foreign-123", ownership_evidence_sha256="b" * 64)
    with pytest.raises(DBAPIError, match="current unused bootstrap"):
        await _observe(capacity_guard_database, registration, physical.model_copy(update={field: replacement[field]}))


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["registered", "withdrawn"])
async def test_current_bootstrap_rechecks_lifecycle_even_when_physical_replay_succeeds(
    capacity_guard_database, transition,
):
    _, registration, request, physical, bound = await _prepared(capacity_guard_database)
    await _observe(capacity_guard_database, registration, physical)
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        if transition == "registered":
            await store.register_worker(_worker(request), bootstrap_capability=_CAPABILITY)
        else:
            await store.withdraw_unregistered_worker(_withdrawal(request))
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        assert await store.bind_slurm_job(physical) == bound
    with pytest.raises(DBAPIError, match="current unused bootstrap"):
        await _observe(capacity_guard_database, registration, physical)


@pytest.mark.asyncio
async def test_current_bootstrap_rejects_stale_agent_binding(capacity_guard_database):
    fence, registration, _, physical, _ = await _prepared(capacity_guard_database)
    await _reconfigure_registration(capacity_guard_database, fence, registration)
    with pytest.raises(DBAPIError):
        await _observe(capacity_guard_database, registration, physical)


def test_current_bootstrap_surface_is_owner_pinned_executor_only(capacity_guard_database):
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    signature = "loom_capacity_guard.observe_current_executable_bootstrap(uuid,uuid,jsonb,bytea,text)"
    try:
        with engine.connect() as connection:
            row = connection.execute(text(
                "SELECT pg_get_userbyid(proowner), prosecdef, proconfig "
                "FROM pg_proc WHERE oid = to_regprocedure(:signature)"
            ), {"signature": signature}).one()
            assert row == (_value(capacity_guard_database, "owner_role"), True, ["search_path=pg_catalog"])
            for role in ("executor_role", "agent_role", "observer_role", "runtime_role"):
                allowed = connection.execute(text(
                    "SELECT has_function_privilege(:role, :signature, 'EXECUTE')"
                ), {"role": _value(capacity_guard_database, role), "signature": signature}).scalar_one()
                assert allowed is (role == "executor_role")
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_current_bootstrap_refuses_an_already_started_serializable_snapshot(capacity_guard_database):
    _, registration, _, physical, _ = await _prepared(capacity_guard_database)
    async with _serializable_executor_session(capacity_guard_database) as session:
        # A serializable read-only transaction can legally serialize before a later
        # withdrawal. It cannot be reused to issue fresh preparation evidence.
        await session.execute(text("SELECT 1"))
        with pytest.raises(ExecutableAdmissionError, match="fresh owned transaction"):
            await ExecutableAdmissionStore(session, registration=registration).observe_current_bootstrap(physical)


@pytest.mark.asyncio
async def test_current_bootstrap_refuses_read_committed(capacity_guard_database):
    _, registration, _, physical, _ = await _prepared(capacity_guard_database)
    with pytest.raises(DBAPIError, match="SERIALIZABLE"):
        await _observe(capacity_guard_database, registration, physical, isolation="READ COMMITTED")


@pytest.mark.asyncio
async def test_current_bootstrap_refuses_an_external_serializable_snapshot(capacity_guard_database):
    _, registration, _, physical, _ = await _prepared(capacity_guard_database)
    engine = create_async_engine(_value(capacity_guard_database, "executor_url"), isolation_level="SERIALIZABLE")
    try:
        async with engine.connect() as connection, connection.begin():
            await connection.execute(text("SELECT 1"))
            async with AsyncSession(bind=connection, join_transaction_mode="create_savepoint") as session:
                assert not session.in_transaction()
                with pytest.raises(ExecutableAdmissionError, match="fresh owned transaction"):
                    await ExecutableAdmissionStore(session, registration=registration).observe_current_bootstrap(physical)
            assert connection.in_transaction()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_current_bootstrap_rejects_expiry_without_refreshing_historical_bind(capacity_guard_database):
    _, registration, _, physical, bound = await _prepared(capacity_guard_database, short_lived=True)
    observed = await _observe(capacity_guard_database, registration, physical)
    await asyncio.sleep(max(0, (observed.bootstrap_expires_at - datetime.now(UTC)).total_seconds()) + 0.05)
    async with _serializable_executor_session(capacity_guard_database) as session:
        assert await ExecutableAdmissionStore(session, registration=registration).bind_slurm_job(physical) == bound
    with pytest.raises(DBAPIError, match="current unused bootstrap"):
        await _observe(capacity_guard_database, registration, physical)


@pytest.mark.asyncio
async def test_current_bootstrap_rejects_superseded_bootstrap(capacity_guard_database):
    _, registration, request, physical, _ = await _prepared(capacity_guard_database)
    await _observe(capacity_guard_database, registration, physical)
    await _protect_bootstrap(
        capacity_guard_database, registration, bootstrap_sha256="b" * 64,
        request=request, proposal_epoch=2,
    )
    with pytest.raises(DBAPIError, match="current unused bootstrap"):
        await _observe(capacity_guard_database, registration, physical)


@pytest.mark.asyncio
async def test_current_bootstrap_does_not_wait_behind_admission_writer(capacity_guard_database):
    _, registration, _, physical, _ = await _prepared(capacity_guard_database)
    async with _serializable_executor_session(capacity_guard_database) as session:
        # Use the real executor operation to hold its admission lock, not a
        # privileged test-only mutation of protected state.
        await ExecutableAdmissionStore(session, registration=registration).bind_slurm_job(physical)
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            await asyncio.wait_for(_observe(capacity_guard_database, registration, physical), timeout=2)
    await _observe(capacity_guard_database, registration, physical)


@pytest.mark.asyncio
async def test_executor_client_uses_fresh_bounded_application_observation(capacity_guard_database):
    from loom_capacity_executor.admission_client import DatabaseExecutableAdmissionClient

    _, registration, request, physical, _ = await _prepared(capacity_guard_database)
    engine = create_async_engine(_value(capacity_guard_database, "executor_url"), isolation_level="SERIALIZABLE")
    async with DatabaseExecutableAdmissionClient(
        engine, subject_id=registration.subject_id, subject_incarnation=registration.subject_incarnation,
        statement_timeout_ms=1200, lock_timeout_ms=800,
    ) as client:
        first = await client.observe_current_bootstrap(physical)
        assert first.physical_binding == physical
        await client.withdraw_unregistered_worker(_withdrawal(request))
        with pytest.raises(DBAPIError, match="current unused bootstrap"):
            await client.observe_current_bootstrap(physical)


@pytest.mark.asyncio
async def test_sql_observation_never_refreshes_transaction_snapshot_age(capacity_guard_database):
    """Even a direct SQL caller gets the conservative age of its retained snapshot."""
    _, registration, request, physical, _ = await _prepared(capacity_guard_database)
    async with _serializable_executor_session(capacity_guard_database) as session:
        snapshot_time = (await session.execute(text("SELECT transaction_timestamp()"))).scalar_one()
        # A concurrent agent can publish a replacement while this SERIALIZABLE
        # transaction still legitimately sees the old bootstrap. That observation
        # must not be relabeled with a newer timestamp after replacement.
        await _protect_bootstrap(capacity_guard_database, registration, request=request,
            bootstrap_sha256="b" * 64, proposal_epoch=2)
        wire = canonical_executable_bytes(physical)
        observed = (await session.execute(text(
            "SELECT loom_capacity_guard.observe_current_executable_bootstrap("
            ":subject, :incarnation, CAST(:payload AS jsonb), CAST(:wire AS bytea), :digest)"
        ), {"subject": registration.subject_id, "incarnation": registration.subject_incarnation,
            "payload": wire.decode(), "wire": wire, "digest": canonical_executable_digest(physical)})).scalar_one()
        assert datetime.fromisoformat(observed["observed_at"]) == snapshot_time
    # The supported API owns a fresh snapshot and must see that replacement.
    with pytest.raises(DBAPIError, match="current unused bootstrap"):
        await _observe(capacity_guard_database, registration, physical)
