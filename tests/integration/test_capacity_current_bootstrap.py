"""Current pre-registration evidence must not inherit historical replay authority."""

import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.executable_admission import ExecutableAdmissionStore
from loom_capacity_manager.executable_contracts import canonical_executable_digest
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


async def _prepared(database):
    fence, registration = await _initialize_and_register(database)
    request = await _protect_bootstrap(database, registration, bootstrap_sha256=_DIGEST)
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
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        observed = await store.observe_current_bootstrap(physical)
        assert observed.physical_binding == physical
        assert observed.request_digest == canonical_executable_digest(physical)
        assert observed.agent_incarnation == registration.agent_incarnation
        assert observed.bootstrap_sha256 == _DIGEST
        assert before <= observed.observed_at < observed.bootstrap_expires_at
        assert observed.observation_state == "current-unused-bootstrap"
        again = await store.observe_current_bootstrap(physical)
        assert again.observed_at >= observed.observed_at
        # Historical physical replay remains unchanged; observation is not a new event.
        assert await store.bind_slurm_job(physical) == bound
        registered = await store.register_worker(_worker(request), bootstrap_capability=_CAPABILITY)
        assert registered.protected_high_water == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["operation_id", "slurm_job_id", "ownership_evidence_sha256"])
async def test_current_bootstrap_rejects_physical_substitution(capacity_guard_database, field):
    _, registration, _, physical, _ = await _prepared(capacity_guard_database)
    replacement = dict(operation_id=uuid4(), slurm_job_id="foreign-123", ownership_evidence_sha256="b" * 64)
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        with pytest.raises(DBAPIError, match="current unused bootstrap"):
            await store.observe_current_bootstrap(physical.model_copy(update={field: replacement[field]}))


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["registered", "withdrawn"])
async def test_current_bootstrap_rechecks_lifecycle_even_when_physical_replay_succeeds(
    capacity_guard_database, transition,
):
    _, registration, request, physical, bound = await _prepared(capacity_guard_database)
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.observe_current_bootstrap(physical)
        if transition == "registered":
            await store.register_worker(_worker(request), bootstrap_capability=_CAPABILITY)
        else:
            await store.withdraw_unregistered_worker(_withdrawal(request))
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        assert await store.bind_slurm_job(physical) == bound
        with pytest.raises(DBAPIError, match="current unused bootstrap"):
            await store.observe_current_bootstrap(physical)


@pytest.mark.asyncio
async def test_current_bootstrap_rejects_stale_agent_binding(capacity_guard_database):
    fence, registration, _, physical, _ = await _prepared(capacity_guard_database)
    await _reconfigure_registration(capacity_guard_database, fence, registration)
    async with _serializable_executor_session(capacity_guard_database) as session:
        with pytest.raises(DBAPIError):
            await ExecutableAdmissionStore(session, registration=registration).observe_current_bootstrap(physical)


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
