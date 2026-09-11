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


async def registration_input(values, monkeypatch, *, terminal=False, expires_at=None):
    from tests.integration import test_personal_dev_build_guard_execution as execution

    original = execution.bootstrap
    monkeypatch.setattr(execution, "bootstrap", lambda proposal: original(proposal).model_copy(
        update={"bootstrap_sha256": sha256(BOOTSTRAP.encode("ascii")).hexdigest(),
            **({"expires_at": expires_at} if expires_at is not None else {})}))
    if terminal:
        from tests.integration.test_personal_dev_build_guard_terminal import (
            terminal_input,
            terminal_store,
        )

        evidence, physical = await terminal_input(values)
        factory, _engine, installation, *_ = values
        async with factory.begin() as session:
            await terminal_store(session, installation).import_evidence(evidence)
    else:
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


@pytest.mark.parametrize("boundary", ["secret", "job", "binding", "epoch", "predecessor", "cancelled", "withdrawn", "terminal"])
async def test_native_registration_rejects_unowned_or_closed_bootstrap(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, _registration, platform_request = prepared_input
    request, physical = await registration_input(prepared_input, monkeypatch, terminal=boundary == "terminal")
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


async def test_registration_and_withdrawal_serialize_without_ambiguous_worker(prepared_input, monkeypatch):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    request, physical = await registration_input(prepared_input, monkeypatch)

    async def compete(register):
        for attempt in range(3):
            try:
                async with factory.begin() as session:
                    if register:
                        await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP)
                    else:
                        await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
                return True
            except DBAPIError as exc:
                if getattr(exc.orig, "sqlstate", None) == "40001" and attempt < 2:
                    continue
                assert "registered" in str(exc) or "revoked" in str(exc)
                return False
        raise AssertionError("serialization retries exhausted")

    outcomes = await asyncio.gather(compete(True), compete(False))
    assert sorted(outcomes) == [False, True]
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == outcomes[0]
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_withdrawals")) == outcomes[1]
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_registration_corrupt_receipt_rolls_back_credential(prepared_input, monkeypatch):
    import json

    factory, engine, installation, *_ = prepared_input
    request, _physical = await registration_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(*args, **kwargs):
            result = json.loads(await original(*args, **kwargs))
            result["worker_id"] = str(uuid4())
            return json.dumps(result, sort_keys=True, separators=(",", ":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == 0


async def test_registration_immutable_and_nondestructive_downgrade(prepared_input, monkeypatch, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    request, _physical = await registration_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP)
        with pytest.raises(DBAPIError, match="permission denied"):
            async with session.begin_nested():
                await session.execute(text("SELECT * FROM loom_capacity_build_guard.worker_registrations"))
    for mutation in ("UPDATE loom_capacity_build_guard.worker_registrations SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.worker_registrations", "TRUNCATE loom_capacity_build_guard.worker_registrations"):
        with engine.begin() as connection, pytest.raises(DBAPIError, match="append-only"):
            connection.execute(text(mutation))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0015")


@pytest.mark.parametrize("boundary", ["schema", "worker", "epoch", "secret"])
async def test_registration_sql_rejects_substituted_authority(prepared_input, monkeypatch, boundary):
    import json

    factory, engine, installation, *_ = prepared_input
    request, _physical = await registration_input(prepared_input, monkeypatch)
    payload = request.model_dump(mode="json")
    if boundary == "schema":
        payload["schema_version"] = "2"
    elif boundary == "worker":
        payload["worker_id"] = payload["worker_incarnation"]
    elif boundary == "epoch":
        payload["protected_registration_epoch"] = 3
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.register_worker(
                    :installation,CAST(:payload AS jsonb),:wire,:digest,:bootstrap)"""),
                    {"installation": installation.id, "payload": wire.decode("ascii"), "wire": wire,
                        "digest": sha256(wire).hexdigest(), "bootstrap": "f" * 64 if boundary == "secret" else sha256(BOOTSTRAP.encode("ascii")).hexdigest()})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == 0


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path", "helper"])
def test_registration_callable_privilege_drift_is_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.register_worker(uuid,jsonb,bytea,text,text)"
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
        "helper": "ALTER FUNCTION loom_capacity_build_guard.observe_registered_worker(uuid,jsonb) SECURITY DEFINER"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


async def test_registration_rechecks_bootstrap_expiry_after_source_lock_wait(prepared_input, monkeypatch):
    import asyncio
    from datetime import UTC, datetime, timedelta

    factory, engine, installation, _plan, registration, *_ = prepared_input
    expiry = datetime.now(UTC) + timedelta(seconds=5)
    request, _physical = await registration_input(prepared_input, monkeypatch, expires_at=expiry)
    connected = asyncio.Event()
    pid = None

    async def exchange():
        nonlocal pid
        async with factory.begin() as session:
            pid = await session.scalar(text("SELECT pg_backend_pid()"))
            connected.set()
            return await store(session, installation).register_worker(request, bootstrap_capability=BOOTSTRAP)

    task = None
    try:
        async with asyncio.timeout(15):
            with engine.begin() as blocker:
                blocker.execute(text("SELECT id FROM personal_dev_candidate_build_attempts WHERE id=:id FOR UPDATE"),
                    {"id": registration.build_attempt.id})
                task = asyncio.create_task(exchange())
                await connected.wait()
                # Observe the actual database lock wait before allowing expiry.
                while True:
                    with engine.connect() as observer:
                        waiting = observer.scalar(text("SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=:pid"), {"pid": pid})
                    if waiting:
                        break
                    assert not task.done(), "registration never reached the source lock"
                    await asyncio.sleep(0.01)
                assert datetime.now(UTC) < expiry
                # This is a deadline test: hold the proven lock until the database
                # clock, rather than an assumed scheduling delay, crosses expiry.
                while True:
                    with engine.connect() as observer:
                        expired = observer.scalar(text("SELECT clock_timestamp() >= :expiry"), {"expiry": expiry})
                    if expired:
                        break
                    await asyncio.sleep(0.02)
            with pytest.raises(DBAPIError, match=r"bootstrap.*expiry"):
                await task
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == 0
