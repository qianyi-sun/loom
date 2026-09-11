"""Lost workers settle before protected release, never wait on final release."""

from importlib import import_module

import pytest
from sqlalchemy import text

from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registered_release import (
    registered_release_input,
)
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def coordinator(factory, installation, manager, **kwargs):
    return import_module("loom_capacity_build_guard.terminal_recovery").BuildTerminalRecoveryCoordinator(
        session_factory=factory, installation=installation, manager=manager, **kwargs)


@pytest.mark.parametrize("result", ["unclaimed", "live", "failed", "artifact-ready", "cancelled"])
async def test_terminal_recovery_settles_then_releases_without_retiring_hold(prepared_input, monkeypatch, result):
    factory, engine, installation, *_ = prepared_input
    release, claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result=result, drain=False)
    calls = []

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            assert intent_id == release.binding.intent_id
            with engine.begin() as connection:
                connection.execute(text("SET LOCAL lock_timeout='100ms'"))
                connection.execute(text("SELECT id FROM loom_capacity_build_guard.installations WHERE id=:id FOR UPDATE"), {"id": installation.id})
            calls.append(intent_id)
            return terminal

        async def get_final_release_witness(self, intent_id):
            pytest.fail("terminal settlement cannot wait for final release")

    runtime = coordinator(factory, installation, Manager())
    results = await runtime.reconcile()
    assert len(results) == 1 and results[0].state == "released"
    assert calls == [release.binding.intent_id]
    assert await runtime.reconcile() == ()
    async with factory.begin() as session:
        observed = await store(session, installation).observe_intent(release.binding)
        assert observed.release is not None and observed.release.live_claim_count == 0
        assert observed.drain is not None and observed.drain.live_claim_count == 0
        outcome = await store(session, installation).read_outcome(claim) if result != "unclaimed" else None
        assert (outcome.request.result if outcome else None) == ("interrupted" if result == "live" else None if result == "unclaimed" else result)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 0
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_candidates WHERE status='building'")) == 1


@pytest.mark.parametrize("boundary", ["missing", "wrong-binding", "cancel"])
async def test_terminal_recovery_requires_exact_evidence_and_propagates_cancel(prepared_input, monkeypatch, boundary):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    _release, _claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="live", drain=False)

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            if boundary == "cancel":
                raise asyncio.CancelledError
            return None if boundary == "missing" else terminal.model_copy(update={
                "binding": terminal.binding.model_copy(update={"account_id": "foreign"})})

    runtime = coordinator(factory, installation, Manager())
    if boundary == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await runtime.reconcile()
    else:
        result = (await runtime.reconcile())[0]
        assert result.state == ("unavailable" if boundary == "missing" else "failed")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_discovery_omits_secrets_and_released_workers(prepared_input, monkeypatch):
    from hashlib import sha256

    from loom_capacity_build_guard.terminal_discovery import BuildGuardTerminalDiscovery
    from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

    factory, _engine, installation, *_ = prepared_input
    request, claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        page = await BuildGuardTerminalDiscovery(session, installation=installation).read_pending()
        assert len(page.workers) == 1 and page.workers[0].claim == claim
        assert CREDENTIAL not in page.model_dump_json()
        assert sha256(CREDENTIAL.encode()).hexdigest() not in page.model_dump_json()
        assert not page.executable
        assert (await BuildGuardTerminalDiscovery(session, installation=installation).read_pending(
            after_event_id=page.workers[0].event_id)).workers == ()
    async with factory.begin() as session:
        await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        assert (await BuildGuardTerminalDiscovery(session, installation=installation).read_pending()).workers == ()


@pytest.mark.parametrize("boundary", ["after", "through", "limit", "installation"])
async def test_native_discovery_rejects_sql_bounds_and_wrong_installation(prepared_input, monkeypatch, boundary):
    from uuid import uuid4

    from sqlalchemy.exc import DBAPIError

    factory, _engine, installation, *_ = prepared_input
    await registered_release_input(prepared_input, monkeypatch)
    arguments = {"installation": installation.id, "after": 0, "through": None, "limit": 1}
    arguments.update({"after": -1} if boundary == "after" else {"after": 1, "through": 0} if boundary == "through"
        else {"limit": 65} if boundary == "limit" else {"installation": uuid4()})
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match=r"bounds|installation"):
            async with session.begin_nested():
                await session.scalar(text("SELECT loom_capacity_build_guard.read_pending_native_workers(:installation,:after,:through,:limit)"), arguments)


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path"])
def test_native_discovery_privilege_drift_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.read_pending_native_workers(uuid,bigint,bigint,integer)"
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


@pytest.mark.parametrize("boundary", ["import", "settlement", "drain", "release"])
async def test_terminal_recovery_replays_each_committed_interruption(prepared_input, monkeypatch, boundary):
    import asyncio

    from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
    from loom_capacity_build_guard.terminal_store import BuildGuardTerminalStore

    factory, engine, installation, *_ = prepared_input
    _request, claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="live", drain=False)
    # Observe the next transaction boundary after a successful stage and cancel
    # before doing any work in the following stage. Earlier evidence is committed.
    methods = {"import": (BuildGuardTerminalStore, "settle_interrupted"),
        "settlement": (BuildGuardExecutionStore, "observe_intent"),
        "drain": (BuildGuardTerminalStore, "release_terminal_worker"),
        "release": (BuildGuardTerminalStore, "release_terminal_worker")}
    cls, name = methods[boundary]
    original = getattr(cls, name)
    triggered = []

    async def interrupted(*args, **kwargs):
        if not triggered:
            triggered.append(True)
            raise asyncio.CancelledError
        return await original(*args, **kwargs)

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            return terminal

    runtime = coordinator(factory, installation, Manager())
    if boundary != "release":
        monkeypatch.setattr(cls, name, interrupted)
        with pytest.raises(asyncio.CancelledError):
            await runtime.reconcile()
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    assert (await runtime.reconcile())[0].state == "released"
    restarted = coordinator(factory, installation, Manager())
    assert await restarted.reconcile() == ()
    async with factory.begin() as session:
        assert (await store(session, installation).read_outcome(claim)).request.result == "interrupted"


async def test_terminal_sweep_retries_early_missing_worker_after_later_progress(prepared_input, monkeypatch, sessions, tmp_path):
    from tests.integration import test_personal_dev_build_guard_registration as registration_module
    from tests.integration.test_personal_dev_build_guard_recovery import other_pool_input

    factory, _engine, installation, *_ = prepared_input
    first, _claim, first_terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="unclaimed", drain=False)
    other = await other_pool_input(prepared_input, sessions, tmp_path)
    monkeypatch.setattr(registration_module, "CREDENTIAL", "z" * 43)
    second, _claim, second_terminal, _drain = await registered_release_input(other, monkeypatch, result="unclaimed", drain=False)
    witnesses = {second.binding.intent_id: second_terminal}
    calls = []

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            calls.append(intent_id)
            return witnesses.get(intent_id)

    runtime = coordinator(factory, installation, Manager(), batch_size=1)
    assert (await runtime.reconcile())[0].state == "unavailable"
    assert (await runtime.reconcile())[0].state == "released"
    witnesses[first.binding.intent_id] = first_terminal
    assert (await runtime.reconcile())[0].state == "released"
    assert calls == [first.binding.intent_id, second.binding.intent_id, first.binding.intent_id]
    assert await runtime.reconcile() == ()


async def test_worker_release_wins_while_terminal_recovery_is_fetching(prepared_input, monkeypatch):
    from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

    factory, _engine, installation, *_ = prepared_input
    request, _claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
    released = []

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            async with factory.begin() as session:
                released.append(await store(session, installation).acknowledge_release(request,
                    current_worker_credential=CREDENTIAL))
            return terminal

    runtime = coordinator(factory, installation, Manager())
    assert (await runtime.reconcile())[0].state == "released"
    assert await runtime.reconcile() == ()
    async with factory.begin() as session:
        assert (await store(session, installation).observe_intent(request.binding)).release == released[0]


async def test_discovery_rejects_uncommitted_claim_and_corrupt_receipt(prepared_input, monkeypatch):
    from sqlalchemy.exc import DBAPIError

    from loom_capacity_build_guard.terminal_discovery import BuildGuardTerminalDiscovery
    from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

    factory, _engine, installation, *_ = prepared_input
    _request, claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="unclaimed", drain=False)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
        with pytest.raises(DBAPIError, match="committed"):
            async with session.begin_nested():
                await BuildGuardTerminalDiscovery(session, installation=installation).read_pending()
    async with factory.begin() as session:
        original = session.scalar

        # Preserve canonical bytes while substituting the claim's worker identity.
        async def corrupt_claim(statement, *args, **kwargs):
            import json

            result = json.loads(await original(statement, *args, **kwargs))
            result["workers"][0]["claim"]["worker_id"] = str(claim.worker_incarnation)
            from loom_capacity_build_guard.terminal_discovery import PendingNativeWorkerPageV1
            from loom_capacity_manager.contracts import canonical_bytes

            return canonical_bytes(PendingNativeWorkerPageV1.model_validate_json(json.dumps(result))).decode("ascii")

        monkeypatch.setattr(session, "scalar", corrupt_claim)
        with pytest.raises(ValueError, match="binding"):
            await BuildGuardTerminalDiscovery(session, installation=installation).read_pending()


async def test_claim_committed_after_discovery_is_recovered_on_next_sweep(prepared_input, monkeypatch):
    from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

    factory, engine, installation, *_ = prepared_input
    _request, claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="unclaimed", drain=False)
    claimed = []

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            if not claimed:
                async with factory.begin() as session:
                    claimed.append(await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL))
            return terminal

    runtime = coordinator(factory, installation, Manager())
    first = (await runtime.reconcile())[0]
    assert first.state == "failed" and first.failure == "authority"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    assert (await runtime.reconcile())[0].state == "released"
    async with factory.begin() as session:
        assert (await store(session, installation).read_outcome(claim)).request.result == "interrupted"
