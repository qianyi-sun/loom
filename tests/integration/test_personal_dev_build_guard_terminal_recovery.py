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
