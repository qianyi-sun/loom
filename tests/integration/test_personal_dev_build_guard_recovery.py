"""Management recovery joins real guard state across committed network steps."""

from importlib import import_module

import pytest
from sqlalchemy import text

from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_hold_retirement import (
    ready_hold,
    release_witness,
    retirement,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_release_outbox import outbox
from tests.integration.test_personal_dev_build_guard_terminal import terminal_input
from tests.integration.test_personal_dev_build_guard_withdrawal import withdrawal
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def coordinator(factory, installation, manager, **kwargs):
    return import_module("loom_capacity_build_guard.recovery").BuildRecoveryCoordinator(
        session_factory=factory, installation=installation, manager=manager, **kwargs)


async def test_discovery_requires_committed_ack_and_excludes_retired_holds(prepared_input):
    factory, _engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked", acknowledge=False)
    async with factory.begin() as session:
        assert await retirement(session, installation).read_pending() == ()
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest)
    async with factory.begin() as session:
        pending = await retirement(session, installation).read_pending()
        assert pending == (publication,)
        assert await retirement(session, installation).read_pending(after_event_id=publication.event_id) == ()
        await retirement(session, installation).retire(witness)
    async with factory.begin() as session:
        assert await retirement(session, installation).read_pending() == ()


@pytest.mark.parametrize("kind", ["prepared-revoked", "withdrawn"])
async def test_recovery_commits_terminal_before_retiring_without_network_locks(prepared_input, kind):
    factory, engine, installation, *_ = prepared_input
    terminal = None
    if kind == "withdrawn":
        terminal, physical = await terminal_input(prepared_input)
        async with factory.begin() as session:
            await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
        async with factory.begin() as session:
            publication = await outbox(session, installation).read_next()
            await outbox(session, installation).acknowledge(publication,
                manager_acknowledgement_digest=publication.publication_digest)
        witness = release_witness(publication, terminal)
    else:
        witness = await ready_hold(prepared_input, kind)
    calls = []

    class Manager:
        async def get_final_release_witness(self, intent_id):
            assert intent_id == witness.release.binding.intent_id
            # A network callback can acquire installation authority: discovery
            # committed its locks before issuing this authenticated request.
            with engine.begin() as connection:
                connection.execute(text("SET LOCAL lock_timeout='100ms'"))
                connection.execute(text("SELECT id FROM loom_capacity_build_guard.installations WHERE id=:id FOR UPDATE"), {"id": installation.id})
            calls.append("release")
            return witness

        async def get_build_terminal_inventory_evidence(self, intent_id):
            assert intent_id == witness.release.binding.intent_id
            calls.append("terminal")
            return terminal

    runtime = coordinator(factory, installation, Manager())
    results = await runtime.reconcile()
    assert len(results) == 1 and results[0].state == "retired"
    assert calls == (["release", "terminal"] if terminal else ["release"])
    assert await runtime.reconcile() == ()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == (terminal is not None)
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


async def test_missing_witness_is_retried_after_cursor_wrap(prepared_input):
    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked")
    responses = [None, witness]

    class Manager:
        async def get_final_release_witness(self, intent_id):
            return responses.pop(0)

    runtime = coordinator(factory, installation, Manager(), batch_size=1)
    assert (await runtime.reconcile())[0].state == "unavailable"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    assert (await runtime.reconcile())[0].state == "retired"
