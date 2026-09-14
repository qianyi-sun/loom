"""Management recovers retained attempts after the worker and its hold are gone."""

from importlib import import_module

import pytest
from sqlalchemy import text

from tests.integration.test_native_recovery_publication import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_native_recovery_publication import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_native_recovery_publication import (
    prepared_input as prepared_input,
)
from tests.integration.test_native_recovery_publication import recovery_input
from tests.integration.test_native_recovery_publication import (
    sessions as sessions,
)
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL


async def retained_attempt(values, owner_sessions, monkeypatch, *, finalized=True):
    from tests.integration import test_personal_dev_build_guard_registration as registration
    from tests.integration.test_personal_dev_build_guard_terminal import terminal_input

    witnesses = []

    async def physical(inputs):
        terminal, binding = await terminal_input(inputs)
        witnesses.append(terminal)
        return None, None, None, binding, None

    monkeypatch.setattr(registration, "bound_input", physical)
    contracts, claim, profile, preparation, final = await recovery_input(values, owner_sessions, monkeypatch)
    factory, _engine, installation, *_ = values
    async with factory.begin() as session:
        prepared = await store(session, installation).publish_recovery(
            contracts.NativeRecoveryPublicationV1(claim=claim, record=preparation), worker_credential=CREDENTIAL)
    finalized_receipt = None
    if finalized:
        async with factory.begin() as session:
            finalized_receipt = await store(session, installation).publish_recovery(
                contracts.NativeRecoveryPublicationV1(claim=claim, record=final), worker_credential=CREDENTIAL)
    return claim, profile, prepared, finalized_receipt, witnesses[0]


@pytest.mark.parametrize("boundary", ["preparation-only", "finalized", "expired", "retired-hold", "not-terminal"])
async def test_management_readback_survives_lost_worker_and_retired_hold(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    module = import_module("loom_capacity_build_guard.native_terminal_recovery")
    from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator
    from tests.integration.test_personal_dev_build_guard_hold_retirement import release_witness, retirement
    from tests.integration.test_personal_dev_build_guard_release_outbox import outbox

    claim, profile, prepared, final, terminal = await retained_attempt(
        prepared_input, owner_sessions, monkeypatch, finalized=boundary != "preparation-only")
    factory, engine, installation, _plan, source, *_ = prepared_input
    if boundary in {"expired", "retired-hold"}:
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
                {"id": source.build_attempt.id})

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            assert intent_id == claim.binding.intent_id
            return terminal

    if boundary != "not-terminal":
        result = await BuildTerminalRecoveryCoordinator(session_factory=factory, installation=installation, manager=Manager()).reconcile()
        assert len(result) == 1 and result[0].state == "released"
    if boundary == "retired-hold":
        async with factory.begin() as session:
            publication = await outbox(session, installation).read_next()
        async with factory.begin() as session:
            await outbox(session, installation).acknowledge(publication, manager_acknowledgement_digest=publication.publication_digest)
        async with factory.begin() as session:
            await retirement(session, installation).retire(release_witness(publication, terminal))
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
    async with factory.begin() as session:
        result = await module.NativeTerminalRecoveryStore(session, installation=installation).read(claim.operation_id)
    if boundary == "not-terminal":
        assert result is None
        return
    assert result.preparation == prepared and result.finalization == final
    assert result.profile == profile and result.host.boot_id == prepared.request.record.boot_id
    assert result.terminal.binding == claim.binding and result.release.binding == claim.binding
    assert not result.executable
    assert CREDENTIAL not in result.model_dump_json()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == 1
