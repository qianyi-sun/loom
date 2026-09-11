"""Only exact manager release plus committed local cleanup may retire a hold."""

from datetime import UTC, datetime
from importlib import import_module

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import ExecutableFinalReleaseWitnessV2, ExecutableReleasedShapeV2
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_release_outbox import outbox, release_input
from tests.integration.test_personal_dev_build_guard_terminal import terminal_input, terminal_store
from tests.integration.test_personal_dev_build_guard_withdrawal import withdrawal
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def retirement(session, installation):
    return import_module("loom_capacity_build_guard.hold_retirement").BuildGuardHoldRetirementStore(session, installation=installation)


async def ready_hold(values, kind, *, acknowledge=True, import_terminal=True):
    factory, _engine, installation, *_ = values
    terminal = None
    if kind == "withdrawn":
        terminal, _physical = await terminal_input(values)
        async with factory.begin() as session:
            await store(session, installation).withdraw_unregistered_worker(withdrawal(terminal.binding))
        if import_terminal:
            async with factory.begin() as session:
                await terminal_store(session, installation).import_evidence(terminal)
    else:
        await release_input(values, kind)
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
    if acknowledge:
        async with factory.begin() as session:
            await outbox(session, installation).acknowledge(publication,
                manager_acknowledgement_digest=publication.publication_digest)
    binding = publication.release.binding
    # Simulate only the authenticated manager response. Native SQL runs for real;
    # this fixture is not a native end-to-end manager execution acceptance.
    witness = ExecutableFinalReleaseWitnessV2(
        release=ExecutableReleasedShapeV2(binding=binding, inventory_sequence=terminal.inventory_sequence if terminal else 1,
            terminal_kind="slurm-job" if terminal else "unused",
            terminal_identity=terminal.record.physical_identity if terminal else binding.shape_instance_id,
            terminal_evidence_sha256=terminal.record.terminal_evidence_sha256 if terminal else "a" * 64,
            protected_registration_epoch=publication.release.protected_registration_epoch,
            protected_release_sha256=publication.release.protected_release_sha256, bootstrap_revoked=True),
        protected_release=publication.release, protected_acknowledgement_sha256=publication.publication_digest,
        command_sequence=10, command_request_sha256="c" * 64, released_at=datetime.now(UTC))
    return witness


@pytest.mark.parametrize("kind", ["prepared-revoked", "withdrawn"])
async def test_exact_native_hold_retirement_is_atomic_and_replayable(prepared_input, kind):
    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, kind)
    async with factory.begin() as session:
        result = await retirement(session, installation).retire(witness)
    assert result.binding == witness.release.binding
    assert result.retirement_state == "retired"
    async with factory.begin() as session:
        assert await retirement(session, installation).retire(witness) == result
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


@pytest.mark.parametrize("boundary", ["ack", "terminal", "physical", "proof", "intent"])
async def test_native_hold_retirement_rejects_missing_or_changed_authority(prepared_input, boundary):
    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "withdrawn", acknowledge=boundary != "ack", import_terminal=boundary != "terminal")
    if boundary in {"physical", "proof", "intent"}:
        field = {"physical": "terminal_identity", "proof": "terminal_evidence_sha256"}.get(boundary)
        changed = witness.release.model_copy(update={field: "f" * 64}) if field else witness.release.model_copy(
            update={"binding": witness.release.binding.model_copy(update={"deployment_generation": witness.release.binding.deployment_generation+1})})
        witness = witness.model_copy(update={"release": changed})
    async with factory.begin() as session:
        with pytest.raises((ValueError, DBAPIError)):
            await retirement(session, installation).retire(witness)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 0


async def test_native_hold_retirement_rolls_back_corrupt_receipt(prepared_input, monkeypatch):
    import json

    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked")
    async with factory.begin() as session:
        original = session.scalar
        async def corrupt(*args, **kwargs):
            result = json.loads(await original(*args, **kwargs))
            result["witness_sha256"] = "f" * 64
            return json.dumps(result, sort_keys=True, separators=(",", ":"))
        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await retirement(session, installation).retire(witness)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 0
