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


async def retained_attempt(values, owner_sessions, monkeypatch, *, finalized=True, credential=CREDENTIAL):
    from tests.integration import test_personal_dev_build_guard_registration as registration
    from tests.integration.test_personal_dev_build_guard_terminal import terminal_input

    witnesses = []

    async def physical(inputs):
        terminal, binding = await terminal_input(inputs)
        witnesses.append(terminal)
        return None, None, None, binding, None

    monkeypatch.setattr(registration, "bound_input", physical)
    contracts, claim, profile, preparation, final = await recovery_input(values, owner_sessions, monkeypatch, credential=credential)
    factory, _engine, installation, *_ = values
    async with factory.begin() as session:
        prepared = await store(session, installation).publish_recovery(
            contracts.NativeRecoveryPublicationV1(claim=claim, record=preparation), worker_credential=credential)
    finalized_receipt = None
    if finalized:
        async with factory.begin() as session:
            finalized_receipt = await store(session, installation).publish_recovery(
                contracts.NativeRecoveryPublicationV1(claim=claim, record=final), worker_credential=credential)
    return claim, profile, prepared, finalized_receipt, witnesses[0]


@pytest.mark.parametrize("boundary", ["preparation-only", "finalized", "expired", "retired-hold", "not-terminal"])
async def test_management_readback_survives_lost_worker_and_retired_hold(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    module = import_module("loom_capacity_build_guard.native_terminal_recovery")
    from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator
    from tests.integration.test_personal_dev_build_guard_hold_retirement import (
        release_witness,
        retirement,
    )
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
        reader = module.NativeTerminalRecoveryStore(session, installation=installation)
        page = await reader.discover(limit=1)
        result = await reader.read(claim.operation_id)
    if boundary == "not-terminal":
        assert result is None and page.attempts == ()
        return
    assert page.installation_id == installation.id and len(page.attempts) == 1
    assert page.attempts[0].claim_id == claim.operation_id and not page.executable
    async with factory.begin() as session:
        following = await module.NativeTerminalRecoveryStore(session, installation=installation).discover(
            after_event_id=page.attempts[0].event_id, through_event_id=page.through_event_id, limit=1)
        assert following.attempts == () and following.through_event_id == page.through_event_id
    assert result.preparation == prepared and result.finalization == final
    assert result.profile == profile and result.host.boot_id == prepared.request.record.boot_id
    assert result.terminal.binding == claim.binding and result.release.binding == claim.binding
    assert not result.executable
    assert CREDENTIAL not in result.model_dump_json()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == 1


@pytest.mark.parametrize("boundary", ["uncommitted-terminal", "uncommitted-release", "missing-claim", "foreign-installation"])
async def test_terminal_readback_requires_committed_exact_scope(prepared_input, owner_sessions, monkeypatch, boundary):
    from dataclasses import replace
    from uuid import uuid4

    from sqlalchemy.exc import DBAPIError

    from loom_capacity_agent.admission import ExecutableDrainRequestV2, ExecutableReleaseRequestV2
    from loom_capacity_build_guard.native_terminal_recovery import NativeTerminalRecoveryStore
    from loom_capacity_manager.contracts import canonical_bytes
    from tests.integration.test_personal_dev_build_guard_outcomes import outcome_request
    from tests.integration.test_personal_dev_build_guard_terminal import terminal_store

    claim, _profile, _prepared, _final, terminal = await retained_attempt(prepared_input, owner_sessions, monkeypatch)
    factory, _engine, installation, *_ = prepared_input
    async with factory.begin() as session:
        await store(session, installation).record_outcome(outcome_request(claim, result="failed"), worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        await store(session, installation).begin_drain(ExecutableDrainRequestV2(binding=claim.binding,
            operation_id=uuid4(), worker_id=claim.worker_id, worker_incarnation=claim.worker_incarnation,
            expected_claim_high_water=1, drain_epoch=3))
    release = ExecutableReleaseRequestV2(binding=claim.binding, operation_id=uuid4(),
        reporter_incarnation=installation.document.reporter_incarnation, bootstrap_registration_epoch=1,
        protected_registration_epoch=2, expected_claim_high_water=1, release_epoch=4)
    if boundary != "uncommitted-release":
        async with factory.begin() as session:
            await store(session, installation).acknowledge_release(release, current_worker_credential=CREDENTIAL)
    if boundary != "uncommitted-terminal":
        async with factory.begin() as session:
            await terminal_store(session, installation).import_evidence(terminal)
    async with factory.begin() as session:
        if boundary == "uncommitted-release":
            await store(session, installation).acknowledge_release(release, current_worker_credential=CREDENTIAL)
        if boundary == "uncommitted-terminal":
            await terminal_store(session, installation).import_evidence(terminal)
        selected = installation
        if boundary == "foreign-installation":
            document = installation.document.model_copy(update={"owner_user_id": uuid4()})
            selected = replace(installation, document=document, wire_payload=canonical_bytes(document))
        reader = NativeTerminalRecoveryStore(session, installation=selected)
        if boundary == "missing-claim":
            assert await reader.read(uuid4()) is None
        else:
            with pytest.raises(DBAPIError, match=r"committed|installation binding"):
                await reader.read(claim.operation_id)
            with pytest.raises(DBAPIError, match=r"committed|installation binding"):
                await reader.discover()
    # The failure did not roll back the caller's unrelated terminal/release work.
    async with factory.begin() as session:
        assert await NativeTerminalRecoveryStore(session, installation=installation).read(claim.operation_id) is not None


@pytest.mark.parametrize("bounds", [(-1, None, 1), (1, 0, 1), (0, None, 0), (0, None, 65), (None, None, 1)])
async def test_terminal_discovery_rejects_invalid_bounds_in_sql(prepared_input, bounds):
    from sqlalchemy.exc import DBAPIError

    factory, _engine, installation, *_ = prepared_input
    after, through, limit = bounds
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="bounds"):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.discover_terminal_native_recovery(
                    :installation,:wire,:after,:through,:limit)"""),
                    {"installation": installation.id, "wire": installation.wire_payload,
                        "after": after, "through": through, "limit": limit})


@pytest.mark.parametrize("field", ["reporter_incarnation", "request_digest", "bootstrap_registration_epoch", "protected_registration_epoch", "release_epoch"])
async def test_terminal_recovery_rejects_mutated_release_response(prepared_input, owner_sessions, monkeypatch, field):
    import json
    from uuid import uuid4

    from loom_capacity_build_guard.native_terminal_recovery import NativeTerminalRecoveryStore
    from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator

    claim, _profile, _prepared, _final, terminal = await retained_attempt(prepared_input, owner_sessions, monkeypatch)
    factory, _engine, installation, *_ = prepared_input

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            return terminal

    assert (await BuildTerminalRecoveryCoordinator(session_factory=factory, installation=installation, manager=Manager()).reconcile())[0].state == "released"
    async with factory.begin() as session:
        original = session.scalar

        async def corrupted(*args, **kwargs):
            value = json.loads(await original(*args, **kwargs))
            value["release"][field] = str(uuid4()) if field == "reporter_incarnation" else "f" * 64 if field == "request_digest" else 9
            return json.dumps(value, sort_keys=True, separators=(",", ":"))

        monkeypatch.setattr(session, "scalar", corrupted)
        with pytest.raises(ValueError, match=r"release|binding|response"):
            await NativeTerminalRecoveryStore(session, installation=installation).read(claim.operation_id)


@pytest.mark.parametrize("signature", ["read_terminal_native_recovery(uuid,bytea,uuid)", "discover_terminal_native_recovery(uuid,bytea,bigint,bigint,integer)"])
@pytest.mark.parametrize("boundary", ["grant", "public", "search-path"])
def test_terminal_recovery_readback_privilege_drift_fails_closed(build_guard_database, signature, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard." + signature
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC", "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


async def test_terminal_discovery_pages_and_resets_across_retried_allocations(prepared_input, owner_sessions, monkeypatch):
    from uuid import uuid4

    from loom_capacity_build_guard.native_terminal_recovery import NativeTerminalRecoveryStore
    from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator
    from tests.integration import test_personal_dev_build_guard_terminal as terminal_fixture
    from tests.integration.test_personal_dev_build_guard_hold_retirement import (
        release_witness,
        retirement,
    )
    from tests.integration.test_personal_dev_build_guard_release_outbox import outbox

    factory, _engine, installation, plan, source, platform = prepared_input

    async def settle(values, *, retire_hold):
        claim, _profile, _prepared, _final, terminal = await retained_attempt(values, owner_sessions, monkeypatch,
            credential="a" * 43 if retire_hold else "z" * 43)

        class Manager:
            async def get_build_terminal_inventory_evidence(self, intent_id):
                return terminal

        assert (await BuildTerminalRecoveryCoordinator(session_factory=factory, installation=installation, manager=Manager()).reconcile())[0].state == "released"
        if retire_hold:
            async with factory.begin() as session:
                publication = await outbox(session, installation).read_next()
            async with factory.begin() as session:
                await outbox(session, installation).acknowledge(publication, manager_acknowledgement_digest=publication.publication_digest)
            async with factory.begin() as session:
                await retirement(session, installation).retire(release_witness(publication, terminal))
        return claim

    first = await settle(prepared_input, retire_hold=True)
    async with factory.begin() as session:
        original_page = await NativeTerminalRecoveryStore(session, installation=installation).discover(limit=1)
    assert original_page.attempts[0].claim_id == first.operation_id
    binding = plan.shapes[0].binding.model_copy(update={"intent_id": uuid4(), "tranche_id": uuid4(),
        "shape_instance_id": plan.shapes[0].binding.shape_instance_id + "-retry"})
    successor = plan.model_copy(update={"plan_id": uuid4(), "proposal_id": uuid4(), "admission_incarnation": uuid4(),
        "shapes": (plan.shapes[0].model_copy(update={"binding": binding}),),
        "allowances": (plan.allowances[0].model_copy(update={"allowance_id": uuid4(),
            "submission_intent_id": binding.intent_id, "shape_instance_id": binding.shape_instance_id}),)})
    original_physical = terminal_fixture.physical
    monkeypatch.setattr(terminal_fixture, "physical", lambda registration: original_physical(registration).model_copy(update={"slurm_job_id": "1235"}))
    second = await settle((factory, _engine, installation, successor, source, platform), retire_hold=False)
    async with factory.begin() as session:
        reader = NativeTerminalRecoveryStore(session, installation=installation)
        assert (await reader.discover(after_event_id=original_page.attempts[0].event_id,
            through_event_id=original_page.through_event_id, limit=1)).attempts == ()
        reset = await reader.discover(limit=1)
        assert reset.through_event_id > original_page.through_event_id
        assert reset.attempts[0].claim_id == first.operation_id
        following = await reader.discover(after_event_id=reset.attempts[0].event_id,
            through_event_id=reset.through_event_id, limit=1)
        assert following.attempts[0].claim_id == second.operation_id
        assert following.attempts[0].event_id > reset.attempts[0].event_id
        assert (await reader.discover(after_event_id=following.attempts[0].event_id,
            through_event_id=reset.through_event_id, limit=1)).attempts == ()
        assert await reader.read(first.operation_id) is not None and await reader.read(second.operation_id) is not None
