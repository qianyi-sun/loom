"""Only exact manager release plus committed local cleanup may retire a hold."""

from datetime import UTC, datetime
from importlib import import_module

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import (
    ExecutableFinalReleaseWitnessV2,
    ExecutableReleasedShapeV2,
)
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
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
        terminal, physical = await terminal_input(values)
        async with factory.begin() as session:
            await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
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
    return release_witness(publication, terminal)


def release_witness(publication, terminal):
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


@pytest.mark.parametrize("source_state", ["live", "cancelled", "expired"])
async def test_retirement_clears_accounting_and_only_live_unregistered_work_requeues(prepared_input, source_state):
    from loom_capacity_build_guard.demand_store import BuildGuardDemandStore

    factory, engine, installation, _plan, registration, request = prepared_input
    witness = await ready_hold(prepared_input, "withdrawn")
    with engine.begin() as connection:
        if source_state == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
        elif source_state == "expired":
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
                {"id": registration.build_attempt.id})
    async with factory.begin() as session:
        await retirement(session, installation).retire(witness)
    async with factory.begin() as session:
        demand = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
    assert demand.current_assignments == demand.fixed_claims == ()
    assert bool(demand.pending_unassigned) == (source_state == "live")


async def test_retirement_replay_never_removes_successor_assignment_hold(prepared_input):
    from uuid import uuid4

    from loom_capacity_build_guard.plan_store import BuildGuardPlanStore

    factory, engine, installation, plan, registration, request = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked")
    async with factory.begin() as session:
        original = await retirement(session, installation).retire(witness)
    binding = plan.shapes[0].binding.model_copy(update={"intent_id": uuid4(), "tranche_id": uuid4(),
        "shape_instance_id": plan.shapes[0].binding.shape_instance_id + "-retry"})
    successor = plan.model_copy(update={"plan_id": uuid4(), "proposal_id": uuid4(), "admission_incarnation": uuid4(),
        "shapes": (plan.shapes[0].model_copy(update={"binding": binding}),),
        "allowances": (plan.allowances[0].model_copy(update={"allowance_id": uuid4(),
            "submission_intent_id": binding.intent_id, "shape_instance_id": binding.shape_instance_id}),)})
    async with factory.begin() as session:
        following = await BuildGuardPlanStore(session, installation=installation).prepare(successor, sources={request.id: registration})
    async with factory.begin() as session:
        assert await retirement(session, installation).retire(witness) == original
        with pytest.raises(DBAPIError, match="hold"):
            await BuildGuardPlanStore(session, installation=installation).authorize_publication(plan.plan_id)
        with pytest.raises(DBAPIError, match="disposition"):
            await BuildGuardPlanStore(session, installation=installation).prepare(plan, sources={request.id: registration})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT assignment_id FROM loom_capacity_build_guard.request_holds WHERE request_id=:id"),
            {"id": request.id}) == following.assignments[0].id


async def test_retirement_requires_prior_ack_commit(prepared_input):
    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked", acknowledge=False)
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest)
        with pytest.raises(DBAPIError, match="committed"):
            await retirement(session, installation).retire(witness)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    async with factory.begin() as session:
        await retirement(session, installation).retire(witness)


async def test_retirement_requires_prior_terminal_commit(prepared_input):
    factory, engine, installation, *_ = prepared_input
    terminal, physical = await terminal_input(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
    async with factory.begin() as session:
        await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest)
    witness = release_witness(publication, terminal)
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(terminal)
        with pytest.raises(DBAPIError, match="committed native terminal"):
            await retirement(session, installation).retire(witness)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    async with factory.begin() as session:
        await retirement(session, installation).retire(witness)


async def test_retirement_changed_replay_preserves_original_evidence(prepared_input):
    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked")
    async with factory.begin() as session:
        original = await retirement(session, installation).retire(witness)
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="exact replay"):
            await retirement(session, installation).retire(witness.model_copy(update={"command_sequence": witness.command_sequence + 1}))
        assert await retirement(session, installation).retire(witness) == original
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 1


async def test_retirement_cannot_consume_another_installed_owners_hold(prepared_input, owner_sessions, tmp_path):
    from uuid import uuid4

    from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
    from loom_capacity_manager.executable_contracts import (
        canonical_executable_bytes,
        canonical_executable_digest,
    )
    from tests.unit.test_personal_dev_build_admission import admission_input

    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "withdrawn")
    values = admission_input(tmp_path)
    member = values["member"]
    subject, incarnation = uuid4(), uuid4()
    member = member.model_copy(update={
        "configuration": member.configuration.model_copy(update={"subject_id": subject, "subject_incarnation": incarnation}),
        "acknowledgement": member.acknowledgement.model_copy(update={"subject_id": subject, "subject_incarnation": incarnation})})
    owner_factory, owner = owner_sessions
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        foreign = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(member=member, runtime=values["runtime"])
    assert foreign.document.owner_user_id != installation.document.owner_user_id
    wire = canonical_executable_bytes(witness)
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="installation binding"):
            await retirement(session, foreign).retire(witness)
        with pytest.raises(DBAPIError, match="committed bootstrap"):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.retire_request_hold(
                    :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                    {"installation": foreign.id, "payload": wire.decode("ascii"), "wire": wire,
                        "digest": canonical_executable_digest(witness)})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 0


async def test_retirement_concurrent_replay_has_one_immutable_outcome(prepared_input):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked")
    async def compete():
        for _ in range(3):
            try:
                async with factory.begin() as session:
                    return await retirement(session, installation).retire(witness)
            except DBAPIError as exc:
                if getattr(exc.orig, "sqlstate", None) != "40001":
                    raise
        raise AssertionError("serialization retry exhausted")
    results = await asyncio.gather(compete(), compete())
    assert results[0] == results[1]
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 1


async def test_retirement_ledger_is_immutable_and_blocks_lossy_downgrade(prepared_input, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked")
    async with factory.begin() as session:
        await retirement(session, installation).retire(witness)
        with pytest.raises(DBAPIError, match="permission denied"):
            async with session.begin_nested():
                await session.execute(text("DELETE FROM loom_capacity_build_guard.request_holds"))
    for mutation in ("UPDATE loom_capacity_build_guard.hold_retirements SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.hold_retirements", "TRUNCATE loom_capacity_build_guard.hold_retirements"):
        with engine.begin() as connection, pytest.raises(DBAPIError, match="append-only"):
            connection.execute(text(mutation))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0014")


@pytest.mark.parametrize("boundary", ["schema", "inventory", "protected", "command", "installation", "binding"])
async def test_retirement_direct_sql_cannot_substitute_evidence(prepared_input, boundary):
    import hashlib
    import json
    from uuid import uuid4

    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "withdrawn")
    payload = witness.model_dump(mode="json")
    if boundary == "schema":
        payload["schema_version"] = "2"
    elif boundary == "inventory":
        payload["release"]["inventory_sequence"] += 1
    elif boundary == "protected":
        payload["protected_release"]["protected_release_sha256"] = "f" * 64
    elif boundary == "command":
        payload["command_sequence"] = 0
    elif boundary == "binding":
        payload["release"]["binding"]["subject_id"] = str(uuid4())
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.retire_request_hold(
                    :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                    {"installation": uuid4() if boundary == "installation" else installation.id,
                        "payload": wire.decode("ascii"), "wire": wire, "digest": hashlib.sha256(wire).hexdigest()})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 0


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path", "helper"])
def test_hold_retirement_privilege_drift_is_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.retire_request_hold(uuid,jsonb,bytea,text)"
    sql = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
        "helper": "ALTER FUNCTION loom_capacity_build_guard.hold_retirement_receipt(loom_capacity_build_guard.hold_retirements) SECURITY DEFINER"}
    with engine.begin() as connection:
        connection.execute(text(sql[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
