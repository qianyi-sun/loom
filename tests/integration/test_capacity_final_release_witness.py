"""Final release evidence is retained with the exact command, not reconstructed."""

import json
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import (
    ExecutableIntentCloseV2,
    ExecutablePartialReleaseV2,
    ExecutableProtectedReleaseV2,
    canonical_executable_digest,
)
from loom_capacity_manager.models import (
    CapacityExecutableIntent,
    CapacityExecutableProtectedReleaseReceipt,
)
from loom_capacity_manager.store import ExecutionConflictError
from tests.capacity_execution_fixtures import executor_binding
from tests.integration.test_capacity_typed_membership_execution import typed_management
from tests.integration.test_capacity_typed_terminal_sql import terminal_inventory


async def ready_release(session):
    store, preparation, member, binding, inventory = await terminal_inventory(session)
    await store.ingest_typed_executor_inventory(session, inventory, management=typed_management(preparation))
    await store.begin_intent_close(session, ExecutableIntentCloseV2(binding=binding, command_sequence=4))
    protected = ExecutableProtectedReleaseV2(
        binding=binding, reporter_incarnation=member.acknowledgement.reporter_incarnation,
        bootstrap_registration_epoch=1, protected_registration_epoch=2,
        bootstrap_revoked=True, protected_release_sha256="b" * 64,
    )
    await store.acknowledge_protected_release(session, protected, actor="owner-agent", idempotency_key=UUID(int=124001))
    release = await store.next_pool_work(session, executor_binding(binding.pool_id))
    assert isinstance(release, ExecutablePartialReleaseV2)
    return store, member, protected, release


async def read_witness(session, store, member, release, **changes):
    args = dict(subject_id=member.configuration.subject_id,
        subject_incarnation=member.configuration.subject_incarnation,
        reporter_incarnation=member.acknowledgement.reporter_incarnation,
        intent_id=release.releases[0].binding.intent_id)
    return await store.subject_final_release_witness(session, **(args | changes))


async def test_final_release_witness_retains_exact_protected_receipt_after_replay(capacity_session):
    store, member, protected, release = await ready_release(capacity_session)
    assert await read_witness(capacity_session, store, member, release) is None
    await store.release_shapes(capacity_session, release)
    witness = await read_witness(capacity_session, store, member, release)
    assert witness.release == release.releases[0]
    assert witness.protected_release == protected
    assert witness.protected_acknowledgement_sha256 == canonical_executable_digest(protected)
    assert witness.command_sequence == release.command_sequence
    assert witness.command_request_sha256 == canonical_executable_digest(release)
    assert witness.released_at.tzinfo is not None
    await store.acknowledge_protected_release(capacity_session,
        protected.model_copy(update={"protected_registration_epoch": 3, "protected_release_sha256": "c" * 64}),
        actor="owner-agent", idempotency_key=UUID(int=124002))
    assert (await store.release_shapes(capacity_session, release)).replayed
    assert await read_witness(capacity_session, store, member, release) == witness
    assert await read_witness(capacity_session, store, member, release, subject_id=UUID(int=124003)) is None
    with pytest.raises(ExecutionConflictError, match="reporter"):
        await read_witness(capacity_session, store, member, release, reporter_incarnation=UUID(int=124003))


async def test_final_release_witness_rolls_back_with_command(capacity_session):
    store, member, _protected, release = await ready_release(capacity_session)
    with pytest.raises(RuntimeError, match="lost transaction"):
        async with capacity_session.begin_nested():
            await store.release_shapes(capacity_session, release)
            assert await read_witness(capacity_session, store, member, release) is not None
            raise RuntimeError("lost transaction")
    assert await read_witness(capacity_session, store, member, release) is None
    assert not (await store.release_shapes(capacity_session, release)).replayed
    assert await read_witness(capacity_session, store, member, release) is not None


@pytest.mark.parametrize("mutation", (
    "UPDATE capacity_executable_final_release_witnesses SET released_at = released_at",
    "DELETE FROM capacity_executable_final_release_witnesses",
    "TRUNCATE capacity_executable_final_release_witnesses",
))
async def test_final_release_witness_is_immutable(capacity_session, mutation):
    store, member, _protected, release = await ready_release(capacity_session)
    await store.release_shapes(capacity_session, release)
    await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    with pytest.raises(DBAPIError, match="append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(text(mutation))
    assert await read_witness(capacity_session, store, member, release) is not None


async def test_final_release_witness_downgrade_retains_authority(capacity_session):
    from alembic import command

    from tests.integration.test_capacity_build_membership_sql import _config

    store, _member, _protected, release = await ready_release(capacity_session)
    await store.release_shapes(capacity_session, release)
    await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    with pytest.raises(RuntimeError, match="retained final release"):
        async with capacity_session.begin_nested():
            connection = await capacity_session.connection()
            await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0022"))


async def stage_command(session, store, release):
    intent = await session.scalar(select(CapacityExecutableIntent).where(
        CapacityExecutableIntent.intent_id == release.releases[0].binding.intent_id))
    assert intent is not None
    receipt = await store._record_command(session, intent, sequence=release.command_sequence,
        operation_kind="release", request_digest=canonical_executable_digest(release),
        result_payload={"tranche_id": str(release.tranche_id),
            "released_shape_ids": [item.binding.shape_instance_id for item in release.releases], "executable": True})
    await session.flush()
    return receipt


@pytest.mark.parametrize("tamper", ("schema", "physical", "protected", "command", "binding"))
async def test_final_release_witness_rejects_direct_sql_substitution(capacity_session, tamper):
    store, _member, _protected, release = await ready_release(capacity_session)
    command = await stage_command(capacity_session, store, release)
    protected = await capacity_session.scalar(select(CapacityExecutableProtectedReleaseReceipt))
    payload = release.releases[0].model_dump(mode="json")
    if tamper == "schema":
        payload["schema_version"] = "2"
    elif tamper == "physical":
        payload["terminal_evidence_sha256"] = "f" * 64
    elif tamper == "binding":
        payload["binding"]["subject_id"] = str(UUID(int=124099))
    params = dict(intent=release.releases[0].binding.intent_id,
        protected=UUID(int=124099) if tamper == "protected" else protected.id,
        command=UUID(int=124099) if tamper == "command" else command.id, payload=json.dumps(payload))
    with pytest.raises(DBAPIError, match="final release"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(text("""
                INSERT INTO capacity_executable_final_release_witnesses
                (intent_id,protected_receipt_id,command_receipt_id,release_payload,released_at)
                VALUES (:intent,:protected,:command,CAST(:payload AS jsonb),clock_timestamp())
            """), params)


async def test_final_release_transition_requires_witness_at_commit_boundary(capacity_session):
    _store, _member, _protected, release = await ready_release(capacity_session)
    with pytest.raises(DBAPIError, match="atomic retained witness"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(text("""
                UPDATE capacity_executable_intents SET state='released',released_at=clock_timestamp()
                WHERE intent_id=:intent
            """), {"intent": release.releases[0].binding.intent_id})
            await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))


async def test_final_release_witness_without_transition_cannot_commit(capacity_session):
    store, _member, _protected, release = await ready_release(capacity_session)
    command = await stage_command(capacity_session, store, release)
    protected = await capacity_session.scalar(select(CapacityExecutableProtectedReleaseReceipt))
    with pytest.raises(DBAPIError, match="atomic retained witness"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(text("""
                INSERT INTO capacity_executable_final_release_witnesses
                (intent_id,protected_receipt_id,command_receipt_id,release_payload,released_at)
                VALUES (:intent,:protected,:command,CAST(:payload AS jsonb),clock_timestamp())
            """), dict(intent=release.releases[0].binding.intent_id,
                protected=protected.id, command=command.id, payload=release.releases[0].model_dump_json()))
            await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))


async def test_legacy_release_replay_does_not_fabricate_witness(capacity_session):
    from alembic import command

    from tests.integration.test_capacity_build_membership_sql import _config

    store, member, _protected, release = await ready_release(capacity_session)
    connection = await capacity_session.connection()
    await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0022"))
    await stage_command(capacity_session, store, release)
    await capacity_session.execute(text("""
        UPDATE capacity_executable_intents SET state='released',released_at=clock_timestamp()
        WHERE intent_id=:intent
    """), {"intent": release.releases[0].binding.intent_id})
    await connection.run_sync(lambda sync: command.upgrade(_config(sync), "capacity_0023"))
    capacity_session.expire_all()
    assert (await store.release_shapes(capacity_session, release)).replayed
    assert await read_witness(capacity_session, store, member, release) is None


async def test_pristine_proposal_discard_commits_without_fabricated_witness(capacity_session):
    from tests.integration.test_capacity_manager_execution_store import (
        test_newer_sealed_epoch_supersedes_a_stale_proposal,
    )

    await test_newer_sealed_epoch_supersedes_a_stale_proposal(capacity_session)
    await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert await capacity_session.scalar(text("SELECT count(*) FROM capacity_executable_final_release_witnesses")) == 0


async def test_multi_shape_release_retains_one_command_and_exact_per_intent_proofs(capacity_session):
    from datetime import UTC, datetime, timedelta

    from loom_capacity_manager.executable_contracts import (
        ExecutableBootstrapAcknowledgementV2,
        ExecutableBootstrapProposalV2,
    )
    from loom_capacity_manager.execution_store import CapacityExecutionStore
    from tests.capacity_fixtures import demand_snapshot
    from tests.integration.test_capacity_manager_execution_store import _batched_admission_proposal

    store = CapacityExecutionStore()
    executor, bindings, _plan = await _batched_admission_proposal(store, capacity_session)
    reporter = demand_snapshot().reporter_incarnation
    await store.begin_intent_close(capacity_session, ExecutableIntentCloseV2(binding=bindings[0], command_sequence=3))
    bootstrap = ExecutableBootstrapProposalV2(binding=bindings[1], command_sequence=4,
        proposal_epoch=1, bootstrap_sha256="7" * 64, expires_at=datetime.now(UTC) + timedelta(minutes=1))
    await store.propose_bootstrap(capacity_session, bootstrap)
    await store.acknowledge_bootstrap(capacity_session, ExecutableBootstrapAcknowledgementV2(
        binding=bindings[1], proposal_epoch=1, proposal_digest=canonical_executable_digest(bootstrap),
        reporter_incarnation=reporter, bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64, protected_admission_sha256="3" * 64),
        actor="development", idempotency_key=UUID(int=124201))
    await store.begin_intent_close(capacity_session, ExecutableIntentCloseV2(binding=bindings[1], command_sequence=5))
    for index, binding in enumerate(bindings):
        await store.acknowledge_protected_release(capacity_session, ExecutableProtectedReleaseV2(
            binding=binding, reporter_incarnation=reporter, bootstrap_registration_epoch=1,
            protected_registration_epoch=2, bootstrap_revoked=True, protected_release_sha256=str(index + 1) * 64),
            actor="development", idempotency_key=UUID(int=124202 + index))
    pieces = [await store.next_pool_work(capacity_session, executor, cleanup_only=True,
        cleanup_intent_id=binding.intent_id) for binding in bindings]
    assert all(isinstance(piece, ExecutablePartialReleaseV2) for piece in pieces)
    release = pieces[0].model_copy(update={"releases": tuple(piece.releases[0] for piece in pieces)})
    wrong = release.model_copy(update={"releases": (release.releases[0],
        release.releases[1].model_copy(update={"terminal_evidence_sha256": "f" * 64}))})
    with pytest.raises(ExecutionConflictError, match="exact protected and physical"):
        await store.release_shapes(capacity_session, wrong)
    assert await capacity_session.scalar(text("SELECT count(*) FROM capacity_executable_final_release_witnesses")) == 0
    await store.release_shapes(capacity_session, release)
    await capacity_session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert await capacity_session.scalar(text("SELECT count(DISTINCT command_receipt_id) FROM capacity_executable_final_release_witnesses")) == 1
    for item in release.releases:
        witness = await store.subject_final_release_witness(capacity_session,
            subject_id=item.binding.subject_id, subject_incarnation=item.binding.subject_incarnation,
            reporter_incarnation=reporter, intent_id=item.binding.intent_id)
        assert witness.release == item and witness.release.terminal_kind == "unused"
        assert witness.command_request_sha256 == canonical_executable_digest(release)
    assert (await store.release_shapes(capacity_session, release)).replayed
