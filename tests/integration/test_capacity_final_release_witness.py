"""Final release evidence is retained with the exact command, not reconstructed."""

from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.executable_contracts import (
    ExecutableIntentCloseV2,
    ExecutablePartialReleaseV2,
    ExecutableProtectedReleaseV2,
    canonical_executable_digest,
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
    async with capacity_session.begin_nested():
        with pytest.raises(DBAPIError, match="append-only"):
            await capacity_session.execute(text(mutation))
        await capacity_session.rollback()


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
