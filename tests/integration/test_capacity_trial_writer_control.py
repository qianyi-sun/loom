"""Real PostgreSQL boundaries for nonblocking writer freeze and initialization."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_capacity_agent_store import (
    _initialize_and_register,
    _owner_session,
    _seed_trial,
)
from tests.integration.test_capacity_trial_writer_fence import (
    _control_session,
    _freeze,
    _initialize,
    _legacy_engine,
)


@pytest.mark.asyncio
async def test_freeze_refuses_active_writer_then_counts_committed_ledger(
    capacity_guard_database: dict[str, object],
) -> None:
    database = capacity_guard_database
    trial = _seed_trial(database)
    initial = await _initialize(database)
    operation = uuid4()
    engine = _legacy_engine(database)
    try:
        with engine.connect() as writer:
            writer.execute(
                text("UPDATE public.trials SET submit_priority = 101 WHERE id = :id"), {"id": trial}
            )
            with pytest.raises(DBAPIError) as refusal:
                await _freeze(database, initial["writer_incarnation"], operation)
            assert refusal.value.orig.sqlstate == "55P03"
            assert "could not obtain lock" in str(refusal.value.orig)
            # A failed control transaction releases all authority locks before
            # retry. These exclusive NOWAIT locks detect leaked shared locks.
            async with _control_session(database) as owner:
                await owner.execute(
                    text(
                        "SELECT singleton_id FROM loom_capacity_guard.authority_state FOR UPDATE NOWAIT"
                    )
                )
                await owner.execute(
                    text(
                        "SELECT agent_incarnation FROM loom_capacity_guard.agent_registrations FOR UPDATE NOWAIT"
                    )
                )
            writer.commit()
        frozen = await _freeze(database, initial["writer_incarnation"], operation)
        assert frozen["high_water"] == 1
        assert await _freeze(database, initial["writer_incarnation"], operation) == frozen
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_control_rejects_snapshot_isolation(
    capacity_guard_database: dict[str, object],
    isolation: str,
) -> None:
    database = capacity_guard_database
    initial = await _initialize(database)
    with pytest.raises(DBAPIError) as refusal:
        async with _control_session(database, isolation=isolation) as owner:
            await owner.execute(
                text(
                    "SELECT loom_capacity_guard.freeze_trial_writer(CAST(:writer AS uuid), CAST(:operation AS uuid))"
                ),
                {"writer": initial["writer_incarnation"], "operation": uuid4()},
            )
    assert refusal.value.orig.sqlstate == "25001"


@pytest.mark.asyncio
async def test_ledger_counts_insert_update_delete_but_not_noop_or_rollback(
    capacity_guard_database: dict[str, object],
) -> None:
    database = capacity_guard_database
    source = _seed_trial(database)
    initial = await _initialize(database)
    clone = uuid4()
    engine = _legacy_engine(database)
    try:
        with engine.begin() as writer:
            writer.execute(
                text("UPDATE public.trials SET submit_priority = 99 WHERE id = :id"), {"id": clone}
            )
            writer.execute(
                text(
                    "INSERT INTO public.trials (id, team_id, task_id, config, requires_caps, state, submit_priority) SELECT :clone, team_id, task_id, config, requires_caps, state, submit_priority FROM public.trials WHERE id = :source"
                ),
                {"clone": clone, "source": source},
            )
            writer.execute(
                text("UPDATE public.trials SET submit_priority = 99 WHERE id = :id"), {"id": clone}
            )
            writer.execute(text("DELETE FROM public.trials WHERE id = :id"), {"id": clone})
        with engine.connect() as rolled_back:
            rolled_back.execute(
                text("UPDATE public.trials SET submit_priority = 99 WHERE id = :id"), {"id": source}
            )
            rolled_back.rollback()
        frozen = await _freeze(database, initial["writer_incarnation"], uuid4())
        assert frozen["high_water"] == 3
        async with _control_session(database) as owner:
            assert (
                await owner.execute(
                    text(
                        "SELECT operation FROM loom_capacity_guard.trial_writer_mutations ORDER BY mutation_id"
                    )
                )
            ).scalars().all() == ["INSERT", "UPDATE", "DELETE"]
        with pytest.raises(DBAPIError, match="append-only"):
            async with _control_session(database) as owner:
                await owner.execute(text("DELETE FROM loom_capacity_guard.trial_writer_mutations"))
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
async def test_old_transaction_cannot_omit_new_initialization_boundary(
    capacity_guard_database: dict[str, object],
    isolation: str,
) -> None:
    database = capacity_guard_database
    trial = _seed_trial(database)
    _, registration = await _initialize_and_register(database)
    engine = _legacy_engine(database, isolation=isolation)
    try:
        with engine.connect() as writer:
            writer.execute(text("SELECT count(*) FROM public.trials")).scalar_one()
            initial = await _initialize(database, registration=registration)
            update = text("UPDATE public.trials SET submit_priority = 101 WHERE id = :id")
            if isolation == "READ COMMITTED":
                writer.execute(update, {"id": trial})
                writer.commit()
            else:
                with pytest.raises(DBAPIError) as refusal:
                    writer.execute(update, {"id": trial})
                assert refusal.value.orig.sqlstate == "40001"
                writer.rollback()
        frozen = await _freeze(database, initial["writer_incarnation"], uuid4())
        assert frozen["high_water"] == (1 if isolation == "READ COMMITTED" else 0)
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_reconfiguration", [False, True])
async def test_freeze_serializes_with_actual_authority_and_agent_reconfiguration(
    capacity_guard_database: dict[str, object],
    commit_reconfiguration: bool,
) -> None:
    database = capacity_guard_database
    fence, registration = await _initialize_and_register(database)
    initial = await _initialize(database, registration=registration)
    operation = uuid4()
    replacement_fence = fence.model_copy(update={"configuration_generation": 12})
    replacement_registration = registration.model_copy(update={"configuration_generation": 12})
    async with _owner_session(database) as (agent_store, guard_store, session):
        await guard_store.reconfigure_disabled_authority(
            replacement_fence, expected_configuration_generation=11
        )
        await agent_store.reconfigure_agent(
            replacement_registration, expected_configuration_generation=11
        )
        # Use the real stores, retaining their transaction's row locks while a
        # separate READ COMMITTED connection attempts the protected freeze.
        with pytest.raises(DBAPIError) as busy:
            await _freeze(database, initial["writer_incarnation"], operation)
        assert busy.value.orig.sqlstate == "55P03"
        if not commit_reconfiguration:
            await session.rollback()

    if commit_reconfiguration:
        # A committed rollover must not silently relabel the old writer binding.
        # A protected rollover operation is still needed before live use.
        with pytest.raises(DBAPIError) as stale:
            await _freeze(database, initial["writer_incarnation"], operation)
        assert stale.value.orig.sqlstate == "55000"
        assert "binding changed" in str(stale.value.orig)
    else:
        frozen = await _freeze(database, initial["writer_incarnation"], operation)
        assert frozen["frozen"] is True
        assert frozen["high_water"] == 0


@pytest.mark.asyncio
async def test_initialization_refuses_unbound_active_writer_then_retries(
    capacity_guard_database: dict[str, object],
) -> None:
    database = capacity_guard_database
    trial = _seed_trial(database)
    _, registration = await _initialize_and_register(database)
    engine = _legacy_engine(database)
    try:
        with engine.connect() as writer:
            writer.execute(
                text("UPDATE public.trials SET submit_priority = 101 WHERE id = :id"),
                {"id": trial},
            )
            with pytest.raises(DBAPIError) as busy:
                await _initialize(database, registration=registration)
            assert busy.value.orig.sqlstate == "55P03"
            writer.commit()
        initial = await _initialize(database, registration=registration)
        with engine.begin() as writer:
            writer.execute(
                text("UPDATE public.trials SET submit_priority = 102 WHERE id = :id"),
                {"id": trial},
            )
        frozen = await _freeze(database, initial["writer_incarnation"], uuid4())
        # Only the committed write after the explicit initialization boundary
        # belongs to this incarnation, not the earlier unbound mutation.
        assert frozen["high_water"] == 1
    finally:
        engine.dispose()
