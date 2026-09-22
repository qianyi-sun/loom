"""Real SQL interception for the trial domain, not complete fleet-freeze proof."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.support.historical_capacity import (
    _value,
    historical_agent_rows,
    seed_unprotected_trial,
)


@asynccontextmanager
async def _control_session(database: dict[str, object], *, isolation: str = "READ COMMITTED"):
    engine = create_async_engine(_value(database, "migrator_url"), isolation_level=isolation)
    owner = engine.sync_engine.dialect.identifier_preparer.quote(_value(database, "owner_role"))
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"SET LOCAL ROLE {owner}"))
            yield connection
    finally:
        await engine.dispose()


async def _initialize(database: dict[str, object], *, registration=None) -> dict[str, object]:
    if registration is None:
        _, registration = await historical_agent_rows(database)
    async with _control_session(database) as session:
        result = await session.execute(
            text(
                "SELECT loom_capacity_guard.initialize_trial_writer_fence("
                "CAST(:agent AS uuid), CAST(:writer AS uuid))"
            ),
            {"agent": registration.agent_incarnation, "writer": uuid4()},
        )
        observation = result.scalar_one()
    assert isinstance(observation, dict)
    assert observation["subject_id"] == str(registration.subject_id)
    assert observation["high_water"] == 0
    assert observation["frozen"] is False
    return observation


async def _freeze(
    database: dict[str, object], writer: object, operation: UUID
) -> dict[str, object]:
    async with _control_session(database) as session:
        result = await session.execute(
            text(
                "SELECT loom_capacity_guard.freeze_trial_writer("
                "CAST(:writer AS uuid), CAST(:operation AS uuid))"
            ),
            {"writer": writer, "operation": operation},
        )
        observation = result.scalar_one()
    assert isinstance(observation, dict)
    return observation


def _legacy_engine(database: dict[str, object], *, isolation: str = "READ COMMITTED"):
    # Test-only provisioning of an ordinary direct writer. The existing runtime
    # role is a distinct non-owner LOGIN with no privileged role memberships.
    admin = create_engine(_value(database, "admin_url"))
    role = admin.dialect.identifier_preparer.quote(_value(database, "runtime_role"))
    try:
        with admin.begin() as connection:
            connection.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {role}")
            connection.exec_driver_sql(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.trials TO {role}"
            )
    finally:
        admin.dispose()
    return create_engine(_value(database, "runtime_url"), isolation_level=isolation)


@pytest.mark.asyncio
async def test_trial_writer_counts_committed_writes_not_observations_or_rollbacks(
    capacity_guard_database: dict[str, object],
) -> None:
    database = capacity_guard_database
    trial = seed_unprotected_trial(database)
    initial = await _initialize(database)
    engine = _legacy_engine(database)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE public.trials SET submit_priority = 101 WHERE id = :id"),
                {"id": trial},
            )
        with engine.connect() as connection:
            connection.execute(
                text("UPDATE public.trials SET submit_priority = 102 WHERE id = :id"),
                {"id": trial},
            )
            connection.rollback()
            assert (
                connection.execute(
                    text("SELECT submit_priority FROM public.trials WHERE id = :id"),
                    {"id": trial},
                ).scalar_one()
                == 101
            )
        operation = uuid4()
        frozen = await _freeze(database, initial["writer_incarnation"], operation)
        assert frozen["high_water"] == 1
        assert frozen["frozen"] is True
        assert frozen["freeze_operation_id"] == str(operation)
        assert await _freeze(database, initial["writer_incarnation"], operation) == frozen
        with pytest.raises(DBAPIError, match="frozen"):
            with engine.begin() as connection:
                connection.execute(
                    text("UPDATE public.trials SET submit_priority = 103 WHERE id = :id"),
                    {"id": trial},
                )
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
async def test_trial_writer_transaction_open_before_freeze_cannot_write_afterward(
    capacity_guard_database: dict[str, object], isolation: str
) -> None:
    database = capacity_guard_database
    trial = seed_unprotected_trial(database)
    initial = await _initialize(database)
    engine = _legacy_engine(database, isolation=isolation)
    try:
        with engine.connect() as stale:
            # Establish an actual transaction snapshot before another session
            # freezes the domain. The first mutation comes after freeze commits.
            stale.execute(text("SELECT count(*) FROM public.trials")).scalar_one()
            frozen = await _freeze(database, initial["writer_incarnation"], uuid4())
            assert frozen["high_water"] == 0
            with pytest.raises(DBAPIError) as rejected:
                stale.execute(
                    text("UPDATE public.trials SET submit_priority = 999 WHERE id = :id"),
                    {"id": trial},
                )
            assert rejected.value.orig.sqlstate in {"55000", "40001"}
            stale.rollback()
        with engine.connect() as restarted:
            assert (
                restarted.execute(
                    text("SELECT submit_priority FROM public.trials WHERE id = :id"),
                    {"id": trial},
                ).scalar_one()
                == 100
            )
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_trial_writer_ordinary_login_cannot_freeze_or_rewrite_counter(
    capacity_guard_database: dict[str, object],
) -> None:
    database = capacity_guard_database
    initial = await _initialize(database)
    engine = _legacy_engine(database)
    try:
        for statement in (
            "UPDATE loom_capacity_guard.trial_writer_fence SET high_water = 0",
            "DELETE FROM loom_capacity_guard.trial_writer_mutations",
            "SELECT loom_capacity_guard.freeze_trial_writer("
            "CAST(:writer AS uuid), CAST(:operation AS uuid))",
        ):
            with pytest.raises(DBAPIError) as rejected:
                with engine.begin() as connection:
                    connection.execute(
                        text(statement),
                        {"writer": initial["writer_incarnation"], "operation": uuid4()},
                    )
            assert rejected.value.orig.sqlstate == "42501"
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", [False, True])
async def test_trial_writer_preserves_existing_lock_then_update_transaction_order(
    capacity_guard_database: dict[str, object],
    bound: bool,
) -> None:
    """The result route locks its trial before the later UPDATE statement."""
    database = capacity_guard_database
    trial = seed_unprotected_trial(database)
    initialized = await _initialize(database) if bound else None
    provisioner = _legacy_engine(database)
    provisioner.dispose()
    engine = create_async_engine(_value(database, "runtime_url"))
    observer = create_async_engine(_value(database, "admin_url"))
    update = text("UPDATE public.trials SET submit_priority = submit_priority + 1 WHERE id = :id")
    pending = None
    try:
        async with engine.connect() as first, engine.connect() as second:
            await first.execute(text("SET statement_timeout = '5s'"))
            await second.execute(text("SET statement_timeout = '5s'"))
            first_pid = (await first.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            second_pid = (await second.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            await first.execute(
                text("SELECT id FROM public.trials WHERE id = :id FOR UPDATE"), {"id": trial}
            )
            pending = asyncio.create_task(second.execute(update, {"id": trial}))
            # Wait for the real conflicting row lock, not a guessed sleep.
            async with observer.connect() as probe:
                async with asyncio.timeout(5):
                    while not (
                        await probe.execute(
                            text("SELECT :holder = ANY(pg_catalog.pg_blocking_pids(:waiter))"),
                            {"holder": first_pid, "waiter": second_pid},
                        )
                    ).scalar_one():
                        if pending.done():
                            await pending
                            pytest.fail("competing UPDATE did not wait for the locked trial")
                        await asyncio.sleep(0.01)
            await first.execute(update, {"id": trial})
            await first.commit()
            await pending
            await second.commit()
        async with engine.connect() as readback:
            assert (
                await readback.execute(
                    text("SELECT submit_priority FROM public.trials WHERE id = :id"),
                    {"id": trial},
                )
            ).scalar_one() == 102
        if initialized is not None:
            frozen = await _freeze(database, initialized["writer_incarnation"], uuid4())
            assert frozen["high_water"] == 2
    finally:
        if pending is not None:
            pending.cancel()
            with suppress(asyncio.CancelledError, DBAPIError):
                await pending
        await engine.dispose()
        await observer.dispose()
