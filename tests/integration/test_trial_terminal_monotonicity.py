from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, Trial


@pytest.fixture
async def terminal_trial_sessions(
    postgres_url: str,
) -> AsyncIterator[tuple[async_sessionmaker, UUID]]:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    trial_id, team_id = uuid4(), uuid4()
    task_id = f"terminal-invariant/{trial_id}"
    try:
        async with sessions() as session:
            session.add_all(
                [
                    Team(id=team_id, name=f"terminal-invariant-{team_id}"),
                    Task(id=task_id, checksum="a" * 64, config={}),
                    Trial(
                        id=trial_id,
                        team_id=team_id,
                        task_id=task_id,
                        config={},
                        requires_caps={},
                        state="claimed",
                        result={"reward": 0.0},
                    ),
                ]
            )
            await session.commit()
        yield sessions, trial_id
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(Trial).where(Trial.id == trial_id))
            await session.execute(delete(Task).where(Task.id == task_id))
            await session.execute(delete(Team).where(Team.id == team_id))
        await engine.dispose()


@pytest.mark.parametrize("terminal", ["succeeded", "failed", "cancelled"])
@pytest.mark.parametrize(
    "nonterminal", ["queued", "claimed", "running", "materializing", "protected-pending"]
)
async def test_database_rejects_terminal_trial_reopening(
    terminal_trial_sessions, terminal, nonterminal
):
    sessions, trial_id = terminal_trial_sessions
    finished = datetime.now(UTC)
    async with sessions() as session:
        await session.execute(
            update(Trial).where(Trial.id == trial_id).values(state=terminal, finished_at=finished)
        )
        await session.commit()
        with pytest.raises(IntegrityError) as rejected:
            async with session.begin_nested():
                await session.execute(
                    text("UPDATE trials SET state=:state, finished_at=NULL WHERE id=:id"),
                    {"state": nonterminal, "id": trial_id},
                )
        assert rejected.value.orig.sqlstate == "23514"
        assert rejected.value.orig.diag.constraint_name == "trials_terminal_state_monotonic"
        trial = await session.get(Trial, trial_id)
        assert trial.state == terminal
        assert trial.finished_at == finished


@pytest.mark.parametrize(
    "before,after",
    [
        ("queued", "claimed"),
        ("claimed", "running"),
        ("running", "queued"),
        ("claimed", "protected-pending"),
        ("protected-pending", "queued"),
        ("running", "materializing"),
        ("materializing", "succeeded"),
        ("failed", "succeeded"),
        ("succeeded", "failed"),
        ("cancelled", "cancelled"),
    ],
)
async def test_database_preserves_trial_retry_and_terminal_correction(
    terminal_trial_sessions, before, after
):
    sessions, trial_id = terminal_trial_sessions
    async with sessions() as session:
        await session.execute(update(Trial).where(Trial.id == trial_id).values(state=before))
        await session.commit()
        await session.execute(
            update(Trial).where(Trial.id == trial_id).values(state=after, failure_message="updated")
        )
        await session.commit()
        trial = await session.get(Trial, trial_id)
        assert trial.state == after
        assert trial.failure_message == "updated"


async def test_database_rejects_cached_writer_after_concurrent_terminal_commit(
    terminal_trial_sessions,
):
    sessions, trial_id = terminal_trial_sessions
    async with sessions() as stale, sessions() as writer, sessions() as observer:
        cached = await stale.get(Trial, trial_id)
        await writer.execute(update(Trial).where(Trial.id == trial_id).values(state="cancelled"))
        stale_pid = await stale.scalar(text("SELECT pg_backend_pid()"))
        writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
        cached.state = "queued"
        pending = asyncio.create_task(stale.flush())
        try:
            async with asyncio.timeout(5):
                while not await observer.scalar(
                    text("SELECT :writer = ANY(pg_blocking_pids(:stale))"),
                    {"writer": writer_pid, "stale": stale_pid},
                ):
                    assert not pending.done(), "stale writer did not reach the row lock"
                    await asyncio.sleep(0.01)
            await writer.commit()
            with pytest.raises(IntegrityError) as rejected:
                await pending
            assert rejected.value.orig.diag.constraint_name == "trials_terminal_state_monotonic"
        finally:
            await writer.rollback()
            await asyncio.gather(pending, return_exceptions=True)
            await stale.rollback()
        trial = await observer.get(Trial, trial_id)
        assert trial.state == "cancelled"


async def test_terminal_guard_checks_before_trigger_rewrites_on_metadata_updates(
    terminal_trial_sessions,
):
    sessions, trial_id = terminal_trial_sessions
    async with sessions() as session:
        await session.execute(update(Trial).where(Trial.id == trial_id).values(state="failed"))
        await session.commit()
        # Transaction-local test trigger models a future writer that changes NEW
        # state even though the caller did not name state in its UPDATE statement.
        await session.execute(
            text("""
            CREATE FUNCTION test_reopen_terminal_trial() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN NEW.state := 'queued'; RETURN NEW; END $$;
            CREATE TRIGGER test_reopen_terminal_trial BEFORE UPDATE OF failure_message
            ON trials FOR EACH ROW EXECUTE FUNCTION test_reopen_terminal_trial();
        """)
        )
        try:
            with pytest.raises(IntegrityError) as rejected:
                async with session.begin_nested():
                    await session.execute(
                        update(Trial).where(Trial.id == trial_id).values(failure_message="late")
                    )
            assert rejected.value.orig.diag.constraint_name == "trials_terminal_state_monotonic"
        finally:
            # Roll back test-only DDL and the failed update, not shared schema.
            await session.rollback()
        trial = await session.get(Trial, trial_id)
        assert trial.state == "failed" and trial.failure_message is None


async def test_terminal_guard_rolls_back_entire_multirow_statement(terminal_trial_sessions):
    sessions, trial_id = terminal_trial_sessions
    async with sessions() as session:
        trial = await session.get(Trial, trial_id)
        trial.state = "failed"
        await session.commit()
        other = Trial(
            id=uuid4(),
            team_id=trial.team_id,
            task_id=trial.task_id,
            config={},
            requires_caps={},
            state="claimed",
        )
        session.add(other)
        await session.flush()
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                await session.execute(
                    text(
                        "UPDATE trials SET state='queued', failure_message='changed' WHERE id IN (:a,:b)"
                    ),
                    {"a": trial_id, "b": other.id},
                )
        await session.refresh(trial)
        await session.refresh(other)
        assert trial.state == "failed" and trial.failure_message is None
        assert other.state == "claimed" and other.failure_message is None
        await session.rollback()  # discard this test's second Trial
