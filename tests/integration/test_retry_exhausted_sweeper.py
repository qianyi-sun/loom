"""Integration tests for the retry-exhausted sweeper (issue #163).

Scenarios covered:
1. Happy path: queued trials with attempt_count >= max_attempts are
   transitioned to state='failed' with failure_reason='retry_exhausted'
   and finished_at set.
2. Trials with attempts remaining are left untouched.
3. Non-queued trials (e.g. running) with attempt_count >= max_attempts
   are left untouched — the crash detector handles those separately.
4. Idempotency: running the sweep twice produces no additional changes.
"""

from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_control_plane.retry_exhausted_sweeper import sweep_retry_exhausted


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str) -> None:  # type: ignore[return]
    """Remove all test rows after each test so tests are independent."""
    yield  # type: ignore[misc]
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(Trial))
        await s.execute(delete(TeamQuota))
        await s.execute(delete(Task))
        await s.execute(delete(Team))
        await s.commit()
    await engine.dispose()


async def _make_session_factory(postgres_url: str):  # type: ignore[return]
    """Return an async session factory for `postgres_url`."""
    engine = create_async_engine(postgres_url)
    return async_sessionmaker(engine, expire_on_commit=False), engine


async def _seed_baseline(session_factory, *, max_attempts: int = 3) -> tuple:
    """Insert a team + task row and return (team_id, task_id)."""
    team_id = uuid4()
    task_id = f"t-{uuid4().hex[:8]}"
    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"team-{team_id}"))
        await s.execute(insert(TeamQuota).values(
            team_id=team_id,
            max_attempts=max_attempts,
        ))
        await s.execute(insert(Task).values(
            id=task_id, checksum="0" * 64, config={},
        ))
        await s.commit()
    return team_id, task_id


async def test_transitions_exhausted_queued_trials_to_failed(
    postgres_url: str,
) -> None:
    """Scenario 1: three queued trials with attempt_count == max_attempts
    should all be transitioned to failed='retry_exhausted'."""
    factory, engine = await _make_session_factory(postgres_url)
    try:
        team_id, task_id = await _seed_baseline(factory, max_attempts=3)
        trial_ids = [uuid4(), uuid4(), uuid4()]
        async with factory() as s:
            for tid in trial_ids:
                await s.execute(insert(Trial).values(
                    id=tid,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={},
                    state="queued",
                    attempt_count=3,  # == max_attempts → exhausted
                ))
            await s.commit()

        async with factory() as s:
            swept = await sweep_retry_exhausted(s)
            await s.commit()

        assert len(swept) == 3
        assert set(swept) == set(trial_ids)

        async with factory() as s:
            for tid in trial_ids:
                row = (await s.execute(
                    select(Trial).where(Trial.id == tid),
                )).scalar_one()
                assert row.state == "failed", f"trial {tid}: expected failed, got {row.state}"
                assert row.failure_reason == "retry_exhausted"
                assert row.finished_at is not None
    finally:
        await engine.dispose()


async def test_leaves_trials_with_attempts_remaining_untouched(
    postgres_url: str,
) -> None:
    """Scenario 2: a queued trial with attempt_count < max_attempts must
    not be touched by the sweep."""
    factory, engine = await _make_session_factory(postgres_url)
    try:
        team_id, task_id = await _seed_baseline(factory, max_attempts=3)
        trial_id = uuid4()
        async with factory() as s:
            await s.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="queued",
                attempt_count=1,  # < max_attempts=3
            ))
            await s.commit()

        async with factory() as s:
            swept = await sweep_retry_exhausted(s)
            await s.commit()

        assert swept == []

        async with factory() as s:
            row = (await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )).scalar_one()
            assert row.state == "queued"
            assert row.failure_reason is None
            assert row.finished_at is None
    finally:
        await engine.dispose()


async def test_leaves_non_queued_trials_untouched(
    postgres_url: str,
) -> None:
    """Scenario 3: a *running* trial with attempt_count >= max_attempts
    must not be swept — it's actively being worked on; the crash detector
    handles it if the worker dies."""
    factory, engine = await _make_session_factory(postgres_url)
    try:
        team_id, task_id = await _seed_baseline(factory, max_attempts=3)
        trial_id = uuid4()
        async with factory() as s:
            await s.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="running",
                attempt_count=3,  # >= max_attempts but NOT queued
            ))
            await s.commit()

        async with factory() as s:
            swept = await sweep_retry_exhausted(s)
            await s.commit()

        assert swept == []

        async with factory() as s:
            row = (await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )).scalar_one()
            assert row.state == "running"
            assert row.failure_reason is None
    finally:
        await engine.dispose()


async def test_sweep_is_idempotent(postgres_url: str) -> None:
    """Scenario 4: running the sweep twice on the same exhausted trials
    produces no changes on the second tick (they're already failed)."""
    factory, engine = await _make_session_factory(postgres_url)
    try:
        team_id, task_id = await _seed_baseline(factory, max_attempts=2)
        trial_id = uuid4()
        async with factory() as s:
            await s.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="queued",
                attempt_count=2,  # == max_attempts
            ))
            await s.commit()

        # First sweep — should transition
        async with factory() as s:
            first = await sweep_retry_exhausted(s)
            await s.commit()
        assert len(first) == 1

        # Second sweep — trial is now failed, not queued; should be a no-op
        async with factory() as s:
            second = await sweep_retry_exhausted(s)
            await s.commit()
        assert second == []

        # State must still be failed from the first sweep
        async with factory() as s:
            row = (await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )).scalar_one()
            assert row.state == "failed"
            assert row.failure_reason == "retry_exhausted"
            assert row.finished_at is not None
    finally:
        await engine.dispose()
