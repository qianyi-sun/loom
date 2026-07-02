from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial, Worker
from loom_control_plane.retry_exhausted_sweeper import sweep_retry_exhausted
from loom_control_plane.scheduler.crash_detector import reclaim_expired_workers


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str):  # type: ignore[no-untyped-def]
    yield
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(Trial))
        await s.execute(delete(Worker))
        await s.execute(delete(TeamQuota))
        await s.execute(delete(Team))
        await s.execute(delete(Task))
        await s.commit()
    await engine.dispose()


async def test_reclaim_expired_workers_moves_trials_to_queued(postgres_url: str):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="h",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=60),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="t",
                config={},
                requires_caps={},
                state="running",
                worker_id=worker_id,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(s, expiry_sec=15)
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "queued"
        assert row.worker_id is None
        assert row.next_attempt_at is not None

    await engine.dispose()


async def test_reclaim_leaves_fresh_workers_alone(postgres_url: str):
    """A worker with a recent heartbeat is NOT reclaimed."""
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"a-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="h",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),  # very fresh
                status="active",
            )
        )
        await s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="t",
                config={},
                requires_caps={},
                state="running",
                worker_id=worker_id,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(s, expiry_sec=15)
        await s.commit()
    assert n == 0

    await engine.dispose()


async def test_reclaim_worker_exit_before_started_records_diagnostic(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"e-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="exited",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=600),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=120),
                started_at=None,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(s, expiry_sec=300)
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "queued"
        assert row.worker_id is None
        assert row.failure_reason == "worker_lost_claim"
        assert row.failure_message is not None
        assert "claimed_without_started_reclaimed" in row.failure_message
        assert str(worker_id) in row.failure_message
        assert str(trial_id) in row.failure_message
        assert "started_at=NULL" in row.failure_message
        assert row.next_attempt_at is not None

    await engine.dispose()


async def test_reclaim_stale_claimed_without_started_at_even_when_worker_is_fresh(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"s-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fresh",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=600),
                started_at=None,
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        )
        await s.commit()
    assert n == 1

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "queued"
        assert row.worker_id is None
        assert row.failure_reason == "worker_lost_claim"
        assert row.failure_message is not None
        assert "claimed_without_started_reclaimed" in row.failure_message
        assert str(worker_id) in row.failure_message
        assert str(trial_id) in row.failure_message
        assert "started_at=NULL" in row.failure_message
        assert row.next_attempt_at is not None

    await engine.dispose()


async def test_final_pre_start_reclaim_keeps_retry_exhausted_diagnostic(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"f-{team_id}"))
        await s.execute(insert(TeamQuota).values(team_id=team_id, max_attempts=3))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fresh-final",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=600),
                started_at=None,
                attempt_count=3,
            )
        )
        await s.commit()

    async with session_factory() as s:
        reclaimed = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        )
        await s.commit()
    assert reclaimed == 1

    async with session_factory() as s:
        swept = await sweep_retry_exhausted(s)
        await s.commit()
    assert swept == [trial_id]

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "failed"
        assert row.worker_id is None
        assert row.failure_reason == "retry_exhausted"
        assert row.failure_message is not None
        assert "claimed_without_started_reclaimed" in row.failure_message
        assert str(worker_id) in row.failure_message
        assert str(trial_id) in row.failure_message
        assert row.started_at is None

    await engine.dispose()


async def test_reclaim_stale_claimed_keeps_started_trial_on_fresh_worker(
    postgres_url: str,
):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"t-{trial_id.hex}"

    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"r-{team_id}"))
        await s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fresh-running",
                version="v",
                capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_id,
                claimed_at=datetime.now(UTC) - timedelta(seconds=600),
                started_at=datetime.now(UTC) - timedelta(seconds=590),
            )
        )
        await s.commit()

    async with session_factory() as s:
        n = await reclaim_expired_workers(
            s,
            expiry_sec=15,
            claimed_without_start_expiry_sec=300,
        )
        await s.commit()
    assert n == 0

    async with session_factory() as s:
        row = (
            await s.execute(
                select(Trial).where(Trial.id == trial_id),
            )
        ).scalar_one()
        assert row.state == "claimed"
        assert row.worker_id == worker_id

    await engine.dispose()
