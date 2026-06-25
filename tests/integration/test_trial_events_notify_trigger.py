"""#5 Slice 3d — trial_events INSERT trigger fires NOTIFY on the
`trial_events_inserted` channel with `<trial_id>:<seq>` payload.

Consumer wiring for the SSE /stream endpoint follows in a separate
PR (Slice 3e); this just pins the DB-side contract so the consumer
can be built against a known channel name + payload format."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import psycopg
import pytest
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    TrialEvent,
    Worker,
)

_CHANNEL = "trial_events_inserted"


@pytest.fixture
def seed(postgres_url: str) -> Iterator[tuple[UUID, UUID]]:
    """Insert a trial we can attach events to."""
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    raw = f"w_{uuid4().hex}"
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:index"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Worker).values(
            id=worker_id, hostname="h", version="v", capabilities=[],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC), status="active",
        ))
        s.execute(insert(Task).values(
            id="notify-task", checksum="0" * 64, config={},
        ))
        s.execute(insert(Trial).values(
            id=trial_id, team_id=team_id, task_id="notify-task",
            config={}, requires_caps={}, state="running",
            worker_id=worker_id,
        ))
        s.commit()
    try:
        yield trial_id, worker_id
    finally:
        with sl() as s:
            s.execute(delete(TrialEvent))
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


def _psycopg_dsn_from_sqla(url: str) -> str:
    """SQLAlchemy URLs prefix `postgresql+psycopg://`; psycopg wants
    the bare `postgresql://` form."""
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url[len("postgresql+psycopg://"):]
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url[len("postgresql+asyncpg://"):]
    return url


async def test_trigger_fires_notify_with_trial_seq_payload(
    seed: tuple[UUID, UUID], postgres_url: str,
) -> None:
    """An INSERT into trial_events produces a NOTIFY on the
    `trial_events_inserted` channel with `<trial_id>:<seq>` payload.
    Consumers (SSE /stream, future CLI tools) can filter by trial_id
    prefix without hitting the table."""
    trial_id, _worker_id = seed
    dsn = _psycopg_dsn_from_sqla(postgres_url)

    received: list[str] = []
    listen_ready = asyncio.Event()

    async def listener() -> None:
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True,
        ) as conn:
            await conn.execute(f"LISTEN {_CHANNEL}")
            listen_ready.set()
            async for notify in conn.notifies():
                received.append(notify.payload)
                if len(received) >= 2:
                    return

    task = asyncio.create_task(listener())
    await listen_ready.wait()

    # Insert two events via a fresh connection so the NOTIFY commits
    # land on the listener.
    async with await psycopg.AsyncConnection.connect(dsn) as writer:
        async with writer.cursor() as cur:
            await cur.execute(
                "INSERT INTO trial_events "
                "(trial_id, seq, kind, source, schema_version, payload) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
                (
                    str(trial_id), 0, "trial_start", "worker", 1,
                    '{"seq":0,"kind":"trial_start"}',
                ),
            )
            await cur.execute(
                "INSERT INTO trial_events "
                "(trial_id, seq, kind, source, schema_version, payload) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
                (
                    str(trial_id), 1, "step_start", "worker", 1,
                    '{"seq":1,"kind":"step_start"}',
                ),
            )
        await writer.commit()

    try:
        await asyncio.wait_for(task, timeout=5.0)
    except TimeoutError as exc:
        task.cancel()
        raise AssertionError(
            f"only received {len(received)} notifies in 5s: {received!r}",
        ) from exc

    assert len(received) == 2
    assert received[0] == f"{trial_id}:0"
    assert received[1] == f"{trial_id}:1"


async def test_trigger_payload_lets_listener_filter_by_trial(
    seed: tuple[UUID, UUID], postgres_url: str,
) -> None:
    """The payload format (`<trial_id>:<seq>`) lets a single LISTEN
    connection serve many trials by string-prefix filtering. Pins
    that contract for the future SSE consumer."""
    target_trial_id, _ = seed
    other_trial_id = uuid4()
    dsn = _psycopg_dsn_from_sqla(postgres_url)

    # Seed the OTHER trial so its FK constraint passes.
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    other_team = uuid4()
    with sl() as s:
        s.execute(insert(Team).values(id=other_team, name=f"y-{other_team}"))
        s.execute(insert(TeamQuota).values(team_id=other_team))
        s.execute(insert(Trial).values(
            id=other_trial_id, team_id=other_team, task_id="notify-task",
            config={}, requires_caps={}, state="running",
        ))
        s.commit()
    engine.dispose()

    received_payloads: list[str] = []
    listen_ready = asyncio.Event()
    target_prefix = f"{target_trial_id}:"

    async def listener() -> None:
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True,
        ) as conn:
            await conn.execute(f"LISTEN {_CHANNEL}")
            listen_ready.set()
            async for notify in conn.notifies():
                # Mimic the consumer filter: only keep notifies for
                # OUR target trial.
                if notify.payload.startswith(target_prefix):
                    received_payloads.append(notify.payload)
                    return

    task = asyncio.create_task(listener())
    await listen_ready.wait()

    async with await psycopg.AsyncConnection.connect(dsn) as writer:
        async with writer.cursor() as cur:
            # Insert against OTHER trial first — listener filters it out.
            await cur.execute(
                "INSERT INTO trial_events "
                "(trial_id, seq, kind, source, schema_version, payload) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
                (
                    str(other_trial_id), 0, "trial_start", "worker", 1,
                    '{"seq":0,"kind":"trial_start"}',
                ),
            )
            # Then insert against TARGET trial — listener accepts.
            await cur.execute(
                "INSERT INTO trial_events "
                "(trial_id, seq, kind, source, schema_version, payload) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
                (
                    str(target_trial_id), 0, "trial_start", "worker", 1,
                    '{"seq":0,"kind":"trial_start"}',
                ),
            )
        await writer.commit()

    try:
        await asyncio.wait_for(task, timeout=5.0)
    finally:
        # Cleanup the OTHER trial.
        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            s.execute(delete(TrialEvent).where(
                TrialEvent.trial_id == other_trial_id,
            ))
            s.execute(delete(Trial).where(Trial.id == other_trial_id))
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == other_team))
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        engine.dispose()

    assert received_payloads == [f"{target_trial_id}:0"]
