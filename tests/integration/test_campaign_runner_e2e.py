"""run_once fans a campaign into per-task trial submissions, with
idempotency_key carrying the campaign+task identity (Plan 19 Task 5).

The Control Plane is mocked via httpx.MockTransport — the handler
mirrors the real CP's INSERT into the trials table so the test
exercises the round trip: service runner → /trials → DB row →
service re-reads via Trial.campaign_id.
"""

from __future__ import annotations

import hashlib
import json as _json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Campaign, Task, Team, TeamQuota, Token, Trial
from loom_service.campaign_runner import (
    _idempotency_key,
    next_campaign_state,
    run_once,
)


@pytest.fixture
async def runner_setup(
    postgres_url: str,
) -> AsyncIterator[tuple[
    async_sessionmaker, httpx.AsyncClient, UUID, list[str],
]]:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    team_id = uuid4()
    task_ids = [f"local/runner-{i}" for i in range(5)]
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        for tid in task_ids:
            s.execute(insert(Task).values(
                id=tid, checksum="x" * 64, config={},
                source="local", license="MIT",
            ))
        s.commit()
    sync_engine.dispose()

    # Mock CP: handler that mirrors the real INSERT (with idempotency
    # ON CONFLICT semantics — checks for existing key, returns its id).
    captured: list[dict] = []

    def cp_handler(req: httpx.Request) -> httpx.Response:
        if req.url.path != "/trials" or req.method != "POST":
            return httpx.Response(404)
        body = _json.loads(req.content.decode())
        captured.append(body)
        e = create_engine(postgres_url)
        local = sessionmaker(e)
        try:
            with local() as s:
                idem = body.get("idempotency_key")
                if idem is not None:
                    existing = s.execute(
                        select(Trial).where(
                            Trial.idempotency_key == idem,
                        ),
                    ).scalar_one_or_none()
                    if existing is not None:
                        return httpx.Response(
                            201,
                            json={
                                "trial_id": str(existing.id),
                                "state": existing.state,
                                "submitted_at": (
                                    existing.submitted_at.isoformat()
                                ),
                            },
                        )
                new_id = uuid4()
                s.execute(insert(Trial).values(
                    id=new_id,
                    task_id=body["task_id"],
                    team_id=team_id,
                    state="queued",
                    config=body["config"],
                    requires_caps={},
                    submitted_at=datetime.now(UTC),
                    campaign_id=(
                        UUID(body["campaign_id"])
                        if body.get("campaign_id") else None
                    ),
                    idempotency_key=idem,
                ))
                s.commit()
            return httpx.Response(
                201,
                json={
                    "trial_id": str(new_id),
                    "state": "queued",
                    "submitted_at": datetime.now(UTC).isoformat(),
                },
            )
        finally:
            e.dispose()

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(cp_handler),
        base_url="http://cp",
    )

    try:
        yield session_factory, http_client, team_id, task_ids
    finally:
        await http_client.aclose()
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Campaign))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_runner_fans_out_5_trials(
    runner_setup: tuple[async_sessionmaker, httpx.AsyncClient, UUID, list[str]],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids = runner_setup
    async with session_factory() as s:
        c = Campaign(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={"agent": {"name": "fake"}},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.campaign_id == cid),
        ).scalars().all()
        campaign_row = s.execute(
            select(Campaign).where(Campaign.id == cid),
        ).scalar_one()
    sync_engine.dispose()

    assert len(trials) == 5
    assert {t.task_id for t in trials} == set(task_ids)
    # Each trial carries the deterministic idempotency_key.
    assert all(
        t.idempotency_key == _idempotency_key(cid, t.task_id)
        for t in trials
    )
    # State transitioned: submitted → running (trials in queued state).
    assert campaign_row.state == "running"


async def test_runner_is_idempotent(
    runner_setup: tuple[async_sessionmaker, httpx.AsyncClient, UUID, list[str]],
    postgres_url: str,
) -> None:
    """Running the runner twice produces exactly 5 trials, not 10."""
    session_factory, http_client, team_id, _task_ids = runner_setup
    async with session_factory() as s:
        c = Campaign(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    for _ in range(2):
        await run_once(
            session_factory=session_factory,
            http_client=http_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.campaign_id == cid),
        ).scalars().all()
    sync_engine.dispose()
    assert len(trials) == 5


async def test_runner_advances_to_finished_when_all_terminal(
    runner_setup: tuple[async_sessionmaker, httpx.AsyncClient, UUID, list[str]],
    postgres_url: str,
) -> None:
    """After the runner submits, externally mark every trial succeeded
    and run again — the runner should transition the campaign to
    finished."""
    session_factory, http_client, team_id, _task_ids = runner_setup
    async with session_factory() as s:
        c = Campaign(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    await run_once(
        session_factory=session_factory, http_client=http_client,
        batch_size=10, submit_rate_per_sec=100,
    )

    # Mark every trial succeeded.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    from sqlalchemy import update
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.campaign_id == cid)
            .values(state="succeeded", finished_at=datetime.now(UTC)),
        )
        s.commit()
    sync_engine.dispose()

    # Tick once more — no new trials to submit; state should advance.
    await run_once(
        session_factory=session_factory, http_client=http_client,
        batch_size=10, submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        campaign_row = s.execute(
            select(Campaign).where(Campaign.id == cid),
        ).scalar_one()
    sync_engine.dispose()
    assert campaign_row.state == "finished"
    assert campaign_row.finished_at is not None


# Sanity import to keep `next_campaign_state` referenced.
_ = next_campaign_state
