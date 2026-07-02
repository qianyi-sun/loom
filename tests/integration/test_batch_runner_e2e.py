"""run_once fans a batch into per-task trial submissions, with
idempotency_key carrying the batch+task identity (Plan 19 Task 5).

The Control Plane is mocked via httpx.MockTransport — the handler
mirrors the real CP's INSERT into the trials table so the test
exercises the round trip: service runner → /trials → DB row →
service re-reads via Trial.batch_id.
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

from loom.db.schema import (
    Batch,
    Benchmark,
    ProviderConnection,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
)
from loom_service.batch_runner import (
    _idempotency_key,
    next_batch_state,
    run_once,
)


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


@pytest.fixture
async def runner_setup(
    postgres_url: str,
) -> AsyncIterator[tuple[
    async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
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
        s.execute(insert(Benchmark).values(
            id="runner-benchmark",
            display_name="Runner Benchmark",
            upstream_kind="fixture",
            upstream_locator="local",
            upstream_revision="",
            license_spdx="MIT",
            license_url="https://example/license",
            splits=["test"],
        ))
        for tid in task_ids:
            s.execute(insert(Task).values(
                id=tid, checksum="x" * 64,
                config=_valid_task_config(tid),
                source="local", license="MIT",
                benchmark_id="runner-benchmark",
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
                    batch_id=(
                        UUID(body["batch_id"])
                        if body.get("batch_id") else None
                    ),
                    idempotency_key=idem,
                    sample_idx=int(body.get("sample_idx") or 0),
                    combination_idx=int(body.get("combination_idx") or 0),
                    provider_connection_id=(
                        UUID(body["provider_connection_id"])
                        if body.get("provider_connection_id") else None
                    ),
                    provider_model_id=body.get("provider_model_id"),
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
        yield session_factory, http_client, team_id, task_ids, captured
    finally:
        await http_client.aclose()
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_runner_fans_out_5_trials(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
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
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()

    assert len(trials) == 5
    assert {t.task_id for t in trials} == set(task_ids)
    # Each trial carries the deterministic idempotency_key (sample 0).
    assert all(
        t.idempotency_key == _idempotency_key(cid, t.task_id, 0)
        for t in trials
    )
    assert all(t.sample_idx == 0 for t in trials)
    # State transitioned: submitted → running (trials in queued state).
    assert batch_row.state == "running"


async def test_runner_fans_out_required_worker_pool_coverage_units(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="coverage",
            task_filter={
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "first_n",
                "n": 1,
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=3,
            required_worker_pools=["oldlab", "k8s-worker"],
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

    assert [body.get("required_worker_pool") for body in captured] == [
        None,
        "oldlab",
        "k8s-worker",
    ]
    assert [body["idempotency_key"] for body in captured] == [
        _idempotency_key(cid, task_ids[0], 0),
        _idempotency_key(
            cid,
            task_ids[0],
            1,
            required_worker_pool="oldlab",
        ),
        _idempotency_key(
            cid,
            task_ids[0],
            2,
            required_worker_pool="k8s-worker",
        ),
    ]

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.sample_idx.asc()),
        ).scalars().all()
    sync_engine.dispose()

    assert len(trials) == 3
    assert [t.sample_idx for t in trials] == [0, 1, 2]


async def test_runner_is_idempotent(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    """Running the runner twice produces exactly 5 trials, not 10."""
    session_factory, http_client, team_id, _task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
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
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
    sync_engine.dispose()
    assert len(trials) == 5


async def test_runner_uses_rerun_targets_instead_of_original_filter(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    targets = [
        {
            "task_id": task_ids[1],
            "sample_idx": 2,
            "combination_idx": 0,
            "original_trial_id": str(uuid4()),
            "failure_reason": "gateway_error",
        },
        {
            "task_id": task_ids[3],
            "sample_idx": 1,
            "combination_idx": 0,
            "original_trial_id": str(uuid4()),
            "failure_reason": "gateway_error",
        },
    ]
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="rerun",
            task_filter={"license": "MIT"},
            trial_config={"agent": {"name": "fake"}},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=2,
            n_per_task=99,
            rerun_targets=targets,
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

    assert [body["task_id"] for body in captured] == [task_ids[1], task_ids[3]]
    assert [body["sample_idx"] for body in captured] == [2, 1]
    assert [body["combination_idx"] for body in captured] == [0, 0]
    assert [body["idempotency_key"] for body in captured] == [
        _idempotency_key(cid, task_ids[1], 2, combination_idx=0),
        _idempotency_key(cid, task_ids[3], 1, combination_idx=0),
    ]

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
    sync_engine.dispose()
    assert [(t.task_id, t.sample_idx, t.combination_idx) for t in trials] == [
        (task_ids[1], 2, 0),
        (task_ids[3], 1, 0),
    ]


async def test_runner_advances_to_finished_when_all_terminal(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    """After the runner submits, externally mark every trial succeeded
    and run again — the runner should transition the batch to
    finished."""
    session_factory, http_client, team_id, _task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
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
            .where(Trial.batch_id == cid)
            .values(
                state="succeeded",
                result={"aggregate_reward": 1.0},
                finished_at=datetime.now(UTC),
            ),
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
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()
    assert batch_row.state == "finished"
    assert batch_row.result_status == "succeeded"
    assert batch_row.finished_at is not None


async def test_runner_fans_out_n_samples_per_task(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    """Plan 23: batch.n_per_task=3 produces 3 trials per matched
    task, each with a distinct sample_idx 0..2, each carrying the
    sample-aware idempotency key. Re-running the runner is still
    idempotent — no duplicates."""
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id, name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=15,
            n_per_task=3,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    for _ in range(2):  # 2nd tick must NOT duplicate
        await run_once(
            session_factory=session_factory, http_client=http_client,
            batch_size=10, submit_rate_per_sec=100,
        )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
    sync_engine.dispose()

    assert len(trials) == 15
    # Every (task_id, sample_idx) pair shows up exactly once.
    pairs = {(t.task_id, t.sample_idx) for t in trials}
    assert pairs == {(tid, s_idx) for tid in task_ids for s_idx in range(3)}
    # Idempotency keys include sample_idx and are unique.
    keys = {t.idempotency_key for t in trials}
    assert len(keys) == 15
    assert all(
        t.idempotency_key
        == _idempotency_key(cid, t.task_id, t.sample_idx)
        for t in trials
    )


async def test_runner_honors_benchmark_ids_first_n_filter(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "first_n",
                "n": 1,
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=1,
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
            select(Trial)
            .where(Trial.batch_id == cid)
            .order_by(Trial.task_id.asc()),
        ).scalars().all()
    sync_engine.dispose()

    assert [t.task_id for t in trials] == [task_ids[0]]


async def test_runner_honors_subset_filter_variants(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, _captured = runner_setup
    cases = [
        (
            "last",
            {
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "last_n",
                "n": 2,
            },
            task_ids[-2:],
        ),
        (
            "random",
            {
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "random_n",
                "n": 2,
                "seed": 42,
            },
            ["local/runner-0", "local/runner-4"],
        ),
        (
            "explicit",
            {
                "task_ids": [task_ids[3], task_ids[1]],
                "subset_kind": "explicit",
            },
            [task_ids[1], task_ids[3]],
        ),
    ]

    created: list[tuple[UUID, list[str]]] = []
    async with session_factory() as s:
        for name, task_filter, expected in cases:
            c = Batch(
                team_id=team_id,
                name=name,
                task_filter=task_filter,
                trial_config={},
                state="submitted",
                created_by_token_prefix="abcdef12",
                expected_trial_count=len(expected),
            )
            s.add(c)
            await s.flush()
            created.append((c.id, expected))
        await s.commit()

    await run_once(
        session_factory=session_factory,
        http_client=http_client,
        batch_size=10,
        submit_rate_per_sec=100,
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for batch_id, expected in created:
            trials = s.execute(
                select(Trial)
                .where(Trial.batch_id == batch_id)
                .order_by(Trial.task_id.asc()),
            ).scalars().all()
            assert [t.task_id for t in trials] == expected
    sync_engine.dispose()


async def test_runner_honors_empty_benchmark_ids_filter(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, _task_ids, _captured = runner_setup
    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="empty",
            task_filter={"benchmark_ids": []},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=0,
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
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
    sync_engine.dispose()

    assert trials == []


async def test_runner_forwards_batch_provider_connection_fields(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, _task_ids, captured = runner_setup
    conn_id = uuid4()
    async with session_factory() as s:
        s.add(ProviderConnection(
            id=conn_id,
            team_id=team_id,
            provider_type="openai-compatible",
            display_name="Lab vLLM",
            base_url="https://api.openai.com/v1",
            upstream_host="api.openai.com",
            resolved_egress_ips=["104.18.0.1"],
            encrypted_api_key_ref="test://runner",
            status="valid",
            pricing_source="tokens-only",
            rate_card_provider="openai",
            created_by="test:runner",
        ))
        await s.flush()
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={"license": "MIT"},
            trial_config={"agent_name": "litellm"},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=5,
            provider_connection_id=conn_id,
            provider_model_id="deepseek-chat",
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

    assert captured
    assert {body["provider_connection_id"] for body in captured} == {
        str(conn_id),
    }
    assert {body["provider_model_id"] for body in captured} == {
        "deepseek-chat",
    }

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
    sync_engine.dispose()
    assert {t.provider_connection_id for t in trials} == {conn_id}
    assert {t.provider_model_id for t in trials} == {"deepseek-chat"}


async def test_runner_skips_invalid_task_configs_and_adjusts_expected(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, task_ids, captured = runner_setup
    bad_task_id = "local/runner-broken"
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Task).values(
            id=bad_task_id,
            checksum="b" * 64,
            config={},
            source="local",
            license="MIT",
        ))
    sync_engine.dispose()

    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={"license": "MIT"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=6,
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

    submitted_task_ids = {body["task_id"] for body in captured}
    assert submitted_task_ids == set(task_ids)
    assert bad_task_id not in submitted_task_ids

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()
    assert batch_row.expected_trial_count == 5
    assert batch_row.result_status == "partial_failed"
    assert batch_row.state == "running"


async def test_runner_finishes_batch_when_all_task_configs_are_invalid(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, http_client, team_id, _task_ids, captured = runner_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        for idx in range(2):
            conn.execute(insert(Task).values(
                id=f"local/all-broken-{idx}",
                checksum=str(idx) * 64,
                config={},
                source="local",
                license="BrokenOnly",
            ))
    sync_engine.dispose()

    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="C",
            task_filter={"license": "BrokenOnly"},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=2,
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

    assert captured == []
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()
    assert batch_row.expected_trial_count == 0
    assert batch_row.state == "finished"
    assert batch_row.result_status == "all_failed"
    assert batch_row.finished_at is not None


async def test_runner_finishes_batch_when_trial_submit_policy_rejects_task(
    runner_setup: tuple[
        async_sessionmaker, httpx.AsyncClient, UUID, list[str], list[dict],
    ],
    postgres_url: str,
) -> None:
    session_factory, _http_client, team_id, task_ids, _captured = runner_setup

    async with session_factory() as s:
        c = Batch(
            team_id=team_id,
            name="policy-blocked",
            task_filter={
                "benchmark_ids": ["runner-benchmark"],
                "subset_kind": "first_n",
                "n": 1,
            },
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=1,
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        cid = c.id

    rejected: list[dict] = []

    def rejecting_cp_handler(req: httpx.Request) -> httpx.Response:
        if req.url.path != "/trials" or req.method != "POST":
            return httpx.Response(404)
        body = _json.loads(req.content.decode())
        rejected.append(body)
        return httpx.Response(
            403,
            json={
                "detail": "task license proprietary-MAA not in team allowlist",
            },
        )

    rejecting_client = httpx.AsyncClient(
        transport=httpx.MockTransport(rejecting_cp_handler),
        base_url="http://cp",
    )
    try:
        await run_once(
            session_factory=session_factory,
            http_client=rejecting_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )
        await run_once(
            session_factory=session_factory,
            http_client=rejecting_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )
    finally:
        await rejecting_client.aclose()

    assert [body["task_id"] for body in rejected] == [task_ids[0]]

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        trials = s.execute(
            select(Trial).where(Trial.batch_id == cid),
        ).scalars().all()
        batch_row = s.execute(
            select(Batch).where(Batch.id == cid),
        ).scalar_one()
    sync_engine.dispose()

    assert trials == []
    assert batch_row.expected_trial_count == 0
    assert batch_row.state == "finished"
    assert batch_row.result_status == "all_failed"
    assert batch_row.failure_reason == "fanout_submit_failed"
    assert "proprietary-MAA" in (batch_row.failure_message or "")
    assert batch_row.finished_at is not None


# Sanity import to keep `next_batch_state` referenced.
_ = next_batch_state
