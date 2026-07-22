"""Accepted batches retain their profile-resolved task selection (#749)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    Batch,
    Benchmark,
    BenchmarkAlias,
    Task,
    Team,
    TeamMembership,
    Token,
    Trial,
    User,
    Worker,
)
from loom_service.app import create_app
from loom_service.batch_runner import run_once
from loom_service.config import LoomServiceSettings

PUBLIC_ALIAS = "terminal-bench-2"
ACTIVE_PROFILE = "terminal-bench-2@tb2.1-r6"
NEXT_PROFILE = "terminal-bench-2@tb2.2-r7"
HISTORICAL_PROFILE = "terminal-bench-2@tb2.0-91e10457"
ACTIVE_TASK_ID = f"{ACTIVE_PROFILE}/chess-best-move"
NEXT_TASK_ID = f"{NEXT_PROFILE}/chess-best-move"
HISTORICAL_TASK_ID = f"{HISTORICAL_PROFILE}/legacy-chess-best-move"


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
async def profile_batch_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID, async_sessionmaker[AsyncSession]]]:
    for key, value in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)

    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.settings = settings
    app.state.session_factory = session_factory
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token("loom_admin_" + "A" * 43)
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    team_id = uuid4()
    user_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sync_session = sessionmaker(sync_engine)
    with sync_session() as sync:
        sync.execute(insert(Team).values(id=team_id, name=f"tb21-{team_id}"))
        sync.execute(
            insert(User).values(
                id=user_id,
                username=f"tb21-owner-{team_id.hex[:8]}",
                username_normalized=f"tb21-owner-{team_id.hex[:8]}",
                status="active",
                is_platform_admin=False,
            ),
        )
        sync.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_token.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=datetime.now(UTC),
            ),
        )
        sync.execute(
            insert(TeamMembership).values(team_id=team_id, user_id=user_id, role="owner"),
        )
        sync.execute(
            insert(Worker).values(
                id=uuid4(),
                hostname="tb21-fixture-worker",
                version="test",
                capabilities=[{"backend": "docker"}],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            ),
        )
        for benchmark_id, execution_state in (
            (ACTIVE_PROFILE, "runnable"),
            (NEXT_PROFILE, "runnable"),
            (HISTORICAL_PROFILE, "historical"),
        ):
            sync.execute(
                insert(Benchmark).values(
                    id=benchmark_id,
                    display_name=benchmark_id,
                    upstream_kind="fixture",
                    upstream_locator="fixture://tb2",
                    upstream_revision="fixture",
                    license_spdx="MIT",
                    license_url="https://example.test/license",
                    splits=["test"],
                    execution_state=execution_state,
                ),
            )
        for task_id, benchmark_id in (
            (ACTIVE_TASK_ID, ACTIVE_PROFILE),
            (NEXT_TASK_ID, NEXT_PROFILE),
            (HISTORICAL_TASK_ID, HISTORICAL_PROFILE),
        ):
            sync.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="x" * 64,
                    config=_valid_task_config(task_id),
                    source=f"fixture://{task_id}",
                    license="MIT",
                    benchmark_id=benchmark_id,
                ),
            )
        sync.execute(insert(BenchmarkAlias).values(alias=PUBLIC_ALIAS, benchmark_id=ACTIVE_PROFILE))
        sync.commit()
    sync_engine.dispose()

    try:
        yield app, raw_token, team_id, session_factory
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        with sessionmaker(sync_engine)() as sync:
            sync.execute(delete(Trial))
            sync.execute(delete(Batch))
            sync.execute(delete(Token))
            sync.execute(
                delete(Task).where(
                    Task.id.in_([ACTIVE_TASK_ID, NEXT_TASK_ID, HISTORICAL_TASK_ID]),
                ),
            )
            sync.execute(delete(BenchmarkAlias).where(BenchmarkAlias.alias == PUBLIC_ALIAS))
            sync.execute(
                delete(Benchmark).where(
                    Benchmark.id.in_([ACTIVE_PROFILE, NEXT_PROFILE, HISTORICAL_PROFILE]),
                ),
            )
            sync.execute(delete(Worker).where(Worker.hostname == "tb21-fixture-worker"))
            sync.execute(delete(TeamMembership).where(TeamMembership.team_id == team_id))
            sync.execute(delete(User).where(User.id == user_id))
            sync.execute(delete(Team).where(Team.id == team_id))
            sync.commit()
        sync_engine.dispose()


async def create_batch(client: httpx.AsyncClient) -> UUID:
    response = await client.post(
        "/api/v1/batches",
        headers={"Authorization": f"Bearer {client.headers['x-test-token']}"},
        json={
            "name": "TB2 alias snapshot",
            "task_filter": {"benchmark_id": PUBLIC_ALIAS},
            "trial_config": {"agent": {"name": "oracle"}},
        },
    )
    assert response.status_code == 201, response.text
    return UUID(response.json()["batch_id"])


async def repoint_alias(
    session: AsyncSession,
    alias: str,
    benchmark_id: str,
) -> None:
    await session.execute(
        update(BenchmarkAlias)
        .where(BenchmarkAlias.alias == alias)
        .values(benchmark_id=benchmark_id),
    )
    await session.commit()


async def test_batch_keeps_submission_time_tasks_after_alias_move(
    profile_batch_setup: tuple[FastAPI, str, UUID, async_sessionmaker[AsyncSession]],
) -> None:
    app, raw_token, _team_id, session_factory = profile_batch_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://service",
        headers={"x-test-token": raw_token},
    ) as client:
        batch_id = await create_batch(client)

    async with session_factory() as session:
        batch = await session.get(Batch, batch_id)
        assert batch is not None
        assert batch.task_filter == {"benchmark_id": PUBLIC_ALIAS}
        assert batch.resolved_task_ids == [ACTIVE_TASK_ID]
        assert batch.source_provenance[0]["resolved_profile"] == ACTIVE_PROFILE
        await repoint_alias(session, PUBLIC_ALIAS, NEXT_PROFILE)

    submitted: list[dict[str, object]] = []

    def control_plane(request: httpx.Request) -> httpx.Response:
        submitted.append(json.loads(request.content.decode()))
        return httpx.Response(201)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(control_plane),
        base_url="http://cp",
    ) as control_plane_client:
        await run_once(
            session_factory=session_factory,
            http_client=control_plane_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )

    assert [item["task_id"] for item in submitted] == [ACTIVE_TASK_ID]


async def test_http_submission_exposes_machine_readable_retired_code(
    profile_batch_setup: tuple[FastAPI, str, UUID, async_sessionmaker[AsyncSession]],
) -> None:
    app, raw_token, _team_id, _session_factory = profile_batch_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://service") as client:
        response = await client.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "name": "Retired TB2 profile",
                "task_filter": {"benchmark_id": HISTORICAL_PROFILE},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert response.status_code == 409, response.text
    assert response.json()["detail"]["reason"] == "benchmark_retired"


async def test_legacy_null_snapshot_runner_resolves_historical_profile(
    profile_batch_setup: tuple[FastAPI, str, UUID, async_sessionmaker[AsyncSession]],
) -> None:
    _app, _raw_token, team_id, session_factory = profile_batch_setup
    async with session_factory() as session:
        batch = Batch(
            team_id=team_id,
            name="legacy historical TB2 batch",
            task_filter={"benchmark_id": HISTORICAL_PROFILE},
            trial_config={"agent": {"name": "oracle"}},
            state="submitted",
            created_by_token_prefix="legacy",
            expected_trial_count=1,
            resolved_task_ids=None,
        )
        session.add(batch)
        await session.commit()

    submitted: list[dict[str, object]] = []

    def control_plane(request: httpx.Request) -> httpx.Response:
        submitted.append(json.loads(request.content.decode()))
        return httpx.Response(201)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(control_plane),
        base_url="http://cp",
    ) as control_plane_client:
        await run_once(
            session_factory=session_factory,
            http_client=control_plane_client,
            batch_size=10,
            submit_rate_per_sec=100,
        )

    assert [item["task_id"] for item in submitted] == [HISTORICAL_TASK_ID]
