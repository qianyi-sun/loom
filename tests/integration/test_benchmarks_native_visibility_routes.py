"""Native benchmark surfaces must not leak user TaskSets (#242 sub-plan 1)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, TaskSet, Team, TeamQuota, Token
from loom.db.task_set_visibility import visible_task_sets
from loom_cli.benchmark_readiness import run_readiness_audit
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def _seed_task_set(
    session,
    *,
    team_id: UUID,
    slug: str,
    evaluation_ready: bool = False,
    soft_deleted: bool = False,
) -> str:
    task_set_id = f"ts/{team_id}/{slug}"
    session.execute(
        insert(TaskSet).values(
            id=task_set_id,
            owning_team_id=team_id,
            slug=slug,
            display_name=f"TaskSet {slug}",
            status="ready",
            intents=["trajectory_generation", "evaluation"]
            if evaluation_ready
            else ["trajectory_generation"],
            evaluation_ready=evaluation_ready,
            manifest_blob_uri=f"s3://bucket/tasksets/user/{team_id}/{slug}/manifest.yaml",
            soft_deleted_at=datetime.now(UTC) if soft_deleted else None,
        ),
    )
    return task_set_id


@pytest.fixture
async def native_visibility_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, str, UUID, UUID]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )

    team_a = uuid4()
    team_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for team_id, raw in ((team_a, raw_a), (team_b, raw_b)):
            s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            s.execute(insert(TeamQuota).values(team_id=team_id))
            s.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw.encode()).digest(),
                    type="team",
                    scopes=["read:own"],
                    team_id=team_id,
                    issued_at=datetime.now(UTC),
                ),
            )
        for bid, dn in (("humaneval", "HumanEval"), ("mbpp", "MBPP")):
            s.execute(
                insert(Benchmark).values(
                    id=bid,
                    display_name=dn,
                    upstream_kind="huggingface",
                    upstream_locator=f"upstream/{bid}",
                    upstream_revision="",
                    license_spdx="MIT",
                    license_url=f"https://example/{bid}",
                    splits=["test"],
                ),
            )
        _seed_task_set(s, team_id=team_a, slug="trajectory-only")
        _seed_task_set(
            s,
            team_id=team_a,
            slug="eval-ready",
            evaluation_ready=True,
        )
        _seed_task_set(
            s,
            team_id=team_b,
            slug="team-b-private",
        )
        s.commit()
    try:
        yield app, raw_a, raw_b, team_a, team_b
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(TaskSet))
            s.execute(delete(Benchmark).where(Benchmark.id.in_(["humaneval", "mbpp"])))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team).where(Team.id.in_([team_a, team_b])))
            s.commit()
        sync_engine.dispose()


async def test_benchmarks_api_lists_only_native_benchmarks(
    native_visibility_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    app, raw_a, raw_b, _team_a, _team_b = native_visibility_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        for raw in (raw_a, raw_b):
            r = await ac.get(
                "/api/v1/benchmarks?include_empty=true",
                headers={"Authorization": f"Bearer {raw}"},
            )
            assert r.status_code == 200
            ids = {item["id"] for item in r.json()["items"]}
            assert ids == {"humaneval", "mbpp"}
            assert not any(item_id.startswith("ts/") for item_id in ids)


async def test_benchmark_detail_does_not_resolve_task_set_id(
    native_visibility_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    app, raw_a, _raw_b, team_a, _team_b = native_visibility_setup
    task_set_id = f"ts/{team_a}/eval-ready"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/benchmarks/{task_set_id}",
            headers={"Authorization": f"Bearer {raw_a}"},
        )
    assert r.status_code == 404


async def test_readiness_audit_excludes_task_sets(
    native_visibility_setup: tuple[FastAPI, str, str, UUID, UUID],
    postgres_url: str,
) -> None:
    _app, _raw_a, _raw_b, _team_a, _team_b = native_visibility_setup
    items = await run_readiness_audit(db_url=postgres_url)
    ids = {item.id for item in items}
    assert "humaneval" in ids or "mbpp" in ids
    assert not any(item_id.startswith("ts/") for item_id in ids)


async def test_visible_task_sets_isolates_cross_team_rows(
    native_visibility_setup: tuple[FastAPI, str, str, UUID, UUID],
    postgres_url: str,
) -> None:
    _app, _raw_a, _raw_b, team_a, team_b = native_visibility_setup
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            visible_a = list(
                (await session.scalars(visible_task_sets(team_id=team_a))).all(),
            )
            visible_b = list(
                (await session.scalars(visible_task_sets(team_id=team_b))).all(),
            )
    finally:
        await engine.dispose()

    ids_a = {row.id for row in visible_a}
    ids_b = {row.id for row in visible_b}
    assert ids_a == {f"ts/{team_a}/trajectory-only", f"ts/{team_a}/eval-ready"}
    assert ids_b == {f"ts/{team_b}/team-b-private"}
    assert ids_a.isdisjoint(ids_b)


async def test_soft_deleted_task_set_not_visible_to_owner(
    native_visibility_setup: tuple[FastAPI, str, str, UUID, UUID],
    postgres_url: str,
) -> None:
    _app, _raw_a, _raw_b, team_a, _team_b = native_visibility_setup
    sync_engine = create_engine(postgres_url)
    with sessionmaker(sync_engine)() as session:
        _seed_task_set(
            session,
            team_id=team_a,
            slug="deleted-set",
            soft_deleted=True,
        )
        session.commit()
    sync_engine.dispose()

    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            visible = list(
                (await session.scalars(visible_task_sets(team_id=team_a))).all(),
            )
    finally:
        await engine.dispose()
    assert f"ts/{team_a}/deleted-set" not in {row.id for row in visible}
