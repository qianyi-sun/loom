"""Workflows CRUD + launch routes (Plan 22)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Benchmark,
    Campaign,
    Task,
    Team,
    TeamQuota,
    Token,
    Workflow,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def wf_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, str, str]]:
    """Spin up loom_service with a benchmark + task + tokens for
    workflow tests.

    Returns (app, admin_raw, admin_team_raw, team_raw):
    - admin_raw: an admin token with `admin:workflows` scope and NO team_id
    - admin_team_raw: a team token whose team is also referenced from
      `admin_raw` for cleanup, used to test launching as a regular team
    - team_raw: same as admin_team_raw, distinct name for readability
    """
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
        engine, expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")
    app.state.gateway_client = httpx.AsyncClient(base_url="http://gw")

    admin_raw = f"loom_admin_{uuid4().hex}"
    team_id = uuid4()
    team_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"wf-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(admin_raw.encode()).digest(),
            type="admin",
            scopes=["admin:tokens", "admin:workflows"],
            team_id=None,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(team_raw.encode()).digest(),
            type="team",
            scopes=["submit", "read:own"],
            team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        # A benchmark to anchor the workflow against, plus one task
        # belonging to it so the launch's task_filter materializes
        # to a non-empty Campaign.
        s.execute(insert(Benchmark).values(
            id="humaneval",
            display_name="HumanEval",
            upstream_kind="huggingface",
            upstream_locator="openai/openai_humaneval",
            upstream_revision="main",
            license_spdx="MIT",
            license_url="https://example.com/license",
            splits=["test"],
            imported_by="test",
        ))
        s.execute(insert(Task).values(
            id="humaneval/HumanEval/0",
            checksum="0" * 64,
            config={},
            source="huggingface://openai/openai_humaneval",
        ))
        s.commit()
    try:
        yield app, admin_raw, team_raw, team_raw
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Campaign))
            s.execute(delete(Workflow))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _ac(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    )


def _wf_body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "name": "humaneval-claude",
        "description": "ad-hoc humaneval run",
        "benchmark_id": "humaneval",
        "agent_name": "claude-code",
        "agent_version": "2.1.0",
        "model_provider": "anthropic",
        "model_name": "claude-opus-4-7",
        "backend": "docker",
        "concurrency": 1,
        "task_filter": {"benchmark_id": "humaneval"},
        "trial_config": {},
    }
    body.update(overrides)
    return body


async def test_admin_can_create_and_read(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    app, admin_raw, _team_raw, _ = wf_setup
    async with _ac(app) as ac:
        r = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
    assert r.status_code == 201
    wf_id = r.json()["id"]

    async with _ac(app) as ac:
        r = await ac.get(
            f"/api/v1/workflows/{wf_id}",
            headers={"Authorization": f"Bearer {admin_raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "humaneval-claude"
    assert body["agent_version"] == "2.1.0"
    assert body["benchmark_id"] == "humaneval"


async def test_team_token_403_on_create(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    """Mutations require `admin:workflows` — team tokens 403."""
    app, _admin_raw, team_raw, _ = wf_setup
    async with _ac(app) as ac:
        r = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {team_raw}"},
            json=_wf_body(name="should-not-create"),
        )
    assert r.status_code == 403


async def test_team_token_can_list(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    """Reads are open — team users see the same global workflows."""
    app, admin_raw, team_raw, _ = wf_setup
    async with _ac(app) as ac:
        await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
        r = await ac.get(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {team_raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["name"] == "humaneval-claude"


async def test_create_rejects_unknown_benchmark(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    app, admin_raw, _team_raw, _ = wf_setup
    async with _ac(app) as ac:
        r = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(benchmark_id="not-a-real-benchmark"),
        )
    assert r.status_code == 400
    assert "unknown benchmark_id" in r.json()["detail"]


async def test_create_rejects_unknown_backend(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    app, admin_raw, _team_raw, _ = wf_setup
    async with _ac(app) as ac:
        r = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(backend="bogus"),
        )
    assert r.status_code == 400


async def test_create_rejects_duplicate_active_name(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    app, admin_raw, _team_raw, _ = wf_setup
    async with _ac(app) as ac:
        r1 = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
        r2 = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
    assert r1.status_code == 201
    assert r2.status_code == 409


async def test_team_can_launch_creates_campaign_with_back_reference(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    app, admin_raw, team_raw, _ = wf_setup
    async with _ac(app) as ac:
        cr = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
        wf_id = cr.json()["id"]
        lr = await ac.post(
            f"/api/v1/workflows/{wf_id}/launch",
            headers={"Authorization": f"Bearer {team_raw}"},
            json={},
        )
    assert lr.status_code == 201
    body = lr.json()
    assert body["workflow_id"] == wf_id
    assert body["expected_trial_count"] >= 1
    assert body["state"] == "submitted"


async def test_launch_uses_optional_name_override(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    app, admin_raw, team_raw, _ = wf_setup
    async with _ac(app) as ac:
        cr = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
        wf_id = cr.json()["id"]
        lr = await ac.post(
            f"/api/v1/workflows/{wf_id}/launch",
            headers={"Authorization": f"Bearer {team_raw}"},
            json={"name": "my-explicit-campaign-name"},
        )
    assert lr.status_code == 201


async def test_admin_token_cannot_launch_without_team(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    """Admin tokens have no team_id; launching would orphan the
    Campaign. Reject explicitly so the operator switches to a team
    token for the actual run."""
    app, admin_raw, _team_raw, _ = wf_setup
    async with _ac(app) as ac:
        cr = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
        wf_id = cr.json()["id"]
        lr = await ac.post(
            f"/api/v1/workflows/{wf_id}/launch",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json={},
        )
    assert lr.status_code == 400
    assert "team" in lr.json()["detail"].lower()


async def test_delete_soft_deletes_and_frees_name(
    wf_setup: tuple[FastAPI, str, str, str],
) -> None:
    app, admin_raw, _team_raw, _ = wf_setup
    async with _ac(app) as ac:
        r1 = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
        wf_id = r1.json()["id"]
        d = await ac.delete(
            f"/api/v1/workflows/{wf_id}",
            headers={"Authorization": f"Bearer {admin_raw}"},
        )
        assert d.status_code == 204
        # Detail returns 404 after deletion.
        g = await ac.get(
            f"/api/v1/workflows/{wf_id}",
            headers={"Authorization": f"Bearer {admin_raw}"},
        )
        assert g.status_code == 404
        # Name is free for reuse.
        r2 = await ac.post(
            "/api/v1/workflows",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json=_wf_body(),
        )
        assert r2.status_code == 201
