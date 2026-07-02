"""Integration tests for ``/api/v1/tasksets`` (#242 sub-plan 2)."""

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
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.minio import MinioContainer

from loom.db.schema import (
    TaskSet,
    TaskSetManifest,
    TaskSetMaterializationJob,
    Team,
    TeamQuota,
    Token,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

_MANIFEST_YAML = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: sample-tasks
  display_name: Sample Tasks
source:
  type: https
  locator: https://example.com/data.jsonl
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }}"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - artifacts: [out.txt]
"""


def _manifest_bytes(*, intents: str = "", verifier: str = "") -> bytes:
    body = _MANIFEST_YAML
    if intents:
        body = body.replace(
            "metadata:",
            f"intents:\n{intents}\nmetadata:",
            1,
        )
    if verifier:
        body = body.replace(
            "task_template:",
            f"{verifier}\ntask_template:",
            1,
        )
    return body.encode()


@pytest.fixture(scope="module")
def tasksets_minio() -> MinioContainer:
    with MinioContainer() as m:
        yield m


@pytest.fixture
async def tasksets_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    tasksets_minio: MinioContainer,
) -> AsyncIterator[tuple[FastAPI, dict[str, str], dict[str, UUID]]]:
    cfg = tasksets_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )
    if not tasksets_minio.get_client().bucket_exists(settings.artifacts_bucket):
        tasksets_minio.get_client().make_bucket(settings.artifacts_bucket)

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
                    scopes=["read:own", "submit"],
                    team_id=team_id,
                    issued_at=datetime.now(UTC),
                ),
            )
        s.commit()

    tokens = {"team_a": raw_a, "team_b": raw_b}
    teams = {"team_a": team_a, "team_b": team_b}
    try:
        yield app, tokens, teams
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(TaskSetMaterializationJob))
            s.execute(delete(TaskSetManifest))
            s.execute(delete(TaskSet))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


@pytest.mark.asyncio
async def test_post_taskset_happy_path(tasksets_setup) -> None:
    app, tokens, teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    task_set_id = f"ts/{teams['team_a']}/sample-tasks"
    assert body["task_set_id"] == task_set_id
    assert body["status"] == "materializing"
    assert body["capabilities"] == ["trajectory-only"]
    assert body["evaluation_ready"] is False

    sync_engine = create_engine(str(app.state.settings.db_url))
    with sync_engine.begin() as conn:
        job_state = conn.execute(
            text(
                "SELECT state FROM task_set_materialization_jobs "
                "WHERE task_set_id = :id",
            ),
            {"id": task_set_id},
        ).scalar_one()
    sync_engine.dispose()
    assert job_state == "queued"


@pytest.mark.asyncio
async def test_verifier_infers_evaluation(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    manifest = _manifest_bytes(
        verifier="verifier:\n  type: pytest\n  file: verifier/test.py\n",
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": ("manifest.yaml", manifest, "application/x-yaml"),
                "verifier": ("verifier/test.py", b"def test_x(): pass", "text/x-python"),
            },
        )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert "evaluation" in body["inferred_intents"]
    assert body["capabilities"] == ["both"]
    assert any(w["code"] == "evaluation_inferred_from_verifier" for w in body["warnings"])


@pytest.mark.asyncio
async def test_cross_team_get_returns_404(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_b']}"},
        )
    assert post.status_code == 202
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_evaluation_without_verifier_returns_400(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    manifest = _manifest_bytes(intents="  - evaluation\n")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={"manifest": ("manifest.yaml", manifest, "application/x-yaml")},
        )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "verifier_required_for_evaluation"


@pytest.mark.asyncio
async def test_duplicate_slug_returns_409(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        files = {"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")}
        first = await client.post("/api/v1/tasksets", headers=headers, files=files)
        second = await client.post("/api/v1/tasksets", headers=headers, files=files)
    assert first.status_code == 202
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_get_delete_rebuild(tasksets_setup) -> None:
    app, tokens, _teams = tasksets_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Authorization": f"Bearer {tokens['team_a']}"}
        post = await client.post(
            "/api/v1/tasksets",
            headers=headers,
            files={"manifest": ("manifest.yaml", _manifest_bytes(), "application/x-yaml")},
        )
        task_set_id = post.json()["task_set_id"]
        get_resp = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["materialization_job_state"] == "queued"
        assert get_resp.json()["error_summary"] == []

        rebuild = await client.post(
            f"/api/v1/tasksets/{task_set_id}/rebuild",
            headers=headers,
        )
        assert rebuild.status_code == 409

        delete_resp = await client.delete(
            f"/api/v1/tasksets/{task_set_id}",
            headers=headers,
        )
        assert delete_resp.status_code == 204

        gone = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert gone.status_code == 404
