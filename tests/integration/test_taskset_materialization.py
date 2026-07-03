"""Integration tests for TaskSet materialization worker (#242 sub-plan 3)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.minio import MinioContainer

from loom.db.schema import (
    Task,
    TaskSet,
    TaskSetManifest,
    TaskSetMaterializationJob,
    Team,
    TeamQuota,
    Token,
    User,
)
from loom.taskset.transform_sandbox import TransformSandboxConfig
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.taskset_materializer import run_once

_MANIFEST_INLINE = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: inline-tasks
  display_name: Inline Tasks
source:
  type: jsonl-inline
  locator: |
    {"id":"1","question":"What is 1+1?"}
    {"id":"2","question":"What is 2+2?"}
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
    - name: main
      artifacts: [out.txt]
"""

_MANIFEST_PARTIAL = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: partial-tasks
  display_name: Partial Tasks
source:
  type: jsonl-inline
  locator: |
    {"id":"1","question":"ok"}
    {"bad":"missing id"}
    {"id":"3","question":"ok3"}
    {"id":"4"}
    {"id":"5","question":"ok5"}
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "task"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - name: main
      artifacts: [out.txt]
"""

_MANIFEST_TRANSFORM = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: transform-tasks
  display_name: Transform Tasks
source:
  type: jsonl-inline
  locator: |
    {"id":"1"}
instance_mapping:
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "task"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - name: main
      artifacts: [out.txt]
transform:
  file: transform.py
"""

_MANIFEST_DNS_INCOMPATIBLE = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: dns-incompatible-tasks
  display_name: DNS Incompatible Tasks
intents:
  - evaluation
source:
  type: jsonl-inline
  locator: |
    {"id":"1","question":"break dns"}
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "task"
  environment:
    os: linux
    dockerfile: environment/Dockerfile
  agent:
    name: default
  steps:
    - name: main
      artifacts: [out.txt]
verifier:
  type: script
  file: environment/Dockerfile
"""


@pytest.fixture(scope="module")
def materialization_minio() -> MinioContainer:
    with MinioContainer() as m:
        yield m


@pytest.fixture
async def materialization_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    materialization_minio: MinioContainer,
    tmp_path: Path,
) -> AsyncIterator[tuple[FastAPI, dict[str, str], dict[str, UUID]]]:
    cfg = materialization_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    cache_root = tmp_path / "upstream-cache"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SVC_TASKSET_MATERIALIZER_UPSTREAM_CACHE_ROOT": str(cache_root),
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
    if not materialization_minio.get_client().bucket_exists(settings.artifacts_bucket):
        materialization_minio.get_client().make_bucket(settings.artifacts_bucket)

    team_a = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name=f"t-{team_a}"))
        s.execute(insert(TeamQuota).values(team_id=team_a))
        user_id = uuid4()
        username = f"MatOwner-{team_a.hex[:8]}"
        s.execute(
            insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username.casefold(),
                status="active",
                is_platform_admin=False,
            ),
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_a.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_a,
                created_by_user_id=user_id,
                issued_at=datetime.now(UTC),
            ),
        )
        s.commit()

    tokens = {"team_a": raw_a}
    teams = {"team_a": team_a}
    try:
        yield app, tokens, teams
    finally:
        await app.state.http_client.aclose()
        if hasattr(app.state, "batch_runner_task"):
            app.state.batch_runner_task.cancel()
        if hasattr(app.state, "taskset_materializer_task"):
            app.state.taskset_materializer_task.cancel()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Task).where(Task.task_set_id.is_not(None)))
            s.execute(delete(TaskSetMaterializationJob))
            s.execute(delete(TaskSetManifest))
            s.execute(delete(TaskSet))
            s.execute(delete(Token))
            s.execute(delete(User))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _run_materializer_once(app: FastAPI) -> None:
    settings = app.state.settings
    await run_once(
        session_factory=app.state.session_factory,
        minio_client=app.state.minio_client,
        artifacts_bucket=settings.artifacts_bucket,
        upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
        batch_size=settings.taskset_materializer_batch_size,
        claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
        transform_config=TransformSandboxConfig(
            enabled=settings.taskset_materializer_transforms_enabled,
            network_isolated=settings.taskset_materializer_transform_network_isolated,
            wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
            cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
            memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
        ),
    )


@pytest.mark.asyncio
async def test_materialization_e2e_jsonl_inline(materialization_setup) -> None:
    app, tokens, teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_INLINE.encode(),
                    "application/x-yaml",
                ),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        assert task_set_id == f"ts/{teams['team_a']}/inline-tasks"

        await _run_materializer_once(app)

        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["status"] == "ready"
    assert body["task_count"] == 2
    assert body["materialization_job_state"] == "succeeded"

    async with app.state.session_factory() as session:
        rows = (await session.execute(
            select(Task).where(Task.task_set_id == task_set_id),
        )).scalars().all()
    assert len(rows) == 2
    assert all(row.source and row.source.startswith("s3://") for row in rows)


@pytest.mark.asyncio
async def test_materialization_partial_status(materialization_setup) -> None:
    app, tokens, _teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_PARTIAL.encode(),
                    "application/x-yaml",
                ),
            },
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    body = get_resp.json()
    assert body["status"] == "partial"
    assert body["task_count"] == 3
    assert len(body["error_summary"]) >= 2


@pytest.mark.asyncio
async def test_materialization_transform_fails_when_gates_disabled(materialization_setup) -> None:
    app, tokens, _teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_TRANSFORM.encode(),
                    "application/x-yaml",
                ),
                "transform": ("transform.py", b"def transform(row): return row", "text/x-python"),
            },
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    body = get_resp.json()
    assert body["status"] == "failed"
    assert body["status_reason"] == "transform_unsupported_on_host"
    assert body["materialization_job_state"] == "failed"


@pytest.mark.asyncio
async def test_materialization_transform_succeeds_when_gates_enabled(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED", "true")
    monkeypatch.setenv("LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED", "true")
    app, tokens, _teams = materialization_setup
    app.state.settings = LoomServiceSettings(_env_file=None)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_TRANSFORM.encode(),
                    "application/x-yaml",
                ),
                "transform": ("transform.py", b"def transform(row): return row", "text/x-python"),
            },
        )
        assert post.status_code == 202
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    body = get_resp.json()
    assert body["status"] == "ready"
    assert body["task_count"] == 1
    assert body["materialization_job_state"] == "succeeded"


@pytest.mark.asyncio
async def test_materialization_rejects_incompatible_task_bundle(
    materialization_setup,
) -> None:
    app, tokens, _teams = materialization_setup
    verifier = (
        b"FROM debian:bookworm\n"
        b"COPY broken_resolv.conf /app/broken_resolv.conf\n"
        b"RUN cp /app/broken_resolv.conf /etc/resolv.conf\n"
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_DNS_INCOMPATIBLE.encode(),
                    "application/x-yaml",
                ),
                "verifier": ("environment/Dockerfile", verifier, "text/x-dockerfile"),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["status"] == "failed"
    assert body["status_reason"] == "bundle_compatibility_error"
    assert body["task_count"] == 0
    assert body["materialization_job_state"] == "failed"
    assert body["error_summary"] == [
        {
            "row": "1",
            "code": "TASK_COMPAT_DNS_MUTATION",
            "severity": "error",
            "path": "environment/Dockerfile",
            "line": "3",
            "phase": "agent_layer_build",
            "message": (
                "Dockerfile mutates DNS/NSS configuration before Loom installs "
                "the service-mode agent layer"
            ),
            "hint": (
                "Do not change /etc/resolv.conf in the task image before agent "
                "setup. Move network breakage into the task runtime or verifier "
                "phase after the agent is installed. Move DNS breakage into a "
                "phase that runs after Loom has installed the agent layer."
            ),
            "evidence": '{"target":"/etc/resolv.conf"}',
        }
    ]

    async with app.state.session_factory() as session:
        rows = (await session.execute(
            select(Task).where(Task.task_set_id == task_set_id),
        )).scalars().all()
        job = (await session.execute(
            select(TaskSetMaterializationJob).where(
                TaskSetMaterializationJob.task_set_id == task_set_id,
            ),
        )).scalar_one()
    assert rows == []
    assert job.failure_reason == "bundle_compatibility_error"
