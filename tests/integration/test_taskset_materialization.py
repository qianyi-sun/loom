"""Integration tests for TaskSet materialization worker (#242 sub-plan 3)."""

from __future__ import annotations

import asyncio
import hashlib
import io
import tarfile
import threading
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, delete, insert, select, update
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
from loom.taskset.materialize import MaterializeOutput
from loom.taskset.transform_sandbox import TransformSandboxConfig
from loom_service import taskset_materializer
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

_MANIFEST_BUNDLE_UPLOAD = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: bundle-upload-tasks
  display_name: Bundle Upload Tasks
intents:
  - evaluation
source:
  type: bundle-upload
  locator: bundle.tar.gz
  subset: tasks
limits:
  max_instances: 1
"""


def _add_tar_file(tar: tarfile.TarFile, name: str, body: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(body)
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(body))


def _bundle_tar_bytes() -> bytes:
    task_toml = b"""
version = "1"

[metadata]
id = "source-useful-frontier-5003/alpha"
name = "Alpha Task"

[environment]
dockerfile = "environment/Dockerfile"

[verifier]
name = "script"

[verifier.args]
script_path = "verifier/check.sh"
"""
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        _add_tar_file(tar, "tasks/alpha/task.toml", task_toml)
        _add_tar_file(tar, "tasks/alpha/instruction.md", b"Create answer.txt\n")
        _add_tar_file(tar, "tasks/alpha/data/input.txt", b"per-task payload\n")
        _add_tar_file(tar, "tasks/alpha/environment/Dockerfile", b"FROM alpine:3.20\n")
        _add_tar_file(tar, "tasks/alpha/verifier/check.sh", b"#!/bin/sh\nexit 0\n")
    return out.getvalue()


def _unsafe_symlink_bundle_tar_bytes() -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        info = tarfile.TarInfo("tasks/alpha/escape")
        info.type = tarfile.SYMTYPE
        info.linkname = "../outside"
        tar.addfile(info)
    return out.getvalue()


def _unsafe_traversal_bundle_tar_bytes() -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        _add_tar_file(tar, "../outside.txt", b"escape\n")
    return out.getvalue()


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


async def _submit_inline_task_set(
    app: FastAPI,
    *,
    token: str,
) -> str:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {token}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_INLINE.encode(),
                    "application/x-yaml",
                ),
            },
        )
    assert response.status_code == 202, response.text
    return response.json()["task_set_id"]


@pytest.mark.asyncio
async def test_materialization_reclaims_only_stale_lease_heartbeats(
    materialization_setup,
) -> None:
    app, tokens, _teams = materialization_setup
    await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "test-owner-a"
    owner_b = "test-owner-b"

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    assert len(claimed_by_a) == 1
    lease_a = claimed_by_a[0]

    now = datetime.now(UTC)
    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(
                claimed_at=now - timedelta(minutes=5),
                lease_heartbeat_at=now,
            ),
        )
        await session.commit()
        assert await taskset_materializer.reclaim_stale_jobs(
            session,
            claim_ttl_sec=60,
        ) == 0

    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(lease_heartbeat_at=now - timedelta(minutes=5)),
        )
        await session.commit()
        assert await taskset_materializer.reclaim_stale_jobs(
            session,
            claim_ttl_sec=60,
        ) == 1
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(next_attempt_at=now - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_b,
        )

    assert len(claimed_by_b) == 1
    lease_b = claimed_by_b[0]
    assert lease_b.id == lease_a.id
    assert lease_b.lease_epoch > lease_a.lease_epoch
    assert lease_b.claimed_by == owner_b


@pytest.mark.asyncio
async def test_materialization_stale_owner_cannot_overwrite_reclaimed_winner(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _teams = materialization_setup
    task_set_id = await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "test-owner-a"
    owner_b = "test-owner-b"
    loop = asyncio.get_running_loop()
    materialization_started = asyncio.Event()
    release_owner_a = threading.Event()
    materialization_calls = 0

    def materialize_for_owner_race(**_kwargs: object) -> MaterializeOutput:
        nonlocal materialization_calls
        materialization_calls += 1
        if materialization_calls == 1:
            loop.call_soon_threadsafe(materialization_started.set)
            assert release_owner_a.wait(timeout=10)
            return MaterializeOutput(
                status="failed",
                status_reason="owner_a_result",
                job_failure_reason="owner_a_result",
                job_failure_message="owner A resumed after reclaim",
            )
        return MaterializeOutput(
            status="failed",
            status_reason="owner_b_result",
            job_failure_reason="owner_b_result",
            job_failure_message="owner B is the fenced winner",
        )

    monkeypatch.setattr(
        taskset_materializer,
        "materialize_task_set",
        materialize_for_owner_race,
    )

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    assert len(claimed_by_a) == 1
    lease_a = claimed_by_a[0]

    owner_a_task = asyncio.create_task(
        taskset_materializer._materialize_claimed_job(
            app.state.session_factory,
            job_id=lease_a.id,
            minio_client=app.state.minio_client,
            artifacts_bucket=app.state.settings.artifacts_bucket,
            upstream_cache_root=app.state.settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(
                enabled=False,
                network_isolated=False,
            ),
        ),
    )
    await asyncio.wait_for(materialization_started.wait(), timeout=5)

    stale_at = datetime.now(UTC) - timedelta(minutes=5)
    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(claimed_at=stale_at, lease_heartbeat_at=stale_at),
        )
        await session.commit()
        assert await taskset_materializer.reclaim_stale_jobs(
            session,
            claim_ttl_sec=60,
        ) == 1
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == lease_a.id)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_b,
        )
    assert len(claimed_by_b) == 1

    await taskset_materializer._materialize_claimed_job(
        app.state.session_factory,
        job_id=lease_a.id,
        minio_client=app.state.minio_client,
        artifacts_bucket=app.state.settings.artifacts_bucket,
        upstream_cache_root=app.state.settings.taskset_materializer_upstream_cache_root,
        transform_config=TransformSandboxConfig(
            enabled=False,
            network_isolated=False,
        ),
    )

    async with app.state.session_factory() as session:
        winner_before_resume = await session.get(
            TaskSetMaterializationJob,
            lease_a.id,
        )
        task_set_before_resume = await session.get(TaskSet, task_set_id)
    assert winner_before_resume is not None
    assert task_set_before_resume is not None
    winner_snapshot = (
        winner_before_resume.state,
        winner_before_resume.lease_epoch,
        winner_before_resume.claimed_by,
        winner_before_resume.failure_reason,
        task_set_before_resume.status,
        task_set_before_resume.status_reason,
    )

    release_owner_a.set()
    owner_a_result = (await asyncio.gather(owner_a_task, return_exceptions=True))[0]

    async with app.state.session_factory() as session:
        winner_after_resume = await session.get(
            TaskSetMaterializationJob,
            lease_a.id,
        )
        task_set_after_resume = await session.get(TaskSet, task_set_id)
    assert winner_after_resume is not None
    assert task_set_after_resume is not None
    assert (
        winner_after_resume.state,
        winner_after_resume.lease_epoch,
        winner_after_resume.claimed_by,
        winner_after_resume.failure_reason,
        task_set_after_resume.status,
        task_set_after_resume.status_reason,
    ) == winner_snapshot
    lease_lost = getattr(taskset_materializer, "LeaseLost", RuntimeError)
    assert isinstance(owner_a_result, lease_lost)


@pytest.mark.asyncio
async def test_materialization_rejects_stale_lease_for_every_state_transition(
    materialization_setup,
) -> None:
    app, tokens, _teams = materialization_setup
    await _submit_inline_task_set(app, token=tokens["team_a"])
    owner_a = "test-owner-a"
    owner_b = "test-owner-b"

    async with app.state.session_factory() as session:
        claimed_by_a = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_a,
        )
    assert len(claimed_by_a) == 1
    initial_job = claimed_by_a[0]
    stale_at = datetime.now(UTC) - timedelta(minutes=5)
    async with app.state.session_factory() as session:
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == initial_job.id)
            .values(claimed_at=stale_at, lease_heartbeat_at=stale_at),
        )
        await session.commit()
        assert await taskset_materializer.reclaim_stale_jobs(
            session,
            claim_ttl_sec=60,
        ) == 1
        await session.execute(
            update(TaskSetMaterializationJob)
            .where(TaskSetMaterializationJob.id == initial_job.id)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)),
        )
        await session.commit()
        claimed_by_b = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner_b,
        )
    assert len(claimed_by_b) == 1

    lease_type = getattr(taskset_materializer, "MaterializationLease", None)
    lease_lost = getattr(taskset_materializer, "LeaseLost", None)
    transition = getattr(taskset_materializer, "_update_job_for_lease", None)
    assert lease_type is not None
    assert lease_lost is not None
    assert callable(transition)
    stale_lease = lease_type(
        job_id=initial_job.id,
        lease_epoch=initial_job.lease_epoch,
        claimed_by=owner_a,
    )
    now = datetime.now(UTC)
    transitions = (
        (("claimed", "running"), {"lease_heartbeat_at": now}),
        (("claimed",), {"state": "running", "started_at": now}),
        (("running",), {"state": "queued", "next_attempt_at": now}),
        (("running",), {"state": "failed", "finished_at": now}),
    )
    for states, values in transitions:
        async with app.state.session_factory() as session:
            with pytest.raises(lease_lost):
                await transition(
                    session,
                    lease=stale_lease,
                    states=states,
                    values=values,
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
    assert body["status_reason"] == "transform_unavailable_in_internal_trusted"
    assert body["materialization_job_state"] == "failed"


@pytest.mark.asyncio
async def test_materialization_transform_rejects_legacy_gates_before_fetch_or_run(
    materialization_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED", "true")
    monkeypatch.setenv("LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED", "true")
    app, tokens, _teams = materialization_setup
    app.state.settings = LoomServiceSettings(_env_file=None)
    transport = ASGITransport(app=app)
    with (
        patch(
            "loom.taskset.materialize._fetch_blob_bytes",
            side_effect=AssertionError("v1 rejection must not fetch blobs"),
        ) as fetch_blob,
        patch(
            "loom.taskset.transform_sandbox.run_transform",
            side_effect=AssertionError("v1 rejection must not invoke a runner"),
        ) as run,
    ):
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
                    "transform": (
                        "transform.py",
                        b"def transform(row): return row",
                        "text/x-python",
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
    assert body["status"] == "failed"
    assert body["status_reason"] == "transform_unavailable_in_internal_trusted"
    assert body["task_count"] == 0
    assert body["materialization_job_state"] == "failed"
    fetch_blob.assert_not_called()
    run.assert_not_called()


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


@pytest.mark.asyncio
async def test_materialization_e2e_bundle_upload_preserves_per_task_assets(
    materialization_setup,
) -> None:
    app, tokens, teams = materialization_setup
    settings = app.state.settings
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_BUNDLE_UPLOAD.encode(),
                    "application/x-yaml",
                ),
                "bundle": ("bundle.tar.gz", _bundle_tar_bytes(), "application/gzip"),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        assert task_set_id == f"ts/{teams['team_a']}/bundle-upload-tasks"
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["status"] == "ready"
    assert body["task_count"] == 1
    assert body["evaluation_ready"] is True
    assert body["materialization_job_state"] == "succeeded"

    async with app.state.session_factory() as session:
        row = (await session.execute(
            select(Task).where(Task.task_set_id == task_set_id),
        )).scalar_one()

    assert row.id == (
        f"{task_set_id}/tasks/source-useful-frontier-5003_alpha"
    )
    assert row.config["task"]["id"] == "source-useful-frontier-5003/alpha"
    assert row.config["environment"]["os"] == "linux"
    assert row.config["verifier"]["args"]["script_path"] == "verifier/check.sh"
    data_key = (
        f"tasksets/user/{teams['team_a']}/bundle-upload-tasks/"
        "tasks/source-useful-frontier-5003_alpha/data/input.txt"
    )
    payload = app.state.minio_client.get_object(
        Bucket=settings.artifacts_bucket,
        Key=data_key,
    )["Body"].read()
    assert payload == b"per-task payload\n"


@pytest.mark.asyncio
@pytest.mark.parametrize("archive_factory", [
    _unsafe_symlink_bundle_tar_bytes,
    _unsafe_traversal_bundle_tar_bytes,
])
async def test_materialization_rejects_bundle_upload_unsafe_archives(
    materialization_setup,
    archive_factory: Callable[[], bytes],
) -> None:
    app, tokens, _teams = materialization_setup
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        post = await client.post(
            "/api/v1/tasksets",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
            files={
                "manifest": (
                    "manifest.yaml",
                    _MANIFEST_BUNDLE_UPLOAD.encode(),
                    "application/x-yaml",
                ),
                "bundle": (
                    "bundle.tar.gz",
                    archive_factory(),
                    "application/gzip",
                ),
            },
        )
        assert post.status_code == 202, post.text
        task_set_id = post.json()["task_set_id"]
        await _run_materializer_once(app)
        get_resp = await client.get(
            f"/api/v1/tasksets/{task_set_id}",
            headers={"Authorization": f"Bearer {tokens['team_a']}"},
        )
    body = get_resp.json()
    assert body["status"] == "failed"
    assert body["status_reason"] == "bundle_extract_unsafe"
    assert body["materialization_job_state"] == "failed"
