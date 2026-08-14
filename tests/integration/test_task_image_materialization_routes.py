from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import TaskImageMaterialization, Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def materialization_namespace() -> str:
    return f"task-image-route/{uuid4()}"


@pytest.fixture
def builder_token(
    postgres_url: str,
    materialization_namespace: str,
) -> Iterator[str]:
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    raw = f"task_image_builder_{uuid4().hex}"
    token_hash = hashlib.sha256(raw.encode()).digest()
    with sessions() as session:
        session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["worker:claim", "worker:report", "task-image:build"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        session.commit()
    try:
        yield raw
    finally:
        with sessions() as session:
            session.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id.startswith(materialization_namespace)
                )
            )
            session.execute(delete(Token).where(Token.token_hash == token_hash))
            session.commit()
        engine.dispose()


@pytest.fixture
def client(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[TestClient]:
    for key, value in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)
    app = create_app(ControlPlaneSettings(_env_file=None))
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def create_materialization(
    postgres_url: str,
    materialization_namespace: str,
) -> Callable[..., UUID]:
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)

    def create(
        *,
        cpu_arch: str = "x86_64",
        max_attempts: int = 3,
        state: str = "queued",
    ) -> UUID:
        materialization_id = uuid4()
        task_id = f"{materialization_namespace}/{materialization_id}"
        key = hashlib.sha256(task_id.encode()).hexdigest()
        with sessions() as session:
            session.execute(
                insert(TaskImageMaterialization).values(
                    id=materialization_id,
                    materialization_key=key,
                    task_id=task_id,
                    task_checksum="a" * 64,
                    cpu_arch=cpu_arch,
                    task_config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "cpu_arch": cpu_arch,
                            "dockerfile": "environment/Dockerfile",
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                    task_source=f"s3://loom-tasks/{key}",
                    state=state,
                    max_attempts=max_attempts,
                )
            )
            session.commit()
        return materialization_id

    return create


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _claim(
    client: TestClient,
    token: str,
    *,
    builder_id: str,
    cpu_arch: str = "x86_64",
):
    return client.post(
        "/api/v1/internal/task-image-materializations/claim",
        headers=_headers(token),
        json={"builder_id": builder_id, "cpu_arch": cpu_arch},
    )


def _mutation(
    client: TestClient,
    token: str,
    materialization_id: str,
    operation: str,
    *,
    builder_id: str,
    lease_epoch: int,
    extra: dict[str, Any] | None = None,
):
    return client.post(
        f"/api/v1/internal/task-image-materializations/{materialization_id}/{operation}",
        headers=_headers(token),
        json={
            "builder_id": builder_id,
            "lease_epoch": lease_epoch,
            **(extra or {}),
        },
    )


def test_claim_is_native_architecture_specific_and_atomic(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
) -> None:
    x86_id = create_materialization(cpu_arch="x86_64")
    create_materialization(cpu_arch="arm64")

    response = _claim(client, builder_token, builder_id="builder-x86")

    assert response.status_code == 200, response.text
    claim = response.json()
    assert claim["id"] == str(x86_id)
    assert claim["cpu_arch"] == "x86_64"
    assert claim["lease_epoch"] == 1
    assert claim["attempt_count"] == 1
    assert claim["task_source"].startswith("s3://loom-tasks/")
    assert _claim(client, builder_token, builder_id="other-x86").status_code == 204


def test_start_heartbeat_and_complete_reject_stale_lease(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization()
    claim = _claim(client, builder_token, builder_id="builder-a").json()

    started = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "start",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"],
    )
    assert started.status_code == 200, started.text
    assert started.json()["state"] == "running"

    stale = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "heartbeat",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"] + 1,
    )
    assert stale.status_code == 409

    heartbeat = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "heartbeat",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"],
    )
    assert heartbeat.status_code == 200, heartbeat.text
    assert heartbeat.json()["state"] == "running"

    completed = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "complete",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"],
        extra={
            "registry_images": {
                "task": "registry.example/loom/task@sha256:" + "b" * 64,
            }
        },
    )
    assert completed.status_code == 200, completed.text
    assert completed.json()["state"] == "ready"

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            assert row.state == "ready"
            assert row.claimed_by is None
            assert row.lease_expires_at is None
            assert row.registry_images["task"].endswith("b" * 64)
    finally:
        engine.dispose()


def test_retryable_failure_requeues_and_reclaim_advances_epoch(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization()
    claim = _claim(client, builder_token, builder_id="builder-a").json()
    failed = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "fail",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"],
        extra={
            "retryable": True,
            "failure_reason": "registry_unavailable",
            "failure_message": "temporary outage",
        },
    )
    assert failed.status_code == 200, failed.text
    assert failed.json()["state"] == "queued"
    assert failed.json()["next_attempt_at"] is not None

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_id)
                .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
            )
            session.commit()
    finally:
        engine.dispose()

    reclaimed = _claim(client, builder_token, builder_id="builder-b")
    assert reclaimed.status_code == 200, reclaimed.text
    assert reclaimed.json()["id"] == str(materialization_id)
    assert reclaimed.json()["lease_epoch"] == claim["lease_epoch"] + 1
    assert reclaimed.json()["attempt_count"] == 2


def test_retryable_failure_at_attempt_limit_is_terminal(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
) -> None:
    materialization_id = create_materialization(max_attempts=1)
    claim = _claim(client, builder_token, builder_id="builder-a").json()

    failed = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "fail",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"],
        extra={
            "retryable": True,
            "failure_reason": "registry_unavailable",
            "failure_message": "still unavailable",
        },
    )

    assert failed.status_code == 200, failed.text
    assert failed.json()["state"] == "failed"
    assert failed.json()["next_attempt_at"] is None


def test_claim_recovers_expired_lease_with_new_epoch(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization()
    first = _claim(client, builder_token, builder_id="builder-old").json()

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_id)
                .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )
            session.commit()
    finally:
        engine.dispose()

    reclaimed = _claim(client, builder_token, builder_id="builder-new")

    assert reclaimed.status_code == 200, reclaimed.text
    assert reclaimed.json()["id"] == str(materialization_id)
    assert reclaimed.json()["lease_epoch"] == first["lease_epoch"] + 1
    assert reclaimed.json()["attempt_count"] == 2


def test_builder_routes_require_worker_token(
    client: TestClient,
    create_materialization: Callable[..., UUID],
) -> None:
    create_materialization()

    response = client.post(
        "/api/v1/internal/task-image-materializations/claim",
        json={"builder_id": "unauthorized", "cpu_arch": "x86_64"},
    )

    assert response.status_code == 401
