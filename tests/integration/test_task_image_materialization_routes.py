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

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEvidence,
    Team,
    Token,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def materialization_namespace(postgres_url: str) -> Iterator[str]:
    namespace = f"task-image-route/{uuid4()}"
    try:
        yield namespace
    finally:
        engine = create_engine(postgres_url)
        with sessionmaker(engine)() as session:
            session.execute(delete(Trial).where(Trial.task_id.startswith(namespace)))
            materialization_ids = select(TaskImageMaterialization.id).where(
                TaskImageMaterialization.task_id.startswith(namespace)
            )
            session.execute(
                delete(TaskImagePublicationEvidence).where(
                    TaskImagePublicationEvidence.materialization_id.in_(materialization_ids)
                )
            )
            session.execute(
                delete(TaskImageMaterializationAttempt).where(
                    TaskImageMaterializationAttempt.materialization_id.in_(materialization_ids)
                )
            )
            session.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id.startswith(namespace)
                )
            )
            session.execute(delete(Task).where(Task.id.startswith(namespace)))
            session.execute(delete(Team).where(Team.name.startswith(namespace)))
            session.commit()
        engine.dispose()


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
                scopes=["task-image:build"],
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
            session.execute(delete(Token).where(Token.token_hash == token_hash))
            session.commit()
        engine.dispose()


@pytest.fixture
def registry_gc_token(
    postgres_url: str,
    materialization_namespace: str,
) -> Iterator[str]:
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    raw = f"task_image_registry_gc_{uuid4().hex}"
    token_hash = hashlib.sha256(raw.encode()).digest()
    with sessions() as session:
        session.execute(
            insert(Token).values(
                token_hash=token_hash,
                type="worker",
                scopes=["task-image:gc"],
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
        "LOOM_CP_TASK_IMAGE_BUILDER_LEASE_SECONDS": "17",
        "LOOM_CP_TASK_IMAGE_REGISTRY_GRACE_HOURS": "48",
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
        registry_images: dict[str, str] | None = None,
        unreferenced_at: datetime | None = None,
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
                    registry_images=registry_images or {},
                    unreferenced_at=unreferenced_at,
                )
            )
            session.commit()
        return materialization_id

    return create


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _assert_configured_lease(payload: dict[str, Any]) -> None:
    lease_expires_at = datetime.fromisoformat(payload["lease_expires_at"])
    remaining = (lease_expires_at - datetime.now(UTC)).total_seconds()
    assert 14 <= remaining <= 18


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
    _assert_configured_lease(claim)
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
    _assert_configured_lease(started.json())

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
    _assert_configured_lease(heartbeat.json())

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


def test_complete_requires_every_dockerfile_component(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization()
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            row.task_config = {
                **row.task_config,
                "environment": {
                    **row.task_config["environment"],
                    "sidecars": [
                        {
                            "name": "database",
                            "dockerfile": "environment/database.Dockerfile",
                        }
                    ],
                },
            }
            session.commit()
    finally:
        engine.dispose()

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

    incomplete = _mutation(
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

    assert incomplete.status_code == 400, incomplete.text
    assert incomplete.json()["detail"] == (
        "registry_images do not match the task snapshot (missing=sidecar:database; unexpected=none)"
    )


def test_builder_routes_reject_malformed_or_unexpected_publication_evidence(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
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

    malformed = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "complete",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"],
        extra={
            "registry_images": {
                "task": "registry.example/loom/task @sha256:" + "b" * 64,
            }
        },
    )
    assert malformed.status_code == 400, malformed.text

    unexpected = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "fail",
        builder_id="builder-a",
        lease_epoch=claim["lease_epoch"],
        extra={
            "retryable": False,
            "failure_reason": "publication_failed",
            "failure_message": "failed after publishing an unknown component",
            "registry_images": {
                "unknown": "registry.example/loom/unknown@sha256:" + "c" * 64,
            },
        },
    )
    assert unexpected.status_code == 400, unexpected.text


def test_publication_evidence_is_append_only_across_retries(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization()
    first_claim = _claim(client, builder_token, builder_id="builder-a").json()
    first_image = "registry.example/loom/task@sha256:" + "4" * 64
    recorded = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "publication",
        builder_id="builder-a",
        lease_epoch=first_claim["lease_epoch"],
        extra={
            "attempt_count": first_claim["attempt_count"],
            "component": "task",
            "registry_image": first_image,
        },
    )
    assert recorded.status_code == 200, recorded.text
    failed = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "fail",
        builder_id="builder-a",
        lease_epoch=first_claim["lease_epoch"],
        extra={
            "retryable": True,
            "failure_reason": "temporary_failure",
            "failure_message": "retry",
            "registry_images": {"task": first_image},
        },
    )
    assert failed.status_code == 200, failed.text

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

    second_claim = _claim(client, builder_token, builder_id="builder-b").json()
    assert second_claim["registry_images"] == {"task": first_image}
    recorded_again = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "publication",
        builder_id="builder-b",
        lease_epoch=second_claim["lease_epoch"],
        extra={
            "attempt_count": second_claim["attempt_count"],
            "component": "task",
            "registry_image": first_image,
        },
    )
    assert recorded_again.status_code == 200, recorded_again.text
    started = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "start",
        builder_id="builder-b",
        lease_epoch=second_claim["lease_epoch"],
    )
    assert started.status_code == 200, started.text
    completed = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "complete",
        builder_id="builder-b",
        lease_epoch=second_claim["lease_epoch"],
        extra={"registry_images": {"task": first_image}},
    )
    assert completed.status_code == 200, completed.text
    assert [
        (entry["attempt_count"], entry["lease_epoch"], entry["registry_image"])
        for entry in completed.json()["registry_image_history"]
    ] == [
        (first_claim["attempt_count"], first_claim["lease_epoch"], first_image),
        (second_claim["attempt_count"], second_claim["lease_epoch"], first_image),
    ]
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            evidence = session.scalars(
                select(TaskImagePublicationEvidence)
                .where(TaskImagePublicationEvidence.materialization_id == materialization_id)
                .order_by(TaskImagePublicationEvidence.lease_epoch)
            ).all()
        assert [(row.attempt_number, row.lease_epoch) for row in evidence] == [
            (first_claim["attempt_count"], first_claim["lease_epoch"]),
            (second_claim["attempt_count"], second_claim["lease_epoch"]),
        ]
    finally:
        engine.dispose()


def test_legacy_publication_body_is_accepted_only_for_current_live_lease(
    client: TestClient,
    builder_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization()
    first = _claim(client, builder_token, builder_id="builder-legacy").json()
    registry_image = "registry.example/loom/task@sha256:" + "7" * 64
    current = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "publication",
        builder_id="builder-legacy",
        lease_epoch=first["lease_epoch"],
        extra={"component": "task", "registry_image": registry_image},
    )
    assert current.status_code == 200, current.text

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
    second = _claim(client, builder_token, builder_id="builder-current").json()
    assert second["lease_epoch"] > first["lease_epoch"]

    stale = _mutation(
        client,
        builder_token,
        str(materialization_id),
        "publication",
        builder_id="builder-legacy",
        lease_epoch=first["lease_epoch"],
        extra={"component": "task", "registry_image": registry_image},
    )
    assert stale.status_code == 409, stale.text


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
            "registry_images": {
                "task": "registry.example/loom/task@sha256:" + "6" * 64,
            },
        },
    )
    assert failed.status_code == 200, failed.text
    assert failed.json()["state"] == "queued"
    assert failed.json()["next_attempt_at"] is not None
    assert failed.json()["registry_images"]["task"].endswith("6" * 64)

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


def test_builder_routes_reject_tokens_with_additional_scopes(
    client: TestClient,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    create_materialization()
    raw = f"overprivileged_task_image_builder_{uuid4().hex}"
    token_hash = hashlib.sha256(raw.encode()).digest()
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.execute(
                insert(Token).values(
                    token_hash=token_hash,
                    type="worker",
                    scopes=["task-image:build", "worker:claim"],
                    team_id=None,
                    issued_at=datetime.now(UTC),
                )
            )
            session.commit()

        response = _claim(client, raw, builder_id="overprivileged")

        assert response.status_code == 401
    finally:
        with sessionmaker(engine)() as session:
            session.execute(delete(Token).where(Token.token_hash == token_hash))
            session.commit()
        engine.dispose()


def test_registry_gc_claim_and_complete_retires_unreferenced_image(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
) -> None:
    materialization_id = create_materialization(
        state="ready",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "d" * 64,
        },
        unreferenced_at=datetime.now(UTC) - timedelta(days=8),
    )

    claimed = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-a"},
    )

    assert claimed.status_code == 200, claimed.text
    payload = claimed.json()
    assert payload["id"] == str(materialization_id)
    assert payload["state"] == "retiring"
    assert payload["registry_images"]["task"].endswith("d" * 64)

    completed = client.post(
        f"/api/v1/internal/task-image-materializations/registry-gc/{materialization_id}/complete",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-a", "lease_epoch": payload["lease_epoch"]},
    )

    assert completed.status_code == 200, completed.text
    assert completed.json()["state"] == "retired"
    assert completed.json()["registry_images"] == {}


def test_registry_gc_retires_unreferenced_partial_failed_publication(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
) -> None:
    materialization_id = create_materialization(
        state="failed",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "5" * 64,
        },
        unreferenced_at=datetime.now(UTC) - timedelta(days=8),
    )

    claimed = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-partial"},
    )

    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["id"] == str(materialization_id)
    assert claimed.json()["registry_images"]["task"].endswith("5" * 64)


def test_registry_gc_does_not_claim_current_task_image(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization(
        state="ready",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "f" * 64,
        },
        unreferenced_at=datetime.now(UTC) - timedelta(days=8),
    )
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            session.execute(
                insert(Task).values(
                    id=row.task_id,
                    checksum="sha256:" + row.task_checksum,
                    config=row.task_config,
                )
            )
            session.commit()
    finally:
        engine.dispose()

    response = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-current"},
    )

    assert response.status_code == 204
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            assert row.unreferenced_at is None
    finally:
        engine.dispose()


def test_registry_gc_does_not_claim_image_linked_to_nonterminal_trial(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
    materialization_namespace: str,
    postgres_url: str,
) -> None:
    materialization_id = create_materialization(
        state="ready",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "c" * 64,
        },
        unreferenced_at=datetime.now(UTC) - timedelta(days=8),
    )
    team_id = uuid4()
    trial_id = uuid4()
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            session.execute(
                insert(Team).values(
                    id=team_id,
                    name=f"{materialization_namespace}/team",
                )
            )
            session.execute(
                insert(Task).values(
                    id=row.task_id,
                    checksum="b" * 64,
                    config=row.task_config,
                )
            )
            session.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=row.task_id,
                    config={},
                    requires_caps={},
                    state="queued",
                )
            )
            session.execute(
                insert(TrialTaskImageMaterialization).values(
                    trial_id=trial_id,
                    materialization_id=materialization_id,
                )
            )
            session.commit()
    finally:
        engine.dispose()

    response = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-live-trial"},
    )

    assert response.status_code == 204
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.execute(update(Trial).where(Trial.id == trial_id).values(state="failed"))
            session.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_id)
                .values(unreferenced_at=datetime.now(UTC) - timedelta(hours=49))
            )
            session.commit()
    finally:
        engine.dispose()

    after_terminal = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-terminal-trial"},
    )
    assert after_terminal.status_code == 200, after_terminal.text
    assert after_terminal.json()["id"] == str(materialization_id)


def test_registry_gc_recovers_expired_lease_and_fences_old_owner(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization(
        state="ready",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "9" * 64,
        },
        unreferenced_at=datetime.now(UTC) - timedelta(days=8),
    )
    first = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-old"},
    ).json()

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

    reclaimed = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-new"},
    )

    assert reclaimed.status_code == 200, reclaimed.text
    second = reclaimed.json()
    assert second["id"] == str(materialization_id)
    assert second["lease_epoch"] == first["lease_epoch"] + 1

    stale = client.post(
        f"/api/v1/internal/task-image-materializations/registry-gc/{materialization_id}/complete",
        headers=_headers(registry_gc_token),
        json={
            "gc_id": "registry-gc-old",
            "lease_epoch": first["lease_epoch"],
        },
    )
    assert stale.status_code == 409


def test_registry_gc_requeues_referenced_image_after_gc_lease_expires(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization(
        state="ready",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "8" * 64,
        },
        unreferenced_at=datetime.now(UTC) - timedelta(days=8),
    )
    claimed = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-abandoned"},
    )
    assert claimed.status_code == 200, claimed.text

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            session.execute(
                insert(Task).values(
                    id=row.task_id,
                    checksum=row.task_checksum,
                    config=row.task_config,
                )
            )
            row.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            session.commit()
    finally:
        engine.dispose()

    response = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-recovery"},
    )
    assert response.status_code == 204

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            assert row.state == "queued"
            assert row.registry_images == {}
            assert row.claimed_by is None
            assert row.unreferenced_at is None
    finally:
        engine.dispose()


def test_registry_gc_marks_unreferenced_images_and_uses_configured_grace(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization(
        state="ready",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "7" * 64,
        },
    )

    first = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-mark"},
    )
    assert first.status_code == 204

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            assert row.unreferenced_at is not None
            row.unreferenced_at = datetime.now(UTC) - timedelta(hours=47)
            session.commit()
    finally:
        engine.dispose()

    before_grace = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-early"},
    )
    assert before_grace.status_code == 204

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_id)
                .values(unreferenced_at=datetime.now(UTC) - timedelta(hours=49))
            )
            session.commit()
    finally:
        engine.dispose()

    after_grace = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-eligible"},
    )
    assert after_grace.status_code == 200, after_grace.text
    assert after_grace.json()["id"] == str(materialization_id)


def test_registry_gc_completion_requeues_image_referenced_during_delete(
    client: TestClient,
    registry_gc_token: str,
    create_materialization: Callable[..., UUID],
    postgres_url: str,
) -> None:
    materialization_id = create_materialization(
        state="ready",
        registry_images={
            "task": "registry.example/loom/task@sha256:" + "e" * 64,
        },
        unreferenced_at=datetime.now(UTC) - timedelta(days=8),
    )
    claimed = client.post(
        "/api/v1/internal/task-image-materializations/registry-gc/claim",
        headers=_headers(registry_gc_token),
        json={"gc_id": "registry-gc-race"},
    ).json()

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            row = session.scalar(
                select(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )
            assert row is not None
            session.execute(
                insert(Task).values(
                    id=row.task_id,
                    checksum=row.task_checksum,
                    config=row.task_config,
                )
            )
            session.commit()
    finally:
        engine.dispose()

    completed = client.post(
        f"/api/v1/internal/task-image-materializations/registry-gc/{materialization_id}/complete",
        headers=_headers(registry_gc_token),
        json={
            "gc_id": "registry-gc-race",
            "lease_epoch": claimed["lease_epoch"],
        },
    )

    assert completed.status_code == 200, completed.text
    assert completed.json()["state"] == "queued"
    assert completed.json()["registry_images"] == {}
