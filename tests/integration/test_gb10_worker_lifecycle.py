from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    GB10WorkerNodeStatus,
    GB10WorkerPoolDesiredState,
    Token,
    Worker,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "G" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        f'[admin]\ntoken = "{RAW_ADMIN_TOKEN}"\ncreated_at = "2026-06-26T00:00:00Z"\nversion = 1\n',
        encoding="utf-8",
    )
    path.chmod(0o600)


def _set_cp_env(monkeypatch: pytest.MonkeyPatch, postgres_url: str) -> None:
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
):
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url)
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    return create_app(ControlPlaneSettings(_env_file=None))


@pytest.fixture(autouse=True)
def clean_gb10_lifecycle(postgres_url: str) -> Iterator[None]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(delete(GB10WorkerNodeStatus))
        s.execute(delete(GB10WorkerPoolDesiredState))
        s.execute(delete(Worker))
        s.execute(delete(Token))
        s.commit()
    try:
        yield
    finally:
        with session_factory() as s:
            s.execute(delete(GB10WorkerNodeStatus))
            s.execute(delete(GB10WorkerPoolDesiredState))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


def test_desired_state_node_report_and_status_round_trip(app) -> None:
    with TestClient(app) as client:
        desired = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "image_tag": "2026-06-26-gb10",
                "max_concurrent": 10,
                "env_config_version": "gb10-env-v2",
                "source_git_commit": "76875ac6d38c91c947c44b22788348db27a8d45b",
                "target_slots": 10,
                "host_intents": {"trt-gb10-1": "draining"},
                "rollout_policy": {
                    "mode": "canary",
                    "canary_hosts": ["trt-gb10-1"],
                },
                "env": {"LOOM_WORKER_BLOCKING_IO_MAX_WORKERS": "40"},
            },
        )
        assert desired.status_code == 200, desired.text
        body = desired.json()
        assert body["environment"] == "production"
        assert body["pool_name"] == "gb10"
        assert body["image_tag"] == "2026-06-26-gb10"
        assert body["max_concurrent"] == 10
        assert body["env_config_version"] == "gb10-env-v2"
        assert body["source_git_commit"] == ("76875ac6d38c91c947c44b22788348db27a8d45b")
        assert body["target_slots"] == 10
        assert body["host_intents"] == {"trt-gb10-1": "draining"}
        assert body["previous_image_tag"] is None
        assert body["rollout_policy"]["mode"] == "canary"

        fetched = client.get(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert fetched.status_code == 200, fetched.text
        assert fetched.json()["image_tag"] == "2026-06-26-gb10"
        assert fetched.json()["source_git_commit"] == ("76875ac6d38c91c947c44b22788348db27a8d45b")

        reported = client.post(
            "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-1/report",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "current_image_tag": "2026-06-26-gb10",
                "current_max_concurrent": 10,
                "current_env_config_version": "gb10-env-v2",
                "current_intent": "draining",
                "apply_state": "applied",
                "last_apply_result": "compose restarted and worker heartbeat verified",
                "agent_version": "test-agent",
                "compose_project_dir": "/opt/loom",
                "source_git_commit": "76875ac6d38c91c947c44b22788348db27a8d45b",
                "source_git_dirty": False,
            },
        )
        assert reported.status_code == 200, reported.text
        node = reported.json()
        assert node["hostname"] == "trt-gb10-1"
        assert node["desired_image_tag"] == "2026-06-26-gb10"
        assert node["desired_max_concurrent"] == 10
        assert node["desired_env_config_version"] == "gb10-env-v2"
        assert node["desired_source_git_commit"] == ("76875ac6d38c91c947c44b22788348db27a8d45b")
        assert node["desired_intent"] == "draining"
        assert node["current_intent"] == "draining"
        assert node["apply_state"] == "applied"
        assert node["source_git_commit"] == "76875ac6d38c91c947c44b22788348db27a8d45b"
        assert node["source_git_dirty"] is False

        status = client.get(
            "/admin/gb10-worker-pools/status",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert status.status_code == 200, status.text
        status_body = status.json()

    assert len(status_body["desired_states"]) == 1
    assert len(status_body["nodes"]) == 1
    assert status_body["nodes"][0]["hostname"] == "trt-gb10-1"
    assert (
        status_body["nodes"][0]["source_git_commit"] == "76875ac6d38c91c947c44b22788348db27a8d45b"
    )
    assert status_body["nodes"][0]["source_git_dirty"] is False
    assert status_body["desired_states"][0]["source_git_commit"] == (
        "76875ac6d38c91c947c44b22788348db27a8d45b"
    )
    assert status_body["nodes"][0]["desired_source_git_commit"] == (
        "76875ac6d38c91c947c44b22788348db27a8d45b"
    )
    assert status_body["nodes"][0]["last_heartbeat_at"] is not None


def test_desired_state_update_records_previous_version_for_rollback(app) -> None:
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        first = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "old-image",
                "max_concurrent": 5,
                "env_config_version": "old-env",
            },
        )
        assert first.status_code == 200, first.text

        second = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "new-image",
                "max_concurrent": 10,
                "env_config_version": "new-env",
            },
        )
        assert second.status_code == 200, second.text
        body = second.json()

    assert body["image_tag"] == "new-image"
    assert body["previous_image_tag"] == "old-image"
    assert body["previous_max_concurrent"] == 5
    assert body["previous_env_config_version"] == "old-env"


def test_desired_state_rejects_secret_looking_env(app) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "image_tag": "safe",
                "max_concurrent": 10,
                "env_config_version": "safe-env",
                "env": {"LOOM_WORKER_TOKEN": "loom_w_secret"},
            },
        )

    assert response.status_code == 400, response.text
    assert "secret-looking" in response.json()["detail"]


def test_node_report_redacts_secret_looking_status_text(app) -> None:
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        desired = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "safe",
                "max_concurrent": 10,
                "env_config_version": "safe-env",
            },
        )
        assert desired.status_code == 200, desired.text

        report = client.post(
            "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-1/report",
            headers=headers,
            json={
                "current_image_tag": "safe",
                "current_max_concurrent": 10,
                "current_env_config_version": "safe-env",
                "apply_state": "failed",
                "last_apply_result": "failed while using loom_w_secret",
                "error_message": "provider key sk-secret leaked by subprocess",
            },
        )
        assert report.status_code == 200, report.text
        status = client.get("/admin/gb10-worker-pools/status", headers=headers)
        assert status.status_code == 200, status.text

    body = status.text
    assert "loom_w_secret" not in body
    assert "sk-secret" not in body
    assert "<redacted>" in body


def test_status_links_active_fresh_worker_by_hostname_and_pool(
    app,
    postgres_url: str,
) -> None:
    worker_id = _seed_worker(
        postgres_url,
        hostname="trt-gb10-1",
        pool_name="gb10",
        capabilities=[{"backend": "docker"}],
    )
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        desired = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "staging-abc123",
                "max_concurrent": 10,
                "env_config_version": "staging-abc123",
                "host_intents": {"trt-gb10-1": "active"},
            },
        )
        assert desired.status_code == 200, desired.text
        report = client.post(
            "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-1/report",
            headers=headers,
            json={
                "current_image_tag": "staging-abc123",
                "current_max_concurrent": 10,
                "current_env_config_version": "staging-abc123",
                "current_intent": "active",
                "apply_state": "applied",
                "last_apply_result": "already current",
            },
        )
        assert report.status_code == 200, report.text

        status = client.get(
            "/admin/gb10-worker-pools/status",
            headers=headers,
        )
        assert status.status_code == 200, status.text
        node = status.json()["nodes"][0]

    assert node["worker_id"] == worker_id
    assert node["worker_status"] == "active"
    assert node["worker_fresh"] is True
    assert node["worker_backend_names"] == ["docker"]


def test_status_marks_stale_linked_worker_not_fresh(
    app,
    postgres_url: str,
) -> None:
    worker_id = _seed_worker(
        postgres_url,
        hostname="trt-gb10-1",
        pool_name="gb10",
        capabilities=[{"backend": "docker"}],
        last_seen_at=datetime.now(UTC) - timedelta(seconds=120),
    )
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        assert (
            client.put(
                "/admin/gb10-worker-pools/production/gb10/desired-state",
                headers=headers,
                json={
                    "image_tag": "staging-abc123",
                    "max_concurrent": 10,
                    "env_config_version": "staging-abc123",
                    "host_intents": {"trt-gb10-1": "active"},
                },
            ).status_code
            == 200
        )
        report = client.post(
            "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-1/report",
            headers=headers,
            json={
                "current_image_tag": "staging-abc123",
                "current_max_concurrent": 10,
                "current_env_config_version": "staging-abc123",
                "current_intent": "active",
                "apply_state": "applied",
                "worker_id": worker_id,
            },
        )
        assert report.status_code == 200, report.text
        status = client.get("/admin/gb10-worker-pools/status", headers=headers)
        assert status.status_code == 200, status.text
        node = status.json()["nodes"][0]

    assert node["worker_id"] == worker_id
    assert node["worker_status"] == "active"
    assert node["worker_fresh"] is False


def test_status_lists_fresh_worker_without_node_report_as_unlinked(
    app,
    postgres_url: str,
) -> None:
    worker_id = _seed_worker(
        postgres_url,
        hostname="trt-gb10-2",
        pool_name="gb10",
        capabilities=[{"backend": "docker"}],
    )
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        desired = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "staging-abc123",
                "max_concurrent": 10,
                "env_config_version": "staging-abc123",
                "host_intents": {
                    "trt-gb10-1": "active",
                    "trt-gb10-2": "stopped",
                },
            },
        )
        assert desired.status_code == 200, desired.text
        status = client.get(
            "/admin/gb10-worker-pools/status?environment=production&pool_name=gb10",
            headers=headers,
        )
        assert status.status_code == 200, status.text

    body = status.json()
    assert body["nodes"] == []
    assert body["unlinked_workers"] == [
        {
            "worker_id": worker_id,
            "hostname": "trt-gb10-2",
            "pool_name": "gb10",
            "worker_status": "active",
            "worker_last_seen_at": body["unlinked_workers"][0]["worker_last_seen_at"],
            "worker_fresh": True,
            "worker_backend_names": ["docker"],
            "worker_drain_state": "drained",
            "max_concurrent": 10,
        }
    ]


def test_status_lists_second_fresh_registration_for_node_as_unlinked(
    app,
    postgres_url: str,
) -> None:
    worker_ids = {
        _seed_worker(
            postgres_url,
            hostname="trt-gb10-1",
            pool_name="gb10",
            capabilities=[{"backend": "docker"}],
        )
        for _ in range(2)
    }
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        assert (
            client.put(
                "/admin/gb10-worker-pools/production/gb10/desired-state",
                headers=headers,
                json={
                    "image_tag": "staging-abc123",
                    "max_concurrent": 10,
                    "env_config_version": "staging-abc123",
                    "host_intents": {"trt-gb10-1": "active"},
                },
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-1/report",
                headers=headers,
                json={
                    "current_image_tag": "staging-abc123",
                    "current_max_concurrent": 10,
                    "current_env_config_version": "staging-abc123",
                    "current_intent": "active",
                    "apply_state": "applied",
                },
            ).status_code
            == 200
        )
        status = client.get(
            "/admin/gb10-worker-pools/status?environment=production&pool_name=gb10",
            headers=headers,
        )
        assert status.status_code == 200, status.text

    body = status.json()
    assert len(body["nodes"]) == 1
    assert len(body["unlinked_workers"]) == 1
    assert body["unlinked_workers"][0]["worker_fresh"] is True
    assert {
        body["nodes"][0]["worker_id"],
        body["unlinked_workers"][0]["worker_id"],
    } == worker_ids


# ──────────────────────────────────────────────────────────────────────
# #368: stopped host intent must drain the worker registry
# ──────────────────────────────────────────────────────────────────────


def _seed_worker(
    postgres_url: str,
    *,
    hostname: str,
    pool_name: str = "gb10",
    max_concurrent: int = 10,
    capabilities: list[dict[str, str]] | None = None,
    status: str = "active",
    last_seen_at: datetime | None = None,
) -> str:
    """Insert an active worker row directly. Returns the worker id as
    str (UUID) so tests can pass it back in as the report worker_id."""
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    worker_id = uuid4()
    with session_factory() as s:
        s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname=hostname,
                version="test",
                capabilities=(
                    capabilities if capabilities is not None else [{"backend": "docker"}]
                ),
                pool_name=pool_name,
                max_concurrent=max_concurrent,
                drain_state="active",
                registered_at=datetime.now(UTC),
                last_seen_at=last_seen_at or datetime.now(UTC),
                status=status,
            )
        )
        s.commit()
    engine.dispose()
    return str(worker_id)


def _fetch_worker(postgres_url: str, worker_id: str) -> dict[str, str | None]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        row = s.execute(
            select(Worker).where(Worker.id == worker_id),
        ).scalar_one()
        out = {
            "drain_state": row.drain_state,
            "drain_reason": row.drain_reason,
            "drain_owner": row.drain_owner,
        }
    engine.dispose()
    return out


def test_stopped_host_intent_forces_worker_drain_regardless_of_apply_result(
    app,
    postgres_url: str,
) -> None:
    """#368: a stopped host intent must drain the worker registry
    even when the node-agent reports `apply_state=applied` /
    `last_apply_result='already current'` — the operator has
    expressed intent and the scheduler must not keep counting the
    host as active capacity.
    """
    worker_id = _seed_worker(postgres_url, hostname="trt-gb10-15")
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        desired = client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "staging-6b76a48",
                "max_concurrent": 10,
                "env_config_version": "staging-6b76a48",
                "host_intents": {"trt-gb10-15": "stopped"},
            },
        )
        assert desired.status_code == 200, desired.text

        # Node-agent bug: reports apply_state=applied /
        # last_apply_result=already current, but container is still up
        # (would still be heartbeating in production).
        report = client.post(
            "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-15/report",
            headers=headers,
            json={
                "current_image_tag": "staging-6b76a48",
                "current_max_concurrent": 10,
                "current_env_config_version": "staging-6b76a48",
                "current_intent": "stopped",
                "apply_state": "applied",
                "last_apply_result": "already current",
                "worker_id": worker_id,
            },
        )
        assert report.status_code == 200, report.text

    worker_state = _fetch_worker(postgres_url, worker_id)
    assert worker_state["drain_state"] == "drained", (
        "Worker registry must reflect the stopped host intent; "
        "otherwise `loom resources status` and the scheduler will "
        "keep counting the host as active capacity (#368)."
    )
    assert worker_state["drain_owner"] == "gb10-lifecycle"
    assert "desired_intent=stopped" in (worker_state["drain_reason"] or "")


def test_desired_draining_intent_immediately_fences_all_hostname_registrations(
    app,
    postgres_url: str,
) -> None:
    """Desired intent is the claim gate even before a node report arrives."""
    worker_ids = {
        _seed_worker(postgres_url, hostname="trt-gb10-15")
        for _ in range(2)
    }
    with TestClient(app) as client:
        response = client.put(
            "/admin/gb10-worker-pools/staging/gb10/desired-state",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "image_tag": "staging-6b76a48",
                "max_concurrent": 10,
                "env_config_version": "staging-6b76a48",
                "host_intents": {"trt-gb10-15": "draining"},
                "target_slots": 0,
            },
        )
        assert response.status_code == 200, response.text

    states = {_fetch_worker(postgres_url, worker_id)["drain_state"] for worker_id in worker_ids}
    assert states == {"draining"}
    assert {
        _fetch_worker(postgres_url, worker_id)["drain_owner"] for worker_id in worker_ids
    } == {"gb10-lifecycle"}


def test_active_node_report_recovers_lifecycle_owned_draining_registration(
    app,
    postgres_url: str,
) -> None:
    worker_id = _seed_worker(postgres_url, hostname="trt-gb10-15")
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        drain = client.put(
            "/admin/gb10-worker-pools/staging/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "staging-6b76a48",
                "max_concurrent": 10,
                "env_config_version": "staging-6b76a48",
                "host_intents": {"trt-gb10-15": "draining"},
            },
        )
        assert drain.status_code == 200, drain.text
        recover = client.put(
            "/admin/gb10-worker-pools/staging/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "staging-6b76a48",
                "max_concurrent": 10,
                "env_config_version": "staging-6b76a48",
                "host_intents": {"trt-gb10-15": "active"},
            },
        )
        assert recover.status_code == 200, recover.text
        assert _fetch_worker(postgres_url, worker_id)["drain_state"] == "draining"

        report = client.post(
            "/admin/gb10-worker-pools/staging/gb10/nodes/trt-gb10-15/report",
            headers=headers,
            json={
                "current_image_tag": "staging-6b76a48",
                "current_max_concurrent": 10,
                "current_env_config_version": "staging-6b76a48",
                "current_intent": "active",
                "apply_state": "applied",
                "last_apply_result": "docker compose worker reconciled",
            },
        )
        assert report.status_code == 200, report.text

    recovered = _fetch_worker(postgres_url, worker_id)
    assert recovered["drain_state"] == "active"
    assert recovered["drain_owner"] is None


def test_stopped_intent_reconciliation_ignores_hosts_with_no_worker_id(
    app,
    postgres_url: str,
) -> None:
    """A node report with no linked worker_id (worker never
    registered / already unlinked) must not blow up the reconciliation
    path — the endpoint must still return 200 and the desired state
    must still be recorded."""
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        assert (
            client.put(
                "/admin/gb10-worker-pools/production/gb10/desired-state",
                headers=headers,
                json={
                    "image_tag": "staging-6b76a48",
                    "max_concurrent": 10,
                    "env_config_version": "staging-6b76a48",
                    "host_intents": {"trt-gb10-15": "stopped"},
                },
            ).status_code
            == 200
        )
        report = client.post(
            "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-15/report",
            headers=headers,
            json={
                "current_image_tag": "staging-6b76a48",
                "current_max_concurrent": 10,
                "current_env_config_version": "staging-6b76a48",
                "current_intent": "stopped",
                "apply_state": "applied",
            },
        )
        assert report.status_code == 200, report.text


def test_stopped_intent_reconciliation_is_idempotent_across_heartbeats(
    app,
    postgres_url: str,
) -> None:
    """Node heartbeats are periodic — the reconciliation must not
    reset `drain_requested_at` on every report, and repeated
    heartbeats must keep the worker drained (not flip back)."""
    worker_id = _seed_worker(postgres_url, hostname="trt-gb10-15")
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "staging-6b76a48",
                "max_concurrent": 10,
                "env_config_version": "staging-6b76a48",
                "host_intents": {"trt-gb10-15": "stopped"},
            },
        )
        report_body: dict[str, object] = {
            "current_image_tag": "staging-6b76a48",
            "current_max_concurrent": 10,
            "current_env_config_version": "staging-6b76a48",
            "current_intent": "stopped",
            "apply_state": "applied",
            "worker_id": worker_id,
        }
        assert (
            client.post(
                "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-15/report",
                headers=headers,
                json=report_body,
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-15/report",
                headers=headers,
                json=report_body,
            ).status_code
            == 200
        )
    worker_state = _fetch_worker(postgres_url, worker_id)
    assert worker_state["drain_state"] == "drained"


def test_active_host_intent_does_not_touch_worker_drain_state(
    app,
    postgres_url: str,
) -> None:
    """The reconciliation must only trigger on `stopped` — an active
    or default host intent must leave the worker registry alone."""
    worker_id = _seed_worker(postgres_url, hostname="trt-gb10-1")
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        client.put(
            "/admin/gb10-worker-pools/production/gb10/desired-state",
            headers=headers,
            json={
                "image_tag": "staging-6b76a48",
                "max_concurrent": 10,
                "env_config_version": "staging-6b76a48",
                "host_intents": {"trt-gb10-1": "active"},
            },
        )
        assert (
            client.post(
                "/admin/gb10-worker-pools/production/gb10/nodes/trt-gb10-1/report",
                headers=headers,
                json={
                    "current_image_tag": "staging-6b76a48",
                    "current_max_concurrent": 10,
                    "current_env_config_version": "staging-6b76a48",
                    "current_intent": "active",
                    "apply_state": "applied",
                    "worker_id": worker_id,
                },
            ).status_code
            == 200
        )
    worker_state = _fetch_worker(postgres_url, worker_id)
    assert worker_state["drain_state"] == "active"
    assert worker_state["drain_reason"] is None
