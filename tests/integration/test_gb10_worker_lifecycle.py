from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import sessionmaker

from loom.db.schema import GB10WorkerNodeStatus, GB10WorkerPoolDesiredState, Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "G" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{RAW_ADMIN_TOKEN}\"\n"
        "created_at = \"2026-06-26T00:00:00Z\"\n"
        "version = 1\n",
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
        s.execute(delete(Token))
        s.commit()
    try:
        yield
    finally:
        with session_factory() as s:
            s.execute(delete(GB10WorkerNodeStatus))
            s.execute(delete(GB10WorkerPoolDesiredState))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


def test_desired_state_node_report_and_status_round_trip(app) -> None:
    with TestClient(app) as client:
        desired = client.put(
            "/admin/gb10-worker-pools/production/gb10-arm64/desired-state",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "image_tag": "2026-06-26-gb10",
                "max_concurrent": 10,
                "env_config_version": "gb10-env-v2",
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
        assert body["pool_name"] == "gb10-arm64"
        assert body["image_tag"] == "2026-06-26-gb10"
        assert body["max_concurrent"] == 10
        assert body["env_config_version"] == "gb10-env-v2"
        assert body["target_slots"] == 10
        assert body["host_intents"] == {"trt-gb10-1": "draining"}
        assert body["previous_image_tag"] is None
        assert body["rollout_policy"]["mode"] == "canary"

        fetched = client.get(
            "/admin/gb10-worker-pools/production/gb10-arm64/desired-state",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert fetched.status_code == 200, fetched.text
        assert fetched.json()["image_tag"] == "2026-06-26-gb10"

        reported = client.post(
            "/admin/gb10-worker-pools/production/gb10-arm64/nodes/trt-gb10-1/report",
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
            },
        )
        assert reported.status_code == 200, reported.text
        node = reported.json()
        assert node["hostname"] == "trt-gb10-1"
        assert node["desired_image_tag"] == "2026-06-26-gb10"
        assert node["desired_max_concurrent"] == 10
        assert node["desired_env_config_version"] == "gb10-env-v2"
        assert node["desired_intent"] == "draining"
        assert node["current_intent"] == "draining"
        assert node["apply_state"] == "applied"

        status = client.get(
            "/admin/gb10-worker-pools/status",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert status.status_code == 200, status.text
        status_body = status.json()

    assert len(status_body["desired_states"]) == 1
    assert len(status_body["nodes"]) == 1
    assert status_body["nodes"][0]["hostname"] == "trt-gb10-1"
    assert status_body["nodes"][0]["last_heartbeat_at"] is not None


def test_desired_state_update_records_previous_version_for_rollback(app) -> None:
    with TestClient(app) as client:
        headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
        first = client.put(
            "/admin/gb10-worker-pools/production/gb10-arm64/desired-state",
            headers=headers,
            json={
                "image_tag": "old-image",
                "max_concurrent": 5,
                "env_config_version": "old-env",
            },
        )
        assert first.status_code == 200, first.text

        second = client.put(
            "/admin/gb10-worker-pools/production/gb10-arm64/desired-state",
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
            "/admin/gb10-worker-pools/production/gb10-arm64/desired-state",
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
            "/admin/gb10-worker-pools/production/gb10-arm64/desired-state",
            headers=headers,
            json={
                "image_tag": "safe",
                "max_concurrent": 10,
                "env_config_version": "safe-env",
            },
        )
        assert desired.status_code == 200, desired.text

        report = client.post(
            "/admin/gb10-worker-pools/production/gb10-arm64/nodes/trt-gb10-1/report",
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
