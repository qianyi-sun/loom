from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token, WorkerPoolAutoscalerPolicy
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "A" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{RAW_ADMIN_TOKEN}\"\n"
        "created_at = \"2026-06-27T00:00:00Z\"\n"
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
def clean_autoscaler_policies(postgres_url: str) -> Iterator[None]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(delete(WorkerPoolAutoscalerPolicy))
        s.execute(delete(Token))
        s.commit()
    try:
        yield
    finally:
        with session_factory() as s:
            s.execute(delete(WorkerPoolAutoscalerPolicy))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


def _policy_payload() -> dict[str, object]:
    return {
        "actuator": "slurm",
        "enabled": True,
        "min_slots": 6,
        "max_slots": 30,
        "scale_up_threshold_slots": 1,
        "scale_down_idle_seconds": 600,
        "scale_up_cooldown_seconds": 60,
        "scale_down_cooldown_seconds": 300,
        "drain_timeout_seconds": 600,
        "force": False,
        "actuator_config": {
            "allowed_nodes": ["oldlab-1", "oldlab-2"],
            "requested_concurrency": 6,
            "cpu_arch": "x86_64",
        },
    }


def test_policy_put_get_and_status_round_trip(app) -> None:
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        upserted = client.put(
            "/admin/worker-pool-autoscaler-policies/production/oldlab",
            headers=headers,
            json=_policy_payload(),
        )
        assert upserted.status_code == 200, upserted.text
        body = upserted.json()
        assert body["environment"] == "production"
        assert body["pool_name"] == "oldlab"
        assert body["actuator"] == "slurm"
        assert body["enabled"] is True
        assert body["min_slots"] == 6
        assert body["max_slots"] == 30
        assert body["last_decision"] is None

        fetched = client.get(
            "/admin/worker-pool-autoscaler-policies/production/oldlab",
            headers=headers,
        )
        assert fetched.status_code == 200, fetched.text
        assert fetched.json()["actuator_config"]["allowed_nodes"] == [
            "oldlab-1",
            "oldlab-2",
        ]

        status = client.get(
            "/admin/worker-pool-autoscalers/status",
            headers=headers,
        )
        assert status.status_code == 200, status.text
        status_body = status.json()
        assert status_body["policies"][0]["pool_name"] == "oldlab"


def test_policy_rejects_max_slots_below_min_slots(app) -> None:
    payload = _policy_payload()
    payload["min_slots"] = 10
    payload["max_slots"] = 5

    with TestClient(app) as client:
        response = client.put(
            "/admin/worker-pool-autoscaler-policies/production/oldlab",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=payload,
        )

    assert response.status_code == 400, response.text
    assert "max_slots" in response.json()["detail"]
