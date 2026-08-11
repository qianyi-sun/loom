from __future__ import annotations

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY = _ROOT / "deploy" / "dev-fleet"


def test_shared_fixture_is_one_postgres_and_minio_without_inline_secrets() -> None:
    text = (_DEPLOY / "shared-fixture.yaml").read_text(encoding="utf-8")
    docs = [doc for doc in yaml.safe_load_all(text) if doc]
    stateful = [doc for doc in docs if doc["kind"] == "StatefulSet"]
    assert [doc["metadata"]["name"] for doc in stateful] == [
        "loom-dev-postgres",
        "loom-dev-minio",
    ]
    assert all(doc["metadata"]["namespace"] == "loom-dev" for doc in docs[1:])
    assert "secretKeyRef" in text
    assert "password:" not in text.lower()
    minio = next(doc for doc in stateful if doc["metadata"]["name"] == "loom-dev-minio")
    assert [container["name"] for container in minio["spec"]["template"]["spec"]["containers"]] == [
        "minio",
        "admin",
    ]
    ingress = next(doc for doc in docs if doc["kind"] == "Ingress")
    assert ingress["spec"]["rules"][0]["host"] == "minio.dev.yylx.world"


def test_global_supervisor_timer_has_one_writer_and_hardened_paths() -> None:
    service = (_DEPLOY / "loom-global-dev-fleet-autoscaler.service").read_text(encoding="utf-8")
    timer = (_DEPLOY / "loom-global-dev-fleet-autoscaler.timer").read_text(encoding="utf-8")
    assert "global_dev_fleet_autoscaler_external_once.py" in service
    assert "--global-budget ${LOOM_DEV_GLOBAL_BUDGET}" in service
    assert "--management-db-url-file" in service
    assert "--fixture-admin-db-url-file" in service
    assert "User=loom-dev-autoscaler" in service
    assert "UMask=0077" in service
    assert "NoNewPrivileges=true" in service
    assert "ProtectSystem=strict" in service
    assert "OnUnitActiveSec=15s" in timer
    assert "Persistent=true" in timer
