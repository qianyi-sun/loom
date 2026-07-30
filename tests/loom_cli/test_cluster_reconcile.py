from __future__ import annotations

import argparse
import json
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli import cluster_cmd


def _deploy(name: str, image: str, *, replicas: int = 1, namespace: str = "loom-staging") -> dict:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "replicas": replicas,
            "template": {"spec": {"containers": [{"name": name, "image": image}]}},
        },
    }


def test_read_live_objects_reads_present_skips_absent_and_unnamed() -> None:
    desired = [_deploy("a", "i:1"), _deploy("b", "i:1"), {"kind": "Deployment", "metadata": {}}]

    def fake_getter(kind: str, namespace: str, name: str, *, context: Any) -> dict | None:
        return _deploy("a", "i:1") if name == "a" else None

    live = cluster_cmd._read_live_objects(desired, context=None, getter=fake_getter)
    assert [obj["metadata"]["name"] for obj in live] == ["a"]


def test_reconcile_shadow_emits_desired_vs_live_drift_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    desired_docs = [
        _deploy("loom-service", "loom:NEW", replicas=3),
        _deploy("loom-minio", "minio:1"),
    ]
    monkeypatch.setattr(
        cluster_cmd, "render_manifests", lambda config: yaml.safe_dump_all(desired_docs)
    )

    def fake_getter(kind: str, namespace: str, name: str, *, context: Any) -> dict | None:
        if name == "loom-service":
            return _deploy("loom-service", "loom:OLD", replicas=2)  # drifted
        return None  # loom-minio absent from live

    monkeypatch.setattr(cluster_cmd, "_kubectl_get_json", fake_getter)

    args = argparse.Namespace(config=None, context=None, target="rev9", shadow=True)
    assert cluster_cmd._reconcile(args) == 0

    out = json.loads(capsys.readouterr().out)
    assert out["in_sync"] is False
    assert out["target"] == "rev9"
    by_name = {r["name"]: r for r in out["resources"]}
    assert by_name["loom-service"]["status"] == "modified"
    assert "spec.replicas" in by_name["loom-service"]["changed_paths"]
    assert by_name["loom-minio"]["status"] == "absent-from-live"


def test_reconcile_requires_shadow_flag(capsys: pytest.CaptureFixture[str]) -> None:
    args = argparse.Namespace(config=None, context=None, target=None, shadow=False)
    assert cluster_cmd._reconcile(args) == 2
    assert "shadow" in capsys.readouterr().err


def test_reconcile_surfaces_live_read_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cluster_cmd, "render_manifests", lambda config: yaml.safe_dump_all([_deploy("x", "i:1")])
    )

    def boom(kind: str, namespace: str, name: str, *, context: Any) -> dict | None:
        raise RuntimeError("kubectl get Deployment/x failed: connection refused")

    monkeypatch.setattr(cluster_cmd, "_kubectl_get_json", boom)
    args = argparse.Namespace(config=None, context=None, target=None, shadow=True)
    assert cluster_cmd._reconcile(args) == 2
    assert "connection refused" in capsys.readouterr().err
