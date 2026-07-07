from __future__ import annotations

import json
from typing import Any

from loom_cli.cluster_cmd import apply_manifests


class _FakeApps:
    def __init__(self, statefulsets: dict[str, dict[str, Any]]) -> None:
        self.statefulsets = statefulsets

    def read_namespaced_stateful_set(self, *, name: str, namespace: str) -> dict[str, Any]:
        return self.statefulsets[name]


def _live_minio_statefulset(*, storage: str = "500Gi") -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "loom-minio", "namespace": "loom-staging"},
        "spec": {
            "podManagementPolicy": "OrderedReady",
            "replicas": 1,
            "selector": {"matchLabels": {"app": "loom-minio"}},
            "serviceName": "loom-minio",
            "template": {
                "metadata": {"labels": {"app": "loom-minio"}},
                "spec": {"containers": [{"name": "minio", "image": "minio/minio"}]},
            },
            "volumeClaimTemplates": [
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {"name": "data"},
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": storage}},
                        "storageClassName": "",
                        "volumeMode": "Filesystem",
                        "volumeName": "loom-staging-minio-data",
                    },
                },
            ],
        },
    }


def _rendered_manifest(*, storage: str = "500Gi") -> str:
    return f"""
apiVersion: v1
kind: Service
metadata:
  name: loom-minio
spec:
  ports:
    - port: 9000
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: loom-minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: loom-minio
  serviceName: loom-minio
  template:
    metadata:
      labels:
        app: loom-minio
    spec:
      containers:
        - name: minio
          image: minio/minio
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: {storage}
"""


def test_apply_manifests_patches_existing_statefulset_without_immutable_pvc_defaults(
    monkeypatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def _run(cmd: list[str], **kwargs: Any) -> Any:
        calls.append({"cmd": cmd, **kwargs})
        stdout = (
            "service/loom-minio configured\n"
            if cmd[:2] == ["kubectl", "apply"]
            else "statefulset.apps/loom-minio patched\n"
        )
        return type(
            "Completed",
            (),
            {"returncode": 0, "stdout": stdout, "stderr": ""},
        )()

    monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/kubectl")
    monkeypatch.setattr("subprocess.run", _run)

    result = apply_manifests(
        _rendered_manifest(),
        "loom-staging",
        context="kind-loom-staging",
        apps_v1=_FakeApps({"loom-minio": _live_minio_statefulset()}),
    )

    assert result.returncode == 0
    assert len(calls) == 2
    assert calls[0]["cmd"][:4] == ["kubectl", "apply", "-n", "loom-staging"]
    assert "kind: StatefulSet" not in calls[0]["input"]
    assert calls[1]["cmd"][:5] == [
        "kubectl",
        "patch",
        "statefulset",
        "loom-minio",
        "-n",
    ]
    patch = json.loads(calls[1]["cmd"][calls[1]["cmd"].index("-p") + 1])
    assert "volumeClaimTemplates" not in patch["spec"]
    assert patch["spec"]["template"]["metadata"]["labels"] == {"app": "loom-minio"}
    assert result.summary_lines == [
        "service/loom-minio configured",
        "statefulset.apps/loom-minio patched",
    ]


def test_apply_manifests_rejects_real_statefulset_volume_claim_drift(monkeypatch) -> None:
    subprocess_calls: list[list[str]] = []
    monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/kubectl")
    monkeypatch.setattr(
        "subprocess.run",
        lambda cmd, **_kwargs: subprocess_calls.append(cmd),
    )

    result = apply_manifests(
        _rendered_manifest(storage="500Gi"),
        "loom-staging",
        context=None,
        apps_v1=_FakeApps({"loom-minio": _live_minio_statefulset(storage="400Gi")}),
    )

    assert result.returncode == 1
    assert subprocess_calls == []
    assert "loom-minio" in result.stderr
    assert "volumeClaimTemplates" in result.stderr
    assert "immutable" in result.stderr
