"""Migration step substrate bootstrap contracts (#206)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s09_migrate import (
    MigrateStep,
    _write_stateful_substrate_manifest,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def _rendered_manifest() -> str:
    return """
apiVersion: v1
kind: PersistentVolume
metadata:
  name: loom-staging-postgres-data
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: loom-staging-minio-data
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: loom-staging-worker-trajectories-data
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: loom-worker-trajectories
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: loom-postgres
---
apiVersion: v1
kind: Service
metadata:
  name: loom-postgres
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-pgbouncer
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: loom-minio
---
apiVersion: v1
kind: Service
metadata:
  name: loom-minio
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-control-plane
"""


def _prepare_candidate_worktree(ev: EvidenceDirectory) -> Path:
    worktree = ev.step_dir(1, "worktree").path / "src"
    package_dir = worktree / "src" / "loom_cli"
    package_dir.mkdir(parents=True)
    (package_dir / "__main__.py").write_text("raise SystemExit(0)\n")
    return worktree


def test_stateful_substrate_manifest_filters_only_static_storage_and_stateful_services(
    tmp_path: Path,
) -> None:
    rendered = tmp_path / "rendered.yaml"
    target = tmp_path / "stateful-substrate.yaml"
    rendered.write_text(_rendered_manifest(), encoding="utf-8")

    resources = _write_stateful_substrate_manifest(
        rendered,
        target,
        namespace="loom-staging",
    )

    assert resources == [
        "PersistentVolume/loom-staging-postgres-data",
        "PersistentVolume/loom-staging-minio-data",
        "PersistentVolume/loom-staging-worker-trajectories-data",
        "PersistentVolumeClaim/loom-worker-trajectories",
        "StatefulSet/loom-postgres",
        "Service/loom-postgres",
        "StatefulSet/loom-minio",
        "Service/loom-minio",
    ]
    docs = [doc for doc in yaml.safe_load_all(target.read_text(encoding="utf-8")) if doc]
    assert [(doc["kind"], doc["metadata"]["name"]) for doc in docs] == [
        ("PersistentVolume", "loom-staging-postgres-data"),
        ("PersistentVolume", "loom-staging-minio-data"),
        ("PersistentVolume", "loom-staging-worker-trajectories-data"),
        ("PersistentVolumeClaim", "loom-worker-trajectories"),
        ("StatefulSet", "loom-postgres"),
        ("Service", "loom-postgres"),
        ("StatefulSet", "loom-minio"),
        ("Service", "loom-minio"),
    ]


def test_migrate_bootstraps_stateful_substrate_before_migration_job(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ctx = make_ctx(tmp_path, namespace="loom-staging", image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    ev.step_dir(7, "render").artifact_path("rendered.yaml").write_text(
        _rendered_manifest(),
        encoding="utf-8",
    )
    step_dir = ev.step_dir(9, "migrate")
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs: Any) -> SubprocessResult:
        calls.append(list(argv))
        if list(argv)[:3] == [sys.executable, "-m", "loom_cli"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="apiVersion: batch/v1\nkind: Job\n",
                stderr="",
            )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="ok\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s09_migrate.run_captured", fake_run)

    result = MigrateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    substrate = str(step_dir.artifact_path("stateful-substrate.yaml"))
    assert calls[1] == ["kubectl", "-n", "loom-staging", "apply", "-f", substrate]
    assert calls[2] == [
        "kubectl",
        "-n",
        "loom-staging",
        "rollout",
        "status",
        "statefulset/loom-postgres",
        "--timeout=300s",
    ]
    assert calls[3] == [
        "kubectl",
        "-n",
        "loom-staging",
        "rollout",
        "status",
        "statefulset/loom-minio",
        "--timeout=300s",
    ]
    assert calls[4][:4] == ["kubectl", "-n", "loom-staging", "apply"]
    assert calls[5][:5] == ["kubectl", "-n", "loom-staging", "wait", "--for=condition=complete"]
