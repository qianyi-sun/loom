from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


def _stateful_set(name: str, *, release: str, lineage: str) -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": name,
            "namespace": "loom-dev",
            "annotations": {"loom.dev/trusted-release-sha256": release},
        },
        "spec": {
            "serviceName": name,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "app", "image": release}]},
            },
            "volumeClaimTemplates": [
                {
                    "metadata": {
                        "name": "data",
                        "annotations": {"loom.dev/storage-lineage": lineage},
                    },
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "longhorn",
                        "resources": {"requests": {"storage": "20Gi"}},
                    },
                }
            ],
        },
    }


def _write_manifest(path: Path, *, release: str, lineage: str) -> None:
    documents = [
        _stateful_set("loom-dev-postgres", release=release, lineage=lineage),
        _stateful_set("loom-dev-minio", release=release, lineage=lineage),
    ]
    path.write_text(yaml.safe_dump_all(documents, sort_keys=False), encoding="utf-8")


def _run_guard(current: Path, previous: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "loom.personal_dev_storage_lineage_guard",
            "--current",
            str(current),
            "--previous",
            str(previous),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_guard_accepts_release_fresh_outer_stateful_metadata(tmp_path: Path) -> None:
    current = tmp_path / "current.yaml"
    previous = tmp_path / "previous.yaml"
    _write_manifest(current, release="current-release", lineage="installed-lineage")
    _write_manifest(previous, release="previous-release", lineage="installed-lineage")

    result = _run_guard(current, previous)

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_guard_rejects_claim_template_lineage_drift(tmp_path: Path) -> None:
    current = tmp_path / "current.yaml"
    previous = tmp_path / "previous.yaml"
    _write_manifest(current, release="current-release", lineage="changed-lineage")
    _write_manifest(previous, release="previous-release", lineage="installed-lineage")

    result = _run_guard(current, previous)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == (
        "personal-dev storage lineage rejected: "
        "StatefulSet claim templates differ from installed storage lineage\n"
    )
