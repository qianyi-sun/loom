from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loom_cli.__main__ import main
from loom_cli.minio_storage_preflight import (
    MinioStorageThresholds,
    build_minio_storage_preflight,
    validate_minio_storage_preflight_artifact,
)

DF_OUTPUT = """Filesystem     1024-blocks      Used Available Capacity Mounted on
/dev/nvme0n1p2  7839322112 5051154432 2469776384      68% /data
"""

DU_OUTPUT = """/data/artifacts\t16777216
/data/trajectories\t7340032
/data/loom-benchmarks\t5242880
/data/loom-tasks\t1048576
"""


class _FakeRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> str:
        self.calls.append(argv)
        if "df -Pk /data" in argv:
            return DF_OUTPUT
        if any("du -sk" in part for part in argv):
            return DU_OUTPUT
        raise AssertionError(f"unexpected command: {argv}")


def _stopped_artifact() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "outcome": "stop",
        "namespace": "loom-staging",
        "pod": "loom-minio-0",
        "generated_at": "2026-07-06T00:00:00Z",
        "filesystem": {
            "path": "/data",
            "size_bytes": 1000,
            "used_bytes": 920,
            "free_bytes": 80,
            "used_percent": 92.0,
            "free_percent": 8.0,
        },
        "thresholds": {
            "warn_free_percent": 25.0,
            "stop_free_percent": 15.0,
        },
        "checks": [
            {
                "name": "minio-data-free-space",
                "outcome": "stop",
                "detail": "free space 8.0% is below stop threshold 15.0%",
                "remediation": "Reclaim MinIO space or provision storage before submitting a large batch.",
            }
        ],
    }


def test_build_minio_storage_preflight_records_filesystem_buckets_and_headroom() -> None:
    runner = _FakeRunner()

    report = build_minio_storage_preflight(
        namespace="loom-staging",
        pod="loom-minio-0",
        thresholds=MinioStorageThresholds(
            warn_free_percent=25.0,
            stop_free_percent=15.0,
        ),
        estimated_batch_bytes=512 * 1024 * 1024,
        run_command=runner,
    )

    assert report["outcome"] == "pass"
    assert report["storage_contract"]["mode"] == "hostpath-local-pv"
    assert report["filesystem"]["path"] == "/data"
    assert report["filesystem"]["size_bytes"] == 7839322112 * 1024
    assert report["filesystem"]["used_percent"] == 68.0
    buckets = {bucket["name"]: bucket["usage_bytes"] for bucket in report["buckets"]}
    assert buckets == {
        "artifacts": 16777216 * 1024,
        "trajectories": 7340032 * 1024,
        "loom-benchmarks": 5242880 * 1024,
        "loom-tasks": 1048576 * 1024,
    }
    assert report["headroom"]["estimated_batch_bytes"] == 512 * 1024 * 1024
    assert "kubectl" in runner.calls[0][0]
    assert "-n" in runner.calls[0]
    assert "loom-staging" in runner.calls[0]


def test_validate_minio_storage_preflight_artifact_requires_override_on_stop(
    tmp_path: Path,
) -> None:
    path = tmp_path / "storage-preflight.json"
    path.write_text(json.dumps(_stopped_artifact()), encoding="utf-8")

    result = validate_minio_storage_preflight_artifact(path, allow_stop_override=False)

    assert result.ok is False
    assert result.outcome == "stop"
    assert "explicit storage override" in result.message


def test_minio_storage_preflight_cli_writes_json_and_fails_without_override(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.build_minio_storage_preflight",
        lambda **_kwargs: _stopped_artifact(),
    )
    output = tmp_path / "storage-preflight.json"

    rc = main(
        [
            "cluster",
            "minio-storage-preflight",
            "--namespace",
            "loom-staging",
            "--output",
            str(output),
            "--format",
            "json",
        ]
    )

    assert rc == 1
    assert json.loads(output.read_text(encoding="utf-8"))["outcome"] == "stop"
    assert json.loads(capsys.readouterr().out)["outcome"] == "stop"


def test_minio_storage_preflight_cli_allows_explicit_stop_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.build_minio_storage_preflight",
        lambda **_kwargs: _stopped_artifact(),
    )

    rc = main(
        [
            "cluster",
            "minio-storage-preflight",
            "--namespace",
            "loom-staging",
            "--output",
            str(tmp_path / "storage-preflight.json"),
            "--allow-storage-stop-override",
        ]
    )

    assert rc == 0
