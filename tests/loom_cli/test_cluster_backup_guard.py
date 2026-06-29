from __future__ import annotations

import json
from datetime import UTC, datetime

from loom_cli.__main__ import main
from loom_cli.cluster_backup_guard import (
    REQUIRED_BACKUP_COMPONENTS,
    validate_backup_manifest,
    write_backup_manifest,
)


def test_backup_manifest_records_components_without_secret_contents(tmp_path):
    postgres = tmp_path / "postgres.dump"
    postgres.write_text("pg-dump-bytes\n", encoding="utf-8")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "artifact.bin").write_bytes(b"artifact-bytes")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_text("secret-store-master-key: raw-secret-value\n", encoding="utf-8")
    manifest_path = tmp_path / "backup-manifest.json"

    manifest = write_backup_manifest(
        environment="public-beta",
        namespace="loom-public-beta",
        output_path=manifest_path,
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
        now=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
    )

    assert manifest["schema_version"] == 1
    assert set(REQUIRED_BACKUP_COMPONENTS).issubset(manifest["components"])
    assert manifest["verification"]["status"] == "verified"
    raw_manifest = manifest_path.read_text(encoding="utf-8")
    assert "raw-secret-value" not in raw_manifest
    assert "secret-store-master-key" not in raw_manifest
    assert validate_backup_manifest(
        manifest_path,
        environment="public-beta",
        namespace="loom-public-beta",
        max_age_hours=24,
        now=datetime(2026, 6, 29, 12, 5, tzinfo=UTC),
    ) == []


def test_validate_backup_manifest_rejects_missing_secret_backup(tmp_path):
    manifest = {
        "schema_version": 1,
        "environment": "public-beta",
        "namespace": "loom-public-beta",
        "created_at": "2026-06-29T12:00:00+00:00",
        "components": {
            "postgres": {"path": str(tmp_path / "pg.dump"), "size_bytes": 1},
            "minio": {"path": str(tmp_path / "minio"), "size_bytes": 1},
        },
        "verification": {"status": "verified"},
    }
    path = tmp_path / "backup-manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    problems = validate_backup_manifest(
        path,
        environment="public-beta",
        namespace="loom-public-beta",
        max_age_hours=24,
        now=datetime(2026, 6, 29, 12, 5, tzinfo=UTC),
    )

    assert any("k8s_secrets" in problem for problem in problems)


def test_validate_backup_manifest_rejects_stale_snapshot(tmp_path):
    postgres = tmp_path / "postgres.dump"
    postgres.write_text("pg", encoding="utf-8")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "object").write_text("obj", encoding="utf-8")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_text("redacted", encoding="utf-8")
    manifest_path = tmp_path / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
        now=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
    )

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        max_age_hours=24,
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert any("stale" in problem for problem in problems)


def test_cli_backup_manifest_and_check_round_trip(tmp_path, capsys):
    postgres = tmp_path / "postgres.dump"
    postgres.write_text("pg", encoding="utf-8")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "object").write_text("obj", encoding="utf-8")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_text("secret: do-not-print\n", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"

    rc = main([
        "cluster", "backup", "manifest",
        "--environment", "public-beta",
        "--namespace", "loom-public-beta",
        "--output", str(manifest_path),
        "--postgres-dump", str(postgres),
        "--minio-snapshot", str(minio),
        "--k8s-secrets", str(secrets),
    ])

    assert rc == 0
    assert manifest_path.exists()
    assert "secret: do-not-print" not in capsys.readouterr().out

    rc = main([
        "cluster", "backup", "check",
        "--environment", "public-beta",
        "--namespace", "loom-public-beta",
        "--manifest", str(manifest_path),
    ])

    assert rc == 0
    assert "backup manifest verified" in capsys.readouterr().out
