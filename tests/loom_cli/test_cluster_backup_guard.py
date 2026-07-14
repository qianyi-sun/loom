from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_backup_guard import (
    REQUIRED_BACKUP_COMPONENTS,
    infer_environment,
    validate_backup_manifest,
    write_backup_manifest,
)


def _write_private_bundle(
    root: Path,
    *,
    created_at: datetime,
    postgres_path: Path | None = None,
) -> Path:
    root.mkdir(mode=0o700)
    postgres_dir = root / "postgres"
    postgres_dir.mkdir(mode=0o700)
    postgres = postgres_path or postgres_dir / "loom.dump"
    if postgres_path is None:
        postgres.write_bytes(b"postgres-dump")
        postgres.chmod(0o600)
    minio = root / "minio"
    minio.mkdir(mode=0o700)
    artifact = minio / "artifact.bin"
    artifact.write_bytes(b"artifact-bytes")
    artifact.chmod(0o600)
    secrets = root / "secrets"
    secrets.mkdir(mode=0o700)
    secret = secrets / "loom-secrets.yaml"
    secret.write_bytes(b"secret-bytes")
    secret.chmod(0o600)
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
        now=created_at,
    )
    return manifest_path


@pytest.mark.parametrize(
    ("namespace", "environment"),
    [
        ("loom-staging", "development"),
        ("loom-production", "development"),
        ("loom-staging", "production"),
        ("loom-production", "staging"),
    ],
)
def test_infer_environment_rejects_protected_namespace_disagreement(
    namespace: str,
    environment: str,
) -> None:
    with pytest.raises(ValueError) as exc_info:
        infer_environment(environment=environment, namespace=namespace)

    assert "protected namespace" in str(exc_info.value)
    assert environment not in str(exc_info.value)


@pytest.mark.parametrize(
    ("namespace", "environment", "expected"),
    [
        ("loom-staging", "staging", "staging"),
        ("loom-production", "production", "production"),
        ("loom-custom", "development", "development"),
        ("loom-custom", "preview", "preview"),
    ],
)
def test_infer_environment_keeps_matching_and_nonprotected_explicit_targets(
    namespace: str,
    environment: str,
    expected: str,
) -> None:
    assert infer_environment(environment=environment, namespace=namespace) == expected


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

    assert manifest["schema_version"] == 1
    assert set(REQUIRED_BACKUP_COMPONENTS).issubset(manifest["components"])
    assert manifest["verification"]["status"] == "verified"
    raw_manifest = manifest_path.read_text(encoding="utf-8")
    assert "raw-secret-value" not in raw_manifest
    assert "secret-store-master-key" not in raw_manifest
    assert (
        validate_backup_manifest(
            manifest_path,
            environment="staging",
            namespace="loom-staging",
            max_age_hours=24,
            now=datetime(2026, 6, 29, 12, 5, tzinfo=UTC),
        )
        == []
    )


def test_validate_backup_manifest_rejects_component_changed_after_manifest(tmp_path):
    postgres = tmp_path / "postgres.dump"
    postgres.write_bytes(b"original-dump")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "artifact.bin").write_bytes(b"artifact-bytes")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_bytes(b"secret-bytes")
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
    )

    postgres.write_bytes(b"changed-after-manifest")

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
    )

    assert any("sha256 does not match" in problem for problem in problems)


def test_validate_backup_manifest_rejects_non_integer_recorded_byte_count(tmp_path):
    postgres = tmp_path / "postgres.dump"
    postgres.write_bytes(b"x")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "artifact.bin").write_bytes(b"artifact-bytes")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_bytes(b"secret-bytes")
    manifest_path = tmp_path / "backup-manifest.json"
    manifest = write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
    )
    manifest["components"]["postgres"]["size_bytes"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
    )

    assert any("size_bytes does not match" in problem for problem in problems)


def test_validate_backup_manifest_rejects_boolean_schema_version(tmp_path):
    manifest_path = _write_private_bundle(
        tmp_path / "bundle",
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_path.chmod(0o600)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert any("schema_version" in problem for problem in problems)


def test_resume_integrity_check_rejects_symlink_but_not_age(tmp_path):
    postgres = tmp_path / "postgres.dump"
    postgres.write_bytes(b"original-dump")
    postgres.chmod(0o600)
    minio = tmp_path / "minio"
    minio.mkdir(mode=0o700)
    artifact = minio / "artifact.bin"
    artifact.write_bytes(b"artifact-bytes")
    artifact.chmod(0o600)
    secrets = tmp_path / "secrets.yaml"
    secrets.write_bytes(b"secret-bytes")
    secrets.chmod(0o600)
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
        now=datetime(2026, 6, 27, 12, 0, tzinfo=UTC),
    )
    replacement = tmp_path / "replacement.dump"
    replacement.write_bytes(b"original-dump")
    replacement.chmod(0o600)
    postgres.unlink()
    postgres.symlink_to(replacement)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        enforce_freshness=False,
        now=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
    )

    assert not any("stale" in problem for problem in problems)
    assert any("symlink" in problem for problem in problems)


def test_resume_integrity_check_still_rejects_future_timestamp(tmp_path):
    manifest_path = _write_private_bundle(
        tmp_path / "bundle",
        created_at=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        enforce_freshness=False,
        now=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )

    assert any("future" in problem for problem in problems)


def test_strict_integrity_rejects_component_outside_manifest_root(tmp_path):
    outside = tmp_path / "outside.dump"
    outside.write_bytes(b"postgres-dump")
    outside.chmod(0o600)
    manifest_path = _write_private_bundle(
        tmp_path / "bundle",
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
        postgres_path=outside,
    )

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert any("outside manifest root" in problem for problem in problems)


def test_strict_integrity_rejects_non_private_manifest_root(tmp_path):
    manifest_path = _write_private_bundle(
        tmp_path / "bundle",
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )
    manifest_path.parent.chmod(0o755)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert any("manifest root" in problem and "0700" in problem for problem in problems)


def test_strict_integrity_preserves_schema_one_zero_byte_nested_file_semantics(tmp_path):
    root = tmp_path / "bundle"
    manifest_path = _write_private_bundle(
        root,
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )
    empty_object = root / "minio" / "empty-object"
    empty_object.write_bytes(b"")
    empty_object.chmod(0o600)
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components={
            "postgres": root / "postgres" / "loom.dump",
            "minio": root / "minio",
            "k8s_secrets": root / "secrets",
        },
        now=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert problems == []


@pytest.mark.parametrize(
    "mutation",
    [
        "manifest_symlink",
        "manifest_root_symlink",
        "component_symlink",
        "nested_symlink",
        "dangling_nested_symlink",
        "fifo",
    ],
)
def test_strict_integrity_rejects_symlink_and_nonregular_boundaries(
    tmp_path: Path,
    mutation: str,
) -> None:
    real_root = tmp_path / "bundle"
    manifest_path = _write_private_bundle(
        real_root,
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )
    if mutation == "manifest_symlink":
        target = real_root / "manifest-target.json"
        manifest_path.rename(target)
        manifest_path.symlink_to(target.name)
    elif mutation == "manifest_root_symlink":
        linked_root = tmp_path / "linked-bundle"
        linked_root.symlink_to(real_root.name)
        manifest_path = linked_root / "backup-manifest.json"
    elif mutation == "component_symlink":
        component = real_root / "minio"
        target = real_root / "minio-target"
        component.rename(target)
        component.symlink_to(target.name)
    elif mutation in {"nested_symlink", "dangling_nested_symlink"}:
        nested = real_root / "minio" / "artifact.bin"
        nested.unlink()
        target = real_root / "replacement.bin"
        if mutation == "nested_symlink":
            target.write_bytes(b"artifact-bytes")
            target.chmod(0o600)
        nested.symlink_to(target)
    else:
        postgres = real_root / "postgres" / "loom.dump"
        postgres.unlink()
        os.mkfifo(postgres, mode=0o600)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert problems
    if mutation != "fifo":
        assert any("symlink" in problem for problem in problems)
    else:
        assert any("regular file or directory" in problem for problem in problems)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("wrong_owner", "owner UID"),
        ("manifest_mode", "0600"),
        ("component_file_mode", "0600"),
        ("component_directory_mode", "0700"),
    ],
)
def test_strict_integrity_rejects_wrong_owner_and_private_modes(
    tmp_path: Path,
    mutation: str,
    expected: str,
) -> None:
    root = tmp_path / "bundle"
    manifest_path = _write_private_bundle(
        root,
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )
    expected_uid = os.getuid()
    if mutation == "wrong_owner":
        expected_uid += 1
    elif mutation == "manifest_mode":
        manifest_path.chmod(0o640)
    elif mutation == "component_file_mode":
        (root / "minio" / "artifact.bin").chmod(0o640)
    else:
        (root / "minio").chmod(0o750)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=expected_uid,
        require_private_files=True,
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert any(expected in problem for problem in problems)


def test_strict_integrity_does_not_use_path_resolve_precheck(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = _write_private_bundle(
        tmp_path / "bundle",
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )

    def reject_resolve(_path: Path, *_args: object, **_kwargs: object) -> Path:
        raise AssertionError("strict validation must not call Path.resolve")

    monkeypatch.setattr(Path, "resolve", reject_resolve)

    assert (
        validate_backup_manifest(
            manifest_path,
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.getuid(),
            require_private_files=True,
            now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
        )
        == []
    )


@pytest.mark.parametrize("mutation", ["same_size", "rename", "add", "remove", "kind"])
def test_default_integrity_recomputes_directory_and_kind_metadata(
    tmp_path: Path,
    mutation: str,
) -> None:
    root = tmp_path / "bundle"
    manifest_path = _write_private_bundle(
        root,
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )
    artifact = root / "minio" / "artifact.bin"
    if mutation == "same_size":
        artifact.write_bytes(b"changed-bytes!")
    elif mutation == "rename":
        artifact.rename(root / "minio" / "renamed.bin")
    elif mutation == "add":
        (root / "minio" / "added.bin").write_bytes(b"added")
    elif mutation == "remove":
        artifact.unlink()
    else:
        postgres = root / "postgres" / "loom.dump"
        payload = postgres.read_bytes()
        postgres.unlink()
        postgres.mkdir()
        (postgres / "dump.bin").write_bytes(payload)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        now=datetime(2026, 6, 30, 12, 1, tzinfo=UTC),
    )

    assert problems


def test_directory_digest_is_independent_of_file_creation_order(tmp_path: Path) -> None:
    digests: list[str] = []
    for index, names in enumerate((("a", "b"), ("b", "a"))):
        root = tmp_path / f"order-{index}"
        root.mkdir()
        postgres = root / "postgres.dump"
        postgres.write_bytes(b"postgres")
        minio = root / "minio"
        minio.mkdir()
        for name in names:
            (minio / name).write_bytes(name.encode("utf-8"))
        secrets = root / "secrets.yaml"
        secrets.write_bytes(b"secrets")
        manifest = write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=root / "manifest.json",
            components={
                "postgres": postgres,
                "minio": minio,
                "k8s_secrets": secrets,
            },
        )
        digests.append(manifest["components"]["minio"]["sha256"])

    assert digests[0] == digests[1]


def test_manifest_directory_hashing_streams_files_instead_of_reading_them_whole(
    tmp_path,
    monkeypatch,
):
    postgres = tmp_path / "postgres.dump"
    postgres.write_bytes(b"postgres")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "artifact.bin").write_bytes(b"artifact")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_bytes(b"secrets")

    def reject_read_bytes(_path):
        raise AssertionError("whole-file read is not allowed")

    monkeypatch.setattr(type(postgres), "read_bytes", reject_read_bytes)

    manifest = write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=tmp_path / "backup-manifest.json",
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
    )

    assert manifest["components"]["minio"]["file_count"] == 1


def test_validate_backup_manifest_rejects_missing_secret_backup(tmp_path):
    manifest = {
        "schema_version": 1,
        "environment": "staging",
        "namespace": "loom-staging",
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
        environment="staging",
        namespace="loom-staging",
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


def test_validate_backup_manifest_rejects_near_expiry_snapshot(tmp_path):
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
        min_remaining_hours=2,
        now=datetime(2026, 6, 30, 11, 1, tzinfo=UTC),
    )

    assert any("expires too soon" in problem for problem in problems)
    assert any("requires at least 2h remaining" in problem for problem in problems)


def test_cli_backup_manifest_and_check_round_trip(tmp_path, capsys):
    postgres = tmp_path / "postgres.dump"
    postgres.write_text("pg", encoding="utf-8")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "object").write_text("obj", encoding="utf-8")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_text("secret: do-not-print\n", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"

    rc = main(
        [
            "cluster",
            "backup",
            "manifest",
            "--environment",
            "staging",
            "--namespace",
            "loom-staging",
            "--output",
            str(manifest_path),
            "--postgres-dump",
            str(postgres),
            "--minio-snapshot",
            str(minio),
            "--k8s-secrets",
            str(secrets),
        ]
    )

    assert rc == 0
    assert manifest_path.exists()
    assert "secret: do-not-print" not in capsys.readouterr().out

    rc = main(
        [
            "cluster",
            "backup",
            "check",
            "--environment",
            "staging",
            "--namespace",
            "loom-staging",
            "--manifest",
            str(manifest_path),
        ]
    )

    assert rc == 0
    assert "backup manifest verified" in capsys.readouterr().out
