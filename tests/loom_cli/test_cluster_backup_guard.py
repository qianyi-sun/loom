from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_backup_guard import (
    REQUIRED_BACKUP_COMPONENTS,
    ROLLOUT_CHECKPOINT_COMPONENTS,
    BackupTraversalLimits,
    backup_manifest_created_at,
    backup_manifest_sha256,
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


def _write_bounded_components(root: Path) -> dict[str, Path]:
    root.mkdir(mode=0o700)
    postgres = root / "postgres.dump"
    postgres.write_bytes(b"p")
    postgres.chmod(0o600)
    minio = root / "minio"
    minio.mkdir(mode=0o700)
    nested = minio / "a"
    nested.mkdir(mode=0o700)
    nested_object = nested / "x"
    nested_object.write_bytes(b"x")
    nested_object.chmod(0o600)
    sibling_object = minio / "a-file"
    sibling_object.write_bytes(b"a")
    sibling_object.chmod(0o600)
    secrets = root / "secrets.yaml"
    secrets.write_bytes(b"s")
    secrets.chmod(0o600)
    return {
        "postgres": postgres,
        "minio": minio,
        "k8s_secrets": secrets,
    }


def _bounded_limits(**overrides: Any) -> BackupTraversalLimits:
    values: dict[str, Any] = {
        "max_files": 4,
        "max_entries": 6,
        "max_total_bytes": 4,
        "max_depth": 2,
        "max_directory_entries": 2,
        "max_elapsed_seconds": 60.0,
        "max_manifest_bytes": 1024 * 1024,
    }
    values.update(overrides)
    return BackupTraversalLimits(**values)


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


def test_rollout_checkpoint_manifest_replaces_minio_payload_with_inventory(tmp_path):
    postgres = tmp_path / "postgres.dump"
    postgres.write_bytes(b"postgres")
    inventory = tmp_path / "object-inventory.json"
    inventory.write_text('{"inventory_root":"abc"}\n', encoding="utf-8")
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "loom-secrets.yaml").write_bytes(b"secret")
    manifest_path = tmp_path / "backup-manifest.json"

    manifest = write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components={
            "postgres": postgres,
            "object_inventory": inventory,
            "k8s_secrets": secrets,
        },
        now=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        schema_version=2,
    )

    assert manifest["schema_version"] == 2
    assert set(manifest["components"]) == set(ROLLOUT_CHECKPOINT_COMPONENTS)
    assert "minio" not in manifest["components"]
    assert (
        validate_backup_manifest(
            manifest_path,
            environment="staging",
            namespace="loom-staging",
            now=datetime(2026, 6, 29, 12, 5, tzinfo=UTC),
        )
        == []
    )


def test_manifest_schema_required_components_cannot_be_substituted(tmp_path):
    manifest_path = _write_private_bundle(
        tmp_path / "bundle",
        created_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = 2
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

    assert any("required_components" in problem for problem in problems)
    assert any("object_inventory" in problem for problem in problems)


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


def test_backup_traversal_succeeds_at_every_exact_limit(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    limits = _bounded_limits()
    manifest_path = root / "backup-manifest.json"

    manifest = write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
        limits=limits,
    )

    assert manifest["components"]["minio"]["file_count"] == 2
    assert (
        validate_backup_manifest(
            manifest_path,
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.getuid(),
            require_private_files=True,
            limits=limits,
        )
        == []
    )


@pytest.mark.parametrize("duration", [float("inf"), float("nan")])
def test_backup_traversal_limits_reject_non_finite_deadline(duration: float) -> None:
    with pytest.raises(ValueError, match="max_elapsed_seconds"):
        BackupTraversalLimits(max_elapsed_seconds=duration)


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"max_files": 3}, "file count limit"),
        ({"max_entries": 5}, "entry count limit"),
        ({"max_total_bytes": 3}, "byte limit"),
        ({"max_depth": 1}, "depth limit"),
    ],
    ids=("file-count", "entry-count", "byte-count", "depth"),
)
def test_write_backup_manifest_fails_closed_when_traversal_limit_is_exceeded(
    tmp_path: Path,
    overrides: dict[str, int],
    expected: str,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"

    with pytest.raises(ValueError, match=expected):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=manifest_path,
            components=components,
            limits=_bounded_limits(**overrides),
        )

    assert not manifest_path.exists()


def test_write_backup_manifest_fails_closed_when_deadline_is_exceeded(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    readings = iter((0.0, 0.0, 2.0))

    def monotonic() -> float:
        return next(readings, 2.0)

    with pytest.raises(ValueError, match="deadline"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=root / "backup-manifest.json",
            components=components,
            limits=_bounded_limits(
                max_elapsed_seconds=1.0,
                monotonic=monotonic,
            ),
        )


def test_directory_window_stops_after_limit_plus_one_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    minio = components["minio"]
    for name in ("b", "c"):
        path = minio / name
        path.write_bytes(name.encode("utf-8"))
        path.chmod(0o600)
    real_scandir = os.scandir
    yielded = 0

    class CountingScandir:
        def __init__(self, directory_fd: int) -> None:
            self._iterator = real_scandir(directory_fd)

        def __enter__(self) -> CountingScandir:
            return self

        def __exit__(self, *_args: object) -> None:
            self._iterator.close()

        def __iter__(self) -> CountingScandir:
            return self

        def close(self) -> None:
            self._iterator.close()

        def __next__(self) -> os.DirEntry[str]:
            nonlocal yielded
            entry = next(self._iterator)
            yielded += 1
            return entry

    def counting_scandir(directory_fd: int) -> CountingScandir:
        return CountingScandir(directory_fd)

    def reject_unbounded_listdir(_path: object) -> list[str]:
        raise AssertionError("unbounded directory inventory is not allowed")

    monkeypatch.setattr(os, "scandir", counting_scandir)
    monkeypatch.setattr(os, "listdir", reject_unbounded_listdir)

    with pytest.raises(ValueError, match="directory entry limit"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=root / "backup-manifest.json",
            components=components,
            limits=_bounded_limits(
                max_files=10,
                max_entries=20,
                max_total_bytes=10,
                max_directory_entries=2,
            ),
        )

    assert yielded == 3


def test_nested_directory_windows_reserve_shared_entry_budget_while_retained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    minio = components["minio"]
    nested = minio / "a"
    for directory, names in (
        (minio, ("z1", "z2")),
        (nested, ("y1", "y2", "y3")),
    ):
        for name in names:
            path = directory / name
            path.write_bytes(name.encode("utf-8"))
            path.chmod(0o600)
    real_scandir = os.scandir
    yielded = 0

    class CountingScandir:
        def __init__(self, directory_fd: int) -> None:
            self._iterator = real_scandir(directory_fd)

        def __enter__(self) -> CountingScandir:
            return self

        def __exit__(self, *_args: object) -> None:
            self._iterator.close()

        def __iter__(self) -> CountingScandir:
            return self

        def close(self) -> None:
            self._iterator.close()

        def __next__(self) -> os.DirEntry[str]:
            nonlocal yielded
            entry = next(self._iterator)
            yielded += 1
            return entry

    monkeypatch.setattr(os, "scandir", CountingScandir)

    with pytest.raises(ValueError, match="entry count limit"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=root / "backup-manifest.json",
            components=components,
            limits=_bounded_limits(
                max_files=20,
                max_entries=7,
                max_total_bytes=20,
                max_directory_entries=10,
            ),
        )

    assert yielded == 6


def test_component_wise_directory_order_matches_schema_one_digest_and_validator(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    manifest = write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
        limits=_bounded_limits(),
    )
    expected = hashlib.sha256()
    for relative, payload in (("a/x", b"x"), ("a-file", b"a")):
        expected.update(relative.encode("utf-8"))
        expected.update(b"\0")
        expected.update(hashlib.sha256(payload).digest())

    assert manifest["components"]["minio"]["sha256"] == expected.hexdigest()
    assert (
        validate_backup_manifest(
            manifest_path,
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.getuid(),
            require_private_files=True,
            limits=_bounded_limits(),
        )
        == []
    )


def test_strict_validation_fails_closed_on_traversal_limit(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
    )

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        limits=_bounded_limits(max_files=3),
    )

    assert any("file count limit" in problem for problem in problems)


def test_strict_validation_rejects_directory_mutation_during_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
    )
    target = root / "minio" / "a-file"
    target_inode = target.stat().st_ino
    real_read = os.read
    mutated = False

    def mutating_read(fd: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(fd, size)
        if chunk and not mutated and os.fstat(fd).st_ino == target_inode:
            added = root / "minio" / "added-during-walk"
            added.write_bytes(b"added")
            added.chmod(0o600)
            mutated = True
        return chunk

    monkeypatch.setattr(os, "read", mutating_read)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
    )

    assert mutated
    assert any("changed during inspection" in problem for problem in problems)


def test_manifest_writer_rejects_earlier_component_mutation_during_later_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    earlier = components["k8s_secrets"]
    target_inode = (components["minio"] / "a-file").stat().st_ino
    real_read = os.read
    mutated = False

    def mutating_read(fd: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(fd, size)
        if chunk and not mutated and os.fstat(fd).st_ino == target_inode:
            earlier.write_bytes(b"n")
            earlier.chmod(0o600)
            mutated = True
        return chunk

    monkeypatch.setattr(os, "read", mutating_read)

    with pytest.raises(ValueError, match="components changed during inspection"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=root / "backup-manifest.json",
            components=components,
        )

    assert mutated


def test_strict_validation_rejects_earlier_component_mutation_during_later_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
    )
    earlier = components["postgres"]
    target_inode = (components["minio"] / "a-file").stat().st_ino
    real_read = os.read
    mutated = False

    def mutating_read(fd: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(fd, size)
        if chunk and not mutated and os.fstat(fd).st_ino == target_inode:
            earlier.write_bytes(b"n")
            earlier.chmod(0o600)
            mutated = True
        return chunk

    monkeypatch.setattr(os, "read", mutating_read)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
    )

    assert mutated
    assert any("components changed during inspection" in problem for problem in problems)


@pytest.mark.parametrize("component", ["postgres", "minio", "k8s_secrets"])
def test_strict_validation_rejects_manifest_root_replacement_during_component_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    component: str,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
    )
    target = components[component]
    if target.is_dir():
        target = target / "a-file"
    target_inode = target.stat().st_ino
    manifest_payload = manifest_path.read_bytes()
    real_read = os.read
    mutated = False

    def replacing_read(fd: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(fd, size)
        if chunk and not mutated and os.fstat(fd).st_ino == target_inode:
            root.rename(tmp_path / "original-bundle")
            _write_bounded_components(root)
            replacement_manifest = root / manifest_path.name
            replacement_manifest.write_bytes(manifest_payload)
            replacement_manifest.chmod(0o600)
            mutated = True
        return chunk

    monkeypatch.setattr(os, "read", replacing_read)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
    )

    assert mutated
    assert any("path changed during inspection" in problem for problem in problems)


def test_strict_validation_rejects_manifest_mutation_during_component_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
    )
    target_inode = (components["minio"] / "a-file").stat().st_ino
    original_payload = manifest_path.read_bytes()
    changed_payload = original_payload.replace(b'"staging"', b'"STAGING"', 1)
    assert len(changed_payload) == len(original_payload)
    real_read = os.read
    mutated = False

    def mutating_read(fd: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(fd, size)
        if chunk and not mutated and os.fstat(fd).st_ino == target_inode:
            manifest_path.write_bytes(changed_payload)
            manifest_path.chmod(0o600)
            mutated = True
        return chunk

    monkeypatch.setattr(os, "read", mutating_read)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
    )

    assert mutated
    assert any("manifest changed during inspection" in problem for problem in problems)


@pytest.mark.parametrize("symlink_kind", ["component", "ancestor"])
def test_manifest_writer_rejects_component_path_symlinks(
    tmp_path: Path,
    symlink_kind: str,
) -> None:
    real_root = tmp_path / "real-bundle"
    components = _write_bounded_components(real_root)
    if symlink_kind == "component":
        target = components["minio"]
        linked = real_root / "linked-minio"
        linked.symlink_to(target.name)
        components["minio"] = linked
    else:
        linked_root = tmp_path / "linked-bundle"
        linked_root.symlink_to(real_root.name)
        components = {
            name: linked_root / path.relative_to(real_root) for name, path in components.items()
        }

    output_path = tmp_path / "backup-manifest.json"
    with pytest.raises(ValueError, match="symlink"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=output_path,
            components=components,
        )

    assert not output_path.exists()


def test_manifest_encoding_is_bounded_before_output_callback(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    output_called = False

    def write_output(_path: Path, _payload: bytes) -> None:
        nonlocal output_called
        output_called = True

    with pytest.raises(ValueError, match="manifest size limit"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=root / "backup-manifest.json",
            components=components,
            limits=_bounded_limits(max_manifest_bytes=1),
            write_output=write_output,
        )

    assert not output_called


def test_manifest_output_callback_receives_single_bounded_payload(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    payloads: list[bytes] = []

    def write_output(path: Path, payload: bytes) -> None:
        assert path == manifest_path
        payloads.append(payload)
        path.write_bytes(payload)

    manifest = write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
        limits=_bounded_limits(),
        write_output=write_output,
    )

    assert payloads == [manifest_path.read_bytes()]
    assert json.loads(payloads[0]) == manifest
    assert manifest_path.stat().st_mode & 0o777 == 0o600


def test_manifest_output_callback_cannot_persist_corrupted_payload(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"

    def corrupt_output(path: Path, _payload: bytes) -> None:
        path.write_bytes(b"{}")

    with pytest.raises(ValueError, match="persisted payload does not match"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=manifest_path,
            components=components,
            write_output=corrupt_output,
        )


def test_manifest_output_callback_cannot_leave_symlink(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    target = root / "manifest-target"

    def symlink_output(path: Path, payload: bytes) -> None:
        target.write_bytes(payload)
        path.symlink_to(target.name)

    with pytest.raises(ValueError, match="persisted safely"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=manifest_path,
            components=components,
            write_output=symlink_output,
        )


def test_manifest_output_parent_creation_does_not_follow_symlink_ancestor(
    tmp_path: Path,
) -> None:
    components = _write_bounded_components(tmp_path / "bundle")
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(outside.name)
    output_path = linked / "missing" / "backup-manifest.json"

    with pytest.raises(ValueError, match="symlink"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=output_path,
            components=components,
        )

    assert not (outside / "missing").exists()


def test_manifest_output_callback_returning_after_deadline_fails_closed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    now = 0.0

    def monotonic() -> float:
        return now

    def slow_output(path: Path, payload: bytes) -> None:
        nonlocal now
        path.write_bytes(payload)
        now = 2.0

    with pytest.raises(ValueError, match="deadline"):
        write_backup_manifest(
            environment="staging",
            namespace="loom-staging",
            output_path=manifest_path,
            components=components,
            limits=BackupTraversalLimits(
                max_elapsed_seconds=1.0,
                monotonic=monotonic,
            ),
            write_output=slow_output,
        )


def test_manifest_reads_and_digest_are_bounded(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    components = _write_bounded_components(root)
    manifest_path = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest_path,
        components=components,
    )
    limits = _bounded_limits(max_manifest_bytes=manifest_path.stat().st_size - 1)

    problems = validate_backup_manifest(
        manifest_path,
        environment="staging",
        namespace="loom-staging",
        limits=limits,
    )

    assert any("manifest size limit" in problem for problem in problems)
    with pytest.raises(ValueError, match="hashed safely"):
        backup_manifest_sha256(
            manifest_path,
            expected_owner_uid=os.getuid(),
            limits=limits,
        )
    with pytest.raises(ValueError, match="read safely"):
        backup_manifest_created_at(
            manifest_path,
            expected_owner_uid=os.getuid(),
            limits=limits,
        )


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


def test_cli_backup_check_accepts_explicit_reviewed_limit_above_100k(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}\n", encoding="utf-8")
    observed: list[BackupTraversalLimits] = []

    def validate_with_simulated_large_manifest(
        *_args: object,
        **kwargs: object,
    ) -> list[str]:
        limits = kwargs.get("limits")
        effective = limits if isinstance(limits, BackupTraversalLimits) else BackupTraversalLimits()
        observed.append(effective)
        if effective.max_files <= 579_720:
            return ["backup component 'k8s_secrets' traversal file count limit exceeded"]
        return []

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.validate_backup_manifest",
        validate_with_simulated_large_manifest,
    )
    base_argv = [
        "cluster",
        "backup",
        "check",
        "--environment",
        "staging",
        "--namespace",
        "loom-staging",
        "--manifest",
        str(manifest),
    ]

    assert main(base_argv) == 1
    assert "traversal file count limit exceeded" in capsys.readouterr().err

    assert (
        main(
            [
                *base_argv,
                "--backup-max-files",
                "1000004",
                "--backup-max-entries",
                "16000000",
                "--backup-max-total-bytes",
                str(16 * 1024**4),
            ]
        )
        == 0
    )
    assert "backup manifest verified" in capsys.readouterr().out
    assert [limits.max_files for limits in observed] == [100_000, 1_000_004]


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--backup-max-files", "0"),
        ("--backup-max-entries", "-1"),
        ("--backup-max-total-bytes", "not-an-integer"),
    ],
)
def test_cli_backup_check_rejects_invalid_explicit_limits(
    tmp_path: Path,
    flag: str,
    value: str,
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}\n", encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "cluster",
                "backup",
                "check",
                "--environment",
                "staging",
                "--namespace",
                "loom-staging",
                "--manifest",
                str(manifest),
                flag,
                value,
            ]
        )

    assert excinfo.value.code == 2
