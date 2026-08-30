from __future__ import annotations

import asyncio
import hashlib
import json
import os
import select
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_service.personal_dev_builder as builder_module
from loom.personal_dev_candidate import PersonalDevCandidateLimits
from loom_service.personal_dev_builder import build_personal_dev_builder_runtime
from loom_service.personal_dev_candidate_gc import build_personal_dev_artifact_collector

_FILE_OWNER_UID = os.geteuid()
_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _management_uid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(builder_module.os, "geteuid", lambda: 65532)
    monkeypatch.setattr(
        builder_module,
        "_SCANNER_CACHE_PROTECTED_UID",
        _FILE_OWNER_UID,
        raising=False,
    )
    monkeypatch.setattr(
        builder_module,
        "_SCANNER_CACHE_PROTECTED_GID",
        os.getegid(),
        raising=False,
    )


def _settings(tmp_path: Path, **overrides):
    executable = tmp_path / "tool"
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    database = b"trivy-db"
    database_metadata = b'{"DownloadedAt":"2026-08-18T00:00:00Z","NextUpdate":"2026-08-19T00:00:00Z","UpdatedAt":"2026-08-18T00:00:00Z","Version":2}'
    java_database = b"trivy-java-db"
    java_database_metadata = b'{"DownloadedAt":"2026-08-18T00:00:00Z","NextUpdate":"2026-08-19T00:00:00Z","UpdatedAt":"2026-08-18T00:00:00Z","Version":1}'
    scanner_binary_sha256 = hashlib.sha256(executable.read_bytes()).hexdigest()
    database_sha256 = hashlib.sha256(database).hexdigest()
    database_metadata_sha256 = hashlib.sha256(database_metadata).hexdigest()
    java_database_sha256 = hashlib.sha256(java_database).hexdigest()
    java_database_metadata_sha256 = hashlib.sha256(java_database_metadata).hexdigest()
    scanner_without_identity = {
        "binary_platform": "linux/amd64",
        "binary_sha256": scanner_binary_sha256,
        "database_metadata_sha256": database_metadata_sha256,
        "database_sha256": database_sha256,
        "java_database_metadata_sha256": java_database_metadata_sha256,
        "java_database_sha256": java_database_sha256,
        "lock_sha256": "1" * 64,
        "trivy_version": "v0.70.0",
    }
    cache_identity_sha256 = hashlib.sha256(
        b"loom-personal-dev-scanner-cache-v1\0"
        + json.dumps(
            scanner_without_identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    cache = tmp_path / "scanner-cache" / "generations" / cache_identity_sha256
    (cache / "db").mkdir(parents=True)
    (cache / "java-db").mkdir()
    (cache / "fanal").mkdir()
    (cache / "db" / "trivy.db").write_bytes(database)
    (cache / "db" / "metadata.json").write_bytes(database_metadata)
    (cache / "java-db" / "trivy-java.db").write_bytes(java_database)
    (cache / "java-db" / "metadata.json").write_bytes(java_database_metadata)
    identity = {
        "cache_identity_sha256": cache_identity_sha256,
        "database_metadata_sha256": database_metadata_sha256,
        "database_sha256": database_sha256,
        "java_database_metadata_sha256": java_database_metadata_sha256,
        "java_database_sha256": java_database_sha256,
        "scanner_binary_sha256": scanner_binary_sha256,
        "schema_version": 1,
    }
    (cache / "identity.json").write_bytes(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii")
    )
    for protected in (
        cache / "db" / "trivy.db",
        cache / "db" / "metadata.json",
        cache / "java-db" / "trivy-java.db",
        cache / "java-db" / "metadata.json",
        cache / "identity.json",
    ):
        protected.chmod(0o444)
    for directory in (cache / "db", cache / "java-db", cache):
        directory.chmod(0o555)
    scanner_identity = (
        "trivy-bin-sha256:"
        + scanner_binary_sha256
        + ":db-sha256:"
        + database_sha256
        + ":java-db-sha256:"
        + java_database_sha256
    )
    registry_auth = tmp_path / "registry" / "config.json"
    registry_auth.parent.mkdir()
    registry_auth.write_text('{"auths":{"registry.example":{}}}')
    registry_auth.chmod(0o640)
    values = {
        "dev_instances_enabled": True,
        "personal_dev_builder_enabled": True,
        "personal_dev_candidate_gc_retention_sec": 86400,
        "personal_dev_candidate_gc_lease_sec": 900,
        "personal_dev_candidate_gc_poll_interval_sec": 30.0,
        "artifacts_bucket": "artifacts",
        "personal_dev_source_max_archive_bytes": 1024 * 1024,
        "personal_dev_builder_lease_sec": 4200,
        "personal_dev_builder_max_artifact_bytes": 8 * 1024 * 1024,
        "personal_dev_builder_max_image_archive_bytes": 512 * 1024,
        "personal_dev_builder_image": "registry.example/builder@sha256:" + "a" * 64,
        "personal_dev_builder_runtime_class_name": "loom-personal-dev-builder",
        "dev_instance_kubectl_path": executable,
        "dev_instance_kube_context": "dev",
        "personal_dev_builder_scanner_path": executable,
        "personal_dev_builder_scanner_cache_dir": cache,
        "personal_dev_builder_scanner_identity": scanner_identity,
        "personal_dev_builder_scanner_cache_identity_sha256": (
            cache_identity_sha256
        ),
        "personal_dev_builder_scanner_database_metadata_sha256": (
            database_metadata_sha256
        ),
        "personal_dev_builder_scanner_java_database_metadata_sha256": (
            java_database_metadata_sha256
        ),
        "personal_dev_builder_scanner_policy_sha256": "c" * 64,
        "personal_dev_builder_skopeo_path": executable,
        "personal_dev_builder_docker_path": executable,
        "personal_dev_builder_registry_prefix": "registry.example/personal-dev",
        "personal_dev_builder_registry_auth_file": registry_auth,
        "personal_dev_builder_publisher_identity": (
            "system:serviceaccount:loom-dev:candidate-exporter"
        ),
        "personal_dev_trusted_launcher_profile_sha256": "d" * 64,
        "personal_dev_protocol_versions_json": json.dumps(
            {
                "capacity-agent": "v1",
                "claim-guard": "v1",
                "control-plane-worker": "v1",
                "database-migrations": "expand-compatible-v1",
                "personal-dev-activation": "v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_builder_runtime_is_inert_unless_explicitly_enabled(tmp_path: Path) -> None:
    settings = _settings(tmp_path, personal_dev_builder_enabled=False)
    assert (
        build_personal_dev_builder_runtime(
            settings,  # type: ignore[arg-type]
            minio_client=object(),
        )
        is None
    )


def test_builder_runtime_wires_one_exact_bounded_authority(tmp_path: Path) -> None:
    runtime = build_personal_dev_builder_runtime(
        _settings(tmp_path),  # type: ignore[arg-type]
        minio_client=object(),
    )

    assert runtime is not None
    assert runtime.manifest_config.max_artifact_bytes == 8 * 1024 * 1024
    assert runtime.manifest_config.runtime_class_name == "loom-personal-dev-builder"
    assert runtime.capabilities.max_artifact_bytes == 8 * 1024 * 1024
    assert runtime.exporter.registry_prefix == "registry.example/personal-dev"


async def test_builder_run_loop_starts_one_lease_owner_per_global_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches configuring a global limit without starting concurrent workers."""
    started: set[str] = set()
    cancelled: set[str] = set()
    first_started = asyncio.Event()
    release = asyncio.Event()

    class _Coordinator:
        def __init__(self, **kwargs: object) -> None:
            self.builder_id = str(kwargs["builder_id"])

        async def build_once(self, *, now: object) -> bool:
            del now
            started.add(self.builder_id)
            first_started.set()
            try:
                await release.wait()
            finally:
                cancelled.add(self.builder_id)
            return True

    monkeypatch.setattr(builder_module, "PersonalDevBuildCoordinator", _Coordinator)
    task = asyncio.create_task(
        builder_module.personal_dev_builder_run_loop(
            session_factory=object(),  # type: ignore[arg-type]
            source=object(),  # type: ignore[arg-type]
            executor=object(),  # type: ignore[arg-type]
            limits=PersonalDevCandidateLimits(
                global_active_builds=3,
                per_owner_active_builds=1,
            ),
            builder_id="loom-service:test",
            lease_seconds=4200,
            registry_prefix="registry.example/personal-dev",
            poll_interval_seconds=1.0,
        )
    )
    try:
        await first_started.wait()
        await asyncio.sleep(0)
        assert started == {
            "loom-service:test:worker:0",
            "loom-service:test:worker:1",
            "loom-service:test:worker:2",
        }
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert cancelled == started


def test_builder_runtime_rejects_placeholder_safety_authority(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="launcher profile"):
        build_personal_dev_builder_runtime(
            _settings(tmp_path, personal_dev_trusted_launcher_profile_sha256=""),  # type: ignore[arg-type]
            minio_client=object(),
        )


@pytest.mark.parametrize(
    "drift",
    [
        "identity",
        "database-metadata",
        "java-database-metadata",
        "generation-name",
        "writable-protected-file",
        "writable-protected-directory",
        "unexpected-root-entry",
        "unexpected-database-entry",
        "protected-symlink",
        "protected-hardlink",
        "fanal-symlink",
        "management-owned-generation",
        "unexpected-protected-owner",
        "unexpected-protected-group",
        "noncanonical-identity",
        "nonfinite-identity",
        "duplicate-identity-field",
        "database",
        "java-database",
        "binary-symlink",
        "binary",
    ],
)
def test_builder_runtime_revalidates_release_bound_scanner_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    settings = _settings(tmp_path)
    cache = settings.personal_dev_builder_scanner_cache_dir
    if drift == "identity":
        identity = cache / "identity.json"
        identity.chmod(0o644)
        value = json.loads(identity.read_bytes())
        value["cache_identity_sha256"] = "f" * 64
        identity.write_bytes(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
        )
        identity.chmod(0o444)
    elif drift == "database-metadata":
        settings.personal_dev_builder_scanner_database_metadata_sha256 = "f" * 64
    elif drift == "java-database-metadata":
        settings.personal_dev_builder_scanner_java_database_metadata_sha256 = "f" * 64
    elif drift == "generation-name":
        settings.personal_dev_builder_scanner_cache_identity_sha256 = "f" * 64
    elif drift == "writable-protected-file":
        (cache / "db" / "trivy.db").chmod(0o644)
    elif drift == "writable-protected-directory":
        (cache / "db").chmod(0o755)
    elif drift == "unexpected-root-entry":
        cache.chmod(0o755)
        (cache / "unexpected").write_text("unexpected")
        cache.chmod(0o555)
    elif drift == "unexpected-database-entry":
        database = cache / "db"
        database.chmod(0o755)
        (database / "unexpected").write_text("unexpected")
        database.chmod(0o555)
    elif drift == "protected-symlink":
        database = cache / "db"
        protected = database / "trivy.db"
        target = tmp_path / "linked-database"
        target.write_bytes(protected.read_bytes())
        target.chmod(0o444)
        database.chmod(0o755)
        protected.unlink()
        protected.symlink_to(target)
        database.chmod(0o555)
    elif drift == "protected-hardlink":
        database = cache / "db"
        protected = database / "trivy.db"
        target = tmp_path / "hardlinked-database"
        target.write_bytes(protected.read_bytes())
        target.chmod(0o444)
        database.chmod(0o755)
        protected.unlink()
        os.link(target, protected)
        database.chmod(0o555)
    elif drift == "fanal-symlink":
        cache.chmod(0o755)
        (cache / "fanal").rmdir()
        (cache / "fanal").symlink_to(tmp_path)
        cache.chmod(0o555)
    elif drift == "management-owned-generation":
        monkeypatch.setattr(builder_module.os, "geteuid", lambda: _FILE_OWNER_UID)
    elif drift == "unexpected-protected-owner":
        monkeypatch.setattr(
            builder_module,
            "_SCANNER_CACHE_PROTECTED_UID",
            _FILE_OWNER_UID + 1,
            raising=False,
        )
    elif drift == "unexpected-protected-group":
        monkeypatch.setattr(
            builder_module,
            "_SCANNER_CACHE_PROTECTED_GID",
            os.getegid() + 1,
            raising=False,
        )
    elif drift in {
        "noncanonical-identity",
        "nonfinite-identity",
        "duplicate-identity-field",
    }:
        identity_path = cache / "identity.json"
        identity_path.chmod(0o644)
        if drift == "noncanonical-identity":
            identity_path.write_text(
                json.dumps(json.loads(identity_path.read_bytes()), indent=2)
            )
        elif drift == "nonfinite-identity":
            value = json.loads(identity_path.read_bytes())
            value["schema_version"] = float("nan")
            identity_path.write_text(
                json.dumps(value, sort_keys=True, separators=(",", ":"))
            )
        else:
            identity_path.write_bytes(
                identity_path.read_bytes().replace(
                    b"{",
                    b'{"schema_version":1,',
                    1,
                )
            )
        identity_path.chmod(0o444)
    elif drift in {"database", "java-database"}:
        protected = (
            cache / "db" / "trivy.db"
            if drift == "database"
            else cache / "java-db" / "trivy-java.db"
        )
        protected.chmod(0o644)
        protected.write_bytes(b"changed")
        protected.chmod(0o444)
    elif drift == "binary-symlink":
        executable = settings.personal_dev_builder_scanner_path
        target = tmp_path / "real-scanner"
        executable.rename(target)
        executable.symlink_to(target)
    else:
        settings.personal_dev_builder_scanner_path.write_text("changed")
        settings.personal_dev_builder_scanner_path.chmod(0o755)

    with pytest.raises(RuntimeError, match="scanner cache binding"):
        build_personal_dev_builder_runtime(
            settings,  # type: ignore[arg-type]
            minio_client=object(),
        )


def test_scanner_binary_hash_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    scanner = tmp_path / "trivy"
    os.mkfifo(scanner)
    command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path\n"
            "from loom_service.personal_dev_builder import _regular_file_sha256\n"
            "import sys\n"
            "print('ready', flush=True)\n"
            "sys.stdin.readline()\n"
            "try:\n"
            "    _regular_file_sha256(\n"
            "        Path(sys.argv[1]), label='scanner executable', maximum_bytes=1024,\n"
            "    )\n"
            "except RuntimeError:\n"
            "    raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
        str(scanner),
    ]
    environment = {
        **os.environ,
        "PYTHONPATH": f"{_ROOT / 'src'}:{_ROOT}",
    }

    process = subprocess.Popen(
        command,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdin is not None
        initialized, _, _ = select.select([process.stdout], [], [], 30)
        if not initialized:
            pytest.fail("scanner binary FIFO subprocess did not initialize")
        ready = process.stdout.readline()
        if ready != "ready\n":
            assert process.stderr is not None
            if process.poll() is None:
                process.kill()
                process.wait()
            pytest.fail(
                "scanner binary FIFO subprocess initialization failed: "
                + process.stderr.read()
            )
        process.stdin.write("verify\n")
        process.stdin.flush()
        try:
            returncode = process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pytest.fail("scanner binary FIFO blocked before type validation")
        assert process.stderr is not None
        assert returncode == 0, process.stderr.read()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_artifact_collector_is_wired_only_for_personal_lifecycle(tmp_path: Path) -> None:
    disabled_root = tmp_path / "disabled"
    enabled_root = tmp_path / "enabled"
    disabled_root.mkdir()
    enabled_root.mkdir()
    settings = _settings(disabled_root, dev_instances_enabled=False)
    assert (
        build_personal_dev_artifact_collector(
            settings,  # type: ignore[arg-type]
            minio_client=object(),
        )
        is None
    )
    builder_disabled_root = tmp_path / "builder-disabled"
    builder_disabled_root.mkdir()
    builder_disabled = _settings(
        builder_disabled_root,
        personal_dev_builder_enabled=False,
    )
    assert (
        build_personal_dev_artifact_collector(
            builder_disabled,  # type: ignore[arg-type]
            minio_client=object(),
        )
        is None
    )

    configured = _settings(enabled_root)
    collector = build_personal_dev_artifact_collector(
        configured,  # type: ignore[arg-type]
        minio_client=object(),
    )
    assert collector is not None
    assert collector.objects.expected_bucket == "artifacts"
    assert collector.registry.expected_registry_prefix == "registry.example/personal-dev"


def test_artifact_collector_rejects_invalid_gc_settings_at_startup(tmp_path: Path) -> None:
    for override in (
        {"personal_dev_candidate_gc_retention_sec": -1},
        {"personal_dev_candidate_gc_lease_sec": 0},
        {"personal_dev_candidate_gc_poll_interval_sec": 0.0},
    ):
        root = tmp_path / str(len(list(tmp_path.iterdir())))
        root.mkdir()
        with pytest.raises(RuntimeError, match="GC settings"):
            build_personal_dev_artifact_collector(
                _settings(root, **override),  # type: ignore[arg-type]
                minio_client=object(),
            )
