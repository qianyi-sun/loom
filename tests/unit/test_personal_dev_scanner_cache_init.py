from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

import loom.personal_dev_scanner_cache_init as scanner_init
from loom.personal_dev_scanner_cache import (
    PersonalDevScannerCacheBinding,
    PersonalDevScannerCacheFiles,
)
from loom.personal_dev_scanner_cache_init import (
    PersonalDevScannerCacheInstallError,
    install_personal_dev_scanner_cache,
)

_DATABASE_METADATA = (
    b'{"DownloadedAt":"2026-08-18T18:40:00Z","NextUpdate":"2026-08-19T18:35:46Z",'
    b'"UpdatedAt":"2026-08-18T18:35:46Z","Version":2}'
)
_JAVA_DATABASE_METADATA = (
    b'{"DownloadedAt":"2026-08-18T18:40:00Z","NextUpdate":"2026-08-21T01:10:28Z",'
    b'"UpdatedAt":"2026-08-18T01:10:28Z","Version":1}'
)
_STAGING_NAME = ".loom-scanner-cache-staging-" + "1" * 24


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _source(tmp_path: Path, name: str, *, marker: bytes = b"one") -> Path:
    source = tmp_path / name
    (source / "db").mkdir(parents=True)
    (source / "java-db").mkdir()
    (source / "db/trivy.db").write_bytes(b"vulnerability-db:" + marker)
    (source / "db/metadata.json").write_bytes(_DATABASE_METADATA)
    (source / "java-db/trivy-java.db").write_bytes(b"java-db:" + marker)
    (source / "java-db/metadata.json").write_bytes(_JAVA_DATABASE_METADATA)
    for path in source.rglob("*"):
        path.chmod(0o555 if path.is_dir() else 0o444)
    source.chmod(0o555)
    return source


def _binding(source: Path) -> PersonalDevScannerCacheBinding:
    files = PersonalDevScannerCacheFiles(
        database_sha256=_sha256((source / "db/trivy.db").read_bytes()),
        database_metadata_sha256=_sha256((source / "db/metadata.json").read_bytes()),
        java_database_sha256=_sha256((source / "java-db/trivy-java.db").read_bytes()),
        java_database_metadata_sha256=_sha256(
            (source / "java-db/metadata.json").read_bytes()
        ),
    )
    binary_sha256 = _sha256(b"trivy-v0.74.0")
    identity_sha256 = hashlib.sha256(
        b"test-personal-dev-scanner-cache-v1\0"
        + binary_sha256.encode("ascii")
        + files.canonical_bytes()
    ).hexdigest()
    return PersonalDevScannerCacheBinding(
        cache_identity_sha256=identity_sha256,
        scanner_binary_sha256=binary_sha256,
        files=files,
    )


def _destination(tmp_path: Path) -> Path:
    destination = tmp_path / "scanner-cache"
    destination.mkdir(mode=0o770)
    return destination


def _identity_bytes(binding: PersonalDevScannerCacheBinding) -> bytes:
    return json.dumps(
        {
            "cache_identity_sha256": binding.cache_identity_sha256,
            "database_metadata_sha256": binding.files.database_metadata_sha256,
            "database_sha256": binding.files.database_sha256,
            "java_database_metadata_sha256": (
                binding.files.java_database_metadata_sha256
            ),
            "java_database_sha256": binding.files.java_database_sha256,
            "scanner_binary_sha256": binding.scanner_binary_sha256,
            "schema_version": 1,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _install_command(
    source: Path,
    destination: Path,
    binding: PersonalDevScannerCacheBinding,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "loom.personal_dev_scanner_cache_init",
        "--source-root",
        str(source),
        "--destination-root",
        str(destination),
        "--cache-identity-sha256",
        binding.cache_identity_sha256,
        "--scanner-binary-sha256",
        binding.scanner_binary_sha256,
        "--database-sha256",
        binding.files.database_sha256,
        "--database-metadata-sha256",
        binding.files.database_metadata_sha256,
        "--java-database-sha256",
        binding.files.java_database_sha256,
        "--java-database-metadata-sha256",
        binding.files.java_database_metadata_sha256,
    ]


def test_installer_publishes_exact_protected_generation(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)

    installed = install_personal_dev_scanner_cache(source, destination, expected=binding)

    generation = destination / "generations" / binding.cache_identity_sha256
    assert installed == generation
    assert (destination / "active-generation").read_text(encoding="ascii") == (
        binding.cache_identity_sha256 + "\n"
    )
    assert (generation / "identity.json").read_bytes() == _identity_bytes(binding)
    assert {
        path.relative_to(generation).as_posix() for path in generation.rglob("*")
    } == {
        "db",
        "db/metadata.json",
        "db/trivy.db",
        "fanal",
        "identity.json",
        "java-db",
        "java-db/metadata.json",
        "java-db/trivy-java.db",
    }
    assert stat.S_IMODE((destination / "generations").stat().st_mode) == 0o755
    assert stat.S_IMODE(generation.stat().st_mode) == 0o555
    for protected_directory in (generation / "db", generation / "java-db"):
        assert stat.S_IMODE(protected_directory.stat().st_mode) == 0o555
    for protected_file in (
        generation / "identity.json",
        generation / "db/metadata.json",
        generation / "db/trivy.db",
        generation / "java-db/metadata.json",
        generation / "java-db/trivy-java.db",
        destination / "active-generation",
    ):
        metadata = protected_file.stat()
        assert stat.S_IMODE(metadata.st_mode) == 0o444
        assert metadata.st_uid == os.geteuid()
        assert metadata.st_gid == os.getegid()
        assert metadata.st_nlink == 1
    fanal = generation / "fanal"
    assert stat.S_IMODE(fanal.stat().st_mode) == 0o770
    assert fanal.stat().st_uid == os.geteuid()
    assert fanal.stat().st_gid == os.getegid()


def test_installer_enforces_generation_modes_under_restrictive_umask(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    previous_umask = os.umask(0o077)
    try:
        installed = install_personal_dev_scanner_cache(
            source,
            destination,
            expected=binding,
        )
    finally:
        os.umask(previous_umask)

    assert stat.S_IMODE((destination / "generations").stat().st_mode) == 0o755
    assert stat.S_IMODE(installed.stat().st_mode) == 0o555
    assert stat.S_IMODE((installed / "db/trivy.db").stat().st_mode) == 0o444


def test_identical_install_is_idempotent(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    first = install_personal_dev_scanner_cache(source, destination, expected=binding)
    identity_inode = (first / "identity.json").stat().st_ino
    marker_inode = (destination / "active-generation").stat().st_ino

    second = install_personal_dev_scanner_cache(source, destination, expected=binding)

    assert second == first
    assert (second / "identity.json").stat().st_ino == identity_inode
    assert (destination / "active-generation").stat().st_ino == marker_inode


def test_installer_retains_current_and_previous_generation_only(tmp_path: Path) -> None:
    destination = _destination(tmp_path)
    bindings: list[PersonalDevScannerCacheBinding] = []
    sources: list[Path] = []
    for index, marker in enumerate((b"one", b"two", b"three"), start=1):
        source = _source(tmp_path, f"source-{index}", marker=marker)
        sources.append(source)
        bindings.append(_binding(source))

    install_personal_dev_scanner_cache(sources[0], destination, expected=bindings[0])
    install_personal_dev_scanner_cache(sources[1], destination, expected=bindings[1])
    assert {item.name for item in (destination / "generations").iterdir()} == {
        bindings[0].cache_identity_sha256,
        bindings[1].cache_identity_sha256,
    }

    install_personal_dev_scanner_cache(sources[2], destination, expected=bindings[2])

    assert {item.name for item in (destination / "generations").iterdir()} == {
        bindings[1].cache_identity_sha256,
        bindings[2].cache_identity_sha256,
    }
    assert (destination / "active-generation").read_text() == (
        bindings[2].cache_identity_sha256 + "\n"
    )


def test_installer_removes_one_old_interrupted_staging_directory(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    generations = destination / "generations"
    generations.mkdir(mode=0o755)
    staging = generations / _STAGING_NAME
    (staging / "db").mkdir(parents=True, mode=0o700)
    staging.chmod(0o700)
    (staging / "db/partial").write_bytes(b"partial")
    old = time.time() - 7200
    os.utime(staging / "db/partial", (old, old))
    os.utime(staging / "db", (old, old))
    os.utime(staging, (old, old))
    binding = _binding(source)

    install_personal_dev_scanner_cache(source, destination, expected=binding)

    assert not staging.exists()
    assert (generations / binding.cache_identity_sha256).is_dir()


def test_installer_removes_one_recent_orphaned_staging_directory(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    generations = destination / "generations"
    generations.mkdir(mode=0o755)
    staging = generations / _STAGING_NAME
    (staging / "db").mkdir(parents=True, mode=0o700)
    staging.chmod(0o700)
    (staging / "db/partial").write_bytes(b"partial")
    binding = _binding(source)

    install_personal_dev_scanner_cache(source, destination, expected=binding)

    assert not staging.exists()
    assert (generations / binding.cache_identity_sha256).is_dir()


def test_installer_refuses_a_concurrent_destination_lock(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    lock_path = destination / ".loom-scanner-cache-installer.lock"
    lock_path.touch(mode=0o600)
    with lock_path.open("r+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        result = subprocess.run(
            _install_command(source, destination, binding),
            text=True,
            capture_output=True,
            check=False,
        )

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "error: personal-dev scanner cache installation failed\n"
    assert not (destination / "active-generation").exists()
    assert not (destination / "generations" / binding.cache_identity_sha256).exists()


def test_installer_detects_destination_lock_path_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    lock_path = destination / ".loom-scanner-cache-installer.lock"
    displaced_lock = destination / "displaced-lock"
    original_flock = scanner_init.fcntl.flock

    def replace_lock_after_acquisition(descriptor: int, operation: int) -> None:
        original_flock(descriptor, operation)
        lock_path.rename(displaced_lock)
        lock_path.touch(mode=0o600)

    monkeypatch.setattr(scanner_init.fcntl, "flock", replace_lock_after_acquisition)

    with pytest.raises(PersonalDevScannerCacheInstallError, match="installation failed"):
        install_personal_dev_scanner_cache(source, destination, expected=binding)

    assert not (destination / "active-generation").exists()
    assert not (destination / "generations").exists()


def test_module_entrypoint_installs_the_generation(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    result = subprocess.run(
        _install_command(source, destination, binding),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""
    assert (
        destination / "generations" / binding.cache_identity_sha256 / "identity.json"
    ).read_bytes() == _identity_bytes(binding)


@pytest.mark.parametrize(
    "problem",
    [
        "source-symlink",
        "file-symlink",
        "hardlink",
        "fifo",
        "wrong-hash",
        "malformed-metadata",
        "extra",
    ],
)
def test_installer_rejects_unsafe_or_inconsistent_source(
    tmp_path: Path,
    problem: str,
) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    if problem == "source-symlink":
        linked = tmp_path / "source-link"
        linked.symlink_to(source, target_is_directory=True)
        source = linked
    elif problem == "file-symlink":
        target = source / "db/trivy.db"
        target.parent.chmod(0o755)
        target.chmod(0o644)
        payload = target.read_bytes()
        target.unlink()
        external = tmp_path / "external-db"
        external.write_bytes(payload)
        target.symlink_to(external)
        target.parent.chmod(0o555)
    elif problem == "hardlink":
        os.link(source / "db/trivy.db", tmp_path / "second-link")
    elif problem == "fifo":
        target = source / "db/trivy.db"
        target.parent.chmod(0o755)
        target.unlink()
        os.mkfifo(target)
        target.parent.chmod(0o555)
    elif problem == "wrong-hash":
        binding = replace(
            binding,
            files=replace(binding.files, database_sha256="1" * 64),
        )
    elif problem == "malformed-metadata":
        target = source / "db/metadata.json"
        target.chmod(0o644)
        target.write_bytes(b"{}")
        target.chmod(0o444)
        binding = replace(
            binding,
            files=replace(binding.files, database_metadata_sha256=_sha256(b"{}")),
        )
    else:
        source.chmod(0o755)
        (source / "extra").write_bytes(b"unexpected")
        source.chmod(0o555)

    with pytest.raises(PersonalDevScannerCacheInstallError, match="installation failed"):
        install_personal_dev_scanner_cache(source, destination, expected=binding)

    assert not (destination / "active-generation").exists()
    assert not list(destination.glob(".active-generation-*"))


def test_installer_detects_source_change_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    database = source / "db/trivy.db"
    database_inode = database.stat().st_ino
    original_read = scanner_init.os.read
    changed = False

    def read_then_change(descriptor: int, amount: int) -> bytes:
        nonlocal changed
        payload = original_read(descriptor, amount)
        if not changed and os.fstat(descriptor).st_ino == database_inode:
            changed = True
            database.chmod(0o644)
            database.write_bytes(b"changed-database:" + b"x" * len(payload))
            database.chmod(0o444)
        return payload

    monkeypatch.setattr(scanner_init.os, "read", read_then_change)

    with pytest.raises(PersonalDevScannerCacheInstallError, match="installation failed"):
        install_personal_dev_scanner_cache(source, destination, expected=binding)

    assert not (destination / "active-generation").exists()


@pytest.mark.parametrize("problem", ["root-symlink", "generations-symlink"])
def test_installer_rejects_destination_symlinks(tmp_path: Path, problem: str) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    if problem == "root-symlink":
        destination.rmdir()
        destination.symlink_to(outside, target_is_directory=True)
    else:
        (destination / "generations").symlink_to(outside, target_is_directory=True)

    with pytest.raises(PersonalDevScannerCacheInstallError, match="installation failed"):
        install_personal_dev_scanner_cache(source, destination, expected=_binding(source))

    assert not tuple(outside.iterdir())


def test_installer_rejects_tampered_digest_named_generation(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    binding = _binding(source)
    generation = install_personal_dev_scanner_cache(source, destination, expected=binding)
    database = generation / "db/trivy.db"
    database.chmod(0o644)
    database.write_bytes(b"tampered")
    database.chmod(0o444)

    with pytest.raises(PersonalDevScannerCacheInstallError, match="installation failed"):
        install_personal_dev_scanner_cache(source, destination, expected=binding)

    assert database.read_bytes() == b"tampered"
    assert generation.is_dir()


@pytest.mark.parametrize("problem", ["invalid-active", "unexpected", "too-many"])
def test_installer_rejects_ambiguous_generation_state(tmp_path: Path, problem: str) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    generations = destination / "generations"
    generations.mkdir(mode=0o755)
    if problem == "invalid-active":
        marker = destination / "active-generation"
        marker.write_text("not-a-digest\n", encoding="ascii")
        marker.chmod(0o444)
    elif problem == "unexpected":
        (generations / "unexpected").write_bytes(b"x")
    else:
        for index in range(17):
            (generations / f"{index + 1:064x}").mkdir()

    with pytest.raises(PersonalDevScannerCacheInstallError, match="installation failed"):
        install_personal_dev_scanner_cache(source, destination, expected=_binding(source))

    assert not list(generations.glob(".loom-scanner-cache-staging-*"))


def test_installer_refuses_stale_cleanup_above_sixteen_gibibytes(tmp_path: Path) -> None:
    source = _source(tmp_path, "source")
    destination = _destination(tmp_path)
    generations = destination / "generations"
    generations.mkdir(mode=0o755)
    staging = generations / _STAGING_NAME
    staging.mkdir(mode=0o700)
    huge = staging / "partial"
    with huge.open("wb") as stream:
        stream.truncate(16 * 1024 * 1024 * 1024 + 1)
    old = time.time() - 7200
    os.utime(huge, (old, old))
    os.utime(staging, (old, old))

    with pytest.raises(PersonalDevScannerCacheInstallError, match="installation failed"):
        install_personal_dev_scanner_cache(source, destination, expected=_binding(source))

    assert staging.is_dir()
    assert not (destination / "active-generation").exists()
