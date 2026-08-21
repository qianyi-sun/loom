#!/usr/bin/env python3
"""Verify an offline Phase 1 task-image-builder host release bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import lzma
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

MAX_METADATA_BYTES = 64 * 1024 * 1024
MAX_ARTIFACT_BYTES = 1024 * 1024 * 1024
MAX_BINARY_BYTES = 512 * 1024 * 1024
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
FINGERPRINT_RE = re.compile(r"^[A-F0-9]{40}$")
ARCHITECTURE_MAP = {"x86_64": "amd64", "aarch64": "arm64"}
EXPECTED_SNAPSHOT = "20260820T000000Z"
EXPECTED_SUITES = ("noble", "noble-updates")
PACKAGE_ORDER = ("libsubid4", "uidmap", "quota")
EXPECTED_PACKAGE_SUITES = {
    "libsubid4": "noble-updates",
    "uidmap": "noble-updates",
    "quota": "noble",
}
EXPECTED_SETUID_PATHS = {"./usr/bin/newgidmap", "./usr/bin/newuidmap"}
EXPECTED_RUNTIME_RELEASE = "rootless-runtime-v1"
EXPECTED_RUNTIME_BINARIES = {
    "buildctl",
    "buildkit-runc",
    "buildkitd",
    "fuse-overlayfs",
    "rootlessctl",
    "rootlesskit",
    "slirp4netns",
}


class HostReleaseError(ValueError):
    """The offline host release or bundle is unsafe."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class CommandRunner(Protocol):
    def run(
        self,
        args: Sequence[str],
        *,
        input_bytes: bytes | None = None,
    ) -> CommandResult: ...


class SubprocessCommandRunner:
    def run(
        self,
        args: Sequence[str],
        *,
        input_bytes: bytes | None = None,
    ) -> CommandResult:
        result = subprocess.run(
            list(args),
            input=input_bytes,
            capture_output=True,
            check=False,
        )
        return CommandResult(
            result.returncode,
            result.stdout.decode("utf-8", errors="strict"),
            result.stderr.decode("utf-8", errors="replace"),
        )


@dataclass(frozen=True)
class PackageArtifact:
    package: str
    source_suite: str
    version: str
    architecture: str
    filename: str
    size: int
    sha256: str


@dataclass(frozen=True)
class RepositoryIndex:
    suite: str
    inrelease_path: str
    inrelease_size: int
    inrelease_sha256: str
    packages_path: str
    packages_size: int
    packages_sha256: str


@dataclass(frozen=True)
class RepositoryMetadata:
    base_url: str
    indexes: Mapping[str, RepositoryIndex]


@dataclass(frozen=True)
class HostRelease:
    source_path: Path
    release: str
    runtime_manifest: str
    signer_fingerprint: str
    keyring_name: str
    keyring_sha256: str
    snapshot: str
    architecture_map: Mapping[str, str]
    repositories: Mapping[str, RepositoryMetadata]
    packages: Mapping[str, Mapping[str, PackageArtifact]]


@dataclass
class VerifiedHostBundle:
    architecture: str
    bundle_digest: str
    snapshot_root: Path
    package_paths: tuple[Path, ...]
    runtime_paths: tuple[Path, ...]

    def close(self) -> None:
        try:
            metadata = self.snapshot_root.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise HostReleaseError("bundle snapshot cannot be inspected for cleanup") from exc
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise HostReleaseError("bundle snapshot metadata is unsafe for cleanup")
        try:
            shutil.rmtree(self.snapshot_root)
        except OSError as exc:
            raise HostReleaseError("bundle snapshot cleanup failed") from exc


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise HostReleaseError(f"{label} must be an object")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise HostReleaseError(f"{label} must be a non-empty string")
    return value


def _exact_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise HostReleaseError(f"{label} fields are invalid")


def _bounded_size(value: object, maximum: int, label: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 < value <= maximum
    ):
        raise HostReleaseError(f"{label} is invalid")
    return value


def _safe_relative(value: object, label: str) -> str:
    raw = _string(value, label)
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts or path.name in {"", "."}:
        raise HostReleaseError(f"{label} is not a safe relative path")
    return raw


def _read_regular(path: Path, limit: int, label: str) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            initial = os.fstat(descriptor)
            if not stat.S_ISREG(initial.st_mode):
                raise HostReleaseError(f"{label} must be a regular file")
            if initial.st_mode & 0o022:
                raise HostReleaseError(f"{label} is group/world writable")
            if initial.st_size > limit:
                raise HostReleaseError(f"{label} exceeds its size limit")

            chunks: list[bytes] = []
            remaining = limit + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            final = os.fstat(descriptor)
            initial_identity = (
                initial.st_dev,
                initial.st_ino,
                initial.st_size,
                initial.st_mtime_ns,
                initial.st_ctime_ns,
            )
            final_identity = (
                final.st_dev,
                final.st_ino,
                final.st_size,
                final.st_mtime_ns,
                final.st_ctime_ns,
            )
            if initial_identity != final_identity or len(payload) != initial.st_size:
                raise HostReleaseError(f"{label} changed while being read")
            if len(payload) > limit:
                raise HostReleaseError(f"{label} exceeds its size limit")
            return payload
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise HostReleaseError(f"{label} cannot be read safely") from exc


def _load_json(path: Path, label: str) -> dict[str, object]:
    payload = _read_regular(path, MAX_METADATA_BYTES, label)
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HostReleaseError(f"{label} is not valid JSON") from exc
    return _object(value, label)


def load_host_release(path: Path) -> HostRelease:
    raw = _load_json(path, "host release")
    _exact_keys(
        raw,
        {
            "schema",
            "release",
            "runtime_manifest",
            "ubuntu",
            "architecture_map",
            "repositories",
            "packages",
        },
        "host release",
    )
    if raw["schema"] != "loom.task-image-builder-host-release/v2":
        raise HostReleaseError("host release schema is invalid")
    release_name = _string(raw["release"], "release")
    if release_name != "host-release-v2":
        raise HostReleaseError("host release name is invalid")
    runtime_manifest = _safe_relative(raw["runtime_manifest"], "runtime manifest")

    ubuntu = _object(raw["ubuntu"], "ubuntu")
    _exact_keys(
        ubuntu,
        {
            "os_id",
            "version_id",
            "snapshot",
            "component",
            "signer_fingerprint",
            "keyring_name",
            "keyring_sha256",
        },
        "ubuntu",
    )
    if ubuntu["os_id"] != "ubuntu" or ubuntu["version_id"] != "24.04":
        raise HostReleaseError("host operating system contract is invalid")
    if ubuntu["component"] != "main":
        raise HostReleaseError("Ubuntu component is invalid")
    signer = _string(ubuntu["signer_fingerprint"], "signer fingerprint")
    if FINGERPRINT_RE.fullmatch(signer) is None:
        raise HostReleaseError("signer fingerprint is invalid")
    keyring_name = _safe_relative(ubuntu["keyring_name"], "keyring name")
    if PurePosixPath(keyring_name).name != keyring_name:
        raise HostReleaseError("keyring name must not contain a directory")
    keyring_sha256 = _string(ubuntu["keyring_sha256"], "keyring digest")
    if SHA256_RE.fullmatch(keyring_sha256) is None:
        raise HostReleaseError("keyring digest is invalid")
    snapshot = _string(ubuntu["snapshot"], "Ubuntu snapshot")
    if snapshot != EXPECTED_SNAPSHOT:
        raise HostReleaseError("Ubuntu snapshot is invalid")

    architecture_map_raw = _object(raw["architecture_map"], "architecture map")
    if not architecture_map_raw:
        raise HostReleaseError("architecture map is empty")
    architecture_map: dict[str, str] = {}
    for native, debian in architecture_map_raw.items():
        if native not in ARCHITECTURE_MAP or debian != ARCHITECTURE_MAP[native]:
            raise HostReleaseError("architecture map is invalid")
        architecture_map[native] = debian

    repositories_raw = _object(raw["repositories"], "repositories")
    packages_raw = _object(raw["packages"], "packages")
    if set(repositories_raw) != set(architecture_map.values()) or set(packages_raw) != set(
        architecture_map.values()
    ):
        raise HostReleaseError("architecture release rows are incomplete")

    repositories: dict[str, RepositoryMetadata] = {}
    packages: dict[str, dict[str, PackageArtifact]] = {}
    for architecture in architecture_map.values():
        repository = _object(repositories_raw[architecture], f"{architecture} repository")
        _exact_keys(
            repository,
            {"base_url", "indexes"},
            f"{architecture} repository",
        )
        base_url = _string(repository["base_url"], "repository base URL")
        expected_base_url = f"https://snapshot.ubuntu.com/ubuntu/{snapshot}"
        if base_url != expected_base_url:
            raise HostReleaseError("repository base URL is invalid")
        indexes_raw = _object(repository["indexes"], f"{architecture} indexes")
        if tuple(indexes_raw) != EXPECTED_SUITES:
            raise HostReleaseError("repository suite set is invalid")
        indexes: dict[str, RepositoryIndex] = {}
        for suite in EXPECTED_SUITES:
            index = _object(indexes_raw[suite], f"{architecture} {suite} index")
            _exact_keys(
                index,
                {
                    "inrelease_path",
                    "inrelease_size",
                    "inrelease_sha256",
                    "packages_path",
                    "packages_size",
                    "packages_sha256",
                },
                f"{architecture} {suite} index",
            )
            inrelease_path = _safe_relative(index["inrelease_path"], "InRelease path")
            packages_path = _safe_relative(index["packages_path"], "Packages path")
            if inrelease_path != f"dists/{suite}/InRelease" or packages_path != (
                f"dists/{suite}/main/binary-{architecture}/Packages.xz"
            ):
                raise HostReleaseError("repository index path is invalid")
            inrelease_size = _bounded_size(
                index["inrelease_size"],
                MAX_METADATA_BYTES,
                "repository index size",
            )
            packages_size = _bounded_size(
                index["packages_size"],
                MAX_METADATA_BYTES,
                "repository index size",
            )
            inrelease_sha256 = _string(index["inrelease_sha256"], "InRelease digest")
            packages_sha256 = _string(index["packages_sha256"], "Packages digest")
            if any(
                SHA256_RE.fullmatch(digest) is None
                for digest in (inrelease_sha256, packages_sha256)
            ):
                raise HostReleaseError("repository index digest is invalid")
            indexes[suite] = RepositoryIndex(
                suite=suite,
                inrelease_path=inrelease_path,
                inrelease_size=inrelease_size,
                inrelease_sha256=inrelease_sha256,
                packages_path=packages_path,
                packages_size=packages_size,
                packages_sha256=packages_sha256,
            )
        repositories[architecture] = RepositoryMetadata(base_url, indexes)

        architecture_packages = _object(packages_raw[architecture], f"{architecture} packages")
        if set(architecture_packages) != set(PACKAGE_ORDER):
            raise HostReleaseError("package set is invalid")
        parsed_packages: dict[str, PackageArtifact] = {}
        for package in PACKAGE_ORDER:
            item = _object(architecture_packages[package], f"{architecture} {package}")
            _exact_keys(
                item,
                {
                    "package",
                    "source_suite",
                    "version",
                    "architecture",
                    "filename",
                    "size",
                    "sha256",
                },
                f"{architecture} {package}",
            )
            if item["package"] != package or item["architecture"] != architecture:
                raise HostReleaseError("package identity is invalid")
            size = item["size"]
            if not isinstance(size, int) or isinstance(size, bool) or not 0 < size <= MAX_ARTIFACT_BYTES:
                raise HostReleaseError("package size is invalid")
            digest = _string(item["sha256"], "package digest")
            if SHA256_RE.fullmatch(digest) is None:
                raise HostReleaseError("package digest is invalid")
            parsed_packages[package] = PackageArtifact(
                package,
                _string(item["source_suite"], "package source suite"),
                _string(item["version"], "package version"),
                architecture,
                _safe_relative(item["filename"], "package filename"),
                size,
                digest,
            )
            if parsed_packages[package].source_suite != EXPECTED_PACKAGE_SUITES[package]:
                raise HostReleaseError("package source suite is invalid")
        packages[architecture] = parsed_packages

    return HostRelease(
        source_path=path.resolve(),
        release=release_name,
        runtime_manifest=runtime_manifest,
        signer_fingerprint=signer,
        keyring_name=keyring_name,
        keyring_sha256=keyring_sha256,
        snapshot=snapshot,
        architecture_map=architecture_map,
        repositories=repositories,
        packages=packages,
    )


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _expected_bundle_paths(
    release: HostRelease,
    debian_architecture: str,
    runtime: Mapping[str, object],
    native_architecture: str,
) -> set[str]:
    architecture = _object(
        _object(runtime.get("architectures"), "runtime architectures").get(native_architecture),
        "runtime architecture",
    )
    artifacts = architecture.get("artifacts")
    if not isinstance(artifacts, list):
        raise HostReleaseError("runtime artifacts are invalid")
    runtime_paths: set[str] = set()
    for item_raw in artifacts:
        item = _object(item_raw, "runtime artifact")
        runtime_paths.add(f"runtime/{_safe_relative(item.get('name'), 'runtime artifact name')}")
    package_paths = {
        f"packages/{PurePosixPath(item.filename).name}"
        for item in release.packages[debian_architecture].values()
    }
    apt_paths = {
        f"apt/{suite}.{kind}"
        for suite in release.repositories[debian_architecture].indexes
        for kind in ("InRelease", "Packages.xz")
    }
    return {
        release.keyring_name,
        *apt_paths,
        *package_paths,
        *runtime_paths,
    }


def _verify_layout(bundle: Path, expected: set[str]) -> None:
    try:
        root = bundle.lstat()
    except OSError as exc:
        raise HostReleaseError("bundle layout is unavailable") from exc
    if not stat.S_ISDIR(root.st_mode) or root.st_mode & 0o022:
        raise HostReleaseError("bundle layout root is unsafe")
    observed_files: set[str] = set()
    observed_directories = {".", "apt", "packages", "runtime"}
    for current, directory_names, file_names in os.walk(bundle, followlinks=False):
        current_path = Path(current)
        relative_dir = current_path.relative_to(bundle).as_posix()
        if relative_dir not in observed_directories:
            raise HostReleaseError("bundle layout contains an unexpected directory")
        for name in directory_names:
            child = current_path / name
            metadata = child.lstat()
            relative = child.relative_to(bundle).as_posix()
            if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
                raise HostReleaseError("bundle layout contains an unsafe directory")
            if relative not in observed_directories:
                raise HostReleaseError("bundle layout contains an unexpected directory")
        for name in file_names:
            relative = (current_path / name).relative_to(bundle).as_posix()
            observed_files.add(relative)
    if observed_files != expected:
        raise HostReleaseError("bundle layout does not match the release")


def _snapshot_bundle(
    bundle: Path,
    expected: set[str],
    *,
    required_owner: int,
) -> Path:
    by_directory: dict[str, set[str]] = {".": set(), "apt": set(), "packages": set(), "runtime": set()}
    for relative in expected:
        path = PurePosixPath(relative)
        if (
            path.is_absolute()
            or ".." in path.parts
            or len(path.parts) not in {1, 2}
            or (len(path.parts) == 2 and path.parts[0] not in {"apt", "packages", "runtime"})
        ):
            raise HostReleaseError("bundle snapshot path is unsafe")
        directory = "." if len(path.parts) == 1 else path.parts[0]
        by_directory[directory].add(path.name)
    by_directory["."].update({"apt", "packages", "runtime"})

    source_root = -1
    source_directories: dict[str, int] = {}
    source_files: dict[str, tuple[int, os.stat_result, int]] = {}
    snapshot_root: Path | None = None
    try:
        source_root = os.open(
            bundle,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        root_metadata = os.fstat(source_root)
        if not stat.S_ISDIR(root_metadata.st_mode) or root_metadata.st_mode & 0o022:
            raise HostReleaseError("bundle layout root is unsafe")
        if set(os.listdir(source_root)) != by_directory["."]:
            raise HostReleaseError("bundle layout does not match the release")

        for directory_name in ("apt", "packages", "runtime"):
            descriptor = os.open(
                directory_name,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_NOFOLLOW
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=source_root,
            )
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
                os.close(descriptor)
                raise HostReleaseError("bundle layout contains an unsafe directory")
            if set(os.listdir(descriptor)) != by_directory[directory_name]:
                os.close(descriptor)
                raise HostReleaseError("bundle layout does not match the release")
            source_directories[directory_name] = descriptor

        for relative in sorted(expected):
            path = PurePosixPath(relative)
            directory_name = "." if len(path.parts) == 1 else path.parts[0]
            directory_descriptor = source_root if directory_name == "." else source_directories[directory_name]
            limit = (
                MAX_METADATA_BYTES
                if directory_name in {".", "apt"}
                else MAX_ARTIFACT_BYTES
            )
            descriptor = os.open(
                path.name,
                os.O_RDONLY
                | os.O_NOFOLLOW
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=directory_descriptor,
            )
            metadata = os.fstat(descriptor)
            if metadata.st_mode & 0o022:
                os.close(descriptor)
                raise HostReleaseError("bundle input is group- or world-writable")
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
                os.close(descriptor)
                raise HostReleaseError("bundle input metadata is unsafe")
            source_files[relative] = (descriptor, metadata, limit)

        snapshot_root = Path(tempfile.mkdtemp(prefix="loom-host-bundle-snapshot-"))
        snapshot_root.chmod(0o700)
        snapshot_metadata = snapshot_root.lstat()
        if (
            not stat.S_ISDIR(snapshot_metadata.st_mode)
            or snapshot_metadata.st_uid != required_owner
            or stat.S_IMODE(snapshot_metadata.st_mode) != 0o700
        ):
            raise HostReleaseError("bundle snapshot is not owner-private")
        for directory_name in ("apt", "packages", "runtime"):
            (snapshot_root / directory_name).mkdir(mode=0o700)

        for relative, (source, initial, limit) in source_files.items():
            destination_path = snapshot_root / relative
            destination = os.open(
                destination_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o400,
            )
            try:
                remaining = limit + 1
                copied = 0
                while remaining:
                    chunk = os.read(source, min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    copied += len(chunk)
                    remaining -= len(chunk)
                    view = memoryview(chunk)
                    while view:
                        written = os.write(destination, view)
                        if written <= 0:
                            raise HostReleaseError("bundle snapshot write failed")
                        view = view[written:]
                final = os.fstat(source)
                if copied > limit or copied != initial.st_size or (
                    initial.st_dev,
                    initial.st_ino,
                    initial.st_size,
                    initial.st_mtime_ns,
                    initial.st_ctime_ns,
                ) != (
                    final.st_dev,
                    final.st_ino,
                    final.st_size,
                    final.st_mtime_ns,
                    final.st_ctime_ns,
                ):
                    raise HostReleaseError("bundle input changed while being snapshotted")
                os.fsync(destination)
            finally:
                os.close(destination)
        return snapshot_root
    except HostReleaseError:
        if snapshot_root is not None:
            shutil.rmtree(snapshot_root, ignore_errors=True)
        raise
    except OSError as exc:
        if snapshot_root is not None:
            shutil.rmtree(snapshot_root, ignore_errors=True)
        raise HostReleaseError("bundle cannot be snapshotted safely") from exc
    finally:
        for descriptor, _, _ in source_files.values():
            os.close(descriptor)
        for descriptor in source_directories.values():
            os.close(descriptor)
        if source_root >= 0:
            os.close(source_root)


def _verify_signature(
    keyring: Path,
    inrelease: Path,
    fingerprint: str,
    runner: CommandRunner,
) -> None:
    result = runner.run(
        ("/usr/bin/gpgv", "--status-fd=1", "--keyring", str(keyring), str(inrelease))
    )
    if result.returncode != 0:
        raise HostReleaseError("Ubuntu repository signature is invalid")
    forbidden = ("EXPKEYSIG", "KEYEXPIRED", "SIGEXPIRED", "REVKEYSIG", "BADSIG")
    if any(marker in result.stdout for marker in forbidden):
        raise HostReleaseError("Ubuntu repository signature is not current")
    fingerprints = [
        fields[2]
        for line in result.stdout.splitlines()
        if (fields := line.split())[:2] == ["[GNUPG:]", "VALIDSIG"] and len(fields) >= 3
    ]
    if fingerprints != [fingerprint]:
        raise HostReleaseError("Ubuntu repository signer is invalid")


def _release_index_digest(
    inrelease: bytes,
    relative_packages_path: str,
    expected_suite: str,
) -> tuple[str, int]:
    try:
        text = inrelease.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HostReleaseError("InRelease is not UTF-8") from exc
    suites = [line.removeprefix("Suite: ") for line in text.splitlines() if line.startswith("Suite: ")]
    if suites != [expected_suite]:
        raise HostReleaseError("InRelease suite is invalid")
    rows: list[tuple[str, int]] = []
    in_sha256 = False
    for line in text.splitlines():
        if line == "SHA256:":
            if in_sha256:
                raise HostReleaseError("InRelease contains duplicate SHA256 sections")
            in_sha256 = True
            continue
        if not in_sha256:
            continue
        if not line.startswith(" "):
            break
        fields = line.split()
        if len(fields) != 3:
            raise HostReleaseError("InRelease SHA256 row is invalid")
        if fields[2] != relative_packages_path:
            continue
        if SHA256_RE.fullmatch(fields[0]) is None or not fields[1].isdigit():
            raise HostReleaseError("InRelease Packages digest is invalid")
        rows.append((fields[0], int(fields[1])))
    if len(rows) != 1:
        raise HostReleaseError("InRelease does not authenticate exactly one Packages index")
    return rows[0]


def _decompress_packages_index(payload: bytes) -> bytes:
    if not payload:
        raise HostReleaseError("Packages index is empty")
    xz_magic = b"\xfd7zXZ\x00"
    remaining = payload
    expanded = bytearray()
    saw_stream = False
    while remaining:
        padding = len(remaining) - len(remaining.lstrip(b"\0"))
        if padding:
            if not saw_stream:
                raise HostReleaseError("Packages index contains trailing bytes")
            if padding % 4:
                raise HostReleaseError("Packages index XZ padding is not aligned")
            remaining = remaining[padding:]
            if not remaining:
                break
        if not remaining.startswith(xz_magic):
            raise HostReleaseError("Packages index contains trailing bytes")
        saw_stream = True
        decompressor = lzma.LZMADecompressor(format=lzma.FORMAT_XZ)
        try:
            stream = decompressor.decompress(
                remaining,
                max_length=MAX_METADATA_BYTES - len(expanded) + 1,
            )
        except lzma.LZMAError as exc:
            raise HostReleaseError("Packages index is invalid") from exc
        expanded.extend(stream)
        if len(expanded) > MAX_METADATA_BYTES:
            raise HostReleaseError("Packages index expands beyond its limit")
        if not decompressor.eof:
            raise HostReleaseError("Packages index is incomplete")
        if len(decompressor.unused_data) >= len(remaining):
            raise HostReleaseError("Packages index stream made no progress")
        remaining = decompressor.unused_data
    if not saw_stream:
        raise HostReleaseError("Packages index is empty")
    return bytes(expanded)


def _package_stanzas(payload: bytes) -> dict[str, dict[str, str]]:
    expanded = _decompress_packages_index(payload)
    try:
        text = expanded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HostReleaseError("Packages index is invalid") from exc
    parsed: dict[str, dict[str, str]] = {}
    for paragraph in text.strip().split("\n\n"):
        fields: dict[str, str] = {}
        current_field: str | None = None
        for line in paragraph.splitlines():
            if line.startswith((" ", "\t")):
                if current_field is None:
                    raise HostReleaseError("Packages stanza is invalid")
                fields[current_field] += "\n" + line[1:]
                continue
            if not line or ": " not in line:
                raise HostReleaseError("Packages stanza is invalid")
            key, value = line.split(": ", 1)
            if key in fields:
                raise HostReleaseError("Packages stanza contains a duplicate field")
            fields[key] = value
            current_field = key
        package = fields.get("Package")
        if package in PACKAGE_ORDER:
            if package in parsed:
                raise HostReleaseError("Packages index contains a duplicate package")
            parsed[package] = fields
    return parsed


def _verify_package_contents(package: str, output: str) -> None:
    setuid_paths: set[str] = set()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 6 or len(fields[0]) != 10:
            raise HostReleaseError("package contents output is invalid")
        mode = fields[0]
        path = fields[-1]
        if mode[3] in "sS" or mode[6] in "sS":
            setuid_paths.add(path)
    expected = EXPECTED_SETUID_PATHS if package == "uidmap" else set()
    if setuid_paths != expected:
        raise HostReleaseError("package setuid payload is invalid")


def _runtime_binary_payloads(
    runtime_paths: Mapping[str, Path],
    binary_digests: Mapping[str, object],
) -> dict[str, bytes]:
    if set(binary_digests) != EXPECTED_RUNTIME_BINARIES:
        raise HostReleaseError("runtime binary allowlist is invalid")
    direct = {
        "slirp4netns": runtime_paths.get("slirp4netns"),
        "fuse-overlayfs": runtime_paths.get("fuse-overlayfs"),
    }
    payloads: dict[str, bytes] = {}
    archives = [path for name, path in runtime_paths.items() if name.endswith(".tar.gz")]
    for binary, digest_raw in binary_digests.items():
        if not isinstance(binary, str) or PurePosixPath(binary).name != binary:
            raise HostReleaseError("runtime binary name is invalid")
        digest = _string(digest_raw, "runtime binary digest")
        if SHA256_RE.fullmatch(digest) is None:
            raise HostReleaseError("runtime binary digest is invalid")
        direct_path = direct.get(binary)
        if direct_path is not None:
            payload = _read_regular(direct_path, MAX_BINARY_BYTES, "runtime binary")
        else:
            matches: list[bytes] = []
            for archive_path in archives:
                try:
                    with tarfile.open(archive_path, mode="r:gz") as archive:
                        for member in archive.getmembers():
                            member_path = PurePosixPath(member.name)
                            if member_path.is_absolute() or ".." in member_path.parts:
                                raise HostReleaseError("runtime archive contains an unsafe path")
                            if member_path.name != binary:
                                continue
                            if not member.isfile() or member.size > MAX_BINARY_BYTES:
                                raise HostReleaseError("runtime archive binary is unsafe")
                            extracted = archive.extractfile(member)
                            if extracted is None:
                                raise HostReleaseError("runtime archive binary is unavailable")
                            matches.append(extracted.read(MAX_BINARY_BYTES + 1))
                except (OSError, tarfile.TarError) as exc:
                    raise HostReleaseError("runtime archive is invalid") from exc
            if len(matches) != 1:
                raise HostReleaseError("runtime binary is missing or ambiguous")
            payload = matches[0]
        if _sha256(payload) != digest:
            raise HostReleaseError("runtime binary digest is invalid")
        payloads[binary] = payload
    return payloads


def _verify_static_binaries(payloads: Mapping[str, bytes], runner: CommandRunner) -> None:
    with tempfile.TemporaryDirectory(prefix="loom-host-release-") as temporary:
        root = Path(temporary)
        for binary, payload in sorted(payloads.items()):
            path = root / binary
            path.write_bytes(payload)
            path.chmod(0o500)
            result = runner.run(("/usr/bin/readelf", "-d", str(path)))
            if result.returncode != 0 or "(NEEDED)" in result.stdout:
                raise HostReleaseError("runtime binary is not statically linked")


def verify_host_bundle(
    bundle: Path,
    release: HostRelease,
    architecture: str,
    runner: CommandRunner,
    *,
    runtime_manifest_path: Path | None = None,
    required_snapshot_owner: int | None = None,
) -> VerifiedHostBundle:
    debian_architecture = release.architecture_map.get(architecture)
    if debian_architecture is None:
        raise HostReleaseError("native architecture is not in the host release")
    runtime_path = runtime_manifest_path or release.source_path.parent / release.runtime_manifest
    runtime = _load_json(runtime_path, "runtime manifest")
    if runtime.get("schema") != "loom.task-image-builder-rootless-runtime/v1":
        raise HostReleaseError("runtime manifest schema is invalid")
    if runtime.get("release") != EXPECTED_RUNTIME_RELEASE:
        raise HostReleaseError("runtime manifest release is invalid")
    expected = _expected_bundle_paths(release, debian_architecture, runtime, architecture)
    snapshot_root = _snapshot_bundle(
        bundle,
        expected,
        required_owner=(
            os.geteuid() if required_snapshot_owner is None else required_snapshot_owner
        ),
    )

    try:
        _verify_layout(snapshot_root, expected)
        keyring_path = snapshot_root / release.keyring_name
        keyring = _read_regular(keyring_path, MAX_METADATA_BYTES, "Ubuntu archive keyring")
        if _sha256(keyring) != release.keyring_sha256:
            raise HostReleaseError("Ubuntu archive keyring digest is invalid")
        repository = release.repositories[debian_architecture]
        package_indexes: dict[str, dict[str, dict[str, str]]] = {}
        for suite, index in repository.indexes.items():
            inrelease_path = snapshot_root / "apt" / f"{suite}.InRelease"
            inrelease = _read_regular(inrelease_path, MAX_METADATA_BYTES, "InRelease")
            packages_path = snapshot_root / "apt" / f"{suite}.Packages.xz"
            packages_payload = _read_regular(
                packages_path,
                MAX_METADATA_BYTES,
                "Packages index",
            )
            if (
                len(inrelease) != index.inrelease_size
                or _sha256(inrelease) != index.inrelease_sha256
                or len(packages_payload) != index.packages_size
                or _sha256(packages_payload) != index.packages_sha256
            ):
                raise HostReleaseError("repository pinned metadata is invalid")
            _verify_signature(
                keyring_path,
                inrelease_path,
                release.signer_fingerprint,
                runner,
            )
            prefix = f"dists/{suite}/"
            if not index.packages_path.startswith(prefix):
                raise HostReleaseError("Packages path is outside the signed suite")
            signed_packages_path = index.packages_path.removeprefix(prefix)
            expected_digest, expected_size = _release_index_digest(
                inrelease,
                signed_packages_path,
                suite,
            )
            if (
                len(packages_payload) != expected_size
                or _sha256(packages_payload) != expected_digest
            ):
                raise HostReleaseError("Packages index does not match signed metadata")
            package_indexes[suite] = _package_stanzas(packages_payload)

        package_paths: list[Path] = []
        for package in PACKAGE_ORDER:
            artifact = release.packages[debian_architecture][package]
            fields = package_indexes[artifact.source_suite].get(package)
            if fields is None:
                raise HostReleaseError("package signed metadata is absent")
            expected_fields = {
                "Package": artifact.package,
                "Version": artifact.version,
                "Architecture": artifact.architecture,
                "Filename": artifact.filename,
                "Size": str(artifact.size),
                "SHA256": artifact.sha256,
            }
            if any(fields.get(key) != value for key, value in expected_fields.items()):
                raise HostReleaseError("package signed metadata is invalid")
            package_path = snapshot_root / "packages" / PurePosixPath(artifact.filename).name
            package_payload = _read_regular(package_path, MAX_ARTIFACT_BYTES, "package artifact")
            if len(package_payload) != artifact.size or _sha256(package_payload) != artifact.sha256:
                raise HostReleaseError("package artifact digest or size is invalid")
            control_fields = {
                "Package": artifact.package,
                "Version": artifact.version,
                "Architecture": artifact.architecture,
            }
            for field, expected_value in control_fields.items():
                metadata = runner.run(
                    ("/usr/bin/dpkg-deb", "--field", str(package_path), field)
                )
                if metadata.returncode != 0 or metadata.stdout.splitlines() != [expected_value]:
                    raise HostReleaseError("package control metadata is invalid")
            contents = runner.run(("/usr/bin/dpkg-deb", "--contents", str(package_path)))
            if contents.returncode != 0:
                raise HostReleaseError("package contents cannot be inspected")
            _verify_package_contents(package, contents.stdout)
            package_paths.append(package_path)

        runtime_architectures = _object(runtime.get("architectures"), "runtime architectures")
        runtime_architecture = _object(runtime_architectures.get(architecture), "runtime architecture")
        artifacts_raw = runtime_architecture.get("artifacts")
        binaries_raw = _object(runtime_architecture.get("binaries"), "runtime binaries")
        if not isinstance(artifacts_raw, list) or len(artifacts_raw) != 4:
            raise HostReleaseError("runtime artifact set is invalid")
        runtime_paths: list[Path] = []
        runtime_by_installed_name: dict[str, Path] = {}
        for item_raw in artifacts_raw:
            item = _object(item_raw, "runtime artifact")
            name = _safe_relative(item.get("name"), "runtime artifact name")
            if PurePosixPath(name).name != name:
                raise HostReleaseError("runtime artifact name must not contain a directory")
            runtime_digest = _string(item.get("sha256"), "runtime artifact digest")
            if SHA256_RE.fullmatch(runtime_digest) is None:
                raise HostReleaseError("runtime artifact digest is invalid")
            artifact_path = snapshot_root / "runtime" / name
            payload = _read_regular(artifact_path, MAX_ARTIFACT_BYTES, "runtime artifact")
            if _sha256(payload) != runtime_digest:
                raise HostReleaseError("runtime artifact digest is invalid")
            runtime_paths.append(artifact_path)
            if name.startswith("slirp4netns"):
                runtime_by_installed_name["slirp4netns"] = artifact_path
            elif name.startswith("fuse-overlayfs"):
                runtime_by_installed_name["fuse-overlayfs"] = artifact_path
            else:
                runtime_by_installed_name[name] = artifact_path
        binary_payloads = _runtime_binary_payloads(runtime_by_installed_name, binaries_raw)
        _verify_static_binaries(binary_payloads, runner)

        bundle_hasher = hashlib.sha256()
        for relative in sorted(expected):
            payload = _read_regular(snapshot_root / relative, MAX_ARTIFACT_BYTES, "bundle input")
            bundle_hasher.update(
                relative.encode("utf-8") + b"\0" + _sha256(payload).encode("ascii") + b"\0"
            )
        return VerifiedHostBundle(
            architecture=architecture,
            bundle_digest=bundle_hasher.hexdigest(),
            snapshot_root=snapshot_root,
            package_paths=tuple(package_paths),
            runtime_paths=tuple(runtime_paths),
        )
    except BaseException:
        shutil.rmtree(snapshot_root, ignore_errors=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--release", type=Path, required=True)
    verify.add_argument("--runtime-manifest", type=Path, required=True)
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument("--architecture", choices=tuple(ARCHITECTURE_MAP), required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    verified: VerifiedHostBundle | None = None
    try:
        release = load_host_release(args.release)
        verified = verify_host_bundle(
            args.bundle,
            release,
            args.architecture,
            SubprocessCommandRunner(),
            runtime_manifest_path=args.runtime_manifest,
        )
    except HostReleaseError as exc:
        print(json.dumps({"error": str(exc), "verified": False}, sort_keys=True))
        return 1
    try:
        print(
            json.dumps(
                {
                    "architecture": verified.architecture,
                    "bundle_digest": verified.bundle_digest,
                    "verified": True,
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        verified.close()


if __name__ == "__main__":
    raise SystemExit(main())
