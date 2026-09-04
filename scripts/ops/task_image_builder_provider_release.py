#!/usr/bin/env python3
"""Assemble a deterministic, inert Phase 2C provider release."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
import shutil
import stat
import struct
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal, cast

if __package__ in {None, ""}:
    from task_image_builder_guard_release import (  # type: ignore[import-not-found]
        GuardReleaseError as _GuardReleaseError,
    )
    from task_image_builder_guard_release import (
        verify_release_directory as _verify_guard_release_directory,
    )
else:
    from scripts.ops.task_image_builder_guard_release import (
        GuardReleaseError as _GuardReleaseError,
    )
    from scripts.ops.task_image_builder_guard_release import (
        verify_release_directory as _verify_guard_release_directory,
    )

Architecture = Literal["x86_64", "aarch64"]

_SPEC = Path("deploy/task-image-builder/provider-release-v1.json")
_SPEC_SCHEMA = "loom.task-image-builder-provider-release-spec/v1"
_BUNDLE_SCHEMA = "loom.task-image-builder-provider-bundle/v1"
_MANIFEST = "release-manifest.json"
_SET_MANIFEST = "provider-release-set-manifest.json"
_SET_SCHEMA = "loom.task-image-builder-provider-release-set/v1"
_MAX_BYTES = 16 * 1024 * 1024
_MAX_BPFTOOL_BYTES = 64 * 1024 * 1024
_MAX_RUNTIME_MEMBER_BYTES = 1024 * 1024 * 1024
_MAX_JSON_BYTES = 4 * 1024 * 1024
_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")
_MACHINES: dict[Architecture, int] = {"x86_64": 62, "aarch64": 183}
_ARCHITECTURES: tuple[Architecture, ...] = ("x86_64", "aarch64")
_RUNTIME_ARCH: dict[Architecture, str] = {"x86_64": "amd64", "aarch64": "arm64"}
_RUNTIME_MEMBERS = (
    "buildctl",
    "buildkitd",
    "buildkit-runc",
    "rootlesskit",
    "rootlessctl",
    "slirp4netns",
    "fuse-overlayfs",
)
_GUARD_INTERPRETER = "/usr/bin/python3 -I -B"
_GUARD_MEMBER_LAYOUT = (
    ("bpftool", 0o555),
    "guard-network-map-schema-v1.json",
    "guard-network-v1.bpf.build.json",
    "guard-network-v1.bpf.o",
    "loom-task-image-builder-node-guard.service",
    ("loom-task-image-builder-guard.pyz", 0o555),
)
_GUARD_MEMBERS = tuple(
    item[0] if isinstance(item, tuple) else item
    for item in _GUARD_MEMBER_LAYOUT
)
_GUARD_MEMBER_MODES = {
    item[0] if isinstance(item, tuple) else item: item[1] if isinstance(item, tuple) else 0o444
    for item in _GUARD_MEMBER_LAYOUT
}
_RELEASE_MANIFEST_KEYS = {
    "architecture",
    "authority_contract_version",
    "files",
    "guard_release_sha256",
    "provider_install_root",
    "release_sha256",
    "release_spec_sha256",
    "runtime_release",
    "runtime_x_crypto",
    "schema",
    "supervisor_relative_path",
}
_RELEASE_MEMBER_KEYS = {"mode", "path", "sha256"}
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100


class ProviderReleaseError(ValueError):
    """The requested provider release input or publication is unsafe."""


@dataclass(frozen=True, slots=True)
class ProviderRelease:
    release_sha256: str
    directory: Path
    manifest_path: Path
    sidecar_path: Path
    manifest: dict[str, object]


@dataclass(frozen=True, slots=True)
class VerifiedProviderRelease:
    release_sha256: str
    architecture: Architecture
    directory: Path
    manifest_payload: bytes
    manifest: dict[str, object]
    members: tuple[VerifiedProviderMember, ...]


_FileIdentity = tuple[int, int, int, int, int, int, int, int, int]


@dataclass(frozen=True, slots=True)
class VerifiedProviderMember:
    path: str
    mode: int
    sha256: str
    size: int
    source_path: Path
    _source_identity: _FileIdentity

    def copy_to(self, destination: Path) -> None:
        """Copy this exact verified inode without buffering it in memory."""

        _copy_verified_member(self, destination)

    def read_bytes(self, *, maximum: int) -> bytes:
        """Read a small verified member while preserving its inode binding."""

        if self.size > maximum:
            raise ProviderReleaseError("release input is empty or too large")
        payload = _read_regular(self.source_path, maximum=maximum, executable=self.mode == 0o555)
        try:
            identity = _file_identity(self.source_path.lstat())
        except OSError as exc:
            raise ProviderReleaseError("release input is unavailable") from exc
        if (
            identity != self._source_identity
            or len(payload) != self.size
            or _digest(payload) != self.sha256
        ):
            raise ProviderReleaseError("release input changed while being read")
        return payload


@dataclass(frozen=True, slots=True)
class _ReleaseMemberInput:
    path: str
    mode: int
    sha256: str
    size: int
    payload: bytes | None = None
    source: VerifiedProviderMember | None = None


@dataclass(frozen=True, slots=True)
class _Input:
    path: PurePosixPath
    sha256: str
    destination: str | None = None
    mode: int | None = None


@dataclass(frozen=True, slots=True)
class _Spec:
    authority_contract_version: int
    provider_install_root: str
    supervisor_relative_path: str
    guard_release_path: PurePosixPath
    guard_release_sha256: str
    guard_bundle_sha256: dict[Architecture, str]
    host_release_path: PurePosixPath
    host_release_sha256: str
    runtime_manifest_path: PurePosixPath
    runtime_manifest_sha256: str
    supervisor_sources: tuple[_Input, ...]
    supervisor_sha256: dict[Architecture, str]
    configs: tuple[_Input, ...]
    scripts: tuple[_Input, ...]


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _file_identity(metadata: os.stat_result) -> _FileIdentity:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        metadata.st_uid,
        metadata.st_gid,
    )


def _inspect_regular(
    path: Path,
    *,
    maximum: int,
    executable: bool = False,
) -> tuple[str, bytes, _FileIdentity, int]:
    """Hash one stable regular file with fixed memory and retain its identity."""

    descriptor = -1
    try:
        initial = path.lstat()
        if (
            not stat.S_ISREG(initial.st_mode)
            or stat.S_ISLNK(initial.st_mode)
            or initial.st_nlink != 1
        ):
            raise ProviderReleaseError("release input must be a single-link regular file")
        if initial.st_mode & 0o022:
            raise ProviderReleaseError("release input is group/world writable")
        if executable and initial.st_mode & 0o111 == 0:
            raise ProviderReleaseError("release executable is not executable")
        if initial.st_size <= 0 or initial.st_size > maximum:
            raise ProviderReleaseError("release input is empty or too large")
        identity = _file_identity(initial)
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        if _file_identity(os.fstat(descriptor)) != identity:
            raise ProviderReleaseError("release input changed while opening")
        digest = hashlib.sha256()
        header = bytearray()
        total = 0
        while total <= maximum:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - total))
            if not chunk:
                break
            if len(header) < _ELF_HEADER.size:
                header.extend(chunk[: _ELF_HEADER.size - len(header)])
            digest.update(chunk)
            total += len(chunk)
        if total != initial.st_size or total > maximum:
            raise ProviderReleaseError("release input is empty or too large")
        if (
            _file_identity(os.fstat(descriptor)) != identity
            or _file_identity(path.lstat()) != identity
        ):
            raise ProviderReleaseError("release input changed while being read")
        return digest.hexdigest(), bytes(header), identity, total
    except ProviderReleaseError:
        raise
    except OSError as exc:
        raise ProviderReleaseError("release input is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _copy_verified_member(member: VerifiedProviderMember, destination: Path) -> None:
    source_descriptor = -1
    destination_descriptor = -1
    published = False
    try:
        if _file_identity(member.source_path.lstat()) != member._source_identity:
            raise ProviderReleaseError("release input changed before copying")
        source_descriptor = os.open(
            member.source_path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        if _file_identity(os.fstat(source_descriptor)) != member._source_identity:
            raise ProviderReleaseError("release input changed while opening")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination_descriptor = os.open(
            destination,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            member.mode,
        )
        digest = hashlib.sha256()
        total = 0
        while total <= member.size:
            chunk = os.read(
                source_descriptor,
                min(1024 * 1024, member.size + 1 - total),
            )
            if not chunk:
                break
            digest.update(chunk)
            position = 0
            while position < len(chunk):
                written = os.write(destination_descriptor, chunk[position:])
                if written <= 0:
                    raise ProviderReleaseError("release output could not be written")
                position += written
            total += len(chunk)
        if (
            total != member.size
            or digest.hexdigest() != member.sha256
            or _file_identity(os.fstat(source_descriptor)) != member._source_identity
            or _file_identity(member.source_path.lstat()) != member._source_identity
        ):
            raise ProviderReleaseError("release input changed while being copied")
        os.fsync(destination_descriptor)
        os.fchmod(destination_descriptor, member.mode)
        os.fsync(destination_descriptor)
        published = True
    except ProviderReleaseError:
        raise
    except OSError as exc:
        raise ProviderReleaseError("release output could not be written") from exc
    finally:
        if source_descriptor >= 0:
            os.close(source_descriptor)
        if destination_descriptor >= 0:
            os.close(destination_descriptor)
        if not published:
            try:
                destination.unlink()
            except FileNotFoundError:
                pass


def _buffered_member(path: str, mode: int, payload: bytes) -> _ReleaseMemberInput:
    return _ReleaseMemberInput(
        path=path,
        mode=mode,
        sha256=_digest(payload),
        size=len(payload),
        payload=payload,
    )


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
        and value != "0" * 64
    )


def _safe_relative(value: object, *, suffix: str | None = None) -> PurePosixPath:
    if not isinstance(value, str):
        raise ProviderReleaseError("release input path is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != value
        or (suffix is not None and path.suffix != suffix)
    ):
        raise ProviderReleaseError("release input path is invalid")
    return path


def _validate_root(path: Path, label: str) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise ProviderReleaseError(f"{label} must be an absolute path")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ProviderReleaseError(f"{label} is unavailable") from exc
    if path.is_symlink() or resolved != path or not stat.S_ISDIR(metadata.st_mode):
        raise ProviderReleaseError(f"{label} must be a non-symlink directory")
    return path


def _assert_safe_parents(root: Path, relative: PurePosixPath) -> Path:
    current = root
    for part in relative.parts[:-1]:
        current /= part
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise ProviderReleaseError("release input parent is unavailable") from exc
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise ProviderReleaseError("release input parent is unsafe")
    return root.joinpath(*relative.parts)


def _read_regular(path: Path, *, maximum: int, executable: bool = False) -> bytes:
    descriptor = -1
    try:
        initial = path.lstat()
        if (
            not stat.S_ISREG(initial.st_mode)
            or stat.S_ISLNK(initial.st_mode)
            or initial.st_nlink != 1
        ):
            raise ProviderReleaseError("release input must be a single-link regular file")
        if initial.st_mode & 0o022:
            raise ProviderReleaseError("release input is group/world writable")
        if executable and initial.st_mode & 0o111 == 0:
            raise ProviderReleaseError("release executable is not executable")
        if initial.st_size <= 0 or initial.st_size > maximum:
            raise ProviderReleaseError("release input is empty or too large")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        identity = (
            initial.st_dev,
            initial.st_ino,
            initial.st_mode,
            initial.st_nlink,
            initial.st_size,
            initial.st_mtime_ns,
            initial.st_ctime_ns,
        )
        if identity != (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_nlink,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        ):
            raise ProviderReleaseError("release input changed while opening")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        final_path = path.lstat()
        if len(payload) != initial.st_size or len(payload) > maximum:
            raise ProviderReleaseError("release input is empty or too large")
        final_identity = (
            final.st_dev,
            final.st_ino,
            final.st_mode,
            final.st_nlink,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        )
        final_path_identity = (
            final_path.st_dev,
            final_path.st_ino,
            final_path.st_mode,
            final_path.st_nlink,
            final_path.st_size,
            final_path.st_mtime_ns,
            final_path.st_ctime_ns,
        )
        if final_identity != identity or final_path_identity != identity:
            raise ProviderReleaseError("release input changed while being read")
        return payload
    except ProviderReleaseError:
        raise
    except OSError as exc:
        raise ProviderReleaseError("release input is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _record(value: object, *, artifact: bool) -> _Input:
    if not isinstance(value, dict):
        raise ProviderReleaseError("release specification record is invalid")
    expected = {"path", "sha256", "destination", "mode"} if artifact else {"path", "sha256"}
    if set(value) != expected:
        raise ProviderReleaseError("release specification record is invalid")
    path = _safe_relative(value["path"])
    digest = value["sha256"]
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or digest == "0" * 64
    ):
        raise ProviderReleaseError("release specification digest is invalid")
    if not artifact:
        return _Input(path, digest)
    destination = value["destination"]
    mode = value["mode"]
    destination_path = _safe_relative(destination)
    if destination_path.name in {"", ".", ".."} or destination_path.as_posix() in {
        _MANIFEST,
        "current",
    }:
        raise ProviderReleaseError("release artifact layout is invalid")
    if mode not in {"0444", "0555"}:
        raise ProviderReleaseError("release artifact layout is invalid")
    return _Input(path, digest, destination_path.as_posix(), int(cast(str, mode), 8))


def _checked_payload(root: Path, item: _Input, *, maximum: int, executable: bool = False) -> bytes:
    path = _assert_safe_parents(root, item.path)
    payload = _read_regular(path, maximum=maximum, executable=executable)
    if _digest(payload) != item.sha256:
        raise ProviderReleaseError("release input digest differs from specification")
    return payload


def _load_spec(root: Path) -> _Spec:
    spec_path = _assert_safe_parents(root, PurePosixPath(_SPEC.as_posix()))
    payload = _read_regular(spec_path, maximum=_MAX_JSON_BYTES)
    try:
        raw = json.loads(payload, object_pairs_hook=_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ProviderReleaseError("release specification is not valid JSON") from exc
    if (
        not isinstance(raw, dict)
        or raw.get("schema") != _SPEC_SCHEMA
        or raw.get("version") != 1
        or not isinstance(raw.get("authority_contract_version"), int)
        or raw.get("authority_contract_version") != 2
        or not isinstance(raw.get("provider_install_root"), str)
        or not isinstance(raw.get("supervisor_relative_path"), str)
        or not isinstance(raw.get("guard_release"), dict)
        or not isinstance(raw.get("host_release"), dict)
        or not isinstance(raw.get("runtime_manifest"), dict)
        or not isinstance(raw.get("supervisor"), dict)
        or not isinstance(raw.get("configs"), list)
        or not isinstance(raw.get("scripts"), list)
    ):
        raise ProviderReleaseError("release specification shape is invalid")
    provider_install_root = cast(str, raw["provider_install_root"])
    supervisor_relative_path = cast(str, raw["supervisor_relative_path"])
    if not provider_install_root.startswith("/") or ".." in provider_install_root.split("/"):
        raise ProviderReleaseError("release specification path binding is invalid")
    relative = _safe_relative(supervisor_relative_path)
    if relative.as_posix() != "bin/loom-task-builder-supervisor":
        raise ProviderReleaseError("release specification path binding is invalid")
    guard_release = cast(dict[str, object], raw["guard_release"])
    host_release = cast(dict[str, object], raw["host_release"])
    runtime_manifest = cast(dict[str, object], raw["runtime_manifest"])
    supervisor = cast(dict[str, object], raw["supervisor"])
    guard_release_path = _safe_relative(guard_release.get("path"), suffix=".json")
    host_release_path = _safe_relative(host_release.get("path"), suffix=".json")
    runtime_manifest_path = _safe_relative(runtime_manifest.get("path"), suffix=".json")
    for record in (guard_release, host_release, runtime_manifest):
        digest = record.get("sha256")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or digest == "0" * 64
        ):
            raise ProviderReleaseError("release specification digest is invalid")
    bundle_sha256 = guard_release.get("bundle_sha256")
    if not isinstance(bundle_sha256, dict):
        raise ProviderReleaseError("release specification shape is invalid")
    raw_supervisor_sources = supervisor.get("sources")
    if not isinstance(raw_supervisor_sources, list):
        raise ProviderReleaseError("release specification source coverage is invalid")
    supervisor_sources = tuple(
        _record(item, artifact=False) for item in raw_supervisor_sources
    )
    if not supervisor_sources or tuple(item.path for item in supervisor_sources) != tuple(
        sorted(item.path for item in supervisor_sources)
    ):
        raise ProviderReleaseError("release specification source coverage is invalid")
    expected_sources = tuple(
        PurePosixPath(path.relative_to(root).as_posix())
        for path in sorted((root / "cmd/loom-task-image-builder-supervisor").glob("*.go"))
    )
    if tuple(item.path for item in supervisor_sources) != expected_sources:
        raise ProviderReleaseError("release specification source coverage is invalid")
    supervisor_sha256: dict[Architecture, str] = {}
    raw_supervisor_sha256 = supervisor.get("sha256")
    if not isinstance(raw_supervisor_sha256, dict):
        raise ProviderReleaseError("release specification shape is invalid")
    for architecture in _ARCHITECTURES:
        digest = raw_supervisor_sha256.get(architecture)
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or digest == "0" * 64
        ):
            raise ProviderReleaseError("release specification digest is invalid")
        supervisor_sha256[architecture] = digest
    configs = tuple(_record(item, artifact=True) for item in raw["configs"])
    scripts = tuple(_record(item, artifact=True) for item in raw["scripts"])
    config_destinations = tuple(
        item.destination for item in configs if item.destination is not None
    )
    script_destinations = tuple(
        item.destination for item in scripts if item.destination is not None
    )
    if (
        len(config_destinations) != len(configs)
        or len(script_destinations) != len(scripts)
        or config_destinations != tuple(sorted(config_destinations))
        or script_destinations != tuple(sorted(script_destinations))
    ):
        raise ProviderReleaseError("release specification artifact coverage is invalid")
    if any(
        "/release-manifest.json" in destination
        for destination in (*config_destinations, *script_destinations)
    ):
        raise ProviderReleaseError("release specification artifact coverage is invalid")
    if len({item.destination for item in (*configs, *scripts)}) != len(configs) + len(scripts):
        raise ProviderReleaseError("release specification artifact coverage is invalid")
    guard_bundle_sha256: dict[Architecture, str] = {}
    for architecture in _ARCHITECTURES:
        digest = bundle_sha256.get(architecture)
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or digest == "0" * 64
        ):
            raise ProviderReleaseError("release specification digest is invalid")
        guard_bundle_sha256[architecture] = digest
    return _Spec(
        authority_contract_version=cast(int, raw["authority_contract_version"]),
        provider_install_root=provider_install_root,
        supervisor_relative_path=supervisor_relative_path,
        guard_release_path=guard_release_path,
        guard_release_sha256=cast(str, guard_release["sha256"]),
        guard_bundle_sha256=guard_bundle_sha256,
        host_release_path=host_release_path,
        host_release_sha256=cast(str, host_release["sha256"]),
        runtime_manifest_path=runtime_manifest_path,
        runtime_manifest_sha256=cast(str, runtime_manifest["sha256"]),
        supervisor_sources=supervisor_sources,
        supervisor_sha256=supervisor_sha256,
        configs=configs,
        scripts=scripts,
    )


def _validate_elf(payload: bytes, architecture: Architecture) -> None:
    if len(payload) < _ELF_HEADER.size:
        raise ProviderReleaseError("release executable architecture is invalid")
    header = _ELF_HEADER.unpack_from(payload)
    ident = header[0]
    if ident[:7] != b"\x7fELF\x02\x01\x01" or header[2] != _MACHINES[architecture]:
        raise ProviderReleaseError("release executable architecture is invalid")


def _read_json_path(path: Path, *, label: str) -> dict[str, object]:
    payload = _read_regular(path, maximum=_MAX_JSON_BYTES)
    try:
        value = json.loads(payload, object_pairs_hook=_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ProviderReleaseError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise ProviderReleaseError(f"{label} is invalid")
    return value


def _load_guard_release(
    path: Path,
    *,
    architecture: Architecture,
    expected_release_sha256: str,
    expected_release_spec_sha256: str,
) -> dict[str, tuple[int, bytes]]:
    try:
        release = _verify_guard_release_directory(
            path,
            expected_release_sha256=expected_release_sha256,
            expected_architecture=architecture,
        )
    except _GuardReleaseError as exc:
        raise ProviderReleaseError(str(exc)) from exc
    if release.manifest.get("release_spec_sha256") != expected_release_spec_sha256:
        raise ProviderReleaseError("guard release differs from reviewed specification")
    if tuple(name for name, _mode, _payload in release.members) != _GUARD_MEMBERS:
        raise ProviderReleaseError("guard release inventory is invalid")
    return {name: (mode, payload) for name, mode, payload in release.members}


def _runtime_member_destination(name: str) -> str:
    if name in {"buildctl", "buildkit-runc", "buildkitd"}:
        return f"runtime/{name}"
    return f"bin/{name}"


_RUNTIME_MEMBER_DESTINATIONS = frozenset(
    _runtime_member_destination(name)
    for name in _RUNTIME_MEMBERS
)


def _release_member_maximum(path: str) -> int:
    if path in _RUNTIME_MEMBER_DESTINATIONS:
        return _MAX_RUNTIME_MEMBER_BYTES
    if path == "bpftool":
        return _MAX_BPFTOOL_BYTES
    return _MAX_BYTES


def _runtime_manifest_binding(
    manifest_path: Path,
    *,
    architecture: Architecture,
) -> tuple[dict[str, str], str, str, str]:
    manifest = _read_json_path(manifest_path, label="runtime manifest")
    if (
        manifest.get("schema") != "loom.task-image-builder-rootless-runtime/v2"
        or manifest.get("release") != "rootless-runtime-v2"
        or not isinstance(manifest.get("toolchain"), dict)
    ):
        raise ProviderReleaseError("runtime manifest is invalid")
    toolchain = cast(dict[str, object], manifest["toolchain"])
    if toolchain.get("x_crypto") != "v0.55.0":
        raise ProviderReleaseError("runtime manifest x/crypto binding is invalid")
    image = toolchain.get("image")
    image_sha256 = toolchain.get("image_sha256")
    if (
        not isinstance(image, str)
        or not isinstance(image_sha256, str)
        or len(image_sha256) != 64
        or any(character not in "0123456789abcdef" for character in image_sha256)
    ):
        raise ProviderReleaseError("runtime manifest is invalid")
    arch_name = _RUNTIME_ARCH[architecture]
    architectures = manifest.get("architectures")
    if not isinstance(architectures, dict) or not isinstance(architectures.get(arch_name), dict):
        raise ProviderReleaseError("runtime manifest is invalid")
    entry = cast(dict[str, object], architectures[arch_name])
    if entry.get("platform") != f"linux/{arch_name}" or not isinstance(entry.get("members"), dict):
        raise ProviderReleaseError("runtime manifest is invalid")
    raw_members = cast(dict[str, object], entry["members"])
    if tuple(sorted(raw_members)) != tuple(sorted(_RUNTIME_MEMBERS)):
        raise ProviderReleaseError("runtime member inventory is invalid")
    expected: dict[str, str] = {}
    for name in _RUNTIME_MEMBERS:
        digest = raw_members.get(name)
        if not _is_sha256(digest):
            raise ProviderReleaseError("runtime member digest is invalid")
        expected[name] = cast(str, digest)
    return (
        expected,
        cast(str, manifest["release"]),
        cast(str, toolchain["x_crypto"]),
        f"{image}@sha256:{image_sha256}",
    )


def _validate_runtime_directory(path: Path) -> Path:
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ProviderReleaseError("runtime member directory is unavailable") from exc
    if (
        path.is_symlink()
        or resolved != path
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o222
    ):
        raise ProviderReleaseError("runtime member directory is unsafe")
    return path


def _load_runtime(
    manifest_path: Path,
    runtime_root: Path,
    *,
    architecture: Architecture,
) -> tuple[dict[str, VerifiedProviderMember], str, str, str]:
    expected, runtime_release, runtime_x_crypto, toolchain_image = _runtime_manifest_binding(
        manifest_path,
        architecture=architecture,
    )
    runtime_dir = _validate_root(runtime_root, "runtime root") / "runtime"
    runtime_dir = _validate_runtime_directory(runtime_dir)
    try:
        entries = sorted(runtime_dir.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise ProviderReleaseError("runtime member directory is unavailable") from exc
    if {item.name for item in entries} != set(_RUNTIME_MEMBERS):
        raise ProviderReleaseError("runtime member inventory is invalid")
    members: dict[str, VerifiedProviderMember] = {}
    for item in entries:
        digest, header, identity, size = _inspect_regular(
            item,
            maximum=_MAX_RUNTIME_MEMBER_BYTES,
            executable=True,
        )
        if digest != expected.get(item.name):
            raise ProviderReleaseError("runtime member digest differs from manifest")
        _validate_elf(header, architecture)
        members[item.name] = VerifiedProviderMember(
            path=_runtime_member_destination(item.name),
            mode=0o555,
            sha256=digest,
            size=size,
            source_path=item,
            _source_identity=identity,
        )
    return members, runtime_release, runtime_x_crypto, toolchain_image


def _write_payload(descriptor: int, payload: bytes, mode: int) -> None:
    try:
        position = 0
        while position < len(payload):
            written = os.write(descriptor, payload[position:])
            if written <= 0:
                raise ProviderReleaseError("release output could not be written")
            position += written
        os.fsync(descriptor)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    except OSError as exc:
        raise ProviderReleaseError("release output could not be written") from exc


def _write_file(path: Path, payload: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            mode,
        )
        _write_payload(descriptor, payload, mode)
    except ProviderReleaseError:
        raise
    except OSError as exc:
        raise ProviderReleaseError("release output could not be written") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _seal_release_tree(candidate: Path) -> None:
    directories = sorted(
        (path for path in candidate.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        directory.chmod(0o555)
    candidate.chmod(0o555)


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise ProviderReleaseError("atomic no-replace publication is unavailable")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            _AT_FDCWD,
            os.fsencode(source),
            _AT_FDCWD,
            os.fsencode(destination),
            _RENAME_NOREPLACE,
        )
        == 0
    ):
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ProviderReleaseError("release destination collision")
    raise ProviderReleaseError("atomic release publication failed") from OSError(
        error, os.strerror(error)
    )


def _publish_sidecar(path: Path, payload: bytes) -> None:
    descriptor = -1
    candidate: Path | None = None
    published = False
    try:
        descriptor, raw_candidate = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        candidate = Path(raw_candidate)
        _write_payload(descriptor, payload, 0o444)
        os.close(descriptor)
        descriptor = -1
        _rename_noreplace(candidate, path)
        published = True
    except ProviderReleaseError:
        raise
    except OSError as exc:
        raise ProviderReleaseError("release output could not be written") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if candidate is not None and not published:
            try:
                candidate.unlink()
            except FileNotFoundError:
                pass


def _copy_release_leaf(release: ProviderRelease, destination: Path, *, architecture: Architecture) -> None:
    del architecture
    try:
        shutil.copytree(release.directory, destination, symlinks=False)
    except OSError as exc:
        raise ProviderReleaseError("release set leaf copy failed") from exc


def _release_set_manifest(staged: Mapping[Architecture, ProviderRelease]) -> tuple[str, bytes, dict[str, object]]:
    identity: dict[str, object] = {
        "schema": _SET_SCHEMA,
        "architectures": {
            architecture: {
                "architecture": architecture,
                "path": release.release_sha256,
                "release_manifest_sha256": _digest(
                    _read_regular(release.manifest_path, maximum=_MAX_JSON_BYTES)
                ),
                "release_sha256": release.release_sha256,
            }
            for architecture, release in sorted(staged.items())
        },
    }
    release_set_sha256 = _digest(_canonical(identity))
    manifest = dict(identity)
    manifest["release_set_sha256"] = release_set_sha256
    manifest_payload = _canonical(manifest)
    return release_set_sha256, manifest_payload, manifest


def _build_supervisor_in_container(
    source_root: Path,
    architecture: Architecture,
    *,
    image: str = "golang:1.23.4-bookworm",
) -> bytes:
    goarch = {"x86_64": "amd64", "aarch64": "arm64"}[architecture]
    output_root = Path(tempfile.mkdtemp(prefix=".provider-supervisor."))
    try:
        output_path = output_root / "loom-task-builder-supervisor"
        command = [
            "docker",
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "-v",
            f"{source_root}:/src:ro",
            "-v",
            f"{output_root}:/out",
            "-w",
            "/src",
            image,
            "sh",
            "-lc",
            (
                "export PATH=/usr/local/go/bin:$PATH; "
                "CGO_ENABLED=0 GOOS=linux GOARCH="
                + goarch
                + " go build -trimpath -buildvcs=false "
                + "-ldflags \"-buildid= -s -w\" "
                + "-o /out/loom-task-builder-supervisor ./cmd/loom-task-image-builder-supervisor"
            ),
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            cwd=source_root,
            timeout=600,
        )
        if completed.returncode != 0:
            raise ProviderReleaseError(
                "supervisor build failed: "
                + completed.stderr.strip().splitlines()[-1]
                if completed.stderr.strip()
                else "supervisor build failed"
            )
        return _read_regular(output_path, maximum=_MAX_BYTES, executable=True)
    except OSError as exc:
        raise ProviderReleaseError("supervisor build is unavailable") from exc
    finally:
        shutil.rmtree(output_root, ignore_errors=True)


def _build_deterministic_supervisor(
    source_root: Path,
    architecture: Architecture,
    *,
    expected_sha256: str,
    builder: Callable[[Path, Architecture], bytes],
) -> bytes:
    first = builder(source_root, architecture)
    second = builder(source_root, architecture)
    for payload in (first, second):
        _validate_elf(payload, architecture)
    if first != second:
        raise ProviderReleaseError("supervisor build is not deterministic")
    if _digest(first) != expected_sha256:
        raise ProviderReleaseError("supervisor digest differs from specification")
    return first


def build_release(
    source_root: Path,
    output_root: Path,
    architecture: Architecture,
    *,
    guard_release_directory: Path | None = None,
    runtime_root: Path | None = None,
    build_supervisor: Callable[[Path, Architecture], bytes] | None = None,
) -> ProviderRelease:
    source_root = _validate_root(source_root, "source root")
    if output_root.exists():
        output_root = _validate_root(output_root, "output root")
    else:
        output_root.mkdir(parents=True, mode=0o755)
        output_root = _validate_root(output_root, "output root")
    if architecture not in {"x86_64", "aarch64"}:
        raise ProviderReleaseError("release architecture is invalid")
    spec_payload = _read_regular(
        _assert_safe_parents(source_root, PurePosixPath(_SPEC.as_posix())),
        maximum=_MAX_JSON_BYTES,
    )
    spec = _load_spec(source_root)
    _checked_payload(source_root, _Input(spec.guard_release_path, spec.guard_release_sha256), maximum=_MAX_JSON_BYTES)
    host_release_path = _assert_safe_parents(source_root, spec.host_release_path)
    runtime_manifest_path = _assert_safe_parents(source_root, spec.runtime_manifest_path)
    if _digest(_read_regular(host_release_path, maximum=_MAX_JSON_BYTES)) != spec.host_release_sha256:
        raise ProviderReleaseError("release input digest differs from specification")
    if _digest(_read_regular(runtime_manifest_path, maximum=_MAX_JSON_BYTES)) != spec.runtime_manifest_sha256:
        raise ProviderReleaseError("release input digest differs from specification")
    host_release = _read_json_path(host_release_path, label="host release")
    if host_release.get("runtime_manifest") != spec.runtime_manifest_path.name:
        raise ProviderReleaseError("host release does not bind the runtime manifest")
    for item in (*spec.configs, *spec.scripts, *spec.supervisor_sources):
        _checked_payload(
            source_root,
            item,
            maximum=_MAX_JSON_BYTES,
            executable=item.mode == 0o555,
        )
    guard_dir = _validate_root(
        guard_release_directory if guard_release_directory is not None else source_root,
        "guard release directory",
    )
    if guard_release_directory is None:
        raise ProviderReleaseError("guard release directory is required")
    guard_members = _load_guard_release(
        guard_dir,
        architecture=architecture,
        expected_release_sha256=spec.guard_bundle_sha256[architecture],
        expected_release_spec_sha256=spec.guard_release_sha256,
    )
    runtime_payloads, runtime_release, runtime_x_crypto, toolchain_image = _load_runtime(
        runtime_manifest_path,
        runtime_root if runtime_root is not None else source_root,
        architecture=architecture,
    )
    builder = build_supervisor or (
        lambda src, arch: _build_supervisor_in_container(src, arch, image=toolchain_image)
    )
    supervisor_payload = _build_deterministic_supervisor(
        source_root,
        architecture,
        expected_sha256=spec.supervisor_sha256[architecture],
        builder=builder,
    )
    members: list[_ReleaseMemberInput] = []
    for item in spec.configs:
        members.append(
            _buffered_member(
                cast(str, item.destination),
                cast(int, item.mode),
                _checked_payload(source_root, item, maximum=_MAX_JSON_BYTES),
            )
        )
    for item in spec.scripts:
        members.append(
            _buffered_member(
                cast(str, item.destination),
                cast(int, item.mode),
                _checked_payload(source_root, item, maximum=_MAX_JSON_BYTES, executable=True),
            )
        )
    for name in _GUARD_MEMBERS:
        mode, payload = guard_members[name]
        members.append(_buffered_member(name, mode, payload))
    members.extend(
        [
            _buffered_member(
                "bin/loom-task-builder-supervisor",
                0o555,
                supervisor_payload,
            ),
            *(
                _ReleaseMemberInput(
                    path=runtime_member.path,
                    mode=runtime_member.mode,
                    sha256=runtime_member.sha256,
                    size=runtime_member.size,
                    source=runtime_member,
                )
                for runtime_member in runtime_payloads.values()
            ),
        ]
    )
    members.sort(key=lambda item: item.path)
    if len({member.path for member in members}) != len(members):
        raise ProviderReleaseError("release member inventory is invalid")
    identity: dict[str, object] = {
        "schema": _BUNDLE_SCHEMA,
        "architecture": architecture,
        "authority_contract_version": spec.authority_contract_version,
        "guard_release_sha256": guard_dir.name,
        "provider_install_root": spec.provider_install_root,
        "release_spec_sha256": _digest(spec_payload),
        "runtime_release": runtime_release,
        "runtime_x_crypto": runtime_x_crypto,
        "supervisor_relative_path": spec.supervisor_relative_path,
        "files": [
            {
                "path": member.path,
                "mode": f"{member.mode:04o}",
                "sha256": member.sha256,
            }
            for member in members
        ],
    }
    release_sha256 = _digest(_canonical(identity))
    manifest = dict(identity)
    manifest["release_sha256"] = release_sha256
    manifest_payload = _canonical(manifest)
    directory = output_root / release_sha256
    if directory.exists() or directory.is_symlink():
        raise ProviderReleaseError("release destination collision")
    candidate = Path(tempfile.mkdtemp(prefix=".provider-release.", dir=output_root))
    try:
        for member in members:
            target = candidate / member.path
            if member.payload is not None and member.source is None:
                _write_file(target, member.payload, member.mode)
            elif member.payload is None and member.source is not None:
                member.source.copy_to(target)
            else:
                raise ProviderReleaseError("release member source is invalid")
        _write_file(candidate / _MANIFEST, manifest_payload, 0o444)
        _seal_release_tree(candidate)
        _rename_noreplace(candidate, directory)
    except BaseException:
        shutil.rmtree(candidate, ignore_errors=True)
        raise
    sidecar_path = output_root / f"{release_sha256}.manifest.json"
    _publish_sidecar(sidecar_path, manifest_payload)
    return ProviderRelease(
        release_sha256=release_sha256,
        directory=directory,
        manifest_path=directory / _MANIFEST,
        sidecar_path=sidecar_path,
        manifest=manifest,
    )


def build_certified_releases(
    source_root: Path,
    output_root: Path,
    *,
    guard_release_directories: Mapping[Architecture, Path],
    runtime_roots: Mapping[Architecture, Path],
    build_supervisor: Callable[[Path, Architecture], bytes] | None = None,
) -> dict[Architecture, ProviderRelease]:
    if set(guard_release_directories) != set(_ARCHITECTURES) or set(runtime_roots) != set(_ARCHITECTURES):
        raise ProviderReleaseError("whole-release certification requires both architectures")
    if output_root.exists():
        output_root = _validate_root(output_root, "output root")
    else:
        output_root.mkdir(parents=True, mode=0o755)
        output_root = _validate_root(output_root, "output root")
    staging = Path(tempfile.mkdtemp(prefix=".provider-release-set.", dir=output_root))
    staging.chmod(0o755)
    final_candidate: Path | None = None
    try:
        staged: dict[Architecture, ProviderRelease] = {}
        for architecture in _ARCHITECTURES:
            staged[architecture] = build_release(
                source_root,
                staging,
                architecture,
                guard_release_directory=guard_release_directories[architecture],
                runtime_root=runtime_roots[architecture],
                build_supervisor=build_supervisor,
            )
        release_set_sha256, release_set_payload, _release_set = _release_set_manifest(staged)
        release_set_directory = output_root / release_set_sha256
        if release_set_directory.exists() or release_set_directory.is_symlink():
            raise ProviderReleaseError("release destination collision")
        for release in staged.values():
            if (output_root / release.release_sha256).exists() or (
                output_root / f"{release.release_sha256}.manifest.json"
            ).exists():
                raise ProviderReleaseError("release destination collision")
        final_candidate = Path(
            tempfile.mkdtemp(prefix=".provider-release-set-final.", dir=output_root)
        )
        final_candidate.chmod(0o755)
        for architecture in _ARCHITECTURES:
            release = staged[architecture]
            _copy_release_leaf(
                release,
                final_candidate / release.release_sha256,
                architecture=architecture,
            )
        _write_file(final_candidate / _SET_MANIFEST, release_set_payload, 0o444)
        _seal_release_tree(final_candidate)
        try:
            _rename_noreplace(final_candidate, release_set_directory)
        except ProviderReleaseError as exc:
            raise ProviderReleaseError("release set publication failed") from exc
        return {
            architecture: ProviderRelease(
                release_sha256=release.release_sha256,
                directory=release_set_directory / release.release_sha256,
                manifest_path=release_set_directory / release.release_sha256 / _MANIFEST,
                sidecar_path=release_set_directory / release.release_sha256 / _MANIFEST,
                manifest=release.manifest,
            )
            for architecture, release in staged.items()
        }
    except ProviderReleaseError:
        raise
    except OSError as exc:
        raise ProviderReleaseError("release set publication failed") from exc
    except BaseException:
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _read_reviewed_spec(
    source_root: Path,
    *,
    architecture: Architecture,
) -> tuple[_Spec, bytes, dict[str, str], str, str]:
    source_root = _validate_root(source_root, "reviewed source root")
    spec_path = _assert_safe_parents(source_root, PurePosixPath(_SPEC.as_posix()))
    spec_payload = _read_regular(spec_path, maximum=_MAX_JSON_BYTES)
    spec = _load_spec(source_root)
    _checked_payload(
        source_root,
        _Input(spec.guard_release_path, spec.guard_release_sha256),
        maximum=_MAX_JSON_BYTES,
    )
    host_release_path = _assert_safe_parents(source_root, spec.host_release_path)
    runtime_manifest_path = _assert_safe_parents(source_root, spec.runtime_manifest_path)
    if _digest(_read_regular(host_release_path, maximum=_MAX_JSON_BYTES)) != spec.host_release_sha256:
        raise ProviderReleaseError("release input digest differs from specification")
    if _digest(_read_regular(runtime_manifest_path, maximum=_MAX_JSON_BYTES)) != spec.runtime_manifest_sha256:
        raise ProviderReleaseError("release input digest differs from specification")
    host_release = _read_json_path(host_release_path, label="host release")
    if host_release.get("runtime_manifest") != spec.runtime_manifest_path.name:
        raise ProviderReleaseError("host release does not bind the runtime manifest")
    for item in (*spec.configs, *spec.scripts, *spec.supervisor_sources):
        _checked_payload(
            source_root,
            item,
            maximum=_MAX_JSON_BYTES,
            executable=item.mode == 0o555,
        )
    runtime_members, runtime_release, runtime_x_crypto, _toolchain_image = _runtime_manifest_binding(
        runtime_manifest_path,
        architecture=architecture,
    )
    return spec, spec_payload, runtime_members, runtime_release, runtime_x_crypto


def _verified_member_records(
    release: VerifiedProviderRelease,
) -> tuple[tuple[str, int, str], ...]:
    return tuple(
        (member.path, member.mode, member.sha256)
        for member in release.members
    )


def _guard_member_records(
    member_records: tuple[tuple[str, int, str], ...],
) -> tuple[tuple[str, int, str], ...]:
    return tuple(
        record
        for record in member_records
        if record[0] in _GUARD_MEMBERS
    )


def _guard_member_payload_records(
    members: tuple[VerifiedProviderMember, ...],
) -> tuple[tuple[str, int, str, int], ...]:
    records = {
        member.path: (member.path, member.mode, member.sha256, member.size)
        for member in members
        if member.path in _GUARD_MEMBERS
    }
    return tuple(records[name] for name in _GUARD_MEMBERS if name in records)


def _guard_bundle_identity_sha256(
    guard_records: tuple[tuple[str, int, str, int], ...],
    *,
    architecture: Architecture,
    release_spec_sha256: str,
) -> str:
    expected_names = _GUARD_MEMBERS
    if tuple(name for name, _mode, _sha256, _size in guard_records) != expected_names:
        raise ProviderReleaseError("release guard member inventory is invalid")
    files: list[dict[str, object]] = []
    for name, mode, digest, size in guard_records:
        if mode != _GUARD_MEMBER_MODES[name] or not _is_sha256(digest) or size <= 0:
            raise ProviderReleaseError("release guard member identity is invalid")
        files.append(
            {
                "mode": f"{mode:04o}",
                "path": name,
                "sha256": digest,
                "size": size,
            }
        )
    return _digest(
        _canonical(
            {
                "architecture": architecture,
                "files": files,
                "interpreter": _GUARD_INTERPRETER,
                "release_spec_sha256": release_spec_sha256,
                "schema": "loom.task-image-builder-guard-bundle/v1",
            }
        )
    )


def _expected_spec_bound_records(
    spec: _Spec,
    architecture: Architecture,
    runtime_members: Mapping[str, str],
    guard_records: tuple[tuple[str, int, str], ...],
) -> tuple[tuple[str, int, str], ...]:
    records: list[tuple[str, int, str]] = []
    for item in (*spec.configs, *spec.scripts):
        if item.destination is None or item.mode is None:
            raise ProviderReleaseError("release specification artifact coverage is invalid")
        records.append((item.destination, item.mode, item.sha256))
    records.extend(guard_records)
    for name in _RUNTIME_MEMBERS:
        records.append((_runtime_member_destination(name), 0o555, runtime_members[name]))
    records.append(
        (
            "bin/loom-task-builder-supervisor",
            0o555,
            spec.supervisor_sha256[architecture],
        )
    )
    return tuple(sorted(records, key=lambda item: item[0]))


def verify_release_directory(
    path: Path,
    *,
    expected_release_sha256: str,
    expected_architecture: Architecture,
    expected_uid: int,
    expected_gid: int,
) -> VerifiedProviderRelease:
    path = _validate_root(path, "release directory")
    manifest_path = path / _MANIFEST
    manifest_payload = _read_regular(manifest_path, maximum=_MAX_JSON_BYTES)
    manifest = _read_json_path(manifest_path, label="release manifest")
    root_metadata = path.lstat()
    if (
        root_metadata.st_uid != expected_uid
        or root_metadata.st_gid != expected_gid
        or stat.S_IMODE(root_metadata.st_mode) & 0o222
    ):
        raise ProviderReleaseError("release directory metadata is invalid")
    manifest_metadata = manifest_path.lstat()
    if (
        stat.S_IMODE(manifest_metadata.st_mode) != 0o444
        or manifest_metadata.st_uid != expected_uid
        or manifest_metadata.st_gid != expected_gid
    ):
        raise ProviderReleaseError("release member metadata is invalid")
    if (
        set(manifest) != _RELEASE_MANIFEST_KEYS
        or manifest_payload != _canonical(manifest)
        or manifest.get("schema") != _BUNDLE_SCHEMA
        or manifest.get("release_sha256") != expected_release_sha256
        or manifest.get("architecture") != expected_architecture
        or not isinstance(manifest.get("files"), list)
    ):
        raise ProviderReleaseError("release manifest is invalid")
    manifest_identity = dict(manifest)
    manifest_identity.pop("release_sha256", None)
    if _digest(_canonical(manifest_identity)) != expected_release_sha256:
        raise ProviderReleaseError("release manifest digest is invalid")
    members: list[VerifiedProviderMember] = []
    seen = {_MANIFEST}
    expected_directories: set[str] = set()
    for record in cast(list[object], manifest["files"]):
        if (
            not isinstance(record, dict)
            or set(record) != _RELEASE_MEMBER_KEYS
            or not isinstance(record["path"], str)
            or not isinstance(record["mode"], str)
            or not _is_sha256(record["sha256"])
        ):
            raise ProviderReleaseError("release manifest is invalid")
        relative = _safe_relative(record["path"])
        if relative.as_posix() in seen:
            raise ProviderReleaseError("release manifest is invalid")
        seen.add(relative.as_posix())
        parent = relative.parent
        while parent != PurePosixPath("."):
            expected_directories.add(parent.as_posix())
            parent = parent.parent
        member_path = _assert_safe_parents(path, relative)
        try:
            mode = int(record["mode"], 8)
        except ValueError as exc:
            raise ProviderReleaseError("release manifest is invalid") from exc
        if record["mode"] != f"{mode:04o}" or mode not in {0o444, 0o555}:
            raise ProviderReleaseError("release manifest is invalid")
        digest, header, source_identity, size = _inspect_regular(
            member_path,
            maximum=_release_member_maximum(relative.as_posix()),
            executable=mode == 0o555,
        )
        if (
            stat.S_IMODE(source_identity[2]) != mode
            or source_identity[7] != expected_uid
            or source_identity[8] != expected_gid
        ):
            raise ProviderReleaseError("release member metadata is invalid")
        if digest != record["sha256"]:
            raise ProviderReleaseError("release member digest is invalid")
        if relative.as_posix() in (
            _RUNTIME_MEMBER_DESTINATIONS
            | {"bpftool", "bin/loom-task-builder-supervisor"}
        ):
            _validate_elf(header, expected_architecture)
        members.append(
            VerifiedProviderMember(
                path=relative.as_posix(),
                mode=mode,
                sha256=digest,
                size=size,
                source_path=member_path,
                _source_identity=source_identity,
            )
        )
    actual_files: set[str] = set()
    actual_directories: set[str] = set()
    for item in path.rglob("*"):
        metadata = item.lstat()
        actual_relative = item.relative_to(path).as_posix()
        if stat.S_ISLNK(metadata.st_mode):
            raise ProviderReleaseError("release member inventory is invalid")
        if stat.S_ISDIR(metadata.st_mode):
            if (
                metadata.st_uid != expected_uid
                or metadata.st_gid != expected_gid
                or stat.S_IMODE(metadata.st_mode) & 0o222
            ):
                raise ProviderReleaseError("release directory metadata is invalid")
            actual_directories.add(actual_relative)
        elif stat.S_ISREG(metadata.st_mode):
            actual_files.add(actual_relative)
        else:
            raise ProviderReleaseError("release member inventory is invalid")
    expected_files = {member.path for member in members} | {_MANIFEST}
    if actual_files != expected_files or actual_directories != expected_directories:
        raise ProviderReleaseError("release member inventory is invalid")
    return VerifiedProviderRelease(
        release_sha256=expected_release_sha256,
        architecture=expected_architecture,
        directory=path,
        manifest_payload=manifest_payload,
        manifest=manifest,
        members=tuple(members),
    )


def verify_release_directory_against_spec(
    path: Path,
    *,
    source_root: Path,
    expected_architecture: Architecture,
    expected_uid: int,
    expected_gid: int,
    expected_release_sha256: str | None = None,
) -> VerifiedProviderRelease:
    """Verify a release against the reviewed provider spec, not caller digest."""

    release = verify_release_directory(
        path,
        expected_release_sha256=path.name,
        expected_architecture=expected_architecture,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
    )
    (
        spec,
        spec_payload,
        runtime_members,
        runtime_release,
        runtime_x_crypto,
    ) = _read_reviewed_spec(source_root, architecture=expected_architecture)
    member_records = _verified_member_records(release)
    guard_records = _guard_member_records(member_records)
    guard_payload_records = _guard_member_payload_records(release.members)
    if (
        release.manifest.get("release_spec_sha256") != _digest(spec_payload)
        or release.manifest.get("architecture") != expected_architecture
        or release.manifest.get("authority_contract_version") != spec.authority_contract_version
        or release.manifest.get("provider_install_root") != spec.provider_install_root
        or release.manifest.get("supervisor_relative_path") != spec.supervisor_relative_path
        or release.manifest.get("runtime_release") != runtime_release
        or release.manifest.get("runtime_x_crypto") != runtime_x_crypto
        or release.manifest.get("guard_release_sha256")
        != spec.guard_bundle_sha256[expected_architecture]
        or _guard_bundle_identity_sha256(
            guard_payload_records,
            architecture=expected_architecture,
            release_spec_sha256=spec.guard_release_sha256,
        )
        != spec.guard_bundle_sha256[expected_architecture]
    ):
        raise ProviderReleaseError("release differs from reviewed specification")
    expected_records = _expected_spec_bound_records(
        spec,
        expected_architecture,
        runtime_members,
        guard_records,
    )
    if member_records != expected_records:
        raise ProviderReleaseError("release differs from reviewed specification")
    identity: dict[str, object] = {
        "architecture": expected_architecture,
        "authority_contract_version": spec.authority_contract_version,
        "files": [
            {"path": name, "mode": f"{mode:04o}", "sha256": digest}
            for name, mode, digest in expected_records
        ],
        "guard_release_sha256": spec.guard_bundle_sha256[expected_architecture],
        "provider_install_root": spec.provider_install_root,
        "release_spec_sha256": _digest(spec_payload),
        "runtime_release": runtime_release,
        "runtime_x_crypto": runtime_x_crypto,
        "schema": _BUNDLE_SCHEMA,
        "supervisor_relative_path": spec.supervisor_relative_path,
    }
    reviewed_release_sha256 = _digest(_canonical(identity))
    if (
        release.release_sha256 != reviewed_release_sha256
        or release.manifest != {**identity, "release_sha256": reviewed_release_sha256}
        or (
            expected_release_sha256 is not None
            and expected_release_sha256 != reviewed_release_sha256
        )
    ):
        raise ProviderReleaseError("release differs from reviewed specification")
    return release


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--guard-release-directory-x86-64", type=Path, required=True)
    parser.add_argument("--guard-release-directory-aarch64", type=Path, required=True)
    parser.add_argument("--runtime-root-x86-64", type=Path, required=True)
    parser.add_argument("--runtime-root-aarch64", type=Path, required=True)
    args = parser.parse_args(argv)
    releases = build_certified_releases(
        args.source_root.resolve(strict=True),
        args.output_root.resolve(),
        guard_release_directories={
            "x86_64": args.guard_release_directory_x86_64.resolve(strict=True),
            "aarch64": args.guard_release_directory_aarch64.resolve(strict=True),
        },
        runtime_roots={
            "x86_64": args.runtime_root_x86_64.resolve(strict=True),
            "aarch64": args.runtime_root_aarch64.resolve(strict=True),
        },
    )
    print(
        json.dumps(
            {
                architecture: {
                    "path": str(release.directory),
                    "release_sha256": release.release_sha256,
                }
                for architecture, release in sorted(releases.items())
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
