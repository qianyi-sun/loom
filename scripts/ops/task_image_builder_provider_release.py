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
from uuid import uuid4

Architecture = Literal["x86_64", "aarch64"]

_SPEC = Path("deploy/task-image-builder/provider-release-v1.json")
_SPEC_SCHEMA = "loom.task-image-builder-provider-release-spec/v1"
_BUNDLE_SCHEMA = "loom.task-image-builder-provider-bundle/v1"
_MANIFEST = "release-manifest.json"
_MAX_BYTES = 16 * 1024 * 1024
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
_GUARD_MEMBERS = (
    "guard-network-map-schema-v1.json",
    "guard-network-v1.bpf.build.json",
    "guard-network-v1.bpf.o",
    "loom-task-image-builder-guard.pyz",
    "loom-task-image-builder-node-guard.service",
)
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
    members: tuple[tuple[str, int, bytes], ...]


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
        if initial.st_mode & 0o002 or (executable and initial.st_mode & 0o020):
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


def _load_guard_release(path: Path, *, architecture: Architecture, expected_release_sha256: str) -> dict[str, bytes]:
    if (
        path.name != expected_release_sha256
        or len(expected_release_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_release_sha256)
        or expected_release_sha256 == "0" * 64
    ):
        raise ProviderReleaseError("guard release directory identity is invalid")
    manifest_path = path / _MANIFEST
    manifest_payload = _read_regular(manifest_path, maximum=_MAX_JSON_BYTES)
    try:
        manifest = json.loads(manifest_payload, object_pairs_hook=_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ProviderReleaseError("guard release manifest is invalid") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema") != "loom.task-image-builder-guard-bundle/v1"
        or set(manifest) != {"architecture", "files", "release_sha256", "schema"}
        or _canonical(manifest) != manifest_payload
        or manifest.get("architecture") != architecture
        or manifest.get("release_sha256") != expected_release_sha256
        or not isinstance(manifest.get("files"), list)
    ):
        raise ProviderReleaseError("guard release manifest is invalid")
    identity = dict(manifest)
    identity.pop("release_sha256", None)
    if _digest(_canonical(identity)) != expected_release_sha256:
        raise ProviderReleaseError("guard release digest is invalid")
    try:
        entries = sorted(path.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise ProviderReleaseError("guard release directory is unavailable") from exc
    if {item.name for item in entries} != {*_GUARD_MEMBERS, _MANIFEST}:
        raise ProviderReleaseError("guard release inventory is invalid")
    expected_files = cast(list[object], manifest["files"])
    expected_names = tuple(sorted(_GUARD_MEMBERS))
    if len(expected_files) != len(expected_names):
        raise ProviderReleaseError("guard release file manifest is invalid")
    payloads: dict[str, bytes] = {}
    for record, name in zip(expected_files, expected_names, strict=True):
        mode = 0o555 if name.endswith(".pyz") else 0o444
        if (
            not isinstance(record, dict)
            or set(record) != {"mode", "path", "sha256"}
            or record.get("path") != name
            or record.get("mode") != f"{mode:04o}"
            or not isinstance(record.get("sha256"), str)
        ):
            raise ProviderReleaseError("guard release file manifest is invalid")
        item = path / name
        payload = _read_regular(
            item,
            maximum=_MAX_BYTES,
            executable=mode == 0o555,
        )
        try:
            metadata = item.lstat()
        except OSError as exc:
            raise ProviderReleaseError("guard release member is unavailable") from exc
        if (
            stat.S_IMODE(metadata.st_mode) != mode
            or _digest(payload) != record["sha256"]
        ):
            raise ProviderReleaseError("guard release member differs from manifest")
        payloads[name] = payload
    return payloads


def _load_runtime(
    manifest_path: Path,
    runtime_root: Path,
    *,
    architecture: Architecture,
) -> tuple[dict[str, bytes], str, str, str]:
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
    expected = cast(dict[str, object], entry["members"])
    if tuple(sorted(expected)) != tuple(sorted(_RUNTIME_MEMBERS)):
        raise ProviderReleaseError("runtime member inventory is invalid")
    runtime_dir = _validate_root(runtime_root, "runtime root") / "runtime"
    try:
        entries = sorted(runtime_dir.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise ProviderReleaseError("runtime member directory is unavailable") from exc
    if {item.name for item in entries} != set(_RUNTIME_MEMBERS):
        raise ProviderReleaseError("runtime member inventory is invalid")
    payloads: dict[str, bytes] = {}
    for item in entries:
        payload = _read_regular(item, maximum=_MAX_BYTES, executable=True)
        if _digest(payload) != expected.get(item.name):
            raise ProviderReleaseError("runtime member digest differs from manifest")
        payloads[item.name] = payload
    return (
        payloads,
        cast(str, manifest["release"]),
        cast(str, toolchain["x_crypto"]),
        f"{image}@sha256:{image_sha256}",
    )


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
    members: list[tuple[str, int, bytes]] = []
    for item in spec.configs:
        members.append(
            (
                cast(str, item.destination),
                cast(int, item.mode),
                _checked_payload(source_root, item, maximum=_MAX_JSON_BYTES),
            )
        )
    for item in spec.scripts:
        members.append(
            (
                cast(str, item.destination),
                cast(int, item.mode),
                _checked_payload(source_root, item, maximum=_MAX_JSON_BYTES, executable=True),
            )
        )
    for name in _GUARD_MEMBERS:
        members.append((name, 0o555 if name.endswith(".pyz") else 0o444, guard_members[name]))
    members.extend(
        [
            ("bin/loom-task-builder-supervisor", 0o555, supervisor_payload),
            ("runtime/buildctl", 0o555, runtime_payloads["buildctl"]),
            ("runtime/buildkit-runc", 0o555, runtime_payloads["buildkit-runc"]),
            ("runtime/buildkitd", 0o555, runtime_payloads["buildkitd"]),
            ("bin/rootlessctl", 0o555, runtime_payloads["rootlessctl"]),
            ("bin/rootlesskit", 0o555, runtime_payloads["rootlesskit"]),
            ("bin/slirp4netns", 0o555, runtime_payloads["slirp4netns"]),
            ("bin/fuse-overlayfs", 0o555, runtime_payloads["fuse-overlayfs"]),
        ]
    )
    members.sort(key=lambda item: item[0])
    if len({name for name, _, _ in members}) != len(members):
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
            {"path": name, "mode": f"{mode:04o}", "sha256": _digest(payload)}
            for name, mode, payload in members
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
        for name, mode, payload in members:
            _write_file(candidate / name, payload, mode)
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
        for release in staged.values():
            if (output_root / release.release_sha256).exists() or (
                output_root / f"{release.release_sha256}.manifest.json"
            ).exists():
                raise ProviderReleaseError("release destination collision")
        published: dict[Architecture, ProviderRelease] = {}
        try:
            for architecture, release in staged.items():
                directory = output_root / release.release_sha256
                sidecar_path = output_root / f"{release.release_sha256}.manifest.json"
                release.directory.chmod(0o755)
                release.directory.rename(directory)
                directory.chmod(0o555)
                release.sidecar_path.rename(sidecar_path)
                published[architecture] = ProviderRelease(
                    release_sha256=release.release_sha256,
                    directory=directory,
                    manifest_path=directory / _MANIFEST,
                    sidecar_path=sidecar_path,
                    manifest=release.manifest,
                )
        except OSError as exc:
            conflict = output_root / f".provider-release-set-conflict.{uuid4()}"
            conflict.mkdir(mode=0o700)
            for release in published.values():
                if release.directory.exists():
                    release.directory.chmod(0o755)
                    release.directory.rename(conflict / release.directory.name)
                if release.sidecar_path.exists():
                    release.sidecar_path.rename(conflict / release.sidecar_path.name)
            raise ProviderReleaseError("release set publication failed") from exc
        return published
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


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
        manifest.get("schema") != _BUNDLE_SCHEMA
        or manifest.get("release_sha256") != expected_release_sha256
        or manifest.get("architecture") != expected_architecture
        or not isinstance(manifest.get("files"), list)
    ):
        raise ProviderReleaseError("release manifest is invalid")
    identity = dict(manifest)
    identity.pop("release_sha256", None)
    if _digest(_canonical(identity)) != expected_release_sha256:
        raise ProviderReleaseError("release manifest digest is invalid")
    members: list[tuple[str, int, bytes]] = []
    seen = {_MANIFEST}
    expected_directories: set[str] = set()
    for record in cast(list[object], manifest["files"]):
        if (
            not isinstance(record, dict)
            or set(record) != {"path", "mode", "sha256"}
            or not isinstance(record["path"], str)
            or not isinstance(record["mode"], str)
            or not isinstance(record["sha256"], str)
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
        metadata = member_path.lstat()
        mode = int(record["mode"], 8)
        payload = _read_regular(member_path, maximum=_MAX_BYTES, executable=mode == 0o555)
        if (
            stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
        ):
            raise ProviderReleaseError("release member metadata is invalid")
        if _digest(payload) != record["sha256"]:
            raise ProviderReleaseError("release member digest is invalid")
        members.append((relative.as_posix(), mode, payload))
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
    expected_files = {name for name, _, _ in members} | {_MANIFEST}
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
