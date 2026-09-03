#!/usr/bin/env python3
"""Assemble one deterministic, content-addressed node-guard release."""

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
import tempfile
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal, cast

Architecture = Literal["x86_64", "aarch64"]

_SPEC = Path("deploy/task-image-builder/guard-release-v1.json")
_SPEC_SCHEMA = "loom.task-image-builder-guard-release-spec/v1"
_BUNDLE_SCHEMA = "loom.task-image-builder-guard-bundle/v1"
_PACKAGE_ROOT = PurePosixPath("src/loom_task_image_builder_guard")
_ARCHIVE = "loom-task-image-builder-guard.pyz"
_MANIFEST = "release-manifest.json"
_MAX_SPEC_BYTES = 1024 * 1024
_MAX_SOURCE_BYTES = 4 * 1024 * 1024
_MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
_MAX_BPF_OBJECT_BYTES = 4 * 1024 * 1024
_MAX_BPFTOOL_BYTES = 64 * 1024 * 1024
_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")
_MACHINES: dict[Architecture, int] = {"x86_64": 62, "aarch64": 183}
_ZIP_ENTRYPOINT = (
    b"from loom_task_image_builder_guard.__main__ import main\n"
    b"raise SystemExit(main())\n"
)
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100


class GuardReleaseError(ValueError):
    """The requested guard release input or publication is unsafe."""


@dataclass(frozen=True, slots=True)
class GuardRelease:
    """Paths and identity of one newly published release."""

    release_sha256: str
    directory: Path
    manifest_path: Path
    sidecar_path: Path
    manifest: dict[str, object]


@dataclass(frozen=True, slots=True)
class _Input:
    path: PurePosixPath
    sha256: str
    destination: str | None = None
    mode: int | None = None


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
        raise GuardReleaseError("release input path is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != value
        or (suffix is not None and path.suffix != suffix)
    ):
        raise GuardReleaseError("release input path is invalid")
    return path


def _validate_root(path: Path, label: str) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise GuardReleaseError(f"{label} must be an absolute path")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise GuardReleaseError(f"{label} is unavailable") from exc
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise GuardReleaseError(f"{label} must be a non-symlink directory")
    return path


def _assert_safe_parents(root: Path, relative: PurePosixPath) -> Path:
    current = root
    for part in relative.parts[:-1]:
        current /= part
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise GuardReleaseError("release input parent is unavailable") from exc
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise GuardReleaseError("release input parent is unsafe")
    return root.joinpath(*relative.parts)


def _read_regular(
    path: Path,
    *,
    maximum: int,
    executable: bool = False,
) -> bytes:
    descriptor = -1
    try:
        initial = path.lstat()
        if (
            not stat.S_ISREG(initial.st_mode)
            or stat.S_ISLNK(initial.st_mode)
            or initial.st_nlink != 1
        ):
            raise GuardReleaseError("release input must be a single-link regular file")
        if initial.st_mode & 0o002 or (executable and initial.st_mode & 0o020):
            raise GuardReleaseError("release input is group/world writable")
        if executable and initial.st_mode & 0o111 == 0:
            raise GuardReleaseError("release executable is not executable")
        if initial.st_size <= 0 or initial.st_size > maximum:
            raise GuardReleaseError("release input is empty or too large")
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
            raise GuardReleaseError("release input changed while opening")
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
        if len(payload) != initial.st_size or len(payload) > maximum:
            raise GuardReleaseError("release input is empty or too large")
        if final_identity != identity or final_path_identity != identity:
            raise GuardReleaseError("release input changed while being read")
        return payload
    except GuardReleaseError:
        raise
    except OSError as exc:
        raise GuardReleaseError("release input is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _checked_payload(
    root: Path,
    item: _Input,
    *,
    maximum: int,
) -> bytes:
    path = _assert_safe_parents(root, item.path)
    payload = _read_regular(path, maximum=maximum)
    if _digest(payload) != item.sha256:
        raise GuardReleaseError("release input digest differs from specification")
    return payload


def _record(value: object, *, artifact: bool) -> _Input:
    if not isinstance(value, dict):
        raise GuardReleaseError("release specification record is invalid")
    expected = {"path", "sha256", "destination", "mode"} if artifact else {"path", "sha256"}
    if set(value) != expected:
        raise GuardReleaseError("release specification record is invalid")
    path = _safe_relative(value["path"], suffix=None)
    digest = value["sha256"]
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or digest == "0" * 64
    ):
        raise GuardReleaseError("release specification digest is invalid")
    if not artifact:
        return _Input(path, digest)
    destination = value["destination"]
    mode = value["mode"]
    if (
        not isinstance(destination, str)
        or PurePosixPath(destination).name != destination
        or destination in {"", ".", "..", _MANIFEST}
        or mode not in {"0444", "0555"}
    ):
        raise GuardReleaseError("release artifact layout is invalid")
    return _Input(path, digest, destination, int(cast(str, mode), 8))


def _load_spec(root: Path) -> tuple[bytes, tuple[_Input, ...], tuple[_Input, ...], _Input]:
    spec_path = _assert_safe_parents(root, PurePosixPath(_SPEC.as_posix()))
    payload = _read_regular(spec_path, maximum=_MAX_SPEC_BYTES)
    try:
        raw = json.loads(payload, object_pairs_hook=_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise GuardReleaseError("release specification is not valid JSON") from None
    if (
        not isinstance(raw, dict)
        or set(raw) != {"schema", "version", "sources", "bpf_source", "artifacts"}
        or raw["schema"] != _SPEC_SCHEMA
        or raw["version"] != 1
        or not isinstance(raw["sources"], list)
        or not isinstance(raw["artifacts"], list)
    ):
        raise GuardReleaseError("release specification shape is invalid")
    sources = tuple(_record(value, artifact=False) for value in raw["sources"])
    artifacts = tuple(_record(value, artifact=True) for value in raw["artifacts"])
    bpf_source = _record(raw["bpf_source"], artifact=False)
    actual_sources = tuple(
        PurePosixPath(path.relative_to(root).as_posix())
        for path in sorted((root / Path(_PACKAGE_ROOT.as_posix())).glob("*.py"))
    )
    if (
        tuple(item.path for item in sources) != actual_sources
        or tuple(item.path for item in sources) != tuple(sorted(item.path for item in sources))
        or not sources
        or len({item.path for item in sources}) != len(sources)
    ):
        raise GuardReleaseError("release specification source coverage is invalid")
    destinations = tuple(cast(str, item.destination) for item in artifacts)
    if (
        destinations
        != (
            "guard-network-map-schema-v1.json",
            "guard-network-v1.bpf.build.json",
            "guard-network-v1.bpf.o",
        )
        or len({item.path for item in artifacts}) != len(artifacts)
    ):
        raise GuardReleaseError("release specification artifact coverage is invalid")
    return payload, sources, artifacts, bpf_source


def _validate_bpf(
    *,
    source: bytes,
    artifacts: dict[str, bytes],
) -> None:
    object_payload = artifacts["guard-network-v1.bpf.o"]
    schema_payload = artifacts["guard-network-map-schema-v1.json"]
    provenance_payload = artifacts["guard-network-v1.bpf.build.json"]
    if len(object_payload) < _ELF_HEADER.size:
        raise GuardReleaseError("BPF object is invalid")
    header = _ELF_HEADER.unpack_from(object_payload)
    ident = header[0]
    if ident[:7] != b"\x7fELF\x02\x01\x01" or header[1] != 1 or header[2] != 247:
        raise GuardReleaseError("BPF object is not little-endian EM_BPF")
    try:
        provenance = json.loads(provenance_payload, object_pairs_hook=_pairs)
        map_schema = json.loads(schema_payload, object_pairs_hook=_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise GuardReleaseError("BPF release metadata is invalid") from None
    expected_provenance_keys = {
        "schema",
        "builder_image",
        "builder_platform",
        "clang_version",
        "target",
        "source_sha256",
        "object_sha256",
        "object_size",
        "map_schema_sha256",
        "program_sections",
        "program_symbols",
        "map_symbols",
    }
    if (
        not isinstance(provenance, dict)
        or set(provenance) != expected_provenance_keys
        or provenance.get("schema") != "loom.task-image-builder-guard-bpf-build/v1"
        or provenance.get("target") != "bpfel"
        or provenance.get("builder_platform") != "linux/amd64"
        or not isinstance(provenance.get("builder_image"), str)
        or "@sha256:" not in cast(str, provenance["builder_image"])
        or provenance.get("source_sha256") != _digest(source)
        or provenance.get("object_sha256") != _digest(object_payload)
        or provenance.get("object_size") != len(object_payload)
        or provenance.get("map_schema_sha256") != _digest(schema_payload)
        or not isinstance(map_schema, dict)
        or set(map_schema) != {"schema", "maps"}
        or map_schema.get("schema") != "loom.task-image-builder-guard-bpf-maps/v1"
    ):
        raise GuardReleaseError("BPF release provenance differs from artifacts")


def _validate_bpftool(payload: bytes, architecture: Architecture) -> None:
    if len(payload) < _ELF_HEADER.size:
        raise GuardReleaseError("bpftool architecture is invalid")
    header = _ELF_HEADER.unpack_from(payload)
    ident = header[0]
    if (
        ident[:7] != b"\x7fELF\x02\x01\x01"
        or header[1] not in {2, 3}
        or header[2] != _MACHINES[architecture]
    ):
        raise GuardReleaseError("bpftool architecture does not match release")


def _write_file(path: Path, payload: bytes, mode: int) -> None:
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
        position = 0
        while position < len(payload):
            position += os.write(descriptor, payload[position:])
        os.fsync(descriptor)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    except OSError as exc:
        raise GuardReleaseError("release output could not be written") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _zipapp(path: Path, sources: tuple[tuple[PurePosixPath, bytes], ...]) -> None:
    entries = ((PurePosixPath("__main__.py"), _ZIP_ENTRYPOINT), *sources)
    try:
        with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_STORED) as archive:
            for name, payload in entries:
                info = zipfile.ZipInfo(name.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.compress_type = zipfile.ZIP_STORED
                info.external_attr = (stat.S_IFREG | 0o444) << 16
                archive.writestr(info, payload)
        path.chmod(0o555)
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except (OSError, zipfile.BadZipFile) as exc:
        raise GuardReleaseError("guard zipapp could not be assembled") from exc


def _file_record(path: Path, mode: int) -> dict[str, object]:
    payload = _read_regular(path, maximum=_MAX_BPFTOOL_BYTES, executable=mode == 0o555)
    metadata = path.stat()
    if stat.S_IMODE(metadata.st_mode) != mode:
        raise GuardReleaseError("release output mode differs from expectation")
    return {
        "mode": f"{mode:04o}",
        "path": path.name,
        "sha256": _digest(payload),
        "size": len(payload),
    }


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise GuardReleaseError("atomic no-replace publication is unavailable")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise GuardReleaseError("release digest directory already exists")
    raise GuardReleaseError("atomic no-replace publication failed") from OSError(
        error, os.strerror(error)
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def build_release(
    source_root: Path,
    bpftool: Path,
    output: Path,
    architecture: Architecture,
) -> GuardRelease:
    """Build and atomically publish one native guard release."""

    root = _validate_root(source_root, "release source root")
    if architecture not in _MACHINES:
        raise GuardReleaseError("release architecture is unsupported")
    if not bpftool.is_absolute():
        raise GuardReleaseError("bpftool path must be absolute")
    bpftool_payload = _read_regular(
        bpftool,
        maximum=_MAX_BPFTOOL_BYTES,
        executable=True,
    )
    _validate_bpftool(bpftool_payload, architecture)
    spec_payload, source_records, artifact_records, bpf_source_record = _load_spec(root)
    sources = tuple(
        (
            PurePosixPath(*item.path.parts[1:]),
            _checked_payload(root, item, maximum=_MAX_SOURCE_BYTES),
        )
        for item in source_records
    )
    bpf_source = _checked_payload(root, bpf_source_record, maximum=_MAX_SOURCE_BYTES)
    artifacts = {
        cast(str, item.destination): _checked_payload(
            root,
            item,
            maximum=_MAX_BPF_OBJECT_BYTES
            if item.destination == "guard-network-v1.bpf.o"
            else _MAX_ARTIFACT_BYTES,
        )
        for item in artifact_records
    }
    _validate_bpf(source=bpf_source, artifacts=artifacts)

    if not output.is_absolute():
        raise GuardReleaseError("release output must be an absolute path")
    try:
        output.mkdir(mode=0o755, parents=True, exist_ok=True)
    except OSError as exc:
        raise GuardReleaseError("release output is unavailable") from exc
    output = _validate_root(output, "release output")
    staging = Path(tempfile.mkdtemp(prefix=".guard-release-", dir=output))
    published = False
    try:
        _zipapp(staging / _ARCHIVE, sources)
        _write_file(staging / "bpftool", bpftool_payload, 0o555)
        for item in artifact_records:
            destination = cast(str, item.destination)
            _write_file(staging / destination, artifacts[destination], cast(int, item.mode))
        files = [
            _file_record(staging / name, mode)
            for name, mode in (
                ("bpftool", 0o555),
                ("guard-network-map-schema-v1.json", 0o444),
                ("guard-network-v1.bpf.build.json", 0o444),
                ("guard-network-v1.bpf.o", 0o444),
                (_ARCHIVE, 0o555),
            )
        ]
        identity: dict[str, object] = {
            "architecture": architecture,
            "files": files,
            "interpreter": "/usr/bin/python3 -I -B",
            "release_spec_sha256": _digest(spec_payload),
            "schema": _BUNDLE_SCHEMA,
        }
        release_sha256 = _digest(_canonical(identity))
        manifest: dict[str, object] = {**identity, "release_sha256": release_sha256}
        manifest_payload = _canonical(manifest)
        _write_file(staging / _MANIFEST, manifest_payload, 0o444)
        _fsync_directory(staging)
        staging.chmod(0o555)
        _fsync_directory(staging)
        release_directory = output / release_sha256
        sidecar = output / f"{release_sha256}.manifest.json"
        if (
            release_directory.exists()
            or release_directory.is_symlink()
            or sidecar.exists()
            or sidecar.is_symlink()
        ):
            raise GuardReleaseError("release digest directory already exists")
        _rename_noreplace(staging, release_directory)
        published = True
        _fsync_directory(output)
        _write_file(sidecar, manifest_payload, 0o444)
        _fsync_directory(output)
        return GuardRelease(
            release_sha256=release_sha256,
            directory=release_directory,
            manifest_path=release_directory / _MANIFEST,
            sidecar_path=sidecar,
            manifest=manifest,
        )
    finally:
        if not published and staging.exists():
            staging.chmod(0o700)
            shutil.rmtree(staging)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--bpftool", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--architecture", choices=sorted(_MACHINES), required=True)
    arguments = parser.parse_args(argv)
    try:
        release = build_release(
            arguments.source_root,
            arguments.bpftool,
            arguments.output,
            cast(Architecture, arguments.architecture),
        )
    except GuardReleaseError as exc:
        parser.error(str(exc))
    print(release.release_sha256)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["GuardRelease", "GuardReleaseError", "build_release", "main"]
