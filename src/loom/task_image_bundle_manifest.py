"""Canonical registration-bound file integrity, separate from legacy identity."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.task_image_build_plan import (
    MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
    MAX_TASK_IMAGE_BUILD_BUNDLE_FILES,
    Digest,
)
from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME

MAX_CONTENT_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_MODE_METADATA_BYTES = 4 * 1024 * 1024
_MAX_TREE_ENTRIES = 16_000
_MAX_TREE_DEPTH = 64


def _path(value: str) -> str:
    if (
        not value or len(value.encode("utf-8")) > 1024
        or value == BUNDLE_FILE_METADATA_NAME or "\\" in value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
        or value.startswith("/") or PurePosixPath(value).as_posix() != value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ValueError("bundle manifest path is invalid")
    return value


class TaskImageBundleManifestFileV1(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    path: Annotated[str, Field(min_length=1, max_length=1024)]
    size_bytes: Annotated[int, Field(ge=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES)]
    sha256: Digest
    mode: Literal["0644", "0755"]

    @field_validator("path")
    @classmethod
    def _safe_path(cls, value: str) -> str:
        return _path(value)


def _mode_bytes(files: tuple[TaskImageBundleManifestFileV1, ...]) -> bytes:
    # This is deliberately the legacy ASCII-escaped mode-sidecar encoding,
    # NOT RFC8785. Its existing provenance digest must retain identical bytes.
    return json.dumps(
        {"schema_version": 1, "files": {item.path: {"mode": item.mode} for item in files}},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


class TaskImageBundleContentManifestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    schema_version: Literal["loom.task-image-bundle-content.v1"] = "loom.task-image-bundle-content.v1"
    task_checksum: Digest
    bundle_file_metadata_sha256: Digest
    files: Annotated[tuple[TaskImageBundleManifestFileV1, ...], Field(min_length=1, max_length=MAX_TASK_IMAGE_BUILD_BUNDLE_FILES)]

    @model_validator(mode="after")
    def _complete_set(self) -> TaskImageBundleContentManifestV1:
        paths = tuple(item.path for item in self.files)
        path_set = set(paths)
        if paths != tuple(sorted(paths)) or len(paths) != len(path_set):
            raise ValueError("bundle manifest files must be uniquely sorted")
        if any(str(parent) in path_set for item in self.files for parent in PurePosixPath(item.path).parents):
            raise ValueError("bundle manifest file is also a directory")
        if sum(item.size_bytes for item in self.files) > MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES:
            raise ValueError("bundle manifest byte quota exceeded")
        mode_bytes = _mode_bytes(self.files)
        if len(mode_bytes) > MAX_MODE_METADATA_BYTES:
            raise ValueError("bundle mode metadata byte quota exceeded")
        if hashlib.sha256(mode_bytes).hexdigest() != self.bundle_file_metadata_sha256:
            raise ValueError("bundle manifest mode provenance is inconsistent")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        result = rfc8785.dumps(self.model_dump(mode="json"))
        if len(result) > MAX_CONTENT_MANIFEST_BYTES:
            raise ValueError("bundle content manifest is too large")
        return result

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def mode_metadata_bytes(self) -> bytes:
        return _mode_bytes(self.files)


def parse_task_image_bundle_manifest(payload: bytes, *, expected_sha256: str) -> TaskImageBundleContentManifestV1:
    """Require exact registered bytes, canonical schema and internally bound modes."""
    try:
        if (
            type(payload) is not bytes or not 0 < len(payload) <= MAX_CONTENT_MANIFEST_BYTES
            or type(expected_sha256) is not str or len(expected_sha256) != 64
            or hashlib.sha256(payload).hexdigest() != expected_sha256
        ):
            raise ValueError("invalid manifest digest or size")
        result = TaskImageBundleContentManifestV1.model_validate_json(payload)
        # Equality to the unique canonical encoding also rejects duplicate keys,
        # omitted defaults, alternate encodings and whitespace ambiguities.
        if result.canonical_bytes != payload:
            raise ValueError("manifest is not canonical")
        return result
    except (ValueError, TypeError):
        raise ValueError("bundle content manifest is invalid") from None


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid,
            value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def task_image_bundle_manifest_key(digest: str) -> str:
    if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None or digest == "0" * 64:
        raise ValueError("bundle content manifest digest is invalid")
    return f"loom-bundle-manifests/v1/sha256/{digest}.json"


def read_verified_task_image_bundle_file(task_dir: Path, file: TaskImageBundleManifestFileV1) -> bytes:
    """Return exactly the registered bytes from an owned no-follow descriptor.

    The returned immutable bytes, not another pathname read, must be passed to
    storage. Descriptor hashes bind uploads even when another await permits the
    staging tree to change. The caller validates the complete manifest first.
    """
    descriptors: list[int] = []
    try:
        file = TaskImageBundleManifestFileV1.model_validate_json(file.model_dump_json())
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        descriptors.append(os.open(task_dir, directory_flags))
        for part in file.path.split("/")[:-1]:
            if os.fstat(descriptors[-1]).st_uid != os.getuid():
                raise ValueError("bundle directory is not owned")
            descriptors.append(os.open(part, directory_flags, dir_fd=descriptors[-1]))
        if os.fstat(descriptors[-1]).st_uid != os.getuid():
            raise ValueError("bundle directory is not owned")
        descriptors.append(os.open(
            file.path.split("/")[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=descriptors[-1],
        ))
        descriptor = descriptors[-1]
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
            or before.st_nlink != 1 or before.st_size != file.size_bytes
            or ("0755" if before.st_mode & 0o111 else "0644") != file.mode
        ):
            raise ValueError("bundle file metadata changed")
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        count = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, file.size_bytes - count + 1))
            if not chunk:
                break
            count += len(chunk)
            if count > file.size_bytes:
                raise ValueError("bundle file size changed")
            digest.update(chunk)
            chunks.append(chunk)
        if (
            count != file.size_bytes or digest.hexdigest() != file.sha256
            or _identity(os.fstat(descriptor)) != _identity(before)
        ):
            raise ValueError("bundle file content changed")
        return b"".join(chunks)
    except (OSError, ValueError):
        raise ValueError("bundle file no longer matches registered content") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def capture_task_image_bundle_manifest(task_dir: Path) -> TaskImageBundleContentManifestV1:
    """Capture an owned regular tree; uploader must still verify its actual bytes.

    Descriptor-relative no-follow traversal, finite quotas and before/after
    metadata checks detect unsafe or changing input. This is not a filesystem
    snapshot primitive: only publishing the captured bytes/descriptors can bind
    a later upload. Never use this to infer authority from untrusted storage.
    """
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        root = os.open(task_dir, directory_flags)
    except OSError:
        raise ValueError("bundle content capture failed") from None
    try:
        legacy = hashlib.sha256()
        files: list[TaskImageBundleManifestFileV1] = []
        snapshots: dict[str, tuple[int, ...]] = {}
        total = 0
        listed_entries = 0

        def scan(directory: int, prefix: str, *, capture: bool, seen: dict[str, tuple[int, ...]]) -> None:
            nonlocal total, listed_entries
            before = os.fstat(directory)
            if before.st_uid != os.getuid() or len(prefix.split("/")) > _MAX_TREE_DEPTH:
                raise ValueError("bundle directory authority or depth is invalid")
            entries: list[str] = []
            with os.scandir(directory) as iterator:
                for entry in iterator:
                    listed_entries += 1
                    if listed_entries > _MAX_TREE_ENTRIES:
                        raise ValueError("bundle tree entry quota exceeded")
                    entries.append(entry.name)
            for name in sorted(entries):
                relative = prefix + name
                value = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if value.st_uid != os.getuid() or not (stat.S_ISDIR(value.st_mode) or stat.S_ISREG(value.st_mode)):
                    raise ValueError("bundle entry must be owned and regular")
                if relative == BUNDLE_FILE_METADATA_NAME:
                    if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
                        raise ValueError("bundle metadata entry is unsafe")
                    continue
                _path(relative)
                if len(seen) >= _MAX_TREE_ENTRIES:
                    raise ValueError("bundle tree entry quota exceeded")
                seen[relative] = _identity(value)
                if stat.S_ISDIR(value.st_mode):
                    child = os.open(name, directory_flags, dir_fd=directory)
                    try:
                        if _identity(os.fstat(child)) != _identity(value):
                            raise ValueError("bundle directory changed")
                        scan(child, relative + "/", capture=capture, seen=seen)
                    finally:
                        os.close(child)
                    continue
                if value.st_nlink != 1:
                    raise ValueError("bundle hardlinks are not supported")
                if not capture:
                    continue
                if len(files) >= MAX_TASK_IMAGE_BUILD_BUNDLE_FILES or value.st_size > MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES - total:
                    raise ValueError("bundle content quota exceeded")
                descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=directory)
                try:
                    opened = os.fstat(descriptor)
                    if _identity(opened) != _identity(value):
                        raise ValueError("bundle file changed")
                    content = hashlib.sha256()
                    legacy.update(b"\x00" + relative.encode("utf-8") + b"\x00")
                    count = 0
                    while True:
                        chunk = os.read(descriptor, min(1024 * 1024, value.st_size - count + 1))
                        if not chunk:
                            break
                        count += len(chunk)
                        if count > value.st_size:
                            raise ValueError("bundle file changed size")
                        content.update(chunk)
                        legacy.update(chunk)
                    if count != value.st_size or _identity(os.fstat(descriptor)) != _identity(value):
                        raise ValueError("bundle file changed during capture")
                    total += count
                    files.append(TaskImageBundleManifestFileV1(
                        path=relative, size_bytes=count, sha256=content.hexdigest(),
                        mode="0755" if value.st_mode & 0o111 else "0644",
                    ))
                finally:
                    os.close(descriptor)
            if _identity(os.fstat(directory)) != _identity(before):
                raise ValueError("bundle directory changed during capture")

        root_identity = _identity(os.fstat(root))
        scan(root, "", capture=True, seen=snapshots)
        refreshed: dict[str, tuple[int, ...]] = {}
        listed_entries = 0
        scan(root, "", capture=False, seen=refreshed)
        if refreshed != snapshots or _identity(os.fstat(root)) != root_identity:
            raise ValueError("bundle tree changed during capture")
        ordered = tuple(sorted(files, key=lambda item: item.path))
        result = TaskImageBundleContentManifestV1(
            task_checksum=legacy.hexdigest(), files=ordered,
            bundle_file_metadata_sha256=hashlib.sha256(_mode_bytes(ordered)).hexdigest(),
        )
        _ = result.canonical_bytes
        return result
    except (OSError, ValueError):
        raise ValueError("bundle content capture failed") from None
    finally:
        os.close(root)
