"""Verify registration-bound files before a Python worker looks up or uses images."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loom.driver.task_image import TaskImageBuildError
from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
from loom.task_image_materialization import (
    canonical_task_checksum,
    task_bundle_content_manifest_digest,
)
from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME

_IDENTITY_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
    "st_gid",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


def _consume_mode_sidecar(task_dir: Path, expected: bytes) -> None:
    """Remove only verified transport metadata from the owned private staging tree."""
    root = os.open(task_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if os.fstat(root).st_uid != os.getuid():
            raise ValueError("bundle directory is not owned")
        try:
            descriptor = os.open(
                BUNDLE_FILE_METADATA_NAME,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                dir_fd=root,
            )
        except FileNotFoundError:
            return  # Object-store downloads already consume this transport file.
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.getuid()
                or before.st_nlink != 1
                or before.st_size != len(expected)
            ):
                raise ValueError("bundle mode sidecar is unsafe or outside bounds")
            actual = bytearray()
            while len(actual) <= len(expected):
                chunk = os.read(descriptor, min(1024 * 1024, len(expected) - len(actual) + 1))
                if not chunk:
                    break
                actual.extend(chunk)
            current = os.stat(BUNDLE_FILE_METADATA_NAME, dir_fd=root, follow_symlinks=False)
            after = os.fstat(descriptor)
            if actual != expected or any(
                getattr(before, field) != getattr(after, field)
                or getattr(before, field) != getattr(current, field)
                for field in _IDENTITY_FIELDS
            ):
                raise ValueError("bundle mode sidecar differs from verified content")
            os.unlink(BUNDLE_FILE_METADATA_NAME, dir_fd=root)
        finally:
            os.close(descriptor)
    finally:
        os.close(root)


def verified_task_image_cache_identity(
    task_dir: Path,
    *,
    task_checksum: str,
    source_provenance: Mapping[str, Any],
) -> str:
    """Return a content-qualified cache key only after checking the owned tree.

    Legacy callers retain their current validation and exact cache keys. Strong
    provenance authenticates per-file boundaries, bytes and modes, not just the
    ambiguous legacy aggregate. The separate domain cannot alias a v1 checksum.
    The private downloaded directory must remain worker-owned until use; this
    does not make a concurrently writable shared directory immutable.
    """
    try:
        digest = task_bundle_content_manifest_digest(source_provenance)
        if not digest:
            return task_checksum
        manifest = capture_task_image_bundle_manifest(task_dir)
        if manifest.digest != digest or manifest.task_checksum != canonical_task_checksum(
            task_checksum
        ):
            raise ValueError("content identity differs")
        if (
            "bundle_file_metadata_sha256" in source_provenance
            and source_provenance["bundle_file_metadata_sha256"]
            != "sha256:" + manifest.bundle_file_metadata_sha256
        ):
            raise ValueError("mode provenance differs")
        # Capture excludes the reserved transport sidecar. It must never be
        # extra, unverified Docker context or trial input under this cache key.
        _consume_mode_sidecar(task_dir, manifest.mode_metadata_bytes)
    except (OSError, ValueError):
        raise TaskImageBuildError("materialized task bundle content manifest mismatch") from None
    return "bundle-manifest-sha256:" + digest
