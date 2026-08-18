#!/usr/bin/env python3
"""Record and verify one immutable capacity-executor installation artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any

_COMPONENT = "capacity-executor"
_MANIFEST_NAME = "release-manifest.json"
_PAYLOAD_NAME = "payload"
_SOURCE_SHA = re.compile(r"^[0-9a-f]{40}$")
_ARCHITECTURES = frozenset({"amd64", "arm64"})
_MAX_MANIFEST_BYTES = 1024 * 1024


class CapacityExecutorReleaseError(ValueError):
    """Raised when a release artifact is incomplete, changed, or unsafe."""


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_identity(*, source_sha: str, architecture: str) -> None:
    if not isinstance(source_sha, str) or _SOURCE_SHA.fullmatch(source_sha) is None:
        raise CapacityExecutorReleaseError("release source SHA must be 40 lowercase hex bytes")
    if not isinstance(architecture, str) or architecture not in _ARCHITECTURES:
        raise CapacityExecutorReleaseError("release architecture must be amd64 or arm64")


def _release_paths(root: Path) -> tuple[Path, Path]:
    if not isinstance(root, Path) or not root.is_absolute():
        raise CapacityExecutorReleaseError("release root must be an absolute path")
    if root.is_symlink() or not root.is_dir():
        raise CapacityExecutorReleaseError("release root must be a non-symlink directory")
    payload = root / _PAYLOAD_NAME
    manifest = root / _MANIFEST_NAME
    if payload.is_symlink() or not payload.is_dir():
        raise CapacityExecutorReleaseError("release payload must be a non-symlink directory")
    return payload, manifest


def _payload_records(payload: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for path in sorted(payload.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(payload).as_posix()
        if path.is_symlink():
            raise CapacityExecutorReleaseError(f"release payload contains a symlink: {relative}")
        metadata = path.stat()
        if stat.S_ISDIR(metadata.st_mode):
            continue
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise CapacityExecutorReleaseError(
                f"release payload entry is not a single-link regular file: {relative}"
            )
        mode = stat.S_IMODE(metadata.st_mode)
        if mode != 0o444:
            raise CapacityExecutorReleaseError(f"release payload file mode is not 0444: {relative}")
        records.append(
            {
                "mode": "0444",
                "path": relative,
                "sha256": _sha256(path),
                "size": metadata.st_size,
            }
        )
    if not records:
        raise CapacityExecutorReleaseError("release payload is empty")
    return records


def record_release(root: Path, *, source_sha: str, architecture: str) -> dict[str, object]:
    """Write a canonical manifest for the exact read-only payload tree."""

    _validate_identity(source_sha=source_sha, architecture=architecture)
    payload, manifest_path = _release_paths(root)
    if manifest_path.exists() or manifest_path.is_symlink():
        raise CapacityExecutorReleaseError("release manifest already exists")
    manifest: dict[str, object] = {
        "architecture": architecture,
        "component": _COMPONENT,
        "files": _payload_records(payload),
        "schema_version": 1,
        "source_sha": source_sha,
    }
    encoded = _canonical_json(manifest)
    descriptor = os.open(
        manifest_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o444,
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        manifest_path.unlink(missing_ok=True)
        raise
    manifest_path.chmod(0o444)
    return manifest


def _load_manifest(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise CapacityExecutorReleaseError("release manifest must be a regular non-symlink file")
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise CapacityExecutorReleaseError("release manifest must be a single-link regular file")
    if stat.S_IMODE(metadata.st_mode) != 0o444:
        raise CapacityExecutorReleaseError("release manifest mode must be 0444")
    if metadata.st_size <= 0 or metadata.st_size > _MAX_MANIFEST_BYTES:
        raise CapacityExecutorReleaseError("release manifest size is invalid")
    encoded = path.read_bytes()
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityExecutorReleaseError("release manifest is not valid JSON") from exc
    if not isinstance(value, dict) or _canonical_json(value) != encoded:
        raise CapacityExecutorReleaseError("release manifest is not canonical JSON")
    return value


def verify_release(
    root: Path,
    *,
    expected_source_sha: str,
    expected_architecture: str,
) -> dict[str, object]:
    """Verify identity, metadata, and every byte of an extracted release artifact."""

    _validate_identity(
        source_sha=expected_source_sha,
        architecture=expected_architecture,
    )
    payload, manifest_path = _release_paths(root)
    manifest = _load_manifest(manifest_path)
    expected_keys = {
        "architecture",
        "component",
        "files",
        "schema_version",
        "source_sha",
    }
    if set(manifest) != expected_keys:
        raise CapacityExecutorReleaseError("release manifest shape is invalid")
    if (
        manifest["schema_version"] != 1
        or manifest["component"] != _COMPONENT
        or manifest["source_sha"] != expected_source_sha
        or manifest["architecture"] != expected_architecture
    ):
        raise CapacityExecutorReleaseError("release manifest identity differs from expectation")
    records = _payload_records(payload)
    if manifest["files"] != records:
        raise CapacityExecutorReleaseError("release payload differs from its manifest")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("record", "verify"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--release-root", type=Path, required=True)
        subparser.add_argument("--source-sha", required=True)
        subparser.add_argument("--architecture", choices=sorted(_ARCHITECTURES), required=True)
    args = parser.parse_args()
    try:
        if args.command == "record":
            record_release(
                args.release_root,
                source_sha=args.source_sha,
                architecture=args.architecture,
            )
        else:
            verify_release(
                args.release_root,
                expected_source_sha=args.source_sha,
                expected_architecture=args.architecture,
            )
    except CapacityExecutorReleaseError as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
