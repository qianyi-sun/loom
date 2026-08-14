#!/usr/bin/env python3
"""Extract one digest-verified runtime archive into a closed directory."""

from __future__ import annotations

import argparse
import os
import shutil
import tarfile
import tempfile
from pathlib import Path, PurePosixPath


class RuntimeAssetError(ValueError):
    """A pinned runtime archive violated the closed extraction contract."""


def _member_path(name: str, expected_root: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if (
        path.is_absolute()
        or not path.parts
        or path.parts[0] != expected_root
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise RuntimeAssetError("runtime archive contains an unsafe path")
    return path


def _validate_symlink(member: tarfile.TarInfo, expected_root: str) -> None:
    target = PurePosixPath(member.linkname)
    if target.is_absolute():
        raise RuntimeAssetError("runtime archive contains an absolute symlink")
    parts = list(PurePosixPath(member.name).parent.parts)
    for part in target.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if len(parts) <= 1:
                raise RuntimeAssetError("runtime archive symlink escapes its root")
            parts.pop()
        else:
            parts.append(part)
    _member_path(PurePosixPath(*parts).as_posix(), expected_root)


def extract_runtime_asset(
    archive_path: Path,
    destination: Path,
    expected_root: str,
) -> None:
    if destination.exists():
        raise RuntimeAssetError("runtime asset destination already exists")
    destination.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    with tarfile.open(archive_path, mode="r:*") as archive:
        members = archive.getmembers()
        seen: set[PurePosixPath] = set()
        total_size = 0
        for member in members:
            path = _member_path(member.name, expected_root)
            if path in seen:
                raise RuntimeAssetError("runtime archive contains a duplicate path")
            seen.add(path)
            if member.isfile():
                total_size += member.size
                if member.size < 0 or total_size > 512 * 1024 * 1024:
                    raise RuntimeAssetError("runtime archive exceeds its byte budget")
            elif member.isdir():
                continue
            elif member.issym():
                _validate_symlink(member, expected_root)
            else:
                raise RuntimeAssetError("runtime archive contains a forbidden entry type")

        temporary = Path(tempfile.mkdtemp(prefix=".runtime-asset-", dir=destination.parent))
        try:
            archive.extractall(temporary)
            extracted_root = temporary / expected_root
            if not extracted_root.is_dir() or extracted_root.is_symlink():
                raise RuntimeAssetError("runtime archive root is not a directory")
            os.replace(extracted_root, destination)
        finally:
            shutil.rmtree(temporary, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("expected_root")
    args = parser.parse_args()
    extract_runtime_asset(args.archive, args.destination, args.expected_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
