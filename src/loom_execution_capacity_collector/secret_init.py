"""Copy projected collector credentials into strict owner-only files."""

from __future__ import annotations

import argparse
import os
import stat
from pathlib import Path

_FILES = ("control-plane-token", "nebius-credentials.json")
_MAX_BYTES = 1024 * 1024


def _read_descriptor(descriptor: int, *, expected_size: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total <= _MAX_BYTES:
        chunk = os.read(descriptor, min(4096, _MAX_BYTES + 1 - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    if total != expected_size:
        raise ValueError("credential changed while being read")
    return b"".join(chunks)


def _read_regular(path: Path, *, require_owner_mode: bool) -> bytes:
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or not 0 < metadata.st_size <= _MAX_BYTES
        or (
            require_owner_mode
            and (metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600)
        )
    ):
        raise ValueError("credential file metadata is invalid")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_size != metadata.st_size
        ):
            raise ValueError("credential changed while being opened")
        return _read_descriptor(descriptor, expected_size=opened.st_size)
    finally:
        os.close(descriptor)


def copy_projected_credentials(source: Path, destination: Path) -> None:
    source_root = source.resolve(strict=True)
    destination.mkdir(mode=0o700, parents=False, exist_ok=True)
    destination_metadata = destination.lstat()
    if (
        destination.is_symlink()
        or not stat.S_ISDIR(destination_metadata.st_mode)
        or destination_metadata.st_uid != os.getuid()
    ):
        raise ValueError("credential destination must be a 0700 directory")
    # Kubernetes fsGroup ownership can widen an emptyDir child directory to
    # 0770 between volume setup and the init container. This directory is
    # owned by the non-root init identity, so normalize it before writing any
    # credential rather than rejecting every fresh Pod.
    destination.chmod(0o700)
    if stat.S_IMODE(destination.stat().st_mode) != 0o700:
        raise ValueError("credential destination must be a 0700 directory")
    for name in _FILES:
        projected = source / name
        resolved = projected.resolve(strict=True)
        try:
            resolved.relative_to(source_root)
        except ValueError as exc:
            raise ValueError("projected credential resolves outside its volume") from exc
        metadata = resolved.stat()
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= _MAX_BYTES:
            raise ValueError("projected credential is not a bounded regular file")
        payload = _read_regular(resolved, require_owner_mode=False)
        target = destination / name
        if target.exists() or target.is_symlink():
            if _read_regular(target, require_owner_mode=True) != payload:
                raise ValueError("existing credential copy does not match projected input")
            continue
        descriptor = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        try:
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare capacity collector credentials")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    copy_projected_credentials(args.source, args.destination)


if __name__ == "__main__":
    main()


__all__ = ["copy_projected_credentials", "main"]
