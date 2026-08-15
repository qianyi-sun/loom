"""Copy Kubernetes projected credentials into strict owner-only regular files."""

from __future__ import annotations

import argparse
import os
import stat
import tempfile
from pathlib import Path

_FILENAMES = (
    "ca.pem",
    "certificate.pem",
    "database-url",
    "private-key.pem",
    "reporter-configuration.json",
    "reporter-token",
)
_MAX_FILE_BYTES = 1024 * 1024


def _read_bounded_descriptor(descriptor: int, *, expected_size: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total <= _MAX_FILE_BYTES:
        chunk = os.read(descriptor, min(4096, _MAX_FILE_BYTES + 1 - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    closed = os.fstat(descriptor)
    if closed.st_size != expected_size or total != expected_size:
        raise ValueError("projected credential changed while being copied")
    return b"".join(chunks)


def _read_existing_destination(destination: Path) -> dict[str, bytes]:
    metadata = destination.lstat()
    if (
        destination.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise ValueError("owner-only credential destination is invalid")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    directory = os.open(destination, flags)
    try:
        opened_directory = os.fstat(directory)
        if (opened_directory.st_dev, opened_directory.st_ino) != (
            metadata.st_dev,
            metadata.st_ino,
        ):
            raise ValueError("owner-only credential destination changed while opening")
        if set(os.listdir(directory)) != set(_FILENAMES):
            raise ValueError("owner-only credential destination has an unexpected file set")
        payloads: dict[str, bytes] = {}
        for filename in _FILENAMES:
            descriptor = os.open(
                filename,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory,
            )
            try:
                opened = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or opened.st_uid != os.getuid()
                    or stat.S_IMODE(opened.st_mode) != 0o600
                    or not 0 < opened.st_size <= _MAX_FILE_BYTES
                ):
                    raise ValueError("owner-only credential destination file is invalid")
                payloads[filename] = _read_bounded_descriptor(
                    descriptor,
                    expected_size=opened.st_size,
                )
            finally:
                os.close(descriptor)
        return payloads
    finally:
        os.close(directory)


def copy_projected_credentials(source: Path, destination: Path) -> None:
    """Copy the exact projected file set without permitting an external target."""

    source_root = source.resolve(strict=True)
    if not source_root.is_dir():
        raise ValueError("projected credential source must be a directory")
    destination_parent = destination.parent.resolve(strict=True)
    if not destination_parent.is_dir() or not destination.name or destination.is_symlink():
        raise ValueError("owner-only credential destination is invalid")
    source_payloads: dict[str, bytes] = {}
    for filename in _FILENAMES:
        projected = source / filename
        resolved = projected.resolve(strict=True)
        try:
            resolved.relative_to(source_root)
        except ValueError as exc:
            raise ValueError("projected credential resolves outside its volume") from exc
        metadata = resolved.stat()
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= _MAX_FILE_BYTES:
            raise ValueError("projected credential is not a bounded regular file")
        source_descriptor = os.open(
            resolved,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened = os.fstat(source_descriptor)
            if (
                (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
                or not stat.S_ISREG(opened.st_mode)
                or not 0 < opened.st_size <= _MAX_FILE_BYTES
            ):
                raise ValueError("projected credential changed while being opened")
            source_payloads[filename] = _read_bounded_descriptor(
                source_descriptor,
                expected_size=opened.st_size,
            )
        finally:
            os.close(source_descriptor)

    if destination.exists():
        if _read_existing_destination(destination) != source_payloads:
            raise ValueError("owner-only credential destination differs from projected input")
        return

    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}-",
            dir=destination_parent,
        )
    )
    try:
        for filename, payload in source_payloads.items():
            target = staging / filename
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
            if target.is_symlink() or stat.S_IMODE(target.stat().st_mode) != 0o600:
                raise ValueError("owner-only credential copy is invalid")
        os.rename(staging, destination_parent / destination.name)
    except BaseException:
        for filename in _FILENAMES:
            try:
                (staging / filename).unlink()
            except FileNotFoundError:
                pass
        try:
            staging.rmdir()
        except FileNotFoundError:
            pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare owner-only capacity credentials")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    arguments = parser.parse_args()
    copy_projected_credentials(arguments.source, arguments.destination)


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()


__all__ = ["copy_projected_credentials", "main"]
