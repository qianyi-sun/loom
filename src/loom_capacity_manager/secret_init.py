"""Prepare owner-only runtime files from projected Kubernetes Secrets."""

from __future__ import annotations

import argparse
import os
import stat
import tempfile
from pathlib import Path
from typing import Literal, cast

CredentialProfile = Literal["manager", "migration"]

_PROFILE_FILES: dict[CredentialProfile, frozenset[str]] = {
    "manager": frozenset(
        {
            "client-ca.pem",
            "database-url",
            "health-certificate.pem",
            "health-private-key.pem",
            "ownership-public-keys.json",
            "principals.json",
            "server-ca.pem",
            "server-certificate.pem",
            "server-private-key.pem",
        }
    ),
    "migration": frozenset({"database-url"}),
}
_MAX_FILE_BYTES = 1024 * 1024


def _read_bounded_descriptor(descriptor: int, *, expected_size: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total <= _MAX_FILE_BYTES:
        chunk = os.read(descriptor, min(64 * 1024, _MAX_FILE_BYTES + 1 - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    closed = os.fstat(descriptor)
    if closed.st_size != expected_size or total != expected_size:
        raise ValueError("projected capacity credential changed while being copied")
    return b"".join(chunks)


def _source_payloads(source: Path, expected: frozenset[str]) -> dict[str, bytes]:
    source_root = source.resolve(strict=True)
    if not source_root.is_dir():
        raise ValueError("projected capacity credential source is not a directory")
    visible = {entry.name for entry in source.iterdir() if not entry.name.startswith("..")}
    if visible != expected:
        raise ValueError("projected capacity credential file set is invalid")
    payloads: dict[str, bytes] = {}
    for filename in expected:
        resolved = (source / filename).resolve(strict=True)
        try:
            resolved.relative_to(source_root)
        except ValueError as exc:
            raise ValueError("projected capacity credential resolves outside its volume") from exc
        metadata = resolved.stat()
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= _MAX_FILE_BYTES:
            raise ValueError("projected capacity credential is not a bounded regular file")
        descriptor = os.open(
            resolved,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened = os.fstat(descriptor)
            if (
                (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
                or not stat.S_ISREG(opened.st_mode)
                or not 0 < opened.st_size <= _MAX_FILE_BYTES
            ):
                raise ValueError("projected capacity credential changed while being opened")
            payloads[filename] = _read_bounded_descriptor(
                descriptor,
                expected_size=opened.st_size,
            )
        finally:
            os.close(descriptor)
    return payloads


def _existing_payloads(
    destination: Path,
    expected: frozenset[str],
) -> dict[str, bytes]:
    metadata = destination.lstat()
    if (
        destination.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise ValueError("owner-only capacity credential destination is invalid")
    visible = {entry.name for entry in destination.iterdir()}
    if visible != expected:
        raise ValueError("owner-only capacity credential destination file set is invalid")
    payloads: dict[str, bytes] = {}
    for filename in expected:
        target = destination / filename
        file_metadata = target.lstat()
        if (
            target.is_symlink()
            or not stat.S_ISREG(file_metadata.st_mode)
            or file_metadata.st_uid != os.getuid()
            or stat.S_IMODE(file_metadata.st_mode) != 0o600
            or not 0 < file_metadata.st_size <= _MAX_FILE_BYTES
        ):
            raise ValueError("owner-only capacity credential destination file is invalid")
        descriptor = os.open(
            target,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (
                file_metadata.st_dev,
                file_metadata.st_ino,
            ):
                raise ValueError("owner-only capacity credential changed while opening")
            payloads[filename] = _read_bounded_descriptor(
                descriptor,
                expected_size=opened.st_size,
            )
        finally:
            os.close(descriptor)
    return payloads


def copy_projected_credentials(
    source: Path,
    destination: Path,
    *,
    profile: CredentialProfile,
) -> None:
    """Copy one exact projected profile into a mode-0700 directory."""

    if profile not in _PROFILE_FILES:
        raise ValueError("capacity credential profile is invalid")
    expected = _PROFILE_FILES[profile]
    source_payloads = _source_payloads(source, expected)
    destination_parent = destination.parent.resolve(strict=True)
    if not destination.name or destination.is_symlink() or not destination_parent.is_dir():
        raise ValueError("owner-only capacity credential destination is invalid")
    if destination.exists():
        if _existing_payloads(destination, expected) != source_payloads:
            raise ValueError(
                "owner-only capacity credential destination differs from projected input"
            )
        return
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination_parent)
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
                raise ValueError("owner-only capacity credential copy is invalid")
        os.rename(staging, destination)
    except BaseException:
        for filename in expected:
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
    parser = argparse.ArgumentParser(description="Prepare capacity-manager credentials")
    parser.add_argument("--profile", choices=tuple(_PROFILE_FILES), required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    arguments = parser.parse_args()
    copy_projected_credentials(
        arguments.source,
        arguments.destination,
        profile=cast(CredentialProfile, arguments.profile),
    )


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()


__all__ = ["CredentialProfile", "copy_projected_credentials", "main"]
