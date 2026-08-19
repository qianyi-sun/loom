"""Prepare owner-only runtime files from projected Kubernetes Secrets."""

from __future__ import annotations

import argparse
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

CredentialProfile = Literal["execution-policy", "manager", "migration"]

_PROFILE_FILES: dict[CredentialProfile, frozenset[str]] = {
    "execution-policy": frozenset({"execution-policy.json"}),
    "manager": frozenset(
        {
            "client-ca.pem",
            "database-url",
            "global-execution-signing-key",
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
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_DIRECTORY", 0)
)


@dataclass(frozen=True)
class _ProjectedSnapshot:
    source_descriptor: int
    data_link_identity: tuple[int, int]
    generation_name: str
    generation_identity: tuple[int, int]
    expected: frozenset[str]
    key_link_identities: dict[str, tuple[int, int]]
    payloads: dict[str, bytes]

    def validate(self) -> None:
        generation_descriptor: int | None = None
        try:
            data_link = os.stat(
                "..data",
                dir_fd=self.source_descriptor,
                follow_symlinks=False,
            )
            generation_name = os.readlink("..data", dir_fd=self.source_descriptor)
            generation_descriptor = os.open(
                self.generation_name,
                _DIRECTORY_FLAGS,
                dir_fd=self.source_descriptor,
            )
            visible = {
                name for name in os.listdir(self.source_descriptor) if not name.startswith("..")
            }
            key_links = {
                filename: (
                    os.stat(
                        filename,
                        dir_fd=self.source_descriptor,
                        follow_symlinks=False,
                    ),
                    os.readlink(filename, dir_fd=self.source_descriptor),
                )
                for filename in self.expected
            }
        except OSError as exc:
            if generation_descriptor is not None:
                os.close(generation_descriptor)
            raise ValueError(
                "projected capacity credential generation changed while being copied"
            ) from exc
        try:
            generation = os.fstat(generation_descriptor)
            if (
                not stat.S_ISLNK(data_link.st_mode)
                or (data_link.st_dev, data_link.st_ino) != self.data_link_identity
                or generation_name != self.generation_name
                or (generation.st_dev, generation.st_ino) != self.generation_identity
                or visible != self.expected
            ):
                raise ValueError(
                    "projected capacity credential generation changed while being copied"
                )
            for filename, (metadata, target) in key_links.items():
                if (
                    not stat.S_ISLNK(metadata.st_mode)
                    or target != f"..data/{filename}"
                    or (metadata.st_dev, metadata.st_ino) != self.key_link_identities[filename]
                ):
                    raise ValueError(
                        "projected capacity credential generation changed while being copied"
                    )
        finally:
            os.close(generation_descriptor)

    def close(self) -> None:
        os.close(self.source_descriptor)


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


def _source_payloads(source: Path, expected: frozenset[str]) -> _ProjectedSnapshot:
    source_metadata = source.lstat()
    if source.is_symlink() or not stat.S_ISDIR(source_metadata.st_mode):
        raise ValueError("projected capacity credential source is not a directory")
    source_descriptor = os.open(source, _DIRECTORY_FLAGS)
    snapshot: _ProjectedSnapshot | None = None
    generation_descriptor: int | None = None
    try:
        opened_source = os.fstat(source_descriptor)
        if (opened_source.st_dev, opened_source.st_ino) != (
            source_metadata.st_dev,
            source_metadata.st_ino,
        ):
            raise ValueError("projected capacity credential source changed while being opened")
        visible = {name for name in os.listdir(source_descriptor) if not name.startswith("..")}
        if visible != expected:
            raise ValueError("projected capacity credential file set is invalid")
        data_link = os.stat(
            "..data",
            dir_fd=source_descriptor,
            follow_symlinks=False,
        )
        if not stat.S_ISLNK(data_link.st_mode):
            raise ValueError("projected capacity credential ..data link is invalid")
        generation_name = os.readlink("..data", dir_fd=source_descriptor)
        if (
            not generation_name.startswith("..")
            or generation_name in {"..", "..data"}
            or Path(generation_name).name != generation_name
        ):
            raise ValueError("projected capacity credential ..data link is invalid")
        generation_descriptor = os.open(
            generation_name,
            _DIRECTORY_FLAGS,
            dir_fd=source_descriptor,
        )
        generation = os.fstat(generation_descriptor)
        if set(os.listdir(generation_descriptor)) != expected:
            raise ValueError("projected capacity credential generation file set is invalid")
        payloads: dict[str, bytes] = {}
        key_link_identities: dict[str, tuple[int, int]] = {}
        source_root = source.resolve(strict=True)
        for filename in expected:
            key_metadata = os.stat(
                filename,
                dir_fd=source_descriptor,
                follow_symlinks=False,
            )
            key_target = os.readlink(filename, dir_fd=source_descriptor)
            if not stat.S_ISLNK(key_metadata.st_mode) or key_target != f"..data/{filename}":
                resolved = (source / filename).resolve(strict=True)
                try:
                    resolved.relative_to(source_root)
                except ValueError as exc:
                    raise ValueError(
                        "projected capacity credential resolves outside its volume"
                    ) from exc
                raise ValueError("projected capacity credential symlink layout is invalid")
            key_link_identities[filename] = (
                key_metadata.st_dev,
                key_metadata.st_ino,
            )
            metadata = os.stat(
                filename,
                dir_fd=generation_descriptor,
                follow_symlinks=False,
            )
            if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= _MAX_FILE_BYTES:
                raise ValueError("projected capacity credential is not a bounded regular file")
            descriptor = os.open(
                filename,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=generation_descriptor,
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
        candidate = _ProjectedSnapshot(
            source_descriptor=source_descriptor,
            data_link_identity=(data_link.st_dev, data_link.st_ino),
            generation_name=generation_name,
            generation_identity=(generation.st_dev, generation.st_ino),
            expected=expected,
            key_link_identities=key_link_identities,
            payloads=payloads,
        )
        candidate.validate()
        snapshot = candidate
        return snapshot
    finally:
        if generation_descriptor is not None:
            os.close(generation_descriptor)
        if snapshot is None:
            os.close(source_descriptor)


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
    snapshot = _source_payloads(source, expected)
    try:
        source_payloads = snapshot.payloads
        destination_parent = destination.parent.resolve(strict=True)
        if not destination.name or destination.is_symlink() or not destination_parent.is_dir():
            raise ValueError("owner-only capacity credential destination is invalid")
        if destination.exists():
            if _existing_payloads(destination, expected) != source_payloads:
                raise ValueError(
                    "owner-only capacity credential destination differs from projected input"
                )
            snapshot.validate()
            return
        staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination_parent))
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
            snapshot.validate()
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
    finally:
        snapshot.close()


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
