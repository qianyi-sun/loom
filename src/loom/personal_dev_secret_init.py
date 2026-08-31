"""Prepare bounded owner-only files from projected personal-dev Secrets."""

from __future__ import annotations

import argparse
import os
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

CredentialProfile = Literal[
    "management-files",
    "activation-public",
    "activation-private",
    "native-builder-public",
]

_PROFILE_FILES: dict[CredentialProfile, frozenset[str]] = {
    "management-files": frozenset(
        {
            "admin-secrets.toml",
            "capacity-lifecycle-ca.pem",
            "capacity-lifecycle-certificate.pem",
            "capacity-lifecycle-private-key.pem",
            "capacity-lifecycle-token",
            "capacity-reporter-ca.pem",
            "capacity-reporter-certificate.pem",
            "capacity-reporter-private-key.pem",
            "config.json",
        }
    ),
    "activation-public": frozenset({"public-key"}),
    "activation-private": frozenset({"private-key"}),
    "native-builder-public": frozenset({"public-key"}),
}
_MAX_FILE_BYTES = 1024 * 1024
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_DIRECTORY", 0)
)
_FILE_READ_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
_GENERIC_ERROR = "personal-dev projected credential copy is invalid"


class PersonalDevCredentialError(ValueError):
    """A projected credential snapshot or owner-only copy is invalid."""


def _invalid() -> PersonalDevCredentialError:
    return PersonalDevCredentialError(_GENERIC_ERROR)


@dataclass(frozen=True)
class _ProjectedSnapshot:
    source_descriptor: int
    data_link_identity: tuple[int, int]
    generation_name: str
    generation_identity: tuple[int, int]
    expected: frozenset[str]
    key_link_identities: dict[str, tuple[int, int]]
    file_identities: dict[str, tuple[int, int, int, int, int, int, int]]
    payloads: dict[str, bytes]

    def validate(self) -> None:
        """Recheck every projection identity immediately before commit."""

        generation_descriptor: int | None = None
        try:
            data_link = os.stat("..data", dir_fd=self.source_descriptor, follow_symlinks=False)
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
            generation_files = {
                filename: os.stat(
                    filename,
                    dir_fd=generation_descriptor,
                    follow_symlinks=False,
                )
                for filename in self.expected
            }
            generation = os.fstat(generation_descriptor)
            if (
                not stat.S_ISLNK(data_link.st_mode)
                or (data_link.st_dev, data_link.st_ino) != self.data_link_identity
                or generation_name != self.generation_name
                or (generation.st_dev, generation.st_ino) != self.generation_identity
                or visible != self.expected
                or set(os.listdir(generation_descriptor)) != self.expected
            ):
                raise _invalid()
            for filename, (metadata, target) in key_links.items():
                if (
                    not stat.S_ISLNK(metadata.st_mode)
                    or target != f"..data/{filename}"
                    or (metadata.st_dev, metadata.st_ino) != self.key_link_identities[filename]
                ):
                    raise _invalid()
                if _file_identity(generation_files[filename]) != self.file_identities[filename]:
                    raise _invalid()
        except PersonalDevCredentialError:
            raise
        except OSError:
            raise _invalid() from None
        finally:
            if generation_descriptor is not None:
                os.close(generation_descriptor)

    def close(self) -> None:
        os.close(self.source_descriptor)


def _read_bounded_descriptor(descriptor: int, *, expected_size: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total <= _MAX_FILE_BYTES:
        try:
            chunk = os.read(descriptor, min(64 * 1024, _MAX_FILE_BYTES + 1 - total))
        except OSError:
            raise _invalid() from None
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    try:
        after = os.fstat(descriptor)
    except OSError:
        raise _invalid() from None
    if after.st_size != expected_size or total != expected_size:
        raise _invalid()
    return b"".join(chunks)


def _file_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _is_private_directory(metadata: os.stat_result) -> bool:
    """Allow only exact owner-owned mode-0700 directories."""

    return (
        stat.S_ISDIR(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o700
    )


def _source_payloads(source: Path, expected: frozenset[str]) -> _ProjectedSnapshot:
    try:
        source_metadata = source.lstat()
        if stat.S_ISLNK(source_metadata.st_mode) or not stat.S_ISDIR(source_metadata.st_mode):
            raise _invalid()
        source_descriptor = os.open(source, _DIRECTORY_FLAGS)
    except PersonalDevCredentialError:
        raise
    except OSError:
        raise _invalid() from None
    snapshot: _ProjectedSnapshot | None = None
    generation_descriptor: int | None = None
    try:
        opened_source = os.fstat(source_descriptor)
        if (opened_source.st_dev, opened_source.st_ino) != (
            source_metadata.st_dev,
            source_metadata.st_ino,
        ):
            raise _invalid()
        visible = {name for name in os.listdir(source_descriptor) if not name.startswith("..")}
        if visible != expected:
            raise _invalid()
        data_link = os.stat("..data", dir_fd=source_descriptor, follow_symlinks=False)
        if not stat.S_ISLNK(data_link.st_mode):
            raise _invalid()
        generation_name = os.readlink("..data", dir_fd=source_descriptor)
        if (
            not generation_name.startswith("..")
            or generation_name in {"..", "..data"}
            or Path(generation_name).name != generation_name
        ):
            raise _invalid()
        generation_descriptor = os.open(
            generation_name,
            _DIRECTORY_FLAGS,
            dir_fd=source_descriptor,
        )
        generation = os.fstat(generation_descriptor)
        if set(os.listdir(generation_descriptor)) != expected:
            raise _invalid()
        payloads: dict[str, bytes] = {}
        key_link_identities: dict[str, tuple[int, int]] = {}
        file_identities: dict[str, tuple[int, int, int, int, int, int, int]] = {}
        for filename in expected:
            key_metadata = os.stat(
                filename,
                dir_fd=source_descriptor,
                follow_symlinks=False,
            )
            key_target = os.readlink(filename, dir_fd=source_descriptor)
            if not stat.S_ISLNK(key_metadata.st_mode) or key_target != f"..data/{filename}":
                raise _invalid()
            key_link_identities[filename] = (key_metadata.st_dev, key_metadata.st_ino)
            metadata = os.stat(
                filename,
                dir_fd=generation_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or not 0 < metadata.st_size <= _MAX_FILE_BYTES
            ):
                raise _invalid()
            file_identities[filename] = _file_identity(metadata)
            descriptor = os.open(filename, _FILE_READ_FLAGS, dir_fd=generation_descriptor)
            try:
                opened = os.fstat(descriptor)
                if (
                    (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
                    or not stat.S_ISREG(opened.st_mode)
                    or opened.st_nlink != 1
                    or not 0 < opened.st_size <= _MAX_FILE_BYTES
                ):
                    raise _invalid()
                payloads[filename] = _read_bounded_descriptor(
                    descriptor,
                    expected_size=opened.st_size,
                )
                closed = os.fstat(descriptor)
                if _file_identity(closed) != _file_identity(opened):
                    raise _invalid()
            finally:
                os.close(descriptor)
        candidate = _ProjectedSnapshot(
            source_descriptor=source_descriptor,
            data_link_identity=(data_link.st_dev, data_link.st_ino),
            generation_name=generation_name,
            generation_identity=(generation.st_dev, generation.st_ino),
            expected=expected,
            key_link_identities=key_link_identities,
            file_identities=file_identities,
            payloads=payloads,
        )
        candidate.validate()
        snapshot = candidate
        return snapshot
    except PersonalDevCredentialError:
        raise
    except OSError:
        raise _invalid() from None
    finally:
        if generation_descriptor is not None:
            os.close(generation_descriptor)
        if snapshot is None:
            os.close(source_descriptor)


def _open_private_parent(destination: Path) -> int:
    if not destination.name or destination.name in {".", ".."}:
        raise _invalid()
    parent = destination.parent
    try:
        metadata = parent.lstat()
        if not _is_private_directory(metadata):
            raise _invalid()
        descriptor = os.open(parent, _DIRECTORY_FLAGS)
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (
            metadata.st_dev,
            metadata.st_ino,
        ) or not _is_private_directory(opened):
            os.close(descriptor)
            raise _invalid()
        return descriptor
    except PersonalDevCredentialError:
        raise
    except OSError:
        raise _invalid() from None


def _existing_payloads(
    parent_descriptor: int,
    destination_name: str,
    expected: frozenset[str],
) -> dict[str, bytes] | None:
    try:
        metadata = os.stat(
            destination_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None
    except OSError:
        raise _invalid() from None
    if not _is_private_directory(metadata):
        raise _invalid()
    try:
        descriptor = os.open(destination_name, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
    except OSError:
        raise _invalid() from None
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
            or not _is_private_directory(opened)
            or set(os.listdir(descriptor)) != expected
        ):
            raise _invalid()
        payloads: dict[str, bytes] = {}
        for filename in expected:
            file_metadata = os.stat(filename, dir_fd=descriptor, follow_symlinks=False)
            if (
                stat.S_ISLNK(file_metadata.st_mode)
                or not stat.S_ISREG(file_metadata.st_mode)
                or file_metadata.st_uid != os.geteuid()
                or stat.S_IMODE(file_metadata.st_mode) != 0o600
                or file_metadata.st_nlink != 1
                or not 0 < file_metadata.st_size <= _MAX_FILE_BYTES
            ):
                raise _invalid()
            file_descriptor = os.open(filename, _FILE_READ_FLAGS, dir_fd=descriptor)
            try:
                file_opened = os.fstat(file_descriptor)
                if (
                    (file_opened.st_dev, file_opened.st_ino)
                    != (file_metadata.st_dev, file_metadata.st_ino)
                    or file_opened.st_uid != os.geteuid()
                    or stat.S_IMODE(file_opened.st_mode) != 0o600
                    or file_opened.st_nlink != 1
                ):
                    raise _invalid()
                payloads[filename] = _read_bounded_descriptor(
                    file_descriptor,
                    expected_size=file_opened.st_size,
                )
                if _file_identity(os.fstat(file_descriptor)) != _file_identity(file_opened):
                    raise _invalid()
            finally:
                os.close(file_descriptor)
        destination_after = os.stat(
            destination_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        directory_after = os.fstat(descriptor)
        if (
            (destination_after.st_dev, destination_after.st_ino)
            != (metadata.st_dev, metadata.st_ino)
            or (directory_after.st_dev, directory_after.st_ino) != (opened.st_dev, opened.st_ino)
            or not _is_private_directory(destination_after)
            or not _is_private_directory(directory_after)
            or set(os.listdir(descriptor)) != expected
        ):
            raise _invalid()
        return payloads
    except PersonalDevCredentialError:
        raise
    except OSError:
        raise _invalid() from None
    finally:
        os.close(descriptor)


def _create_staging(parent_descriptor: int, destination_name: str) -> tuple[str, int]:
    for _attempt in range(64):
        name = f".{destination_name}-{secrets.token_hex(12)}"
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
        except FileExistsError:
            continue
        except OSError:
            raise _invalid() from None
        metadata = os.fstat(descriptor)
        if not _is_private_directory(metadata):
            os.close(descriptor)
            try:
                os.rmdir(name, dir_fd=parent_descriptor)
            except OSError:
                pass
            raise _invalid()
        return name, descriptor
    raise _invalid()


def _cleanup_staging(
    parent_descriptor: int,
    staging_name: str,
    staging_descriptor: int | None,
    expected: frozenset[str],
) -> None:
    if staging_descriptor is not None:
        try:
            os.close(staging_descriptor)
        except OSError:
            pass
    for filename in expected:
        try:
            os.unlink(f"{staging_name}/{filename}", dir_fd=parent_descriptor)
        except OSError:
            pass
    try:
        os.rmdir(staging_name, dir_fd=parent_descriptor)
    except OSError:
        pass


def _copy_new_destination(
    snapshot: _ProjectedSnapshot,
    parent_descriptor: int,
    destination_name: str,
) -> None:
    staging_name, opened_staging = _create_staging(parent_descriptor, destination_name)
    staging_descriptor: int | None = opened_staging
    try:
        for filename, payload in snapshot.payloads.items():
            if staging_descriptor is None:  # pragma: no cover - guarded state
                raise _invalid()
            descriptor = os.open(
                filename,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=staging_descriptor,
            )
            try:
                written = 0
                while written < len(payload):
                    amount = os.write(descriptor, payload[written:])
                    if amount <= 0:
                        raise _invalid()
                    written += amount
                os.fsync(descriptor)
                metadata = os.fstat(descriptor)
                if (
                    metadata.st_uid != os.geteuid()
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                    or metadata.st_nlink != 1
                    or metadata.st_size != len(payload)
                ):
                    raise _invalid()
            finally:
                os.close(descriptor)
        if staging_descriptor is None:  # pragma: no cover - guarded state
            raise _invalid()
        os.fsync(staging_descriptor)
        snapshot.validate()
        os.close(staging_descriptor)
        staging_descriptor = None
        os.rename(
            staging_name,
            destination_name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
        )
    except PersonalDevCredentialError:
        _cleanup_staging(
            parent_descriptor,
            staging_name,
            staging_descriptor,
            snapshot.expected,
        )
        raise
    except OSError:
        _cleanup_staging(
            parent_descriptor,
            staging_name,
            staging_descriptor,
            snapshot.expected,
        )
        raise _invalid() from None


def copy_projected_credentials(
    source: Path,
    destination: Path,
    *,
    profile: CredentialProfile,
) -> None:
    """Copy one exact projected profile into a private immutable-by-replay directory."""

    if profile not in _PROFILE_FILES:
        raise _invalid()
    snapshot = _source_payloads(source, _PROFILE_FILES[profile])
    parent_descriptor: int | None = None
    try:
        parent_descriptor = _open_private_parent(destination)
        existing = _existing_payloads(
            parent_descriptor,
            destination.name,
            snapshot.expected,
        )
        if existing is not None:
            if existing != snapshot.payloads:
                raise _invalid()
            snapshot.validate()
            return
        _copy_new_destination(snapshot, parent_descriptor, destination.name)
    except PersonalDevCredentialError:
        raise
    except (OSError, ValueError):
        raise _invalid() from None
    finally:
        if parent_descriptor is not None:
            os.close(parent_descriptor)
        snapshot.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare personal-dev runtime credentials")
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


__all__ = [
    "CredentialProfile",
    "PersonalDevCredentialError",
    "copy_projected_credentials",
    "main",
]
