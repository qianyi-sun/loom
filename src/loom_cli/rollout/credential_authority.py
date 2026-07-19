"""Single no-follow authority reader for protected rollout input files."""

from __future__ import annotations

import hashlib
import os
import pwd
import stat
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TrustedFileRead:
    payload: bytes
    metadata: os.stat_result
    metadata_fingerprint: str


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def read_trusted_file(
    path: Path,
    *,
    service_uid: int,
    private: bool,
    allow_qianyi_owner: bool = False,
    max_bytes: int = 1024 * 1024,
    require_nonempty: bool = False,
) -> TrustedFileRead:
    """Read once through no-follow parent descriptors and prove metadata stability."""
    normalized = Path(os.path.normpath(path))
    if not normalized.is_absolute() or ".." in path.parts or service_uid < 0 or max_bytes < 1:
        raise ValueError("protected rollout file path or authority is invalid")
    directory_flags = (
        getattr(os, "O_PATH", os.O_RDONLY)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fd: int | None = None
    try:
        directory_fd = os.open("/", directory_flags)
        for component in normalized.parts[1:-1]:
            next_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        fd = os.open(normalized.name, file_flags, dir_fd=directory_fd)
    except OSError as exc:
        if directory_fd is not None:
            os.close(directory_fd)
        raise ValueError("protected rollout file traversal is unsafe") from exc
    os.close(directory_fd)
    try:
        before = os.fstat(fd)
        allowed_owners = {0, service_uid}
        if allow_qianyi_owner:
            try:
                allowed_owners.add(pwd.getpwnam("qianyi").pw_uid)
            except (KeyError, OSError):
                pass
        unsafe_mode = 0o7137 if private else 0o7022
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in allowed_owners
            or stat.S_IMODE(before.st_mode) & unsafe_mode
            or before.st_nlink != 1
            or before.st_size > max_bytes
            or (require_nonempty and before.st_size < 1)
        ):
            raise ValueError("protected rollout file metadata is unsafe")
        payload = bytearray()
        while len(payload) <= max_bytes:
            chunk = os.read(fd, min(65536, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(fd)
        if len(payload) != before.st_size or _identity(before) != _identity(after):
            raise ValueError("protected rollout file changed while it was read")
        metadata_payload = ":".join(str(value) for value in _identity(before)).encode()
        return TrustedFileRead(
            payload=bytes(payload),
            metadata=before,
            metadata_fingerprint=hashlib.sha256(metadata_payload).hexdigest(),
        )
    finally:
        os.close(fd)


__all__ = ["TrustedFileRead", "read_trusted_file"]
