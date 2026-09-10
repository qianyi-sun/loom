"""Dedicated process-only Ed25519 handles from explicit protected existing files.

No generation fallback, key export, environment seed or caller-selected handle.
Key installation and the service account are separate operator-owned provisioning.
"""

from __future__ import annotations

import hmac
import os
import stat
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


class FileSigningKey:
    """Private bytes remain in this dedicated signer process, never its clients."""

    def __init__(self, private: Ed25519PrivateKey) -> None:
        self._private = private

    @property
    def public_key(self) -> bytes:
        return self._private.public_key().public_bytes_raw()

    async def sign(self, preimage: bytes) -> bytes:
        # Ed25519 on a bounded <=64KiB statement is local computation, not
        # detached thread work that could outlive cancellation or a DB fence.
        if type(preimage) is not bytes or not 0 < len(preimage) <= 65 * 1024:
            raise ValueError("invalid signer preimage")
        return self._private.sign(preimage)


def _directory(fd: int, *, final: bool) -> None:
    info = os.fstat(fd)
    mode = stat.S_IMODE(info.st_mode)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in {0, os.geteuid()}:
        raise ValueError("unsafe signer key directory")
    # A root-owned sticky ancestor (e.g. /tmp in disposable tests) cannot rename
    # another owner's pinned child. Every actual key directory is owner-only.
    if mode & 0o022 and not (not final and info.st_uid == 0 and mode & stat.S_ISVTX):
        raise ValueError("writable signer key directory")
    if final and (info.st_uid != os.geteuid() or mode & 0o077):
        raise ValueError("signer key directory must be owner-only")


def _metadata(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_uid,
        info.st_gid, info.st_size, info.st_mtime_ns, info.st_ctime_ns,
    )


def load_signing_key(path: Path, *, expected_public_key: bytes) -> FileSigningKey:
    if (
        not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts
        or len(path.parts) < 3 or type(expected_public_key) is not bytes or len(expected_public_key) != 32
    ):
        raise ValueError("invalid signer key configuration")
    directory_fd: int | None = None
    key_fd: int | None = None
    try:
        directory_fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        _directory(directory_fd, final=False)
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = child
            _directory(directory_fd, final=False)
        _directory(directory_fd, final=True)
        key_fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory_fd)
        before = os.fstat(key_fd)
        if (
            not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) not in {0o400, 0o600}
            or before.st_nlink != 1 or before.st_size != 32
        ):
            raise ValueError("invalid protected signer key file")
        seed = os.read(key_fd, 33)
        if len(seed) != 32 or os.read(key_fd, 1) or _metadata(os.fstat(key_fd)) != _metadata(before):
            raise ValueError("signer key changed during descriptor read")
        private = Ed25519PrivateKey.from_private_bytes(seed)
        if not hmac.compare_digest(private.public_key().public_bytes_raw(), expected_public_key):
            raise ValueError("signer key public identity mismatch")
        return FileSigningKey(private)
    except (OSError, ValueError):
        raise ValueError("protected signer key could not be loaded") from None
    finally:
        if key_fd is not None:
            os.close(key_fd)
        if directory_fd is not None:
            os.close(directory_fd)
