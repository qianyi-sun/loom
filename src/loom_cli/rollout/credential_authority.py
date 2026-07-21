"""Single no-follow authority reader for protected rollout input files."""

from __future__ import annotations

import errno
import hashlib
import os
import pwd
import stat
import struct
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TrustedFileRead:
    payload: bytes
    metadata: os.stat_result
    metadata_fingerprint: str
    acl_fingerprint: str


_POSIX_ACL_XATTR = "system.posix_acl_access"
_POSIX_ACL_VERSION = 2
_ACL_USER_OBJ = 0x01
_ACL_USER = 0x02
_ACL_GROUP_OBJ = 0x04
_ACL_GROUP = 0x08
_ACL_MASK = 0x10
_ACL_OTHER = 0x20
_ACL_UNDEFINED_ID = 0xFFFFFFFF
_MAX_ACL_ENTRIES = 64


def _get_acl_xattr(fd: int, name: str) -> bytes:
    reader = getattr(os, "getxattr", None)
    if reader is None:
        return b""
    value = reader(fd, name)
    if not isinstance(value, bytes):
        raise ValueError("protected rollout file ACL is unavailable")
    return value


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


def safe_content_fingerprint(payload: bytes) -> str:
    """Return the only credential-content representation allowed in evidence."""
    return f"sha256:{hashlib.sha256(payload).hexdigest()[:12]} len={len(payload)}"


def _acl_payload(fd: int) -> bytes:
    try:
        payload = _get_acl_xattr(fd, _POSIX_ACL_XATTR)
    except OSError as exc:
        absent = {errno.ENODATA, errno.ENOTSUP}
        enoattr = getattr(errno, "ENOATTR", None)
        if enoattr is not None:
            absent.add(enoattr)
        if exc.errno in absent:
            return b""
        raise ValueError("protected rollout file ACL is unavailable") from exc
    if len(payload) > 4 + 8 * _MAX_ACL_ENTRIES:
        raise ValueError("protected rollout file ACL is unsafe")
    return payload


def converge_new_private_file(fd: int, *, service_uid: int) -> None:
    """Converge a newly-created private file before publishing any payload."""
    if fd < 0 or service_uid < 0:
        raise ValueError("private rollout file authority is invalid")
    before = os.fstat(fd)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != service_uid
        or before.st_nlink != 1
        or before.st_size != 0
    ):
        raise ValueError("private rollout file metadata is unsafe")
    try:
        os.fchmod(fd, 0o600)
        acl_before = _acl_payload(fd)
        if acl_before:
            remover = getattr(os, "removexattr", None)
            if remover is None:
                raise ValueError("private rollout file ACL cannot be converged")
            try:
                remover(fd, _POSIX_ACL_XATTR)
            except OSError as exc:
                absent = {errno.ENODATA}
                enoattr = getattr(errno, "ENOATTR", None)
                if enoattr is not None:
                    absent.add(enoattr)
                if exc.errno not in absent:
                    raise ValueError("private rollout file ACL cannot be converged") from exc
        after = os.fstat(fd)
        if (
            after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or not stat.S_ISREG(after.st_mode)
            or after.st_uid != service_uid
            or stat.S_IMODE(after.st_mode) != 0o600
            or after.st_nlink != 1
            or after.st_size != 0
            or _acl_payload(fd)
        ):
            raise ValueError("private rollout file ACL convergence is unsafe")
    except OSError as exc:
        raise ValueError("private rollout file ACL convergence failed") from exc


def _service_gid(service_uid: int) -> int:
    try:
        return pwd.getpwuid(service_uid).pw_gid
    except (KeyError, OSError) as exc:
        raise ValueError("protected rollout service identity is unavailable") from exc


def _parse_posix_acl(payload: bytes) -> tuple[tuple[int, int, int], ...]:
    if not payload:
        return ()
    if len(payload) < 4 or (len(payload) - 4) % 8 != 0:
        raise ValueError("protected rollout file ACL is unsafe")
    version = struct.unpack_from("<I", payload)[0]
    if version != _POSIX_ACL_VERSION:
        raise ValueError("protected rollout file ACL is unsafe")
    entries = tuple(
        struct.unpack_from("<HHI", payload, offset) for offset in range(4, len(payload), 8)
    )
    if not entries or len(entries) > _MAX_ACL_ENTRIES:
        raise ValueError("protected rollout file ACL is unsafe")
    return entries


def _validate_private_acl(
    payload: bytes,
    *,
    metadata: os.stat_result,
    service_uid: int,
) -> None:
    service_gid = _service_gid(service_uid)
    entries = _parse_posix_acl(payload)
    if not entries:
        service_can_read = metadata.st_uid == service_uid or (
            metadata.st_gid == service_gid and bool(stat.S_IMODE(metadata.st_mode) & stat.S_IRGRP)
        )
        if not service_can_read:
            raise ValueError("protected rollout file ACL does not grant the service reader")
        return

    by_tag: dict[int, list[tuple[int, int]]] = {}
    for tag, permissions, identifier in entries:
        if (
            tag
            not in {
                _ACL_USER_OBJ,
                _ACL_USER,
                _ACL_GROUP_OBJ,
                _ACL_GROUP,
                _ACL_MASK,
                _ACL_OTHER,
            }
            or permissions & ~0x7
        ):
            raise ValueError("protected rollout file ACL is unsafe")
        by_tag.setdefault(tag, []).append((permissions, identifier))
    for tag in (_ACL_USER_OBJ, _ACL_GROUP_OBJ, _ACL_OTHER):
        values = by_tag.get(tag, [])
        if len(values) != 1 or values[0][1] != _ACL_UNDEFINED_ID:
            raise ValueError("protected rollout file ACL is unsafe")
    masks = by_tag.get(_ACL_MASK, [])
    named_users = by_tag.get(_ACL_USER, [])
    named_groups = by_tag.get(_ACL_GROUP, [])
    if len(masks) > 1 or named_groups:
        raise ValueError("protected rollout file ACL is unsafe")
    if any(identifier != service_uid for _permissions, identifier in named_users):
        raise ValueError("protected rollout file ACL has an undeclared reader")
    if len(named_users) > 1:
        raise ValueError("protected rollout file ACL is unsafe")
    owner_permissions = by_tag[_ACL_USER_OBJ][0][0]
    group_permissions = by_tag[_ACL_GROUP_OBJ][0][0]
    other_permissions = by_tag[_ACL_OTHER][0][0]
    mask_permissions = masks[0][0] if masks else 0x7
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        owner_permissions != (mode >> 6) & 0x7
        or other_permissions != mode & 0x7
        or other_permissions != 0
        or mask_permissions & 0x3
        or group_permissions & 0x3
        or any(permissions & 0x3 for permissions, _identifier in named_users)
    ):
        raise ValueError("protected rollout file ACL permissions are unsafe")
    if masks:
        if mask_permissions != (mode >> 3) & 0x7:
            raise ValueError("protected rollout file ACL mask is inconsistent")
    elif group_permissions != (mode >> 3) & 0x7:
        raise ValueError("protected rollout file ACL group is inconsistent")
    if metadata.st_uid == service_uid:
        service_permissions = owner_permissions
    elif named_users:
        service_permissions = named_users[0][0] & mask_permissions
    elif metadata.st_gid == service_gid:
        service_permissions = group_permissions & mask_permissions
    else:
        service_permissions = 0
    if service_permissions & 0x4 == 0:
        raise ValueError("protected rollout file ACL does not grant the service reader")


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
        acl_before = _acl_payload(fd)
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
        acl_after = _acl_payload(fd)
        if (
            len(payload) != before.st_size
            or _identity(before) != _identity(after)
            or acl_after != acl_before
        ):
            raise ValueError("protected rollout file changed while it was read")
        if private:
            _validate_private_acl(
                acl_after,
                metadata=after,
                service_uid=service_uid,
            )
        metadata_payload = ":".join(str(value) for value in _identity(before)).encode()
        return TrustedFileRead(
            payload=bytes(payload),
            metadata=before,
            metadata_fingerprint=hashlib.sha256(metadata_payload).hexdigest(),
            acl_fingerprint=hashlib.sha256(acl_after).hexdigest(),
        )
    finally:
        os.close(fd)


__all__ = [
    "TrustedFileRead",
    "converge_new_private_file",
    "read_trusted_file",
    "safe_content_fingerprint",
]
