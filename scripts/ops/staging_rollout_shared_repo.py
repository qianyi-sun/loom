#!/usr/bin/python3
"""Converge the fixed shared GB10 checkout authority without path races.

The qianyi account is the user-authorized release operator.  This helper
protects against accidental drift, concurrent replacement, and non-operator
accounts; it does not claim to defend against a malicious process running as
that trusted operator.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import grp
import json
import os
import pwd
import secrets
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

SERVICE_USER = "loom-rollout"
SERVICE_GROUP = "loom-rollout"
CONSUMER_USER = "qianyi"
SHARED_GROUP = "sharedwork"
CONSUMER_PARENT = Path("/shared_work/qianyi")
AUTHORITY_ROOT = CONSUMER_PARENT / ".loom-staging-rollout"
REPOSITORY_ROOT = AUTHORITY_ROOT / "worker-repos"
_DIRECTORY_FLAGS = (
    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class AuthorityError(RuntimeError):
    """A bounded failure safe for installer output."""


@dataclass(frozen=True, slots=True)
class Identity:
    name: str
    uid: int
    gid: int
    groups: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class BoundDirectory:
    path: Path
    fd: int
    device: int
    inode: int

    def assert_stable(self) -> os.stat_result:
        current = os.fstat(self.fd)
        try:
            lexical = self.path.stat(follow_symlinks=False)
        except OSError as exc:
            raise AuthorityError("shared repository authority changed") from exc
        expected = (self.device, self.inode)
        if (
            not stat.S_ISDIR(current.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISDIR(lexical.st_mode)
            or (current.st_dev, current.st_ino) != expected
            or (lexical.st_dev, lexical.st_ino) != expected
        ):
            raise AuthorityError("shared repository authority changed")
        return current


def _identity(username: str, required_primary_group: str | None = None) -> Identity:
    try:
        user = pwd.getpwnam(username)
        groups = tuple(sorted(set(os.getgrouplist(username, user.pw_gid))))
    except (KeyError, OSError) as exc:
        raise AuthorityError("shared repository identity is unavailable") from exc
    if user.pw_uid == 0 or user.pw_gid == 0:
        raise AuthorityError("shared repository identity is inconsistent")
    if required_primary_group is not None:
        try:
            required_gid = grp.getgrnam(required_primary_group).gr_gid
        except KeyError as exc:
            raise AuthorityError("shared repository identity is unavailable") from exc
        if user.pw_gid != required_gid:
            raise AuthorityError("shared repository identity is inconsistent")
    return Identity(username, user.pw_uid, user.pw_gid, groups)


def _open_absolute(path: Path) -> BoundDirectory:
    if not path.is_absolute() or ".." in path.parts:
        raise AuthorityError("shared repository authority path is invalid")
    fd = os.open("/", _DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:]:
            next_fd = os.open(component, _DIRECTORY_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        metadata = os.fstat(fd)
        bound = BoundDirectory(path, fd, metadata.st_dev, metadata.st_ino)
        bound.assert_stable()
        return bound
    except Exception:
        os.close(fd)
        raise


def _open_child(parent: BoundDirectory, name: str) -> BoundDirectory:
    parent.assert_stable()
    fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent.fd)
    metadata = os.fstat(fd)
    child = BoundDirectory(parent.path / name, fd, metadata.st_dev, metadata.st_ino)
    child.assert_stable()
    return child


def _validate_directory(
    directory: BoundDirectory,
    *,
    uid: int,
    gid: int,
    mode: int,
) -> os.stat_result:
    metadata = directory.assert_stable()
    if metadata.st_uid != uid or metadata.st_gid != gid or stat.S_IMODE(metadata.st_mode) != mode:
        raise AuthorityError("shared repository authority metadata is invalid")
    return metadata


def _ensure_child(
    parent: BoundDirectory,
    name: str,
    *,
    uid: int,
    gid: int,
    mode: int,
) -> tuple[BoundDirectory, bool]:
    parent.assert_stable()
    try:
        child = _open_child(parent, name)
    except FileNotFoundError:
        temp_name = f".{name}.tmp-{secrets.token_hex(16)}"
        temp: BoundDirectory | None = None
        try:
            os.mkdir(temp_name, 0o700, dir_fd=parent.fd)
            temp = _open_child(parent, temp_name)
            os.fchown(temp.fd, uid, gid)
            os.fchmod(temp.fd, mode)
            _validate_directory(temp, uid=uid, gid=gid, mode=mode)
            try:
                _rename_noreplace(parent.fd, temp_name, parent.fd, name)
            except FileExistsError:
                os.close(temp.fd)
                temp = None
                os.rmdir(temp_name, dir_fd=parent.fd)
                child = _open_child(parent, name)
                _validate_directory(child, uid=uid, gid=gid, mode=mode)
                return child, False
            child = BoundDirectory(parent.path / name, temp.fd, temp.device, temp.inode)
            temp = None
            _validate_directory(child, uid=uid, gid=gid, mode=mode)
            parent.assert_stable()
            return child, True
        except Exception:
            if temp is not None:
                os.close(temp.fd)
            try:
                os.rmdir(temp_name, dir_fd=parent.fd)
            except FileNotFoundError:
                pass
            raise
    _validate_directory(child, uid=uid, gid=gid, mode=mode)
    parent.assert_stable()
    return child, False


def _rename_noreplace(
    source_fd: int,
    source_name: str,
    destination_fd: int,
    destination_name: str,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rename = libc.renameat2
    except AttributeError as exc:
        raise AuthorityError("atomic shared repository publication is unavailable") from exc
    rename.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    rename.restype = ctypes.c_int
    result = rename(
        source_fd,
        os.fsencode(source_name),
        destination_fd,
        os.fsencode(destination_name),
        1,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, "destination exists")
    raise AuthorityError("atomic shared repository publication failed safely")


def _probe_identity(identity: Identity, checks: tuple[tuple[int, int, bool], ...]) -> bool:
    read_fd, write_fd = os.pipe()
    child = os.fork()
    if child == 0:  # pragma: no cover - result is asserted by the parent
        try:
            os.close(read_fd)
            os.setgroups(list(identity.groups))
            os.setgid(identity.gid)
            os.setuid(identity.uid)
            passed = all(
                os.access(
                    ".",
                    access_mode,
                    dir_fd=fd,
                    effective_ids=True,
                )
                is expected
                for fd, access_mode, expected in checks
            )
            os.write(write_fd, b"1" if passed else b"0")
        except BaseException:
            try:
                os.write(write_fd, b"0")
            except OSError:
                pass
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        payload = os.read(read_fd, 2)
    finally:
        os.close(read_fd)
    _, status = os.waitpid(child, 0)
    return status == 0 and payload == b"1"


def converge(*, ensure: bool) -> dict[str, object]:
    if os.geteuid() != 0:
        raise AuthorityError("shared repository helper requires root")
    service = _identity(SERVICE_USER, SERVICE_GROUP)
    consumer = _identity(CONSUMER_USER)
    try:
        shared_gid = grp.getgrnam(SHARED_GROUP).gr_gid
    except KeyError as exc:
        raise AuthorityError("shared repository identity is unavailable") from exc
    if shared_gid not in consumer.groups or shared_gid in service.groups:
        raise AuthorityError("shared repository group membership is invalid")

    parent = _open_absolute(CONSUMER_PARENT)
    authority: BoundDirectory | None = None
    repository: BoundDirectory | None = None
    created: list[str] = []
    try:
        parent_metadata = _validate_directory(
            parent,
            uid=consumer.uid,
            gid=shared_gid,
            mode=0o2775,
        )
        if ensure:
            authority, authority_created = _ensure_child(
                parent,
                AUTHORITY_ROOT.name,
                uid=service.uid,
                gid=shared_gid,
                mode=0o2750,
            )
            repository, repository_created = _ensure_child(
                authority,
                REPOSITORY_ROOT.name,
                uid=service.uid,
                gid=shared_gid,
                mode=0o2750,
            )
            if authority_created:
                created.append("authority-root")
            if repository_created:
                created.append("repository-root")
        else:
            authority = _open_child(parent, AUTHORITY_ROOT.name)
            repository = _open_child(authority, REPOSITORY_ROOT.name)
            _validate_directory(authority, uid=service.uid, gid=shared_gid, mode=0o2750)
            _validate_directory(repository, uid=service.uid, gid=shared_gid, mode=0o2750)

        if authority is None or repository is None:  # pragma: no cover - invariant
            raise AuthorityError("shared repository authority is unavailable")
        authority_metadata = _validate_directory(
            authority,
            uid=service.uid,
            gid=shared_gid,
            mode=0o2750,
        )
        repository_metadata = _validate_directory(
            repository,
            uid=service.uid,
            gid=shared_gid,
            mode=0o2750,
        )
        service_ok = _probe_identity(
            service,
            (
                (parent.fd, os.W_OK, False),
                (repository.fd, os.W_OK | os.X_OK, True),
            ),
        )
        consumer_ok = _probe_identity(
            consumer,
            (
                (repository.fd, os.R_OK | os.X_OK, True),
                (repository.fd, os.W_OK, False),
            ),
        )
        if not service_ok or not consumer_ok:
            raise AuthorityError("shared repository capability contract is invalid")
        return {
            "schema_version": 1,
            "root": str(REPOSITORY_ROOT),
            "service_user": SERVICE_USER,
            "service_uid": service.uid,
            "service_primary_group": SERVICE_GROUP,
            "service_primary_gid": service.gid,
            "consumer_user": CONSUMER_USER,
            "consumer_uid": consumer.uid,
            "shared_group": SHARED_GROUP,
            "shared_gid": shared_gid,
            "parent_mode": "2775",
            "authority_mode": "2750",
            "repository_mode": "2750",
            "parent_device": parent_metadata.st_dev,
            "parent_inode": parent_metadata.st_ino,
            "authority_device": authority_metadata.st_dev,
            "authority_inode": authority_metadata.st_ino,
            "repository_device": repository_metadata.st_dev,
            "repository_inode": repository_metadata.st_ino,
            "service_capability": "parent-not-writable;repository-writable-searchable",
            "consumer_capability": "repository-readable-searchable-not-writable",
            "created": created,
        }
    finally:
        if repository is not None:
            os.close(repository.fd)
        if authority is not None:
            os.close(authority.fd)
        os.close(parent.fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "ensure"))
    args = parser.parse_args(argv)
    try:
        report = converge(ensure=args.command == "ensure")
    except FileNotFoundError:
        print("error: shared repository authority is not installed", file=sys.stderr)
        return 2
    except (AuthorityError, OSError):
        print("error: shared repository authority check failed safely", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
