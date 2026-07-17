#!/usr/bin/python3
"""Converge the fixed shared GB10 checkout authority without path races.

The qianyi account is the user-authorized release operator.  This helper
protects against accidental drift, concurrent replacement, and non-operator
accounts; it does not claim to defend against a malicious process running as
that trusted operator.
"""

from __future__ import annotations

import argparse
import grp
import json
import os
import pwd
import re
import secrets
import stat
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

try:
    from scripts.ops.staging_rollout_shared_work2 import MountError, mount_identity
except ModuleNotFoundError:  # installed helper executes directly from scripts/ops
    from staging_rollout_shared_work2 import MountError, mount_identity

SERVICE_USER = "loom-rollout"
SERVICE_GROUP = "loom-rollout"
CONSUMER_USER = "qianyi"
SHARED_GROUP = "sharedwork"
CONSUMER_PARENT = Path("/shared_work2/qianyi")
AUTHORITY_ROOT = CONSUMER_PARENT / ".loom-staging-rollout"
REPOSITORY_ROOT = AUTHORITY_ROOT / "worker-repos"

# Service-owned candidate tree (#874): the rollout service owns the root and
# every level below it; the shared group (workers) gets read+traverse only.
# The root itself (e.g. /shared_work/loom) is operator-provisioned; this helper
# validates it is service-owned and ensures the candidate chain beneath it.
SERVICE_DIR_MODE = 0o2750

# The privileged service CLI is deliberately narrow (#874): it only ever
# operates on candidates/<environment>/<sha> beneath a hardcoded, service-owned
# root. It never accepts an arbitrary root or arbitrary path components, so it
# cannot become a generic root-owned directory creator once wired through the
# broker/sudoers.
SERVICE_ROOT = Path("/shared_work/loom")
ALLOWED_ENVIRONMENTS = ("development", "staging", "production")
_CANDIDATE_SHA_RE = re.compile(r"[0-9a-f]{40}")
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
        try:
            os.mkdir(name, 0o700, dir_fd=parent.fd)
        except FileExistsError:
            child = _open_child(parent, name)
            try:
                _validate_directory(child, uid=uid, gid=gid, mode=mode)
                parent.assert_stable()
                return child, False
            except Exception:
                os.close(child.fd)
                raise
        created = os.stat(name, dir_fd=parent.fd, follow_symlinks=False)
        child: BoundDirectory | None = None
        try:
            child = _open_child(parent, name)
            if (child.device, child.inode) != (created.st_dev, created.st_ino):
                raise AuthorityError("shared repository authority changed")
            os.fchown(child.fd, uid, gid)
            os.fchmod(child.fd, mode)
            _validate_directory(child, uid=uid, gid=gid, mode=mode)
            parent.assert_stable()
            return child, True
        except Exception:
            try:
                lexical = os.stat(name, dir_fd=parent.fd, follow_symlinks=False)
                if (lexical.st_dev, lexical.st_ino) == (created.st_dev, created.st_ino):
                    os.rmdir(name, dir_fd=parent.fd)
            finally:
                if child is not None:
                    os.close(child.fd)
            raise
    try:
        _validate_directory(child, uid=uid, gid=gid, mode=mode)
        parent.assert_stable()
        return child, False
    except Exception:
        os.close(child.fd)
        raise


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


def _run_as_identity(identity: Identity, operation: Callable[[], bool]) -> bool:
    read_fd, write_fd = os.pipe()
    child = os.fork()
    if child == 0:  # pragma: no cover - result is asserted by the parent
        try:
            os.close(read_fd)
            os.setgroups(list(identity.groups))
            os.setgid(identity.gid)
            os.setuid(identity.uid)
            os.write(write_fd, b"1" if operation() else b"0")
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


def _probe_private_publication(
    service: Identity,
    consumer: Identity,
    repository: BoundDirectory,
    shared_gid: int,
) -> bool:
    name = f".publication-probe-{secrets.token_hex(16)}"
    child: BoundDirectory | None = None
    try:
        if not _run_as_identity(
            service,
            lambda: _create_private_directory(repository.fd, name),
        ):
            return False
        child = _open_child(repository, name)
        private = _validate_directory(child, uid=service.uid, gid=shared_gid, mode=0o2700)
        if not _probe_identity(
            service,
            ((child.fd, os.W_OK | os.X_OK, True),),
        ) or not _probe_identity(
            consumer,
            ((child.fd, os.R_OK | os.X_OK, False),),
        ):
            return False
        if not _run_as_identity(
            service,
            lambda: _publish_private_directory(repository.fd, name),
        ):
            return False
        published = _validate_directory(child, uid=service.uid, gid=shared_gid, mode=0o750)
        if (published.st_dev, published.st_ino) != (private.st_dev, private.st_ino):
            return False
        if not _probe_identity(
            consumer,
            (
                (child.fd, os.R_OK | os.X_OK, True),
                (child.fd, os.W_OK, False),
            ),
        ):
            return False
        if not _run_as_identity(
            service,
            lambda: _collision_is_rejected(repository.fd, name),
        ):
            return False
        after_collision = child.assert_stable()
        return (after_collision.st_dev, after_collision.st_ino) == (
            published.st_dev,
            published.st_ino,
        )
    finally:
        if child is not None:
            try:
                lexical = os.stat(name, dir_fd=repository.fd, follow_symlinks=False)
                current = os.fstat(child.fd)
                if (lexical.st_dev, lexical.st_ino) == (current.st_dev, current.st_ino):
                    os.rmdir(name, dir_fd=repository.fd)
            finally:
                os.close(child.fd)


def _create_private_directory(parent_fd: int, name: str) -> bool:
    os.mkdir(name, 0o700, dir_fd=parent_fd)
    fd: int | None = None
    try:
        fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        if stat.S_IMODE(os.fstat(fd).st_mode) == 0o2700:
            return True
        os.rmdir(name, dir_fd=parent_fd)
        return False
    except Exception:
        try:
            os.rmdir(name, dir_fd=parent_fd)
        except OSError:
            pass
        raise
    finally:
        if fd is not None:
            os.close(fd)


def _publish_private_directory(parent_fd: int, name: str) -> bool:
    fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    try:
        metadata = os.fstat(fd)
        if stat.S_IMODE(metadata.st_mode) != 0o2700:
            return False
        os.fchmod(fd, 0o750)
        return stat.S_IMODE(os.fstat(fd).st_mode) == 0o750
    finally:
        os.close(fd)


def _collision_is_rejected(parent_fd: int, name: str) -> bool:
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        return True
    return False


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

    try:
        mount_report = mount_identity()
    except MountError as exc:
        raise AuthorityError("shared repository mount identity is invalid") from exc
    mount = _open_absolute(CONSUMER_PARENT.parent)
    parent: BoundDirectory | None = None
    authority: BoundDirectory | None = None
    repository: BoundDirectory | None = None
    created: list[str] = []
    try:
        if ensure:
            parent, parent_created = _ensure_child(
                mount,
                CONSUMER_PARENT.name,
                uid=consumer.uid,
                gid=shared_gid,
                mode=0o2775,
            )
            if parent_created:
                created.append("consumer-parent")
        else:
            parent = _open_child(mount, CONSUMER_PARENT.name)
        if parent is None:  # pragma: no cover - invariant
            raise AuthorityError("shared repository consumer parent is unavailable")
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
        if not _probe_private_publication(service, consumer, repository, shared_gid):
            raise AuthorityError("shared repository atomic publication contract is invalid")
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
            "publication_capability": "private-mkdir-publish-verified",
            "mount": mount_report,
            "created": created,
        }
    finally:
        if repository is not None:
            os.close(repository.fd)
        if authority is not None:
            os.close(authority.fd)
        if parent is not None:
            os.close(parent.fd)
        os.close(mount.fd)


def _converge_service_owned(
    root: Path,
    relative_chain: tuple[str, ...],
    *,
    ensure: bool,
) -> dict[str, object]:
    """Converge a service-owned candidate tree under an operator-provisioned root.

    Unlike converge() (which validates a *consumer*-owned parent), the root here
    is owned by the rollout service and so is every level beneath it. Workers
    (the shared group) get read+traverse only, never write -- that is the
    immutability guarantee for published candidates (#874,
    /shared_work/loom/candidates/<env>/...). The root is provisioned once by an
    operator; this helper validates it is service-owned and ensures the chain.
    """
    if os.geteuid() != 0:
        raise AuthorityError("shared repository helper requires root")
    if not relative_chain or any(
        part in ("", ".", "..") or "/" in part for part in relative_chain
    ):
        raise AuthorityError("shared repository service path is invalid")
    service = _identity(SERVICE_USER, SERVICE_GROUP)
    consumer = _identity(CONSUMER_USER)
    try:
        shared_gid = grp.getgrnam(SHARED_GROUP).gr_gid
    except KeyError as exc:
        raise AuthorityError("shared repository identity is unavailable") from exc
    if shared_gid not in consumer.groups or shared_gid in service.groups:
        raise AuthorityError("shared repository group membership is invalid")

    opened: list[BoundDirectory] = []
    created: list[str] = []
    try:
        root_dir = _open_absolute(root)
        opened.append(root_dir)
        # Root is operator-provisioned and must already be service-owned.
        root_metadata = _validate_directory(
            root_dir, uid=service.uid, gid=shared_gid, mode=SERVICE_DIR_MODE,
        )
        current = root_dir
        for name in relative_chain:
            if ensure:
                child, was_created = _ensure_child(
                    current, name, uid=service.uid, gid=shared_gid, mode=SERVICE_DIR_MODE,
                )
                if was_created:
                    created.append(name)
            else:
                child = _open_child(current, name)
            _validate_directory(child, uid=service.uid, gid=shared_gid, mode=SERVICE_DIR_MODE)
            opened.append(child)
            current = child
        repository = current
        repository_metadata = repository.assert_stable()
        # Service owns the whole tree (writes root + repo); the shared group
        # (workers) may read+traverse but never write -- immutability.
        service_ok = _probe_identity(
            service,
            (
                (root_dir.fd, os.W_OK | os.X_OK, True),
                (repository.fd, os.W_OK | os.X_OK, True),
            ),
        )
        consumer_ok = _probe_identity(
            consumer,
            (
                (root_dir.fd, os.R_OK | os.X_OK, True),
                (root_dir.fd, os.W_OK, False),
                (repository.fd, os.R_OK | os.X_OK, True),
                (repository.fd, os.W_OK, False),
            ),
        )
        if not service_ok or not consumer_ok:
            raise AuthorityError("shared repository capability contract is invalid")
        return {
            "schema_version": 1,
            "model": "service-owned",
            "root": str(root),
            "repository": str(repository.path),
            "relative_chain": list(relative_chain),
            "service_user": SERVICE_USER,
            "service_uid": service.uid,
            "consumer_user": CONSUMER_USER,
            "consumer_uid": consumer.uid,
            "shared_group": SHARED_GROUP,
            "shared_gid": shared_gid,
            "root_mode": "2750",
            "repository_mode": "2750",
            "root_device": root_metadata.st_dev,
            "root_inode": root_metadata.st_ino,
            "repository_device": repository_metadata.st_dev,
            "repository_inode": repository_metadata.st_ino,
            "service_capability": "root-writable;repository-writable-searchable",
            "consumer_capability": "root-readable;repository-readable-searchable-not-writable",
            "created": created,
        }
    finally:
        for directory in reversed(opened):
            os.close(directory.fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "ensure", "service-check", "service-ensure"))
    # The privileged CLI accepts ONLY an environment + candidate SHA; the root
    # and the candidates/<env>/<sha> layout are hardcoded so it can never be
    # turned into a generic root-owned directory creator (#874).
    parser.add_argument("--environment", choices=ALLOWED_ENVIRONMENTS)
    parser.add_argument("--candidate-sha", help="Full 40-character hex candidate commit.")
    args = parser.parse_args(argv)
    try:
        if args.command in ("service-check", "service-ensure"):
            if args.environment is None or args.candidate_sha is None:
                print(
                    "error: service commands require --environment and --candidate-sha",
                    file=sys.stderr,
                )
                return 2
            if _CANDIDATE_SHA_RE.fullmatch(args.candidate_sha) is None:
                print(
                    "error: --candidate-sha must be a full 40-character hex commit",
                    file=sys.stderr,
                )
                return 2
            # Privileged setup ensures ONLY the per-env parent
            # (candidates/<environment>). The final <sha> directory is
            # deliberately NOT created here: the publisher/materializer builds
            # the complete candidate in a private temporary tree and atomically
            # rename-no-replaces it into candidates/<environment>/<sha>. Pre-
            # creating an empty <sha> would block that rename or expose a
            # partial candidate.
            report = _converge_service_owned(
                SERVICE_ROOT,
                ("candidates", args.environment),
                ensure=args.command == "service-ensure",
            )
            # Report the derived publish target for the materializer; do not
            # create it.
            report["candidate_target"] = str(
                SERVICE_ROOT / "candidates" / args.environment / args.candidate_sha,
            )
        else:
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
