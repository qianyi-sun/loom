#!/usr/bin/env python3
"""Install and converge the three persistent oldlab-2 developer sandboxes.

All public mutation commands are plan-only unless ``--execute`` is supplied.
The installed systemd entry point has no repository, path, host, or secret
overrides. Secret values are generated once, written atomically, and never
included in command output.
"""

from __future__ import annotations

import argparse
import base64
import errno
import fcntl
import grp
import hashlib
import io
import json
import os
import pwd
import re
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PROFILES = REPO_ROOT / "deploy/developer-sandboxes"
SOURCE_UNIT = SOURCE_PROFILES / "loom-developer-sandbox@.service"

SANDBOXES = ("qianyi", "hongjian", "devansh")
EXPECTED_HOSTNAME = "trt-eai-oldlab-2"
SHARED_GROUP = "sharedwork"
NFS_ROOT = Path("/shared_work/loom/candidates/sandboxes")
NFS_RUNTIME_ROOT = Path("/shared_work/loom/runtime/sandboxes")
STATE_PARENT = Path("/srv/loom/developer-sandboxes")
CONFIG_ROOT = Path("/etc/loom/developer-sandboxes")
DESIRED_ROOT = CONFIG_ROOT / "desired"
PROFILE_CONFIG_ROOT = CONFIG_ROOT / "profiles"
TRANSACTION_ROOT = Path("/var/lib/loom-developer-sandbox-installer/transactions")
TRANSACTION_LOCK_ROOT = Path("/run/loom-developer-sandbox-installer")
SOURCE_STAGING_ROOT = Path("/var/lib/loom-developer-sandbox-installer/source")
RENEWAL_STATE_ROOT = Path("/var/lib/loom-developer-sandbox-installer/renewals")
COMBINED_RECEIPT_ROOT = Path("/var/lib/loom-shared-capacity/runtime-attestations")
FLEET_ATTESTATION_ROOT = Path("/var/lib/loom-developer-sandbox-links/attestations")
REMOTE_LINK_ISSUANCE_ROOT = Path("/var/lib/loom/developer-sandbox-links/issuance")
REMOTE_LINK_SERVER_ROOT = Path("/etc/loom/developer-sandbox-links/server")
DOMAIN_RUNTIME_PROGRAM = Path("/usr/local/libexec/loom-developer-domain-runtime")
DOMAIN_RUNTIME_CONFIG = Path("/etc/loom/developer-runtime-domains.toml")
UNIT_PATH = Path("/etc/systemd/system/loom-developer-sandbox@.service")
RENEWAL_SERVICE_PATH = Path(
    "/etc/systemd/system/loom-developer-sandbox-attestation-renewal.service",
)
RENEWAL_TIMER_PATH = Path(
    "/etc/systemd/system/loom-developer-sandbox-attestation-renewal.timer",
)
INSTALLED_PROGRAM = Path("/usr/local/libexec/loom-developer-sandbox-host")
UNIT_NAME = "loom-developer-sandbox@{sandbox}.service"
RENEWAL_TIMER = "loom-developer-sandbox-attestation-renewal.timer"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
RENEWAL_HISTORY_RE = re.compile(r"^([0-9]{20})-([0-9a-f]{64})\.json$")
TRANSACTION_TTL = timedelta(minutes=30)
RECEIPT_FRESHNESS = timedelta(seconds=60)
ATTESTATION_TTL = timedelta(minutes=15)
DOMAIN_PEERS = {
    "oldlab": ("oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4", "oldlab-5"),
    "gb10": tuple(
        f"trt-gb10-{index}"
        for index in (1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15)
    ),
}
DOMAIN_PUBLISHERS = {"oldlab": "oldlab-1", "gb10": "trt-gb10-1"}
ELIGIBLE_LINK_NODES = DOMAIN_PEERS["oldlab"] + DOMAIN_PEERS["gb10"]

SECRET_KEYS = (
    "LOOM_DEV_POSTGRES_USER",
    "LOOM_DEV_POSTGRES_PASSWORD",
    "LOOM_DEV_MINIO_ROOT_USER",
    "LOOM_DEV_MINIO_ROOT_PASSWORD",
    "LOOM_CP_STEP_JWT_SIGNING_KEY",
    "LOOM_SECRET_STORE_MASTER_KEY",
    "LOOM_WORKER_TOKEN",
)


class HostConvergeError(RuntimeError):
    """A secret-safe host convergence failure."""


@dataclass(frozen=True, slots=True)
class Profile:
    sandbox: str
    compose_project: str
    canonical_hostname: str
    candidate_root: Path
    state_root: Path
    cache_root: Path
    evidence_root: Path
    runtime_root: Path
    ports: dict[str, int]

    @property
    def secrets_root(self) -> Path:
        return self.state_root / "secrets"

    @property
    def secrets_env(self) -> Path:
        return self.secrets_root / "sandbox.env"

    @property
    def admin_secret(self) -> Path:
        return self.secrets_root / "admin.toml"

    @property
    def state_file(self) -> Path:
        return self.state_root / "sandbox-state.json"

    @property
    def desired_file(self) -> Path:
        return DESIRED_ROOT / f"{self.sandbox}.json"

    def worker_runtime_env(self, sha: str) -> Path:
        return NFS_RUNTIME_ROOT / self.sandbox / sha / "worker-oldlab.env"


@dataclass(frozen=True, slots=True)
class Identity:
    user: str
    group: str
    uid: int
    gid: int


@dataclass(frozen=True, slots=True)
class ActivationReceipt:
    path: Path
    payload_sha256: str
    fleet_payload_sha256: str
    expires_at: datetime


def _load_profile(path: Path) -> Profile:
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError(f"could not load profile {path}") from exc
    sandbox = raw.get("sandbox")
    if sandbox not in SANDBOXES or path.stem != sandbox:
        raise HostConvergeError(f"invalid sandbox profile identity: {path}")
    ports = raw.get("ports")
    if not isinstance(ports, dict) or not ports:
        raise HostConvergeError(f"profile ports are invalid: {path}")
    parsed_ports: dict[str, int] = {}
    for name, value in ports.items():
        if not isinstance(name, str) or type(value) is not int or not 1 <= value <= 65535:
            raise HostConvergeError(f"profile ports are invalid: {path}")
        parsed_ports[name] = value
    profile = Profile(
        sandbox=sandbox,
        compose_project=str(raw.get("compose_project", "")),
        canonical_hostname=str(raw.get("canonical_hostname", "")),
        candidate_root=Path(str(raw.get("candidate_root", ""))),
        state_root=Path(str(raw.get("state_root", ""))),
        cache_root=Path(str(raw.get("cache_root", ""))),
        evidence_root=Path(str(raw.get("evidence_root", ""))),
        runtime_root=Path(str(raw.get("runtime_root", ""))),
        ports=parsed_ports,
    )
    if profile.compose_project != f"loom-sandbox-{sandbox}":
        raise HostConvergeError(f"invalid Compose project in {path}")
    if profile.canonical_hostname != EXPECTED_HOSTNAME:
        raise HostConvergeError(f"invalid host binding in {path}")
    if profile.candidate_root != NFS_ROOT / sandbox:
        raise HostConvergeError(f"invalid candidate root in {path}")
    expected_state = STATE_PARENT / sandbox
    if profile.state_root != expected_state:
        raise HostConvergeError(f"invalid state root in {path}")
    expected_children = {
        profile.cache_root: expected_state / "cache",
        profile.evidence_root: expected_state / "evidence",
        profile.runtime_root: expected_state / "runtime",
    }
    if any(actual != expected for actual, expected in expected_children.items()):
        raise HostConvergeError(f"invalid private roots in {path}")
    return profile


def load_profiles(root: Path = SOURCE_PROFILES) -> tuple[Profile, ...]:
    profiles = tuple(_load_profile(root / f"{sandbox}.toml") for sandbox in SANDBOXES)
    all_ports = [port for profile in profiles for port in profile.ports.values()]
    if len(all_ports) != len(set(all_ports)):
        raise HostConvergeError("sandbox host ports collide")
    for field in ("compose_project", "candidate_root", "state_root"):
        values = [getattr(profile, field) for profile in profiles]
        if len(values) != len(set(values)):
            raise HostConvergeError(f"sandbox {field} values collide")
    return profiles


def _identity(user: str, group: str) -> Identity:
    try:
        account = pwd.getpwnam(user)
        group_row = grp.getgrnam(group)
    except KeyError as exc:
        raise HostConvergeError(f"required host identity is absent: {exc}") from exc
    return Identity(user=user, group=group, uid=account.pw_uid, gid=group_row.gr_gid)


def _run(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    identity: Identity | None = None,
    init_groups: bool = False,
    expected: set[int] | frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    command = list(argv)
    if identity is not None and os.geteuid() != identity.uid:
        prefix = ["runuser", "--user", identity.user]
        if not init_groups:
            prefix.extend(("--group", identity.group))
        child_environment = env or {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
        }
        command = [
            *prefix,
            "--",
            "env",
            "-i",
            *(f"{key}={value}" for key, value in sorted(child_environment.items())),
            *command,
        ]
        env = None
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=dict(env) if env is not None else None,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode not in expected:
        purpose = Path(argv[0]).name if argv else "command"
        raise HostConvergeError(
            f"{purpose} failed safely with exit code {completed.returncode}",
        )
    return completed


def _path_exists_as(path: Path, identity: Identity) -> bool:
    return (
        _run(
            ("test", "-e", str(path)),
            identity=identity,
            expected={0, 1},
        ).returncode
        == 0
    )


def _clean_git_environment() -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
    }


def _atomic_write(
    path: Path,
    content: bytes,
    *,
    mode: int,
    identity: Identity | None = None,
) -> None:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise HostConvergeError("atomic write target path is invalid")
    descriptor = -1
    temporary_name = ""
    try:
        with _seized_directory(path.parent, create=True) as parent_fd:
            try:
                for _attempt in range(16):
                    temporary_name = f".{path.name}.{secrets.token_hex(16)}"
                    try:
                        descriptor = os.open(
                            temporary_name,
                            os.O_WRONLY
                            | os.O_CREAT
                            | os.O_EXCL
                            | getattr(os, "O_CLOEXEC", 0)
                            | os.O_NOFOLLOW,
                            0o600,
                            dir_fd=parent_fd,
                        )
                        break
                    except FileExistsError:
                        continue
                else:
                    raise HostConvergeError(
                        "could not reserve atomic write temporary file",
                    )

                view = memoryview(content)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise HostConvergeError("atomic write made no progress")
                    view = view[written:]
                if identity is not None:
                    os.fchown(descriptor, identity.uid, identity.gid)
                os.fchmod(descriptor, mode)
                os.fsync(descriptor)

                opened = os.fstat(descriptor)
                temporary = os.stat(
                    temporary_name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(temporary.st_mode)
                    or (temporary.st_dev, temporary.st_ino)
                    != (opened.st_dev, opened.st_ino)
                ):
                    raise HostConvergeError("atomic write temporary binding changed")

                _replace_file_at(parent_fd, temporary_name, path.name)
                temporary_name = ""
                rebound = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
                if (
                    not stat.S_ISREG(rebound.st_mode)
                    or (rebound.st_dev, rebound.st_ino)
                    != (opened.st_dev, opened.st_ino)
                ):
                    raise HostConvergeError("atomic write target binding changed")
                os.fsync(parent_fd)
            finally:
                if temporary_name:
                    try:
                        os.unlink(temporary_name, dir_fd=parent_fd)
                        os.fsync(parent_fd)
                    except FileNotFoundError:
                        pass
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError(f"could not atomically write {path.name}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_root_private_directory(path: Path) -> None:
    descriptor = _open_absolute_directory(path, create=True)
    try:
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o700)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
        ):
            raise HostConvergeError(f"root-private directory did not converge: {path}")
    except OSError as exc:
        raise HostConvergeError(f"root-private directory did not converge: {path}") from exc
    finally:
        os.close(descriptor)


def _assert_secure_file(path: Path, identity: Identity, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise HostConvergeError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise HostConvergeError(f"{label} must be a regular non-symlink file")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise HostConvergeError(f"{label} must have mode 0600")
    if (metadata.st_uid, metadata.st_gid) != (identity.uid, identity.gid):
        raise HostConvergeError(f"{label} owner is invalid")


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | os.O_DIRECTORY
        | getattr(os, "O_CLOEXEC", 0)
        | os.O_NOFOLLOW
    )


def _open_absolute_directory(path: Path, *, create: bool) -> int:
    if not path.is_absolute() or ".." in path.parts:
        raise HostConvergeError("private sandbox parent path is invalid")
    descriptor = os.open("/", _directory_open_flags())
    try:
        for component in path.parts[1:]:
            if create:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
            child = os.open(component, _directory_open_flags(), dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


@contextmanager
def _seized_directory(path: Path, *, create: bool) -> Iterator[int]:
    descriptor = -1
    metadata: os.stat_result | None = None
    seized = False
    try:
        descriptor = _open_absolute_directory(path, create=create)
        metadata = os.fstat(descriptor)
        seized = True
        os.fchown(descriptor, os.geteuid(), os.getegid())
        os.fchmod(descriptor, 0o700)
        yield descriptor
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError(f"could not seize directory: {path}") from exc
    finally:
        if descriptor >= 0:
            try:
                if seized and metadata is not None:
                    os.fchown(descriptor, metadata.st_uid, metadata.st_gid)
                    os.fchmod(descriptor, stat.S_IMODE(metadata.st_mode))
            except OSError as exc:
                raise HostConvergeError(
                    f"could not restore directory: {path}",
                ) from exc
            finally:
                os.close(descriptor)


def _replace_file_at(parent_fd: int, source: str, target: str) -> None:
    os.replace(
        source,
        target,
        src_dir_fd=parent_fd,
        dst_dir_fd=parent_fd,
    )


def _mkdir_private_dir_at(parent_fd: int, name: str) -> None:
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    except FileExistsError:
        return


def _private_child_names(profile: Profile) -> tuple[str, ...]:
    children = (
        profile.cache_root,
        profile.evidence_root,
        profile.runtime_root,
        profile.secrets_root,
    )
    if (
        any(path.parent != profile.state_root or path.name in {"", ".", ".."} for path in children)
        or len({path.name for path in children}) != len(children)
    ):
        raise HostConvergeError("private sandbox child paths are invalid")
    return tuple(path.name for path in children)


def _validate_private_directory_fd(
    descriptor: int,
    identity: Identity,
    path: Path,
) -> os.stat_result:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or (metadata.st_uid, metadata.st_gid) != (identity.uid, identity.gid)
    ):
        raise HostConvergeError(f"private sandbox root is invalid: {path}")
    return metadata


@contextmanager
def _private_state_directory(
    profile: Profile,
    identity: Identity,
    *,
    create: bool,
    seize: bool,
) -> Iterator[tuple[int, int, os.stat_result]]:
    parent_fd = -1
    state_fd = -1
    state_metadata: os.stat_result | None = None
    seized = False
    try:
        parent_fd = _open_absolute_directory(profile.state_root.parent, create=create)
        if create:
            _mkdir_private_dir_at(parent_fd, profile.state_root.name)
        state_fd = os.open(
            profile.state_root.name,
            _directory_open_flags(),
            dir_fd=parent_fd,
        )
        state_metadata = os.fstat(state_fd)
        if not stat.S_ISDIR(state_metadata.st_mode):
            raise HostConvergeError(
                f"private sandbox root is unsafe: {profile.state_root}",
            )
        if seize:
            # Temporarily remove the sandbox user's authority over child names.
            # All following mutations use the already-bound descriptor.
            seized = True
            os.fchown(state_fd, os.geteuid(), os.getegid())
            os.fchmod(state_fd, 0o700)
        yield parent_fd, state_fd, state_metadata
        rebound = os.stat(
            profile.state_root.name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        current = os.fstat(state_fd)
        if (
            not stat.S_ISDIR(rebound.st_mode)
            or (rebound.st_dev, rebound.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise HostConvergeError(
                f"private sandbox root binding changed: {profile.state_root}",
            )
    except HostConvergeError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise HostConvergeError(
                f"private sandbox root is unsafe: {profile.state_root}",
            ) from exc
        raise HostConvergeError(
            f"could not access private sandbox root: {profile.state_root}",
        ) from exc
    finally:
        if state_fd >= 0:
            try:
                if seized:
                    os.fchmod(state_fd, 0o700)
                    os.fchown(state_fd, identity.uid, identity.gid)
            except OSError as exc:
                raise HostConvergeError(
                    f"could not restore private sandbox root: {profile.state_root}",
                ) from exc
            finally:
                os.close(state_fd)
        if parent_fd >= 0:
            os.close(parent_fd)


def verify_private_roots(profile: Profile, identity: Identity) -> None:
    child_names = _private_child_names(profile)
    try:
        with _private_state_directory(
            profile,
            identity,
            create=False,
            seize=False,
        ) as (_parent_fd, state_fd, _state_metadata):
            _validate_private_directory_fd(state_fd, identity, profile.state_root)
            for name in child_names:
                descriptor = os.open(name, _directory_open_flags(), dir_fd=state_fd)
                try:
                    _validate_private_directory_fd(
                        descriptor,
                        identity,
                        profile.state_root / name,
                    )
                finally:
                    os.close(descriptor)
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError(
            f"private sandbox root is unavailable: {profile.state_root}",
        ) from exc


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or key in values:
            raise HostConvergeError("sandbox secret env file is malformed")
        values[key] = value
    return values


def _render_env(values: Mapping[str, str]) -> bytes:
    return "".join(f"{key}={values[key]}\n" for key in sorted(values)).encode()


def _new_secret_values(sandbox: str) -> dict[str, str]:
    return {
        "LOOM_DEV_POSTGRES_USER": f"loom_{sandbox}",
        "LOOM_DEV_POSTGRES_PASSWORD": secrets.token_urlsafe(32),
        "LOOM_DEV_MINIO_ROOT_USER": f"loom-{sandbox}",
        "LOOM_DEV_MINIO_ROOT_PASSWORD": secrets.token_urlsafe(32),
        "LOOM_CP_STEP_JWT_SIGNING_KEY": secrets.token_urlsafe(48),
        "LOOM_SECRET_STORE_MASTER_KEY": base64.b64encode(os.urandom(32)).decode(),
        # This bootstrap value is intentionally not authoritative until the
        # local Control Plane mints and persists its hash after first boot.
        "LOOM_WORKER_TOKEN": f"loom_w_{secrets.token_hex(32)}",
        "LOOM_SVC_BATCH_RUNNER_CP_TOKEN": "",
    }


def ensure_private_roots(profile: Profile, identity: Identity) -> None:
    child_names = _private_child_names(profile)
    try:
        with _private_state_directory(
            profile,
            identity,
            create=True,
            seize=True,
        ) as (_parent_fd, state_fd, _state_metadata):
            for name in child_names:
                _mkdir_private_dir_at(state_fd, name)
                descriptor = os.open(name, _directory_open_flags(), dir_fd=state_fd)
                try:
                    metadata = os.fstat(descriptor)
                    if not stat.S_ISDIR(metadata.st_mode):
                        raise HostConvergeError(
                            f"private sandbox root is unsafe: {profile.state_root / name}",
                        )
                    os.fchown(descriptor, identity.uid, identity.gid)
                    os.fchmod(descriptor, 0o700)
                    current = os.fstat(descriptor)
                    rebound = os.stat(name, dir_fd=state_fd, follow_symlinks=False)
                    if (
                        not stat.S_ISDIR(rebound.st_mode)
                        or (current.st_dev, current.st_ino)
                        != (rebound.st_dev, rebound.st_ino)
                    ):
                        raise HostConvergeError(
                            f"private sandbox root binding changed: "
                            f"{profile.state_root / name}",
                        )
                finally:
                    os.close(descriptor)
    except HostConvergeError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise HostConvergeError(
                f"private sandbox root is unsafe: {profile.state_root}",
            ) from exc
        raise HostConvergeError(
            f"could not converge private sandbox root: {profile.state_root}",
        ) from exc
    verify_private_roots(profile, identity)


def verify_secret_files(profile: Profile, identity: Identity) -> None:
    verify_private_roots(profile, identity)
    _assert_secure_file(profile.secrets_env, identity, "sandbox secret env file")
    values = _parse_env_file(profile.secrets_env)
    missing = [key for key in SECRET_KEYS if not values.get(key)]
    if missing:
        raise HostConvergeError(
            "sandbox secret env file is incomplete: " + ", ".join(missing),
        )
    _assert_secure_file(profile.admin_secret, identity, "sandbox admin secret file")
    try:
        payload = tomllib.loads(profile.admin_secret.read_text(encoding="utf-8"))
        token = payload["admin"]["token"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("sandbox admin secret file is invalid") from exc
    if not isinstance(token, str) or not token.startswith("loom_admin_") or len(token) < 43:
        raise HostConvergeError("sandbox admin secret file is invalid")


def ensure_secret_files(profile: Profile, identity: Identity) -> None:
    ensure_private_roots(profile, identity)
    if not profile.secrets_env.exists():
        _atomic_write(
            profile.secrets_env,
            _render_env(_new_secret_values(profile.sandbox)),
            mode=0o600,
            identity=identity,
        )
    if not profile.admin_secret.exists():
        token = f"loom_admin_{secrets.token_urlsafe(32)}"
        content = (f'[admin]\ntoken = "{token}"\nversion = 1\n').encode()
        _atomic_write(
            profile.admin_secret,
            content,
            mode=0o600,
            identity=identity,
        )
    verify_secret_files(profile, identity)


def _git(
    candidate: Path,
    *args: str,
    identity: Identity | None = None,
) -> str:
    result = _run(
        ("git", "-c", f"safe.directory={candidate}", "-C", str(candidate), *args),
        env=_clean_git_environment(),
        identity=identity,
    )
    return result.stdout.strip()


def verify_candidate(
    profile: Profile,
    path: Path,
    sha: str,
    authority: Identity,
) -> str:
    if path != profile.candidate_root / sha or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate path is not exact-SHA bound")
    directory = _run(
        ("test", "-d", str(path)),
        identity=authority,
        expected={0, 1},
    )
    symlink = _run(
        ("test", "-L", str(path)),
        identity=authority,
        expected={0, 1},
    )
    if directory.returncode != 0 or symlink.returncode == 0:
        raise HostConvergeError("candidate directory is unavailable")
    if _git(path, "rev-parse", "--verify", "HEAD", identity=authority) != sha:
        raise HostConvergeError("candidate HEAD does not match requested SHA")
    if _git(path, "rev-parse", "--verify", f"{sha}^{{commit}}", identity=authority) != sha:
        raise HostConvergeError("candidate commit does not resolve exactly")
    if _git(
        path,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        identity=authority,
    ):
        raise HostConvergeError("candidate checkout is not clean")
    tree = _git(path, "rev-parse", "--verify", "HEAD^{tree}", identity=authority)
    if SHA_RE.fullmatch(tree) is None:
        raise HostConvergeError("candidate tree is invalid")
    root_metadata = path.lstat()
    if (
        root_metadata.st_uid != authority.uid
        or root_metadata.st_gid != authority.gid
        or stat.S_IMODE(root_metadata.st_mode) != 0o2750
    ):
        raise HostConvergeError("candidate root metadata is invalid")
    for root, directories, files in os.walk(path, followlinks=False):
        for entry in (
            Path(root),
            *(Path(root) / name for name in (*directories, *files)),
        ):
            metadata = entry.lstat()
            if (metadata.st_uid, metadata.st_gid) != (authority.uid, authority.gid):
                raise HostConvergeError("candidate ownership is invalid")
            if not stat.S_ISLNK(metadata.st_mode) and metadata.st_mode & 0o022:
                raise HostConvergeError("candidate contains a group/world-writable entry")
    return tree


def verify_candidate_root(profile: Profile, authority: Identity) -> None:
    directory = _run(
        ("test", "-d", str(profile.candidate_root)),
        identity=authority,
        expected={0, 1},
    )
    symlink = _run(
        ("test", "-L", str(profile.candidate_root)),
        identity=authority,
        expected={0, 1},
    )
    metadata = _run(
        ("stat", "-Lc", "%u:%g:%a", str(profile.candidate_root)),
        identity=authority,
    ).stdout.strip()
    if (
        directory.returncode != 0
        or symlink.returncode == 0
        or metadata != f"0:{authority.gid}:2750"
    ):
        raise HostConvergeError("candidate root owner or mode is invalid")


def _exact_keys(payload: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(payload) != expected:
        raise HostConvergeError(f"{label} does not match the closed schema")


def _parse_attestation_time(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise HostConvergeError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HostConvergeError(f"{label} is invalid") from exc
    if parsed.tzinfo is None:
        raise HostConvergeError(f"{label} must include timezone")
    return parsed.astimezone(UTC)


def combined_receipt_path(profile: Profile, sha: str) -> Path:
    return COMBINED_RECEIPT_ROOT / profile.sandbox / sha / "combined.json"


def verify_worker_runtime_env(
    profile: Profile,
    sha: str,
    sandbox_group: Identity,
) -> None:
    path = profile.worker_runtime_env(sha)
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise HostConvergeError("OLDLAB worker runtime env is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, sandbox_group.gid)
        or stat.S_IMODE(metadata.st_mode) != 0o640
    ):
        raise HostConvergeError("OLDLAB worker runtime env metadata is invalid")
    values = _parse_env_file(path)
    bundle_root = f"/etc/loom/developer-sandbox-links/clients/{profile.sandbox}/{sha}"
    expected = {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://sandbox-link:8080",
        "LOOM_WORKER_GATEWAY_URL": "http://sandbox-link:9100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://sandbox-link:9000",
        "LOOM_WORKER_SANDBOX_IDENTITY": profile.sandbox,
        "LOOM_WORKER_CANDIDATE_SHA": sha,
        "LOOM_WORKER_TOKEN_FILE_HOST": f"{bundle_root}/worker-token",
        "LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST": f"{bundle_root}/minio-access-key",
        "LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST": f"{bundle_root}/minio-secret-key",
        "LOOM_WORKER_CP_TLS_CA_FILE_HOST": f"{bundle_root}/ca.pem",
        "LOOM_WORKER_CP_TLS_CERT_FILE_HOST": f"{bundle_root}/client.pem",
        "LOOM_WORKER_CP_TLS_KEY_FILE_HOST": f"{bundle_root}/client-key.pem",
    }
    if values != expected:
        raise HostConvergeError("OLDLAB worker runtime env binding is invalid")


def _read_combined_receipt_bytes(path: Path) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        opened = os.fstat(descriptor)
        rebound = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_uid, opened.st_gid) != (0, 0)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino)
        ):
            raise HostConvergeError("combined activation receipt metadata is invalid")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > 8 * 1024 * 1024:
                raise HostConvergeError("combined activation receipt is too large")
        after = path.lstat()
        if (opened.st_dev, opened.st_ino) != (after.st_dev, after.st_ino):
            raise HostConvergeError("combined activation receipt changed during read")
        return b"".join(chunks)
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError("combined activation receipt is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def verify_combined_receipt(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    now: datetime | None = None,
) -> ActivationReceipt:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    path = combined_receipt_path(profile, sha)
    raw = _read_combined_receipt_bytes(path)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("combined activation receipt is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("combined activation receipt is invalid")
    canonical = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        + b"\n"
    )
    if raw != canonical:
        raise HostConvergeError("combined activation receipt is not canonical")
    _exact_keys(
        payload,
        {
            "schema_version",
            "kind",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "collector",
            "fleet_attestation",
            "domains",
            "payload_sha256",
        },
        "combined activation receipt",
    )
    digest = payload["payload_sha256"]
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    expected_digest = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    if (
        payload["schema_version"] != 1
        or payload["kind"] != "loom.developer-runtime-combined-activation"
        or payload["sandbox"] != profile.sandbox
        or payload["candidate_sha"] != sha
        or payload["candidate_tree"] != tree
        or not isinstance(digest, str)
        or digest != expected_digest
    ):
        raise HostConvergeError("combined activation receipt identity or digest is invalid")
    collector = payload["collector"]
    fleet = payload["fleet_attestation"]
    domains = payload["domains"]
    if not all(isinstance(item, dict) for item in (collector, fleet, domains)):
        raise HostConvergeError("combined activation receipt sections are invalid")
    _exact_keys(collector, {"hostname", "collected_at", "expires_at"}, "collector")
    collected_at = _parse_attestation_time(collector["collected_at"], "collected_at")
    expires_at = _parse_attestation_time(collector["expires_at"], "expires_at")
    if (
        collector["hostname"] != EXPECTED_HOSTNAME
        or collected_at > now + timedelta(seconds=30)
        or now - collected_at > RECEIPT_FRESHNESS
        or expires_at <= now
        or expires_at <= collected_at
        or expires_at - collected_at > ATTESTATION_TTL
    ):
        raise HostConvergeError("combined activation receipt is stale or expired")
    _exact_keys(
        fleet,
        {"path", "payload_sha256", "generated_at", "expires_at"},
        "fleet receipt reference",
    )
    fleet_generated = _parse_attestation_time(fleet["generated_at"], "fleet generated_at")
    fleet_expires = _parse_attestation_time(fleet["expires_at"], "fleet expires_at")
    if (
        fleet["path"]
        != str(FLEET_ATTESTATION_ROOT / profile.sandbox / sha / "fleet.json")
        or not isinstance(fleet["payload_sha256"], str)
        or FINGERPRINT_RE.fullmatch(fleet["payload_sha256"]) is None
        or fleet_generated > now + timedelta(seconds=30)
        or now - fleet_generated > RECEIPT_FRESHNESS
        or fleet_generated > collected_at + timedelta(seconds=30)
        or collected_at - fleet_generated > RECEIPT_FRESHNESS
        or fleet_expires <= now
        or fleet_expires - fleet_generated != ATTESTATION_TTL
        or fleet_expires < expires_at
    ):
        raise HostConvergeError("combined fleet receipt binding is invalid")
    if set(domains) != {"oldlab", "gb10"}:
        raise HostConvergeError("combined receipt domain set is incomplete")
    domain_keys = {
        "manifest_path",
        "signature_path",
        "payload_sha256",
        "signature_sha256",
        "key_id",
        "generation",
        "published_at",
        "expires_at",
    }
    for domain in ("oldlab", "gb10"):
        row = domains[domain]
        if not isinstance(row, dict):
            raise HostConvergeError("combined receipt domain input is invalid")
        _exact_keys(row, domain_keys, "combined receipt domain input")
        base = f"/var/lib/loom-developer-domain-attestations/{profile.sandbox}/{sha}"
        published = _parse_attestation_time(row["published_at"], "domain published_at")
        domain_expires = _parse_attestation_time(row["expires_at"], "domain expires_at")
        if (
            row["manifest_path"] != f"{base}/{domain}.json"
            or row["signature_path"] != f"{base}/{domain}.sig"
            or not isinstance(row["payload_sha256"], str)
            or DIGEST_RE.fullmatch(row["payload_sha256"]) is None
            or not isinstance(row["signature_sha256"], str)
            or DIGEST_RE.fullmatch(row["signature_sha256"]) is None
            or not isinstance(row["key_id"], str)
            or DIGEST_RE.fullmatch(row["key_id"]) is None
            or type(row["generation"]) is not int
            or row["generation"] < 1
            or published > now + timedelta(seconds=30)
            or now - published > RECEIPT_FRESHNESS
            or published > collected_at + timedelta(seconds=30)
            or collected_at - published > ATTESTATION_TTL
            or domain_expires <= now
            or domain_expires - published != ATTESTATION_TTL
            or domain_expires < expires_at
        ):
            raise HostConvergeError("combined receipt domain binding is invalid")
    return ActivationReceipt(
        path=path,
        payload_sha256=digest,
        fleet_payload_sha256=fleet["payload_sha256"],
        expires_at=expires_at,
    )


def _renewal_state_file(profile: Profile) -> Path:
    return RENEWAL_STATE_ROOT / f"{profile.sandbox}.json"


def _write_root_exclusive(path: Path, content: bytes) -> None:
    descriptor = -1
    try:
        with _seized_directory(path.parent, create=True) as parent_fd:
            try:
                descriptor = os.open(
                    path.name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=parent_fd,
                )
            except FileExistsError as exc:
                if _read_combined_receipt_bytes(path) != content:
                    raise HostConvergeError(
                        "renewal history generation already exists with different bytes",
                    ) from exc
                return
            view = memoryview(content)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise HostConvergeError("renewal history write made no progress")
                view = view[written:]
            os.fchown(descriptor, 0, 0)
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            opened = os.fstat(descriptor)
            rebound = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(rebound.st_mode)
                or (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino)
            ):
                raise HostConvergeError("renewal history binding changed")
            os.fsync(parent_fd)
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError("could not persist renewal history") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _archive_runtime_attestation(
    profile: Profile,
    sha: str,
    tree: str,
    receipt: ActivationReceipt,
) -> dict[str, Any]:
    try:
        raw = _read_combined_receipt_bytes(receipt.path)
        combined = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise HostConvergeError("combined activation receipt is unavailable") from exc
    if not isinstance(combined, dict):
        raise HostConvergeError("combined activation receipt is invalid")
    canonical = (
        json.dumps(
            combined,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        + b"\n"
    )
    unsigned_combined = {
        key: value for key, value in combined.items() if key != "payload_sha256"
    }
    combined_digest = hashlib.sha256(
        json.dumps(
            unsigned_combined,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    if (
        raw != canonical
        or combined.get("sandbox") != profile.sandbox
        or combined.get("candidate_sha") != sha
        or combined.get("candidate_tree") != tree
        or combined.get("payload_sha256") != combined_digest
        or receipt.payload_sha256 != combined_digest
    ):
        raise HostConvergeError("combined activation receipt binding is invalid")
    collector = combined.get("collector")
    domains = combined.get("domains")
    fleet_reference = combined.get("fleet_attestation")
    if (
        not isinstance(collector, dict)
        or not isinstance(domains, dict)
        or not isinstance(fleet_reference, dict)
    ):
        raise HostConvergeError("combined activation receipt sections are invalid")
    collected_at = _parse_attestation_time(collector.get("collected_at"), "collected_at")
    combined_expires = _parse_attestation_time(collector.get("expires_at"), "expires_at")
    if combined_expires != receipt.expires_at:
        raise HostConvergeError("combined activation receipt expiry binding is invalid")
    fleet_path = fleet_reference.get("path")
    if not isinstance(fleet_path, str):
        raise HostConvergeError("combined fleet receipt path is invalid")
    try:
        fleet_payload = json.loads(_read_combined_receipt_bytes(Path(fleet_path)))
    except json.JSONDecodeError as exc:
        raise HostConvergeError("fleet attestation is invalid") from exc
    if not isinstance(fleet_payload, dict):
        raise HostConvergeError("fleet attestation is invalid")
    fleet_unsigned = {
        key: value for key, value in fleet_payload.items() if key != "payload_sha256"
    }
    fleet_digest = "sha256:" + hashlib.sha256(
        json.dumps(
            fleet_unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    fleet_nodes = fleet_payload.get("nodes")
    fleet_bundle = fleet_payload.get("bundle_generation")
    fleet_server = fleet_payload.get("server")
    if (
        fleet_payload.get("sandbox") != profile.sandbox
        or fleet_payload.get("candidate_sha") != sha
        or fleet_payload.get("payload_sha256") != fleet_digest
        or fleet_digest != receipt.fleet_payload_sha256
        or fleet_reference.get("payload_sha256") != fleet_digest
        or fleet_payload.get("generated_at") != fleet_reference.get("generated_at")
        or fleet_payload.get("expires_at") != fleet_reference.get("expires_at")
        or fleet_payload.get("eligible_nodes") != list(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_nodes, dict)
        or set(fleet_nodes) != set(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_bundle, dict)
        or fleet_bundle.get("candidate_sha") != sha
        or not isinstance(fleet_server, dict)
        or fleet_server.get("active_candidate_sha") != sha
        or fleet_server.get("node") != "oldlab-2"
        or fleet_server.get("unit_active") is not True
        or any(
            not isinstance(node, dict) or node.get("candidate_sha") != sha
            for node in fleet_nodes.values()
        )
    ):
        raise HostConvergeError("fleet attestation host coverage is invalid")
    try:
        domain_generations = {
            domain: int(domains[domain]["generation"])
            for domain in ("oldlab", "gb10")
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise HostConvergeError("combined activation receipt generations are invalid") from exc
    previous = _load_json(_renewal_state_file(profile), "attestation renewal state")
    generation = 1
    previous_digest: str | None = None
    if previous is not None:
        _exact_keys(
            previous,
            {
                "schema_version",
                "sandbox",
                "candidate_sha",
                "candidate_tree",
                "renewal_generation",
                "renewal_payload_sha256",
                "combined_payload_sha256",
                "collected_at",
                "expires_at",
                "domain_generations",
            },
            "attestation renewal state",
        )
        prior_generation = previous.get("renewal_generation")
        previous_digest = previous.get("renewal_payload_sha256")
        previous_combined_digest = previous.get("combined_payload_sha256")
        previous_sha = previous.get("candidate_sha")
        previous_tree = previous.get("candidate_tree")
        previous_domains = previous.get("domain_generations")
        if (
            previous.get("schema_version") != 1
            or previous.get("sandbox") != profile.sandbox
            or not isinstance(previous_sha, str)
            or SHA_RE.fullmatch(previous_sha) is None
            or not isinstance(previous_tree, str)
            or SHA_RE.fullmatch(previous_tree) is None
            or type(prior_generation) is not int
            or prior_generation < 1
            or not isinstance(previous_digest, str)
            or DIGEST_RE.fullmatch(previous_digest) is None
            or not isinstance(previous_combined_digest, str)
            or DIGEST_RE.fullmatch(previous_combined_digest) is None
            or not isinstance(previous_domains, dict)
            or set(previous_domains) != {"oldlab", "gb10"}
            or any(
                type(previous_domains[domain]) is not int
                or previous_domains[domain] < 1
                for domain in ("oldlab", "gb10")
            )
        ):
            raise HostConvergeError("attestation renewal state is invalid")
        if previous_combined_digest == receipt.payload_sha256:
            raise HostConvergeError("attestation renewal replay did not produce fresh proof")
        previous_collected = _parse_attestation_time(
            previous.get("collected_at"),
            "previous renewal collected_at",
        )
        if collected_at <= previous_collected:
            raise HostConvergeError("attestation renewal time did not advance")
        if previous_sha == sha:
            if previous_tree != tree or any(
                domain_generations[domain] <= previous_domains[domain]
                for domain in ("oldlab", "gb10")
            ):
                raise HostConvergeError("domain attestation generation did not advance")
        generation = prior_generation + 1

    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-runtime-attestation-renewal",
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "renewal_generation": generation,
        "previous_payload_sha256": previous_digest,
        "collected_at": collected_at.isoformat(),
        "expires_at": receipt.expires_at.isoformat(),
        "domain_generations": domain_generations,
        "fleet_attestation": fleet_payload,
        "combined_receipt": combined,
    }
    payload = dict(unsigned)
    payload["payload_sha256"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(),
    ).hexdigest()
    content = (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode()
    history = (
        COMBINED_RECEIPT_ROOT
        / profile.sandbox
        / sha
        / "renewals"
        / f"{generation:020d}-{payload['payload_sha256']}.json"
    )
    _write_root_exclusive(history, content)
    state = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "renewal_generation": generation,
        "renewal_payload_sha256": payload["payload_sha256"],
        "combined_payload_sha256": receipt.payload_sha256,
        "collected_at": collected_at.isoformat(),
        "expires_at": receipt.expires_at.isoformat(),
        "domain_generations": domain_generations,
    }
    _ensure_root_private_directory(RENEWAL_STATE_ROOT)
    _atomic_write(
        _renewal_state_file(profile),
        (json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )
    return {
        "path": str(history),
        "payload_sha256": payload["payload_sha256"],
        "previous_payload_sha256": previous_digest,
        "renewal_generation": generation,
        "collected_at": collected_at.isoformat(),
        "expires_at": receipt.expires_at.isoformat(),
        "domain_generations": domain_generations,
    }


def _archived_activation_from_path(
    profile: Profile,
    sha: str,
    tree: str,
    path: Path,
) -> tuple[int, ActivationReceipt]:
    raw = _read_combined_receipt_bytes(path)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("archived activation receipt is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("archived activation receipt is invalid")
    canonical = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        + b"\n"
    )
    _exact_keys(
        payload,
        {
            "schema_version",
            "kind",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "renewal_generation",
            "previous_payload_sha256",
            "collected_at",
            "expires_at",
            "domain_generations",
            "fleet_attestation",
            "combined_receipt",
            "payload_sha256",
        },
        "archived activation receipt",
    )
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    expected_digest = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    generation = payload["renewal_generation"]
    filename = RENEWAL_HISTORY_RE.fullmatch(path.name)
    previous_digest = payload["previous_payload_sha256"]
    if (
        raw != canonical
        or payload["schema_version"] != 1
        or payload["kind"] != "loom.developer-runtime-attestation-renewal"
        or payload["sandbox"] != profile.sandbox
        or payload["candidate_sha"] != sha
        or payload["candidate_tree"] != tree
        or type(generation) is not int
        or generation < 1
        or not isinstance(payload["payload_sha256"], str)
        or payload["payload_sha256"] != expected_digest
        or filename is None
        or int(filename.group(1)) != generation
        or filename.group(2) != expected_digest
        or (
            previous_digest is not None
            and (
                not isinstance(previous_digest, str) or DIGEST_RE.fullmatch(previous_digest) is None
            )
        )
    ):
        raise HostConvergeError("archived activation receipt binding is invalid")

    collected_at = _parse_attestation_time(payload["collected_at"], "collected_at")
    expires_at = _parse_attestation_time(payload["expires_at"], "expires_at")
    if expires_at <= collected_at or expires_at - collected_at > ATTESTATION_TTL:
        raise HostConvergeError("archived activation receipt lifetime is invalid")

    domain_generations = payload["domain_generations"]
    combined = payload["combined_receipt"]
    fleet = payload["fleet_attestation"]
    if (
        not isinstance(domain_generations, dict)
        or set(domain_generations) != {"oldlab", "gb10"}
        or any(
            type(domain_generations[domain]) is not int or domain_generations[domain] < 1
            for domain in ("oldlab", "gb10")
        )
        or not isinstance(combined, dict)
        or not isinstance(fleet, dict)
    ):
        raise HostConvergeError("archived activation receipt sections are invalid")

    combined_digest = combined.get("payload_sha256")
    combined_unsigned = {key: value for key, value in combined.items() if key != "payload_sha256"}
    expected_combined_digest = hashlib.sha256(
        json.dumps(
            combined_unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    collector = combined.get("collector")
    combined_domains = combined.get("domains")
    fleet_reference = combined.get("fleet_attestation")
    if (
        combined.get("schema_version") != 1
        or combined.get("kind") != "loom.developer-runtime-combined-activation"
        or combined.get("sandbox") != profile.sandbox
        or combined.get("candidate_sha") != sha
        or combined.get("candidate_tree") != tree
        or not isinstance(combined_digest, str)
        or combined_digest != expected_combined_digest
        or not isinstance(collector, dict)
        or collector.get("hostname") != EXPECTED_HOSTNAME
        or collector.get("collected_at") != payload["collected_at"]
        or collector.get("expires_at") != payload["expires_at"]
        or not isinstance(combined_domains, dict)
        or set(combined_domains) != {"oldlab", "gb10"}
        or any(
            not isinstance(combined_domains[domain], dict)
            or combined_domains[domain].get("generation") != domain_generations[domain]
            for domain in ("oldlab", "gb10")
        )
        or not isinstance(fleet_reference, dict)
    ):
        raise HostConvergeError("archived combined receipt binding is invalid")

    fleet_unsigned = {key: value for key, value in fleet.items() if key != "payload_sha256"}
    expected_fleet_digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                fleet_unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
    )
    fleet_nodes = fleet.get("nodes")
    fleet_bundle = fleet.get("bundle_generation")
    fleet_server = fleet.get("server")
    if (
        fleet.get("sandbox") != profile.sandbox
        or fleet.get("candidate_sha") != sha
        or fleet.get("payload_sha256") != expected_fleet_digest
        or fleet_reference.get("path")
        != str(FLEET_ATTESTATION_ROOT / profile.sandbox / sha / "fleet.json")
        or fleet_reference.get("payload_sha256") != expected_fleet_digest
        or fleet_reference.get("generated_at") != fleet.get("generated_at")
        or fleet_reference.get("expires_at") != fleet.get("expires_at")
        or fleet.get("eligible_nodes") != list(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_nodes, dict)
        or set(fleet_nodes) != set(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_bundle, dict)
        or fleet_bundle.get("candidate_sha") != sha
        or not isinstance(fleet_server, dict)
        or fleet_server.get("active_candidate_sha") != sha
        or fleet_server.get("node") != "oldlab-2"
        or fleet_server.get("unit_active") is not True
        or any(
            not isinstance(node, dict) or node.get("candidate_sha") != sha
            for node in fleet_nodes.values()
        )
    ):
        raise HostConvergeError("archived fleet attestation binding is invalid")
    fleet_generated = _parse_attestation_time(fleet.get("generated_at"), "fleet generated_at")
    fleet_expires = _parse_attestation_time(fleet.get("expires_at"), "fleet expires_at")
    if (
        fleet_generated > collected_at + timedelta(seconds=30)
        or collected_at - fleet_generated > RECEIPT_FRESHNESS
        or fleet_expires - fleet_generated != ATTESTATION_TTL
        or fleet_expires < expires_at
    ):
        raise HostConvergeError("archived fleet attestation lifetime is invalid")
    return generation, ActivationReceipt(
        path=combined_receipt_path(profile, sha),
        payload_sha256=combined_digest,
        fleet_payload_sha256=expected_fleet_digest,
        expires_at=expires_at,
    )


def _verify_archived_activation(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    desired: Mapping[str, Any] | None = None,
) -> ActivationReceipt:
    history_root = COMBINED_RECEIPT_ROOT / profile.sandbox / sha / "renewals"
    descriptor = -1
    try:
        descriptor = _open_absolute_directory(history_root, create=False)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise HostConvergeError("archived activation directory is unsafe")
        names = sorted(os.listdir(descriptor))
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError("archived activation history is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not names or any(RENEWAL_HISTORY_RE.fullmatch(name) is None for name in names):
        raise HostConvergeError("archived activation history is invalid")
    receipts = [
        _archived_activation_from_path(profile, sha, tree, history_root / name) for name in names
    ]
    receipts.sort(key=lambda item: item[0], reverse=True)
    if desired is None:
        return receipts[0][1]
    for _generation, receipt in receipts:
        try:
            _validate_desired_binding(
                profile,
                desired,
                sha=sha,
                tree=tree,
                receipt=receipt,
            )
        except HostConvergeError:
            continue
        return receipt
    raise HostConvergeError("desired state has no matching archived activation")


def _desired_payload(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    previous_sha: str | None,
    receipt: ActivationReceipt,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_path": str(profile.candidate_root / sha),
        "previous_sha": previous_sha,
        "worker_runtime_env": str(profile.worker_runtime_env(sha)),
        "combined_receipt": str(receipt.path),
        "combined_receipt_sha256": receipt.payload_sha256,
        "fleet_attestation_sha256": receipt.fleet_payload_sha256,
        "receipt_expires_at": receipt.expires_at.isoformat(),
        "secrets_env": str(profile.secrets_env),
        "admin_secret_file": str(profile.admin_secret),
    }


def _load_json(path: Path, label: str) -> dict[str, Any] | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HostConvergeError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise HostConvergeError(f"{label} is unsafe")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HostConvergeError(f"{label} is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError(f"{label} is invalid")
    return payload


def write_desired(
    profile: Profile,
    sha: str,
    tree: str,
    receipt: ActivationReceipt,
) -> dict[str, Any] | None:
    previous = _load_json(profile.desired_file, "sandbox desired state")
    previous_sha = None
    if previous is not None:
        current = previous.get("candidate_sha")
        if isinstance(current, str) and current != sha:
            previous_sha = current
        elif isinstance(previous.get("previous_sha"), str):
            previous_sha = previous["previous_sha"]
    payload = _desired_payload(
        profile,
        sha,
        tree,
        previous_sha=previous_sha,
        receipt=receipt,
    )
    _atomic_write(
        profile.desired_file,
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )
    return previous


def _install_assets(source_root: Path) -> None:
    _ensure_root_private_directory(CONFIG_ROOT)
    _ensure_root_private_directory(DESIRED_ROOT)
    _ensure_root_private_directory(PROFILE_CONFIG_ROOT)
    _atomic_write(
        INSTALLED_PROGRAM,
        (source_root / "scripts/ops/developer_sandbox_host.py").read_bytes(),
        mode=0o755,
    )
    profiles_root = source_root / "deploy/developer-sandboxes"
    _atomic_write(
        UNIT_PATH,
        (profiles_root / "loom-developer-sandbox@.service").read_bytes(),
        mode=0o644,
    )
    _atomic_write(
        RENEWAL_SERVICE_PATH,
        (
            profiles_root / "loom-developer-sandbox-attestation-renewal.service"
        ).read_bytes(),
        mode=0o644,
    )
    _atomic_write(
        RENEWAL_TIMER_PATH,
        (profiles_root / "loom-developer-sandbox-attestation-renewal.timer").read_bytes(),
        mode=0o644,
    )
    for sandbox in SANDBOXES:
        _atomic_write(
            PROFILE_CONFIG_ROOT / f"{sandbox}.toml",
            (profiles_root / f"{sandbox}.toml").read_bytes(),
            mode=0o600,
        )
    _run(("systemctl", "daemon-reload"))


def _read_admin_token(path: Path) -> str:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
        token = payload["admin"]["token"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("sandbox admin secret is invalid") from exc
    if not isinstance(token, str):
        raise HostConvergeError("sandbox admin secret is invalid")
    return token


def _request_json(
    url: str,
    *,
    token: str | None,
    expected: set[int],
) -> tuple[int, dict[str, Any]]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=b"{}",
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read()
    except urllib.error.URLError as exc:
        raise HostConvergeError("sandbox Control Plane is unavailable") from exc
    if status not in expected:
        raise HostConvergeError(f"sandbox Control Plane returned unexpected status {status}")
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError as exc:
        raise HostConvergeError("sandbox Control Plane returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("sandbox Control Plane returned invalid JSON")
    return status, payload


def _wait_for_control_plane(profile: Profile) -> None:
    url = f"http://127.0.0.1:{profile.ports['control_plane']}/healthz"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(1)
    raise HostConvergeError("sandbox Control Plane did not become healthy")


def _update_secret_tokens(
    profile: Profile,
    identity: Identity,
    updates: Mapping[str, str],
) -> None:
    _assert_secure_file(profile.secrets_env, identity, "sandbox secret env file")
    values = _parse_env_file(profile.secrets_env)
    values.update(updates)
    _atomic_write(
        profile.secrets_env,
        _render_env(values),
        mode=0o600,
        identity=identity,
    )


def bootstrap_runtime_tokens(profile: Profile, identity: Identity) -> bool:
    _wait_for_control_plane(profile)
    values = _parse_env_file(profile.secrets_env)
    worker_token = values.get("LOOM_WORKER_TOKEN", "")
    register_url = f"http://127.0.0.1:{profile.ports['control_plane']}/workers/register"
    worker_status, _ = _request_json(
        register_url,
        token=worker_token,
        expected={400, 401},
    )
    batch_token = values.get("LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "")
    if worker_status == 400 and batch_token:
        return False

    admin_token = _read_admin_token(profile.admin_secret)
    base = f"http://127.0.0.1:{profile.ports['control_plane']}/admin"
    updates: dict[str, str] = {}
    if worker_status == 401:
        _, worker_payload = _request_json(
            f"{base}/worker-tokens",
            token=admin_token,
            expected={201},
        )
        raw_worker = worker_payload.get("token")
        if not isinstance(raw_worker, str) or not raw_worker.startswith("loom_w_"):
            raise HostConvergeError("Control Plane returned an invalid worker token")
        updates["LOOM_WORKER_TOKEN"] = raw_worker
    if not batch_token or worker_status == 401:
        _, batch_payload = _request_json(
            f"{base}/batch-runner-tokens",
            token=admin_token,
            expected={201},
        )
        raw_batch = batch_payload.get("token")
        if not isinstance(raw_batch, str) or not raw_batch.startswith("loom_br_"):
            raise HostConvergeError("Control Plane returned an invalid batch token")
        updates["LOOM_SVC_BATCH_RUNNER_CP_TOKEN"] = raw_batch
    _update_secret_tokens(profile, identity, updates)
    return bool(updates)


def _candidate_environment(profile: Profile, candidate: Path) -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "HOME": str(profile.runtime_root),
        "PYTHONPATH": str(candidate / "src"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": str(candidate),
        "GIT_NO_REPLACE_OBJECTS": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _invoke_lifecycle(profile: Profile, sha: str, operation: str) -> None:
    candidate = profile.candidate_root / sha
    program = candidate / "scripts/ops/developer_sandbox.py"
    profile_path = candidate / f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    owner = _identity(profile.sandbox, SHARED_GROUP)
    if (
        _run(
            ("test", "-f", str(program)),
            identity=owner,
            init_groups=True,
            expected={0, 1},
        ).returncode
        != 0
        or _run(
            ("test", "-f", str(profile_path)),
            identity=owner,
            init_groups=True,
            expected={0, 1},
        ).returncode
        != 0
    ):
        raise HostConvergeError("candidate sandbox lifecycle assets are unavailable")
    _run(
        (
            sys.executable,
            str(program),
            operation,
            "--profile",
            str(profile_path),
            "--source-repo",
            str(candidate),
            "--candidate-sha",
            sha,
            "--secrets-env",
            str(profile.secrets_env),
            "--admin-secret-file",
            str(profile.admin_secret),
            "--execute",
        ),
        env=_candidate_environment(profile, candidate),
        identity=owner,
        init_groups=True,
    )


def _desired_for_service(sandbox: str) -> tuple[Profile, dict[str, Any]]:
    profile = _load_profile(PROFILE_CONFIG_ROOT / f"{sandbox}.toml")
    desired = _load_json(profile.desired_file, "sandbox desired state")
    if desired is None or desired.get("sandbox") != sandbox:
        raise HostConvergeError("sandbox desired state is absent or invalid")
    return profile, desired


def _sandbox_state_sha(profile: Profile) -> str | None:
    state = _load_json(profile.state_file, "sandbox lifecycle state")
    if state is None:
        return None
    sha = state.get("candidate_sha")
    if not isinstance(sha, str) or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("sandbox lifecycle state SHA is invalid")
    return sha


def _validate_desired_binding(
    profile: Profile,
    desired: Mapping[str, Any],
    *,
    sha: str,
    tree: str,
    receipt: ActivationReceipt,
) -> None:
    expected = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_path": str(profile.candidate_root / sha),
        "worker_runtime_env": str(profile.worker_runtime_env(sha)),
        "combined_receipt": str(receipt.path),
        "combined_receipt_sha256": receipt.payload_sha256,
        "fleet_attestation_sha256": receipt.fleet_payload_sha256,
        "receipt_expires_at": receipt.expires_at.isoformat(),
        "secrets_env": str(profile.secrets_env),
        "admin_secret_file": str(profile.admin_secret),
    }
    if any(desired.get(key) != value for key, value in expected.items()):
        raise HostConvergeError("sandbox desired state binding is invalid")
    previous = desired.get("previous_sha")
    if previous is not None and (
        not isinstance(previous, str) or SHA_RE.fullmatch(previous) is None or previous == sha
    ):
        raise HostConvergeError("sandbox desired rollback binding is invalid")


def _renew_attestation_locked(
    profile: Profile,
    *,
    sha: str,
    tree: str,
) -> ActivationReceipt:
    relative_program = "scripts/ops/developer_sandbox_remote_link_host.py"
    _run_candidate_program(
        profile,
        sha,
        relative_program,
        "fleet-check",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--execute",
    )
    _publish_domain_attestations(profile, sha, tree)
    receipt = verify_combined_receipt(profile, sha, tree)
    _archive_runtime_attestation(profile, sha, tree, receipt)
    write_desired(profile, sha, tree, receipt)
    return receipt


def _service_converge_locked(
    profile: Profile,
    desired: Mapping[str, Any],
) -> None:
    sandbox = profile.sandbox
    sha = str(desired["candidate_sha"])
    authority = _identity("root", SHARED_GROUP)
    owner = _identity(sandbox, SHARED_GROUP)
    runtime_group = _identity(sandbox, f"loom-sandbox-{sandbox}")
    verify_candidate_root(profile, authority)
    tree = verify_candidate(profile, profile.candidate_root / sha, sha, authority)
    verify_worker_runtime_env(profile, sha, runtime_group)
    try:
        receipt = verify_combined_receipt(profile, sha, tree)
        _validate_desired_binding(
            profile,
            desired,
            sha=sha,
            tree=tree,
            receipt=receipt,
        )
    except HostConvergeError:
        receipt = _verify_archived_activation(
            profile,
            sha,
            tree,
            desired=desired,
        )
        _validate_desired_binding(
            profile,
            desired,
            sha=sha,
            tree=tree,
            receipt=receipt,
        )
    ensure_secret_files(profile, owner)
    current = _sandbox_state_sha(profile)
    if current is None:
        _invoke_lifecycle(profile, sha, "create")
    else:
        _invoke_lifecycle(profile, sha, "update")
    if bootstrap_runtime_tokens(profile, owner):
        _invoke_lifecycle(profile, sha, "update")
    _invoke_lifecycle(profile, sha, "check")
    verify_listening_ports(profile)


def service_converge(sandbox: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    profile, _desired = _desired_for_service(sandbox)
    with _activation_lock(profile):
        transaction = _transaction_payload(profile)
        if transaction is not None and transaction["phase"] != "desired-written":
            _recover_transaction(profile, transaction)
            transaction = None
        locked_profile, desired = _desired_for_service(sandbox)
        if transaction is not None and desired.get("candidate_sha") != transaction.get(
            "candidate_sha",
        ):
            raise HostConvergeError(
                "sandbox desired state does not match pending activation transaction",
            )
        _service_converge_locked(locked_profile, desired)
        if transaction is not None and transaction["operation"] != "rollback":
            _write_transaction(
                profile,
                operation=str(transaction["operation"]),
                sha=str(transaction["candidate_sha"]),
                tree=str(transaction["candidate_tree"]),
                phase="committed",
                previous_desired=transaction["previous_desired"],
                previous_relay_sha=transaction["previous_relay_sha"],
            )
            _remove_transaction(profile)


def service_check(sandbox: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    profile, desired = _desired_for_service(sandbox)
    sha = str(desired["candidate_sha"])
    authority = _identity("root", SHARED_GROUP)
    verify_candidate_root(profile, authority)
    tree = verify_candidate(
        profile,
        profile.candidate_root / sha,
        sha,
        authority,
    )
    verify_worker_runtime_env(profile, sha, _identity(sandbox, f"loom-sandbox-{sandbox}"))
    receipt = verify_combined_receipt(profile, sha, tree)
    _validate_desired_binding(
        profile,
        desired,
        sha=sha,
        tree=tree,
        receipt=receipt,
    )
    verify_secret_files(profile, _identity(sandbox, SHARED_GROUP))
    _invoke_lifecycle(profile, sha, "check")
    verify_listening_ports(profile)


def renew_attestations(profiles: Sequence[Profile], *, execute: bool) -> None:
    if not execute:
        raise HostConvergeError("attestation renewal requires --execute")
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    renewed: list[str] = []
    with _install_lock():
        for profile in profiles:
            with _activation_lock(profile):
                desired = _load_json(profile.desired_file, "sandbox desired state")
                if desired is None:
                    continue
                sha = desired.get("candidate_sha")
                if not isinstance(sha, str) or SHA_RE.fullmatch(sha) is None:
                    raise HostConvergeError(
                        f"{profile.sandbox} desired candidate SHA is invalid",
                    )
                authority = _identity("root", SHARED_GROUP)
                verify_candidate_root(profile, authority)
                tree = verify_candidate(
                    profile,
                    profile.candidate_root / sha,
                    sha,
                    authority,
                )
                verify_worker_runtime_env(
                    profile,
                    sha,
                    _identity(profile.sandbox, f"loom-sandbox-{profile.sandbox}"),
                )
                _renew_attestation_locked(profile, sha=sha, tree=tree)
                renewed.append(profile.sandbox)
    if not renewed:
        raise HostConvergeError("no installed sandbox desired state was found")


def verify_listening_ports(profile: Profile) -> None:
    result = _run(("ss", "-H", "-ltn"))
    listeners: set[tuple[str, int]] = set()
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        local = fields[3]
        host, separator, raw_port = local.rpartition(":")
        if not separator:
            continue
        try:
            port = int(raw_port)
        except ValueError:
            continue
        listeners.add((host.strip("[]"), port))
    missing = sorted(
        port for port in profile.ports.values() if ("127.0.0.1", port) not in listeners
    )
    if missing:
        raise HostConvergeError(
            "sandbox loopback ports are not listening: " + ", ".join(str(port) for port in missing),
        )


def _candidate_program(profile: Profile, sha: str, relative: str) -> Path:
    path = profile.candidate_root / sha / relative
    if not path.is_file() or path.is_symlink():
        raise HostConvergeError("exact candidate operation asset is unavailable")
    return path


def _run_candidate_program(
    profile: Profile,
    sha: str,
    relative: str,
    *arguments: str,
) -> dict[str, Any]:
    completed = _run(
        (
            sys.executable,
            str(_candidate_program(profile, sha, relative)),
            *arguments,
        ),
        env=_candidate_environment(profile, profile.candidate_root / sha),
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("exact candidate helper returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("exact candidate helper returned invalid JSON")
    return payload


def _ssh(
    node: str,
    argv: Sequence[str],
    *,
    input_bytes: bytes | None = None,
    expected: set[int] | frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[bytes]:
    completed = subprocess.run(
        (
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            node,
            *argv,
        ),
        input=input_bytes,
        check=False,
        capture_output=True,
    )
    if completed.returncode not in expected:
        raise HostConvergeError(
            f"remote {Path(argv[0]).name if argv else 'command'} failed safely "
            f"on {node} with exit code {completed.returncode}",
        )
    return completed


def _verify_remote_candidate(
    profile: Profile,
    node: str,
    sha: str,
    tree: str,
    shared_gid: int,
) -> None:
    domain = next(
        (name for name, nodes in DOMAIN_PEERS.items() if node in nodes),
        None,
    )
    if domain is None:
        raise HostConvergeError("remote candidate node is outside the closed inventory")
    result = _ssh(
        node,
        (
            "sudo",
            "-n",
            str(DOMAIN_RUNTIME_PROGRAM),
            "inspect-candidate",
            "--config",
            str(DOMAIN_RUNTIME_CONFIG),
            "--domain",
            domain,
            "--sandbox",
            profile.sandbox,
            "--candidate-sha",
            sha,
            "--candidate-tree",
            tree,
        ),
    )
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("remote candidate verifier returned invalid JSON") from exc
    if (
        not isinstance(report, dict)
        or report.get("operation") != "inspect-candidate"
        or report.get("domain") != domain
        or report.get("sandbox") != profile.sandbox
        or report.get("candidate_sha") != sha
        or report.get("candidate_tree") != tree
        or report.get("candidate_uid") != 0
        or report.get("candidate_gid") != shared_gid
        or report.get("candidate_mode") != "2750"
        or report.get("candidate_clean") is not True
    ):
        raise HostConvergeError(f"{node} candidate identity or metadata is invalid")


def _archive_credentials(
    source: Path,
    *,
    worker_token: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name in ("ca.pem", "client.pem", "client-key.pem"):
            path = source / name
            content = path.read_bytes()
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o600 if name == "client-key.pem" else 0o644
            info.uid = 0
            info.gid = 0
            archive.addfile(info, io.BytesIO(content))
        for name, value in (
            ("worker-token", worker_token),
            ("minio-access-key", minio_access_key),
            ("minio-secret-key", minio_secret_key),
        ):
            content = (value + "\n").encode()
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o600
            info.uid = 0
            info.gid = 0
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def _remove_source_stage(path: Path, sha: str) -> None:
    if path != SOURCE_STAGING_ROOT / sha:
        raise HostConvergeError("candidate source staging path is invalid")
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise HostConvergeError("candidate source staging metadata is invalid")
    shutil.rmtree(path)
    _fsync_directory(path.parent)


@contextmanager
def _candidate_source_stage(sha: str) -> Iterator[tuple[Path, str]]:
    if SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate SHA must be full lowercase 40-hex")
    authority = _identity("root", SHARED_GROUP)
    head = _git(REPO_ROOT, "rev-parse", "--verify", "HEAD", identity=authority)
    tree = _git(REPO_ROOT, "rev-parse", "--verify", "HEAD^{tree}", identity=authority)
    status = _git(
        REPO_ROOT,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        identity=authority,
    )
    if head != sha or SHA_RE.fullmatch(tree) is None or status:
        raise HostConvergeError("installer source is not the clean exact candidate")
    _ensure_root_private_directory(SOURCE_STAGING_ROOT)
    stage = SOURCE_STAGING_ROOT / sha
    # A prior process may have died after staging. The root-private, exact-SHA
    # namespace is disposable and is always rebuilt from the verified checkout.
    _remove_source_stage(stage, sha)
    _ensure_root_private_directory(stage)
    bundle = stage / "candidate.bundle"
    temporary = stage / ".candidate.bundle.tmp"
    manifest = stage / "manifest.json"
    try:
        _run(
            (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
                "-C",
                str(REPO_ROOT),
                "bundle",
                "create",
                str(temporary),
                "HEAD",
            ),
            env=_clean_git_environment(),
        )
        os.chown(temporary, 0, 0)
        os.chmod(temporary, 0o600)
        descriptor = os.open(temporary, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, bundle)
        _fsync_directory(stage)
        heads = _run(
            (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
                "bundle",
                "list-heads",
                str(bundle),
            ),
            env=_clean_git_environment(),
        ).stdout.splitlines()
        if heads != [f"{sha} HEAD"]:
            raise HostConvergeError("candidate source bundle is not exact-HEAD bounded")
        payload = {
            "schema_version": 1,
            "status": "staged",
            "candidate_sha": sha,
            "candidate_tree": tree,
            "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        _atomic_write(
            manifest,
            (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            mode=0o600,
        )
        yield bundle, tree
    finally:
        temporary.unlink(missing_ok=True)
        _remove_source_stage(stage, sha)


def _materialization_archive(bundle: Path, sha: str, tree: str) -> bytes:
    def committed(relative: str) -> bytes:
        return _run(
            (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
                "-C",
                str(REPO_ROOT),
                "show",
                f"{sha}:{relative}",
            ),
            env=_clean_git_environment(),
        ).stdout.encode()

    files = {
        "candidate.bundle": (bundle.read_bytes(), 0o600),
        "developer_sandbox_domain_runtime.py": (
            committed("scripts/ops/developer_sandbox_domain_runtime.py"),
            0o700,
        ),
        "runtime-domains.toml": (
            committed("deploy/developer-sandboxes/runtime-domains.toml"),
            0o600,
        ),
        "manifest.json": (
            (
                json.dumps(
                    {
                        "schema_version": 1,
                        "candidate_sha": sha,
                        "candidate_tree": tree,
                        "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode(),
            0o600,
        ),
    }
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, (content, mode) in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = mode
            info.uid = 0
            info.gid = 0
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def _runtime_bootstrap_archive(sha: str) -> bytes:
    files: dict[str, tuple[bytes, int]] = {}
    for relative, target, mode in (
        (
            "scripts/ops/developer_sandbox_domain_runtime.py",
            "developer_sandbox_domain_runtime.py",
            0o700,
        ),
        (
            "deploy/developer-sandboxes/runtime-domains.toml",
            "runtime-domains.toml",
            0o600,
        ),
    ):
        content = _run(
            (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
                "-C",
                str(REPO_ROOT),
                "show",
                f"{sha}:{relative}",
            ),
            env=_clean_git_environment(),
        ).stdout.encode()
        files[target] = (content, mode)
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, (content, mode) in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = mode
            info.uid = 0
            info.gid = 0
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def _remote_stage_failure_path(
    profile: Profile,
    sha: str,
    domain: str,
    node: str,
) -> Path:
    return SOURCE_STAGING_ROOT / "failures" / (
        f"{profile.sandbox}-{sha}-{domain}-{node}.json"
    )


def _cleanup_remote_stage(
    profile: Profile,
    sha: str,
    domain: str,
    node: str,
    stage: Path,
) -> None:
    failure = _remote_stage_failure_path(profile, sha, domain, node)
    try:
        _ssh(node, ("sudo", "-n", "rm", "-rf", "--", str(stage)))
    except HostConvergeError:
        _ensure_root_private_directory(failure.parent)
        _atomic_write(
            failure,
            (
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "remote-cleanup-failed",
                        "sandbox": profile.sandbox,
                        "candidate_sha": sha,
                        "domain": domain,
                        "node": node,
                        "stage": str(stage),
                        "recorded_at": datetime.now(UTC).isoformat(),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode(),
            mode=0o600,
        )
        raise
    if failure.exists():
        failure.unlink()
        _fsync_directory(failure.parent)


def _bootstrap_domain_runtime_hosts(profile: Profile, sha: str) -> None:
    archive = _runtime_bootstrap_archive(sha)
    for domain, nodes in DOMAIN_PEERS.items():
        for node in nodes:
            stage = Path(
                f"/run/loom-developer-sandbox-installer/bootstrap/"
                f"{profile.sandbox}/{sha}/{domain}/{node}",
            )
            _cleanup_remote_stage(profile, sha, domain, node, stage)
            try:
                _ssh(node, ("sudo", "-n", "install", "-d", "-m", "0700", str(stage)))
                _ssh(
                    node,
                    (
                        "sudo",
                        "-n",
                        "tar",
                        "--extract",
                        "--file=-",
                        "--directory",
                        str(stage),
                        "--no-same-owner",
                        "--no-same-permissions",
                    ),
                    input_bytes=archive,
                )
                for name, mode in (
                    ("developer_sandbox_domain_runtime.py", "0700"),
                    ("runtime-domains.toml", "0600"),
                ):
                    _ssh(
                        node,
                        ("sudo", "-n", "chown", "root:root", str(stage / name)),
                    )
                    _ssh(node, ("sudo", "-n", "chmod", mode, str(stage / name)))
                _ssh(
                    node,
                    (
                        "sudo",
                        "-n",
                        "python3",
                        str(stage / "developer_sandbox_domain_runtime.py"),
                        "host-converge",
                        "--config",
                        str(stage / "runtime-domains.toml"),
                        "--domain",
                        domain,
                        "--execute",
                    ),
                )
            finally:
                _cleanup_remote_stage(profile, sha, domain, node, stage)


def _materialize_domain_candidates(
    profile: Profile,
    sha: str,
    tree: str,
    bundle: Path,
) -> None:
    archive = _materialization_archive(bundle, sha, tree)
    for domain, publisher in DOMAIN_PUBLISHERS.items():
        stage = Path(
            f"/run/loom-developer-sandbox-installer/source/"
            f"{profile.sandbox}/{sha}/{domain}",
        )
        _cleanup_remote_stage(profile, sha, domain, publisher, stage)
        try:
            _ssh(publisher, ("sudo", "-n", "install", "-d", "-m", "0700", str(stage)))
            _ssh(
                publisher,
                (
                    "sudo",
                    "-n",
                    "tar",
                    "--extract",
                    "--file=-",
                    "--directory",
                    str(stage),
                    "--no-same-owner",
                    "--no-same-permissions",
                ),
                input_bytes=archive,
            )
            for name, mode in (
                ("candidate.bundle", "0600"),
                ("developer_sandbox_domain_runtime.py", "0700"),
                ("runtime-domains.toml", "0600"),
                ("manifest.json", "0600"),
            ):
                _ssh(
                    publisher,
                    (
                        "sudo",
                        "-n",
                        "chown",
                        "root:root",
                        str(stage / name),
                    ),
                )
                _ssh(
                    publisher,
                    ("sudo", "-n", "chmod", mode, str(stage / name)),
                )
            _ssh(
                publisher,
                (
                    "sudo",
                    "-n",
                    "python3",
                    str(stage / "developer_sandbox_domain_runtime.py"),
                    "materialize",
                    "--config",
                    str(stage / "runtime-domains.toml"),
                    "--domain",
                    domain,
                    "--sandbox",
                    profile.sandbox,
                    "--candidate-sha",
                    sha,
                    "--candidate-tree",
                    tree,
                    "--source-bundle",
                    str(stage / "candidate.bundle"),
                    "--execute",
                ),
            )
        finally:
            _cleanup_remote_stage(profile, sha, domain, publisher, stage)


def _install_remote_link_fleet(
    profile: Profile,
    sha: str,
    tree: str,
    authority: Identity,
) -> None:
    values = _parse_env_file(profile.secrets_env)
    program = "scripts/ops/developer_sandbox_remote_link_host.py"
    _run_candidate_program(
        profile,
        sha,
        program,
        "prepare-rotation",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--execute",
    )
    issuance = REMOTE_LINK_ISSUANCE_ROOT / profile.sandbox / sha
    _run_candidate_program(
        profile,
        sha,
        program,
        "install-server",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--credential-source",
        str(issuance / "server"),
        "--execute",
    )
    for node in ELIGIBLE_LINK_NODES:
        _verify_remote_candidate(profile, node, sha, tree, authority.gid)
        inbox = Path(
            f"/run/loom-developer-sandbox-installer/{profile.sandbox}/{sha}/{node}",
        )
        archive = _archive_credentials(
            issuance / "clients" / node,
            worker_token=values["LOOM_WORKER_TOKEN"],
            minio_access_key=values["LOOM_DEV_MINIO_ROOT_USER"],
            minio_secret_key=values["LOOM_DEV_MINIO_ROOT_PASSWORD"],
        )
        _cleanup_remote_stage(profile, sha, "credentials", node, inbox)
        try:
            _ssh(node, ("sudo", "-n", "install", "-d", "-m", "0700", str(inbox)))
            _ssh(
                node,
                (
                    "sudo",
                    "-n",
                    "tar",
                    "--extract",
                    "--file=-",
                    "--directory",
                    str(inbox),
                    "--no-same-owner",
                    "--no-same-permissions",
                ),
                input_bytes=archive,
            )
            remote_program = profile.candidate_root / sha / program
            _ssh(
                node,
                (
                    "sudo",
                    "-n",
                    "python3",
                    str(remote_program),
                    "install-client",
                    "--sandbox",
                    profile.sandbox,
                    "--candidate-sha",
                    sha,
                    "--node",
                    node,
                    "--credential-source",
                    str(inbox),
                    "--worker-token-file",
                    str(inbox / "worker-token"),
                    "--minio-access-key-file",
                    str(inbox / "minio-access-key"),
                    "--minio-secret-key-file",
                    str(inbox / "minio-secret-key"),
                    "--execute",
                ),
            )
        finally:
            _cleanup_remote_stage(profile, sha, "credentials", node, inbox)
    _run_candidate_program(
        profile,
        sha,
        program,
        "activate-server",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--execute",
    )
    _run_candidate_program(
        profile,
        sha,
        program,
        "fleet-check",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--execute",
    )


def _worker_env_seed(profile: Profile, sha: str) -> bytes:
    bundle = f"/etc/loom/developer-sandbox-links/clients/{profile.sandbox}/{sha}"
    values = {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://sandbox-link:8080",
        "LOOM_WORKER_GATEWAY_URL": "http://sandbox-link:9100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://sandbox-link:9000",
        "LOOM_WORKER_SANDBOX_IDENTITY": profile.sandbox,
        "LOOM_WORKER_CANDIDATE_SHA": sha,
        "LOOM_WORKER_TOKEN_FILE_HOST": f"{bundle}/worker-token",
        "LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST": f"{bundle}/minio-access-key",
        "LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST": f"{bundle}/minio-secret-key",
        "LOOM_WORKER_CP_TLS_CA_FILE_HOST": f"{bundle}/ca.pem",
        "LOOM_WORKER_CP_TLS_CERT_FILE_HOST": f"{bundle}/client.pem",
        "LOOM_WORKER_CP_TLS_KEY_FILE_HOST": f"{bundle}/client-key.pem",
    }
    return _render_env(values)


def _converge_domain_runtime_hosts(
    profile: Profile,
    sha: str,
    tree: str,
    authority: Identity,
) -> None:
    relative_program = "scripts/ops/developer_sandbox_domain_runtime.py"
    config_relative = "deploy/developer-sandboxes/runtime-domains.toml"
    for domain, nodes in DOMAIN_PEERS.items():
        for node in nodes:
            _verify_remote_candidate(profile, node, sha, tree, authority.gid)
            candidate = profile.candidate_root / sha
            _ssh(
                node,
                (
                    "sudo",
                    "-n",
                    "python3",
                    str(candidate / relative_program),
                    "host-converge",
                    "--config",
                    str(candidate / config_relative),
                    "--domain",
                    domain,
                    "--execute",
                ),
            )


def _publish_domain_attestations(
    profile: Profile,
    sha: str,
    tree: str,
) -> None:
    relative_program = "scripts/ops/developer_sandbox_domain_runtime.py"
    config_relative = "deploy/developer-sandboxes/runtime-domains.toml"
    seed = _worker_env_seed(profile, sha)
    for domain in DOMAIN_PEERS:
        publisher = DOMAIN_PUBLISHERS[domain]
        seed_path = Path(
            f"/run/loom-developer-sandbox-installer/{profile.sandbox}-{sha}-{domain}.env",
        )
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            info = tarfile.TarInfo(seed_path.name)
            info.size = len(seed)
            info.mode = 0o600
            info.uid = 0
            info.gid = 0
            tar.addfile(info, io.BytesIO(seed))
        try:
            _cleanup_remote_stage(
                profile,
                sha,
                f"{domain}-env",
                publisher,
                seed_path,
            )
            _ssh(
                publisher,
                ("sudo", "-n", "install", "-d", "-m", "0700", str(seed_path.parent)),
            )
            _ssh(
                publisher,
                (
                    "sudo",
                    "-n",
                    "tar",
                    "--extract",
                    "--file=-",
                    "--directory",
                    str(seed_path.parent),
                    "--no-same-owner",
                    "--no-same-permissions",
                ),
                input_bytes=archive.getvalue(),
            )
            candidate = profile.candidate_root / sha
            _ssh(
                publisher,
                (
                    "sudo",
                    "-n",
                    "python3",
                    str(candidate / relative_program),
                    "attest",
                    "--config",
                    str(candidate / config_relative),
                    "--domain",
                    domain,
                    "--sandbox",
                    profile.sandbox,
                    "--candidate-sha",
                    sha,
                    "--candidate-tree",
                    tree,
                    "--worker-env-seed",
                    str(seed_path),
                    "--execute",
                ),
            )
        finally:
            _cleanup_remote_stage(
                profile,
                sha,
                f"{domain}-env",
                publisher,
                seed_path,
            )
    _run_candidate_program(
        profile,
        sha,
        relative_program,
        "collect",
        "--config",
        str(profile.candidate_root / sha / config_relative),
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--execute",
    )


def _read_policy(profile: Profile, pool: str) -> dict[str, Any] | None:
    token = _read_admin_token(profile.admin_secret)
    environment = f"sandbox-{profile.sandbox}"
    url = (
        f"http://127.0.0.1:{profile.ports['control_plane']}"
        f"/admin/worker-pool-autoscaler-policies/{environment}/{pool}"
    )
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise HostConvergeError("capacity policy readback failed safely") from exc
    except urllib.error.URLError as exc:
        raise HostConvergeError("capacity policy readback is unavailable") from exc
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("capacity policy readback is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("capacity policy readback is invalid")
    return payload


def _assert_capacity_units_stopped(profile: Profile) -> None:
    for instance in (f"{profile.sandbox}-gb10", f"{profile.sandbox}-oldlab"):
        for suffix in ("timer", "service"):
            unit = f"loom-shared-capacity-adapter@{instance}.{suffix}"
            active = _run(("systemctl", "is-active", unit), expected={0, 3, 4})
            if active.returncode == 0:
                raise HostConvergeError("shared capacity adapter is active during prepare")
        enabled = _run(
            ("systemctl", "is-enabled", f"loom-shared-capacity-adapter@{instance}.timer"),
            expected={0, 1, 3, 4},
        )
        if enabled.returncode == 0:
            raise HostConvergeError("shared capacity adapter timer is enabled during prepare")


def assert_capacity_quiescent(profile: Profile) -> None:
    _assert_capacity_units_stopped(profile)
    for pool in ("gb10", "oldlab"):
        policy = _read_policy(profile, pool)
        if policy is None:
            continue
        lease = policy.get("capacity_lease_state")
        lease_state = lease.get("state") if isinstance(lease, dict) else None
        if policy.get("enabled") is not False or policy.get("max_slots") != 0:
            raise HostConvergeError("shared capacity policy is not drained")
        if lease_state not in {None, "retired"}:
            raise HostConvergeError("shared capacity lease is still nonterminal")


def verify_nfs_mount() -> None:
    result = _run(("findmnt", "-n", "-o", "FSTYPE,TARGET", "-T", str(NFS_ROOT)))
    fields = result.stdout.split()
    if len(fields) != 2 or fields[0] not in {"nfs", "nfs4"} or fields[1] != "/shared_work":
        raise HostConvergeError("candidate namespace is not on the expected /shared_work NFS mount")


def verify_state_parent() -> None:
    shared = _identity("root", SHARED_GROUP)
    try:
        metadata = STATE_PARENT.lstat()
    except OSError as exc:
        raise HostConvergeError("sandbox state parent is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o2750
        or (metadata.st_uid, metadata.st_gid) != (0, shared.gid)
    ):
        raise HostConvergeError("sandbox state parent owner or mode is invalid")


def _require_live_host() -> None:
    if os.geteuid() != 0:
        raise HostConvergeError("host convergence must run as root")
    hostname = socket.gethostname().rstrip(".").lower()
    if hostname != EXPECTED_HOSTNAME:
        raise HostConvergeError(
            f"host convergence requires {EXPECTED_HOSTNAME}, got {hostname}",
        )


def _migration_tree(candidate: Path, publisher: Identity) -> str:
    result = _run(
        (
            "git",
            "-c",
            f"safe.directory={candidate}",
            "-C",
            str(candidate),
            "rev-parse",
            "--verify",
            "HEAD:migrations",
        ),
        env=_clean_git_environment(),
        identity=publisher,
    )
    tree = result.stdout.strip()
    if SHA_RE.fullmatch(tree) is None:
        raise HostConvergeError("candidate migration tree is invalid")
    return tree


def verify_developer_docker_access(identity: Identity) -> None:
    _run(
        ("docker", "info", "--format", "{{.ServerVersion}}"),
        env={"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"},
        identity=identity,
        init_groups=True,
    )


def verify_candidate_consumer(profile: Profile, sha: str, identity: Identity) -> None:
    candidate = profile.candidate_root / sha
    for relative in (
        "scripts/ops/developer_sandbox.py",
        f"deploy/developer-sandboxes/{profile.sandbox}.toml",
        "deploy/docker-compose.dev.yml",
    ):
        result = _run(
            ("test", "-r", str(candidate / relative)),
            identity=identity,
            init_groups=True,
            expected={0, 1},
        )
        if result.returncode != 0:
            raise HostConvergeError(
                f"{profile.sandbox} cannot read the immutable candidate through sharedwork",
            )


def verify_candidate_profile_bytes(profile: Profile, sha: str, publisher: Identity) -> None:
    relative = f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    candidate = profile.candidate_root / sha / f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    source = _run(
        (
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            "-C",
            str(REPO_ROOT),
            "show",
            f"{sha}:{relative}",
        ),
        env=_clean_git_environment(),
        identity=publisher,
    )
    expected = hashlib.sha256(source.stdout.encode()).hexdigest()
    result = _run(("sha256sum", str(candidate)), identity=publisher)
    actual = result.stdout.split(maxsplit=1)[0] if result.stdout else ""
    if actual != expected:
        raise HostConvergeError(
            f"candidate changed the fixed host profile for {profile.sandbox}",
        )


def require_migration_compatible_update(
    profile: Profile,
    target_sha: str,
    publisher: Identity,
) -> None:
    desired = _load_json(profile.desired_file, "sandbox desired state")
    if desired is None:
        return
    current_sha = desired.get("candidate_sha")
    if not isinstance(current_sha, str) or SHA_RE.fullmatch(current_sha) is None:
        raise HostConvergeError("sandbox desired SHA is invalid")
    if current_sha == target_sha:
        return
    current = profile.candidate_root / current_sha
    verify_candidate(profile, current, current_sha, publisher)
    if _migration_tree(current, publisher) != _migration_tree(
        profile.candidate_root / target_sha,
        publisher,
    ):
        raise HostConvergeError(
            "candidate update crosses a migration-tree change; "
            "use a reviewed backup and restore workflow",
        )


def rollback(profile: Profile, target_sha: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    with _install_lock():
        desired: dict[str, Any] | None = None
        target_tree = ""
        previous_relay: str | None = None
        try:
            with _activation_lock(profile):
                orphan = _transaction_payload(profile)
                if orphan is not None:
                    _recover_transaction(profile, orphan)
                desired = _load_json(profile.desired_file, "sandbox desired state")
                if desired is None:
                    raise HostConvergeError("sandbox desired state is absent")
                current_sha = desired.get("candidate_sha")
                if target_sha != desired.get("previous_sha") or not isinstance(
                    current_sha,
                    str,
                ):
                    raise HostConvergeError(
                        "rollback target must equal the recorded previous SHA",
                    )
                authority = _identity("root", SHARED_GROUP)
                current = profile.candidate_root / current_sha
                target = profile.candidate_root / target_sha
                verify_candidate(profile, current, current_sha, authority)
                target_tree = verify_candidate(profile, target, target_sha, authority)
                if _migration_tree(current, authority) != _migration_tree(
                    target,
                    authority,
                ):
                    raise HostConvergeError(
                        "rollback crosses a migration-tree change; "
                        "restore a reviewed data backup instead",
                    )
                verify_worker_runtime_env(
                    profile,
                    target_sha,
                    _identity(
                        profile.sandbox,
                        f"loom-sandbox-{profile.sandbox}",
                    ),
                )
                receipt = _verify_archived_activation(
                    profile,
                    target_sha,
                    target_tree,
                )
                replacement = _desired_payload(
                    profile,
                    target_sha,
                    target_tree,
                    previous_sha=current_sha,
                    receipt=receipt,
                )
                previous_relay = _current_relay_sha(profile)
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="preparing",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
                _atomic_write(
                    profile.desired_file,
                    (
                        json.dumps(
                            replacement,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode(),
                    mode=0o600,
                )
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="desired-written",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
            _run(("systemctl", "start", UNIT_NAME.format(sandbox=profile.sandbox)))
            with _activation_lock(profile):
                transaction = _transaction_payload(profile)
                if (
                    transaction is None
                    or transaction["operation"] != "rollback"
                    or transaction["candidate_sha"] != target_sha
                    or transaction["phase"] != "desired-written"
                ):
                    raise HostConvergeError(
                        "rollback transaction changed during sandbox convergence",
                    )
                _restore_relay(profile, target_sha, current_sha)
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="link-installed",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
                _renew_attestation_locked(
                    profile,
                    sha=target_sha,
                    tree=target_tree,
                )
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="domains-proved",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
                service_check(profile.sandbox)
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="committed",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
                _remove_transaction(profile)
        except Exception:
            with _activation_lock(profile):
                transaction = _transaction_payload(profile)
                if transaction is not None:
                    try:
                        _recover_transaction(profile, transaction)
                    except Exception as recovery_exc:
                        raise HostConvergeError(
                            f"{profile.sandbox} rollback and previous-candidate "
                            "recovery both failed",
                        ) from recovery_exc
            raise


def _nfs_readback_commands(profile: Profile, sha: str) -> list[list[str]]:
    path = profile.candidate_root / sha
    remote = ["stat", "-Lc", "%i:%u:%g:%a:%n", str(profile.candidate_root), str(path)]
    return [
        ["ssh", "-o", "BatchMode=yes", host, "--", *remote]
        for host in ("oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4", "oldlab-5")
    ]


def plan_document(profiles: Sequence[Profile], sha: str, operation: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate SHA must be full lowercase 40-hex")
    rows = []
    for profile in profiles:
        rows.append(
            {
                "sandbox": profile.sandbox,
                "compose_project": profile.compose_project,
                "candidate": str(profile.candidate_root / sha),
                "candidate_owner": f"root:{SHARED_GROUP}",
                "candidate_group_world_writable": False,
                "worker_runtime_env": str(profile.worker_runtime_env(sha)),
                "combined_receipt": str(combined_receipt_path(profile, sha)),
                "state_root": str(profile.state_root),
                "private_owner": f"{profile.sandbox}:{SHARED_GROUP}",
                "private_mode": "0700",
                "secrets_env": str(profile.secrets_env),
                "admin_secret_file": str(profile.admin_secret),
                "secret_mode": "0600",
                "ports": profile.ports,
                "unit": UNIT_NAME.format(sandbox=profile.sandbox),
                "nfs_readback_commands": _nfs_readback_commands(profile, sha),
            },
        )
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-host-plan",
        "operation": operation,
        "mutation_authorized": False,
        "host": EXPECTED_HOSTNAME,
        "candidate_sha": sha,
        "sandboxes": rows,
        "rollback": {
            "preserves_compose_volumes": True,
            "requires_recorded_previous_sha": True,
            "requires_equal_migration_tree": True,
        },
    }


def _transaction_file(profile: Profile) -> Path:
    return TRANSACTION_ROOT / f"{profile.sandbox}.json"


@contextmanager
def _activation_lock(profile: Profile) -> Iterator[None]:
    _ensure_root_private_directory(TRANSACTION_LOCK_ROOT)
    lock_path = TRANSACTION_LOCK_ROOT / f"{profile.sandbox}.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
        ):
            raise HostConvergeError("sandbox activation lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


@contextmanager
def _install_lock() -> Iterator[None]:
    _ensure_root_private_directory(TRANSACTION_LOCK_ROOT)
    lock_path = TRANSACTION_LOCK_ROOT / "install.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
        ):
            raise HostConvergeError("global install lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _write_transaction(
    profile: Profile,
    *,
    operation: str = "install",
    sha: str,
    tree: str,
    phase: str,
    previous_desired: Mapping[str, Any] | None,
    previous_relay_sha: str | None,
) -> None:
    now = datetime.now(UTC)
    existing = _load_json(_transaction_file(profile), "sandbox activation transaction")
    started_at = now
    expires_at = now + TRANSACTION_TTL
    if (
        isinstance(existing, dict)
        and existing.get("sandbox") == profile.sandbox
        and existing.get("operation", "install") == operation
        and existing.get("candidate_sha") == sha
        and existing.get("candidate_tree") == tree
    ):
        started_at = _parse_attestation_time(
            existing.get("started_at"),
            "transaction started_at",
        )
        expires_at = _parse_attestation_time(
            existing.get("expires_at"),
            "transaction expires_at",
        )
    payload = {
        "schema_version": 2,
        "sandbox": profile.sandbox,
        "operation": operation,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "phase": phase,
        "started_at": started_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "previous_desired": dict(previous_desired) if previous_desired is not None else None,
        "previous_relay_sha": previous_relay_sha,
    }
    _ensure_root_private_directory(TRANSACTION_ROOT)
    _atomic_write(
        _transaction_file(profile),
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )


def _transaction_payload(profile: Profile) -> dict[str, Any] | None:
    payload = _load_json(_transaction_file(profile), "sandbox activation transaction")
    if payload is None:
        return None
    schema_version = payload.get("schema_version")
    expected_keys = {
        "schema_version",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "phase",
        "started_at",
        "expires_at",
        "previous_desired",
        "previous_relay_sha",
    }
    if schema_version == 2:
        expected_keys.add("operation")
    _exact_keys(
        payload,
        expected_keys,
        "sandbox activation transaction",
    )
    operation = payload.get("operation", "install")
    if (
        schema_version not in {1, 2}
        or payload["sandbox"] != profile.sandbox
        or operation not in {"install", "rollback"}
        or SHA_RE.fullmatch(str(payload["candidate_sha"])) is None
        or SHA_RE.fullmatch(str(payload["candidate_tree"])) is None
        or payload["phase"]
        not in {
            "prepared",
            "preparing",
            "link-installed",
            "fleet-proved",
            "domains-proved",
            "desired-written",
            "committed",
        }
        or (
            payload["previous_desired"] is not None
            and not isinstance(payload["previous_desired"], dict)
        )
        or (
            payload["previous_relay_sha"] is not None
            and SHA_RE.fullmatch(str(payload["previous_relay_sha"])) is None
        )
    ):
        raise HostConvergeError("sandbox activation transaction binding is invalid")
    _parse_attestation_time(payload["started_at"], "transaction started_at")
    _parse_attestation_time(payload["expires_at"], "transaction expires_at")
    payload["operation"] = operation
    return payload


def _remove_transaction(profile: Profile) -> None:
    path = _transaction_file(profile)
    if path.exists():
        path.unlink()
        _fsync_directory(path.parent)


def _current_relay_sha(profile: Profile) -> str | None:
    current = REMOTE_LINK_SERVER_ROOT / profile.sandbox / "current"
    try:
        metadata = current.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HostConvergeError("sandbox relay current pointer is unavailable") from exc
    if not stat.S_ISLNK(metadata.st_mode):
        raise HostConvergeError("sandbox relay current pointer is invalid")
    target = os.readlink(current)
    prefix = "candidates/"
    sha = target.removeprefix(prefix)
    if target != prefix + sha or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("sandbox relay current pointer is invalid")
    return sha


def _restore_relay(profile: Profile, target_sha: str | None, transaction_sha: str) -> None:
    program = "scripts/ops/developer_sandbox_remote_link_host.py"
    if target_sha is not None:
        _run_candidate_program(
            profile,
            target_sha,
            program,
            "rollback-server",
            "--sandbox",
            profile.sandbox,
            "--candidate-sha",
            target_sha,
            "--execute",
        )
        return
    unit = f"loom-developer-sandbox-link@{profile.sandbox}.service"
    _run(("systemctl", "disable", "--now", unit), expected={0, 1, 5})
    current = REMOTE_LINK_SERVER_ROOT / profile.sandbox / "current"
    if current.is_symlink():
        current.unlink()
        _fsync_directory(current.parent)


def _invalidate_receipt(profile: Profile, sha: str) -> None:
    path = combined_receipt_path(profile, sha)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(metadata.st_mode):
        raise HostConvergeError("combined activation receipt path is a directory")
    path.unlink()
    _fsync_directory(path.parent)


def _recover_transaction(profile: Profile, transaction: Mapping[str, Any]) -> None:
    if transaction.get("phase") == "committed":
        _remove_transaction(profile)
        return
    sha = str(transaction["candidate_sha"])
    operation = str(transaction.get("operation", "install"))
    previous = transaction["previous_desired"]
    previous_relay = transaction["previous_relay_sha"]
    if previous is None:
        if profile.desired_file.exists():
            profile.desired_file.unlink()
            _fsync_directory(profile.desired_file.parent)
        current_state = _sandbox_state_sha(profile)
        lifecycle_operation = "destroy" if current_state == sha else "prepare-stop"
        try:
            _invoke_lifecycle(profile, sha, lifecycle_operation)
        except HostConvergeError:
            if lifecycle_operation != "prepare-stop":
                raise
    else:
        previous_sha = previous.get("candidate_sha")
        if not isinstance(previous_sha, str) or SHA_RE.fullmatch(previous_sha) is None:
            raise HostConvergeError("previous desired state in transaction is invalid")
        _atomic_write(
            profile.desired_file,
            (json.dumps(previous, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            mode=0o600,
        )
        _invoke_lifecycle(profile, previous_sha, "update")
    _restore_relay(profile, previous_relay, sha)
    if operation == "install":
        _invalidate_receipt(profile, sha)
    _remove_transaction(profile)


def _install_materialized(
    profiles: Sequence[Profile],
    sha: str,
    source_bundle: Path,
    source_tree: str,
) -> None:
    authority = _identity("root", SHARED_GROUP)
    assets_installed = False
    fingerprints: dict[tuple[str, str], str] = {}
    candidates: list[tuple[Profile, str, Identity]] = []
    if profiles:
        _bootstrap_domain_runtime_hosts(profiles[0], sha)
    for profile in profiles:
        owner = _identity(profile.sandbox, SHARED_GROUP)
        runtime_group = _identity(profile.sandbox, f"loom-sandbox-{profile.sandbox}")
        verify_developer_docker_access(owner)
        ensure_secret_files(profile, owner)
        _materialize_domain_candidates(profile, sha, source_tree, source_bundle)
        verify_candidate_root(profile, authority)
        tree = verify_candidate(profile, profile.candidate_root / sha, sha, authority)
        if tree != source_tree:
            raise HostConvergeError("materialized candidate tree differs from source bundle")
        if not assets_installed:
            _install_assets(profile.candidate_root / sha)
            assets_installed = True
        verify_candidate_profile_bytes(profile, sha, authority)
        verify_candidate_consumer(profile, sha, owner)
        require_migration_compatible_update(profile, sha, authority)
        _converge_domain_runtime_hosts(profile, sha, tree, authority)
        values = _parse_env_file(profile.secrets_env)
        admin = _read_admin_token(profile.admin_secret)
        for key in (
            "LOOM_DEV_POSTGRES_PASSWORD",
            "LOOM_DEV_MINIO_ROOT_PASSWORD",
            "LOOM_CP_STEP_JWT_SIGNING_KEY",
            "LOOM_SECRET_STORE_MASTER_KEY",
            "LOOM_WORKER_TOKEN",
        ):
            fingerprints[(profile.sandbox, key)] = hashlib.sha256(
                values[key].encode(),
            ).hexdigest()
        fingerprints[(profile.sandbox, "admin")] = hashlib.sha256(
            admin.encode(),
        ).hexdigest()
        candidates.append((profile, tree, runtime_group))
    for key in {key for _, key in fingerprints}:
        matching_fingerprints = [
            fingerprint
            for (sandbox, candidate_key), fingerprint in fingerprints.items()
            if candidate_key == key
        ]
        if len(matching_fingerprints) != len(set(matching_fingerprints)):
            raise HostConvergeError(f"cross-sandbox secret collision detected for {key}")
    for profile, tree, runtime_group in candidates:
        previous: dict[str, Any] | None = None
        previous_relay: str | None = None
        try:
            with _activation_lock(profile):
                orphan = _transaction_payload(profile)
                if orphan is not None:
                    _recover_transaction(profile, orphan)
                previous = _load_json(profile.desired_file, "sandbox desired state")
                previous_relay = _current_relay_sha(profile)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="preparing",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _assert_capacity_units_stopped(profile)
                _invoke_lifecycle(profile, sha, "prepare")
                verify_listening_ports(profile)
                assert_capacity_quiescent(profile)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="prepared",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _install_remote_link_fleet(profile, sha, tree, authority)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="fleet-proved",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _publish_domain_attestations(profile, sha, tree)
                verify_worker_runtime_env(profile, sha, runtime_group)
                receipt = verify_combined_receipt(profile, sha, tree)
                _archive_runtime_attestation(profile, sha, tree, receipt)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="domains-proved",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                write_desired(profile, sha, tree, receipt)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="desired-written",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
            unit = UNIT_NAME.format(sandbox=profile.sandbox)
            _run(("systemctl", "enable", unit))
            _run(("systemctl", "restart", unit))
            with _activation_lock(profile):
                service_check(profile.sandbox)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="committed",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _remove_transaction(profile)
        except Exception:
            with _activation_lock(profile):
                transaction = _transaction_payload(profile)
                if transaction is not None:
                    try:
                        _recover_transaction(profile, transaction)
                    except Exception as recovery_exc:
                        raise HostConvergeError(
                            f"{profile.sandbox} activation and previous-candidate "
                            "recovery both failed",
                        ) from recovery_exc
            raise
    if candidates:
        _run(("systemctl", "enable", "--now", RENEWAL_TIMER))


def install(profiles: Sequence[Profile], sha: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    with _install_lock():
        with _candidate_source_stage(sha) as (source_bundle, source_tree):
            _install_materialized(
                profiles,
                sha,
                source_bundle,
                source_tree,
            )


def _select_profiles(all_profiles: Sequence[Profile], sandbox: str) -> tuple[Profile, ...]:
    if sandbox == "all":
        return tuple(all_profiles)
    return tuple(profile for profile in all_profiles if profile.sandbox == sandbox)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "install", "check"):
        child = subparsers.add_parser(command)
        child.add_argument("--candidate-sha", required=True)
        child.add_argument("--sandbox", choices=(*SANDBOXES, "all"), default="all")
        if command != "plan":
            child.add_argument("--execute", action="store_true")
    rollback_parser = subparsers.add_parser("rollback")
    rollback_parser.add_argument("--sandbox", choices=SANDBOXES, required=True)
    rollback_parser.add_argument("--candidate-sha", required=True)
    rollback_parser.add_argument("--execute", action="store_true")
    for command in ("service-converge", "service-check"):
        child = subparsers.add_parser(command)
        child.add_argument("--sandbox", choices=SANDBOXES, required=True)
    renew = subparsers.add_parser("renew-attestations")
    renew.add_argument("--sandbox", choices=(*SANDBOXES, "all"), default="all")
    renew.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "service-converge":
            service_converge(args.sandbox)
            result = {"status": "succeeded", "sandbox": args.sandbox}
        elif args.command == "service-check":
            service_check(args.sandbox)
            result = {"status": "succeeded", "sandbox": args.sandbox}
        elif args.command == "renew-attestations":
            profiles = load_profiles()
            selected = _select_profiles(profiles, args.sandbox)
            renew_attestations(selected, execute=args.execute)
            result = {
                "status": "succeeded",
                "sandboxes": [profile.sandbox for profile in selected],
            }
        else:
            profiles = load_profiles()
            selected = _select_profiles(profiles, args.sandbox)
            result = plan_document(selected, args.candidate_sha, args.command)
            execute = bool(getattr(args, "execute", False))
            if execute and args.command == "install":
                install(selected, args.candidate_sha)
                result = {**result, "mutation_authorized": True, "status": "succeeded"}
            elif execute and args.command == "check":
                for profile in selected:
                    service_check(profile.sandbox)
                result = {
                    **result,
                    "mutation_authorized": False,
                    "verified": True,
                    "status": "succeeded",
                }
            elif execute and args.command == "rollback":
                rollback(selected[0], args.candidate_sha)
                result = {**result, "mutation_authorized": True, "status": "succeeded"}
        sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except HostConvergeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
