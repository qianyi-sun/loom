#!/usr/bin/env python3
"""Install the GB10 autoscaler controller from an exact sealed checkout."""

from __future__ import annotations

import argparse
import base64
import binascii
import ctypes
import hashlib
import json
import os
import platform
import pwd
import re
import signal
import socket
import stat
import struct
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn, Protocol

_REMOTE_URL = "https://github.com/qianyi-sun/loom.git"
_CONTROLLER = "gx10-01c7"
_CLUSTER = "trt-gb10"
_SERVICE_HOME = Path("/var/lib/loom-rollout")
_SERVICE_UID = 995
_SERVICE_GID = 2007
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
_SOURCE_ARTIFACTS = (
    Path("deploy/slurm/install-loom-gb10-autoscaler-controller.sh"),
    Path("deploy/slurm/loom-gb10-slurm-authority.tmpfiles"),
    Path("scripts/ops/gb10_controller_bootstrap.py"),
    Path("scripts/ops/gb10_external_supervisor_broker.py"),
    Path("scripts/ops/gb10_slurm_acceptance_authority.py"),
    Path("scripts/ops/install_gb10_autoscaler_controller.py"),
)
_ACCEPTANCE_AUTHORITY_RELATIVE = Path("scripts/ops/gb10_slurm_acceptance_authority.py")
_ACCEPTANCE_TMPFILES_RELATIVE = Path("deploy/slurm/loom-gb10-slurm-authority.tmpfiles")
_BROKER_RELATIVE = Path("scripts/ops/gb10_external_supervisor_broker.py")
_TMPFILES_PAYLOAD = (
    b"d /run/loom-gb10-slurm-authority 0700 root root -\n"
    b"d /run/loom-gb10-slurm-authority/jobs 0700 root root -\n"
    b"f /run/loom-gb10-slurm-authority/acceptance.lock 0600 root root -\n"
)
_SUDOERS_PAYLOAD = (
    b"qianyi ALL=(root) NOPASSWD:NOSETENV: "
    b'/usr/local/libexec/loom-gb10-external-supervisor-broker ""\n'
)
_ROOT_ENVIRONMENT = {
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_NOSYSTEM": "1",
    "HOME": "/",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}


class ControllerInstallError(RuntimeError):
    """The controller install request is not safe to execute."""


@dataclass(frozen=True, slots=True)
class HostFacts:
    """Host identity observations required by the controller bootstrap."""

    machine: str
    hostname: str
    cluster_name: str
    service_uid: int
    service_gid: int
    service_home: Path


def validate_host_facts(facts: HostFacts) -> None:
    if facts != HostFacts(
        machine="aarch64",
        hostname=_CONTROLLER,
        cluster_name=_CLUSTER,
        service_uid=_SERVICE_UID,
        service_gid=_SERVICE_GID,
        service_home=_SERVICE_HOME,
    ):
        _fail("controller installer host identity is invalid")


@dataclass(frozen=True, slots=True)
class InstallContext:
    """Exact immutable inputs and filesystem boundary for one controller install."""

    trusted_source_root: Path
    source_root: Path
    source_sha: str
    kubectl_source: Path
    uv_source: Path
    controller_public_key: Path
    legacy_public_key: Path
    system_root: Path = Path("/")
    authority_uid: int = 0
    authority_gid: int = 0
    service_uid: int = 995
    service_gid: int = 2007


class InstallBackend(Protocol):
    """External validators and the final atomic SSH authority publisher."""

    def validate_host(self) -> None: ...

    def validate_kubectl(self, path: Path) -> None: ...

    def validate_uv(self, path: Path) -> None: ...

    def validate_acceptance(self, path: Path) -> None: ...

    def validate_sudoers(self, path: Path) -> None: ...

    def publish_authority(
        self,
        broker_path: Path,
        controller_public_key: Path,
        legacy_public_key: Path,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class _FileSnapshot:
    path: Path
    payload: bytes | None
    mode: int | None
    uid: int | None
    gid: int | None


def _rename_noreplace(source: Path, destination: Path) -> None:
    """Atomically publish a directory without replacing a concurrent target."""

    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as exc:  # pragma: no cover - GB10 glibc provides renameat2
        raise ControllerInstallError("controller installer atomic rename is unavailable") from exc
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(source),
        -100,
        os.fsencode(destination),
        1,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), destination)


def _safe_directory_staging_parent(
    destination_parent: Path,
    *,
    authority_uid: int,
    authority_gid: int,
) -> Path:
    try:
        destination_metadata = os.lstat(destination_parent)
    except OSError as exc:
        raise ControllerInstallError(
            "controller installer directory parent is unavailable"
        ) from exc
    if not stat.S_ISDIR(destination_metadata.st_mode) or stat.S_ISLNK(destination_metadata.st_mode):
        raise ControllerInstallError("controller installer directory parent is unsafe")
    device = destination_metadata.st_dev
    candidate = destination_parent
    while True:
        metadata = os.lstat(candidate)
        if (
            stat.S_ISDIR(metadata.st_mode)
            and not stat.S_ISLNK(metadata.st_mode)
            and metadata.st_dev == device
            and metadata.st_uid == authority_uid
            and metadata.st_gid == authority_gid
            and not stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            return candidate
        parent = candidate.parent
        if parent == candidate:
            break
        candidate = parent
    raise ControllerInstallError("controller installer has no safe directory staging parent")


def _create_directory_atomically(
    path: Path,
    *,
    mode: int,
    uid: int,
    gid: int,
    authority_uid: int,
    authority_gid: int,
) -> tuple[int, int]:
    staging_parent = _safe_directory_staging_parent(
        path.parent,
        authority_uid=authority_uid,
        authority_gid=authority_gid,
    )
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".loom-directory-{path.name}.",
            dir=staging_parent,
        )
    )
    created = os.lstat(temporary)
    if not stat.S_ISDIR(created.st_mode) or stat.S_ISLNK(created.st_mode):
        _fail("controller installer directory staging is unsafe")
    identity = (created.st_dev, created.st_ino)
    published = False
    try:
        os.chown(temporary, uid, gid)
        os.chmod(temporary, mode)
        _fsync_directory(temporary)
        _rename_noreplace(temporary, path)
        published = True
        return identity
    finally:
        if not published and os.path.lexists(temporary):
            metadata = os.lstat(temporary)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != identity
            ):
                raise ControllerInstallError(
                    "controller installer directory staging cleanup is unsafe"
                )
            os.rmdir(temporary)
            _fsync_directory(staging_parent)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, payload: bytes, *, mode: int, uid: int, gid: int) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        os.fchown(descriptor, uid, gid)
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                _fail("controller installer staged file write failed")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if os.path.lexists(temporary):
            os.unlink(temporary)


def _snapshot_file(path: Path) -> _FileSnapshot:
    if not os.path.lexists(path):
        return _FileSnapshot(path=path, payload=None, mode=None, uid=None, gid=None)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            _fail("controller installer managed file is unsafe")
        payload = bytearray()
        while chunk := os.read(descriptor, 64 * 1024):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(after):
            _fail("controller installer managed file changed during inspection")
        return _FileSnapshot(
            path=path,
            payload=bytes(payload),
            mode=stat.S_IMODE(before.st_mode),
            uid=before.st_uid,
            gid=before.st_gid,
        )
    except OSError as exc:
        raise ControllerInstallError("controller installer managed file is unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


class AtomicFileTransaction:
    """Publish files atomically and restore their exact prior state on failure."""

    def __init__(
        self,
        *,
        authority_uid: int | None = None,
        authority_gid: int | None = None,
    ) -> None:
        self._snapshots: list[_FileSnapshot] = []
        self._paths: set[Path] = set()
        self._created_directories: list[tuple[Path, tuple[int, int]]] = []
        self._finalized = False
        self._previous_signal_mask: set[int | signal.Signals] | None = None
        self._authority_uid = os.geteuid() if authority_uid is None else authority_uid
        self._authority_gid = os.getegid() if authority_gid is None else authority_gid
        if self._authority_uid < 0 or self._authority_gid < 0:
            raise ControllerInstallError("controller installer transaction authority is invalid")

    def __enter__(self) -> AtomicFileTransaction:
        if self._finalized or self._previous_signal_mask is not None:
            _fail("controller installer transaction is finalized")
        self._previous_signal_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK,
            {signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM},
        )
        return self

    def _require_open(self) -> None:
        if self._finalized or self._previous_signal_mask is None:
            _fail("controller installer transaction is not active")

    def _restore_signal_mask(self) -> None:
        previous = self._previous_signal_mask
        self._previous_signal_mask = None
        if previous is not None:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    def publish(self, path: Path, payload: bytes, *, mode: int, uid: int, gid: int) -> None:
        self._require_open()
        if (
            not path.is_absolute()
            or ".." in path.parts
            or not isinstance(payload, bytes)
            or not 0 <= mode <= 0o777
            or uid < 0
            or gid < 0
        ):
            _fail("controller installer publication request is invalid")
        if path not in self._paths:
            self._snapshots.append(_snapshot_file(path))
            self._paths.add(path)
        _atomic_write(path, payload, mode=mode, uid=uid, gid=gid)

    def ensure_file(self, path: Path, payload: bytes, *, mode: int, uid: int, gid: int) -> None:
        self._require_open()
        if os.path.lexists(path):
            snapshot = _snapshot_file(path)
            if (
                snapshot.payload != payload
                or snapshot.mode != mode
                or snapshot.uid != uid
                or snapshot.gid != gid
            ):
                _fail("controller installer managed persistent file is unsafe")
            return
        self.publish(path, payload, mode=mode, uid=uid, gid=gid)

    def ensure_directory(self, path: Path, *, mode: int, uid: int, gid: int) -> None:
        self._require_open()
        if (
            not path.is_absolute()
            or ".." in path.parts
            or not 0 <= mode <= 0o777
            or uid < 0
            or gid < 0
        ):
            _fail("controller installer directory request is invalid")
        if os.path.lexists(path):
            metadata = os.lstat(path)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != uid
                or metadata.st_gid != gid
                or stat.S_IMODE(metadata.st_mode) != mode
            ):
                _fail("controller installer managed directory is unsafe")
            return
        identity = _create_directory_atomically(
            path,
            mode=mode,
            uid=uid,
            gid=gid,
            authority_uid=self._authority_uid,
            authority_gid=self._authority_gid,
        )
        self._created_directories.append((path, identity))
        _fsync_directory(path.parent)

    def _rollback(self) -> None:
        for snapshot in reversed(self._snapshots):
            if snapshot.payload is None:
                if os.path.lexists(snapshot.path):
                    metadata = os.lstat(snapshot.path)
                    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                        _fail("controller installer rollback target is unsafe")
                    os.unlink(snapshot.path)
                    _fsync_directory(snapshot.path.parent)
                continue
            assert snapshot.mode is not None
            assert snapshot.uid is not None
            assert snapshot.gid is not None
            _atomic_write(
                snapshot.path,
                snapshot.payload,
                mode=snapshot.mode,
                uid=snapshot.uid,
                gid=snapshot.gid,
            )
        for path, identity in reversed(self._created_directories):
            metadata = os.lstat(path)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != identity
            ):
                _fail("controller installer rollback directory changed")
            os.rmdir(path)
            _fsync_directory(path.parent)

    def rollback(self) -> None:
        self._require_open()
        try:
            self._rollback()
            self._snapshots.clear()
            self._paths.clear()
            self._created_directories.clear()
            self._finalized = True
        finally:
            self._restore_signal_mask()

    def commit(self) -> None:
        self._require_open()
        self._snapshots.clear()
        self._paths.clear()
        self._created_directories.clear()
        self._finalized = True
        self._restore_signal_mask()

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> Literal[False]:
        del exception_type, traceback
        if exception is None:
            return False
        try:
            self.rollback()
        except (ControllerInstallError, OSError) as rollback_error:
            raise ControllerInstallError("controller installer rollback failed") from rollback_error
        return False


def _fail(message: str) -> NoReturn:
    raise ControllerInstallError(message)


def _directory_metadata(path: Path, *, uid: int, gid: int, label: str) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise ControllerInstallError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        _fail(f"{label} is unsafe")


def _validate_source_directories(
    trusted_root: Path,
    source_root: Path,
    source_sha: str,
    *,
    uid: int,
    gid: int,
) -> None:
    if (
        not trusted_root.is_absolute()
        or not source_root.is_absolute()
        or ".." in trusted_root.parts
        or ".." in source_root.parts
        or _SHA_RE.fullmatch(source_sha) is None
        or source_root != trusted_root / source_sha
    ):
        _fail("controller installer source identity is invalid")
    _directory_metadata(
        trusted_root,
        uid=uid,
        gid=gid,
        label="controller installer trusted root",
    )
    _directory_metadata(
        source_root,
        uid=uid,
        gid=gid,
        label="controller installer source root",
    )


def _git(source_root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            [
                "/usr/bin/git",
                "--no-replace-objects",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.hooksPath=/dev/null",
                "-C",
                str(source_root),
                *arguments,
            ],
            capture_output=True,
            check=False,
            env=_ROOT_ENVIRONMENT,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ControllerInstallError("controller installer Git source is unavailable") from exc
    if completed.returncode != 0 or completed.stderr:
        _fail("controller installer Git source is invalid")
    return completed.stdout.strip()


def _validate_git_source(source_root: Path, source_sha: str) -> None:
    forbidden = (
        source_root / ".git/objects/info/alternates",
        source_root / ".git/info/grafts",
        source_root / ".git/shallow",
        source_root / ".git/refs/replace",
    )
    if any(os.path.lexists(path) for path in forbidden):
        _fail("controller installer Git indirection is unsupported")
    if (
        _git(source_root, "rev-parse", "HEAD") != source_sha
        or _git(source_root, "rev-parse", "--abbrev-ref", "HEAD") != "HEAD"
        or _git(source_root, "remote") != "origin"
        or _git(source_root, "config", "--get-all", "remote.origin.url") != _REMOTE_URL
        or _git(source_root, "status", "--porcelain=v1", "--untracked-files=all")
    ):
        _fail("controller installer Git source identity drifted")
    push_url = subprocess.run(
        [
            "/usr/bin/git",
            "--no-replace-objects",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-C",
            str(source_root),
            "config",
            "--get-all",
            "remote.origin.pushurl",
        ],
        capture_output=True,
        check=False,
        env=_ROOT_ENVIRONMENT,
        text=True,
        timeout=30,
    )
    if not (push_url.returncode == 1 and push_url.stdout == "" and push_url.stderr == ""):
        _fail("controller installer Git source push authority appeared")


def _stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_artifact(
    source_root: Path,
    relative: Path,
    *,
    uid: int,
    gid: int,
) -> bytes:
    current = source_root
    for part in relative.parts[:-1]:
        current /= part
        _directory_metadata(
            current,
            uid=uid,
            gid=gid,
            label="controller installer source directory",
        )
    path = source_root / relative
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != uid
            or before.st_gid != gid
            or stat.S_IMODE(before.st_mode) & 0o022
            or not 1 <= before.st_size <= _MAX_ARTIFACT_BYTES
        ):
            _fail("controller installer source artifact is unsafe")
        payload = bytearray()
        while chunk := os.read(descriptor, min(64 * 1024, before.st_size + 1 - len(payload))):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) != before.st_size or _stat_identity(before) != _stat_identity(after):
            _fail("controller installer source artifact changed during inspection")
        return bytes(payload)
    except OSError as exc:
        raise ControllerInstallError("controller installer source artifact is unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_external_input(
    path: Path,
    *,
    uid: int,
    gid: int,
    maximum_bytes: int,
    label: str,
) -> bytes:
    if not path.is_absolute() or ".." in path.parts:
        _fail(f"{label} path is invalid")
    _directory_metadata(path.parent, uid=uid, gid=gid, label=f"{label} parent")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != uid
            or before.st_gid != gid
            or stat.S_IMODE(before.st_mode) & 0o022
            or not 1 <= before.st_size <= maximum_bytes
        ):
            _fail(f"{label} metadata is unsafe")
        payload = bytearray()
        while chunk := os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - len(payload))):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) != before.st_size or _stat_identity(before) != _stat_identity(after):
            _fail(f"{label} changed during inspection")
        return bytes(payload)
    except OSError as exc:
        raise ControllerInstallError(f"{label} is unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _public_key_identity(payload: bytes) -> tuple[str, str]:
    try:
        fields = payload.decode("ascii").strip().split()
    except UnicodeError as exc:
        raise ControllerInstallError("controller installer public key is invalid") from exc
    if len(fields) not in {2, 3} or fields[0] != "ssh-ed25519":
        _fail("controller installer public key is invalid")
    try:
        decoded = base64.b64decode(fields[1], validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ControllerInstallError("controller installer public key is invalid") from exc
    algorithm = b"ssh-ed25519"
    prefix = struct.pack(">I", len(algorithm)) + algorithm
    offset = len(prefix)
    if (
        not decoded.startswith(prefix)
        or len(decoded) < offset + 4
        or struct.unpack(">I", decoded[offset : offset + 4])[0] != 32
        or len(decoded) != offset + 36
        or base64.b64encode(decoded).decode("ascii") != fields[1]
    ):
        _fail("controller installer public key is invalid")
    return fields[0], fields[1]


def _system_path(context: InstallContext, absolute: str) -> Path:
    target = Path(absolute)
    if not target.is_absolute() or ".." in target.parts:
        _fail("controller installer managed path is invalid")
    return context.system_root / target.relative_to("/")


def _validate_context(context: InstallContext) -> None:
    if (
        not context.system_root.is_absolute()
        or ".." in context.system_root.parts
        or context.authority_uid < 0
        or context.authority_gid < 0
        or context.service_uid < 0
        or context.service_gid < 0
    ):
        _fail("controller installer context is invalid")
    _directory_metadata(
        context.system_root,
        uid=context.authority_uid,
        gid=context.authority_gid,
        label="controller installer system root",
    )


def _validate_unmanaged_ancestors(context: InstallContext) -> None:
    for absolute in (
        "/usr",
        "/usr/local",
        "/usr/local/bin",
        "/etc",
        "/opt",
        "/var",
        "/var/lib",
        "/run",
    ):
        _directory_metadata(
            _system_path(context, absolute),
            uid=context.authority_uid,
            gid=context.authority_gid,
            label="controller installer managed ancestor",
        )


def _validate_optional_kubeconfig(context: InstallContext) -> None:
    path = _system_path(context, "/var/lib/loom-staging-rollout/kubeconfig")
    if not os.path.lexists(path):
        return
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != context.service_uid
            or metadata.st_gid != context.service_gid
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            _fail("existing autoscaler kubeconfig metadata is unsafe")
    except OSError as exc:
        raise ControllerInstallError("existing autoscaler kubeconfig is unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


@contextmanager
def _staged_payload(parent: Path, payload: bytes) -> Iterator[Path]:
    descriptor, name = tempfile.mkstemp(prefix=".loom-controller-install.", dir=parent)
    path = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                _fail("controller installer validation staging failed")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        yield path
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if os.path.lexists(path):
            os.unlink(path)


def _assert_file_state(
    path: Path,
    payload: bytes,
    *,
    mode: int,
    uid: int,
    gid: int,
) -> None:
    snapshot = _snapshot_file(path)
    if (
        snapshot.payload != payload
        or snapshot.mode != mode
        or snapshot.uid != uid
        or snapshot.gid != gid
    ):
        _fail("controller installer file readback failed")


class SubprocessBackend:
    """Production boundary for fixed host probes and executable readbacks."""

    def __init__(
        self,
        *,
        host_facts: HostFacts | None = None,
        visudo_path: Path = Path("/usr/sbin/visudo"),
    ) -> None:
        self._host_facts = host_facts
        self._visudo_path = visudo_path

    @staticmethod
    def _run(
        arguments: list[str],
        *,
        timeout: int | None = 30,
    ) -> subprocess.CompletedProcess[bytes]:
        if (
            not arguments
            or not Path(arguments[0]).is_absolute()
            or any(not argument or "\x00" in argument for argument in arguments)
        ):
            _fail("controller installer subprocess request is invalid")
        try:
            completed = subprocess.run(
                arguments,
                capture_output=True,
                check=False,
                env=_ROOT_ENVIRONMENT,
                input=b"",
                timeout=timeout,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ControllerInstallError("controller installer subprocess is unavailable") from exc
        if (
            completed.returncode != 0
            or len(completed.stdout) > 1024 * 1024
            or len(completed.stderr) > 1024 * 1024
        ):
            _fail("controller installer subprocess failed")
        return completed

    def validate_host(self) -> None:
        if self._host_facts is None:
            try:
                account = pwd.getpwnam("loom-rollout")
            except KeyError as exc:
                raise ControllerInstallError(
                    "controller installer service identity is absent"
                ) from exc
            cluster_result = self._run(["/usr/bin/scontrol", "show", "config"])
            if cluster_result.stderr:
                _fail("controller installer Slurm probe is invalid")
            try:
                cluster_lines = cluster_result.stdout.decode("utf-8").splitlines()
            except UnicodeError as exc:
                raise ControllerInstallError("controller installer Slurm probe is invalid") from exc
            clusters = [
                line.split("=", 1)[1].strip()
                for line in cluster_lines
                if "=" in line and line.split("=", 1)[0].strip() == "ClusterName"
            ]
            if len(clusters) != 1:
                _fail("controller installer Slurm probe is invalid")
            facts = HostFacts(
                machine=platform.machine(),
                hostname=socket.gethostname().split(".", 1)[0],
                cluster_name=clusters[0],
                service_uid=account.pw_uid,
                service_gid=account.pw_gid,
                service_home=Path(account.pw_dir),
            )
        else:
            facts = self._host_facts
        validate_host_facts(facts)

    def validate_kubectl(self, path: Path) -> None:
        result = self._run([str(path), "version", "--client", "-o", "json"])
        if result.stderr:
            _fail("controller installer kubectl readback is invalid")
        try:
            document = json.loads(result.stdout)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ControllerInstallError(
                "controller installer kubectl readback is invalid"
            ) from exc
        client = document.get("clientVersion") if isinstance(document, dict) else None
        if not isinstance(client, dict) or client.get("gitVersion") != "v1.36.2":
            _fail("controller installer kubectl readback is invalid")

    def validate_uv(self, path: Path) -> None:
        result = self._run([str(path), "--version"])
        if result.stderr or result.stdout != b"uv 0.11.26 (aarch64-unknown-linux-gnu)\n":
            _fail("controller installer uv readback is invalid")

    def validate_acceptance(self, path: Path) -> None:
        result = self._run(["/usr/bin/python3", str(path), "--help"])
        if result.stderr or not result.stdout:
            _fail("controller installer acceptance readback is invalid")

    def validate_sudoers(self, path: Path) -> None:
        self._run([str(self._visudo_path), "-cf", str(path)])

    def publish_authority(
        self,
        broker_path: Path,
        controller_public_key: Path,
        legacy_public_key: Path,
    ) -> None:
        arguments = [
            str(broker_path),
            "--install-authority",
            str(controller_public_key),
            str(legacy_public_key),
        ]
        if not Path(arguments[0]).is_absolute() or any(
            not argument or "\x00" in argument for argument in arguments
        ):
            _fail("controller installer authority publication request is invalid")
        try:
            completed = subprocess.run(
                arguments,
                check=False,
                env=_ROOT_ENVIRONMENT,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ControllerInstallError(
                "controller installer authority publication is unavailable"
            ) from exc
        if completed.returncode != 0:
            _fail("controller installer authority publication failed")


class ControllerInstaller:
    """Validate all inputs, publish transactionally, then enable SSH authority last."""

    def __init__(self, *, context: InstallContext, backend: InstallBackend) -> None:
        self.context = context
        self.backend = backend

    def install(self) -> None:
        context = self.context
        _validate_context(context)
        _validate_unmanaged_ancestors(context)
        verify_source(
            trusted_root=context.trusted_source_root,
            source_root=context.source_root,
            source_sha=context.source_sha,
            authority_uid=context.authority_uid,
            authority_gid=context.authority_gid,
        )
        source_payloads = {
            relative: _read_artifact(
                context.source_root,
                relative,
                uid=context.authority_uid,
                gid=context.authority_gid,
            )
            for relative in _SOURCE_ARTIFACTS
        }
        acceptance_payload = source_payloads[_ACCEPTANCE_AUTHORITY_RELATIVE]
        tmpfiles_payload = source_payloads[_ACCEPTANCE_TMPFILES_RELATIVE]
        broker_payload = source_payloads[_BROKER_RELATIVE]
        if tmpfiles_payload != _TMPFILES_PAYLOAD:
            _fail("controller installer tmpfiles policy is invalid")
        try:
            compile(acceptance_payload, str(_ACCEPTANCE_AUTHORITY_RELATIVE), "exec")
            compile(broker_payload, str(_BROKER_RELATIVE), "exec")
        except (SyntaxError, ValueError, TypeError) as exc:
            raise ControllerInstallError("controller installer Python asset is invalid") from exc

        self.backend.validate_host()

        kubectl_payload = _read_external_input(
            context.kubectl_source,
            uid=context.authority_uid,
            gid=context.authority_gid,
            maximum_bytes=256 * 1024 * 1024,
            label="controller installer kubectl input",
        )
        uv_payload = _read_external_input(
            context.uv_source,
            uid=context.authority_uid,
            gid=context.authority_gid,
            maximum_bytes=256 * 1024 * 1024,
            label="controller installer uv input",
        )
        controller_key = _read_external_input(
            context.controller_public_key,
            uid=context.authority_uid,
            gid=context.authority_gid,
            maximum_bytes=16 * 1024,
            label="controller installer controller public key",
        )
        legacy_key = _read_external_input(
            context.legacy_public_key,
            uid=context.authority_uid,
            gid=context.authority_gid,
            maximum_bytes=16 * 1024,
            label="controller installer legacy public key",
        )
        if _public_key_identity(controller_key) == _public_key_identity(legacy_key):
            _fail("controller installer public keys are not distinct")
        _validate_optional_kubeconfig(context)

        self.backend.validate_kubectl(context.kubectl_source)
        self.backend.validate_uv(context.uv_source)
        self.backend.validate_acceptance(context.source_root / _ACCEPTANCE_AUTHORITY_RELATIVE)
        with _staged_payload(context.kubectl_source.parent, _SUDOERS_PAYLOAD) as sudoers_stage:
            self.backend.validate_sudoers(sudoers_stage)

        directories = (
            ("/usr/local/libexec", 0o755, context.authority_uid, context.authority_gid),
            ("/etc/sudoers.d", 0o755, context.authority_uid, context.authority_gid),
            ("/etc/tmpfiles.d", 0o755, context.authority_uid, context.authority_gid),
            ("/opt/loom-staging-runner", 0o755, context.authority_uid, context.authority_gid),
            (
                "/opt/loom-staging-runner/candidates",
                0o755,
                context.authority_uid,
                context.authority_gid,
            ),
            (
                "/var/lib/loom-staging-rollout",
                0o750,
                context.service_uid,
                context.service_gid,
            ),
            ("/var/lib/loom-rollout", 0o750, context.service_uid, context.service_gid),
            ("/var/lib/loom-rollout/.config", 0o750, context.service_uid, context.service_gid),
            (
                "/var/lib/loom-rollout/.config/systemd",
                0o750,
                context.service_uid,
                context.service_gid,
            ),
            (
                "/var/lib/loom-rollout/.config/systemd/user",
                0o750,
                context.service_uid,
                context.service_gid,
            ),
            (
                "/run/loom-gb10-slurm-authority",
                0o700,
                context.authority_uid,
                context.authority_gid,
            ),
            (
                "/run/loom-gb10-slurm-authority/jobs",
                0o700,
                context.authority_uid,
                context.authority_gid,
            ),
        )
        files = (
            ("/usr/local/bin/kubectl", kubectl_payload, 0o755),
            ("/usr/local/bin/uv", uv_payload, 0o755),
            ("/etc/tmpfiles.d/loom-gb10-slurm-authority.conf", tmpfiles_payload, 0o644),
            (
                "/usr/local/libexec/loom-gb10-slurm-acceptance-authority",
                acceptance_payload,
                0o755,
            ),
            ("/usr/local/libexec/loom-gb10-external-supervisor-broker", broker_payload, 0o755),
            ("/etc/sudoers.d/loom-gb10-external-supervisor", _SUDOERS_PAYLOAD, 0o440),
        )
        transaction = AtomicFileTransaction(
            authority_uid=context.authority_uid,
            authority_gid=context.authority_gid,
        )
        with transaction:
            for absolute, mode, uid, gid in directories:
                transaction.ensure_directory(
                    _system_path(context, absolute),
                    mode=mode,
                    uid=uid,
                    gid=gid,
                )
            for absolute, payload, mode in files:
                transaction.publish(
                    _system_path(context, absolute),
                    payload,
                    mode=mode,
                    uid=context.authority_uid,
                    gid=context.authority_gid,
                )
            lock_path = _system_path(
                context,
                "/run/loom-gb10-slurm-authority/acceptance.lock",
            )
            transaction.ensure_file(
                lock_path,
                b"",
                mode=0o600,
                uid=context.authority_uid,
                gid=context.authority_gid,
            )

            kubectl_path = _system_path(context, "/usr/local/bin/kubectl")
            uv_path = _system_path(context, "/usr/local/bin/uv")
            acceptance_path = _system_path(
                context,
                "/usr/local/libexec/loom-gb10-slurm-acceptance-authority",
            )
            broker_path = _system_path(
                context,
                "/usr/local/libexec/loom-gb10-external-supervisor-broker",
            )
            sudoers_path = _system_path(
                context,
                "/etc/sudoers.d/loom-gb10-external-supervisor",
            )
            self.backend.validate_kubectl(kubectl_path)
            self.backend.validate_uv(uv_path)
            self.backend.validate_acceptance(acceptance_path)
            self.backend.validate_sudoers(sudoers_path)
            for absolute, payload, mode in files:
                _assert_file_state(
                    _system_path(context, absolute),
                    payload,
                    mode=mode,
                    uid=context.authority_uid,
                    gid=context.authority_gid,
                )
            _assert_file_state(
                lock_path,
                b"",
                mode=0o600,
                uid=context.authority_uid,
                gid=context.authority_gid,
            )

        try:
            # This is deliberately the final external operation. The file transaction
            # remains compensatable and termination signals remain blocked until the
            # broker has either published authority exactly or restored it.
            self.backend.publish_authority(
                broker_path,
                context.controller_public_key,
                context.legacy_public_key,
            )
        except BaseException:
            transaction.rollback()
            raise
        transaction.commit()


def verify_source(
    *,
    trusted_root: Path,
    source_root: Path,
    source_sha: str,
    authority_uid: int | None = None,
    authority_gid: int | None = None,
) -> dict[str, object]:
    uid = os.geteuid() if authority_uid is None else authority_uid
    gid = os.getegid() if authority_gid is None else authority_gid
    _validate_source_directories(
        trusted_root,
        source_root,
        source_sha,
        uid=uid,
        gid=gid,
    )
    _directory_metadata(
        source_root / ".git",
        uid=uid,
        gid=gid,
        label="controller installer Git metadata",
    )
    _validate_git_source(source_root, source_sha)
    artifacts: dict[str, str] = {}
    for relative in _SOURCE_ARTIFACTS:
        _git(source_root, "ls-files", "--error-unmatch", str(relative))
        payload = _read_artifact(source_root, relative, uid=uid, gid=gid)
        expected_blob = _git(source_root, "rev-parse", "--verify", f"HEAD:{relative}")
        actual_blob = hashlib.sha1(
            b"blob " + str(len(payload)).encode("ascii") + b"\0" + payload,
            usedforsecurity=False,
        ).hexdigest()
        if _SHA_RE.fullmatch(expected_blob) is None or actual_blob != expected_blob:
            _fail("controller installer artifact does not match exact commit")
        artifacts[str(relative)] = hashlib.sha256(payload).hexdigest()
    return {"artifacts": artifacts, "source_sha": source_sha}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    verify = subparsers.add_parser("verify-source", allow_abbrev=False)
    verify.add_argument("--trusted-root", type=Path, required=True)
    verify.add_argument("--source-root", type=Path, required=True)
    verify.add_argument("--source-sha", required=True)
    subparsers.add_parser("verify-host", allow_abbrev=False)
    install = subparsers.add_parser("install", allow_abbrev=False)
    install.add_argument("--trusted-root", type=Path, required=True)
    install.add_argument("--source-root", type=Path, required=True)
    install.add_argument("--source-sha", required=True)
    install.add_argument("--kubectl-source", type=Path, required=True)
    install.add_argument("--uv-source", type=Path, required=True)
    install.add_argument("--controller-public-key", type=Path, required=True)
    install.add_argument("--legacy-public-key", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        arguments = _parser().parse_args(argv)
        if arguments.operation == "verify-source":
            evidence = verify_source(
                trusted_root=arguments.trusted_root,
                source_root=arguments.source_root,
                source_sha=arguments.source_sha,
            )
        elif arguments.operation == "verify-host":
            SubprocessBackend().validate_host()
            evidence = {"status": "host-verified"}
        elif arguments.operation == "install":
            if os.geteuid() != 0 or os.getegid() != 0:
                _fail("controller installation requires root")
            context = InstallContext(
                trusted_source_root=arguments.trusted_root,
                source_root=arguments.source_root,
                source_sha=arguments.source_sha,
                kubectl_source=arguments.kubectl_source,
                uv_source=arguments.uv_source,
                controller_public_key=arguments.controller_public_key,
                legacy_public_key=arguments.legacy_public_key,
            )
            ControllerInstaller(context=context, backend=SubprocessBackend()).install()
            return 0
        else:  # pragma: no cover - argparse restricts the operation
            _fail("controller installer operation is invalid")
    except ControllerInstallError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
