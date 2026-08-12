#!/usr/bin/env python3
"""Root forced-command broker for the GB10 external autoscaler supervisor.

The broker accepts one canonical JSON envelope on stdin.  It verifies or
publishes the exact root-owned candidate, then drops to the fixed Slurm service
identity and execs that candidate's typed helper.  It has no command/path
arguments and exposes no arbitrary remote-command surface.
"""

from __future__ import annotations

import base64
import binascii
import fcntl
import json
import os
import pwd
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

CONTROLLER = "gx10-01c7"
CLUSTER = "trt-gb10"
SERVICE_USER = "loom-rollout"
SERVICE_UID = 995
SERVICE_GID = 2007
SERVICE_HOME = Path("/var/lib/loom-rollout")
CANDIDATES_ROOT = Path("/opt/loom-staging-runner/candidates")
REMOTE_URL = "https://github.com/qianyi-sun/loom.git"
SYSTEM_PYTHON = Path("/usr/bin/python3")
UV_BINARY = Path("/usr/local/bin/uv")
SCONTROL = Path("/usr/bin/scontrol")
INSTALLED_BROKER = Path("/usr/local/libexec/loom-gb10-external-supervisor-broker")
REMOTE_SSH_USER = "qianyi"
REMOTE_SSH_HOME = Path("/home/qianyi")
_AUTHORIZED_KEY_MARKER = "loom-gb10-external-supervisor"
_LOCK_NAME = ".loom-gb10-external-supervisor-broker.lock"
_HELPER_MODULE = "loom_cli.rollout.operator.protected_gb10_external_supervisor_transport"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_REQUEST_BYTES = 4 * 1024 * 1024
_MAX_COMMAND_OUTPUT = 1024 * 1024
_MAX_TREE_ENTRIES = 300_000


class BrokerError(RuntimeError):
    """Secret-free fixed broker failure."""


@dataclass(frozen=True, slots=True)
class HelperExecSpec:
    cwd: Path
    argv: tuple[str, ...]
    environment: dict[str, str]


def _public_key_identity(payload: bytes) -> tuple[str, str]:
    if not payload or len(payload) > 16 * 1024:
        raise BrokerError("GB10 external supervisor public key is invalid")
    try:
        text = payload.decode("ascii").strip()
    except UnicodeError as exc:
        raise BrokerError("GB10 external supervisor public key is invalid") from exc
    fields = text.split()
    if len(fields) not in {2, 3} or fields[0] != "ssh-ed25519":
        raise BrokerError("GB10 external supervisor public key is invalid")
    try:
        blob = base64.b64decode(fields[1], validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BrokerError("GB10 external supervisor public key is invalid") from exc
    algorithm = b"ssh-ed25519"
    prefix = len(algorithm).to_bytes(4, "big") + algorithm
    offset = len(prefix)
    if (
        not blob.startswith(prefix)
        or len(blob) < offset + 4
        or int.from_bytes(blob[offset : offset + 4], "big") != 32
        or len(blob) != offset + 36
        or base64.b64encode(blob).decode("ascii") != fields[1]
    ):
        raise BrokerError("GB10 external supervisor public key is invalid")
    return fields[0], fields[1]


def render_authorized_keys(existing: bytes, public_key: bytes) -> bytes:
    """Add exactly one forced key while preserving unrelated file bytes."""

    if len(existing) > 4 * 1024 * 1024 or b"\x00" in existing:
        raise BrokerError("GB10 external supervisor authorized keys are invalid")
    try:
        text = existing.decode("utf-8")
    except UnicodeError as exc:
        raise BrokerError("GB10 external supervisor authorized keys are invalid") from exc
    algorithm, encoded = _public_key_identity(public_key)
    expected = (
        f'restrict,command="/usr/bin/sudo -n -- {INSTALLED_BROKER}" '
        f"{algorithm} {encoded} {_AUTHORIZED_KEY_MARKER}"
    )
    lines = text.splitlines()
    marked = [line for line in lines if line.rstrip().endswith(f" {_AUTHORIZED_KEY_MARKER}")]
    if len(marked) > 1 or (marked and marked[0].strip() != expected):
        raise BrokerError("GB10 external supervisor forced key marker is ambiguous")
    matching = [line for line in lines if encoded in line]
    if marked:
        if matching != marked:
            raise BrokerError("GB10 external supervisor key authority is duplicated")
        return existing
    if matching:
        raise BrokerError("GB10 external supervisor key is already present without force")
    prefix = existing if not existing or existing.endswith(b"\n") else existing + b"\n"
    return prefix + expected.encode("ascii") + b"\n"


def install_forced_key(public_key_path: Path) -> None:
    if not public_key_path.is_absolute() or ".." in public_key_path.parts:
        raise BrokerError("GB10 external supervisor public key path is invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(public_key_path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 1 <= metadata.st_size <= 16 * 1024:
            raise BrokerError("GB10 external supervisor public key metadata is unsafe")
        public_key = os.read(descriptor, 16 * 1024 + 1)
    finally:
        os.close(descriptor)
    _public_key_identity(public_key)
    account = pwd.getpwnam(REMOTE_SSH_USER)
    if account.pw_dir != str(REMOTE_SSH_HOME):
        raise BrokerError("GB10 external supervisor SSH account drifted")
    ssh_dir = REMOTE_SSH_HOME / ".ssh"
    if os.path.lexists(ssh_dir):
        metadata = os.lstat(ssh_dir)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != account.pw_uid
            or metadata.st_gid != account.pw_gid
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise BrokerError("GB10 external supervisor SSH directory is unsafe")
    else:
        os.mkdir(ssh_dir, mode=0o700)
        os.chown(ssh_dir, account.pw_uid, account.pw_gid)
    authorized_keys = ssh_dir / "authorized_keys"
    existing = b""
    if os.path.lexists(authorized_keys):
        descriptor = os.open(authorized_keys, flags)
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != account.pw_uid
                or metadata.st_gid != account.pw_gid
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_size > 4 * 1024 * 1024
            ):
                raise BrokerError("GB10 external supervisor authorized keys are unsafe")
            existing = os.read(descriptor, 4 * 1024 * 1024 + 1)
        finally:
            os.close(descriptor)
    rendered = render_authorized_keys(existing, public_key)
    if rendered == existing:
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=".authorized_keys.", dir=ssh_dir)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, account.pw_uid, account.pw_gid)
        offset = 0
        while offset < len(rendered):
            offset += os.write(descriptor, rendered[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, authorized_keys)
        directory = os.open(ssh_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if os.path.lexists(temporary):
            os.unlink(temporary)


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n"
    )


def parse_request_identity(payload: bytes) -> tuple[str, str]:
    """Validate the outer typed envelope and return its candidate identity."""

    if not payload or len(payload) > _MAX_REQUEST_BYTES or not payload.endswith(b"\n"):
        raise BrokerError("GB10 external supervisor request bytes are invalid")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise BrokerError("GB10 external supervisor request has duplicate fields")
            result[key] = value
        return result

    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise BrokerError("GB10 external supervisor request is invalid") from exc
    if not isinstance(value, dict) or _canonical_json(value) != payload:
        raise BrokerError("GB10 external supervisor request is not canonical")
    operation = value.get("operation")
    common = {"candidate_sha", "candidate_tree", "operation", "schema_version"}
    expected = {
        "observe": common | {"artifact", "predecessor_authority"},
        "apply": common
        | {
            "artifact",
            "attestation_digest",
            "expected",
            "plan_digest",
            "transition_digest",
        },
        "reconcile_compensations": common,
    }
    if (
        value.get("schema_version") != 1
        or type(operation) is not str
        or operation not in expected
        or set(value) != expected[operation]
    ):
        raise BrokerError("GB10 external supervisor request fields are invalid")
    candidate_sha = value.get("candidate_sha")
    candidate_tree = value.get("candidate_tree")
    if (
        type(candidate_sha) is not str
        or _SHA_RE.fullmatch(candidate_sha) is None
        or type(candidate_tree) is not str
        or _SHA_RE.fullmatch(candidate_tree) is None
    ):
        raise BrokerError("GB10 external supervisor candidate identity is invalid")
    return candidate_sha, candidate_tree


def _safe_directory(path: Path, *, owner_uid: int, owner_gid: int, label: str) -> None:
    metadata = os.lstat(path)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or metadata.st_gid != owner_gid
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise BrokerError(f"{label} metadata is unsafe")


def _safe_executable(path: Path, *, owner_uid: int, owner_gid: int, label: str) -> None:
    resolved = path.resolve(strict=True)
    metadata = os.stat(resolved)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or metadata.st_gid != owner_gid
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or not os.access(path, os.X_OK)
    ):
        raise BrokerError(f"{label} is unsafe")


def _safe_tree(
    root: Path,
    *,
    owner_uid: int,
    owner_gid: int,
    system_python: Path,
) -> None:
    _safe_directory(root, owner_uid=owner_uid, owner_gid=owner_gid, label="candidate runtime")
    root_resolved = root.resolve(strict=True)
    entries = 0
    for directory, names, files in os.walk(root, topdown=True, followlinks=False):
        for name in [*names, *files]:
            entries += 1
            if entries > _MAX_TREE_ENTRIES:
                raise BrokerError("candidate runtime inventory is too large")
            path = Path(directory) / name
            metadata = os.lstat(path)
            if metadata.st_uid != owner_uid or metadata.st_gid != owner_gid:
                raise BrokerError("candidate runtime ownership drifted")
            if stat.S_ISLNK(metadata.st_mode):
                target_text = os.readlink(path)
                target = Path(target_text)
                if target.is_absolute():
                    try:
                        resolved = target.resolve(strict=True)
                    except OSError as exc:
                        raise BrokerError("candidate runtime symlink is unsafe") from exc
                    if resolved != system_python.resolve(strict=True):
                        raise BrokerError("candidate runtime symlink escapes authority")
                else:
                    resolved = (path.parent / target).resolve(strict=False)
                    if not resolved.is_relative_to(root_resolved):
                        raise BrokerError("candidate runtime symlink escapes authority")
                continue
            if not (stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)):
                raise BrokerError("candidate runtime contains a special file")
            if stat.S_ISREG(metadata.st_mode) and metadata.st_nlink != 1:
                raise BrokerError("candidate runtime contains an external hardlink")
            if stat.S_IMODE(metadata.st_mode) & 0o022:
                raise BrokerError("candidate runtime is writable outside root")


def _run(
    argv: list[str],
    *,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
    timeout: int = 900,
    check: bool = True,
    run_as: tuple[int, int] | None = None,
) -> subprocess.CompletedProcess[str]:
    if not argv or any(not item or "\x00" in item for item in argv) or not 1 <= timeout <= 1800:
        raise BrokerError("GB10 external supervisor command is invalid")
    env = (
        {
            "HOME": "/root",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
        }
        if environment is None
        else dict(environment)
    )
    privilege_identity: tuple[int, int] | None = None
    if run_as is not None:
        run_uid, run_gid = run_as
        if run_uid < 0 or run_gid < 0:
            raise BrokerError("GB10 external supervisor command identity is invalid")
        if (run_uid, run_gid) != (os.geteuid(), os.getegid()):
            privilege_identity = (run_uid, run_gid)
    if privilege_identity is None:
        result = subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    else:
        result = subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            user=privilege_identity[0],
            group=privilege_identity[1],
            extra_groups=(),
        )
    if (
        len(result.stdout.encode()) > _MAX_COMMAND_OUTPUT
        or len(result.stderr.encode()) > _MAX_COMMAND_OUTPUT
        or (check and result.returncode != 0)
    ):
        raise BrokerError("GB10 external supervisor command failed safely")
    return result


def _git(
    repo: Path,
    *arguments: str,
    check: bool = True,
    run_as: tuple[int, int],
) -> subprocess.CompletedProcess[str]:
    return _run(
        [
            "/usr/bin/git",
            "-c",
            f"safe.directory={repo}",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-C",
            str(repo),
            *arguments,
        ],
        check=check,
        run_as=run_as,
    )


def _validate_candidate_path(
    candidate: Path,
    candidate_sha: str,
    candidate_tree: str,
    *,
    remote_url: str,
    owner_uid: int,
    owner_gid: int,
    inspection_uid: int,
    inspection_gid: int,
    system_python: Path,
) -> Path:
    if _SHA_RE.fullmatch(candidate_sha) is None or _SHA_RE.fullmatch(candidate_tree) is None:
        raise BrokerError("candidate authority is invalid")
    repo = candidate / "repo"
    venv = candidate / "venv"
    _safe_tree(
        candidate,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        system_python=system_python,
    )
    _safe_directory(repo, owner_uid=owner_uid, owner_gid=owner_gid, label="candidate repo")
    _safe_directory(venv, owner_uid=owner_uid, owner_gid=owner_gid, label="candidate venv")
    inspection_identity = (inspection_uid, inspection_gid)
    if _git(repo, "remote", run_as=inspection_identity).stdout.splitlines() != ["origin"]:
        raise BrokerError("candidate origin authority drifted")
    if _git(
        repo,
        "config",
        "--get-all",
        "remote.origin.url",
        run_as=inspection_identity,
    ).stdout.splitlines() != [remote_url]:
        raise BrokerError("candidate origin URL drifted")
    if (
        _git(
            repo,
            "config",
            "--get-all",
            "remote.origin.pushurl",
            check=False,
            run_as=inspection_identity,
        ).returncode
        == 0
    ):
        raise BrokerError("candidate push authority appeared")
    if _git(repo, "rev-parse", "HEAD", run_as=inspection_identity).stdout.strip() != candidate_sha:
        raise BrokerError("candidate commit identity drifted")
    if (
        _git(repo, "rev-parse", "HEAD^{tree}", run_as=inspection_identity).stdout.strip()
        != candidate_tree
    ):
        raise BrokerError("candidate tree identity drifted")
    if _git(
        repo,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        run_as=inspection_identity,
    ).stdout:
        raise BrokerError("candidate checkout is dirty")
    python = venv / "bin/python"
    if python.resolve(strict=True) != system_python.resolve(strict=True):
        raise BrokerError("candidate Python authority drifted")
    _safe_executable(
        python,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        label="candidate Python",
    )
    return candidate


def _validate_candidate(
    candidates_root: Path,
    candidate_sha: str,
    candidate_tree: str,
    *,
    remote_url: str,
    owner_uid: int,
    owner_gid: int,
    inspection_uid: int,
    inspection_gid: int,
    system_python: Path,
) -> Path:
    if not candidates_root.is_absolute() or ".." in candidates_root.parts:
        raise BrokerError("candidate authority is invalid")
    _safe_directory(
        candidates_root,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        label="candidate authority root",
    )
    return _validate_candidate_path(
        candidates_root / candidate_sha,
        candidate_sha,
        candidate_tree,
        remote_url=remote_url,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        inspection_uid=inspection_uid,
        inspection_gid=inspection_gid,
        system_python=system_python,
    )


def candidate_ready(
    candidates_root: Path,
    candidate_sha: str,
    candidate_tree: str,
    *,
    remote_url: str = REMOTE_URL,
    owner_uid: int = 0,
    owner_gid: int = 0,
    inspection_uid: int = SERVICE_UID,
    inspection_gid: int = SERVICE_GID,
    system_python: Path = SYSTEM_PYTHON,
) -> bool:
    try:
        _validate_candidate(
            candidates_root,
            candidate_sha,
            candidate_tree,
            remote_url=remote_url,
            owner_uid=owner_uid,
            owner_gid=owner_gid,
            inspection_uid=inspection_uid,
            inspection_gid=inspection_gid,
            system_python=system_python,
        )
    except (BrokerError, OSError, subprocess.SubprocessError):
        return False
    return True


def _set_fd_owner(descriptor: int, *, owner_uid: int, owner_gid: int) -> None:
    metadata = os.fstat(descriptor)
    if metadata.st_uid != owner_uid or metadata.st_gid != owner_gid:
        os.fchown(descriptor, owner_uid, owner_gid)


def _source_metadata_changed(
    before: os.stat_result,
    after: os.stat_result,
) -> bool:
    return any(
        getattr(before, field) != getattr(after, field)
        for field in (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_nlink",
            "st_uid",
            "st_gid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
    )


def _copy_hardened_directory(
    source: int,
    destination: int,
    *,
    source_uid: int,
    source_gid: int,
    owner_uid: int,
    owner_gid: int,
    entries: list[int],
) -> None:
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    symlink_flags = (
        getattr(os, "O_PATH", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    if not getattr(os, "O_PATH", 0):
        raise BrokerError("candidate runtime symlink inspection is unsupported")
    for name in sorted(os.listdir(source)):
        if not name or name in {".", ".."} or "/" in name or "\x00" in name:
            raise BrokerError("candidate runtime entry name is unsafe")
        entries[0] += 1
        if entries[0] > _MAX_TREE_ENTRIES:
            raise BrokerError("candidate runtime inventory is too large")
        metadata = os.stat(name, dir_fd=source, follow_symlinks=False)
        if metadata.st_uid != source_uid or metadata.st_gid != source_gid:
            raise BrokerError("candidate build ownership drifted")
        if stat.S_ISDIR(metadata.st_mode):
            source_child = os.open(name, directory_flags, dir_fd=source)
            try:
                opened = os.fstat(source_child)
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or opened.st_uid != source_uid
                    or opened.st_gid != source_gid
                    or _source_metadata_changed(metadata, opened)
                ):
                    raise BrokerError("candidate build directory changed during publication")
                os.mkdir(name, mode=0o700, dir_fd=destination)
                destination_child = os.open(name, directory_flags, dir_fd=destination)
                try:
                    _set_fd_owner(
                        destination_child,
                        owner_uid=owner_uid,
                        owner_gid=owner_gid,
                    )
                    _copy_hardened_directory(
                        source_child,
                        destination_child,
                        source_uid=source_uid,
                        source_gid=source_gid,
                        owner_uid=owner_uid,
                        owner_gid=owner_gid,
                        entries=entries,
                    )
                    os.fchmod(destination_child, 0o555)
                    os.fsync(destination_child)
                    if _source_metadata_changed(opened, os.fstat(source_child)):
                        raise BrokerError("candidate build directory changed during publication")
                finally:
                    os.close(destination_child)
            finally:
                os.close(source_child)
            continue
        if stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1:
                raise BrokerError("candidate runtime contains an external hardlink")
            source_file = os.open(name, file_flags, dir_fd=source)
            try:
                opened = os.fstat(source_file)
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or opened.st_uid != source_uid
                    or opened.st_gid != source_gid
                    or opened.st_nlink != 1
                    or _source_metadata_changed(metadata, opened)
                ):
                    raise BrokerError("candidate build file changed during publication")
                destination_file = os.open(
                    name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=destination,
                )
                try:
                    while True:
                        chunk = os.read(source_file, 1024 * 1024)
                        if not chunk:
                            break
                        offset = 0
                        while offset < len(chunk):
                            written = os.write(destination_file, chunk[offset:])
                            if written <= 0:
                                raise BrokerError("candidate runtime copy failed")
                            offset += written
                    if _source_metadata_changed(opened, os.fstat(source_file)):
                        raise BrokerError("candidate build file changed during publication")
                    _set_fd_owner(
                        destination_file,
                        owner_uid=owner_uid,
                        owner_gid=owner_gid,
                    )
                    os.fchmod(
                        destination_file,
                        0o555 if stat.S_IMODE(opened.st_mode) & 0o111 else 0o444,
                    )
                    os.fsync(destination_file)
                finally:
                    os.close(destination_file)
            finally:
                os.close(source_file)
            continue
        if stat.S_ISLNK(metadata.st_mode):
            if metadata.st_nlink != 1:
                raise BrokerError("candidate runtime contains an external hardlink")
            source_link = os.open(name, symlink_flags, dir_fd=source)
            try:
                opened = os.fstat(source_link)
                if (
                    not stat.S_ISLNK(opened.st_mode)
                    or opened.st_uid != source_uid
                    or opened.st_gid != source_gid
                    or opened.st_nlink != 1
                    or _source_metadata_changed(metadata, opened)
                ):
                    raise BrokerError("candidate build symlink changed during publication")
                target = os.readlink("", dir_fd=source_link)
                if _source_metadata_changed(opened, os.fstat(source_link)):
                    raise BrokerError("candidate build symlink changed during publication")
                os.symlink(target, name, dir_fd=destination)
                os.chown(
                    name,
                    owner_uid,
                    owner_gid,
                    dir_fd=destination,
                    follow_symlinks=False,
                )
            finally:
                os.close(source_link)
            continue
        raise BrokerError("candidate runtime contains a special file")


def _copy_hardened_tree(
    source: Path,
    destination: Path,
    *,
    source_uid: int,
    source_gid: int,
    owner_uid: int,
    owner_gid: int,
) -> None:
    """Copy untrusted build output into a new root-controlled immutable tree."""

    if (
        not source.is_absolute()
        or not destination.is_absolute()
        or ".." in source.parts
        or ".." in destination.parts
    ):
        raise BrokerError("candidate runtime copy authority is invalid")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    source_descriptor = os.open(source, flags)
    destination_descriptor = os.open(destination, flags)
    try:
        source_metadata = os.fstat(source_descriptor)
        destination_metadata = os.fstat(destination_descriptor)
        if (
            source_metadata.st_uid != source_uid
            or source_metadata.st_gid != source_gid
            or not stat.S_ISDIR(source_metadata.st_mode)
            or not stat.S_ISDIR(destination_metadata.st_mode)
            or os.listdir(destination_descriptor)
        ):
            raise BrokerError("candidate runtime copy roots are unsafe")
        _set_fd_owner(
            destination_descriptor,
            owner_uid=owner_uid,
            owner_gid=owner_gid,
        )
        _copy_hardened_directory(
            source_descriptor,
            destination_descriptor,
            source_uid=source_uid,
            source_gid=source_gid,
            owner_uid=owner_uid,
            owner_gid=owner_gid,
            entries=[0],
        )
        if _source_metadata_changed(source_metadata, os.fstat(source_descriptor)):
            raise BrokerError("candidate build root changed during publication")
        os.fchmod(destination_descriptor, 0o555)
        os.fsync(destination_descriptor)
    finally:
        os.close(destination_descriptor)
        os.close(source_descriptor)


def _make_tree_removable(root: Path) -> None:
    for directory, names, _files in os.walk(root, topdown=False, followlinks=False):
        for name in names:
            path = Path(directory) / name
            if not stat.S_ISLNK(os.lstat(path).st_mode):
                os.chmod(path, 0o700, follow_symlinks=False)
        os.chmod(directory, 0o700)
    os.chmod(root, 0o700)


def _publish_candidate(
    candidates_root: Path,
    candidate_sha: str,
    candidate_tree: str,
    *,
    remote_url: str,
    owner_uid: int,
    owner_gid: int,
    build_uid: int,
    build_gid: int,
    system_python: Path,
    uv_binary: Path,
) -> Path:
    build_temporary = Path(tempfile.mkdtemp(prefix=f".{candidate_sha}.build.", dir=candidates_root))
    sealed_temporary: Path | None = None
    try:
        os.chown(build_temporary, build_uid, build_gid)
        os.chmod(build_temporary, 0o700)
        build_identity = (build_uid, build_gid)
        repo = build_temporary / "repo"
        _run(["/usr/bin/git", "init", "-q", str(repo)], run_as=build_identity)
        _git(repo, "remote", "add", "origin", remote_url, run_as=build_identity)
        _git(
            repo,
            "fetch",
            "--quiet",
            "--no-tags",
            "--no-recurse-submodules",
            "--filter=blob:none",
            "origin",
            candidate_sha,
            run_as=build_identity,
        )
        _git(
            repo,
            "checkout",
            "--quiet",
            "--detach",
            candidate_sha,
            run_as=build_identity,
        )
        environment = {
            "HOME": str(SERVICE_HOME),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "UV_LINK_MODE": "copy",
            "UV_PROJECT_ENVIRONMENT": str(build_temporary / "venv"),
        }
        _run(
            [
                str(uv_binary),
                "sync",
                "--frozen",
                "--project",
                str(repo),
                "--no-editable",
                "--extra",
                "cluster",
                "--extra",
                "rollout",
                "--reinstall-package",
                "loom",
                "--python",
                str(system_python),
            ],
            environment=environment,
            timeout=1800,
            run_as=build_identity,
        )
        sealed_temporary = Path(
            tempfile.mkdtemp(prefix=f".{candidate_sha}.candidate.", dir=candidates_root)
        )
        _copy_hardened_tree(
            build_temporary,
            sealed_temporary,
            source_uid=build_uid,
            source_gid=build_gid,
            owner_uid=owner_uid,
            owner_gid=owner_gid,
        )
        _validate_candidate_path(
            sealed_temporary,
            candidate_sha,
            candidate_tree,
            remote_url=remote_url,
            owner_uid=owner_uid,
            owner_gid=owner_gid,
            inspection_uid=build_uid,
            inspection_gid=build_gid,
            system_python=system_python,
        )
        if not shutil.rmtree.avoids_symlink_attacks:
            raise BrokerError("candidate build cleanup is unsafe")
        shutil.rmtree(build_temporary)
        final = candidates_root / candidate_sha
        if os.path.lexists(final):
            raise BrokerError("candidate path appeared during publication")
        os.rename(sealed_temporary, final)
        sealed_temporary = None
        directory = os.open(candidates_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return final
    finally:
        if os.path.lexists(build_temporary):
            shutil.rmtree(build_temporary)
        if sealed_temporary is not None and os.path.lexists(sealed_temporary):
            _make_tree_removable(sealed_temporary)
            shutil.rmtree(sealed_temporary)


def ensure_candidate(
    candidates_root: Path,
    candidate_sha: str,
    candidate_tree: str,
    *,
    remote_url: str = REMOTE_URL,
    owner_uid: int = 0,
    owner_gid: int = 0,
    build_uid: int = SERVICE_UID,
    build_gid: int = SERVICE_GID,
    system_python: Path = SYSTEM_PYTHON,
    uv_binary: Path = UV_BINARY,
) -> Path:
    if (
        _SHA_RE.fullmatch(candidate_sha) is None
        or _SHA_RE.fullmatch(candidate_tree) is None
        or not candidates_root.is_absolute()
        or ".." in candidates_root.parts
    ):
        raise BrokerError("candidate publication authority is invalid")
    _safe_directory(
        candidates_root,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        label="candidate authority root",
    )
    lock_path = candidates_root / _LOCK_NAME
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        if candidate_ready(
            candidates_root,
            candidate_sha,
            candidate_tree,
            remote_url=remote_url,
            owner_uid=owner_uid,
            owner_gid=owner_gid,
            inspection_uid=build_uid,
            inspection_gid=build_gid,
            system_python=system_python,
        ):
            return candidates_root / candidate_sha
        if os.path.lexists(candidates_root / candidate_sha):
            raise BrokerError("existing candidate runtime is unsafe")
        return _publish_candidate(
            candidates_root,
            candidate_sha,
            candidate_tree,
            remote_url=remote_url,
            owner_uid=owner_uid,
            owner_gid=owner_gid,
            build_uid=build_uid,
            build_gid=build_gid,
            system_python=system_python,
            uv_binary=uv_binary,
        )
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def helper_exec_spec(
    candidate: Path,
    *,
    service_uid: int,
    service_gid: int,
) -> HelperExecSpec:
    if (
        service_uid != SERVICE_UID
        or service_gid != SERVICE_GID
        or not candidate.is_absolute()
        or ".." in candidate.parts
        or _SHA_RE.fullmatch(candidate.name) is None
    ):
        raise BrokerError("helper execution authority is invalid")
    python = candidate / "venv/bin/python"
    return HelperExecSpec(
        cwd=candidate / "repo",
        argv=(str(python), "-I", "-B", "-m", _HELPER_MODULE),
        environment={
            "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{service_uid}/bus",
            "HOME": str(SERVICE_HOME),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "LOGNAME": SERVICE_USER,
            "PATH": f"{candidate / 'venv/bin'}:/usr/local/bin:/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
            "USER": SERVICE_USER,
            "XDG_CONFIG_HOME": str(SERVICE_HOME / ".config"),
            "XDG_RUNTIME_DIR": f"/run/user/{service_uid}",
        },
    )


def _require_host_authority() -> None:
    if socket.gethostname().split(".", 1)[0] != CONTROLLER:
        raise BrokerError("GB10 controller hostname is invalid")
    account = pwd.getpwnam(SERVICE_USER)
    if (
        account.pw_uid != SERVICE_UID
        or account.pw_gid != SERVICE_GID
        or account.pw_dir != str(SERVICE_HOME)
    ):
        raise BrokerError("GB10 controller service identity is invalid")
    cluster = _run([str(SCONTROL), "show", "config"], timeout=30).stdout.splitlines()
    values = [
        line.split("=", 1)[1].strip()
        for line in cluster
        if line.split("=", 1)[0].strip() == "ClusterName" and "=" in line
    ]
    if values != [CLUSTER]:
        raise BrokerError("GB10 controller Slurm authority is invalid")


def _request_memfd(payload: bytes) -> int:
    descriptor = os.memfd_create("loom-gb10-supervisor-request", flags=0)
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            os.close(descriptor)
            raise BrokerError("GB10 external supervisor request staging failed")
        offset += written
    os.lseek(descriptor, 0, os.SEEK_SET)
    return descriptor


def _exec_helper(candidate: Path, payload: bytes) -> NoReturn:
    spec = helper_exec_spec(candidate, service_uid=SERVICE_UID, service_gid=SERVICE_GID)
    descriptor = _request_memfd(payload)
    os.chdir(spec.cwd)
    os.dup2(descriptor, 0)
    if descriptor != 0:
        os.close(descriptor)
    os.setgroups([])
    os.setgid(SERVICE_GID)
    os.setuid(SERVICE_UID)
    os.execve(spec.argv[0], spec.argv, spec.environment)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    try:
        if os.geteuid() != 0 or os.getegid() != 0:
            raise BrokerError("GB10 external supervisor broker identity is invalid")
        if len(arguments) == 2 and arguments[0] == "--install-authority":
            _require_host_authority()
            install_forced_key(Path(arguments[1]))
            return 0
        if arguments:
            raise BrokerError("GB10 external supervisor broker arguments are invalid")
        payload = sys.stdin.buffer.read(_MAX_REQUEST_BYTES + 1)
        candidate_sha, candidate_tree = parse_request_identity(payload)
        _require_host_authority()
        _safe_executable(UV_BINARY, owner_uid=0, owner_gid=0, label="uv")
        _safe_executable(SYSTEM_PYTHON, owner_uid=0, owner_gid=0, label="system Python")
        candidate = ensure_candidate(CANDIDATES_ROOT, candidate_sha, candidate_tree)
        _exec_helper(candidate, payload)
    except (BrokerError, OSError, KeyError, subprocess.SubprocessError):
        return 1
    return 1  # pragma: no cover - execve never returns


if __name__ == "__main__":
    raise SystemExit(main())
