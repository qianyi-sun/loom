#!/usr/bin/python3
"""Root forced-command broker for the GB10 external autoscaler supervisor.

The broker accepts one canonical JSON envelope on stdin.  It verifies or
publishes the exact root-owned candidate, then either drops to the fixed Slurm
service identity and execs that candidate's typed supervisor helper or invokes
the fixed root-installed Slurm acceptance authority.  It has no command/path
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
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, cast

CONTROLLER = "gx10-01c7"
CLUSTER = "trt-gb10"
SERVICE_USER = "loom-rollout"
SERVICE_UID = 995
SERVICE_GID = 2007
SERVICE_HOME = Path("/var/lib/loom-rollout")
CANDIDATES_ROOT = Path("/opt/loom-staging-runner/candidates")
REMOTE_URL = "https://github.com/qianyi-sun/loom.git"
SYSTEM_PYTHON = Path("/usr/bin/python3")
SYSTEMD_RUN = Path("/usr/bin/systemd-run")
SYSTEMCTL = Path("/usr/bin/systemctl")
RUNUSER = Path("/usr/sbin/runuser")
SQUEUE = Path("/usr/bin/squeue")
SCANCEL = Path("/usr/bin/scancel")
CGROUP_ROOT = Path("/sys/fs/cgroup")
UV_BINARY = Path("/usr/local/bin/uv")
SCONTROL = Path("/usr/bin/scontrol")
ACCEPTANCE_AUTHORITY = Path("/usr/local/libexec/loom-gb10-slurm-acceptance-authority")
ACCEPTANCE_STATE_ROOT = Path("/run/loom-gb10-slurm-authority")
ACCEPTANCE_JOB_STATE_ROOT = ACCEPTANCE_STATE_ROOT / "jobs"
ACCEPTANCE_LOCK_PATH = ACCEPTANCE_STATE_ROOT / "acceptance.lock"
INSTALLED_BROKER = Path("/usr/local/libexec/loom-gb10-external-supervisor-broker")
REMOTE_SSH_USER = "qianyi"
REMOTE_SSH_HOME = Path("/home/qianyi")
_AUTHORIZED_KEY_MARKER = "loom-gb10-external-supervisor"
_ROLLOUT_KEY_MARKER = "loom-staging-rollout"
_LOCK_NAME = ".loom-gb10-external-supervisor-broker.lock"
_HELPER_MODULE = "loom_cli.rollout.operator.protected_gb10_external_supervisor_transport"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_JOB_ID_RE = re.compile(r"^[1-9][0-9]*$")
_PROBE_JOB_NAME_RE = re.compile(r"^loom-accept-[0-9a-f]{7}-[1-9][0-9]*-([0-9a-f]{24})$")
_UNIT_NAME_RE = re.compile(r"^loom-gb10-capacity-([0-9a-f]{24})\.service$")
_STALE_JOB_TEMP_RE = re.compile(r"^\.(?:active-job|broker-job)\.[a-z0-9_]{8}$")
_MAX_REQUEST_BYTES = 4 * 1024 * 1024
_MAX_COMMAND_OUTPUT = 1024 * 1024
_MAX_TREE_ENTRIES = 300_000
_MAX_SYMLINK_HOPS = 40
# Reserve cleanup inside the caller's hard timeout instead of extending it.
_TERMINATION_WINDOW_SECONDS = 45.0
_FORCED_REAP_SECONDS = 2.0
ROOT_UID = 0
ROOT_GID = 0
# Node 2 belongs to the separate exclusive task-image-builder reservation.
_GB10_NODES = tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16)))


class BrokerError(RuntimeError):
    """Secret-free fixed broker failure."""


class BrokerInterruptedError(BrokerError):
    """The broker received a termination signal and is unwinding safely."""


_ACTIVE_PROCESS: subprocess.Popen[str] | None = None
_ACTIVE_PROCESS_TERMINATING = False
_DEFERRED_SIGNAL: int | None = None
_SIGNALS_DEFERRED = False


def _handle_broker_signal(signum: int, _frame: object) -> None:
    global _ACTIVE_PROCESS_TERMINATING, _DEFERRED_SIGNAL
    if _SIGNALS_DEFERRED:
        _DEFERRED_SIGNAL = signum
        return
    process = _ACTIVE_PROCESS
    if process is not None:
        if _ACTIVE_PROCESS_TERMINATING:
            return
        _ACTIVE_PROCESS_TERMINATING = True
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    raise BrokerInterruptedError(
        f"GB10 external supervisor broker interrupted safely by signal {signum}"
    )


def _install_signal_handlers() -> dict[int, Any]:
    previous: dict[int, Any] = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.signal(signum, _handle_broker_signal)
    return previous


def _restore_signal_handlers(previous: dict[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _begin_signal_deferral() -> bool:
    global _SIGNALS_DEFERRED
    previous = _SIGNALS_DEFERRED
    _SIGNALS_DEFERRED = True
    return previous


def _finish_signal_deferral(previous: bool) -> None:
    global _SIGNALS_DEFERRED, _DEFERRED_SIGNAL
    _SIGNALS_DEFERRED = previous
    if previous:
        return
    signum = _DEFERRED_SIGNAL
    _DEFERRED_SIGNAL = None
    if signum is not None:
        raise BrokerInterruptedError(
            f"GB10 external supervisor broker interrupted safely by signal {signum}"
        )


def _close_process_pipes(process: subprocess.Popen[str]) -> None:
    for stream in (process.stdout, process.stderr, process.stdin):
        if stream is not None and not stream.closed:
            stream.close()


def _terminate_and_reap(
    process: subprocess.Popen[str],
    *,
    timeout: float,
    signal_already_sent: bool,
) -> None:
    global _ACTIVE_PROCESS_TERMINATING
    _ACTIVE_PROCESS_TERMINATING = True
    blocked = {signal.SIGTERM, signal.SIGINT}
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
    try:
        if not signal_already_sent:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        forced_window = min(_FORCED_REAP_SECONDS, timeout / 4)
        graceful_window = max(0.0, timeout - forced_window)
        try:
            process.communicate(timeout=graceful_window)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        retry_window = min(0.1, forced_window / 2)
        try:
            process.wait(timeout=max(0.0, forced_window - retry_window))
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=retry_window)
            except subprocess.TimeoutExpired as exc:
                raise BrokerError("GB10 external supervisor command failed safely") from exc
    finally:
        _close_process_pipes(process)
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)


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


def render_authorized_keys(
    existing: bytes,
    public_key: bytes,
    *,
    predecessor_public_key: bytes | None = None,
) -> bytes:
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
    if predecessor_public_key is not None:
        predecessor_algorithm, predecessor_encoded = _public_key_identity(
            predecessor_public_key
        )
        if predecessor_encoded == encoded:
            raise BrokerError("GB10 external supervisor predecessor key is not distinct")
        predecessor_forced = (
            f'restrict,command="/usr/bin/sudo -n -- {INSTALLED_BROKER}" '
            f"{predecessor_algorithm} {predecessor_encoded} {_AUTHORIZED_KEY_MARKER}"
        )
        predecessor_normal = (
            f"{predecessor_algorithm} {predecessor_encoded} {_ROLLOUT_KEY_MARKER}"
        )
        predecessor_matching = [line for line in lines if predecessor_encoded in line]
        controller_matching = [line for line in lines if encoded in line]
        rollout_marked = [
            line
            for line in lines
            if not line.lstrip().startswith("#")
            and line.rstrip().endswith(f" {_ROLLOUT_KEY_MARKER}")
        ]
        if len(predecessor_matching) > 1:
            raise BrokerError("GB10 external supervisor predecessor authority is duplicated")
        if any(line.strip() != predecessor_normal for line in rollout_marked):
            raise BrokerError("GB10 external supervisor rollout authority is ambiguous")
        if len(marked) > 1:
            raise BrokerError("GB10 external supervisor forced key marker is ambiguous")
        if marked and marked[0].strip() == predecessor_forced:
            if predecessor_matching != marked or controller_matching:
                raise BrokerError("GB10 external supervisor predecessor key is ambiguous")
            migrated = text.replace(predecessor_forced, predecessor_normal, 1)
            migrated_prefix = (
                migrated
                if not migrated or migrated.endswith("\n")
                else migrated + "\n"
            )
            return (migrated_prefix + expected + "\n").encode("utf-8")
        if marked and marked[0].strip() == expected:
            if controller_matching != marked or any(
                line.strip() != predecessor_normal for line in predecessor_matching
            ):
                raise BrokerError("GB10 external supervisor key authority is duplicated")
            if not predecessor_matching:
                return (predecessor_normal + "\n").encode("ascii") + existing
            return existing
        if marked:
            raise BrokerError("GB10 external supervisor forced key marker is ambiguous")
        if controller_matching:
            raise BrokerError("GB10 external supervisor key is already present without force")
        if any(line.strip() != predecessor_normal for line in predecessor_matching):
            raise BrokerError("GB10 external supervisor predecessor key is ambiguous")
        existing_prefix = (
            existing if not existing or existing.endswith(b"\n") else existing + b"\n"
        )
        additions = [] if predecessor_matching else [predecessor_normal]
        additions.append(expected)
        return existing_prefix + ("\n".join(additions) + "\n").encode("ascii")
    if len(marked) > 1 or (marked and marked[0].strip() != expected):
        raise BrokerError("GB10 external supervisor forced key marker is ambiguous")
    matching = [line for line in lines if encoded in line]
    if marked:
        if matching != marked:
            raise BrokerError("GB10 external supervisor key authority is duplicated")
        return existing
    if matching:
        raise BrokerError("GB10 external supervisor key is already present without force")
    authorized_keys_prefix = (
        existing if not existing or existing.endswith(b"\n") else existing + b"\n"
    )
    return authorized_keys_prefix + expected.encode("ascii") + b"\n"


def _read_public_key_file(public_key_path: Path) -> bytes:
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
    return public_key


def install_forced_key(
    public_key_path: Path,
    *,
    predecessor_public_key_path: Path | None = None,
) -> None:
    public_key = _read_public_key_file(public_key_path)
    predecessor_public_key = (
        None
        if predecessor_public_key_path is None
        else _read_public_key_file(predecessor_public_key_path)
    )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
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
    rendered = render_authorized_keys(
        existing,
        public_key,
        predecessor_public_key=predecessor_public_key,
    )
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


def _parse_request(payload: bytes) -> dict[str, object]:
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
        "observe_credential": common,
        "publish_credential": common,
        "accept_capacity": common | {"nodes", "profile_sha256"},
    }
    if (
        type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
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
    if operation == "accept_capacity" and (
        value.get("nodes") != list(_GB10_NODES)
        or type(value.get("profile_sha256")) is not str
        or _SHA256_RE.fullmatch(value["profile_sha256"]) is None
    ):
        raise BrokerError("GB10 capacity request authority is invalid")
    return value


def parse_request_identity(payload: bytes) -> tuple[str, str]:
    """Validate the outer typed envelope and return its candidate identity."""

    value = _parse_request(payload)
    return str(value["candidate_sha"]), str(value["candidate_tree"])


def accept_capacity(payload: bytes) -> bytes:
    """Run only the fixed root-installed acceptance authority for a typed request."""

    request = _parse_request(payload)
    if request.get("operation") != "accept_capacity":
        raise BrokerError("GB10 capacity operation is invalid")
    candidate_sha = str(request["candidate_sha"])
    candidate_tree = str(request["candidate_tree"])
    profile_sha256 = str(request["profile_sha256"])
    nodes = cast(list[str], request["nodes"])
    _safe_executable(
        ACCEPTANCE_AUTHORITY,
        owner_uid=ROOT_UID,
        owner_gid=ROOT_GID,
        label="GB10 acceptance authority",
    )
    _safe_executable(SYSTEM_PYTHON, owner_uid=ROOT_UID, owner_gid=ROOT_GID, label="system Python")
    with _acceptance_lock():
        _reconcile_stale_job_states(deadline=time.monotonic() + 60.0)
        unit_name = f"loom-gb10-capacity-{secrets.token_hex(12)}.service"
        job_state_path = ACCEPTANCE_JOB_STATE_ROOT / f"{unit_name}.json"
        cgroup_path = CGROUP_ROOT / "system.slice" / unit_name
        result = _run_contained_authority(
            candidate_sha=candidate_sha,
            unit_name=unit_name,
            job_state_path=job_state_path,
            cgroup_path=cgroup_path,
            timeout=1200,
        )
    artifact_bytes = result.stdout.encode()
    if (
        not artifact_bytes
        or len(artifact_bytes) > _MAX_COMMAND_OUTPUT
        or not artifact_bytes.endswith(b"\n")
    ):
        raise BrokerError("GB10 acceptance authority output is invalid")
    try:
        artifact = json.loads(artifact_bytes)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise BrokerError("GB10 acceptance authority output is invalid") from exc
    if (
        not isinstance(artifact, dict)
        or _canonical_json(artifact) != artifact_bytes
        or artifact.get("schema_version") != 1
        or artifact.get("kind") != "loom_gb10_slurm_acceptance"
        or artifact.get("result") != "pass"
        or artifact.get("candidate_sha") != candidate_sha
        or artifact.get("candidate_tree") != candidate_tree
        or artifact.get("profile_sha256") != profile_sha256
        or artifact.get("nodes") != nodes
    ):
        raise BrokerError("GB10 acceptance authority evidence drifted")
    return _canonical_json(
        {
            "acceptance": artifact,
            "operation": "accept_capacity",
            "schema_version": 1,
            "status": "ok",
        }
    )


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
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != owner_uid
            or opened.st_gid != owner_gid
            or stat.S_IMODE(opened.st_mode) & 0o022
            or (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
        ):
            raise BrokerError(f"{label} metadata is unsafe")
    finally:
        os.close(descriptor)


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


def _validate_relative_symlink_chain(
    path: Path,
    target: Path,
    *,
    root_resolved: Path,
    system_python: Path,
) -> None:
    try:
        resolved_parts = list(path.parent.resolve(strict=True).relative_to(root_resolved).parts)
    except (OSError, ValueError) as exc:
        raise BrokerError("candidate runtime symlink is unsafe") from exc
    pending = deque(target.parts)
    symlink_hops = 0
    while pending:
        part = pending.popleft()
        if part in {"", "."}:
            continue
        if part == "..":
            if not resolved_parts:
                raise BrokerError("candidate runtime symlink escapes authority")
            resolved_parts.pop()
            continue
        resolved_parts.append(part)
        destination = root_resolved.joinpath(*resolved_parts)
        try:
            metadata = os.lstat(destination)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise BrokerError("candidate runtime symlink is unsafe") from exc
        if not stat.S_ISLNK(metadata.st_mode):
            continue
        symlink_hops += 1
        if symlink_hops > _MAX_SYMLINK_HOPS:
            raise BrokerError("candidate runtime symlink is unsafe")
        nested = Path(os.readlink(destination))
        resolved_parts.pop()
        if nested.is_absolute():
            try:
                resolved = nested.resolve(strict=True)
            except OSError as exc:
                raise BrokerError("candidate runtime symlink is unsafe") from exc
            if pending or resolved != system_python.resolve(strict=True):
                raise BrokerError("candidate runtime symlink escapes authority")
            return
        pending.extendleft(reversed(nested.parts))


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
                    # Reject a direct lexical escape before resolving the
                    # complete in-root chain.  Standard venvs terminate at the
                    # separately approved absolute system-Python link.
                    destination = Path(os.path.normpath(path.parent.resolve(strict=True) / target))
                    if not destination.is_relative_to(root_resolved):
                        raise BrokerError("candidate runtime symlink escapes authority")
                    _validate_relative_symlink_chain(
                        path,
                        target,
                        root_resolved=root_resolved,
                        system_python=system_python,
                    )
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
    timeout: float = 900,
    check: bool = True,
    run_as: tuple[int, int] | None = None,
) -> subprocess.CompletedProcess[str]:
    global _ACTIVE_PROCESS, _ACTIVE_PROCESS_TERMINATING
    if not argv or any(not item or "\x00" in item for item in argv) or not 0 < timeout <= 1800:
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
    process: subprocess.Popen[str] | None = None
    termination_window = min(_TERMINATION_WINDOW_SECONDS, timeout / 2)
    execution_timeout = timeout - termination_window
    try:
        blocked = {signal.SIGTERM, signal.SIGINT}
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
        try:
            if privilege_identity is None:
                process = subprocess.Popen(
                    argv,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                    start_new_session=True,
                )
            else:
                process = subprocess.Popen(
                    argv,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                    start_new_session=True,
                    user=privilege_identity[0],
                    group=privilege_identity[1],
                    extra_groups=(),
                )
            _ACTIVE_PROCESS = process
            _ACTIVE_PROCESS_TERMINATING = False
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        stdout, stderr = process.communicate(timeout=execution_timeout)
    except subprocess.TimeoutExpired:
        if process is not None:
            _terminate_and_reap(
                process,
                timeout=termination_window,
                signal_already_sent=_ACTIVE_PROCESS_TERMINATING,
            )
        raise BrokerError("GB10 external supervisor command failed safely") from None
    except BaseException as exc:
        if process is not None:
            _terminate_and_reap(
                process,
                timeout=termination_window,
                signal_already_sent=(
                    isinstance(exc, BrokerInterruptedError) and _ACTIVE_PROCESS_TERMINATING
                ),
            )
        raise
    finally:
        if _ACTIVE_PROCESS is process:
            _ACTIVE_PROCESS = None
            _ACTIVE_PROCESS_TERMINATING = False
    if process is None:
        raise BrokerError("GB10 external supervisor command failed safely")
    result = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    if (
        len(result.stdout.encode()) > _MAX_COMMAND_OUTPUT
        or len(result.stderr.encode()) > _MAX_COMMAND_OUTPUT
        or (check and result.returncode != 0)
    ):
        raise BrokerError("GB10 external supervisor command failed safely")
    return result


def _cleanup_environment() -> dict[str, str]:
    return {
        "HOME": "/root",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }


def _run_cleanup_command(
    argv: list[str],
    *,
    timeout: float = 30,
    check: bool = True,
    deadline: float | None = None,
) -> subprocess.CompletedProcess[str]:
    if not argv or any(not item or "\x00" in item for item in argv):
        raise BrokerError("GB10 cleanup command is invalid")
    effective_timeout = timeout
    if deadline is not None:
        effective_timeout = min(timeout, deadline - time.monotonic())
    if effective_timeout <= 0:
        raise BrokerError("GB10 cleanup time budget exhausted safely")
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=effective_timeout,
            env=_cleanup_environment(),
        )
    except subprocess.TimeoutExpired as exc:
        raise BrokerError("GB10 cleanup command timed out safely") from exc
    if (
        len(result.stdout.encode()) > 64 * 1024
        or len(result.stderr.encode()) > 64 * 1024
        or (check and result.returncode != 0)
    ):
        raise BrokerError("GB10 cleanup command failed safely")
    return result


def _systemctl(
    *arguments: str,
    timeout: float = 30,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return _run_cleanup_command(
        [str(SYSTEMCTL), *arguments],
        timeout=timeout,
        check=check,
    )


def _contained_authority_argv(
    *,
    unit_name: str,
    candidate_sha: str,
    job_state_path: Path,
) -> list[str]:
    if (
        _UNIT_NAME_RE.fullmatch(unit_name) is None
        or _SHA_RE.fullmatch(candidate_sha) is None
        or not job_state_path.is_absolute()
        or ".." in job_state_path.parts
    ):
        raise BrokerError("GB10 acceptance containment identity is invalid")
    return [
        str(SYSTEMD_RUN),
        "--quiet",
        "--wait",
        "--pipe",
        "--collect",
        "--service-type=exec",
        "--slice=system.slice",
        f"--unit={unit_name}",
        "--property=KillMode=control-group",
        "--property=Delegate=no",
        "--property=TimeoutStopSec=45s",
        "--property=RuntimeMaxSec=1140s",
        "--property=Environment=HOME=/root",
        "--property=Environment=LANG=C.UTF-8",
        "--property=Environment=LC_ALL=C.UTF-8",
        "--property=Environment=PATH=/usr/bin:/bin",
        "--",
        str(SYSTEM_PYTHON),
        str(ACCEPTANCE_AUTHORITY),
        "--candidate-sha",
        candidate_sha,
        "--image-tag",
        f"staging-{candidate_sha[:7]}",
        "--job-state-path",
        str(job_state_path),
    ]


def _validate_root_directory(path: Path, *, mode: int, label: str) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise BrokerError(f"{label} metadata is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != ROOT_UID
        or metadata.st_gid != ROOT_GID
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise BrokerError(f"{label} metadata is unsafe")
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != ROOT_UID
            or opened.st_gid != ROOT_GID
            or stat.S_IMODE(opened.st_mode) != mode
            or (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
        ):
            raise BrokerError(f"{label} metadata is unsafe")
    finally:
        os.close(descriptor)


def _canonical_job_state(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _read_persisted_job_state(path: Path) -> dict[str, str | int] | None:
    if not os.path.lexists(path):
        return None
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise BrokerError("GB10 persisted job state is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != ROOT_UID
            or metadata.st_gid != ROOT_GID
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or not 1 <= metadata.st_size <= 1024
        ):
            raise BrokerError("GB10 persisted job state metadata is unsafe")
        encoded = os.read(descriptor, 1025)
        current = os.lstat(path)
        if len(encoded) != metadata.st_size or (
            current.st_dev,
            current.st_ino,
            current.st_size,
        ) != (metadata.st_dev, metadata.st_ino, metadata.st_size):
            raise BrokerError("GB10 persisted job state changed during validation")
    finally:
        os.close(descriptor)
    try:
        value = json.loads(encoded.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise BrokerError("GB10 persisted job state is invalid") from exc
    if (
        not isinstance(value, dict)
        or _canonical_job_state(value) != encoded
        or value.get("schema_version") != 1
        or type(value.get("schema_version")) is not int
    ):
        raise BrokerError("GB10 persisted job state is invalid")
    fields = set(value)
    if fields == {"schema_version", "unit_name"}:
        unit_name = value.get("unit_name")
        if type(unit_name) is not str or _UNIT_NAME_RE.fullmatch(unit_name) is None:
            raise BrokerError("GB10 persisted containment state is invalid")
    elif fields in (
        {"job_name", "schema_version"},
        {"job_id", "job_name", "schema_version"},
    ):
        job_name = value.get("job_name")
        job_id = value.get("job_id")
        if (
            type(job_name) is not str
            or _PROBE_JOB_NAME_RE.fullmatch(job_name) is None
            or (
                job_id is not None
                and (type(job_id) is not str or _JOB_ID_RE.fullmatch(job_id) is None)
            )
        ):
            raise BrokerError("GB10 persisted job state is invalid")
    else:
        raise BrokerError("GB10 persisted job state fields are invalid")
    return cast(dict[str, str | int], value)


def _write_persisted_job_state(
    path: Path,
    *,
    job_name: str | None = None,
    job_id: str | None = None,
    unit_name: str | None = None,
) -> None:
    if unit_name is not None:
        if job_name is not None or job_id is not None or _UNIT_NAME_RE.fullmatch(unit_name) is None:
            raise BrokerError("GB10 persisted containment identity is invalid")
        value: dict[str, str | int] = {"schema_version": 1, "unit_name": unit_name}
    else:
        if (
            job_name is None
            or job_id is None
            or _PROBE_JOB_NAME_RE.fullmatch(job_name) is None
            or _JOB_ID_RE.fullmatch(job_id) is None
        ):
            raise BrokerError("GB10 persisted job identity is invalid")
        value = {"job_id": job_id, "job_name": job_name, "schema_version": 1}
    _validate_root_directory(path.parent, mode=0o700, label="GB10 job state root")
    encoded = _canonical_job_state(value)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".broker-job.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, ROOT_UID, ROOT_GID)
        offset = 0
        while offset < len(encoded):
            written = os.write(descriptor, encoded[offset:])
            if written <= 0:
                raise BrokerError("GB10 persisted job state write failed safely")
            offset += written
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            metadata.st_uid != ROOT_UID
            or metadata.st_gid != ROOT_GID
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size != len(encoded)
        ):
            raise BrokerError("GB10 persisted job state metadata is unsafe")
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_path_directory(path.parent)
        state = _read_persisted_job_state(path)
        if state != value:
            raise BrokerError("GB10 persisted job state readback mismatched")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if os.path.lexists(temporary):
            temporary.unlink()
            _fsync_path_directory(path.parent)


def _fsync_path_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _service_cleanup_command(*argv: str) -> list[str]:
    return [str(RUNUSER), "-u", SERVICE_USER, "--", *argv]


def _parse_unique_job_id(stdout: str, *, job_name: str) -> str | None:
    rows = [line.strip() for line in stdout.splitlines() if line.strip()]
    job_ids: list[str] = []
    for row in rows:
        job_id, separator, observed_name = row.partition("|")
        if (
            not separator
            or "|" in observed_name
            or observed_name != job_name
            or _JOB_ID_RE.fullmatch(job_id) is None
        ):
            raise BrokerError("GB10 persisted job lookup is malformed")
        job_ids.append(job_id)
    if len(job_ids) > 1:
        raise BrokerError("GB10 persisted job lookup is ambiguous")
    return job_ids[0] if job_ids else None


def _observe_unique_persisted_job_id(
    job_name: str,
    *,
    deadline: float | None,
) -> str | None:
    lookup = _run_cleanup_command(
        _service_cleanup_command(
            str(SQUEUE),
            "--noheader",
            f"--user={SERVICE_USER}",
            f"--name={job_name}",
            "--format=%A|%j",
        ),
        timeout=10,
        check=False,
        deadline=deadline,
    )
    if lookup.returncode != 0 or lookup.stderr:
        raise BrokerError("GB10 persisted job lookup failed safely")
    return _parse_unique_job_id(lookup.stdout, job_name=job_name)


def _observe_quiescent_persisted_job_id(
    job_name: str,
    *,
    deadline: float | None,
) -> str | None:
    for observation in range(2):
        job_id = _observe_unique_persisted_job_id(job_name, deadline=deadline)
        if job_id is not None:
            return job_id
        if observation == 0:
            if deadline is not None and deadline - time.monotonic() <= 0.05:
                raise BrokerError("GB10 persisted empty job lookup did not become quiescent")
            time.sleep(0.05)
    return None


def _wait_for_quiescent_persisted_job_empty(
    job_name: str,
    *,
    deadline: float | None,
) -> None:
    convergence_deadline = time.monotonic() + 10.0
    if deadline is not None:
        convergence_deadline = min(convergence_deadline, deadline)
    empty_observations = 0
    while True:
        observed_job_id = _observe_unique_persisted_job_id(
            job_name,
            deadline=convergence_deadline,
        )
        if observed_job_id is None:
            empty_observations += 1
            if empty_observations == 2:
                return
        else:
            empty_observations = 0
        if convergence_deadline - time.monotonic() <= 0.05:
            raise BrokerError("GB10 persisted exact job cleanup did not converge")
        time.sleep(0.05)


def _cleanup_persisted_probe_job(
    path: Path,
    *,
    deadline: float | None,
    expected_unit_name: str | None = None,
) -> None:
    previous_deferral = _begin_signal_deferral()
    failure: BaseException | None = None
    try:
        state = _read_persisted_job_state(path)
        if state is not None:
            unit_name_value = state.get("unit_name")
            if type(unit_name_value) is str:
                if unit_name_value != expected_unit_name:
                    raise BrokerError("GB10 persisted containment identity mismatched")
                path.unlink()
                _fsync_path_directory(path.parent)
                state = None
        if state is not None:
            job_name = cast(str, state["job_name"])
            if expected_unit_name is not None:
                unit_match = _UNIT_NAME_RE.fullmatch(expected_unit_name)
                job_match = _PROBE_JOB_NAME_RE.fullmatch(job_name)
                if (
                    unit_match is None
                    or job_match is None
                    or unit_match.group(1) != job_match.group(1)
                ):
                    raise BrokerError("GB10 persisted job request binding mismatched")
            job_id_value = state.get("job_id")
            job_id: str | None = job_id_value if type(job_id_value) is str else None
            if job_id is None:
                job_id = _observe_quiescent_persisted_job_id(job_name, deadline=deadline)
                if job_id is None:
                    path.unlink()
                    _fsync_path_directory(path.parent)
                else:
                    _write_persisted_job_state(path, job_name=job_name, job_id=job_id)
            if job_id is not None:
                cancel_failure: BaseException | None = None
                try:
                    _run_cleanup_command(
                        _service_cleanup_command(str(SCANCEL), job_id),
                        timeout=10,
                        check=False,
                        deadline=deadline,
                    )
                except BaseException as exc:
                    cancel_failure = exc
                try:
                    _wait_for_quiescent_persisted_job_empty(
                        job_name,
                        deadline=deadline,
                    )
                except BaseException as readback_exc:
                    if cancel_failure is not None:
                        raise cancel_failure from readback_exc
                    raise
                path.unlink()
                _fsync_path_directory(path.parent)
    except BaseException as exc:
        failure = exc
    finally:
        try:
            _finish_signal_deferral(previous_deferral)
        except BaseException as exc:
            if failure is None:
                failure = exc
    if failure is not None:
        raise failure


def _cgroup_is_empty(cgroup_path: Path) -> bool:
    if not os.path.lexists(cgroup_path):
        return True
    metadata = os.lstat(cgroup_path)
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise BrokerError("GB10 containment metadata is unsafe")
    events_path = cgroup_path / "cgroup.events"
    try:
        descriptor = os.open(
            events_path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise BrokerError("GB10 containment evidence is unavailable") from exc
    try:
        current = os.fstat(descriptor)
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 1
            or current.st_size > 1024 * 1024
        ):
            raise BrokerError("GB10 containment evidence is unsafe")
        encoded = os.read(descriptor, 1024 * 1024 + 1)
    finally:
        os.close(descriptor)
    if len(encoded) > 1024 * 1024:
        raise BrokerError("GB10 containment evidence is unsafe")
    try:
        lines = encoded.decode("ascii").splitlines()
    except UnicodeError as exc:
        raise BrokerError("GB10 containment evidence is unsafe") from exc
    populated: list[str] = []
    for line in lines:
        fields = line.split()
        if len(fields) != 2:
            raise BrokerError("GB10 containment evidence is unsafe")
        if fields[0] == "populated":
            populated.append(fields[1])
    if len(populated) != 1 or populated[0] not in {"0", "1"}:
        raise BrokerError("GB10 containment evidence is unsafe")
    return populated[0] == "0"


def _wait_for_empty_cgroup(cgroup_path: Path, *, timeout: float) -> bool:
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        if _cgroup_is_empty(cgroup_path):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)


def _terminate_and_verify_containment(
    *,
    unit_name: str,
    cgroup_path: Path,
    job_state_path: Path,
    graceful_timeout: float = 45.0,
    forced_timeout: float = 10.0,
    hard_deadline: float | None = None,
) -> None:
    if _UNIT_NAME_RE.fullmatch(unit_name) is None:
        raise BrokerError("GB10 containment unit identity is invalid")
    previous_deferral = _begin_signal_deferral()
    failure: BaseException | None = None

    def remaining() -> float:
        if hard_deadline is None:
            return float("inf")
        return max(0.0, hard_deadline - time.monotonic())

    def available(*, reserve: float = 0.0) -> float:
        return max(0.0, remaining() - reserve)

    def record(exc: BaseException) -> None:
        nonlocal failure
        if failure is None:
            failure = exc

    populated = True
    try:
        populated = not _cgroup_is_empty(cgroup_path)
    except BaseException as exc:
        record(exc)
    if populated:
        try:
            timeout = min(10.0, available(reserve=30.0))
            if timeout <= 0:
                raise BrokerError("GB10 acceptance containment time budget exhausted")
            _systemctl(
                "kill",
                "--kill-whom=all",
                "--signal=SIGTERM",
                unit_name,
                timeout=timeout,
            )
        except BaseException as exc:
            record(exc)
        graceful_empty = False
        try:
            grace = min(graceful_timeout, available(reserve=50.0))
            graceful_empty = bool(grace > 0 and _wait_for_empty_cgroup(cgroup_path, timeout=grace))
        except BaseException as exc:
            record(exc)
        if not graceful_empty:
            try:
                timeout = min(10.0, available(reserve=40.0))
                if timeout <= 0:
                    raise BrokerError("GB10 acceptance containment time budget exhausted")
                _systemctl(
                    "kill",
                    "--kill-whom=all",
                    "--signal=SIGKILL",
                    unit_name,
                    timeout=timeout,
                )
            except BaseException as exc:
                record(exc)
            try:
                force = min(forced_timeout, available(reserve=30.0))
                if not (force > 0 and _wait_for_empty_cgroup(cgroup_path, timeout=force)):
                    record(BrokerError("GB10 acceptance containment did not become empty"))
            except BaseException as exc:
                record(exc)
    try:
        if remaining() <= 0:
            raise BrokerError("GB10 acceptance cleanup time budget exhausted")
        _cleanup_persisted_probe_job(
            job_state_path,
            deadline=hard_deadline,
            expected_unit_name=unit_name,
        )
    except BaseException as exc:
        record(exc)
    try:
        if not _cgroup_is_empty(cgroup_path):
            raise BrokerError("GB10 acceptance containment remained populated")
    except BaseException as exc:
        record(exc)
    try:
        _finish_signal_deferral(previous_deferral)
    except BaseException as exc:
        record(exc)
    if failure is not None:
        raise failure


def _run_contained_authority(
    *,
    candidate_sha: str,
    unit_name: str,
    job_state_path: Path,
    cgroup_path: Path,
    timeout: float,
    graceful_timeout: float = 45.0,
    forced_timeout: float = 10.0,
) -> subprocess.CompletedProcess[str]:
    hard_deadline = time.monotonic() + timeout
    containment_reserve = min(90.0, timeout / 2)
    launch_timeout = timeout - containment_reserve
    result: subprocess.CompletedProcess[str] | None = None
    failure: BaseException | None = None
    try:
        if os.path.lexists(job_state_path):
            raise BrokerError("GB10 acceptance containment state already exists")
        _write_persisted_job_state(job_state_path, unit_name=unit_name)
        result = _run(
            _contained_authority_argv(
                unit_name=unit_name,
                candidate_sha=candidate_sha,
                job_state_path=job_state_path,
            ),
            timeout=launch_timeout,
        )
    except BaseException as exc:
        failure = exc
    try:
        _terminate_and_verify_containment(
            unit_name=unit_name,
            cgroup_path=cgroup_path,
            job_state_path=job_state_path,
            graceful_timeout=graceful_timeout,
            forced_timeout=forced_timeout,
            hard_deadline=hard_deadline,
        )
    except BaseException as exc:
        failure = exc
    if failure is not None:
        raise failure
    if result is None:
        raise BrokerError("GB10 contained authority returned no result")
    return result


def _remove_verified_stale_job_temp(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise BrokerError("GB10 stale job temporary metadata is unsafe") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != ROOT_UID
            or metadata.st_gid != ROOT_GID
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > 1024
        ):
            raise BrokerError("GB10 stale job temporary metadata is unsafe")
        current = os.lstat(path)
        if (
            current.st_dev,
            current.st_ino,
            current.st_uid,
            current.st_gid,
            current.st_mode,
            current.st_nlink,
            current.st_size,
        ) != (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_uid,
            metadata.st_gid,
            metadata.st_mode,
            metadata.st_nlink,
            metadata.st_size,
        ):
            raise BrokerError("GB10 stale job temporary metadata changed during validation")
        path.unlink()
        _fsync_path_directory(path.parent)
    finally:
        os.close(descriptor)


def _reconcile_stale_job_states(*, deadline: float | None = None) -> None:
    _validate_root_directory(
        ACCEPTANCE_JOB_STATE_ROOT,
        mode=0o700,
        label="GB10 acceptance job state root",
    )
    for entry in sorted(os.scandir(ACCEPTANCE_JOB_STATE_ROOT), key=lambda item: item.name):
        if _STALE_JOB_TEMP_RE.fullmatch(entry.name) is not None:
            _remove_verified_stale_job_temp(Path(entry.path))
            continue
        if not entry.name.endswith(".service.json") or "/" in entry.name or "\x00" in entry.name:
            raise BrokerError("GB10 acceptance job state inventory is invalid")
        unit_name = entry.name.removesuffix(".json")
        if _UNIT_NAME_RE.fullmatch(unit_name) is None:
            raise BrokerError("GB10 acceptance stale unit identity is invalid")
        _terminate_and_verify_containment(
            unit_name=unit_name,
            cgroup_path=CGROUP_ROOT / "system.slice" / unit_name,
            job_state_path=Path(entry.path),
            graceful_timeout=min(
                45.0, max(0.0, (deadline or time.monotonic() + 45) - time.monotonic())
            ),
            forced_timeout=10.0,
            hard_deadline=deadline,
        )


@contextmanager
def _acceptance_lock() -> Any:
    _validate_root_directory(
        ACCEPTANCE_STATE_ROOT,
        mode=0o700,
        label="GB10 acceptance runtime root",
    )
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(ACCEPTANCE_LOCK_PATH, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, ROOT_UID, ROOT_GID)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != ROOT_UID
            or metadata.st_gid != ROOT_GID
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise BrokerError("GB10 acceptance lock metadata is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


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
            timeout=1200,
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


def _main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    try:
        if os.geteuid() != 0 or os.getegid() != 0:
            raise BrokerError("GB10 external supervisor broker identity is invalid")
        if len(arguments) in {2, 3} and arguments[0] == "--install-authority":
            _require_host_authority()
            install_forced_key(
                Path(arguments[1]),
                predecessor_public_key_path=(
                    None if len(arguments) == 2 else Path(arguments[2])
                ),
            )
            return 0
        if arguments:
            raise BrokerError("GB10 external supervisor broker arguments are invalid")
        payload = sys.stdin.buffer.read(_MAX_REQUEST_BYTES + 1)
        request = _parse_request(payload)
        _require_host_authority()
        if request.get("operation") == "accept_capacity":
            sys.stdout.buffer.write(accept_capacity(payload))
            return 0
        candidate_sha = str(request["candidate_sha"])
        candidate_tree = str(request["candidate_tree"])
        _safe_executable(UV_BINARY, owner_uid=0, owner_gid=0, label="uv")
        _safe_executable(SYSTEM_PYTHON, owner_uid=0, owner_gid=0, label="system Python")
        candidate = ensure_candidate(CANDIDATES_ROOT, candidate_sha, candidate_tree)
        _exec_helper(candidate, payload)
    except (BrokerError, OSError, KeyError, subprocess.SubprocessError):
        return 1
    return 1  # pragma: no cover - execve never returns


def main(argv: list[str] | None = None) -> int:
    previous = _install_signal_handlers()
    try:
        return _main(argv)
    finally:
        _restore_signal_handlers(previous)


if __name__ == "__main__":
    raise SystemExit(main())
