#!/usr/bin/env python3
"""Fail-closed controller orchestration for one Phase 1 builder node."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import time
import tomllib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Protocol

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops import task_image_builder_authority as authority  # noqa: E402

ZERO_HASH = "0" * 64
INERT_BLOCKER = "phase2_guard_provider_release_missing"
CLEANUP_ABSENCE = {
    "state": "absent",
    "processes_absent": True,
    "mounts_absent": True,
    "job_directory_absent": True,
}
RECEIPT_SCHEMA = "loom.task-image-builder-node-maintenance/v1"
HOST_CONVERGER = "scripts/ops/task_image_builder_host_converge.py"
MAINTENANCE_SCRIPT = "scripts/ops/task_image_builder_node_maintenance.py"
DEFAULT_TIMEOUT_SECONDS = 300.0
PRE_RUNNING_POLL_BUDGET_SECONDS = 2 * DEFAULT_TIMEOUT_SECONDS
RESERVATION_OPERATIONAL_MARGIN_SECONDS = 300.0
RESERVATION_DURATION_SECONDS = (
    PRE_RUNNING_POLL_BUDGET_SECONDS + RESERVATION_OPERATIONAL_MARGIN_SECONDS
)
BUILDER_JOBS_ROOT = Path("/var/lib/loom-task-builder/jobs")
CGROUP_CONFIG = Path("/etc/slurm/cgroup.conf")
DESIRED_CGROUP = (
    b"CgroupPlugin=autodetect\n"
    b"ConstrainCores=yes\n"
    b"ConstrainRAMSpace=yes\n"
    b"ConstrainSwapSpace=yes\n"
    b"ConstrainDevices=yes\n"
)
NONTERMINAL_SLURM_STATES = frozenset(
    {
        "COMPLETING",
        "CONFIGURING",
        "PENDING",
        "REQUEUED",
        "REQUEUE_FED",
        "REQUEUE_HOLD",
        "RESIZING",
        "RUNNING",
        "SIGNALING",
        "STAGE_OUT",
        "SUSPENDED",
    }
)
TERMINAL_SLURM_STATES = frozenset(
    {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "REVOKED",
        "SPECIAL_EXIT",
        "STOPPED",
        "TIMEOUT",
    }
)
MAX_DEVICE_PROGRAMS = 64
MAX_DEVICE_FIELD_BYTES = 256


class MaintenanceError(RuntimeError):
    """The one-node maintenance operation cannot safely continue."""


class Transition(StrEnum):
    PRE_STATE_RECORDED = "pre_state_recorded"
    DRAINED = "drained"
    IDLE = "idle"
    HOST_PREFLIGHTED = "host_preflighted"
    HOST_APPLIED = "host_applied"
    DAEMON_RESTARTED = "daemon_restarted"
    READBACK_VERIFIED = "readback_verified"
    ADMISSION_VERIFIED = "admission_verified"
    RESERVATION_CREATED = "reservation_created"
    SMOKE_QUEUED = "smoke_queued"
    SMOKE_PENDING = "smoke_pending"
    SMOKE_RUNNING = "smoke_running"
    SMOKE_OBSERVED = "smoke_observed"
    SMOKE_RELEASED = "smoke_released"
    SMOKE_COMPLETED = "smoke_completed"
    SMOKE_CLEANED = "smoke_cleaned"
    RESERVATION_DELETED = "reservation_deleted"
    EMERGENCY_CONTAINED = "emergency_contained"
    PREPARED = "prepared"
    BLOCKED = "blocked"
    ROLLED_BACK = "rolled_back"
    DRAINED_ROLLBACK_FAILED = "drained_rollback_failed"


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class CommandRunner(Protocol):
    def run(self, args: tuple[str, ...]) -> CommandResult: ...


class SubprocessCommandRunner:
    def run(self, args: tuple[str, ...]) -> CommandResult:
        try:
            result = subprocess.run(
                args,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=False,
                timeout=30,
                env={"LC_ALL": "C", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise MaintenanceError("maintenance command execution failed") from exc
        if len(result.stdout) > 1024 * 1024 or len(result.stderr) > 1024 * 1024:
            raise MaintenanceError("maintenance command output exceeds its limit")
        try:
            stdout = result.stdout.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MaintenanceError("maintenance command output is not UTF-8") from exc
        return CommandResult(result.returncode, stdout, result.stderr.decode("utf-8", "replace"))


@dataclass(frozen=True)
class NodePolicy:
    cluster_id: str
    controller: str
    builder_nodes: tuple[str, ...]
    trial_partition: str
    builder_partition: str
    account: str
    qos: str
    cpus: int
    memory_mib: int
    pids: int
    wall_time: str
    swap_bytes: int


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _inert() -> dict[str, object]:
    return {
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": [INERT_BLOCKER],
    }


def _read_regular(path: Path, label: str) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise MaintenanceError(f"{label} is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 2 * 1024 * 1024:
            raise MaintenanceError(f"{label} is unsafe")
        payload = os.read(descriptor, 2 * 1024 * 1024 + 1)
        final = os.fstat(descriptor)
        if len(payload) > 2 * 1024 * 1024 or (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise MaintenanceError(f"{label} changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _read_owner_regular(
    path: Path,
    label: str,
    *,
    owner: int,
    mode: int,
    maximum: int = 64 * 1024,
) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise MaintenanceError(f"{label} is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != owner
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_size > maximum
        ):
            raise MaintenanceError(f"{label} metadata is unsafe")
        payload = os.read(descriptor, maximum + 1)
        final = os.fstat(descriptor)
        if len(payload) > maximum or (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        ) != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ):
            raise MaintenanceError(f"{label} changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise MaintenanceError(f"{label} is invalid")
    return value


def _integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise MaintenanceError(f"{label} is invalid")
    return value


def _valid_operation_id(value: str) -> bool:
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return parsed.version == 4 and str(parsed) == value


def _load_policy(
    candidate_root: Path, cluster_id: str, slurm_node: str
) -> tuple[NodePolicy, bytes]:
    if not candidate_root.is_absolute() or candidate_root.is_symlink():
        raise MaintenanceError("candidate root is unsafe")
    policy_path = candidate_root / "deploy/task-image-builder/prerequisites-v1.toml"
    host_converger = candidate_root / HOST_CONVERGER
    if not host_converger.is_file() or host_converger.is_symlink():
        raise MaintenanceError("candidate host converger is unavailable")
    payload = _read_regular(policy_path, "candidate prerequisite policy")
    try:
        raw = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise MaintenanceError("candidate prerequisite policy is invalid") from exc
    if (
        raw.get("schema") != "loom.task-image-builder-prerequisites/v1"
        or raw.get("production_certification_allowed") is not False
        or raw.get("certified_nodes") != []
        or raw.get("unconditional_blockers") != [INERT_BLOCKER]
    ):
        raise MaintenanceError("candidate prerequisite policy is not inert")
    resources = raw.get("resource_profile")
    if not isinstance(resources, dict):
        raise MaintenanceError("candidate resource profile is invalid")
    clusters = [
        item
        for item in raw.get("clusters", [])
        if isinstance(item, dict) and item.get("id") == cluster_id
    ]
    if len(clusters) != 1:
        raise MaintenanceError("candidate cluster policy is not unique")
    cluster = clusters[0]
    nodes = cluster.get("builder_nodes")
    if not isinstance(nodes, list) or not all(isinstance(node, str) for node in nodes):
        raise MaintenanceError("candidate builder node inventory is invalid")
    if len(nodes) != len(set(nodes)) or slurm_node not in nodes:
        raise MaintenanceError("Slurm node is outside the builder inventory")
    return (
        NodePolicy(
            cluster_id=cluster_id,
            controller=_string(cluster.get("controller"), "candidate controller"),
            builder_nodes=tuple(nodes),
            trial_partition=_string(cluster.get("trial_partition"), "candidate trial partition"),
            builder_partition=_string(
                cluster.get("builder_partition"), "candidate builder partition"
            ),
            account=_string(cluster.get("slurm_account"), "candidate Slurm account"),
            qos=_string(cluster.get("slurm_qos"), "candidate Slurm QoS"),
            cpus=_integer(resources.get("cpus"), "candidate CPUs"),
            memory_mib=_integer(resources.get("memory_mib"), "candidate memory"),
            pids=_integer(resources.get("pids"), "candidate pids"),
            wall_time=_string(resources.get("wall_time"), "candidate wall time"),
            swap_bytes=_integer(resources.get("swap_bytes"), "candidate swap"),
        ),
        payload,
    )


def _validate_receipt_root(path: Path, owner: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise MaintenanceError("maintenance receipt root is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != owner
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise MaintenanceError("maintenance receipt root must be owner-only")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_document(
    path: Path,
    document: Mapping[str, object],
    *,
    exclusive: bool,
    owner: int,
) -> bytes:
    payload = _canonical(document) + b"\n"

    def write_payload(descriptor: int) -> None:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise MaintenanceError("maintenance receipt write failed")
            view = view[written:]
        os.fsync(descriptor)

    if exclusive:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            write_payload(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(path.parent)
        return payload
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != owner
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        raise MaintenanceError("maintenance receipt metadata is unsafe")
    temporary = path.parent / f".{path.name}.{uuid.uuid4()}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        write_payload(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
    return payload


def _event(document: dict[str, object], transition: Transition, data: Mapping[str, object]) -> None:
    events = document["events"]
    assert isinstance(events, list)
    previous = ZERO_HASH if not events else str(events[-1]["event_hash"])
    event: dict[str, object] = {
        "sequence": len(events),
        "type": transition.value,
        "previous_hash": previous,
        "data": dict(data),
    }
    event["event_hash"] = _sha(_canonical(event))
    events.append(event)


def _snapshot(runner: CommandRunner, slurm_node: str) -> dict[str, str]:
    result = runner.run(("/usr/bin/scontrol", "show", "node", slurm_node, "-o"))
    if result.returncode != 0:
        raise MaintenanceError("Slurm node readback is unavailable")
    fields = dict(token.split("=", 1) for token in result.stdout.split() if "=" in token)
    if fields.get("NodeName") != slurm_node:
        raise MaintenanceError("Slurm node readback does not bind the target")
    state = fields.get("State")
    reason = fields.get("Reason")
    allocated = fields.get("AllocTRES")
    if not state or reason is None or allocated is None:
        raise MaintenanceError("Slurm node readback is incomplete")
    return {"state": state, "reason": reason, "allocated_tres": allocated}


def _owned_drain_snapshot(
    runner: CommandRunner,
    slurm_node: str,
    loom_reason: str,
) -> dict[str, str]:
    snapshot = _snapshot(runner, slurm_node)
    if "DRAIN" not in snapshot["state"] or snapshot["reason"] != loom_reason:
        raise MaintenanceError("Loom drain ownership is not confirmed")
    return snapshot


def _is_zero_tres(value: str) -> bool:
    if not value:
        return True
    for item in value.split(","):
        name, separator, raw = item.partition("=")
        number = re.fullmatch(r"(\d+)[KMGTP]?", raw)
        if not separator or not name or number is None or int(number.group(1)) != 0:
            return False
    return True


def _require(runner: CommandRunner, command: tuple[str, ...], label: str) -> CommandResult:
    result = runner.run(command)
    return _require_result(result, label)


def _require_result(result: CommandResult, label: str) -> CommandResult:
    if result.returncode != 0:
        raise MaintenanceError(f"{label} failed")
    return result


def _command_observation(command: Sequence[str], result: CommandResult) -> dict[str, object]:
    if (
        len(result.stdout.encode("utf-8")) > 64 * 1024
        or len(result.stderr.encode("utf-8")) > 64 * 1024
    ):
        raise MaintenanceError("maintenance command observation exceeds its limit")
    return {
        "command": list(command),
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _remote(
    runner: CommandRunner,
    target: str,
    command: tuple[str, ...],
    *,
    ssh_config: Path | None,
    candidate_root: Path,
) -> CommandResult:
    allowed = {
        str(candidate_root / HOST_CONVERGER),
        str(candidate_root / MAINTENANCE_SCRIPT),
    }
    if (
        not command
        or command[0] not in allowed
        or not Path(command[0]).is_absolute()
        or any("\x00" in item or "\n" in item or "\r" in item for item in (*command, target))
    ):
        raise MaintenanceError("remote command is unsafe")
    if command[0] == str(candidate_root / HOST_CONVERGER):
        if len(command) < 2 or command[1] not in {"plan", "check", "apply", "rollback"}:
            raise MaintenanceError("remote host command is unsafe")
    elif command[1:2] == ("--internal-node-daemon",):
        if len(command) != 3 or command[2] not in {"restart", "check"}:
            raise MaintenanceError("remote daemon command is unsafe")
    elif command[1:2] == ("--internal-smoke",):
        if (
            len(command) != 5
            or command[2] not in {"observe", "release", "cleanup"}
            or re.fullmatch(r"[1-9][0-9]*", command[3]) is None
            or _valid_operation_id(command[4]) is False
        ):
            raise MaintenanceError("remote smoke command is unsafe")
    else:
        raise MaintenanceError("remote maintenance command is unsafe")
    args: list[str] = [
        "/usr/bin/ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
    ]
    if ssh_config is not None:
        args.extend(("-F", str(ssh_config)))
    args.extend((target, shlex.join(("sudo", "--", *command))))
    return runner.run(tuple(args))


def _daemon_command(candidate_root: Path, action: str) -> tuple[str, ...]:
    return (str(candidate_root / MAINTENANCE_SCRIPT), "--internal-node-daemon", action)


def _smoke_command(
    candidate_root: Path,
    action: str,
    job_id: str,
    operation_id: str,
) -> tuple[str, ...]:
    return (
        str(candidate_root / MAINTENANCE_SCRIPT),
        "--internal-smoke",
        action,
        job_id,
        operation_id,
    )


def _reservation_name(operation_id: str) -> str:
    return "loom_task_builder_maintenance_" + operation_id.replace("-", "")


def _slurm_duration(seconds: float) -> str:
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def _reservation_command(action: str, name: str, slurm_node: str) -> tuple[str, ...]:
    if action == "create":
        return (
            "/usr/bin/scontrol",
            "create",
            "reservation",
            "Name=" + name,
            "Nodes=" + slurm_node,
            "Users=loom-builder",
            "StartTime=now",
            "Duration=" + _slurm_duration(RESERVATION_DURATION_SECONDS),
        )
    return ("/usr/bin/scontrol", "delete", "reservation", "Name=" + name)


def _reservation_readback_command() -> tuple[str, ...]:
    return ("/usr/bin/scontrol", "show", "reservation", "--oneliner")


def _reservation_rows(result: CommandResult) -> dict[str, dict[str, str]]:
    _require_result(result, "maintenance smoke reservation readback")
    rows: dict[str, dict[str, str]] = {}
    for line in result.stdout.splitlines():
        if not line:
            continue
        if line == "No reservations in the system":
            if len(result.stdout.splitlines()) != 1:
                raise MaintenanceError("maintenance smoke reservation readback is invalid")
            return {}
        fields = dict(token.split("=", 1) for token in line.split() if "=" in token)
        name = fields.get("ReservationName")
        if not name or name in rows:
            raise MaintenanceError("maintenance smoke reservation readback is invalid")
        rows[name] = fields
    return rows


def _reservation_binding(
    result: CommandResult,
    name: str,
    slurm_node: str,
) -> dict[str, str]:
    fields = _reservation_rows(result).get(name)
    if (
        fields is None
        or fields.get("Nodes") != slurm_node
        or fields.get("Users") != "loom-builder"
        or fields.get("State") != "ACTIVE"
    ):
        raise MaintenanceError("maintenance smoke reservation binding is invalid")
    return {
        "name": name,
        "node": slurm_node,
        "state": "ACTIVE",
        "user": "loom-builder",
    }


def _reservation_absent(result: CommandResult, name: str) -> dict[str, object]:
    if name in _reservation_rows(result):
        raise MaintenanceError("maintenance smoke reservation still exists")
    return {"name": name, "absent": True}


def _require_daemon_state(result: CommandResult, action: str) -> dict[str, object]:
    if result.returncode != 0:
        raise MaintenanceError(f"named-node slurmd {action} failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise MaintenanceError("named-node slurmd result is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"state", "cgroup_config"}
        or payload.get("state") != "active"
    ):
        raise MaintenanceError("named-node slurmd readback is invalid")
    config = payload.get("cgroup_config")
    if (
        not isinstance(config, dict)
        or set(config) != {"path", "sha256", "contents"}
        or config.get("path") != str(CGROUP_CONFIG)
        or not isinstance(config.get("contents"), str)
        or len(config["contents"].encode("utf-8")) > 64 * 1024
        or config.get("sha256") != _sha(config["contents"].encode("utf-8"))
    ):
        raise MaintenanceError("named-node cgroup readback is invalid")
    return payload


def _host_command(
    candidate_root: Path,
    action: str,
    policy: NodePolicy,
    slurm_node: str,
    bundle: Path,
    receipt_root: Path,
    operation_id: str,
) -> tuple[str, ...]:
    return (
        str(candidate_root / HOST_CONVERGER),
        action,
        "--cluster-id",
        policy.cluster_id,
        "--slurm-node",
        slurm_node,
        "--bundle",
        str(bundle),
        "--receipt-dir",
        str(receipt_root / "host"),
        "--operation-id",
        operation_id,
    )


def _require_host_state(result: CommandResult, state: str, label: str) -> None:
    if result.returncode != 0:
        raise MaintenanceError(f"remote host {label} failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise MaintenanceError(f"remote host {label} result is invalid") from exc
    if not isinstance(payload, dict) or payload.get("state") != state:
        raise MaintenanceError(f"remote host {label} did not reach its required state")
    if (
        payload.get("production_certification_allowed") is not False
        or payload.get("certified_nodes") != []
        or payload.get("blockers") != [INERT_BLOCKER]
    ):
        raise MaintenanceError(f"remote host {label} breached the inert boundary")


def _admission_args(policy: NodePolicy) -> tuple[str, ...]:
    return (
        "--account=" + policy.account,
        "--qos=" + policy.qos,
        "--partition=" + policy.builder_partition,
        "--cpus-per-task=" + str(policy.cpus),
        "--mem=" + str(policy.memory_mib) + "M",
        "--time=" + policy.wall_time,
    )


def _run_as(user: str, sbatch_args: Sequence[str]) -> tuple[str, ...]:
    return ("/usr/sbin/runuser", "--user", user, "--", "/usr/bin/sbatch", *sbatch_args)


def _smoke_script(policy: NodePolicy, operation_id: str) -> str:
    memory_max = policy.memory_mib * 1024 * 1024
    return (
        "set -eu; d=/var/lib/loom-task-builder/jobs/${SLURM_JOB_ID:?}; "
        "trap '/bin/rm -rf -- \"$d\"' EXIT; "
        'umask 077; /usr/bin/install -d -m 700 "$d"; '
        "p=$(/usr/bin/awk -F: '$1 == 0 {print $3}' /proc/self/cgroup); "
        'case "$p" in */job_${SLURM_JOB_ID}/step_*) ;; *) exit 70;; esac; '
        'case "$p" in *[!A-Za-z0-9_./:-]*) exit 70;; esac; '
        'c=/sys/fs/cgroup$p; j=${c%/step_*}; test "$c" != "$j"; '
        'test -r "$j/cgroup.procs"; test -r "$c/cgroup.procs"; '
        'cpuset=$(/bin/cat "$c/cpuset.cpus.effective"); '
        'case "$cpuset" in ""|*[!0-9,-]*) exit 70;; esac; '
        "cpu_count=$(/usr/bin/awk -v list=\"$cpuset\" 'BEGIN { n=split(list,r,\",\"); "
        "for (i=1;i<=n;i++) { m=split(r[i],b,\"-\"); if (m==1 && b[1] != \"\") "
        "total++; else if (m==2 && b[1] <= b[2]) total+=b[2]-b[1]+1; else exit 1 } "
        "print total }'); "
        f'test "$cpu_count" = "{policy.cpus}"; mem=$(/bin/cat "$c/memory.max"); '
        'swap=$(/bin/cat "$c/memory.swap.max"); '
        f'test "$mem" = "{memory_max}"; test "$swap" = "{policy.swap_bytes}"; '
        'test -e "$c/cgroup.controllers"; : "devices enforced by Slurm"; '
        'q="$d/devices.json"; /usr/sbin/bpftool -j cgroup show "$c" >"$q"; '
        'test -s "$q"; /bin/chmod 600 "$q"; '
        'e="$d/evidence.json"; t="$d/.evidence.json.tmp"; '
        "/usr/bin/printf "
        '\'{"schema":"loom.task-image-builder-maintenance-smoke/v1",'
        f'"operation_id":"{operation_id}",'
        '"job_id":"%s","cgroup_path":"%s",'
        '"controls":{"cpuset_cpus_effective":"%s","cpuset_cpu_count":%s,'
        '"memory_max":%s,"memory_swap_max":%s}}\\n\' '
        '"$SLURM_JOB_ID" "$p" "$cpuset" "$cpu_count" "$mem" "$swap" >"$t"; '
        '/bin/chmod 600 "$t"; /bin/mv -f -- "$t" "$e"; '
        "deadline=$(( $(/bin/date +%s) + 300 )); "
        'until test -f "$d/release"; do test "$(/bin/date +%s)" -lt "$deadline" || exit 75; /usr/bin/sleep 1; done'
    )


def _poll(
    condition: Callable[[], bool],
    *,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    poll_interval: float,
) -> None:
    deadline = monotonic() + DEFAULT_TIMEOUT_SECONDS
    while not condition():
        if monotonic() >= deadline:
            raise MaintenanceError("maintenance condition timed out")
        if poll_interval > 0:
            sleep(poll_interval)


def _squeue_value(runner: CommandRunner, job_id: str, field: str) -> str:
    result = _require(
        runner,
        ("/usr/bin/squeue", "--job", job_id, "--noheader", "--format=" + field),
        "maintenance smoke readback",
    )
    rows = [row for row in result.stdout.splitlines() if row]
    if len(rows) != 1:
        raise MaintenanceError("maintenance smoke readback is ambiguous")
    return rows[0]


def _accounting_command(job_id: str) -> tuple[str, ...]:
    return (
        "/usr/bin/sacct",
        "--noheader",
        "--parsable2",
        "--jobs",
        job_id,
        "--format=JobIDRaw,State,ExitCode",
    )


def _normalized_slurm_state(value: str) -> str:
    fields = value.split(maxsplit=1)
    return fields[0].removesuffix("+") if fields else ""


def _top_level_accounting(result: CommandResult, job_id: str) -> dict[str, str] | None:
    _require_result(result, "maintenance smoke completion readback")
    rows = [row for row in result.stdout.splitlines() if row]
    if not rows:
        return None
    top_level = [row for row in rows if row.split("|", 1)[0] == job_id]
    if len(top_level) != 1:
        raise MaintenanceError("maintenance smoke accounting is ambiguous")
    fields = top_level[0].split("|")
    if len(fields) != 3 or fields[0] != job_id:
        raise MaintenanceError("maintenance smoke accounting is invalid")
    state = _normalized_slurm_state(fields[1])
    if state not in NONTERMINAL_SLURM_STATES and state not in TERMINAL_SLURM_STATES:
        raise MaintenanceError(f"maintenance smoke accounting state {state} is unrecognized")
    return {"job_id": job_id, "state": state, "exit_code": fields[2]}


def _remote_json(result: CommandResult, label: str) -> dict[str, object]:
    if result.returncode != 0:
        raise MaintenanceError(f"remote {label} failed")
    if len(result.stdout.encode("utf-8")) > 64 * 1024:
        raise MaintenanceError(f"remote {label} result exceeds its limit")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise MaintenanceError(f"remote {label} result is invalid") from exc
    if not isinstance(value, dict):
        raise MaintenanceError(f"remote {label} result is invalid")
    if result.stdout != _canonical(value).decode("utf-8") + "\n":
        raise MaintenanceError(f"remote {label} result is not canonical")
    return value


def _emit_internal_json(value: Mapping[str, object]) -> None:
    print(_canonical(value).decode("utf-8"))


def _device_programs(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list) or not value or len(value) > MAX_DEVICE_PROGRAMS:
        raise MaintenanceError("maintenance smoke device program evidence is invalid")
    programs: list[dict[str, object]] = []
    for value_program in value:
        if not isinstance(value_program, dict):
            raise MaintenanceError("maintenance smoke device program evidence is invalid")
        program_id = value_program.get("id")
        attach_type = value_program.get("attach_type")
        attach_flags = value_program.get("attach_flags")
        name = value_program.get("name")
        strings = (attach_type, attach_flags, name)
        if (
            not isinstance(program_id, int)
            or isinstance(program_id, bool)
            or program_id <= 0
            or program_id > 2**32 - 1
            or attach_type != "cgroup_device"
            or not all(
                isinstance(item, str)
                and item
                and len(item.encode("utf-8")) <= MAX_DEVICE_FIELD_BYTES
                and "\n" not in item
                and "\r" not in item
                for item in strings
            )
        ):
            raise MaintenanceError("maintenance smoke device program evidence is invalid")
        programs.append(
            {
                "id": program_id,
                "attach_type": attach_type,
                "attach_flags": attach_flags,
                "name": name,
            }
        )
    return programs


def _cpuset_cpu_count(value: object) -> int:
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise MaintenanceError("maintenance smoke cpuset evidence is invalid")
    ranges: list[tuple[int, int]] = []
    for raw in value.split(","):
        fields = raw.split("-")
        if len(fields) not in {1, 2} or any(re.fullmatch(r"[0-9]+", item) is None for item in fields):
            raise MaintenanceError("maintenance smoke cpuset evidence is invalid")
        start = int(fields[0])
        end = start if len(fields) == 1 else int(fields[1])
        if start > end or end > 2**31 - 1:
            raise MaintenanceError("maintenance smoke cpuset evidence is invalid")
        ranges.append((start, end))
    ranges.sort()
    if any(current[0] <= previous[1] for previous, current in pairwise(ranges)):
        raise MaintenanceError("maintenance smoke cpuset evidence is invalid")
    return sum(end - start + 1 for start, end in ranges)


def _validate_smoke_evidence(
    value: object,
    policy: NodePolicy,
    job_id: str,
    operation_id: str,
) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != {
        "schema",
        "operation_id",
        "job_id",
        "cgroup_path",
        "controls",
    }:
        raise MaintenanceError("maintenance smoke evidence is invalid")
    cgroup_path = value.get("cgroup_path")
    if (
        value.get("schema") != "loom.task-image-builder-maintenance-smoke/v1"
        or value.get("operation_id") != operation_id
        or value.get("job_id") != job_id
        or not isinstance(cgroup_path, str)
        or re.fullmatch(
            rf"/[A-Za-z0-9_./:-]*/job_{re.escape(job_id)}/step_[A-Za-z0-9_.:-]+", cgroup_path
        )
        is None
    ):
        raise MaintenanceError("maintenance smoke evidence is not operation-bound")
    controls = value.get("controls")
    expected_numeric = {
        "memory_max": policy.memory_mib * 1024 * 1024,
        "memory_swap_max": policy.swap_bytes,
    }
    if not isinstance(controls, dict) or set(controls) != {
        "cpuset_cpus_effective",
        "cpuset_cpu_count",
        *expected_numeric,
        "devices",
    }:
        raise MaintenanceError("maintenance smoke control evidence is invalid")
    devices = controls.get("devices")
    if not isinstance(devices, dict) or set(devices) != {"cgroup_path", "programs"}:
        raise MaintenanceError("maintenance smoke control evidence does not match policy")
    try:
        programs = _device_programs(devices.get("programs"))
        observed_cpu_count = _cpuset_cpu_count(controls.get("cpuset_cpus_effective"))
    except MaintenanceError as exc:
        raise MaintenanceError("maintenance smoke control evidence does not match policy") from exc
    if (
        controls.get("cpuset_cpu_count") != observed_cpu_count
        or observed_cpu_count != policy.cpus
        or any(controls.get(key) != expected for key, expected in expected_numeric.items())
        or devices.get("cgroup_path") != cgroup_path
    ):
        raise MaintenanceError("maintenance smoke control evidence does not match policy")
    normalized_controls = dict(controls)
    normalized_controls["devices"] = {"cgroup_path": cgroup_path, "programs": programs}
    return {
        "schema": value["schema"],
        "operation_id": operation_id,
        "job_id": job_id,
        "cgroup_path": cgroup_path,
        "controls": normalized_controls,
    }


def _document(
    policy: NodePolicy,
    policy_payload: bytes,
    candidate_root: Path,
    operation_id: str,
    slurm_node: str,
    pre_state: Mapping[str, str],
) -> dict[str, object]:
    try:
        binding = authority.load_authority_binding(candidate_root)
    except authority.AuthorityError as exc:
        raise MaintenanceError("candidate authority component binding is invalid") from exc
    return {
        "schema": RECEIPT_SCHEMA,
        "operation_id": operation_id,
        "cluster_id": policy.cluster_id,
        "slurm_node": slurm_node,
        "candidate_digest": _sha(
            _canonical(
                {
                    "policy": _sha(policy_payload),
                    **binding.as_dict(),
                }
            )
        ),
        "policy_digest": _sha(policy_payload),
        **binding.as_dict(),
        **_inert(),
        "pre_state": dict(pre_state),
        "observations": {
            "daemon": None,
            "admission": None,
            "reservation": None,
            "smoke": None,
            "emergency_containment": None,
        },
        "terminal_state": "applying",
        "failure": None,
        "events": [],
    }


def _finalize(
    document: dict[str, object],
    transition: Transition,
    receipt_path: Path,
    owner: int,
    **data: object,
) -> None:
    document["terminal_state"] = transition.value
    _event(document, transition, data)
    _write_document(receipt_path, document, exclusive=False, owner=owner)


def _observations(document: Mapping[str, object]) -> dict[str, object]:
    value = document.get("observations")
    if not isinstance(value, dict):
        raise MaintenanceError("maintenance observations are invalid")
    return dict(value)


def maintain_node(
    action: str,
    cluster_id: str,
    slurm_node: str,
    candidate_root: Path,
    bundle: Path,
    receipt_root: Path,
    runner: CommandRunner,
    *,
    ssh_config: Path | None = None,
    operation_id: str | None = None,
    effective_uid: int | None = None,
    required_owner: int = 0,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    poll_interval: float = 1.0,
) -> dict[str, object]:
    if action not in {"plan", "check", "apply"}:
        raise MaintenanceError("maintenance action is invalid")
    policy, policy_payload = _load_policy(candidate_root, cluster_id, slurm_node)
    owner = os.geteuid() if effective_uid is None else effective_uid
    selected_operation = operation_id or str(uuid.uuid4())
    try:
        parsed = uuid.UUID(selected_operation)
    except ValueError as exc:
        raise MaintenanceError("operation ID is invalid") from exc
    if parsed.version != 4 or str(parsed) != selected_operation:
        raise MaintenanceError("operation ID is invalid")
    loom_reason = f"loom-task-builder-phase1/host-release-v1/{selected_operation}"
    if ssh_config is not None and (not ssh_config.is_absolute() or ssh_config.is_symlink()):
        raise MaintenanceError("SSH config is unsafe")
    if cluster_id == "gb10" and ssh_config is None:
        ssh_config = candidate_root / "deploy/worker-pools/gb10/ssh_config"
    if ssh_config is not None and not ssh_config.is_file():
        raise MaintenanceError("SSH config is unavailable")

    initial = _snapshot(runner, slurm_node)

    def remote_host(host_action: str, expected: str) -> None:
        _require_host_state(
            _remote(
                runner,
                slurm_node,
                _host_command(
                    candidate_root,
                    host_action,
                    policy,
                    slurm_node,
                    bundle,
                    receipt_root,
                    selected_operation,
                ),
                ssh_config=ssh_config,
                candidate_root=candidate_root,
            ),
            expected,
            host_action,
        )

    if action == "plan":
        remote_host("plan", "planned")
        return {**_inert(), "state": "planned", "pre_state": initial}
    if action == "check":
        remote_host("check", "host_prepared")
        return {**_inert(), "state": "checked", "pre_state": initial}
    if owner != required_owner:
        raise MaintenanceError("maintenance apply requires root")
    _validate_receipt_root(receipt_root, required_owner)
    receipt_path = receipt_root / f"{selected_operation}.json"
    document = _document(
        policy, policy_payload, candidate_root, selected_operation, slurm_node, initial
    )
    _event(document, Transition.PRE_STATE_RECORDED, {"pre_state": initial})
    _write_document(receipt_path, document, exclusive=True, owner=required_owner)
    foreign_drain = "DRAIN" in initial["state"] and initial["reason"] != loom_reason
    if foreign_drain:
        document["failure"] = "foreign drain"
        _finalize(
            document,
            Transition.BLOCKED,
            receipt_path,
            required_owner,
            error="foreign drain",
        )
        return {**_inert(), "state": "blocked", "receipt": str(receipt_path)}
    host_apply_started = False
    smoke_submitted = False
    smoke_job_id: str | None = None
    smoke_release_facts: dict[str, object] | None = None
    reservation_name = _reservation_name(selected_operation)
    reservation_created = False
    reservation_cleanup_pending = False

    def persist(transition: Transition, **data: object) -> None:
        _event(document, transition, data)
        _write_document(receipt_path, document, exclusive=False, owner=required_owner)

    def leave_blocked(error: MaintenanceError) -> dict[str, object]:
        document["failure"] = str(error)
        try:
            _finalize(document, Transition.BLOCKED, receipt_path, required_owner, error=str(error))
        except (MaintenanceError, OSError):
            pass
        return {**_inert(), "state": "blocked", "receipt": str(receipt_path)}

    def create_reservation() -> None:
        nonlocal reservation_cleanup_pending, reservation_created
        readback_command = _reservation_readback_command()
        prior_readback = _require(
            runner,
            readback_command,
            "maintenance smoke reservation prior readback",
        )
        prior_absence = _reservation_absent(prior_readback, reservation_name)
        create_command = _reservation_command("create", reservation_name, slurm_node)
        create_result = _require(runner, create_command, "maintenance smoke reservation")
        reservation_created = True
        reservation_cleanup_pending = True
        reservation_facts: dict[str, object] = {
            "name": reservation_name,
            "prior_readback": _command_observation(readback_command, prior_readback),
            "prior_absence": prior_absence,
            "create": _command_observation(create_command, create_result),
            "create_readback": None,
            "binding": None,
            "delete": None,
            "delete_readback": None,
            "absence": None,
        }
        document["observations"] = {
            **_observations(document),
            "reservation": reservation_facts,
        }
        create_readback = _require(
            runner,
            readback_command,
            "maintenance smoke reservation create readback",
        )
        reservation_facts["create_readback"] = _command_observation(
            readback_command, create_readback
        )
        reservation_facts["binding"] = _reservation_binding(
            create_readback, reservation_name, slurm_node
        )
        document["observations"] = {
            **_observations(document),
            "reservation": reservation_facts,
        }
        persist(
            Transition.RESERVATION_CREATED,
            name=reservation_name,
            create=reservation_facts["create"],
            create_readback=reservation_facts["create_readback"],
            binding=reservation_facts["binding"],
        )

    def delete_reservation() -> None:
        nonlocal reservation_cleanup_pending, reservation_created
        if not reservation_cleanup_pending:
            return
        observations = _observations(document)
        current_facts = observations.get("reservation")
        reservation_facts = (
            dict(current_facts) if isinstance(current_facts, dict) else {"name": reservation_name}
        )
        if reservation_created:
            delete_command = _reservation_command("delete", reservation_name, slurm_node)
            delete_result = _require(
                runner,
                delete_command,
                "maintenance smoke reservation cleanup",
            )
            reservation_created = False
            reservation_facts["delete"] = _command_observation(delete_command, delete_result)
            document["observations"] = {**observations, "reservation": reservation_facts}
        if not isinstance(reservation_facts.get("delete"), dict):
            raise MaintenanceError("maintenance smoke reservation delete evidence is unavailable")
        readback_command = _reservation_readback_command()
        delete_readback = _require(
            runner,
            readback_command,
            "maintenance smoke reservation delete readback",
        )
        reservation_facts["delete_readback"] = _command_observation(
            readback_command, delete_readback
        )
        reservation_facts["absence"] = _reservation_absent(delete_readback, reservation_name)
        document["observations"] = {**observations, "reservation": reservation_facts}
        persist(
            Transition.RESERVATION_DELETED,
            name=reservation_name,
            delete=reservation_facts["delete"],
            delete_readback=reservation_facts["delete_readback"],
            absence=reservation_facts["absence"],
        )
        reservation_cleanup_pending = False

    def contain_submitted_smoke(job_id: str) -> None:
        nonlocal smoke_release_facts
        release_command = _smoke_command(candidate_root, "release", job_id, selected_operation)
        if smoke_release_facts is None:
            try:
                release_result = _remote(
                    runner,
                    slurm_node,
                    release_command,
                    ssh_config=ssh_config,
                    candidate_root=candidate_root,
                )
            except (MaintenanceError, OSError) as release_error:
                smoke_release_facts = {
                    "command": list(release_command),
                    "error": (
                        str(release_error)
                        if isinstance(release_error, MaintenanceError)
                        else "maintenance I/O failed"
                    ),
                    "outcome": "transport_unavailable",
                }
            else:
                smoke_release_facts = _command_observation(release_command, release_result)
                try:
                    released = _remote_json(
                        release_result,
                        "emergency maintenance smoke release",
                    )
                except MaintenanceError as release_error:
                    smoke_release_facts["error"] = str(release_error)
                    smoke_release_facts["outcome"] = "failed"
                else:
                    if released == {"state": "released"}:
                        smoke_release_facts["outcome"] = "released"
                    else:
                        smoke_release_facts["error"] = (
                            "emergency maintenance smoke release is invalid"
                        )
                        smoke_release_facts["outcome"] = "failed"

        current = _snapshot(runner, slurm_node)
        drain_operation: dict[str, object] | None = None
        if "DRAIN" in current["state"]:
            if current["reason"] != loom_reason:
                raise MaintenanceError("Loom drain ownership was lost before rollback")
        else:
            drain_command = (
                "/usr/bin/scontrol",
                "update",
                f"NodeName={slurm_node}",
                "State=DRAIN",
                f"Reason={loom_reason}",
            )
            drain_result = _require(runner, drain_command, "rollback Slurm drain")
            drain_operation = _command_observation(drain_command, drain_result)
        owned_drain = _owned_drain_snapshot(runner, slurm_node, loom_reason)

        accounting_command = _accounting_command(job_id)
        accounting_facts: dict[str, object] | None = None

        def smoke_terminal() -> bool:
            nonlocal accounting_facts
            accounting_result = _require(
                runner,
                accounting_command,
                "maintenance smoke completion readback",
            )
            top_level = _top_level_accounting(accounting_result, job_id)
            if top_level is None:
                return False
            if top_level["state"] in NONTERMINAL_SLURM_STATES:
                return False
            accounting_facts = {
                "readback": _command_observation(accounting_command, accounting_result),
                "top_level": top_level,
            }
            return True

        _poll(smoke_terminal, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        if accounting_facts is None:
            raise MaintenanceError("maintenance smoke terminal accounting is unavailable")

        cleanup_command = _smoke_command(candidate_root, "cleanup", job_id, selected_operation)
        cleanup_facts: dict[str, object] | None = None

        def smoke_absent() -> bool:
            nonlocal cleanup_facts
            cleanup_result = _remote(
                runner,
                slurm_node,
                cleanup_command,
                ssh_config=ssh_config,
                candidate_root=candidate_root,
            )
            cleanup = _remote_json(cleanup_result, "maintenance smoke cleanup")
            if cleanup == {"state": "present"}:
                return False
            if cleanup != CLEANUP_ABSENCE:
                raise MaintenanceError("maintenance smoke cleanup is invalid")
            cleanup_facts = {
                **_command_observation(cleanup_command, cleanup_result),
                **{key: cleanup[key] for key in CLEANUP_ABSENCE if key != "state"},
            }
            return True

        _poll(smoke_absent, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        if cleanup_facts is None:
            raise MaintenanceError("maintenance smoke cleanup is unavailable")

        active_command = (
            "/usr/bin/squeue",
            "--nodelist",
            slurm_node,
            "--states",
            "RUNNING,COMPLETING",
            "--noheader",
            "--format=%i",
        )
        idle_facts: dict[str, object] | None = None

        def rollback_idle() -> bool:
            nonlocal idle_facts
            active_result = _require(runner, active_command, "Slurm active-job readback")
            snapshot = _owned_drain_snapshot(runner, slurm_node, loom_reason)
            active_jobs = active_result.stdout.split()
            zero_tres = _is_zero_tres(snapshot["allocated_tres"])
            if active_jobs or not zero_tres:
                return False
            idle_facts = {
                "active_job_readback": _command_observation(active_command, active_result),
                "node_readback": snapshot,
                "zero_active_jobs": True,
                "zero_allocated_tres": True,
            }
            return True

        _poll(rollback_idle, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        if idle_facts is None:
            raise MaintenanceError("rollback idle readback is unavailable")
        containment = {
            "job_id": job_id,
            "release": smoke_release_facts,
            "accounting": accounting_facts,
            "cleanup": cleanup_facts,
            "drain_operation": drain_operation,
            "owned_drain": owned_drain,
            "idle": idle_facts,
        }
        document["observations"] = {
            **_observations(document),
            "emergency_containment": containment,
        }
        persist(Transition.EMERGENCY_CONTAINED, **containment)

    try:
        if "DRAIN" not in initial["state"] or initial["reason"] != loom_reason:
            _require(
                runner,
                (
                    "/usr/bin/scontrol",
                    "update",
                    f"NodeName={slurm_node}",
                    "State=DRAIN",
                    f"Reason={loom_reason}",
                ),
                "Slurm drain",
            )
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        persist(Transition.DRAINED, reason=loom_reason)

        def idle() -> bool:
            jobs = _require(
                runner,
                (
                    "/usr/bin/squeue",
                    "--nodelist",
                    slurm_node,
                    "--states",
                    "RUNNING,COMPLETING",
                    "--noheader",
                    "--format=%i",
                ),
                "Slurm active-job readback",
            )
            owned = _owned_drain_snapshot(runner, slurm_node, loom_reason)
            return not jobs.stdout.strip() and _is_zero_tres(owned["allocated_tres"])

        _poll(idle, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        persist(Transition.IDLE)
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        remote_host("plan", "planned")
        persist(Transition.HOST_PREFLIGHTED)
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        host_apply_started = True
        remote_host("apply", "host_prepared")
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        persist(Transition.HOST_APPLIED)
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        restart_facts = _require_daemon_state(
            _remote(
                runner,
                slurm_node,
                _daemon_command(candidate_root, "restart"),
                ssh_config=ssh_config,
                candidate_root=candidate_root,
            ),
            "restart",
        )
        persist(Transition.DAEMON_RESTARTED)
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        check_facts = _require_daemon_state(
            _remote(
                runner,
                slurm_node,
                _daemon_command(candidate_root, "check"),
                ssh_config=ssh_config,
                candidate_root=candidate_root,
            ),
            "check",
        )
        document["observations"] = {
            **_observations(document),
            "daemon": {"restart": restart_facts, "check": check_facts},
        }
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        remote_host("check", "host_prepared")
        persist(Transition.READBACK_VERIFIED)
        test_payload = "--wrap=/usr/bin/true"
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        builder_admission_command = _run_as(
            "loom-builder", ("--test-only", *_admission_args(policy), test_payload)
        )
        builder_admission = _require(
            runner,
            builder_admission_command,
            "builder admission",
        )
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        rollout_admission_command = _run_as(
            "loom-rollout", ("--test-only", *_admission_args(policy), test_payload)
        )
        denied = runner.run(rollout_admission_command)
        if denied.returncode == 0:
            raise MaintenanceError("legacy rollout admission was accepted")
        document["observations"] = {
            **_observations(document),
            "admission": {
                "builder": _command_observation(builder_admission_command, builder_admission),
                "rollout_rejected": _command_observation(rollout_admission_command, denied),
            },
        }
        persist(Transition.ADMISSION_VERIFIED)
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        create_reservation()
        smoke_result = _require(
            runner,
            _run_as(
                "loom-builder",
                (
                    "--parsable",
                    *_admission_args(policy),
                    f"--nodelist={slurm_node}",
                    f"--reservation={reservation_name}",
                    "--wrap=" + _smoke_script(policy, selected_operation),
                ),
            ),
            "maintenance smoke submission",
        )
        smoke_submitted = True
        job_id = smoke_result.stdout.strip()
        if not re.fullmatch(r"[1-9][0-9]*", job_id):
            raise MaintenanceError("maintenance smoke job ID is invalid")
        smoke_job_id = job_id
        document["observations"] = {
            **_observations(document),
            "smoke": {
                "job_id": job_id,
                "allocation": None,
                "cgroup": None,
                "cgroup_path": None,
                "cleanup": None,
            },
        }
        persist(Transition.SMOKE_QUEUED, job_id=job_id)

        def smoke_pending() -> bool:
            state = _squeue_value(runner, job_id, "%T")
            if state in TERMINAL_SLURM_STATES:
                raise MaintenanceError(f"maintenance smoke entered terminal state {state}")
            if state not in NONTERMINAL_SLURM_STATES:
                raise MaintenanceError(f"maintenance smoke state {state} is unrecognized")
            return (
                state == "PENDING"
                and _squeue_value(runner, job_id, "%R")
                == f"ReqNodeNotAvail, UnavailableNodes:{slurm_node}"
            )

        _poll(smoke_pending, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        persist(Transition.SMOKE_PENDING, job_id=job_id)
        _owned_drain_snapshot(runner, slurm_node, loom_reason)
        _require(
            runner,
            ("/usr/bin/scontrol", "update", f"NodeName={slurm_node}", "State=RESUME"),
            "owned Slurm resume",
        )

        def smoke_running() -> bool:
            state = _squeue_value(runner, job_id, "%T")
            if state != "RUNNING":
                if state in TERMINAL_SLURM_STATES:
                    raise MaintenanceError(f"maintenance smoke entered terminal state {state}")
                if state in NONTERMINAL_SLURM_STATES:
                    return False
                raise MaintenanceError(f"maintenance smoke state {state} is unrecognized")
            if _squeue_value(runner, job_id, "%N") != slurm_node:
                raise MaintenanceError("maintenance smoke is not allocated to the target node")
            jobs = _require(
                runner,
                (
                    "/usr/bin/squeue",
                    "--nodelist",
                    slurm_node,
                    "--states",
                    "RUNNING,COMPLETING",
                    "--noheader",
                    "--format=%i",
                ),
                "maintenance smoke allocation readback",
            )
            if jobs.stdout.split() != [job_id]:
                raise MaintenanceError("maintenance smoke was not the sole first allocation")
            return True

        _poll(smoke_running, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        smoke_observation = _observations(document)
        smoke_facts = smoke_observation.get("smoke")
        if not isinstance(smoke_facts, dict):
            raise MaintenanceError("maintenance smoke observations are invalid")
        smoke_facts = dict(smoke_facts)
        smoke_facts["allocation"] = {"node": slurm_node, "sole_first_allocation": True}
        smoke_observation["smoke"] = smoke_facts
        document["observations"] = smoke_observation
        persist(Transition.SMOKE_RUNNING, job_id=job_id)

        observed_evidence: dict[str, object] | None = None

        def smoke_observed() -> bool:
            nonlocal observed_evidence
            observed = _remote_json(
                _remote(
                    runner,
                    slurm_node,
                    _smoke_command(candidate_root, "observe", job_id, selected_operation),
                    ssh_config=ssh_config,
                    candidate_root=candidate_root,
                ),
                "maintenance smoke evidence observation",
            )
            if observed == {"state": "pending"}:
                return False
            if set(observed) != {"state", "evidence"} or observed.get("state") != "observed":
                raise MaintenanceError("maintenance smoke evidence observation is invalid")
            observed_evidence = _validate_smoke_evidence(
                observed.get("evidence"), policy, job_id, selected_operation
            )
            return True

        _poll(smoke_observed, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        if observed_evidence is None:
            raise MaintenanceError("maintenance smoke evidence observation is unavailable")
        smoke_observation = _observations(document)
        smoke_facts = smoke_observation.get("smoke")
        if not isinstance(smoke_facts, dict):
            raise MaintenanceError("maintenance smoke observations are invalid")
        smoke_facts = dict(smoke_facts)
        smoke_facts["cgroup"] = observed_evidence["controls"]
        smoke_facts["cgroup_path"] = observed_evidence["cgroup_path"]
        smoke_observation["smoke"] = smoke_facts
        document["observations"] = smoke_observation
        persist(Transition.SMOKE_OBSERVED, job_id=job_id, evidence=observed_evidence)

        release_command = _smoke_command(candidate_root, "release", job_id, selected_operation)
        release_result = _remote(
            runner,
            slurm_node,
            release_command,
            ssh_config=ssh_config,
            candidate_root=candidate_root,
        )
        released = _remote_json(release_result, "maintenance smoke release")
        if released != {"state": "released"}:
            raise MaintenanceError("maintenance smoke release is invalid")
        smoke_release_facts = {
            **_command_observation(release_command, release_result),
            "outcome": "released",
        }
        persist(
            Transition.SMOKE_RELEASED,
            job_id=job_id,
            release=smoke_release_facts,
        )

        accounting_command = _accounting_command(job_id)
        accounting_facts: dict[str, object] | None = None

        def smoke_completed() -> bool:
            nonlocal accounting_facts
            current = _require(
                runner,
                accounting_command,
                "maintenance smoke completion readback",
            )
            top_level = _top_level_accounting(current, job_id)
            if top_level is None:
                return False
            if top_level == {"job_id": job_id, "state": "COMPLETED", "exit_code": "0:0"}:
                accounting_facts = {
                    "readback": _command_observation(accounting_command, current),
                    "top_level": top_level,
                }
                return True
            if top_level["state"] in NONTERMINAL_SLURM_STATES:
                return False
            raise MaintenanceError("maintenance smoke did not complete successfully")

        _poll(smoke_completed, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        if accounting_facts is None:
            raise MaintenanceError("maintenance smoke accounting is unavailable")
        persist(
            Transition.SMOKE_COMPLETED,
            job_id=job_id,
            accounting=accounting_facts,
        )

        cleanup_command = _smoke_command(candidate_root, "cleanup", job_id, selected_operation)
        cleanup_facts: dict[str, object] | None = None

        def smoke_cleaned() -> bool:
            nonlocal cleanup_facts
            cleanup_result = _remote(
                runner,
                slurm_node,
                cleanup_command,
                ssh_config=ssh_config,
                candidate_root=candidate_root,
            )
            cleanup = _remote_json(cleanup_result, "maintenance smoke cleanup")
            if cleanup == {"state": "present"}:
                return False
            if cleanup != CLEANUP_ABSENCE:
                raise MaintenanceError("maintenance smoke cleanup is invalid")
            cleanup_facts = {
                **_command_observation(cleanup_command, cleanup_result),
                **{key: cleanup[key] for key in CLEANUP_ABSENCE if key != "state"},
            }
            return True

        _poll(smoke_cleaned, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
        if cleanup_facts is None:
            raise MaintenanceError("maintenance smoke cleanup is unavailable")
        smoke_observation = _observations(document)
        smoke_facts = smoke_observation.get("smoke")
        if not isinstance(smoke_facts, dict):
            raise MaintenanceError("maintenance smoke observations are invalid")
        smoke_facts = dict(smoke_facts)
        smoke_facts["cleanup"] = {
            key: CLEANUP_ABSENCE[key] for key in CLEANUP_ABSENCE if key != "state"
        }
        smoke_observation["smoke"] = smoke_facts
        document["observations"] = smoke_observation
        persist(
            Transition.SMOKE_CLEANED,
            job_id=job_id,
            cleanup=cleanup_facts,
        )
        delete_reservation()
        remote_host("check", "host_prepared")
        _require(
            runner,
            _run_as(
                "loom-rollout",
                ("--test-only", "--partition=" + policy.trial_partition, test_payload),
            ),
            "ordinary trial admission",
        )
        _finalize(document, Transition.PREPARED, receipt_path, required_owner, job_id=job_id)
        return {**_inert(), "state": "prepared", "receipt": str(receipt_path)}
    except (MaintenanceError, OSError) as exc:
        error = (
            exc if isinstance(exc, MaintenanceError) else MaintenanceError("maintenance I/O failed")
        )
        if not host_apply_started:
            return leave_blocked(error)
        document["failure"] = str(error)
        try:
            if smoke_submitted and smoke_job_id is None:
                return leave_blocked(
                    MaintenanceError(
                        f"{error}; containment: submitted smoke job ID is unverifiable"
                    )
                )
            if smoke_job_id is not None:
                try:
                    contain_submitted_smoke(smoke_job_id)
                except (MaintenanceError, OSError) as containment_error:
                    detail = (
                        str(containment_error)
                        if isinstance(containment_error, MaintenanceError)
                        else "maintenance I/O failed"
                    )
                    return leave_blocked(MaintenanceError(f"{error}; containment: {detail}"))
            else:
                current = _snapshot(runner, slurm_node)
                if current["reason"] != loom_reason:
                    if "DRAIN" in current["state"]:
                        return leave_blocked(
                            MaintenanceError("Loom drain ownership was lost before rollback")
                        )
                    _require(
                        runner,
                        (
                            "/usr/bin/scontrol",
                            "update",
                            f"NodeName={slurm_node}",
                            "State=DRAIN",
                            f"Reason={loom_reason}",
                        ),
                        "rollback Slurm drain",
                    )
                    _owned_drain_snapshot(runner, slurm_node, loom_reason)
                _poll(idle, monotonic=monotonic, sleep=sleep, poll_interval=poll_interval)
            try:
                delete_reservation()
            except (MaintenanceError, OSError) as reservation_error:
                detail = (
                    str(reservation_error)
                    if isinstance(reservation_error, MaintenanceError)
                    else "maintenance I/O failed"
                )
                return leave_blocked(MaintenanceError(f"{error}; reservation cleanup: {detail}"))
            remote_host("rollback", "rolled_back")
            _require_daemon_state(
                _remote(
                    runner,
                    slurm_node,
                    _daemon_command(candidate_root, "restart"),
                    ssh_config=ssh_config,
                    candidate_root=candidate_root,
                ),
                "restart",
            )
            _require_daemon_state(
                _remote(
                    runner,
                    slurm_node,
                    _daemon_command(candidate_root, "check"),
                    ssh_config=ssh_config,
                    candidate_root=candidate_root,
                ),
                "check",
            )
            _owned_drain_snapshot(runner, slurm_node, loom_reason)
            _finalize(
                document, Transition.ROLLED_BACK, receipt_path, required_owner, error=str(error)
            )
            return {**_inert(), "state": "rolled_back", "receipt": str(receipt_path)}
        except (MaintenanceError, OSError) as rollback_error:
            document["failure"] = f"{error}; rollback: {rollback_error}"
            try:
                _finalize(
                    document,
                    Transition.DRAINED_ROLLBACK_FAILED,
                    receipt_path,
                    required_owner,
                    error=str(error),
                    rollback_error=str(rollback_error),
                )
                digest = _sha(_read_regular(receipt_path, "maintenance receipt"))
                _owned_drain_snapshot(runner, slurm_node, loom_reason)
                failed_reason = f"{loom_reason}/rollback-failed/{digest}"
                _require(
                    runner,
                    (
                        "/usr/bin/scontrol",
                        "update",
                        f"NodeName={slurm_node}",
                        "State=DRAIN",
                        f"Reason={failed_reason}",
                    ),
                    "rollback-failed Slurm drain",
                )
                final_snapshot = _snapshot(runner, slurm_node)
                if (
                    "DRAIN" not in final_snapshot["state"]
                    or final_snapshot["reason"] != failed_reason
                ):
                    raise MaintenanceError("rollback-failed Slurm drain readback is invalid")
            except (MaintenanceError, OSError) as drain_error:
                document["failure"] = f"{error}; rollback: {rollback_error}; drain: {drain_error}"
                try:
                    _finalize(
                        document,
                        Transition.BLOCKED,
                        receipt_path,
                        required_owner,
                        error=document["failure"],
                    )
                except (MaintenanceError, OSError):
                    pass
                return {**_inert(), "state": "blocked", "receipt": str(receipt_path)}
            return {**_inert(), "state": "drained_rollback_failed", "receipt": str(receipt_path)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "check", "apply"))
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--slurm-node", required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--receipt-root", type=Path, required=True)
    parser.add_argument("--ssh-config", type=Path)
    parser.add_argument("--operation-id")
    return parser


def _internal_node_daemon(action: str) -> int:
    if action not in {"restart", "check"}:
        return 2
    command = (
        "/usr/bin/systemctl",
        "restart" if action == "restart" else "is-active",
        "slurmd",
    )
    try:
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=30,
            env={"LC_ALL": "C", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
        )
    except (OSError, subprocess.TimeoutExpired):
        return 1
    if result.returncode != 0:
        return 1
    try:
        config = _read_regular(CGROUP_CONFIG, "active Slurm cgroup configuration")
        contents = config.decode("utf-8")
    except (MaintenanceError, UnicodeDecodeError):
        return 1
    _emit_internal_json(
        {
            "state": "active",
            "cgroup_config": {
                "path": str(CGROUP_CONFIG),
                "sha256": _sha(config),
                "contents": contents,
            },
        }
    )
    return 0


def _smoke_processes_absent(proc_root: Path, job_id: str) -> bool:
    component = f"job_{job_id}"
    try:
        entries = list(os.scandir(proc_root))
    except OSError as exc:
        raise MaintenanceError("maintenance smoke process readback is unavailable") from exc
    if len(entries) > 131_072:
        raise MaintenanceError("maintenance smoke process readback is too large")
    for entry in entries:
        if not entry.name.isdecimal():
            continue
        try:
            if not entry.is_dir(follow_symlinks=False):
                continue
            descriptor = os.open(
                Path(entry.path) / "cgroup",
                os.O_RDONLY | os.O_NOFOLLOW,
            )
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise MaintenanceError("maintenance smoke process readback is unavailable") from exc
        try:
            payload = os.read(descriptor, 65_537)
        except OSError as exc:
            raise MaintenanceError("maintenance smoke process readback is unavailable") from exc
        finally:
            os.close(descriptor)
        if len(payload) > 65_536:
            raise MaintenanceError("maintenance smoke process readback is too large")
        try:
            lines = payload.decode("utf-8").splitlines()
        except UnicodeDecodeError as exc:
            raise MaintenanceError("maintenance smoke process readback is invalid") from exc
        for line in lines:
            fields = line.split(":", 2)
            if len(fields) != 3:
                raise MaintenanceError("maintenance smoke process readback is invalid")
            if component in Path(fields[2]).parts:
                return False
    return True


def _mountinfo_unescape(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        return chr(int(match.group(1), 8))

    return re.sub(r"\\([0-7]{3})", replace, value)


def _smoke_mounts_absent(mountinfo_path: Path, job_root: Path, job_id: str) -> bool:
    try:
        payload = _read_regular(mountinfo_path, "maintenance smoke mount readback")
        lines = payload.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise MaintenanceError("maintenance smoke mount readback is invalid") from exc
    if len(lines) > 65_536:
        raise MaintenanceError("maintenance smoke mount readback is too large")
    component = f"job_{job_id}"
    for line in lines:
        fields = line.split()
        if "-" not in fields or len(fields) < 10:
            raise MaintenanceError("maintenance smoke mount readback is invalid")
        separator = fields.index("-")
        if separator < 6 or len(fields) < separator + 4:
            raise MaintenanceError("maintenance smoke mount readback is invalid")
        for raw_path in (fields[3], fields[4]):
            observed = Path(_mountinfo_unescape(raw_path))
            if component in observed.parts or observed == job_root or job_root in observed.parents:
                return False
    return True


def _internal_smoke(
    action: str,
    job_id: str,
    operation_id: str,
    *,
    jobs_root: Path = BUILDER_JOBS_ROOT,
    smoke_owner: int = 993,
    proc_root: Path = Path("/proc"),
    mountinfo_path: Path = Path("/proc/self/mountinfo"),
) -> int:
    if (
        action not in {"observe", "release", "cleanup"}
        or re.fullmatch(r"[1-9][0-9]*", job_id) is None
        or not _valid_operation_id(operation_id)
    ):
        return 2
    job_root = jobs_root / job_id
    metadata: os.stat_result | None
    try:
        metadata = job_root.lstat()
    except FileNotFoundError:
        metadata = None
        if action != "cleanup":
            _emit_internal_json({"state": "pending"})
            return 0
    except OSError:
        return 1
    if metadata is not None and (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != smoke_owner
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        return 1
    if action == "cleanup":
        if metadata is not None:
            _emit_internal_json({"state": "present"})
            return 0
        try:
            processes_absent = _smoke_processes_absent(proc_root, job_id)
            mounts_absent = _smoke_mounts_absent(mountinfo_path, job_root, job_id)
        except MaintenanceError:
            return 1
        if not processes_absent or not mounts_absent:
            _emit_internal_json({"state": "present"})
            return 0
        _emit_internal_json(
            {
                "state": "absent",
                "processes_absent": True,
                "mounts_absent": True,
                "job_directory_absent": True,
            }
        )
        return 0
    if metadata is None:
        return 1
    evidence_path = job_root / "evidence.json"
    try:
        evidence_payload = _read_owner_regular(
            evidence_path,
            "maintenance smoke evidence",
            owner=smoke_owner,
            mode=0o600,
        )
        evidence = json.loads(evidence_payload)
    except MaintenanceError:
        try:
            evidence_path.lstat()
        except FileNotFoundError:
            if action == "observe":
                _emit_internal_json({"state": "pending"})
                return 0
        except OSError:
            pass
        return 1
    except json.JSONDecodeError:
        return 1
    if (
        not isinstance(evidence, dict)
        or evidence.get("schema") != "loom.task-image-builder-maintenance-smoke/v1"
        or evidence.get("job_id") != job_id
        or evidence.get("operation_id") != operation_id
    ):
        return 1
    if action == "observe":
        controls = evidence.get("controls")
        if not isinstance(controls, dict) or "devices" in controls:
            return 1
        try:
            devices_payload = _read_owner_regular(
                job_root / "devices.json",
                "maintenance smoke device programs",
                owner=smoke_owner,
                mode=0o600,
            )
            programs = _device_programs(json.loads(devices_payload))
        except (MaintenanceError, json.JSONDecodeError):
            return 1
        enriched = {
            **evidence,
            "controls": {
                **controls,
                "devices": {
                    "cgroup_path": evidence.get("cgroup_path"),
                    "programs": programs,
                },
            },
        }
        _emit_internal_json({"state": "observed", "evidence": enriched})
        return 0
    release = job_root / "release"
    try:
        descriptor = os.open(
            release,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
    except FileExistsError:
        return 1
    except OSError:
        return 1
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        _fsync_directory(job_root)
    except OSError:
        return 1
    _emit_internal_json({"state": "released"})
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    selected = list(sys.argv[1:] if argv is None else argv)
    if len(selected) == 2 and selected[0] == "--internal-node-daemon":
        return _internal_node_daemon(selected[1])
    if len(selected) == 4 and selected[0] == "--internal-smoke":
        return _internal_smoke(selected[1], selected[2], selected[3])
    arguments = _parser().parse_args(selected)
    try:
        result = maintain_node(
            arguments.action,
            arguments.cluster_id,
            arguments.slurm_node,
            arguments.candidate_root,
            arguments.bundle,
            arguments.receipt_root,
            SubprocessCommandRunner(),
            ssh_config=arguments.ssh_config,
            operation_id=arguments.operation_id,
        )
    except (MaintenanceError, OSError) as exc:
        print(json.dumps({**_inert(), "state": "blocked", "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
