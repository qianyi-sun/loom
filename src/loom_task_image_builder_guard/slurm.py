"""Pinned-command Slurm readback for one task-image builder allocation."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import (
    CommandConfig,
    CommandIdentity,
    IdentityConfig,
    SlurmConfig,
)

_JOB_ID = re.compile(r"^[1-9][0-9]{0,31}$")
_MEMORY = re.compile(r"^([1-9][0-9]*)([KMGT])$")
_MAX_EXECUTABLE_BYTES = 128 * 1024 * 1024
_ENVIRONMENT = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class CommandRunner(Protocol):
    def run(self, command: CommandIdentity, argv: tuple[str, ...]) -> CommandResult: ...


def _hash_descriptor(descriptor: int, maximum: int = _MAX_EXECUTABLE_BYTES) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset <= maximum:
        chunk = os.pread(descriptor, min(1024 * 1024, maximum + 1 - offset), offset)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)
        offset += len(chunk)
    raise GuardError("command_identity_invalid")


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


class PinnedCommandRunner:
    """Execute a verified opened command path without a shell or ambient env."""

    def __init__(
        self,
        *,
        trusted_uid: int = 0,
        timeout_seconds: int = 30,
        max_stdout_bytes: int = 1024 * 1024,
        max_stderr_bytes: int = 256 * 1024,
    ) -> None:
        self.trusted_uid = trusted_uid
        self.timeout_seconds = timeout_seconds
        self.max_stdout_bytes = max_stdout_bytes
        self.max_stderr_bytes = max_stderr_bytes

    def run(self, command: CommandIdentity, argv: tuple[str, ...]) -> CommandResult:
        if any(not isinstance(item, str) or "\x00" in item for item in argv):
            raise GuardError("command_arguments_invalid")
        descriptor: int | None = None
        try:
            lexical = os.lstat(command.path)
            descriptor = os.open(
                command.path,
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            )
            opened = os.fstat(descriptor)
            if (
                _file_identity(lexical) != _file_identity(opened)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or opened.st_uid != self.trusted_uid
                or stat.S_IMODE(opened.st_mode) & 0o022
                or not stat.S_IMODE(opened.st_mode) & 0o111
                or opened.st_size <= 0
                or _hash_descriptor(descriptor) != command.sha256
            ):
                raise GuardError("command_identity_invalid")
            completed = subprocess.run(
                (f"/proc/self/fd/{descriptor}", *argv),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=self.timeout_seconds,
                check=False,
                env=_ENVIRONMENT,
                pass_fds=(descriptor,),
            )
            final = os.fstat(descriptor)
            if _file_identity(final) != _file_identity(opened) or (
                _hash_descriptor(descriptor) != command.sha256
            ):
                raise GuardError("command_identity_changed")
            if (
                len(completed.stdout) > self.max_stdout_bytes
                or len(completed.stderr) > self.max_stderr_bytes
            ):
                raise GuardError("command_output_invalid")
            try:
                stdout = completed.stdout.decode("utf-8")
                stderr = completed.stderr.decode("utf-8")
            except UnicodeDecodeError:
                raise GuardError("command_output_invalid") from None
            return CommandResult(completed.returncode, stdout, stderr)
        except GuardError:
            raise
        except subprocess.TimeoutExpired as exc:
            raise GuardError("command_timeout") from exc
        except OSError as exc:
            raise GuardError("command_identity_invalid") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)


@dataclass(frozen=True, slots=True)
class SlurmFacts:
    job_id: str
    node_name: str
    comment: str
    account: str
    partition: str
    qos: str
    cpus: int
    memory_mib: int
    wall_time: str


def _oneline(value: str) -> dict[str, str]:
    lines = [line for line in value.splitlines() if line.strip()]
    if len(lines) != 1:
        raise GuardError("slurm_controller_invalid")
    result: dict[str, str] = {}
    for token in lines[0].split():
        key, separator, field = token.partition("=")
        if not separator or not key or key in result:
            raise GuardError("slurm_controller_invalid")
        result[key] = field
    return result


def _memory_mib(value: str) -> int:
    match = _MEMORY.fullmatch(value)
    if match is None:
        raise GuardError("slurm_resources_invalid")
    count = int(match.group(1))
    unit = match.group(2)
    bytes_value = count * {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}[unit]
    if bytes_value % (1024**2):
        raise GuardError("slurm_resources_invalid")
    return bytes_value // (1024**2)


def _tres(value: str) -> tuple[int, int]:
    fields: dict[str, str] = {}
    for item in value.split(","):
        key, separator, field = item.partition("=")
        if (
            not separator
            or key not in {"cpu", "mem", "node", "billing"}
            or key in fields
            or not field
        ):
            raise GuardError("slurm_accounting_invalid")
        fields[key] = field
    if not {"cpu", "mem", "node"}.issubset(fields) or fields["node"] != "1":
        raise GuardError("slurm_accounting_invalid")
    try:
        cpus = int(fields["cpu"])
        memory = _memory_mib(fields["mem"])
    except (ValueError, GuardError):
        raise GuardError("slurm_accounting_invalid") from None
    return cpus, memory


class SlurmInspector:
    """Compare live controller and accounting evidence with local policy."""

    def __init__(
        self,
        *,
        cluster_id: str,
        node_name: str,
        identity: IdentityConfig,
        policy: SlurmConfig,
        commands: CommandConfig,
        runner: CommandRunner,
    ) -> None:
        self.cluster_id = cluster_id
        self.node_name = node_name
        self.identity = identity
        self.policy = policy
        self.commands = commands
        self.runner = runner

    @staticmethod
    def _successful(result: CommandResult) -> str:
        if result.returncode != 0 or result.stderr:
            raise GuardError("slurm_command_failed")
        return result.stdout

    def observe(self, *, job_id: str, grant_id: UUID) -> SlurmFacts:
        if _JOB_ID.fullmatch(job_id) is None or grant_id.int == 0:
            raise GuardError("slurm_request_invalid")
        control = _oneline(
            self._successful(
                self.runner.run(
                    self.commands.scontrol,
                    ("show", "job", job_id, "--oneliner"),
                )
            )
        )
        required = {
            "JobId",
            "UserId",
            "GroupId",
            "JobState",
            "Account",
            "QOS",
            "Partition",
            "BatchHost",
            "NodeList",
            "NumNodes",
            "NumCPUs",
            "MinMemoryNode",
            "TimeLimit",
            "Features",
            "Comment",
            "Requeue",
            "Restarts",
        }
        if not required.issubset(control):
            raise GuardError("slurm_controller_invalid")
        if control["JobId"] != job_id:
            raise GuardError("slurm_controller_invalid")
        if control["JobState"] != "RUNNING":
            raise GuardError("slurm_job_not_running")
        if (
            control["UserId"] != f"loom-builder({self.identity.uid})"
            or control["GroupId"] != f"loom-task-builder({self.identity.gid})"
        ):
            raise GuardError("slurm_identity_invalid")
        if (
            control["Account"] != self.policy.account
            or control["Partition"] != self.policy.partition
            or control["QOS"] != self.policy.qos
            or control["Features"] != self.policy.feature
        ):
            raise GuardError("slurm_policy_invalid")
        if control["Comment"] != f"loom-task-builder-v1:grant={grant_id}":
            raise GuardError("slurm_grant_invalid")
        if (
            control["BatchHost"] != self.node_name
            or control["NodeList"] != self.node_name
            or control["NumNodes"] != "1"
        ):
            raise GuardError("slurm_node_invalid")
        try:
            cpus = int(control["NumCPUs"])
            memory_mib = _memory_mib(control["MinMemoryNode"])
        except ValueError:
            raise GuardError("slurm_resources_invalid") from None
        if (
            cpus != self.policy.cpus
            or memory_mib != self.policy.memory_mib
            or control["TimeLimit"] != self.policy.wall_time
        ):
            raise GuardError("slurm_resources_invalid")
        if (
            control["Requeue"] != "0"
            or control["Restarts"] != "0"
            or any(
                control.get(name, "N/A") not in {"N/A", "0"} for name in ("ArrayJobId", "HetJobId")
            )
        ):
            raise GuardError("slurm_lifecycle_invalid")

        accounting_output = self._successful(
            self.runner.run(
                self.commands.sacct,
                (
                    "--noheader",
                    "--parsable2",
                    "--allocations",
                    f"--jobs={job_id}",
                    "--format=JobIDRaw,State,User,Group,Account,Cluster,Partition,QOS,"
                    "AllocCPUS,ReqMem,AllocTRES,NodeList,Comment",
                ),
            )
        )
        rows = [line for line in accounting_output.splitlines() if line]
        if len(rows) != 1:
            raise GuardError("slurm_accounting_invalid")
        values = rows[0].split("|")
        if len(values) != 14 or values[-1] != "":
            raise GuardError("slurm_accounting_invalid")
        (
            acct_job,
            state,
            user,
            group,
            account,
            cluster,
            partition,
            qos,
            alloc_cpus,
            request_memory,
            allocated_tres,
            nodes,
            comment,
            _empty,
        ) = values
        try:
            accounting_cpus = int(alloc_cpus)
            accounting_memory = _memory_mib(request_memory)
            tres_cpus, tres_memory = _tres(allocated_tres)
        except (ValueError, GuardError):
            raise GuardError("slurm_accounting_invalid") from None
        if (
            acct_job != job_id
            or state != "RUNNING"
            or user != "loom-builder"
            or group != "loom-task-builder"
            or account != self.policy.account
            or cluster != self.policy.cluster_name
            or partition != self.policy.partition
            or qos != self.policy.qos
            or nodes != self.node_name
            or comment != control["Comment"]
            or accounting_cpus != cpus
            or accounting_memory != memory_mib
            or tres_cpus != cpus
            or tres_memory != memory_mib
        ):
            raise GuardError("slurm_accounting_invalid")
        return SlurmFacts(
            job_id=job_id,
            node_name=self.node_name,
            comment=comment,
            account=account,
            partition=partition,
            qos=qos,
            cpus=cpus,
            memory_mib=memory_mib,
            wall_time=control["TimeLimit"],
        )

    def quarantine_capability(self) -> None:
        result = self.runner.run(
            self.commands.scontrol,
            (
                "update",
                f"NodeName={self.node_name}",
                f"ActiveFeatures-={self.policy.feature}",
            ),
        )
        if result.returncode != 0 or result.stderr or result.stdout:
            raise GuardError("slurm_quarantine_failed")


__all__ = [
    "CommandResult",
    "CommandRunner",
    "PinnedCommandRunner",
    "SlurmFacts",
    "SlurmInspector",
]
