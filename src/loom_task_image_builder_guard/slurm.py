"""Pinned-command Slurm readback for one task-image builder allocation."""

from __future__ import annotations

import ctypes
import hashlib
import os
import platform
import re
import selectors
import signal
import stat
import subprocess
import time
from collections.abc import Callable
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
_MAX_PROC_ENTRIES = 1 << 20
_SYS_PIDFD_SEND_SIGNAL = 424
_SYS_PIDFD_OPEN = 434
_ENVIRONMENT = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}


def _pidfd_open(pid: int) -> int:
    native = getattr(os, "pidfd_open", None)
    if native is not None:
        return int(native(pid, 0))
    if platform.machine() not in {"x86_64", "aarch64"}:
        raise GuardError("command_descendant_check_failed")
    libc = ctypes.CDLL(None, use_errno=True)
    descriptor = int(libc.syscall(_SYS_PIDFD_OPEN, pid, 0))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    os.set_inheritable(descriptor, False)
    return descriptor


def _pidfd_send_signal(descriptor: int, signum: signal.Signals) -> None:
    native = getattr(signal, "pidfd_send_signal", None)
    if native is not None:
        native(descriptor, signum)
        return
    if platform.machine() not in {"x86_64", "aarch64"}:
        raise GuardError("command_descendant_cleanup_failed")
    libc = ctypes.CDLL(None, use_errno=True)
    result = int(libc.syscall(_SYS_PIDFD_SEND_SIGNAL, descriptor, int(signum), 0, 0))
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


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
        progress: Callable[[], None] = lambda: None,
    ) -> None:
        self.trusted_uid = trusted_uid
        self.timeout_seconds = timeout_seconds
        self.max_stdout_bytes = max_stdout_bytes
        self.max_stderr_bytes = max_stderr_bytes
        self.progress = progress

    @classmethod
    def _terminate(cls, process: subprocess.Popen[bytes]) -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
        try:
            deadline = time.monotonic() + 5
            while True:
                descendants = cls._session_descendant_pidfds(process.pid)
                try:
                    if not descendants:
                        break
                    for descriptor in descendants:
                        try:
                            _pidfd_send_signal(descriptor, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        except OSError as exc:
                            raise GuardError(
                                "command_descendant_cleanup_failed"
                            ) from exc
                finally:
                    for descriptor in descendants:
                        os.close(descriptor)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise GuardError("command_descendant_cleanup_failed")
                time.sleep(min(0.01, remaining))
        finally:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    @staticmethod
    def _session_descendant_pidfds(session_id: int) -> tuple[int, ...]:
        def process_session(pid: int) -> tuple[bytes, int] | None:
            try:
                descriptor = os.open(
                    f"/proc/{pid}/stat",
                    os.O_RDONLY
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_NONBLOCK", 0),
                )
                try:
                    payload = os.read(descriptor, 4097)
                finally:
                    os.close(descriptor)
            except (FileNotFoundError, ProcessLookupError):
                return None
            except OSError as exc:
                raise GuardError("command_descendant_check_failed") from exc
            marker = payload.rfind(b") ")
            fields = payload[marker + 2 :].split() if marker >= 0 else []
            try:
                return fields[0], int(fields[3])
            except (IndexError, ValueError):
                raise GuardError("command_descendant_check_failed") from None

        inspected = 0
        descendants: list[int] = []
        try:
            processes = os.scandir("/proc")
        except OSError as exc:
            raise GuardError("command_descendant_check_failed") from exc
        try:
            with processes:
                for process in processes:
                    if not process.name.isascii() or not process.name.isdigit():
                        continue
                    inspected += 1
                    if inspected > _MAX_PROC_ENTRIES:
                        raise GuardError("command_descendant_check_failed")
                    pid = int(process.name)
                    if pid == session_id:
                        continue
                    observed = process_session(pid)
                    if observed is None or observed[0] == b"Z" or observed[1] != session_id:
                        continue
                    try:
                        descriptor = _pidfd_open(pid)
                    except (ProcessLookupError, FileNotFoundError):
                        continue
                    except OSError as exc:
                        raise GuardError("command_descendant_check_failed") from exc
                    confirmed = process_session(pid)
                    if (
                        confirmed is None
                        or confirmed[0] == b"Z"
                        or confirmed[1] != session_id
                    ):
                        os.close(descriptor)
                        continue
                    descendants.append(descriptor)
            return tuple(descendants)
        except BaseException:
            for descriptor in descendants:
                os.close(descriptor)
            raise

    @staticmethod
    def _wait_unreaped(process: subprocess.Popen[bytes], deadline: float) -> None:
        while True:
            try:
                status = os.waitid(
                    os.P_PID,
                    process.pid,
                    os.WEXITED | os.WNOHANG | os.WNOWAIT,
                )
            except (ChildProcessError, OSError) as exc:
                raise GuardError("command_wait_invalid") from exc
            if status is not None:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise GuardError("command_timeout")
            time.sleep(min(0.01, remaining))

    def _bounded_output(
        self,
        process: subprocess.Popen[bytes],
    ) -> tuple[int, bytes, bytes]:
        if process.stdout is None or process.stderr is None:
            raise GuardError("command_output_invalid")
        outputs = {
            process.stdout.fileno(): bytearray(),
            process.stderr.fileno(): bytearray(),
        }
        limits = {
            process.stdout.fileno(): self.max_stdout_bytes,
            process.stderr.fileno(): self.max_stderr_bytes,
        }
        selector = selectors.DefaultSelector()
        deadline = time.monotonic() + self.timeout_seconds
        try:
            for stream in (process.stdout, process.stderr):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise GuardError("command_timeout")
                ready = selector.select(remaining)
                if not ready:
                    raise GuardError("command_timeout")
                for key, _events in ready:
                    descriptor = key.fd
                    maximum = limits[descriptor]
                    budget = maximum + 1 - len(outputs[descriptor])
                    if budget <= 0:
                        raise GuardError("command_output_invalid")
                    chunk = os.read(descriptor, min(64 * 1024, budget))
                    if not chunk:
                        selector.unregister(key.fileobj)
                        self.progress()
                        continue
                    outputs[descriptor].extend(chunk)
                    self.progress()
                    if len(outputs[descriptor]) > maximum:
                        raise GuardError("command_output_invalid")
            self._wait_unreaped(process, deadline)
            descendants = self._session_descendant_pidfds(process.pid)
            for descriptor in descendants:
                os.close(descriptor)
            if descendants:
                self._terminate(process)
                raise GuardError("command_descendants_invalid")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise GuardError("command_timeout")
            try:
                returncode = process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                raise GuardError("command_timeout") from None
            return (
                returncode,
                bytes(outputs[process.stdout.fileno()]),
                bytes(outputs[process.stderr.fileno()]),
            )
        finally:
            selector.close()

    def run(self, command: CommandIdentity, argv: tuple[str, ...]) -> CommandResult:
        if any(not isinstance(item, str) or "\x00" in item for item in argv):
            raise GuardError("command_arguments_invalid")
        descriptor: int | None = None
        try:
            self.progress()
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
            process = subprocess.Popen(
                (f"/proc/self/fd/{descriptor}", *argv),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=_ENVIRONMENT,
                pass_fds=(descriptor,),
                start_new_session=True,
            )
            self.progress()
            try:
                try:
                    returncode, stdout_payload, stderr_payload = self._bounded_output(
                        process
                    )
                except BaseException:
                    self._terminate(process)
                    raise
            finally:
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()
            final = os.fstat(descriptor)
            self.progress()
            final_path = os.lstat(command.path)
            if (
                _file_identity(final) != _file_identity(opened)
                or _file_identity(final_path) != _file_identity(opened)
                or _hash_descriptor(descriptor) != command.sha256
            ):
                raise GuardError("command_identity_changed")
            try:
                stdout = stdout_payload.decode("utf-8")
                stderr = stderr_payload.decode("utf-8")
            except UnicodeDecodeError:
                raise GuardError("command_output_invalid") from None
            return CommandResult(returncode, stdout, stderr)
        except GuardError:
            raise
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


@dataclass(frozen=True, slots=True)
class SlurmTerminalFacts:
    job_id: str
    node_name: str
    comment: str
    account: str
    partition: str
    qos: str
    cpus: int
    memory_mib: int
    wall_time: str
    controller_state: str
    accounting_state: str


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

    def _observe(
        self,
        *,
        job_id: str,
        grant_id: UUID,
        states: frozenset[str],
        state_error: str,
    ) -> tuple[SlurmFacts, str, str]:
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
        controller_state = control["JobState"]
        if controller_state not in states:
            raise GuardError(state_error)
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
            or state != controller_state
            or state not in states
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
        facts = SlurmFacts(
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
        return facts, controller_state, state

    def observe(self, *, job_id: str, grant_id: UUID) -> SlurmFacts:
        facts, _controller_state, _accounting_state = self._observe(
            job_id=job_id,
            grant_id=grant_id,
            states=frozenset({"RUNNING"}),
            state_error="slurm_job_not_running",
        )
        return facts

    def observe_terminal(
        self,
        *,
        job_id: str,
        grant_id: UUID,
    ) -> SlurmTerminalFacts:
        facts, controller_state, accounting_state = self._observe(
            job_id=job_id,
            grant_id=grant_id,
            states=frozenset(
                {
                    "BOOT_FAIL",
                    "CANCELLED",
                    "COMPLETED",
                    "DEADLINE",
                    "FAILED",
                    "NODE_FAIL",
                    "OUT_OF_MEMORY",
                    "PREEMPTED",
                    "TIMEOUT",
                }
            ),
            state_error="slurm_terminal_invalid",
        )
        return SlurmTerminalFacts(
            facts.job_id,
            facts.node_name,
            facts.comment,
            facts.account,
            facts.partition,
            facts.qos,
            facts.cpus,
            facts.memory_mib,
            facts.wall_time,
            controller_state,
            accounting_state,
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
    "SlurmTerminalFacts",
]
