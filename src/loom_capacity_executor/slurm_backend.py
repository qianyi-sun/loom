"""Bounded argv-only Slurm observation and mutation backend."""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import hashlib
import hmac
import os
import re
import signal
import stat
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from loom_capacity_executor.slurm_contracts import (
    MAX_ACCOUNTING_RECORDS,
    MAX_GENERIC_TRES,
    MEBIBYTE,
    SlurmAccountingHighWaterV2,
    SlurmAuthorityV2,
    SlurmCancelRequestV2,
    SlurmExecutableIdentityV2,
    SlurmJobObservationV2,
    SlurmLaunchRequestV2,
    SlurmSubmissionV2,
    SlurmTerminalEvidenceV2,
    SlurmTresValueV2,
    strict_datetime,
)

_MAX_EXECUTABLE_BYTES = 64 * 1024 * 1024
_MAX_INVENTORY_RECORDS = 10_000
_MAX_CONFIG_RECORDS = 4_096
_MAX_SUBMITTED_VERIFIER_BYTES = 64 * 1024
_EXECUTION_NODE_PYTHON = "/usr/bin/python3"
_INVENTORY_AGGREGATE_TRES_RECORDS = 1
_TERMINAL_FIXED_AND_AGGREGATE_TRES_RECORDS = 5
_MAX_INVENTORY_TRES_RECORDS = MAX_GENERIC_TRES + _INVENTORY_AGGREGATE_TRES_RECORDS
_MAX_TERMINAL_TRES_RECORDS = MAX_GENERIC_TRES + _TERMINAL_FIXED_AND_AGGREGATE_TRES_RECORDS
_READ_CHUNK_BYTES = 64 * 1024
_PROCESS_CLEANUP_GRACE_SECONDS = 0.5
_TRUSTED_ENVIRONMENT = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin",
    "PYTHONCOERCECLOCALE": "0",
    "PYTHONUTF8": "1",
}
_MEMORY_PATTERN = re.compile(r"^([1-9][0-9]{0,18})([KMGTP])([cn]?)$")
_CLUSTER_CONFIG_PATTERN = re.compile(r"^ClusterName\s*=\s*(\S+)\s*$")
_CONTROLLER_CONFIG_PATTERN = re.compile(
    r"^SlurmctldHost(?:\[([0-9]{1,3})\])?\s*=\s*"
    r"([A-Za-z0-9][A-Za-z0-9.-]{0,252}[A-Za-z0-9])"
    r"(?:\([^()\s]{1,255}\))?\s*$"
)


class SlurmBackendError(RuntimeError):
    """A scheduler boundary could not prove a safe result."""


class SlurmAuthorityError(SlurmBackendError):
    """Local executable or controller authority was not exact."""


class SlurmCommandError(SlurmBackendError):
    """A bounded Slurm process failed."""


class SlurmOutputError(SlurmBackendError):
    """Slurm returned missing, duplicate, unknown, or malformed data."""


class SlurmStateConflictError(SlurmBackendError):
    """The current physical job does not satisfy a conditional mutation."""


class SlurmSubmissionUncertainError(SlurmBackendError):
    """An sbatch invocation began but no exact physical identity was proved."""


class SlurmCancellationUncertainError(SlurmBackendError):
    """A scancel invocation began but no exact clean result was proved."""


async def _read_bounded(
    stream: asyncio.StreamReader,
    *,
    limit: int,
    exceeded: asyncio.Event,
) -> bytes:
    chunks: list[bytes] = []
    observed = 0
    while chunk := await stream.read(_READ_CHUNK_BYTES):
        observed += len(chunk)
        if observed > limit:
            exceeded.set()
        else:
            chunks.append(chunk)
    return b"".join(chunks)


async def _write_bounded(
    stream: asyncio.StreamWriter,
    payload: bytes,
) -> None:
    try:
        stream.write(payload)
        await stream.drain()
    finally:
        stream.close()
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            await stream.wait_closed()


def _signal_process(process: asyncio.subprocess.Process) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process.pid, signal.SIGKILL)
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.kill()


async def _bounded_process_cleanup(
    process: asyncio.subprocess.Process,
    tasks: tuple[asyncio.Task[bytes | int | None], ...],
) -> None:
    _signal_process(process)
    _completed, pending = await asyncio.wait(
        tasks,
        timeout=_PROCESS_CLEANUP_GRACE_SECONDS,
    )
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.wait(pending, timeout=_PROCESS_CLEANUP_GRACE_SECONDS)
    for task in tasks:
        if task.done() and not task.cancelled():
            task.exception()


@contextlib.contextmanager
def _open_verified_executable(identity: SlurmExecutableIdentityV2) -> Iterator[int]:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(identity.path, flags)
    except OSError:
        raise SlurmAuthorityError("Slurm executable is unavailable or not a nonsymlink") from None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != identity.owner_uid
            or not before.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            or before.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or before.st_size > _MAX_EXECUTABLE_BYTES
        ):
            raise SlurmAuthorityError("Slurm executable identity is unsafe")
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, _READ_CHUNK_BYTES):
            digest.update(chunk)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) or not hmac.compare_digest(digest.hexdigest(), identity.sha256):
            raise SlurmAuthorityError("Slurm executable digest or file identity changed")
        yield descriptor
    finally:
        os.close(descriptor)


def _validate_executable(identity: SlurmExecutableIdentityV2) -> None:
    with _open_verified_executable(identity):
        pass


def _validate_immutable_launcher(
    identity: SlurmExecutableIdentityV2,
    *,
    executor_uid: int,
) -> None:
    with _open_verified_executable(identity):
        pass
    if identity.owner_uid == executor_uid:
        raise SlurmAuthorityError("trusted launcher path is not immutable to executor identity")
    path = Path(identity.path)
    for directory in reversed(path.parents):
        try:
            info = directory.lstat()
        except OSError:
            raise SlurmAuthorityError(
                "trusted launcher directory authority is unavailable"
            ) from None
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid not in {0, identity.owner_uid}
            or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (info.st_uid == executor_uid and info.st_mode & stat.S_IWUSR)
        ):
            raise SlurmAuthorityError("trusted launcher directory is not immutable")


def _execution_node_verifier(
    request: SlurmLaunchRequestV2,
    *,
    executor_uid: int,
) -> bytes:
    launcher_argv = request.trusted_launcher_argv()
    # This fixed interpreter is part of the trusted compute-node kernel/OS boundary.
    script = f"""#!{_EXECUTION_NODE_PYTHON}
from __future__ import annotations

import hashlib
import hmac
import os
import stat

LAUNCHER_PATH = {request.launcher.path!r}
LAUNCHER_SHA256 = {request.launcher.sha256!r}
LAUNCHER_OWNER_UID = {request.launcher.owner_uid!r}
EXECUTOR_UID = {executor_uid!r}
LAUNCHER_ARGV = {launcher_argv!r}
MAX_LAUNCHER_BYTES = {_MAX_EXECUTABLE_BYTES!r}
READ_CHUNK_BYTES = {_READ_CHUNK_BYTES!r}


def fail() -> None:
    raise SystemExit(70)


def validate_directory(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid not in {{0, LAUNCHER_OWNER_UID}}
        or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        or (info.st_uid == EXECUTOR_UID and info.st_mode & stat.S_IWUSR)
    ):
        fail()


path_components = LAUNCHER_PATH.split("/")
if (
    LAUNCHER_OWNER_UID == EXECUTOR_UID
    or not hasattr(os, "O_NOFOLLOW")
    or not hasattr(os, "O_DIRECTORY")
    or not LAUNCHER_PATH.startswith("/")
    or len(path_components) < 2
    or any(component in {{"", ".", ".."}} for component in path_components[1:])
):
    fail()
directory_descriptor = -1
descriptor = -1
try:
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_descriptor = os.open("/", directory_flags)
    validate_directory(directory_descriptor)
    for component in path_components[1:-1]:
        next_descriptor = os.open(component, directory_flags, dir_fd=directory_descriptor)
        os.close(directory_descriptor)
        directory_descriptor = next_descriptor
        validate_directory(directory_descriptor)
    descriptor = os.open(
        path_components[-1],
        os.O_RDONLY | os.O_NOFOLLOW,
        dir_fd=directory_descriptor,
    )
except OSError:
    fail()
finally:
    if directory_descriptor >= 0:
        os.close(directory_descriptor)
try:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_uid != LAUNCHER_OWNER_UID
        or not before.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        or before.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        or before.st_size > MAX_LAUNCHER_BYTES
    ):
        fail()
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, READ_CHUNK_BYTES):
        digest.update(chunk)
    after = os.fstat(descriptor)
    if (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or not hmac.compare_digest(digest.hexdigest(), LAUNCHER_SHA256)
    ):
        fail()
    os.lseek(descriptor, 0, os.SEEK_SET)
    os.set_inheritable(descriptor, True)
    os.execve(f"/proc/self/fd/{{descriptor}}", LAUNCHER_ARGV, dict(os.environ))
except OSError:
    fail()
finally:
    os.close(descriptor)
"""
    payload = script.encode("utf-8")
    if not payload or len(payload) > _MAX_SUBMITTED_VERIFIER_BYTES:
        raise SlurmAuthorityError("submitted execution-node verifier exceeds its bound")
    return payload


def _decode_output(payload: bytes, *, command: str) -> str:
    try:
        value = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise SlurmOutputError(f"{command} output is not UTF-8") from None
    if any(
        character != "\n"
        and (
            ord(character) < 0x20
            or 0x7F <= ord(character) <= 0x9F
            or character in {"\u2028", "\u2029"}
        )
        for character in value
    ):
        raise SlurmOutputError(f"{command} output contains invalid control data")
    return value


def _lines(value: str, *, command: str, maximum: int, allow_empty: bool) -> tuple[str, ...]:
    records = tuple(value.splitlines())
    if not allow_empty and not records:
        raise SlurmOutputError(f"{command} output is missing")
    if len(records) > maximum:
        raise SlurmOutputError(f"{command} output has too many records")
    if any(not record for record in records):
        raise SlurmOutputError(f"{command} output contains an empty record")
    return records


def _controller_config_facts(value: str) -> tuple[str, str]:
    records = tuple(value.splitlines())
    if not records:
        raise SlurmOutputError("scontrol output is missing")
    if len(records) > _MAX_CONFIG_RECORDS:
        raise SlurmOutputError("scontrol output has too many records")
    cluster: str | None = None
    primary_controller: str | None = None
    controller_indexes: set[int] = set()
    for line in records:
        stripped = line.strip()
        if not stripped:
            continue
        cluster_match = _CLUSTER_CONFIG_PATTERN.fullmatch(stripped)
        if cluster_match is not None:
            if cluster is not None:
                raise SlurmOutputError("scontrol cluster authority record is duplicate")
            cluster = cluster_match.group(1)
            continue
        controller_match = _CONTROLLER_CONFIG_PATTERN.fullmatch(stripped)
        if controller_match is not None:
            index = int(controller_match.group(1) or "0")
            if index in controller_indexes:
                raise SlurmOutputError("scontrol controller authority record is duplicate")
            controller_indexes.add(index)
            if index == 0:
                if primary_controller is not None:
                    raise SlurmOutputError(
                        "scontrol primary controller authority record is duplicate"
                    )
                primary_controller = controller_match.group(2)
            continue
        if stripped.startswith("ClusterName") or stripped.startswith("SlurmctldHost"):
            raise SlurmOutputError("scontrol authority record is malformed")
    if cluster is None or primary_controller is None:
        raise SlurmOutputError("scontrol authority record is missing")
    return cluster, primary_controller


def _time_argument(seconds: int) -> str:
    days, remainder = divmod(seconds, 24 * 60 * 60)
    hours, remainder = divmod(remainder, 60 * 60)
    minutes, final_seconds = divmod(remainder, 60)
    return f"{days}-{hours:02d}:{minutes:02d}:{final_seconds:02d}"


def _memory_bytes(value: str, *, cpus: int, nodes: int) -> int:
    match = _MEMORY_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError("Slurm memory value is malformed")
    quantity = int(match.group(1))
    multiplier = {
        "K": 1024,
        "M": MEBIBYTE,
        "G": 1024**3,
        "T": 1024**4,
        "P": 1024**5,
    }[match.group(2)]
    result = quantity * multiplier
    if match.group(3) == "c":
        result *= cpus
    elif match.group(3) == "n":
        result *= max(nodes, 1)
    if result > (1 << 63) - 1:
        raise ValueError("Slurm memory value exceeds its bound")
    return result


def _gres_values(
    value: str,
    *,
    allowed_generic: set[str],
) -> tuple[int, tuple[SlurmTresValueV2, ...]]:
    if value in {"N/A", "(null)"}:
        return 0, ()
    aggregate_gpu: int | None = None
    generic: list[SlurmTresValueV2] = []
    seen: set[str] = set()
    records = value.split(",")
    if len(records) > _MAX_INVENTORY_TRES_RECORDS:
        raise ValueError("Slurm GRES value exceeds its bound")
    for record in records:
        fields = record.split(":")
        if len(fields) == 2:
            resource, count_text = fields
            resource_type = None
        elif len(fields) == 3:
            resource, resource_type, count_text = fields
        else:
            raise ValueError("Slurm GRES value is malformed")
        if not resource or resource_type == "":
            raise ValueError("Slurm GRES name is malformed")
        count = int(count_text)
        if not 0 < count <= (1 << 63) - 1:
            raise ValueError("Slurm GRES count is invalid")
        if resource == "gpu" and resource_type is None:
            if aggregate_gpu is not None:
                raise ValueError("Slurm aggregate GPU value is duplicate")
            aggregate_gpu = count
            continue
        name = f"gres/{resource}"
        if resource_type is not None:
            name += f":{resource_type}"
        if name not in allowed_generic:
            raise ValueError("Slurm GRES value is not configured")
        if name in seen:
            raise ValueError("Slurm GRES value is duplicate")
        seen.add(name)
        generic.append(SlurmTresValueV2(name=name, value=count))
    typed_gpu_total = sum(item.value for item in generic if item.name.startswith("gres/gpu:"))
    if aggregate_gpu is not None and typed_gpu_total and aggregate_gpu != typed_gpu_total:
        raise ValueError("Slurm aggregate and typed GPU values conflict")
    gpus = aggregate_gpu if aggregate_gpu is not None else typed_gpu_total
    if gpus > 1_024:
        raise ValueError("Slurm GPU count exceeds its bound")
    return gpus, tuple(sorted(generic, key=lambda item: item.name))


def _allocated_resources(
    value: str,
    *,
    cpus: int,
    nodes: int,
    requested_memory_bytes: int,
    allowed_generic: set[str],
) -> tuple[int, tuple[SlurmTresValueV2, ...]]:
    records = value.split(",")
    if not records or len(records) > _MAX_TERMINAL_TRES_RECORDS:
        raise ValueError("Slurm allocated TRES value is malformed")
    parsed: dict[str, str] = {}
    fixed_names = {"billing", "cpu", "gres/gpu", "mem", "node"}
    for record in records:
        fields = record.split("=", maxsplit=1)
        if len(fields) != 2 or fields[0] in parsed:
            raise ValueError("Slurm allocated TRES value is malformed or duplicate")
        if fields[0] not in fixed_names and fields[0] not in allowed_generic:
            raise ValueError("Slurm allocated TRES value is unknown")
        parsed[fields[0]] = fields[1]
    if not {"cpu", "mem", "node"} <= set(parsed):
        raise ValueError("Slurm allocated TRES value is incomplete")
    if int(parsed["cpu"]) != cpus or int(parsed["node"]) != nodes:
        raise ValueError("Slurm allocated TRES value conflicts with fixed fields")
    allocated_memory_bytes = _memory_bytes(parsed["mem"], cpus=cpus, nodes=nodes)
    if allocated_memory_bytes != requested_memory_bytes:
        raise ValueError("Slurm requested and allocated memory values conflict")
    generic: list[SlurmTresValueV2] = []
    for name in sorted(set(parsed) & allowed_generic):
        count = int(parsed[name])
        if count <= 0:
            raise ValueError("Slurm allocated generic TRES count is invalid")
        generic.append(SlurmTresValueV2(name=name, value=count))
    aggregate_gpu = int(parsed.get("gres/gpu", "0"))
    typed_gpu_total = sum(item.value for item in generic if item.name.startswith("gres/gpu:"))
    if typed_gpu_total and ("gres/gpu" not in parsed or aggregate_gpu != typed_gpu_total):
        raise ValueError("Slurm allocated aggregate and typed GPU values conflict")
    gpus = aggregate_gpu if "gres/gpu" in parsed else typed_gpu_total
    if not 0 <= gpus <= 1_024:
        raise ValueError("Slurm allocated GPU value is invalid")
    return gpus, tuple(generic)


class AsyncSlurmBackend:
    """Use only typed scheduler values and bounded local Slurm executables."""

    def __init__(self, authority: SlurmAuthorityV2) -> None:
        if not isinstance(authority, SlurmAuthorityV2):
            raise TypeError("Slurm backend requires typed SlurmAuthorityV2")
        self.authority = authority

    async def _run(
        self,
        identity: SlurmExecutableIdentityV2,
        argv: tuple[str, ...],
        *,
        stdin_payload: bytes | None = None,
    ) -> tuple[bytes, bytes]:
        if (
            not argv
            or argv[0] != identity.path
            or any(
                not isinstance(argument, str) or not argument or "\0" in argument
                for argument in argv
            )
        ):
            raise SlurmAuthorityError("Slurm argv is not bound to its executable")
        if stdin_payload is not None and (
            not isinstance(stdin_payload, bytes)
            or not stdin_payload
            or len(stdin_payload) > _MAX_SUBMITTED_VERIFIER_BYTES
        ):
            raise SlurmAuthorityError("Slurm stdin payload is invalid or exceeds its bound")
        if not Path("/proc/self/fd").is_dir():
            raise SlurmAuthorityError("fd-bound Slurm execution is unavailable")
        try:
            with _open_verified_executable(identity) as descriptor:
                process = await asyncio.create_subprocess_exec(
                    *argv,
                    executable=f"/proc/self/fd/{descriptor}",
                    pass_fds=(descriptor,),
                    stdin=(
                        asyncio.subprocess.PIPE
                        if stdin_payload is not None
                        else asyncio.subprocess.DEVNULL
                    ),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=_TRUSTED_ENVIRONMENT,
                    start_new_session=True,
                )
        except OSError:
            raise SlurmCommandError("Slurm command could not start") from None
        assert process.stdout is not None
        assert process.stderr is not None
        if stdin_payload is not None:
            assert process.stdin is not None
        stdout_exceeded = asyncio.Event()
        stderr_exceeded = asyncio.Event()
        stdout_task = asyncio.create_task(
            _read_bounded(
                process.stdout,
                limit=self.authority.max_stdout_bytes,
                exceeded=stdout_exceeded,
            )
        )
        stderr_task = asyncio.create_task(
            _read_bounded(
                process.stderr,
                limit=self.authority.max_stderr_bytes,
                exceeded=stderr_exceeded,
            )
        )
        wait_task = asyncio.create_task(process.wait())
        stdin_task = (
            asyncio.create_task(_write_bounded(process.stdin, stdin_payload))
            if process.stdin is not None and stdin_payload is not None
            else None
        )
        stdout_limit_task = asyncio.create_task(stdout_exceeded.wait())
        stderr_limit_task = asyncio.create_task(stderr_exceeded.wait())
        process_tasks: tuple[asyncio.Task[bytes | int | None], ...] = (
            stdout_task,
            stderr_task,
            wait_task,
            *((stdin_task,) if stdin_task is not None else ()),
        )
        limit_tasks = (stdout_limit_task, stderr_limit_task)
        deadline = asyncio.get_running_loop().time() + self.authority.command_timeout_seconds
        failure: SlurmBackendError | None = None
        try:
            while not all(task.done() for task in process_tasks):
                if stdin_task is not None and stdin_task.done():
                    stdin_failure = stdin_task.exception()
                    if stdin_failure is not None:
                        failure = SlurmCommandError("Slurm command did not accept exact stdin")
                        break
                if stdout_exceeded.is_set() or stderr_exceeded.is_set():
                    failure = SlurmOutputError("Slurm command output exceeded its bound")
                    break
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    failure = SlurmCommandError("Slurm command timed out")
                    break
                active = tuple(task for task in (*process_tasks, *limit_tasks) if not task.done())
                completed, _pending = await asyncio.wait(
                    active,
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not completed:
                    failure = SlurmCommandError("Slurm command timed out")
                    break
            if stdin_task is not None and stdin_task.done():
                stdin_failure = stdin_task.exception()
                if stdin_failure is not None:
                    failure = SlurmCommandError("Slurm command did not accept exact stdin")
            if failure is not None:
                await _bounded_process_cleanup(process, process_tasks)
            if stdout_exceeded.is_set() or stderr_exceeded.is_set():
                failure = SlurmOutputError("Slurm command output exceeded its bound")
            if failure is not None:
                raise failure
            stdout = stdout_task.result()
            stderr = stderr_task.result()
            return_code = wait_task.result()
        except asyncio.CancelledError:
            await _bounded_process_cleanup(process, process_tasks)
            raise
        finally:
            for task in (stdout_limit_task, stderr_limit_task):
                task.cancel()
            await asyncio.gather(stdout_limit_task, stderr_limit_task, return_exceptions=True)
        if return_code != 0:
            raise SlurmCommandError(f"Slurm command failed with exit code {return_code}")
        if stderr:
            raise SlurmOutputError("successful Slurm command returned unexpected stderr")
        return stdout, stderr

    async def _run_text(
        self,
        identity: SlurmExecutableIdentityV2,
        arguments: tuple[str, ...],
        *,
        stdin_payload: bytes | None = None,
    ) -> str:
        stdout, _stderr = await self._run(
            identity,
            (identity.path, *arguments),
            stdin_payload=stdin_payload,
        )
        return _decode_output(stdout, command=Path(identity.path).name)

    def _all_executables(self) -> tuple[SlurmExecutableIdentityV2, ...]:
        values = self.authority.executables
        return (
            values.scontrol,
            values.sacctmgr,
            values.squeue,
            values.sbatch,
            values.scancel,
            values.sacct,
        )

    async def validate_authority(self) -> SlurmAuthorityV2:
        """Fail closed unless local identity, controller, and association are exact."""

        try:
            if os.geteuid() != self.authority.local_uid:
                raise SlurmAuthorityError("Slurm local UID does not match authority")
            for executable in self._all_executables():
                _validate_executable(executable)
            config = await self._run_text(
                self.authority.executables.scontrol,
                ("show", "config"),
            )
            cluster, controller = _controller_config_facts(config)
            if cluster != self.authority.cluster or controller != self.authority.controller_host:
                raise SlurmAuthorityError("Slurm controller or cluster authority does not match")

            association = await self._run_text(
                self.authority.executables.sacctmgr,
                (
                    "--noheader",
                    "--parsable2",
                    "show",
                    "association",
                    "where",
                    f"Cluster={self.authority.cluster}",
                    f"Account={self.authority.account}",
                    f"User={self.authority.submitter}",
                    "format=Cluster,Account,User,Partition,QOS",
                ),
            )
            rows = _lines(association, command="sacctmgr", maximum=1, allow_empty=False)
            fields = rows[0].split("|")
            expected = [
                self.authority.cluster,
                self.authority.account,
                self.authority.submitter,
                self.authority.partition,
                self.authority.qos,
            ]
            if len(fields) != 5 or fields != expected:
                raise SlurmAuthorityError("Slurm association authority does not match")
        except SlurmAuthorityError:
            raise
        except SlurmBackendError as exc:
            raise SlurmAuthorityError(f"Slurm authority validation failed: {exc}") from None
        return self.authority

    def _assert_scope(self, *, cluster: str, account: str, submitter: str) -> None:
        if (
            cluster != self.authority.cluster
            or account != self.authority.account
            or submitter != self.authority.submitter
        ):
            raise SlurmAuthorityError("scheduler value is outside exact Slurm authority")

    def _assert_launch(self, request: SlurmLaunchRequestV2) -> None:
        self._assert_scope(
            cluster=request.cluster,
            account=request.account,
            submitter=request.submitter,
        )
        if request.controller_host != self.authority.controller_host:
            raise SlurmAuthorityError("launch controller is outside Slurm authority")
        if request.partition != self.authority.partition or request.qos != self.authority.qos:
            raise SlurmAuthorityError("launch partition or QoS is outside Slurm authority")
        ceiling = self.authority.resource_ceiling
        if (
            request.cpus > ceiling.cpus
            or request.memory_bytes > ceiling.memory_bytes
            or request.gpus > ceiling.gpus
        ):
            raise SlurmAuthorityError("launch resource request exceeds Slurm authority")
        allowed_tres = {item.name: item.value for item in ceiling.generic_tres}
        if any(item.value > allowed_tres.get(item.name, 0) for item in request.generic_tres):
            raise SlurmAuthorityError("launch TRES request exceeds Slurm authority")

    def _parse_observations(self, output: str) -> tuple[SlurmJobObservationV2, ...]:
        observations: list[SlurmJobObservationV2] = []
        seen: set[str] = set()
        for line in _lines(
            output,
            command="squeue",
            maximum=_MAX_INVENTORY_RECORDS,
            allow_empty=True,
        ):
            fields = line.split("|")
            if len(fields) != 11:
                raise SlurmOutputError("squeue record has an invalid field count")
            job_id, state, submitter, account, partition = fields[:5]
            if job_id in seen:
                raise SlurmOutputError("squeue returned a duplicate job")
            seen.add(job_id)
            if submitter != self.authority.submitter or account != self.authority.account:
                raise SlurmOutputError("squeue returned a foreign association")
            if partition != self.authority.partition:
                raise SlurmOutputError("squeue returned an unknown partition")
            try:
                cpus = int(fields[5])
                nodes = tuple(fields[8].split(",")) if fields[8] else ()
                gpus, generic_tres = _gres_values(
                    fields[7],
                    allowed_generic={
                        item.name for item in self.authority.resource_ceiling.generic_tres
                    },
                )
                if state == "PENDING":
                    pending_reason = fields[9] or None
                else:
                    if fields[9] != fields[8]:
                        raise ValueError("non-pending reason field conflicts with node list")
                    pending_reason = None
                observation = SlurmJobObservationV2.model_validate(
                    {
                        "cluster": self.authority.cluster,
                        "job_id": job_id,
                        "state": state,
                        "submitter": submitter,
                        "account": account,
                        "partition": partition,
                        "cpus": cpus,
                        "memory_bytes": _memory_bytes(fields[6], cpus=cpus, nodes=len(nodes)),
                        "gpus": gpus,
                        "generic_tres": generic_tres,
                        "nodes": nodes,
                        "pending_reason": pending_reason,
                        "ownership_token": fields[10],
                    }
                )
            except (ValueError, ValidationError):
                raise SlurmOutputError("squeue state or resource record is malformed") from None
            observations.append(observation)
        return tuple(sorted(observations, key=lambda item: int(item.job_id)))

    async def inventory(self) -> tuple[SlurmJobObservationV2, ...]:
        await self.validate_authority()
        output = await self._run_text(
            self.authority.executables.squeue,
            (
                "--noheader",
                f"--clusters={self.authority.cluster}",
                f"--user={self.authority.submitter}",
                f"--account={self.authority.account}",
                "--format=%i|%T|%u|%a|%P|%C|%m|%b|%N|%R|%k",
            ),
        )
        return self._parse_observations(output)

    async def submit(self, request: SlurmLaunchRequestV2) -> SlurmSubmissionV2:
        if not isinstance(request, SlurmLaunchRequestV2):
            raise TypeError("Slurm submit requires typed SlurmLaunchRequestV2")
        await self.validate_authority()
        self._assert_launch(request)
        _validate_immutable_launcher(request.launcher, executor_uid=self.authority.local_uid)
        verifier = _execution_node_verifier(request, executor_uid=self.authority.local_uid)
        arguments = [
            "--parsable",
            f"--clusters={request.cluster}",
            f"--partition={request.partition}",
            f"--account={request.account}",
            f"--qos={request.qos}",
            f"--job-name={request.job_name}",
            f"--nodelist={','.join(request.nodes)}",
            f"--nodes={len(request.nodes)}",
            "--ntasks=1",
            f"--cpus-per-task={request.cpus}",
            f"--mem={request.memory_bytes // MEBIBYTE}M",
        ]
        typed_gpu = tuple(
            item for item in request.generic_tres if item.name.startswith("gres/gpu:")
        )
        generic_gres = tuple(
            item for item in request.generic_tres if not item.name.startswith("gres/gpu:")
        )
        if typed_gpu:
            arguments.append(
                "--gpus="
                + ",".join(
                    f"{item.name.removeprefix('gres/gpu:')}:{item.value}" for item in typed_gpu
                )
            )
        elif request.gpus:
            arguments.append(f"--gpus={request.gpus}")
        if generic_gres:
            arguments.append(
                "--gres="
                + ",".join(
                    f"{item.name.removeprefix('gres/')}:{item.value}" for item in generic_gres
                )
            )
        if request.features:
            arguments.append(f"--constraint={'&'.join(request.features)}")
        arguments.extend(
            (
                f"--time={_time_argument(request.time_limit_seconds)}",
                f"--comment={request.ownership_token}",
            )
        )
        try:
            output = await self._run_text(
                self.authority.executables.sbatch,
                tuple(arguments),
                stdin_payload=verifier,
            )
            rows = _lines(output, command="sbatch", maximum=1, allow_empty=False)
            fields = rows[0].split(";")
            if len(fields) != 2 or fields[1] != self.authority.cluster:
                raise SlurmOutputError("sbatch returned an unknown physical identity")
            return SlurmSubmissionV2(cluster=fields[1], job_id=fields[0])
        except (SlurmBackendError, ValidationError):
            raise SlurmSubmissionUncertainError(
                "sbatch began but returned no exact physical identity"
            ) from None

    async def _observe_exact(self, job_id: str) -> SlurmJobObservationV2:
        output = await self._run_text(
            self.authority.executables.squeue,
            (
                "--noheader",
                f"--clusters={self.authority.cluster}",
                f"--jobs={job_id}",
                "--format=%i|%T|%u|%a|%P|%C|%m|%b|%N|%R|%k",
            ),
        )
        observations = self._parse_observations(output)
        if len(observations) != 1 or observations[0].job_id != job_id:
            raise SlurmStateConflictError("exact pending Slurm job could not be reobserved")
        return observations[0]

    async def cancel_pending(self, request: SlurmCancelRequestV2) -> SlurmJobObservationV2:
        if not isinstance(request, SlurmCancelRequestV2):
            raise TypeError("Slurm cancellation requires typed SlurmCancelRequestV2")
        try:
            padded_token = request.ownership_token + "=" * (-len(request.ownership_token) % 4)
            if (
                base64.urlsafe_b64decode(padded_token.encode("ascii")).hex()
                != request.ownership_evidence_sha256
            ):
                raise ValueError
        except (ValueError, binascii.Error):
            raise SlurmStateConflictError(
                "pending cancellation proof digest does not match ownership token"
            ) from None
        await self.validate_authority()
        try:
            self._assert_scope(
                cluster=request.cluster,
                account=request.account,
                submitter=request.submitter,
            )
            observation = await self._observe_exact(request.job_id)
        except (SlurmAuthorityError, SlurmOutputError) as exc:
            raise SlurmStateConflictError("pending cancellation ownership is not exact") from exc
        if (
            observation.cluster != request.cluster
            or observation.submitter != request.submitter
            or observation.account != request.account
            or observation.partition != request.partition
            or observation.cpus != request.cpus
            or observation.memory_bytes != request.memory_bytes
            or observation.gpus != request.gpus
            or observation.generic_tres != request.generic_tres
            or observation.nodes != request.nodes
            or observation.ownership_token != request.ownership_token
            or observation.state != "PENDING"
        ):
            raise SlurmStateConflictError("Slurm job is not exactly owned and pending")
        try:
            output = await self._run_text(
                self.authority.executables.scancel,
                (
                    f"--clusters={request.cluster}",
                    "--state=PENDING",
                    f"--user={request.submitter}",
                    f"--account={request.account}",
                    request.job_id,
                ),
            )
        except SlurmBackendError:
            raise SlurmCancellationUncertainError(
                "scancel began but returned no exact clean result"
            ) from None
        if output:
            raise SlurmCancellationUncertainError(
                "scancel returned unexpected output and requires reobservation"
            )
        return observation

    def _parse_terminal(self, output: str) -> tuple[SlurmTerminalEvidenceV2, ...]:
        terminal: list[SlurmTerminalEvidenceV2] = []
        seen: set[str] = set()
        for line in _lines(
            output,
            command="sacct",
            maximum=MAX_ACCOUNTING_RECORDS,
            allow_empty=True,
        ):
            fields = line.split("|")
            if len(fields) != 16:
                raise SlurmOutputError("sacct terminal record has an invalid field count")
            if fields[0] in seen:
                raise SlurmOutputError("sacct returned duplicate terminal evidence")
            seen.add(fields[0])
            if (
                fields[2] != self.authority.submitter
                or fields[3] != self.authority.account
                or fields[4] != self.authority.cluster
                or fields[5] != self.authority.partition
            ):
                raise SlurmOutputError("sacct returned foreign terminal evidence")
            try:
                cpus = int(fields[11])
                nodes = tuple(fields[14].split(",")) if fields[14] else ()
                requested_memory_bytes = _memory_bytes(fields[12], cpus=cpus, nodes=len(nodes))
                gpus, generic_tres = _allocated_resources(
                    fields[13],
                    cpus=cpus,
                    nodes=len(nodes),
                    requested_memory_bytes=requested_memory_bytes,
                    allowed_generic={
                        item.name for item in self.authority.resource_ceiling.generic_tres
                    },
                )
                item = SlurmTerminalEvidenceV2.model_validate(
                    {
                        "cluster": fields[4],
                        "job_id": fields[0],
                        "state": fields[1],
                        "submitter": fields[2],
                        "account": fields[3],
                        "partition": fields[5],
                        "submitted_at": strict_datetime(fields[6]),
                        "started_at": strict_datetime(fields[7]) if fields[7] else None,
                        "ended_at": strict_datetime(fields[8]),
                        "elapsed_seconds": int(fields[9]),
                        "exit_code": fields[10],
                        "cpus": cpus,
                        "memory_bytes": requested_memory_bytes,
                        "gpus": gpus,
                        "generic_tres": generic_tres,
                        "nodes": nodes,
                        "ownership_token": fields[15],
                    }
                )
            except (ValueError, ValidationError):
                raise SlurmOutputError("sacct terminal record is malformed") from None
            terminal.append(item)
        return tuple(sorted(terminal, key=lambda item: int(item.job_id)))

    async def accounting_high_water(self, *, since: datetime) -> SlurmAccountingHighWaterV2:
        if not isinstance(since, datetime) or since.tzinfo is None or since.utcoffset() is None:
            raise ValueError("Slurm accounting lower bound must be offset-aware")
        await self.validate_authority()
        observed_through = datetime.now(UTC)
        since_utc = since.astimezone(UTC)
        if since_utc > observed_through:
            raise ValueError("Slurm accounting lower bound is in the future")
        output = await self._run_text(
            self.authority.executables.sacct,
            (
                "--noheader",
                "--parsable2",
                "--duplicates",
                "--allocations",
                f"--clusters={self.authority.cluster}",
                f"--accounts={self.authority.account}",
                f"--user={self.authority.submitter}",
                f"--starttime={since_utc.isoformat()}",
                "--state=BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,"
                "OUT_OF_MEMORY,PREEMPTED,REVOKED,TIMEOUT",
                "--format=JobIDRaw,State,User,Account,Cluster,Partition,Submit,Start,End,"
                "ElapsedRaw,"
                "ExitCode,AllocCPUS,ReqMem,AllocTRES,NodeList,Comment",
            ),
        )
        return SlurmAccountingHighWaterV2(
            cluster=self.authority.cluster,
            account=self.authority.account,
            submitter=self.authority.submitter,
            since=since_utc,
            observed_through=observed_through,
            terminal_jobs=self._parse_terminal(output),
        )


__all__ = [
    "AsyncSlurmBackend",
    "SlurmAuthorityError",
    "SlurmBackendError",
    "SlurmCancellationUncertainError",
    "SlurmCommandError",
    "SlurmOutputError",
    "SlurmStateConflictError",
    "SlurmSubmissionUncertainError",
]
