"""Bounded argv-only Slurm observation and mutation backend."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import os
import re
import signal
import stat
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from loom_capacity_executor.slurm_contracts import (
    MAX_ACCOUNTING_RECORDS,
    MEBIBYTE,
    SlurmAccountingHighWaterV2,
    SlurmAuthorityV2,
    SlurmCancelRequestV2,
    SlurmExecutableIdentityV2,
    SlurmJobObservationV2,
    SlurmLaunchRequestV2,
    SlurmSubmissionV2,
    SlurmTerminalEvidenceV2,
    strict_datetime,
)

_MAX_EXECUTABLE_BYTES = 64 * 1024 * 1024
_MAX_INVENTORY_RECORDS = 10_000
_READ_CHUNK_BYTES = 64 * 1024
_TRUSTED_ENVIRONMENT = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin",
    "PYTHONCOERCECLOCALE": "0",
    "PYTHONUTF8": "1",
}
_MEMORY_PATTERN = re.compile(r"^([1-9][0-9]{0,18})([KMGTP])([cn]?)$")


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


def _signal_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process.pid, signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError):
        process.kill()


def _validate_executable(identity: SlurmExecutableIdentityV2) -> None:
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
    finally:
        os.close(descriptor)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) or not hmac.compare_digest(digest.hexdigest(), identity.sha256):
        raise SlurmAuthorityError("Slurm executable digest or file identity changed")


def _decode_output(payload: bytes, *, command: str) -> str:
    try:
        value = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise SlurmOutputError(f"{command} output is not UTF-8") from None
    if "\0" in value or "\r" in value:
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


def _gpu_count(value: str) -> int:
    if value in {"N/A", "(null)"}:
        return 0
    total = 0
    records = value.split(",")
    if len(records) > 64:
        raise ValueError("Slurm GRES value exceeds its bound")
    for record in records:
        fields = record.split(":")
        if len(fields) not in {2, 3} or fields[0] != "gpu":
            raise ValueError("Slurm GRES value is unknown")
        count = int(fields[-1])
        if count <= 0:
            raise ValueError("Slurm GPU count is invalid")
        total += count
    if total > 1_024:
        raise ValueError("Slurm GPU count exceeds its bound")
    return total


def _allocated_gpu_count(value: str, *, cpus: int, nodes: int) -> int:
    records = value.split(",")
    if not records or len(records) > 64:
        raise ValueError("Slurm allocated TRES value is malformed")
    parsed: dict[str, str] = {}
    for record in records:
        fields = record.split("=", maxsplit=1)
        if len(fields) != 2 or fields[0] in parsed:
            raise ValueError("Slurm allocated TRES value is malformed or duplicate")
        if fields[0] not in {"billing", "cpu", "gres/gpu", "mem", "node"}:
            raise ValueError("Slurm allocated TRES value is unknown")
        parsed[fields[0]] = fields[1]
    if not {"cpu", "mem", "node"} <= set(parsed):
        raise ValueError("Slurm allocated TRES value is incomplete")
    if int(parsed["cpu"]) != cpus or int(parsed["node"]) != nodes:
        raise ValueError("Slurm allocated TRES value conflicts with fixed fields")
    _memory_bytes(parsed["mem"], cpus=cpus, nodes=nodes)
    gpus = int(parsed.get("gres/gpu", "0"))
    if not 0 <= gpus <= 1_024:
        raise ValueError("Slurm allocated GPU value is invalid")
    return gpus


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
    ) -> tuple[bytes, bytes]:
        _validate_executable(identity)
        if (
            not argv
            or argv[0] != identity.path
            or any(
                not isinstance(argument, str) or not argument or "\0" in argument
                for argument in argv
            )
        ):
            raise SlurmAuthorityError("Slurm argv is not bound to its executable")
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=_TRUSTED_ENVIRONMENT,
                start_new_session=True,
            )
        except OSError:
            raise SlurmCommandError("Slurm command could not start") from None
        assert process.stdout is not None
        assert process.stderr is not None
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
        stdout_limit_task = asyncio.create_task(stdout_exceeded.wait())
        stderr_limit_task = asyncio.create_task(stderr_exceeded.wait())
        process_tasks = (stdout_task, stderr_task, wait_task)
        watch_tasks = (wait_task, stdout_limit_task, stderr_limit_task)
        try:
            completed, _pending = await asyncio.wait(
                watch_tasks,
                timeout=self.authority.command_timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not completed:
                _signal_process(process)
                await asyncio.gather(*process_tasks, return_exceptions=True)
                raise SlurmCommandError("Slurm command timed out")
            if stdout_limit_task in completed or stderr_limit_task in completed:
                _signal_process(process)
                await asyncio.gather(*process_tasks, return_exceptions=True)
                raise SlurmOutputError("Slurm command output exceeded its bound")
            stdout, stderr, return_code = await asyncio.gather(*process_tasks)
        except asyncio.CancelledError:
            _signal_process(process)
            await asyncio.gather(*process_tasks, return_exceptions=True)
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
    ) -> str:
        stdout, _stderr = await self._run(identity, (identity.path, *arguments))
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
            facts: dict[str, str] = {}
            for line in _lines(config, command="scontrol", maximum=2, allow_empty=False):
                parts = line.split(" = ")
                if len(parts) != 2 or parts[0] not in {"ClusterName", "SlurmctldHost"}:
                    raise SlurmOutputError("scontrol authority record is malformed or unknown")
                if parts[0] in facts:
                    raise SlurmOutputError("scontrol authority record is duplicate")
                facts[parts[0]] = parts[1]
            if facts != {
                "ClusterName": self.authority.cluster,
                "SlurmctldHost": self.authority.controller_host,
            }:
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
                        "gpus": _gpu_count(fields[7]),
                        "nodes": nodes,
                        "pending_reason": fields[9] or None,
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
        _validate_executable(request.launcher)
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
        if request.gpus:
            arguments.append(f"--gpus={request.gpus}")
        if request.generic_tres:
            arguments.append(
                "--tres-per-task="
                + ",".join(f"{item.name}:{item.value}" for item in request.generic_tres)
            )
        if request.features:
            arguments.append(f"--constraint={'&'.join(request.features)}")
        arguments.extend(
            (
                f"--time={_time_argument(request.time_limit_seconds)}",
                f"--comment={request.ownership_token}",
                *request.trusted_launcher_argv(),
            )
        )
        try:
            output = await self._run_text(
                self.authority.executables.sbatch,
                tuple(arguments),
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
            or observation.state != "PENDING"
        ):
            raise SlurmStateConflictError("Slurm job is not exactly owned and pending")
        await self._run_text(
            self.authority.executables.scancel,
            (
                f"--clusters={request.cluster}",
                "--state=PENDING",
                f"--user={request.submitter}",
                f"--account={request.account}",
                request.job_id,
            ),
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
            if len(fields) != 15:
                raise SlurmOutputError("sacct terminal record has an invalid field count")
            if fields[0] in seen:
                raise SlurmOutputError("sacct returned duplicate terminal evidence")
            seen.add(fields[0])
            if (
                fields[2] != self.authority.submitter
                or fields[3] != self.authority.account
                or fields[4] != self.authority.cluster
            ):
                raise SlurmOutputError("sacct returned foreign terminal evidence")
            try:
                cpus = int(fields[10])
                nodes = tuple(fields[13].split(",")) if fields[13] else ()
                item = SlurmTerminalEvidenceV2.model_validate(
                    {
                        "cluster": fields[4],
                        "job_id": fields[0],
                        "state": fields[1],
                        "submitter": fields[2],
                        "account": fields[3],
                        "submitted_at": strict_datetime(fields[5]),
                        "started_at": strict_datetime(fields[6]) if fields[6] else None,
                        "ended_at": strict_datetime(fields[7]),
                        "elapsed_seconds": int(fields[8]),
                        "exit_code": fields[9],
                        "cpus": cpus,
                        "memory_bytes": _memory_bytes(fields[11], cpus=cpus, nodes=len(nodes)),
                        "gpus": _allocated_gpu_count(fields[12], cpus=cpus, nodes=len(nodes)),
                        "nodes": nodes,
                        "ownership_token": fields[14],
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
                "--format=JobIDRaw,State,User,Account,Cluster,Submit,Start,End,ElapsedRaw,"
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
    "SlurmCommandError",
    "SlurmOutputError",
    "SlurmStateConflictError",
    "SlurmSubmissionUncertainError",
]
