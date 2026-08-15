"""Read-only Slurm snapshots for the fenced global pool executors."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, cast
from uuid import UUID

from loom_capacity_manager.contracts import (
    NodeEnvelopeV1,
    ObservedCommitmentV1,
    PoolObservationV1,
    ResourceVectorV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutableInventoryRecordV2,
    ExecutionContextV2,
)

_MAX_SLURM_JSON_BYTES = 8 * 1024 * 1024


class SlurmSnapshotRaceError(ValueError):
    """The controller changed between the node and queue reads."""


@dataclass(frozen=True, slots=True)
class SlurmInventoryPolicy:
    """Trusted physical boundary for one controller-local snapshot."""

    pool_id: Literal["oldlab", "gb10"]
    pool_generation: int
    reporter_incarnation: UUID
    nodes: tuple[NodeEnvelopeV1, ...]
    relevant_partitions: tuple[str, ...]
    slot_resources: ResourceVectorV1
    controller_cluster: str
    slurm_version: tuple[int, int, int]
    data_parser: str
    scontrol_sha256: str
    squeue_sha256: str
    slurm_conf_sha256: str

    def __post_init__(self) -> None:
        node_ids = tuple(node.node_id for node in self.nodes)
        if not node_ids:
            raise ValueError("Slurm inventory policy requires canonical nodes")
        if len(node_ids) != len({node_id.casefold() for node_id in node_ids}):
            raise ValueError("Slurm inventory policy contains a duplicate canonical node")
        if any(node.allocatable.slots <= 0 for node in self.nodes):
            raise ValueError("Slurm inventory nodes require positive allocatable slots")
        if any(node.allocatable.generic for node in self.nodes) or self.slot_resources.generic:
            raise ValueError("Slurm inventory does not support unobservable generic resources")
        object.__setattr__(self, "nodes", tuple(sorted(self.nodes, key=lambda node: node.node_id)))
        if type(self.pool_generation) is not int or self.pool_generation <= 0:
            raise ValueError("Slurm inventory pool generation must be positive")
        if not self.relevant_partitions or len(self.relevant_partitions) != len(
            set(self.relevant_partitions)
        ):
            raise ValueError("Slurm inventory partitions must be unique and canonical")
        object.__setattr__(self, "relevant_partitions", tuple(sorted(self.relevant_partitions)))
        if self.slot_resources.slots != 1 or not any(
            (
                self.slot_resources.cpu_millicores,
                self.slot_resources.memory_bytes,
                self.slot_resources.gpu_count,
                *self.slot_resources.generic.values(),
            )
        ):
            raise ValueError("Slurm inventory slot resources must define one physical slot")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,127}", self.controller_cluster):
            raise ValueError("Slurm controller cluster identity is invalid")
        if (
            len(self.slurm_version) != 3
            or any(type(item) is not int or item < 0 for item in self.slurm_version)
            or self.slurm_version[:2] != (23, 11)
        ):
            raise ValueError("Slurm inventory supports only reviewed Slurm 23.11 releases")
        if not re.fullmatch(r"data_parser/v[0-9]+\.[0-9]+\.[0-9]+", self.data_parser):
            raise ValueError("Slurm data parser identity is invalid")
        for digest in (self.scontrol_sha256, self.squeue_sha256, self.slurm_conf_sha256):
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("Slurm trusted file digest is invalid")


@dataclass(frozen=True, slots=True)
class SlurmReportBinding:
    """Monotonic manager and journal identity for one paired report."""

    pool_sequence: int
    inventory_sequence: int
    execution: ExecutionContextV2
    executor_id: str
    executor_incarnation: UUID
    journal_sequence: int
    journal_digest: str
    journal_checkpoint_sequence: int = 0
    journal_checkpoint_digest: str = "0" * 64


@dataclass(frozen=True, slots=True)
class SlurmCapacityReports:
    """Shadow and executable views derived from one Slurm update."""

    controller_last_update: int
    controller_sha256: str
    pool_observation: PoolObservationV1
    executable_inventory: ExecutableExecutorInventoryV2


@dataclass(frozen=True, slots=True)
class _ParsedJobResources:
    resources: ResourceVectorV1
    node_ids: tuple[str, ...]
    resources_by_node: Mapping[str, ResourceVectorV1]


class ReadOnlySlurmCommandRunner(Protocol):
    """The only command capability accepted by snapshot capture."""

    async def run(self, command: Literal["nodes", "jobs"]) -> bytes:
        """Return one bounded Slurm JSON document."""


_SCONTROL_PATH = "/usr/bin/scontrol"
_SQUEUE_PATH = "/usr/bin/squeue"
_SLURM_CONF_PATH = "/etc/loom/capacity/slurm.conf"
_SLURM_ENVIRONMENT = {
    "HOME": "/",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/bin:/bin",
    "SLURM_CONF": _SLURM_CONF_PATH,
}


def _verify_trusted_file(path: str, expected_sha256: str) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError("trusted Slurm runtime file is unavailable") from exc
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_gid != 0
            or stat.S_IMODE(before.st_mode) & 0o022
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_SLURM_JSON_BYTES
        ):
            raise RuntimeError("trusted Slurm runtime file metadata is unsafe")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(fd, 64 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(fd)
        stable_fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_size", "st_mtime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise RuntimeError("trusted Slurm runtime file changed while reading")
        if digest.hexdigest() != expected_sha256:
            raise RuntimeError("trusted Slurm runtime file digest drifted")
    finally:
        os.close(fd)


class SubprocessReadOnlySlurmCommandRunner:
    """Bounded subprocess transport with no scheduler-mutation command."""

    def __init__(
        self,
        *,
        policy: SlurmInventoryPolicy,
        timeout_seconds: float = 20.0,
    ) -> None:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int | float)
            or not 0 < timeout_seconds <= 60
        ):
            raise ValueError("Slurm command timeout must be positive and bounded")
        self._trusted_files = (
            (_SCONTROL_PATH, policy.scontrol_sha256),
            (_SQUEUE_PATH, policy.squeue_sha256),
            (_SLURM_CONF_PATH, policy.slurm_conf_sha256),
        )
        self._policy = policy
        self._verify_runtime()
        self.timeout_seconds = float(timeout_seconds)

    def _verify_runtime(self) -> None:
        for path, digest in self._trusted_files:
            _verify_trusted_file(path, digest)

    @staticmethod
    async def _read_bounded(
        stream: asyncio.StreamReader,
        *,
        maximum: int,
    ) -> bytes:
        value = bytearray()
        while True:
            chunk = await stream.read(64 * 1024)
            if not chunk:
                return bytes(value)
            value.extend(chunk)
            if len(value) > maximum:
                raise RuntimeError("Slurm command output exceeded its byte bound")

    async def run(self, command: Literal["nodes", "jobs"]) -> bytes:
        commands = {
            "nodes": (_SCONTROL_PATH, "show", "nodes", "--json"),
            "jobs": (_SQUEUE_PATH, "--json"),
        }
        argv = commands.get(command)
        if argv is None:
            raise ValueError("Slurm subprocess runner accepts read-only inventory commands only")
        self._verify_runtime()
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd="/",
            env=_SLURM_ENVIRONMENT,
            close_fds=True,
            start_new_session=True,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        stdout_task = asyncio.create_task(
            self._read_bounded(process.stdout, maximum=_MAX_SLURM_JSON_BYTES)
        )
        stderr_task = asyncio.create_task(self._read_bounded(process.stderr, maximum=64 * 1024))

        async def cleanup() -> None:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            for task in (stdout_task, stderr_task):
                task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            try:
                await asyncio.wait_for(process.wait(), timeout=min(self.timeout_seconds, 5.0))
            except TimeoutError:
                pass

        try:
            async with asyncio.timeout(self.timeout_seconds):
                stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
                return_code = await process.wait()
            if return_code != 0 or stderr:
                raise RuntimeError("Slurm read-only inventory command failed safely")
            self._verify_runtime()
        except asyncio.CancelledError:
            await asyncio.shield(cleanup())
            raise
        except (TimeoutError, RuntimeError):
            await asyncio.shield(cleanup())
            raise RuntimeError("Slurm read-only inventory command failed safely") from None
        return stdout


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Slurm {label} must be an object")
    return value


def _last_update(document: Mapping[str, object]) -> int:
    value = _mapping(document.get("last_update"), "last_update")
    number = value.get("number")
    if (
        value.get("set") is not True
        or value.get("infinite") is not False
        or type(number) is not int
        or number <= 0
    ):
        raise ValueError("Slurm last_update is invalid")
    return number


def _reject_controller_diagnostics(document: Mapping[str, object]) -> None:
    for field in ("errors", "warnings"):
        value = document.get(field)
        if not isinstance(value, list):
            raise ValueError(f"Slurm {field} must be a list")
        if value:
            raise ValueError(f"Slurm snapshot contains {field}")


def _controller_metadata(document: Mapping[str, object]) -> tuple[str, tuple[int, int, int], str]:
    meta = _mapping(document.get("meta"), "metadata")
    slurm = _mapping(meta.get("slurm"), "Slurm metadata")
    cluster = slurm.get("cluster")
    version = _mapping(slurm.get("version"), "Slurm version")
    raw_version = (version.get("major"), version.get("minor"), version.get("micro"))
    plugin = _mapping(meta.get("plugin"), "plugin metadata")
    data_parser = plugin.get("data_parser")
    if (
        not isinstance(cluster, str)
        or not isinstance(data_parser, str)
        or not all(isinstance(item, str) and item.isdigit() for item in raw_version)
    ):
        raise ValueError("Slurm controller metadata is invalid")
    return (
        cluster,
        (
            int(cast(str, raw_version[0])),
            int(cast(str, raw_version[1])),
            int(cast(str, raw_version[2])),
        ),
        data_parser,
    )


def _queue_allocation_digest(document: object) -> str:
    queue = _mapping(document, "job document")
    _reject_controller_diagnostics(queue)
    _last_update(queue)
    raw_jobs = queue.get("jobs")
    if not isinstance(raw_jobs, list):
        raise ValueError("Slurm jobs must be a list")
    allocation_fields = (
        "job_id",
        "job_state",
        "partition",
        "cpus",
        "node_count",
        "memory_per_node",
        "memory_per_cpu",
        "tres_alloc_str",
        "tres_req_str",
        "tres_per_job",
        "tres_per_node",
        "tres_per_socket",
        "tres_per_task",
        "sockets_per_node",
        "tasks",
        "array_task_id",
        "array_task_string",
        "array_max_tasks",
        "gres_detail",
        "job_resources",
    )
    projected: list[dict[str, object]] = []
    for raw_job in raw_jobs:
        job = _mapping(raw_job, "job")
        projected.append({field: job.get(field) for field in allocation_fields})
    projected.sort(
        key=lambda item: json.dumps(
            item,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    )
    return hashlib.sha256(
        json.dumps(
            {"controller": _controller_metadata(queue), "jobs": projected},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _allocated_core_count(node: Mapping[str, object]) -> int:
    sockets = node.get("sockets")
    if not isinstance(sockets, Mapping):
        return 0
    return sum(
        1
        for socket in sockets.values()
        if isinstance(socket, Mapping)
        for cores in (socket.get("cores"),)
        if isinstance(cores, Mapping)
        for state in cores.values()
        if state == "allocated"
    )


def _wrapped_quantity(value: object, label: str) -> int | None:
    wrapped = _mapping(value, label)
    number = wrapped.get("number")
    if wrapped.get("infinite") is not False or type(number) is not int or number < 0:
        raise ValueError(f"Slurm {label} is invalid")
    if wrapped.get("set") is False:
        return None
    if wrapped.get("set") is not True:
        raise ValueError(f"Slurm {label} is invalid")
    return number


def _gpu_count_from_gres(value: object, label: str) -> int:
    if not isinstance(value, str):
        raise ValueError(f"Slurm {label} must be a string")
    total = 0
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        without_indexes = item.split("(", 1)[0]
        if "=" in without_indexes:
            key, raw_count = without_indexes.rsplit("=", 1)
            if key != "gres/gpu" and not key.startswith("gres/gpu:"):
                continue
        else:
            parts = without_indexes.split(":")
            if parts[0] not in {"gpu", "gres/gpu"} or len(parts) < 2:
                continue
            raw_count = parts[-1]
        if not raw_count.isdigit():
            raise ValueError(f"Slurm {label} GPU count is invalid")
        total += int(raw_count)
    return total


def _gpu_count_from_details(value: object, label: str) -> tuple[int, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"Slurm {label} must be a list of strings")
    return tuple(_gpu_count_from_gres(item, label) for item in value)


def _optional_text(value: object, label: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"Slurm {label} must be a string")
    return value


def _requested_gpu_count(job: Mapping[str, object], node_count: int) -> int:
    per_job = _gpu_count_from_gres(
        _optional_text(job.get("tres_per_job"), "TRES per job"),
        "TRES per job",
    )
    per_node = _gpu_count_from_gres(
        _optional_text(job.get("tres_per_node"), "TRES per node"),
        "TRES per node",
    )
    per_socket = _gpu_count_from_gres(
        _optional_text(job.get("tres_per_socket"), "TRES per socket"),
        "TRES per socket",
    )
    per_task = _gpu_count_from_gres(
        _optional_text(job.get("tres_per_task"), "TRES per task"),
        "TRES per task",
    )
    socket_count = 0
    if per_socket:
        sockets_per_node = _wrapped_quantity(job.get("sockets_per_node"), "sockets per node")
        if sockets_per_node is None or sockets_per_node < 1:
            raise ValueError("Slurm GPU per-socket request has no bounded socket count")
        socket_count = sockets_per_node * node_count
    task_count = 0
    if per_task:
        tasks = _wrapped_quantity(job.get("tasks"), "task count")
        if tasks is None or tasks < 1:
            raise ValueError("Slurm GPU per-task request has no bounded task count")
        task_count = tasks
    fallback = _gpu_count_from_gres(
        _optional_text(job.get("tres_req_str"), "requested TRES"),
        "requested TRES",
    )
    explicit = per_job + per_node * node_count + per_socket * socket_count + per_task * task_count
    return max(explicit, fallback)


def _memory_mib_from_tres(value: object, label: str) -> int:
    text = _optional_text(value, label)
    for item in text.split(","):
        key, separator, raw = item.partition("=")
        if key != "mem" or not separator:
            continue
        if not raw:
            raise ValueError(f"Slurm {label} memory is invalid")
        suffix = raw[-1]
        number_text = raw[:-1] if suffix.isalpha() else raw
        if not number_text.isdigit():
            raise ValueError(f"Slurm {label} memory is invalid")
        number = int(number_text)
        factors = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024**2}
        if suffix.isalpha():
            factor = factors.get(suffix.upper())
            if factor is None:
                raise ValueError(f"Slurm {label} memory unit is invalid")
            return math.ceil(number * factor)
        return math.ceil(number / 1024**2)
    return 0


def _is_compact_array(job: Mapping[str, object]) -> bool:
    task_string = _optional_text(job.get("array_task_string"), "array task string")
    if not task_string:
        return False
    task_id = _mapping(job.get("array_task_id"), "array task identity")
    return task_id.get("set") is False


def _slots_for(resources: ResourceVectorV1, slot: ResourceVectorV1) -> int:
    values = [1]
    for used, per_slot in (
        (resources.cpu_millicores, slot.cpu_millicores),
        (resources.memory_bytes, slot.memory_bytes),
        (resources.gpu_count, slot.gpu_count),
    ):
        if per_slot > 0:
            values.append(math.ceil(used / per_slot))
    return max(values)


def _pool_resources(nodes: tuple[NodeEnvelopeV1, ...]) -> ResourceVectorV1:
    generic_keys = {key for node in nodes for key in node.allocatable.generic}
    return ResourceVectorV1(
        slots=sum(node.allocatable.slots for node in nodes),
        cpu_millicores=sum(node.allocatable.cpu_millicores for node in nodes),
        memory_bytes=sum(node.allocatable.memory_bytes for node in nodes),
        gpu_count=sum(node.allocatable.gpu_count for node in nodes),
        generic={
            key: sum(node.allocatable.generic.get(key, 0) for node in nodes)
            for key in generic_keys
        },
    )


def _job_resources(
    job: Mapping[str, object],
    *,
    canonical_nodes: Mapping[str, str],
    relevant_partitions: frozenset[str],
    slot: ResourceVectorV1,
    pool_resources: ResourceVectorV1,
    state: Literal["pending", "active", "draining", "terminal", "unknown"],
) -> _ParsedJobResources | None:
    job_resources = _mapping(job.get("job_resources"), "job_resources")
    allocated_nodes = job_resources.get("allocated_nodes")
    if allocated_nodes is None and state in {"pending", "active", "unknown"}:
        allocated_nodes = []
    if not isinstance(allocated_nodes, list):
        raise ValueError("Slurm allocated_nodes must be a list")
    allocated_node_names_list: list[str] = []
    for raw in allocated_nodes:
        if not isinstance(raw, Mapping):
            raise ValueError("Slurm allocated node identities are invalid")
        node_name = raw.get("nodename")
        if not isinstance(node_name, str):
            raise ValueError("Slurm allocated node identities are invalid")
        allocated_node_names_list.append(node_name)
    allocated_node_names = tuple(allocated_node_names_list)
    if len(allocated_node_names) != len(set(allocated_node_names)):
        raise ValueError("Slurm allocated node identities are invalid")
    relevant = tuple(
        sorted(
            canonical_nodes[node.casefold()]
            for node in allocated_node_names
            if node.casefold() in canonical_nodes
        )
    )
    if not relevant:
        if allocated_node_names:
            return None
        partition = job.get("partition")
        if not isinstance(partition, str):
            raise ValueError("Slurm node-less job partition is invalid")
        if state == "terminal" or partition not in relevant_partitions:
            return None
        if _is_compact_array(job) or state == "unknown":
            return _ParsedJobResources(pool_resources, (), {})
        cpus = _wrapped_quantity(job.get("cpus"), "requested CPUs")
        node_count = _wrapped_quantity(job.get("node_count"), "requested node count")
        if cpus is None or node_count is None or node_count < 1:
            raise ValueError("Slurm pending job resource request is incomplete")
        memory_per_node = _wrapped_quantity(job.get("memory_per_node"), "requested memory per node")
        memory_per_cpu = _wrapped_quantity(job.get("memory_per_cpu"), "requested memory per CPU")
        if memory_per_node is not None:
            memory_mib = memory_per_node * node_count
        elif memory_per_cpu is not None:
            memory_mib = memory_per_cpu * cpus
        else:
            memory_mib = 0
        memory_mib = max(
            memory_mib,
            _memory_mib_from_tres(job.get("tres_req_str"), "requested TRES"),
        )
        resources = ResourceVectorV1(
            cpu_millicores=cpus * 1_000,
            memory_bytes=memory_mib * 1024**2,
            gpu_count=_requested_gpu_count(job, node_count),
        )
        return _ParsedJobResources(
            resources.model_copy(update={"slots": _slots_for(resources, slot)}),
            (),
            {},
        )
    cpu_count = 0
    memory_mib = 0
    detail_counts = _gpu_count_from_details(job.get("gres_detail", []), "allocated GRES")
    if detail_counts and len(detail_counts) != len(allocated_nodes):
        raise ValueError("Slurm allocated GRES does not align with allocated nodes")
    resources_by_node: dict[str, ResourceVectorV1] = {}
    for index, raw in enumerate(allocated_nodes):
        if (
            not isinstance(raw, Mapping)
            or not isinstance(raw.get("nodename"), str)
            or raw["nodename"].casefold() not in canonical_nodes
        ):
            continue
        canonical_id = canonical_nodes[raw["nodename"].casefold()]
        node_cpus = _allocated_core_count(raw)
        cpu_count += node_cpus
        raw_memory = raw.get("memory_allocated")
        if type(raw_memory) is not int or raw_memory < 0:
            raise ValueError("Slurm allocated memory is invalid")
        memory_mib += raw_memory
        node_gpu_count = detail_counts[index] if detail_counts else 0
        node_resources = ResourceVectorV1(
            cpu_millicores=node_cpus * 1_000,
            memory_bytes=raw_memory * 1024**2,
            gpu_count=node_gpu_count,
        )
        resources_by_node[canonical_id] = node_resources.model_copy(
            update={"slots": _slots_for(node_resources, slot)}
        )
    if all(node.casefold() in canonical_nodes for node in allocated_node_names):
        total_cpus = _wrapped_quantity(job.get("cpus"), "allocated CPUs")
        if total_cpus is None:
            raise ValueError("Slurm allocated CPU total is missing")
        cpu_count = total_cpus
    elif cpu_count == 0:
        raise ValueError("Slurm per-node CPU allocation is incomplete")
    gpu_count = (
        sum(detail_counts)
        if detail_counts
        else _gpu_count_from_gres(
            _optional_text(job.get("tres_alloc_str"), "allocated TRES"),
            "allocated TRES",
        )
    )
    if not detail_counts and len(relevant) == 1 and len(relevant) == len(allocated_node_names):
        node_id = relevant[0]
        node_resources = resources_by_node[node_id]
        resources_by_node[node_id] = node_resources.model_copy(update={"gpu_count": gpu_count})
    resources = ResourceVectorV1(
        cpu_millicores=cpu_count * 1_000,
        memory_bytes=memory_mib * 1024**2,
        gpu_count=gpu_count,
    )
    return _ParsedJobResources(
        resources.model_copy(update={"slots": _slots_for(resources, slot)}),
        relevant,
        resources_by_node,
    )


def _job_state(
    value: object,
) -> Literal["pending", "active", "draining", "terminal", "unknown"]:
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise ValueError("Slurm job state is invalid")
    states = frozenset(value)
    if "PENDING" in states:
        return "pending"
    if "COMPLETING" in states:
        return "draining"
    if states & {"CONFIGURING", "RUNNING", "SUSPENDED"}:
        return "active"
    if states & {"BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "TIMEOUT"}:
        return "terminal"
    return "unknown"


def _node_states(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise ValueError("Slurm node state is invalid")
    if len(value) != len(set(value)):
        raise ValueError("Slurm node state is ambiguous")
    return tuple(sorted(value))


def _node_partitions(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise ValueError("Slurm node partitions are invalid")
    if len(value) != len(set(value)):
        raise ValueError("Slurm node partitions are ambiguous")
    return tuple(sorted(value))


def _node_is_schedulable(value: object) -> bool:
    return frozenset(_node_states(value)) <= {"IDLE", "MIXED", "ALLOCATED", "COMPLETING"}


def _validate_node_envelope(
    node: Mapping[str, object],
    *,
    expected: ResourceVectorV1,
    required_partitions: frozenset[str],
) -> None:
    expected_cpu = expected.cpu_millicores // 1_000
    expected_memory = expected.memory_bytes // 1024**2
    if (
        expected.cpu_millicores % 1_000
        or expected.memory_bytes % 1024**2
        or type(node.get("cpus")) is not int
        or type(node.get("effective_cpus")) is not int
        or type(node.get("real_memory")) is not int
        or node["cpus"] != expected_cpu
        or node["effective_cpus"] != expected_cpu
        or node["real_memory"] != expected_memory
        or _gpu_count_from_gres(node.get("gres"), "node GRES") != expected.gpu_count
    ):
        raise ValueError("Slurm node resource envelope drifted")
    if not required_partitions <= frozenset(_node_partitions(node.get("partitions"))):
        raise ValueError("Slurm canonical node omits a required partition")


def _node_allocation_resources(
    node: Mapping[str, object],
    *,
    allocatable: ResourceVectorV1,
    slot: ResourceVectorV1,
) -> ResourceVectorV1:
    cpus = node.get("alloc_cpus")
    memory_mib = node.get("alloc_memory")
    if type(cpus) is not int or cpus < 0 or type(memory_mib) is not int or memory_mib < 0:
        raise ValueError("Slurm node allocation counters are invalid")
    gpu_count = _gpu_count_from_gres(node.get("gres_used"), "node used GRES")
    resources = ResourceVectorV1(
        cpu_millicores=cpus * 1_000,
        memory_bytes=memory_mib * 1024**2,
        gpu_count=gpu_count,
    )
    if (
        resources.cpu_millicores > allocatable.cpu_millicores
        or resources.memory_bytes > allocatable.memory_bytes
        or resources.gpu_count > allocatable.gpu_count
    ):
        raise ValueError("Slurm node allocation exceeds its trusted resource envelope")
    if not any((resources.cpu_millicores, resources.memory_bytes, resources.gpu_count)):
        return resources
    return resources.model_copy(update={"slots": _slots_for(resources, slot)})


def _add_visible_allocation(
    current: ResourceVectorV1,
    addition: ResourceVectorV1,
) -> ResourceVectorV1:
    return ResourceVectorV1(
        slots=current.slots + addition.slots,
        cpu_millicores=current.cpu_millicores + addition.cpu_millicores,
        memory_bytes=current.memory_bytes + addition.memory_bytes,
        gpu_count=current.gpu_count + addition.gpu_count,
    )


def _residual_allocation(
    observed: ResourceVectorV1,
    visible: ResourceVectorV1,
    *,
    slot: ResourceVectorV1,
) -> ResourceVectorV1 | None:
    residual = ResourceVectorV1(
        cpu_millicores=max(observed.cpu_millicores - visible.cpu_millicores, 0),
        memory_bytes=max(observed.memory_bytes - visible.memory_bytes, 0),
        gpu_count=max(observed.gpu_count - visible.gpu_count, 0),
    )
    if not any((residual.cpu_millicores, residual.memory_bytes, residual.gpu_count)):
        return None
    return residual.model_copy(update={"slots": _slots_for(residual, slot)})


def build_slurm_capacity_reports(
    node_document: object,
    job_document_before: object,
    job_document_after: object,
    *,
    policy: SlurmInventoryPolicy,
    binding: SlurmReportBinding,
    source_observed_at: datetime,
) -> SlurmCapacityReports:
    """Build both manager reports from one complete controller snapshot."""

    nodes = _mapping(node_document, "node document")
    jobs_before = _mapping(job_document_before, "job document")
    jobs = _mapping(job_document_after, "job document")
    _reject_controller_diagnostics(nodes)
    _reject_controller_diagnostics(jobs_before)
    _reject_controller_diagnostics(jobs)
    expected_controller = (
        policy.controller_cluster,
        policy.slurm_version,
        policy.data_parser,
    )
    if (
        _controller_metadata(nodes) != expected_controller
        or _controller_metadata(jobs_before) != expected_controller
        or _controller_metadata(jobs) != expected_controller
    ):
        raise ValueError("Slurm controller metadata does not match protected policy")
    controller_last_update = _last_update(nodes)
    if (
        _last_update(jobs_before) != controller_last_update
        or _last_update(jobs) != controller_last_update
        or _queue_allocation_digest(job_document_before)
        != _queue_allocation_digest(job_document_after)
    ):
        raise SlurmSnapshotRaceError("Slurm node and job snapshots have different update epochs")
    allowed_nodes = frozenset(node.node_id for node in policy.nodes)
    raw_nodes = nodes.get("nodes")
    if not isinstance(raw_nodes, list):
        raise ValueError("Slurm nodes must be a list")
    canonical_nodes = {node_id.casefold(): node_id for node_id in allowed_nodes}
    raw_nodes_by_folded_name: dict[str, Mapping[str, object]] = {}
    for raw in raw_nodes:
        if not isinstance(raw, Mapping) or not isinstance(raw.get("name"), str):
            continue
        folded_name = raw["name"].casefold()
        if folded_name in raw_nodes_by_folded_name:
            raise ValueError("Slurm node snapshot contains a duplicate node identity")
        raw_nodes_by_folded_name[folded_name] = raw
    if not canonical_nodes.keys() <= raw_nodes_by_folded_name.keys():
        raise ValueError("Slurm node snapshot is incomplete")
    observed_nodes = {
        canonical_id: raw_nodes_by_folded_name[folded_name]
        for folded_name, canonical_id in canonical_nodes.items()
    }
    allocatable_by_node = {node.node_id: node.allocatable for node in policy.nodes}
    required_partitions = frozenset(policy.relevant_partitions)
    for node_id, raw_node in observed_nodes.items():
        _validate_node_envelope(
            raw_node,
            expected=allocatable_by_node[node_id],
            required_partitions=required_partitions,
        )
    relevant_partitions = frozenset(
        partition
        for node_id in allowed_nodes
        for partition in _node_partitions(observed_nodes[node_id].get("partitions"))
    )
    if not set(policy.relevant_partitions) <= relevant_partitions:
        raise ValueError("Slurm node snapshot omits a required partition")
    raw_jobs = jobs.get("jobs")
    if not isinstance(raw_jobs, list):
        raise ValueError("Slurm jobs must be a list")
    commitments: list[ObservedCommitmentV1] = []
    records: list[ExecutableInventoryRecordV2] = []
    evidence_payload: list[dict[str, object]] = []
    visible_by_node = {
        node_id: ResourceVectorV1()
        for node_id in allowed_nodes
    }
    total_pool_resources = _pool_resources(policy.nodes)
    node_evidence = [
        {
            "node_id": node_id,
            "state": _node_states(observed_nodes[node_id].get("state")),
            "partitions": _node_partitions(observed_nodes[node_id].get("partitions")),
            "cpus": observed_nodes[node_id].get("cpus"),
            "effective_cpus": observed_nodes[node_id].get("effective_cpus"),
            "real_memory": observed_nodes[node_id].get("real_memory"),
            "alloc_cpus": observed_nodes[node_id].get("alloc_cpus"),
            "alloc_memory": observed_nodes[node_id].get("alloc_memory"),
            "gres": observed_nodes[node_id].get("gres"),
            "gres_used": observed_nodes[node_id].get("gres_used"),
            "tres": observed_nodes[node_id].get("tres"),
            "tres_used": observed_nodes[node_id].get("tres_used"),
        }
        for node_id in sorted(allowed_nodes)
    ]
    for node_id in sorted(allowed_nodes):
        if _node_is_schedulable(observed_nodes[node_id].get("state")):
            continue
        physical_identity = f"slurm-node-{node_id}-unavailable"
        resources = allocatable_by_node[node_id]
        evidence_payload.append(
            {
                "physical_kind": "worker",
                "physical_identity": physical_identity,
                "node_ids": (node_id,),
                "resources": resources.model_dump(mode="json"),
                "state": "unknown",
            }
        )
        commitments.append(
            ObservedCommitmentV1(
                kind="physical",
                commitment_id=physical_identity,
                physical_identity=physical_identity,
                pool_id=policy.pool_id,
                pool_generation=policy.pool_generation,
                resources=resources,
                state="quarantined",
                node_ids=(node_id,),
            )
        )
    jobs_by_id: dict[int, Mapping[str, object]] = {}
    for raw_job in raw_jobs:
        job = _mapping(raw_job, "job")
        job_id = job.get("job_id")
        if type(job_id) is not int or job_id <= 0:
            raise ValueError("Slurm job identity is invalid")
        if job_id in jobs_by_id:
            raise ValueError("Slurm queue contains a duplicate job identity")
        jobs_by_id[job_id] = job
    for job_id in sorted(jobs_by_id):
        job = jobs_by_id[job_id]
        state = _job_state(job.get("job_state"))
        parsed = _job_resources(
            job,
            canonical_nodes=canonical_nodes,
            relevant_partitions=relevant_partitions,
            slot=policy.slot_resources,
            pool_resources=total_pool_resources,
            state=state,
        )
        if parsed is None:
            continue
        resources = parsed.resources
        node_ids = parsed.node_ids
        for node_id, node_resources in parsed.resources_by_node.items():
            visible_by_node[node_id] = _add_visible_allocation(
                visible_by_node[node_id],
                node_resources,
            )
        physical_identity = f"slurm-job-{job_id}"
        evidence_payload.append(
            {
                "physical_kind": "slurm-job",
                "physical_identity": physical_identity,
                "job_id": job_id,
                "node_ids": node_ids,
                "resources": resources.model_dump(mode="json"),
                "state": state,
            }
        )
        commitments.append(
            ObservedCommitmentV1(
                kind="physical",
                commitment_id=physical_identity,
                physical_identity=physical_identity,
                pool_id=policy.pool_id,
                pool_generation=policy.pool_generation,
                resources=resources,
                state="quarantined",
                node_ids=node_ids,
            )
        )
    busy_states = frozenset({"MIXED", "ALLOCATED", "COMPLETING"})
    for node_id in sorted(allowed_nodes):
        raw_node = observed_nodes[node_id]
        if not _node_is_schedulable(raw_node.get("state")):
            continue
        observed_allocation = _node_allocation_resources(
            raw_node,
            allocatable=allocatable_by_node[node_id],
            slot=policy.slot_resources,
        )
        residual = _residual_allocation(
            observed_allocation,
            visible_by_node[node_id],
            slot=policy.slot_resources,
        )
        if (
            residual is None
            and busy_states & frozenset(_node_states(raw_node.get("state")))
            and not any(
                (
                    observed_allocation.cpu_millicores,
                    observed_allocation.memory_bytes,
                    observed_allocation.gpu_count,
                    visible_by_node[node_id].cpu_millicores,
                    visible_by_node[node_id].memory_bytes,
                    visible_by_node[node_id].gpu_count,
                )
            )
        ):
            residual = allocatable_by_node[node_id]
        if residual is None:
            continue
        physical_identity = f"slurm-node-{node_id}-hidden-allocation"
        evidence_payload.append(
            {
                "physical_kind": "worker",
                "physical_identity": physical_identity,
                "node_ids": (node_id,),
                "resources": residual.model_dump(mode="json"),
                "state": "unknown",
            }
        )
        commitments.append(
            ObservedCommitmentV1(
                kind="physical",
                commitment_id=physical_identity,
                physical_identity=physical_identity,
                pool_id=policy.pool_id,
                pool_generation=policy.pool_generation,
                resources=residual,
                state="quarantined",
                node_ids=(node_id,),
            )
        )
    controller_sha256 = hashlib.sha256(
        json.dumps(
            {
                "controller": {
                    "cluster": policy.controller_cluster,
                    "slurm_version": policy.slurm_version,
                    "data_parser": policy.data_parser,
                    "scontrol_sha256": policy.scontrol_sha256,
                    "squeue_sha256": policy.squeue_sha256,
                    "slurm_conf_sha256": policy.slurm_conf_sha256,
                    "queue_allocation_sha256": _queue_allocation_digest(job_document_after),
                },
                "last_update": controller_last_update,
                "nodes": node_evidence,
                "records": evidence_payload,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()
    for commitment, evidence in zip(commitments, evidence_payload, strict=True):
        records.append(
            ExecutableInventoryRecordV2(
                physical_identity=commitment.physical_identity,
                physical_kind=cast(Literal["slurm-job", "worker"], evidence["physical_kind"]),
                authority_scope="foreign",
                state=cast(
                    Literal["pending", "active", "draining", "terminal", "unknown"],
                    evidence["state"],
                ),
                resources=commitment.resources,
                node_ids=commitment.node_ids,
                controller_evidence_sha256=controller_sha256,
                terminal_evidence_sha256=(
                    controller_sha256 if evidence["state"] == "terminal" else None
                ),
            )
        )
    return SlurmCapacityReports(
        controller_last_update=controller_last_update,
        controller_sha256=controller_sha256,
        pool_observation=PoolObservationV1(
            pool_id=policy.pool_id,
            pool_generation=policy.pool_generation,
            reporter_incarnation=policy.reporter_incarnation,
            sequence=binding.pool_sequence,
            source_observed_at=source_observed_at,
            health="eligible",
            commitments=tuple(commitments),
        ),
        executable_inventory=ExecutableExecutorInventoryV2(
            execution=binding.execution,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=policy.pool_id,
            pool_generation=policy.pool_generation,
            inventory_sequence=binding.inventory_sequence,
            journal_sequence=binding.journal_sequence,
            journal_digest=binding.journal_digest,
            journal_checkpoint_sequence=binding.journal_checkpoint_sequence,
            journal_checkpoint_digest=binding.journal_checkpoint_digest,
            records=tuple(records),
        ),
    )


async def capture_slurm_capacity_reports(
    runner: ReadOnlySlurmCommandRunner,
    *,
    policy: SlurmInventoryPolicy,
    binding: SlurmReportBinding,
    source_observed_at: datetime,
    max_attempts: int = 3,
) -> SlurmCapacityReports:
    """Capture a stable paired snapshot through two read-only commands."""
    if isinstance(runner, SubprocessReadOnlySlurmCommandRunner) and runner._policy != policy:
        raise ValueError("Slurm subprocess runner policy binding does not match capture policy")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 10:
        raise ValueError("Slurm snapshot max_attempts must be between one and ten")

    for _attempt in range(max_attempts):
        job_before_bytes = await runner.run("jobs")
        node_bytes = await runner.run("nodes")
        job_after_bytes = await runner.run("jobs")
        documents: list[object] = []
        for raw in (job_before_bytes, node_bytes, job_after_bytes):
            if not isinstance(raw, bytes) or len(raw) > _MAX_SLURM_JSON_BYTES:
                raise ValueError("Slurm JSON document exceeds its byte bound")
            try:
                documents.append(json.loads(raw.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("Slurm command returned invalid JSON") from exc
        try:
            job_before, nodes, job_after = documents
            return build_slurm_capacity_reports(
                nodes,
                job_before,
                job_after,
                policy=policy,
                binding=binding,
                source_observed_at=source_observed_at,
            )
        except SlurmSnapshotRaceError:
            continue
    raise SlurmSnapshotRaceError("Slurm snapshot did not stabilize within its retry bound")


__all__ = [
    "SlurmCapacityReports",
    "SlurmInventoryPolicy",
    "SlurmReportBinding",
    "SlurmSnapshotRaceError",
    "SubprocessReadOnlySlurmCommandRunner",
    "build_slurm_capacity_reports",
    "capture_slurm_capacity_reports",
]
