"""Read-only Slurm snapshots for the fenced global pool executors."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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

    def __post_init__(self) -> None:
        node_ids = tuple(node.node_id for node in self.nodes)
        if not node_ids:
            raise ValueError("Slurm inventory policy requires canonical nodes")
        if len(node_ids) != len({node_id.casefold() for node_id in node_ids}):
            raise ValueError("Slurm inventory policy contains a duplicate canonical node")
        if any(node.allocatable.slots <= 0 for node in self.nodes):
            raise ValueError("Slurm inventory nodes require positive allocatable slots")
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


@dataclass(frozen=True, slots=True)
class SlurmCapacityReports:
    """Shadow and executable views derived from one Slurm update."""

    controller_last_update: int
    controller_sha256: str
    pool_observation: PoolObservationV1
    executable_inventory: ExecutableExecutorInventoryV2


class ReadOnlySlurmCommandRunner(Protocol):
    """The only command capability accepted by snapshot capture."""

    async def run(self, command: tuple[str, ...]) -> bytes:
        """Return one bounded Slurm JSON document."""


class SubprocessReadOnlySlurmCommandRunner:
    """Bounded subprocess transport with no scheduler-mutation command."""

    def __init__(
        self,
        *,
        scontrol_path: str,
        squeue_path: str,
        timeout_seconds: float = 20.0,
    ) -> None:
        for path in (scontrol_path, squeue_path):
            if (
                not isinstance(path, str)
                or not Path(path).is_absolute()
                or ".." in Path(path).parts
            ):
                raise ValueError("Slurm command path must be a safe absolute path")
        if scontrol_path == squeue_path:
            raise ValueError("Slurm command paths must be distinct")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int | float)
            or not 0 < timeout_seconds <= 60
        ):
            raise ValueError("Slurm command timeout must be positive and bounded")
        self.scontrol_path = scontrol_path
        self.squeue_path = squeue_path
        self.timeout_seconds = float(timeout_seconds)

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

    async def run(self, command: tuple[str, ...]) -> bytes:
        expected = {
            (self.scontrol_path, "show", "nodes", "--json"),
            (self.squeue_path, "--json"),
        }
        if command not in expected:
            raise ValueError("Slurm subprocess runner accepts read-only inventory commands only")
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        stdout_task = asyncio.create_task(
            self._read_bounded(process.stdout, maximum=_MAX_SLURM_JSON_BYTES)
        )
        stderr_task = asyncio.create_task(self._read_bounded(process.stderr, maximum=64 * 1024))
        try:
            stdout, _stderr = await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task),
                timeout=self.timeout_seconds,
            )
            return_code = await asyncio.wait_for(process.wait(), timeout=self.timeout_seconds)
        except (TimeoutError, RuntimeError):
            if process.returncode is None:
                process.kill()
                await process.wait()
            for task in (stdout_task, stderr_task):
                task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise RuntimeError("Slurm read-only inventory command failed safely") from None
        if return_code != 0:
            raise RuntimeError("Slurm read-only inventory command failed safely")
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


def _gpu_count_from_tres(value: object, label: str) -> int:
    if not isinstance(value, str):
        raise ValueError(f"Slurm {label} must be a string")
    total = 0
    for item in value.split(","):
        key, separator, raw_count = item.partition("=")
        if not separator or (key != "gres/gpu" and not key.startswith("gres/gpu:")):
            continue
        if not raw_count.isdigit():
            raise ValueError(f"Slurm {label} GPU count is invalid")
        total += int(raw_count)
    return total


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


def _job_resources(
    job: Mapping[str, object],
    *,
    canonical_nodes: Mapping[str, str],
    relevant_partitions: frozenset[str],
    slot: ResourceVectorV1,
    state: Literal["pending", "active", "draining", "unknown"],
) -> tuple[ResourceVectorV1, tuple[str, ...]] | None:
    job_resources = _mapping(job.get("job_resources"), "job_resources")
    allocated_nodes = job_resources.get("allocated_nodes")
    if allocated_nodes is None and state == "pending":
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
        partition = job.get("partition")
        if state != "pending" or partition not in relevant_partitions:
            return None
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
        resources = ResourceVectorV1(
            cpu_millicores=cpus * 1_000,
            memory_bytes=memory_mib * 1024**2,
            gpu_count=_gpu_count_from_tres(job.get("tres_req_str"), "requested TRES"),
        )
        return resources.model_copy(update={"slots": _slots_for(resources, slot)}), ()
    cpu_count = 0
    memory_mib = 0
    for raw in allocated_nodes:
        if (
            not isinstance(raw, Mapping)
            or not isinstance(raw.get("nodename"), str)
            or raw["nodename"].casefold() not in canonical_nodes
        ):
            continue
        cpu_count += _allocated_core_count(raw)
        raw_memory = raw.get("memory_allocated")
        if type(raw_memory) is not int or raw_memory < 0:
            raise ValueError("Slurm allocated memory is invalid")
        memory_mib += raw_memory
    if all(node.casefold() in canonical_nodes for node in allocated_node_names):
        total_cpus = _wrapped_quantity(job.get("cpus"), "allocated CPUs")
        if total_cpus is None:
            raise ValueError("Slurm allocated CPU total is missing")
        cpu_count = total_cpus
    elif cpu_count == 0:
        raise ValueError("Slurm per-node CPU allocation is incomplete")
    resources = ResourceVectorV1(
        cpu_millicores=cpu_count * 1_000,
        memory_bytes=memory_mib * 1024**2,
        gpu_count=_gpu_count_from_tres(job.get("tres_alloc_str"), "allocated TRES"),
    )
    return resources.model_copy(update={"slots": _slots_for(resources, slot)}), relevant


def _job_state(value: object) -> Literal["pending", "active", "draining", "unknown"]:
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise ValueError("Slurm job state is invalid")
    states = frozenset(value)
    if "PENDING" in states:
        return "pending"
    if "COMPLETING" in states:
        return "draining"
    if states & {"CONFIGURING", "RUNNING", "SUSPENDED"}:
        return "active"
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


def build_slurm_capacity_reports(
    node_document: object,
    job_document: object,
    *,
    policy: SlurmInventoryPolicy,
    binding: SlurmReportBinding,
    source_observed_at: datetime,
) -> SlurmCapacityReports:
    """Build both manager reports from one complete controller snapshot."""

    nodes = _mapping(node_document, "node document")
    jobs = _mapping(job_document, "job document")
    _reject_controller_diagnostics(nodes)
    _reject_controller_diagnostics(jobs)
    controller_last_update = _last_update(nodes)
    if _last_update(jobs) != controller_last_update:
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
    allocatable_by_node = {node.node_id: node.allocatable for node in policy.nodes}
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
    for raw_job in raw_jobs:
        job = _mapping(raw_job, "job")
        job_id = job.get("job_id")
        if type(job_id) is not int or job_id <= 0:
            raise ValueError("Slurm job identity is invalid")
        state = _job_state(job.get("job_state"))
        parsed = _job_resources(
            job,
            canonical_nodes=canonical_nodes,
            relevant_partitions=relevant_partitions,
            slot=policy.slot_resources,
            state=state,
        )
        if parsed is None:
            continue
        resources, node_ids = parsed
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
    controller_sha256 = hashlib.sha256(
        json.dumps(
            {
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
            records=tuple(records),
        ),
    )


async def capture_slurm_capacity_reports(
    runner: ReadOnlySlurmCommandRunner,
    *,
    scontrol_path: str,
    squeue_path: str,
    policy: SlurmInventoryPolicy,
    binding: SlurmReportBinding,
    source_observed_at: datetime,
    max_attempts: int = 3,
) -> SlurmCapacityReports:
    """Capture a stable paired snapshot through two read-only commands."""

    paths = (scontrol_path, squeue_path)
    if any(
        not isinstance(path, str) or not Path(path).is_absolute() or ".." in Path(path).parts
        for path in paths
    ):
        raise ValueError("Slurm JSON command paths must be safe absolute paths")
    if scontrol_path == squeue_path:
        raise ValueError("Slurm JSON command paths must be distinct")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 10:
        raise ValueError("Slurm snapshot max_attempts must be between one and ten")

    for _attempt in range(max_attempts):
        node_bytes = await runner.run((scontrol_path, "show", "nodes", "--json"))
        job_bytes = await runner.run((squeue_path, "--json"))
        documents: list[object] = []
        for raw in (node_bytes, job_bytes):
            if not isinstance(raw, bytes) or len(raw) > _MAX_SLURM_JSON_BYTES:
                raise ValueError("Slurm JSON document exceeds its byte bound")
            try:
                documents.append(json.loads(raw.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("Slurm command returned invalid JSON") from exc
        try:
            return build_slurm_capacity_reports(
                documents[0],
                documents[1],
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
