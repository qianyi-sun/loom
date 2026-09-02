"""Elastic Slurm worker pool controller.

The controller is intentionally out of the submit path. It periodically
observes queued Loom trials and Slurm worker job records, then submits or
cancels Slurm jobs through a bounded background loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import SlurmWorkerJob, Trial
from loom.worker_token import (
    WORKER_AUTH_FINGERPRINT_ENV_KEY,
    worker_token_fingerprint_from_env_file,
)
from loom_control_plane.slurm_worker_jobs import (
    ACTIVE_STATES,
    SlurmWorkerJobObservation,
    reconcile_slurm_worker_jobs,
    record_slurm_worker_job,
    slurm_cluster_for_pool,
)

logger = logging.getLogger(__name__)

_QUEUE_ACTIVE_STATES = ("claimed", "running")
_QUEUE_READY_STMT = (
    select(func.count())
    .select_from(Trial)
    .where(
        Trial.state == "queued",
        or_(Trial.next_attempt_at.is_(None), Trial.next_attempt_at <= func.now()),
    )
)
_QUEUE_RUNNING_STMT = (
    select(func.count())
    .select_from(Trial)
    .where(
        Trial.state.in_(_QUEUE_ACTIVE_STATES),
    )
)
_SAFE_JOB_NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")
_SAFE_SANDBOX_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")
_CANDIDATE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_GPU_TRES_RE = re.compile(r"^gpu(?::[A-Za-z0-9_.-]+)?:(?P<count>[1-9][0-9]*)$")
_SAFE_OUTPUT_DIR_RE = re.compile(r"^/[A-Za-z0-9._/-]+$")


@dataclass(frozen=True)
class ElasticSlurmWorkerControllerConfig:
    environment: str
    pool_name: str
    allowed_nodes: tuple[str, ...]
    env_file: str
    repo_dir: str
    partition: str
    time_limit: str
    requested_cpus: int
    requested_memory_mib: int
    requested_concurrency: int
    max_jobs: int
    pending_job_cap: int
    min_queued_trials: int
    stale_after_seconds: int
    sbatch_path: str
    squeue_path: str
    sacct_path: str
    scancel_path: str
    command_timeout_seconds: float
    exclusive: bool = False
    slurm_account: str = ""
    slurm_qos: str = ""
    slurm_reservation: str = ""
    sinfo_path: str = "sinfo"
    srun_path: str = "srun"
    resource_aware: bool = False
    probe_mem_available: bool = False
    cpu_per_slot: int = 2
    memory_mib_per_slot: int = 8192
    reserved_cpus: int = 4
    reserved_memory_mib: int = 24_576
    max_concurrency_per_node: int = 8
    max_cpu_load_ratio: float = 1.0
    # #896 per-container hard caps for non-exclusive (packed) workers. Slurm
    # admission requires positive values. They are exported into the worker env so
    # the compose worker + trial/sidecar containers get an absolute CPU/mem/pids
    # ceiling, bounding an escaped container's contention on a double-duty node.
    container_cpus: float = 0.0
    container_memory_mib: int = 0
    container_pids: int = 0
    # Aggregate pids.max applied by the administrator-owned root guard to the
    # non-exclusive job cgroup. 0 is allowed only for exclusive jobs.
    job_pids_max: int = 0
    candidate_sha: str = ""
    gpu_tres: str = ""
    requested_gpus: int = 0
    job_output_dir: str = ""


@dataclass(frozen=True)
class SlurmNodeResource:
    hostname: str
    state: str
    cpus_total: int
    free_memory_mib: int
    cpu_load: float | None
    idle_cpus: int | None = None
    available_memory_mib: int | None = None
    total_memory_mib: int | None = None
    schedulable_memory_mib: int | None = None


@dataclass(frozen=True)
class SlurmNodeCapacityPlan:
    hostname: str
    safe_slots: int
    reason: str


@dataclass(frozen=True)
class SlurmWorkerCapacitySnapshot:
    queued_trials: int
    running_trials: int
    pending_jobs: int
    running_jobs: int
    active_slots: int
    pending_slots: int
    active_nodes: set[str]
    cancellable_pending_job_ids: tuple[str, ...]
    active_job_ids: tuple[str, ...]
    node_resources: Mapping[str, SlurmNodeResource] | None = None


@dataclass(frozen=True)
class SlurmWorkerControllerDecision:
    submit_nodes: tuple[str, ...]
    cancel_job_ids: tuple[str, ...]
    reason: str
    node_capacity: Mapping[str, SlurmNodeCapacityPlan] = field(default_factory=dict)


@dataclass(frozen=True)
class SbatchRequest:
    args: tuple[str, ...]
    stdin: str


@dataclass(frozen=True)
class SlurmWorkerControllerTickResult:
    submitted_job_ids: tuple[str, ...]
    cancelled_job_ids: tuple[str, ...]
    decision: SlurmWorkerControllerDecision


class SlurmWorkerCommandRunner(Protocol):
    async def query_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> list[SlurmWorkerJobObservation]:
        """Return current Slurm observations for known active job IDs."""

    async def submit_worker(
        self,
        *,
        node: str,
        config: ElasticSlurmWorkerControllerConfig,
    ) -> str:
        """Submit one worker job and return the Slurm job ID."""

    async def cancel_job(self, job_id: str) -> None:
        """Cancel one Slurm job without a state condition."""

    async def cancel_pending_job(self, job_id: str) -> None:
        """Cancel a job only if Slurm still considers it pending."""

    async def query_node_resources(
        self,
        nodes: tuple[str, ...],
    ) -> dict[str, SlurmNodeResource]:
        """Return live Slurm resource observations for allowed nodes."""


def _require_nonempty(value: str, name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{name} is required when Slurm worker controller is enabled")
    return value


def _require_positive(value: int | float, name: str) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _validate_job_pids_contract(
    *,
    exclusive: bool,
    job_pids_max: int,
    container_pids: int,
    requested_concurrency: int,
    resource_aware: bool,
    max_concurrency_per_node: int,
) -> None:
    if type(job_pids_max) is not int or job_pids_max < 0:
        raise ValueError("job_pids_max must be a non-negative integer")
    if exclusive:
        return
    if job_pids_max == 0:
        raise ValueError("job_pids_max is required for non-exclusive workers")
    if job_pids_max < container_pids:
        raise ValueError("job_pids_max must not be lower than container_pids")

    concurrency_ceiling = max_concurrency_per_node if resource_aware else requested_concurrency
    minimum_for_slots = container_pids * concurrency_ceiling
    if job_pids_max < minimum_for_slots:
        raise ValueError(
            "job_pids_max must be at least container_pids times the configured "
            f"concurrency ceiling ({minimum_for_slots})",
        )


def _parse_gpu_tres(value: str) -> tuple[str, int]:
    cleaned = value.strip()
    if not cleaned:
        return "", 0
    match = _GPU_TRES_RE.fullmatch(cleaned)
    if match is None:
        raise ValueError("gpu_tres must use gpu[:type]:COUNT with a positive COUNT")
    return cleaned, int(match.group("count"))


def build_controller_config(
    *,
    enabled: bool,
    environment: str,
    pool_name: str,
    allowed_nodes_csv: str,
    env_file: str,
    repo_dir: str,
    partition: str,
    time_limit: str,
    requested_cpus: int,
    requested_memory_mib: int,
    requested_concurrency: int,
    max_jobs: int,
    pending_job_cap: int,
    min_queued_trials: int,
    stale_after_seconds: int,
    sbatch_path: str,
    squeue_path: str,
    sacct_path: str,
    scancel_path: str,
    command_timeout_seconds: float,
    exclusive: bool = False,
    slurm_account: str = "",
    slurm_qos: str = "",
    slurm_reservation: str = "",
    sinfo_path: str = "sinfo",
    srun_path: str = "srun",
    resource_aware: bool = False,
    probe_mem_available: bool = False,
    cpu_per_slot: int = 2,
    memory_mib_per_slot: int = 8192,
    reserved_cpus: int = 4,
    reserved_memory_mib: int = 24_576,
    max_concurrency_per_node: int = 8,
    max_cpu_load_ratio: float = 1.0,
    container_cpus: float = 0.0,
    container_memory_mib: int = 0,
    container_pids: int = 0,
    job_pids_max: int = 0,
    candidate_sha: str = "",
    gpu_tres: str = "",
    job_output_dir: str = "",
) -> ElasticSlurmWorkerControllerConfig | None:
    if not enabled:
        return None

    allowed_nodes = tuple(
        node for node in (part.strip() for part in allowed_nodes_csv.split(",")) if node
    )
    if not allowed_nodes:
        raise ValueError(
            "allowed nodes are required when Slurm worker controller is enabled",
        )

    environment = _require_nonempty(environment, "environment")
    pool_name = _require_nonempty(pool_name, "pool_name")
    env_file = _require_nonempty(env_file, "env_file")
    repo_dir = _require_nonempty(repo_dir, "repo_dir")
    time_limit = _require_nonempty(time_limit, "time_limit")
    sbatch_path = _require_nonempty(sbatch_path, "sbatch_path")
    squeue_path = _require_nonempty(squeue_path, "squeue_path")
    sacct_path = _require_nonempty(sacct_path, "sacct_path")
    scancel_path = _require_nonempty(scancel_path, "scancel_path")
    sinfo_path = _require_nonempty(sinfo_path, "sinfo_path")
    srun_path = _require_nonempty(srun_path, "srun_path")

    _require_positive(requested_cpus, "requested_cpus")
    _require_positive(requested_memory_mib, "requested_memory_mib")
    _require_positive(requested_concurrency, "requested_concurrency")
    _require_positive(max_jobs, "max_jobs")
    _require_positive(pending_job_cap, "pending_job_cap")
    _require_positive(min_queued_trials, "min_queued_trials")
    _require_positive(stale_after_seconds, "stale_after_seconds")
    _require_positive(command_timeout_seconds, "command_timeout_seconds")
    _require_positive(cpu_per_slot, "cpu_per_slot")
    _require_positive(memory_mib_per_slot, "memory_mib_per_slot")
    _require_positive(max_concurrency_per_node, "max_concurrency_per_node")
    _require_positive(max_cpu_load_ratio, "max_cpu_load_ratio")
    if reserved_cpus < 0:
        raise ValueError("reserved_cpus must be non-negative")
    if reserved_memory_mib < 0:
        raise ValueError("reserved_memory_mib must be non-negative")
    if container_cpus < 0:
        raise ValueError("container_cpus must be non-negative")
    if container_memory_mib < 0:
        raise ValueError("container_memory_mib must be non-negative")
    if container_pids < 0:
        raise ValueError("container_pids must be non-negative")
    _validate_job_pids_contract(
        exclusive=exclusive,
        job_pids_max=job_pids_max,
        container_pids=container_pids,
        requested_concurrency=requested_concurrency,
        resource_aware=resource_aware,
        max_concurrency_per_node=max_concurrency_per_node,
    )
    candidate_sha = candidate_sha.strip()
    if candidate_sha and _CANDIDATE_SHA_RE.fullmatch(candidate_sha) is None:
        raise ValueError("candidate_sha must be a 40-character lowercase Git SHA")
    gpu_tres, requested_gpus = _parse_gpu_tres(gpu_tres)
    job_output_dir = job_output_dir.strip().rstrip("/")
    if job_output_dir and (
        _SAFE_OUTPUT_DIR_RE.fullmatch(job_output_dir) is None or ".." in Path(job_output_dir).parts
    ):
        raise ValueError("job_output_dir must be a safe absolute directory")
    if exclusive:
        raise ValueError(
            "exclusive Loom Slurm workers are unsupported; configure exclusive=false "
            "and keep the policy disabled until containment is complete",
        )
    _require_positive(container_cpus, "container_cpus for non-exclusive workers")
    _require_positive(
        container_memory_mib,
        "container_memory_mib for non-exclusive workers",
    )
    _require_positive(container_pids, "container_pids for non-exclusive workers")
    if _SAFE_SANDBOX_ID_RE.fullmatch(environment) is None:
        raise ValueError(
            "environment must be a lowercase sandbox identity for non-exclusive workers",
        )
    if _CANDIDATE_SHA_RE.fullmatch(candidate_sha) is None:
        raise ValueError(
            "candidate_sha is required for non-exclusive workers and must be a "
            "40-character lowercase Git SHA",
        )

    if max_jobs > len(allowed_nodes):
        raise ValueError("max_jobs cannot exceed the number of allowed nodes")
    if pending_job_cap > max_jobs:
        raise ValueError("pending_job_cap cannot exceed max_jobs")

    return ElasticSlurmWorkerControllerConfig(
        environment=environment,
        pool_name=pool_name,
        allowed_nodes=allowed_nodes,
        env_file=env_file,
        repo_dir=repo_dir,
        partition=partition.strip(),
        time_limit=time_limit,
        requested_cpus=requested_cpus,
        requested_memory_mib=requested_memory_mib,
        requested_concurrency=requested_concurrency,
        max_jobs=max_jobs,
        pending_job_cap=pending_job_cap,
        min_queued_trials=min_queued_trials,
        stale_after_seconds=stale_after_seconds,
        sbatch_path=sbatch_path,
        squeue_path=squeue_path,
        sacct_path=sacct_path,
        scancel_path=scancel_path,
        command_timeout_seconds=command_timeout_seconds,
        exclusive=exclusive,
        slurm_account=slurm_account.strip(),
        slurm_qos=slurm_qos.strip(),
        slurm_reservation=slurm_reservation.strip(),
        sinfo_path=sinfo_path,
        srun_path=srun_path,
        resource_aware=resource_aware,
        probe_mem_available=probe_mem_available,
        cpu_per_slot=cpu_per_slot,
        memory_mib_per_slot=memory_mib_per_slot,
        reserved_cpus=reserved_cpus,
        reserved_memory_mib=reserved_memory_mib,
        max_concurrency_per_node=max_concurrency_per_node,
        max_cpu_load_ratio=max_cpu_load_ratio,
        container_cpus=container_cpus,
        container_memory_mib=container_memory_mib,
        container_pids=container_pids,
        job_pids_max=job_pids_max,
        candidate_sha=candidate_sha,
        gpu_tres=gpu_tres,
        requested_gpus=requested_gpus,
        job_output_dir=job_output_dir,
    )


def _normalize_node_state(state: str) -> str:
    normalized = state.strip().lower()
    normalized = normalized.rstrip("*~#")
    if normalized in {"mix", "mixed"}:
        return "mixed"
    if normalized.startswith("idle"):
        return "idle"
    if normalized.startswith("mix"):
        return "mixed"
    if normalized.startswith("alloc"):
        return "allocated"
    if normalized.startswith("drain"):
        return "drain"
    return normalized


def _is_safe_node_state(state: str) -> bool:
    return _normalize_node_state(state) in {"idle", "mixed"}


def compute_node_capacity_plan(
    config: ElasticSlurmWorkerControllerConfig,
    *,
    node: str,
    resource: SlurmNodeResource | None,
    active_nodes: set[str],
) -> SlurmNodeCapacityPlan:
    if node in active_nodes:
        return SlurmNodeCapacityPlan(
            hostname=node,
            safe_slots=0,
            reason="active_loom_job",
        )
    if resource is None:
        return SlurmNodeCapacityPlan(
            hostname=node,
            safe_slots=0,
            reason="missing_resource_snapshot",
        )
    if not _is_safe_node_state(resource.state):
        return SlurmNodeCapacityPlan(
            hostname=node,
            safe_slots=0,
            reason="unsafe_state",
        )
    if resource.cpu_load is None:
        return SlurmNodeCapacityPlan(
            hostname=node,
            safe_slots=0,
            reason="unknown_cpu_load",
        )
    if resource.cpu_load > resource.cpus_total * config.max_cpu_load_ratio:
        return SlurmNodeCapacityPlan(
            hostname=node,
            safe_slots=0,
            reason="cpu_load_high",
        )

    available_cpus = resource.idle_cpus if resource.idle_cpus is not None else resource.cpus_total
    cpu_slots = (available_cpus - config.reserved_cpus) // config.cpu_per_slot
    if cpu_slots < 1:
        return SlurmNodeCapacityPlan(
            hostname=node,
            safe_slots=0,
            reason="insufficient_cpu",
        )
    physically_available_memory_mib = (
        resource.available_memory_mib
        if resource.available_memory_mib is not None
        else resource.free_memory_mib
    )
    usable_memory_mib = (
        physically_available_memory_mib
        if resource.schedulable_memory_mib is None
        else min(physically_available_memory_mib, resource.schedulable_memory_mib)
    )
    memory_slots = (usable_memory_mib - config.reserved_memory_mib) // config.memory_mib_per_slot
    if memory_slots < 1:
        return SlurmNodeCapacityPlan(
            hostname=node,
            safe_slots=0,
            reason="insufficient_memory",
        )

    safe_slots = min(
        int(cpu_slots),
        int(memory_slots),
        config.max_concurrency_per_node,
    )
    return SlurmNodeCapacityPlan(
        hostname=node,
        safe_slots=max(0, safe_slots),
        reason="eligible" if safe_slots > 0 else "no_safe_slots",
    )


def _compute_node_capacity_plans(
    config: ElasticSlurmWorkerControllerConfig,
    snapshot: SlurmWorkerCapacitySnapshot,
) -> dict[str, SlurmNodeCapacityPlan]:
    resources = snapshot.node_resources or {}
    return {
        node: compute_node_capacity_plan(
            config,
            node=node,
            resource=resources.get(node),
            active_nodes=snapshot.active_nodes,
        )
        for node in config.allowed_nodes
    }


def compute_controller_decision(
    config: ElasticSlurmWorkerControllerConfig,
    snapshot: SlurmWorkerCapacitySnapshot,
) -> SlurmWorkerControllerDecision:
    node_capacity = _compute_node_capacity_plans(config, snapshot) if config.resource_aware else {}
    if snapshot.queued_trials < config.min_queued_trials:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=snapshot.cancellable_pending_job_ids,
            reason="queue_drained",
            node_capacity=node_capacity,
        )

    current_jobs = snapshot.pending_jobs + snapshot.running_jobs
    remaining_jobs = max(0, config.max_jobs - current_jobs)
    existing_slots = snapshot.active_slots + snapshot.pending_slots
    missing_slots = max(0, snapshot.queued_trials - existing_slots)
    if missing_slots == 0:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=(),
            reason="capacity_satisfied",
            node_capacity=node_capacity,
        )

    if snapshot.pending_jobs >= config.pending_job_cap:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=(),
            reason="pending_cap_reached",
            node_capacity=node_capacity,
        )

    if remaining_jobs <= 0:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=(),
            reason="max_jobs_reached",
            node_capacity=node_capacity,
        )

    if config.resource_aware:
        candidate_nodes = tuple(node for node, plan in node_capacity.items() if plan.safe_slots > 0)
        if not candidate_nodes:
            return SlurmWorkerControllerDecision(
                submit_nodes=(),
                cancel_job_ids=(),
                reason="no_safe_nodes",
                node_capacity=node_capacity,
            )
        submit_nodes: list[str] = []
        planned_slots = 0
        for node in candidate_nodes:
            if len(submit_nodes) >= remaining_jobs:
                break
            submit_nodes.append(node)
            planned_slots += node_capacity[node].safe_slots
            if planned_slots >= missing_slots:
                break
        return SlurmWorkerControllerDecision(
            submit_nodes=tuple(submit_nodes),
            cancel_job_ids=(),
            reason="queued_backlog",
            node_capacity=node_capacity,
        )

    required_jobs = math.ceil(missing_slots / config.requested_concurrency)
    candidate_nodes = tuple(
        node for node in config.allowed_nodes if node not in snapshot.active_nodes
    )
    if not candidate_nodes:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=(),
            reason="no_available_nodes",
        )

    submit_count = min(required_jobs, remaining_jobs, len(candidate_nodes))
    return SlurmWorkerControllerDecision(
        submit_nodes=candidate_nodes[:submit_count],
        cancel_job_ids=(),
        reason="queued_backlog",
    )


def slurm_submission_config_for_node(
    config: ElasticSlurmWorkerControllerConfig,
    decision: SlurmWorkerControllerDecision,
    *,
    node: str,
) -> ElasticSlurmWorkerControllerConfig:
    if not config.resource_aware:
        return config
    plan = decision.node_capacity.get(node)
    if plan is None or plan.safe_slots <= 0:
        return config
    return replace(
        config,
        requested_concurrency=plan.safe_slots,
        requested_cpus=max(1, plan.safe_slots * config.cpu_per_slot),
        requested_memory_mib=max(1, plan.safe_slots * config.memory_mib_per_slot),
    )


def slurm_sandbox_identity(config: ElasticSlurmWorkerControllerConfig) -> str:
    normalized = _SAFE_JOB_NAME_RE.sub("-", config.environment.lower()).strip("-")
    return (normalized or "sandbox")[:63]


def slurm_compose_project_identity(
    config: ElasticSlurmWorkerControllerConfig,
    job_id: str,
) -> str:
    candidate_label = (config.candidate_sha or "legacy")[:12]
    return f"loom-{slurm_sandbox_identity(config)}-{candidate_label}-{job_id}"


def build_sbatch_request(
    config: ElasticSlurmWorkerControllerConfig,
    *,
    node: str,
) -> SbatchRequest:
    _validate_job_pids_contract(
        exclusive=config.exclusive,
        job_pids_max=config.job_pids_max,
        container_pids=config.container_pids,
        requested_concurrency=config.requested_concurrency,
        resource_aware=config.resource_aware,
        max_concurrency_per_node=config.max_concurrency_per_node,
    )
    job_node = _SAFE_JOB_NAME_RE.sub("-", node).strip("-") or "worker"
    sandbox_identity = slurm_sandbox_identity(config)
    candidate_sha = config.candidate_sha or "legacy"
    candidate_label = candidate_sha[:12]
    export_vars = [
        # Only the executable search path crosses from the controller process.
        # Worker credentials and release settings come from the protected env
        # file or the explicit controller-owned values below. In particular,
        # never propagate the controller-only HOME onto compute nodes.
        "PATH",
        f"LOOM_WORKER_MAX_CONCURRENT={config.requested_concurrency}",
        f"LOOM_WORKER_POOL_NAME={config.pool_name}",
        # Bind the worker identity to the canonical Slurm node selected by the
        # actuator.  Docker's generated hostname is a container ID, which
        # cannot be matched to the owning Slurm job during safe drain/release.
        f"LOOM_WORKER_HOSTNAME={node}",
        f"LOOM_REMOTE_WORKER_ENV_FILE={config.env_file}",
        f"LOOM_REMOTE_WORKER_REPO_DIR={config.repo_dir}",
        f"LOOM_WORKER_SANDBOX_IDENTITY={sandbox_identity}",
        f"LOOM_WORKER_CANDIDATE_SHA={candidate_sha}",
        f"LOOM_WORKER_SLURM_ALLOCATED_GPUS={config.requested_gpus}",
        "LOOM_WORKER_RESTART_POLICY=no",
    ]
    # #896: only export the per-container caps when configured (>0). Unset leaves
    # non-Slurm compose/driver callers may still use their own defaults.
    if config.container_cpus > 0:
        export_vars.append(f"LOOM_WORKER_CONTAINER_CPUS={config.container_cpus}")
    if config.container_memory_mib > 0:
        export_vars.append(
            f"LOOM_WORKER_CONTAINER_MEMORY_MIB={config.container_memory_mib}",
        )
    if config.container_pids > 0:
        export_vars.append(f"LOOM_WORKER_CONTAINER_PIDS={config.container_pids}")
    if not config.exclusive:
        # The batch process must prove and export the exact delegated Slurm job
        # cgroup before Docker Compose starts.  This marker is controller-owned;
        # the remote-worker env file cannot opt itself out of containment.
        export_vars.append("LOOM_WORKER_REQUIRE_CGROUP_PARENT=1")
        export_vars.append(f"LOOM_WORKER_JOB_PIDS_MAX={config.job_pids_max}")
    export = ",".join(export_vars)
    args = [
        config.sbatch_path,
        "--parsable",
        f"--job-name={f'loom-{sandbox_identity}-{candidate_label}-{job_node}'[:128]}",
        f"--nodelist={node}",
        f"--chdir={config.repo_dir}",
    ]
    if config.job_output_dir:
        args.append(f"--output={config.job_output_dir}/slurm-%j.out")
    if config.exclusive:
        raise ValueError(
            "exclusive Loom Slurm workers are unsupported; use exclusive=false",
        )
    # Non-exclusive is the only supported mode (exclusive raises above). Emit the
    # closed, versioned grammar consumed by the administrator-owned root cgroup
    # guard. No free-form user text crosses the privilege boundary.
    args.append(f"--comment=loom-cgroup-v1:pids={config.job_pids_max}")
    args.append(f"--time={config.time_limit}")
    if config.partition:
        args.append(f"--partition={config.partition}")
    if config.slurm_account:
        args.append(f"--account={config.slurm_account}")
    if config.slurm_qos:
        args.append(f"--qos={config.slurm_qos}")
    if config.slurm_reservation:
        args.append(f"--reservation={config.slurm_reservation}")
    if config.gpu_tres:
        args.append(f"--gres={config.gpu_tres}")
    args.extend(
        (
            f"--cpus-per-task={config.requested_cpus}",
            f"--mem={config.requested_memory_mib}M",
            f"--export={export}",
        )
    )

    stdin = """#!/usr/bin/env bash
set -euo pipefail

: "${SLURM_JOB_ID:?SLURM_JOB_ID is required}"
: "${LOOM_WORKER_SANDBOX_IDENTITY:?LOOM_WORKER_SANDBOX_IDENTITY is required}"
: "${LOOM_WORKER_CANDIDATE_SHA:?LOOM_WORKER_CANDIDATE_SHA is required}"

# The controller account's home is intentionally local to the controller and
# may not exist on compute nodes. Give Docker/Compose and XDG consumers a
# private per-allocation runtime instead of inheriting controller state.
job_runtime_dir=""
cleanup_runtime() {
  if [[ -n "$job_runtime_dir" ]]; then
    /usr/bin/rm -rf -- "$job_runtime_dir"
    job_runtime_dir=""
  fi
}
trap cleanup_runtime EXIT
runtime_parent="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}"
job_runtime_dir="$(/usr/bin/mktemp -d "${runtime_parent%/}/loom-worker-${SLURM_JOB_ID}.XXXXXX")"
umask 077
export HOME="$job_runtime_dir/home"
export DOCKER_CONFIG="$job_runtime_dir/docker"
export XDG_CONFIG_HOME="$job_runtime_dir/xdg-config"
export XDG_RUNTIME_DIR="$job_runtime_dir/xdg-runtime"
export TMPDIR="$job_runtime_dir/tmp"
export PYTHONDONTWRITEBYTECODE=1
/usr/bin/mkdir -p "$HOME" "$DOCKER_CONFIG" "$XDG_CONFIG_HOME" "$XDG_RUNTIME_DIR" "$TMPDIR"

export LOOM_WORKER_SLURM_JOB_ID="$SLURM_JOB_ID"
export LOOM_WORKER_SLURM_GPU_DEVICE_IDS="${SLURM_JOB_GPUS:-}"
if [[ "${LOOM_WORKER_SLURM_ALLOCATED_GPUS:?}" -gt 0 && -z "$LOOM_WORKER_SLURM_GPU_DEVICE_IDS" ]]; then
  echo "error: GPU TRES was requested but SLURM_JOB_GPUS is empty" >&2
  exit 2
fi
project_candidate="${LOOM_WORKER_CANDIDATE_SHA:0:12}"
project_job="${SLURM_JOB_ID//[^A-Za-z0-9_-]/-}"
export LOOM_WORKER_COMPOSE_PROJECT="loom-${LOOM_WORKER_SANDBOX_IDENTITY}-${project_candidate}-${project_job}"

cd "$LOOM_REMOTE_WORKER_REPO_DIR"
compose_files=(-f deploy/docker-compose.remote-worker.yml)
if [[ "${LOOM_WORKER_REQUIRE_CGROUP_PARENT:-0}" == "1" ]]; then
  # Docker's systemd cgroup driver only accepts a ``.slice`` parent, so the
  # administrator-owned root guard registers ``loom-job-<id>.slice`` capped to
  # the allocation and discovery returns it; the cgroupfs driver nests directly
  # in the delegated job cgroup. Detecting the driver here keeps both hosts safe.
  worker_cgroup_driver="$(docker info --format '{{.CgroupDriver}}' 2>/dev/null || true)"
  : "${worker_cgroup_driver:?could not read the Docker cgroup driver}"
  export LOOM_WORKER_CGROUP_PARENT="$(
    PYTHONPATH="$LOOM_REMOTE_WORKER_REPO_DIR/src" \
      /usr/bin/python3 -m loom_control_plane.slurm_job_cgroup \
      --job-id "$SLURM_JOB_ID" \
      --pids-max "$LOOM_WORKER_JOB_PIDS_MAX" \
      --wait-seconds 30 \
      --docker-driver "$worker_cgroup_driver"
  )"
  : "${LOOM_WORKER_CGROUP_PARENT:?delegated Slurm job cgroup is required}"
  compose_files+=(-f deploy/docker-compose.remote-worker.cgroup-parent.yml)
else
  unset LOOM_WORKER_CGROUP_PARENT
fi
compose_args=(--project-name "$LOOM_WORKER_COMPOSE_PROJECT" --env-file "$LOOM_REMOTE_WORKER_ENV_FILE" "${compose_files[@]}")

cleanup() {
  status=${1:-$?}
  trap - EXIT INT TERM
  if [[ -n "${compose_pid:-}" ]]; then
    kill "$compose_pid" 2>/dev/null || true
    wait "$compose_pid" 2>/dev/null || true
  fi
  cleanup_status=0
  docker compose "${compose_args[@]}" down --remove-orphans || cleanup_status=$?
  for volume_suffix in remote_worker_trajectories remote_worker_benchmarks; do
    volume_name="${LOOM_WORKER_COMPOSE_PROJECT}_${volume_suffix}"
    if docker volume inspect "$volume_name" >/dev/null 2>&1; then
      volume_status=0
      docker volume rm "$volume_name" || volume_status=$?
      if [[ "$cleanup_status" -eq 0 && "$volume_status" -ne 0 ]]; then
        cleanup_status=$volume_status
      fi
    fi
  done
  if [[ "$status" -eq 0 && "$cleanup_status" -ne 0 ]]; then
    status=$cleanup_status
  fi
  cleanup_runtime
  exit "$status"
}

trap cleanup EXIT
trap 'cleanup 130' INT
trap 'cleanup 143' TERM
docker compose "${compose_args[@]}" up --build &
compose_pid=$!
wait "$compose_pid"
"""
    return SbatchRequest(args=tuple(args), stdin=stdin)


class SubprocessSlurmCommandRunner:
    async def query_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> list[SlurmWorkerJobObservation]:
        if not job_ids:
            return []
        # `squeue` is preferred for live pending/running jobs. Missing IDs are
        # reconciled through `sacct`, which reports recently-terminal jobs.
        config = self._config
        csv_ids = ",".join(job_ids)
        observations: dict[str, SlurmWorkerJobObservation] = {}

        try:
            squeue = await _run_command(
                (
                    config.squeue_path,
                    "-h",
                    "-o",
                    "%i|%T|%N|%R",
                    "-j",
                    csv_ids,
                ),
                timeout=config.command_timeout_seconds,
            )
        except RuntimeError as exc:
            # Slurm 23.11 exits 1 instead of returning an empty result when all
            # requested IDs have already left squeue. Treat only that exact
            # terminal-job condition as a cache miss so sacct can reconcile the
            # recorded jobs. Transport, authentication, and controller errors
            # must still fail closed.
            if "Invalid job id specified" not in str(exc):
                raise
        else:
            observations.update(_parse_slurm_observations(squeue.stdout, job_ids))

        missing_job_ids = tuple(job_id for job_id in job_ids if job_id not in observations)
        if missing_job_ids:
            sacct = await _run_command(
                (
                    config.sacct_path,
                    "-n",
                    "-P",
                    "-o",
                    "JobIDRaw,State,NodeList,Reason",
                    "-j",
                    ",".join(missing_job_ids),
                ),
                timeout=config.command_timeout_seconds,
            )
            observations.update(_parse_slurm_observations(sacct.stdout, missing_job_ids))

        return [observations[job_id] for job_id in job_ids if job_id in observations]

    async def submit_worker(
        self,
        *,
        node: str,
        config: ElasticSlurmWorkerControllerConfig,
    ) -> str:
        self._config = config
        request = build_sbatch_request(config, node=node)
        result = await _run_command(
            request.args,
            stdin=request.stdin,
            timeout=config.command_timeout_seconds,
        )
        job_id = result.stdout.strip().splitlines()[0].split(";", maxsplit=1)[0].strip()
        if not job_id:
            raise RuntimeError("sbatch did not return a job id")
        return job_id

    async def cancel_job(self, job_id: str) -> None:
        config = self._config
        await _run_command(
            (config.scancel_path, job_id),
            timeout=config.command_timeout_seconds,
        )

    async def cancel_pending_job(self, job_id: str) -> None:
        config = self._config
        await _run_command(
            (config.scancel_path, "--state=PENDING", job_id),
            timeout=config.command_timeout_seconds,
        )

    async def query_node_resources(
        self,
        nodes: tuple[str, ...],
    ) -> dict[str, SlurmNodeResource]:
        if not nodes:
            return {}
        config = self._config
        sinfo = await _run_command(
            (
                config.sinfo_path,
                "-h",
                "-N",
                "-n",
                ",".join(nodes),
                "-o",
                "%N|%T|%c|%m|%e|%O|%C",
            ),
            timeout=config.command_timeout_seconds,
        )
        resources = parse_sinfo_node_resources(sinfo.stdout)
        allocated = await _run_command(
            (
                config.sinfo_path,
                "-h",
                "-N",
                "-n",
                ",".join(nodes),
                "-O",
                "NodeList:200,AllocMem:20",
            ),
            timeout=config.command_timeout_seconds,
        )
        allocated_memory = _parse_sinfo_allocated_memory(allocated.stdout)
        if set(allocated_memory) != set(resources):
            raise RuntimeError("Slurm allocated memory snapshot is incomplete")
        for node, node_resource in tuple(resources.items()):
            if node_resource.total_memory_mib is None:
                raise RuntimeError("Slurm total memory snapshot is incomplete")
            resources[node] = replace(
                node_resource,
                schedulable_memory_mib=max(
                    0,
                    node_resource.total_memory_mib - allocated_memory[node],
                ),
            )
        if config.probe_mem_available:
            for node in nodes:
                resource = resources.get(node)
                if resource is None or not _is_safe_node_state(resource.state):
                    continue
                if resource.cpu_load is None or (
                    resource.cpu_load > resource.cpus_total * config.max_cpu_load_ratio
                ):
                    continue
                available_cpus = (
                    resource.idle_cpus if resource.idle_cpus is not None else resource.cpus_total
                )
                if available_cpus - config.reserved_cpus < config.cpu_per_slot:
                    continue
                try:
                    available_memory_mib = await self._query_node_available_memory(node)
                except (OSError, RuntimeError) as exc:
                    logger.warning(
                        "elastic_slurm_worker_node_memory_probe_failed",
                        extra={
                            "environment": config.environment,
                            "pool_name": config.pool_name,
                            "node": node,
                            "err": str(exc),
                        },
                    )
                    continue
                resources[node] = replace(
                    resource,
                    available_memory_mib=available_memory_mib,
                )
        return {node: resources[node] for node in nodes if node in resources}

    async def _query_node_available_memory(self, node: str) -> int:
        config = self._config
        args = [
            config.srun_path,
            "--immediate=3",
            "--nodes=1",
            "--ntasks=1",
            f"--nodelist={node}",
            "--cpus-per-task=1",
            "--mem=16M",
            "--time=00:00:10",
            "--job-name=loom-memory-probe",
            "--kill-on-bad-exit=1",
            "--chdir=/tmp",
            "--export=NIL",
        ]
        if config.partition:
            args.append(f"--partition={config.partition}")
        if config.slurm_account:
            args.append(f"--account={config.slurm_account}")
        if config.slurm_qos:
            args.append(f"--qos={config.slurm_qos}")
        if config.slurm_reservation:
            args.append(f"--reservation={config.slurm_reservation}")
        args.extend(
            (
                "/usr/bin/awk",
                '$1 == "MemAvailable:" { print int($2 / 1024); exit }',
                "/proc/meminfo",
            ),
        )
        result = await _run_command(
            tuple(args),
            timeout=config.command_timeout_seconds,
        )
        available_memory_mib = _parse_optional_int(result.stdout)
        if available_memory_mib is None or available_memory_mib < 0:
            raise RuntimeError("Slurm node memory probe returned no valid MemAvailable value")
        return available_memory_mib

    async def resolve_node_names(self, nodes: tuple[str, ...]) -> dict[str, str]:
        """Map each requested node to its canonical Slurm NodeName.

        Slurm NodeNames are case-sensitive, so this matches case-insensitively
        against the live ``sinfo`` node list and returns the exact NodeName. Only
        names Slurm actually knows are included.
        """
        if not nodes:
            return {}
        config = self._config
        sinfo = await _run_command(
            (config.sinfo_path, "-h", "-N", "-o", "%N"),
            timeout=config.command_timeout_seconds,
        )
        canonical_by_lower: dict[str, str] = {}
        for raw in sinfo.stdout.splitlines():
            name = raw.strip()
            if name:
                canonical_by_lower.setdefault(name.lower(), name)
        return {
            node: canonical_by_lower[node.lower()]
            for node in nodes
            if node.lower() in canonical_by_lower
        }

    def bind_config(
        self,
        config: ElasticSlurmWorkerControllerConfig,
    ) -> SubprocessSlurmCommandRunner:
        self._config = config
        return self

    _config: ElasticSlurmWorkerControllerConfig


@dataclass(frozen=True)
class _CommandResult:
    stdout: str
    stderr: str


async def _run_command(
    args: Sequence[str],
    *,
    timeout: float,
    stdin: str | None = None,
) -> _CommandResult:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdin=asyncio.subprocess.PIPE if stdin is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(stdin.encode("utf-8") if stdin is not None else None),
            timeout=timeout,
        )
    except TimeoutError:
        proc.kill()
        with contextlib.suppress(ProcessLookupError):
            await proc.wait()
        raise RuntimeError(f"Slurm command timed out: {args[0]}") from None
    if proc.returncode != 0:
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"Slurm command failed ({proc.returncode}): {args[0]} {stderr_text}",
        )
    return _CommandResult(
        stdout=stdout.decode("utf-8", errors="replace"),
        stderr=stderr.decode("utf-8", errors="replace"),
    )


def _parse_slurm_observations(
    output: str,
    requested_job_ids: tuple[str, ...],
) -> dict[str, SlurmWorkerJobObservation]:
    requested = set(requested_job_ids)
    observations: dict[str, SlurmWorkerJobObservation] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        job_id = parts[0].split(".", maxsplit=1)[0].strip()
        if job_id not in requested:
            continue
        observations[job_id] = SlurmWorkerJobObservation(
            job_id=job_id,
            slurm_state=parts[1].strip(),
            nodelist=parts[2].strip() if len(parts) > 2 and parts[2].strip() else None,
            pending_reason=parts[3].strip() if len(parts) > 3 and parts[3].strip() else None,
        )
    return observations


def _parse_optional_float(value: str) -> float | None:
    cleaned = value.strip()
    if not cleaned or cleaned in {"*", "N/A", "n/a"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_optional_int(value: str) -> int | None:
    cleaned = value.strip()
    if not cleaned or cleaned in {"*", "N/A", "n/a"}:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_idle_cpus(cpu_state: str) -> int | None:
    parts = [part.strip() for part in cpu_state.split("/")]
    if len(parts) != 4:
        return None
    return _parse_optional_int(parts[1])


def parse_sinfo_node_resources(output: str) -> dict[str, SlurmNodeResource]:
    resources: dict[str, SlurmNodeResource] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 6:
            continue
        hostname = parts[0].strip()
        state = parts[1].strip()
        cpus_total = _parse_optional_int(parts[2])
        total_memory_mib = _parse_optional_int(parts[3])
        free_memory_mib = _parse_optional_int(parts[4])
        cpu_load = _parse_optional_float(parts[5])
        allocated_memory_mib = _parse_optional_int(parts[7]) if len(parts) > 7 else None
        if (
            not hostname
            or cpus_total is None
            or total_memory_mib is None
            or free_memory_mib is None
        ):
            continue
        resources[hostname] = SlurmNodeResource(
            hostname=hostname,
            state=state,
            cpus_total=cpus_total,
            free_memory_mib=free_memory_mib,
            cpu_load=cpu_load,
            idle_cpus=_parse_idle_cpus(parts[6]) if len(parts) > 6 else None,
            total_memory_mib=total_memory_mib,
            schedulable_memory_mib=(
                None
                if allocated_memory_mib is None
                else max(0, total_memory_mib - allocated_memory_mib)
            ),
        )
    return resources


def _parse_sinfo_allocated_memory(output: str) -> dict[str, int]:
    allocated: dict[str, int] = {}
    for raw_line in output.splitlines():
        parts = raw_line.split()
        if len(parts) != 2:
            continue
        hostname = parts[0]
        allocated_memory_mib = _parse_optional_int(parts[1])
        if not hostname or allocated_memory_mib is None or allocated_memory_mib < 0:
            continue
        previous = allocated.get(hostname)
        if previous is not None and previous != allocated_memory_mib:
            raise RuntimeError("Slurm allocated memory snapshot is ambiguous")
        allocated[hostname] = allocated_memory_mib
    return allocated


async def load_capacity_snapshot(
    session: AsyncSession,
    config: ElasticSlurmWorkerControllerConfig,
) -> SlurmWorkerCapacitySnapshot:
    queued_trials = int((await session.execute(_QUEUE_READY_STMT)).scalar_one())
    running_trials = int((await session.execute(_QUEUE_RUNNING_STMT)).scalar_one())
    jobs = (
        (
            await session.execute(
                select(SlurmWorkerJob).where(
                    SlurmWorkerJob.environment == config.environment,
                    SlurmWorkerJob.pool_name == config.pool_name,
                    SlurmWorkerJob.state.in_((*ACTIVE_STATES, "stale")),
                ),
            )
        )
        .scalars()
        .all()
    )

    stale_cutoff = datetime.now(UTC) - timedelta(seconds=config.stale_after_seconds)
    pending_jobs = 0
    running_jobs = 0
    pending_slots = 0
    active_slots = 0
    active_nodes: set[str] = set()
    active_job_ids: list[str] = []
    cancellable_pending_job_ids: list[str] = []
    for job in jobs:
        if job.state == "stale":
            if job.stale_at is not None and job.stale_at >= stale_cutoff:
                active_nodes.add(job.nodelist)
            continue
        active_nodes.add(job.nodelist)
        if job.job_id:
            active_job_ids.append(job.job_id)
        if job.state == "pending":
            pending_jobs += 1
            pending_slots += job.requested_concurrency
            if job.job_id:
                cancellable_pending_job_ids.append(job.job_id)
        elif job.state == "running":
            running_jobs += 1
            active_slots += job.requested_concurrency

    return SlurmWorkerCapacitySnapshot(
        queued_trials=queued_trials,
        running_trials=running_trials,
        pending_jobs=pending_jobs,
        running_jobs=running_jobs,
        active_slots=active_slots,
        pending_slots=pending_slots,
        active_nodes=active_nodes,
        cancellable_pending_job_ids=tuple(cancellable_pending_job_ids),
        active_job_ids=tuple(active_job_ids),
    )


async def with_node_resource_snapshot(
    snapshot: SlurmWorkerCapacitySnapshot,
    *,
    config: ElasticSlurmWorkerControllerConfig,
    runner: SlurmWorkerCommandRunner,
) -> SlurmWorkerCapacitySnapshot:
    if not config.resource_aware:
        return snapshot
    try:
        probe_nodes = tuple(
            node for node in config.allowed_nodes if node not in snapshot.active_nodes
        )
        resources = await runner.query_node_resources(probe_nodes)
    except Exception as exc:
        logger.warning(
            "elastic_slurm_worker_node_resource_query_failed",
            extra={
                "environment": config.environment,
                "pool_name": config.pool_name,
                "err": str(exc),
            },
        )
        resources = {}
    return replace(snapshot, node_resources=resources)


async def _resolve_allowed_node_case(
    config: ElasticSlurmWorkerControllerConfig,
    runner: SlurmWorkerCommandRunner,
) -> ElasticSlurmWorkerControllerConfig:
    """Rewrite ``allowed_nodes`` to the canonical Slurm NodeNames.

    Slurm rejects a nodelist whose case does not match the NodeName, so a config
    that lists a differently-cased name silently drops that node from scheduling
    and any submit to it fails with ``Invalid node name specified``. When the
    runner can resolve names, replace ``allowed_nodes`` with the canonical forms
    and drop any Slurm does not recognise. Falls back to the configured names
    when resolution is unavailable or yields nothing, so the pool is never
    disabled by a transient ``sinfo`` failure.
    """

    resolver = getattr(runner, "resolve_node_names", None)
    if resolver is None:
        return config
    try:
        resolved = await resolver(config.allowed_nodes)
    except Exception as exc:  # a resolution failure must never stop the tick
        logger.warning(
            "elastic_slurm_worker_node_resolution_failed",
            extra={
                "environment": config.environment,
                "pool_name": config.pool_name,
                "err": str(exc),
            },
        )
        return config

    canonical: list[str] = []
    dropped: list[str] = []
    for node in config.allowed_nodes:
        target = resolved.get(node)
        if target is None:
            dropped.append(node)
        elif target not in canonical:
            canonical.append(target)
    if dropped:
        logger.warning(
            "elastic_slurm_worker_unknown_nodes",
            extra={
                "environment": config.environment,
                "pool_name": config.pool_name,
                "dropped": dropped,
            },
        )
    if not canonical or tuple(canonical) == config.allowed_nodes:
        return config
    return replace(config, allowed_nodes=tuple(canonical))


async def run_elastic_slurm_worker_controller_once(
    session: AsyncSession,
    *,
    config: ElasticSlurmWorkerControllerConfig,
    runner: SlurmWorkerCommandRunner,
) -> SlurmWorkerControllerTickResult:
    config = await _resolve_allowed_node_case(config, runner)
    snapshot = await load_capacity_snapshot(session, config)
    if snapshot.active_job_ids:
        try:
            observations = await runner.query_jobs(snapshot.active_job_ids)
            await reconcile_slurm_worker_jobs(
                session,
                observations,
                stale_after_seconds=config.stale_after_seconds,
                slurm_cluster_id=slurm_cluster_for_pool(config.pool_name),
                environment=config.environment,
                pool_name=config.pool_name,
            )
        except Exception as exc:
            logger.warning(
                "elastic_slurm_worker_query_failed",
                extra={
                    "environment": config.environment,
                    "pool_name": config.pool_name,
                    "err": str(exc),
                },
            )
        snapshot = await load_capacity_snapshot(session, config)
    snapshot = await with_node_resource_snapshot(
        snapshot,
        config=config,
        runner=runner,
    )

    decision = compute_controller_decision(config, snapshot)
    logger.info(
        "elastic_slurm_worker_decision",
        extra={
            "environment": config.environment,
            "pool_name": config.pool_name,
            "queued_trials": snapshot.queued_trials,
            "running_trials": snapshot.running_trials,
            "pending_jobs": snapshot.pending_jobs,
            "running_jobs": snapshot.running_jobs,
            "active_slots": snapshot.active_slots,
            "pending_slots": snapshot.pending_slots,
            "submit_nodes": decision.submit_nodes,
            "cancel_job_ids": decision.cancel_job_ids,
            "reason": decision.reason,
        },
    )

    cancelled_job_ids: list[str] = []
    for job_id in decision.cancel_job_ids:
        try:
            await runner.cancel_job(job_id)
            await reconcile_slurm_worker_jobs(
                session,
                [
                    SlurmWorkerJobObservation(
                        job_id=job_id,
                        slurm_state="CANCELLED",
                        pending_reason="cancelled after Loom backlog drained",
                        observed_at=datetime.now(UTC),
                    ),
                ],
                stale_after_seconds=config.stale_after_seconds,
                slurm_cluster_id=slurm_cluster_for_pool(config.pool_name),
                environment=config.environment,
                pool_name=config.pool_name,
            )
            cancelled_job_ids.append(job_id)
        except Exception as exc:
            logger.warning(
                "elastic_slurm_worker_cancel_failed",
                extra={
                    "environment": config.environment,
                    "pool_name": config.pool_name,
                    "job_id": job_id,
                    "err": str(exc),
                },
            )

    submitted_job_ids: list[str] = []
    for node in decision.submit_nodes:
        node_config = slurm_submission_config_for_node(config, decision, node=node)
        try:
            job_id = await runner.submit_worker(node=node, config=node_config)
            await record_slurm_worker_job(
                session,
                environment=node_config.environment,
                pool_name=node_config.pool_name,
                nodelist=node,
                requested_cpus=node_config.requested_cpus,
                requested_memory_mib=node_config.requested_memory_mib,
                requested_pids=node_config.container_pids or None,
                requested_gpu_tres=node_config.gpu_tres or None,
                requested_gpus=node_config.requested_gpus,
                requested_concurrency=node_config.requested_concurrency,
                sandbox_identity=slurm_sandbox_identity(node_config),
                candidate_sha=node_config.candidate_sha or None,
                compose_project=slurm_compose_project_identity(node_config, job_id),
                job_id=job_id,
                slurm_state="PENDING",
                pending_reason=None,
                env=_worker_env(node_config),
                submitted_at=datetime.now(UTC),
            )
            submitted_job_ids.append(job_id)
        except Exception as exc:
            await record_slurm_worker_job(
                session,
                environment=node_config.environment,
                pool_name=node_config.pool_name,
                nodelist=node,
                requested_cpus=node_config.requested_cpus,
                requested_memory_mib=node_config.requested_memory_mib,
                requested_pids=node_config.container_pids or None,
                requested_gpu_tres=node_config.gpu_tres or None,
                requested_gpus=node_config.requested_gpus,
                requested_concurrency=node_config.requested_concurrency,
                sandbox_identity=slurm_sandbox_identity(node_config),
                candidate_sha=node_config.candidate_sha or None,
                job_id=None,
                slurm_state="FAILED",
                pending_reason=None,
                env=_worker_env(node_config),
                submitted_at=datetime.now(UTC),
                submission_error=str(exc),
            )
            logger.warning(
                "elastic_slurm_worker_submit_failed",
                extra={
                    "environment": config.environment,
                    "pool_name": config.pool_name,
                    "node": node,
                    "err": str(exc),
                },
            )

    await session.flush()
    return SlurmWorkerControllerTickResult(
        submitted_job_ids=tuple(submitted_job_ids),
        cancelled_job_ids=tuple(cancelled_job_ids),
        decision=decision,
    )


async def run_elastic_slurm_worker_controller_loop(
    *,
    session_factory: Any,
    config: ElasticSlurmWorkerControllerConfig,
    runner: SlurmWorkerCommandRunner,
    interval_sec: int = 30,
) -> None:
    while True:
        try:
            async with session_factory() as session:
                await run_elastic_slurm_worker_controller_once(
                    session,
                    config=config,
                    runner=runner,
                )
                await session.commit()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(
                "elastic_slurm_worker_controller_error",
                extra={
                    "environment": config.environment,
                    "pool_name": config.pool_name,
                    "err": str(exc),
                },
            )
        await asyncio.sleep(interval_sec)


def _worker_env(config: ElasticSlurmWorkerControllerConfig) -> dict[str, str]:
    env = {
        "LOOM_WORKER_MAX_CONCURRENT": str(config.requested_concurrency),
        "LOOM_WORKER_POOL_NAME": config.pool_name,
        "LOOM_REMOTE_WORKER_ENV_FILE": config.env_file,
        "LOOM_REMOTE_WORKER_REPO_DIR": config.repo_dir,
        "LOOM_WORKER_SANDBOX_IDENTITY": slurm_sandbox_identity(config),
        "LOOM_WORKER_CANDIDATE_SHA": config.candidate_sha,
        "LOOM_WORKER_SLURM_ALLOCATED_GPUS": str(config.requested_gpus),
    }
    try:
        fingerprint = worker_token_fingerprint_from_env_file(Path(config.env_file))
    except OSError as exc:
        logger.warning(
            "slurm_worker_token_fingerprint_unavailable",
            extra={
                "environment": config.environment,
                "pool_name": config.pool_name,
                "env_file": config.env_file,
                "err": str(exc),
            },
        )
    else:
        if fingerprint:
            env[WORKER_AUTH_FINGERPRINT_ENV_KEY] = fingerprint
    return env
