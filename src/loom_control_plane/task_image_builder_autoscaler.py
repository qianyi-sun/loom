"""Scale exclusive Slurm task-image builders from durable queue demand."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import SlurmWorkerJob, TaskImageMaterialization
from loom_control_plane.elastic_slurm_worker_controller import SbatchRequest, _run_command
from loom_control_plane.slurm_worker_jobs import (
    ACTIVE_STATES,
    SlurmWorkerJobObservation,
    reconcile_slurm_worker_jobs,
    redact_env,
)

_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_ABSOLUTE_PATH_RE = re.compile(r"^/[A-Za-z0-9._/${}-]+$")


@dataclass(frozen=True)
class TaskImageBuilderPoolConfig:
    environment: str
    pool_name: str
    slurm_cluster_id: Literal["oldlab", "gb10"]
    cpu_arch: Literal["x86_64", "arm64"]
    allowed_nodes: tuple[str, ...]
    env_file: str
    env_template_file: str
    builder_token_file: str
    repo_dir: str
    partition: str
    time_limit: str
    requested_cpus: int
    requested_memory_mib: int
    requested_concurrency: int
    max_jobs: int
    pending_job_cap: int
    idle_exit_after_seconds: int
    sbatch_path: str
    squeue_path: str
    sacct_path: str
    scancel_path: str
    command_timeout_seconds: float
    registry_docker_config_dir: str
    exclusive: bool = True
    slurm_account: str = ""
    slurm_qos: str = ""
    slurm_reservation: str = ""
    job_output_dir: str = ""
    failure_backoff_seconds: int = 300

    def __post_init__(self) -> None:
        if not self.exclusive:
            raise ValueError("task image builder allocations must be exclusive")
        if self.requested_concurrency != 1:
            raise ValueError("task image builder concurrency must equal one")
        if not self.allowed_nodes:
            raise ValueError("task image builder allowed_nodes must not be empty")
        if self.max_jobs > len(self.allowed_nodes):
            raise ValueError("task image builder max_jobs must not exceed allowed_nodes")
        if self.pending_job_cap > self.max_jobs:
            raise ValueError("task image builder pending_job_cap must not exceed max_jobs")
        if self.cpu_arch == "arm64" and self.slurm_cluster_id != "gb10":
            raise ValueError("arm64 task image builders require the gb10 Slurm cluster")
        if self.cpu_arch == "x86_64" and self.slurm_cluster_id != "oldlab":
            raise ValueError("x86_64 task image builders require the oldlab Slurm cluster")
        for name, string_value in (
            ("environment", self.environment),
            ("pool_name", self.pool_name),
            ("partition", self.partition),
            ("slurm_account", self.slurm_account),
            ("slurm_qos", self.slurm_qos),
            ("slurm_reservation", self.slurm_reservation),
        ):
            if not string_value or _SAFE_NAME_RE.fullmatch(string_value) is None:
                raise ValueError(f"task image builder {name} is invalid")
        for node in self.allowed_nodes:
            if _SAFE_NAME_RE.fullmatch(node) is None:
                raise ValueError("task image builder node name is invalid")
        for name, value in (
            ("env_file", self.env_file),
            ("env_template_file", self.env_template_file),
            ("builder_token_file", self.builder_token_file),
            ("repo_dir", self.repo_dir),
            ("registry_docker_config_dir", self.registry_docker_config_dir),
        ):
            if _SAFE_ABSOLUTE_PATH_RE.fullmatch(value) is None:
                raise ValueError(f"task image builder {name} must be a safe absolute path")
        if _SAFE_ABSOLUTE_PATH_RE.fullmatch(self.job_output_dir) is None:
            raise ValueError("task image builder job_output_dir must be a safe absolute path")
        for name, numeric_value in (
            ("requested_cpus", self.requested_cpus),
            ("requested_memory_mib", self.requested_memory_mib),
            ("max_jobs", self.max_jobs),
            ("pending_job_cap", self.pending_job_cap),
            ("idle_exit_after_seconds", self.idle_exit_after_seconds),
        ):
            if numeric_value <= 0:
                raise ValueError(f"task image builder {name} must be positive")
        if self.command_timeout_seconds <= 0:
            raise ValueError("task image builder command timeout must be positive")
        if (
            type(self.failure_backoff_seconds) is not int
            or not 1 <= self.failure_backoff_seconds <= 3600
        ):
            raise ValueError(
                "task image builder failure backoff must be an integer from 1 to 3600 seconds"
            )


@dataclass(frozen=True)
class TaskImageBuilderAutoscalerResult:
    queued_materializations: int
    submitted_job_ids: tuple[str, ...]
    cancelled_job_ids: tuple[str, ...]
    failure_backoff_active: bool


class TaskImageBuilderSlurmRunner(Protocol):
    async def submit_builder(
        self,
        *,
        node: str,
        config: TaskImageBuilderPoolConfig,
    ) -> str: ...

    async def cancel_pending_job(self, job_id: str) -> None: ...

    async def cancel_job(self, job_id: str) -> None: ...


def build_task_image_builder_sbatch_request(
    config: TaskImageBuilderPoolConfig,
    *,
    node: str,
) -> SbatchRequest:
    if node not in config.allowed_nodes:
        raise ValueError("task image builder node is outside allowed_nodes")
    export_values = [
        "PATH",
        "LOOM_WORKER_MAX_CONCURRENT=1",
        f"LOOM_WORKER_POOL_NAME={config.pool_name}",
        f"LOOM_WORKER_HOSTNAME={node}",
        f"LOOM_REMOTE_WORKER_ENV_FILE={config.env_file}",
        f"LOOM_REMOTE_WORKER_REPO_DIR={config.repo_dir}",
        f"LOOM_TASK_IMAGE_BUILDER_DOCKER_CONFIG_DIR={config.registry_docker_config_dir}",
        f"LOOM_WORKER_TASK_IMAGE_BUILDER_IDLE_EXIT_SECONDS={config.idle_exit_after_seconds}",
        "LOOM_WORKER_REQUIRE_CGROUP_PARENT=0",
        "LOOM_WORKER_RESTART_POLICY=no",
    ]
    args = [
        config.sbatch_path,
        "--parsable",
        f"--job-name=loom-{config.pool_name}-{node}",
        f"--nodelist={node}",
        "--nodes=1",
        "--ntasks=1",
        "--exclusive",
        f"--chdir={config.repo_dir}",
        f"--time={config.time_limit}",
        f"--partition={config.partition}",
    ]
    if config.job_output_dir:
        args.append(f"--output={config.job_output_dir}/task-image-%j.out")
    if config.slurm_account:
        args.append(f"--account={config.slurm_account}")
    if config.slurm_qos:
        args.append(f"--qos={config.slurm_qos}")
    if config.slurm_reservation:
        args.append(f"--reservation={config.slurm_reservation}")
    args.extend(
        (
            f"--cpus-per-task={config.requested_cpus}",
            f"--mem={config.requested_memory_mib}M",
            f"--export={','.join(export_values)}",
        )
    )
    stdin = """#!/usr/bin/env bash
set -euo pipefail
: "${SLURM_JOB_ID:?SLURM_JOB_ID is required}"
: "${LOOM_REMOTE_WORKER_ENV_FILE:?builder env file is required}"
: "${LOOM_REMOTE_WORKER_REPO_DIR:?builder repository is required}"
: "${LOOM_TASK_IMAGE_BUILDER_DOCKER_CONFIG_DIR:?builder registry credentials are required}"

runtime_parent="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}"
job_runtime_dir="$(/usr/bin/mktemp -d "${runtime_parent%/}/loom-task-builder-${SLURM_JOB_ID}.XXXXXX")"
cleanup() {
  /usr/bin/rm -rf -- "$job_runtime_dir"
}
trap cleanup EXIT INT TERM
umask 077
export DOCKER_CONFIG="$job_runtime_dir/docker"
export XDG_CONFIG_HOME="$job_runtime_dir/xdg-config"
export XDG_RUNTIME_DIR="$job_runtime_dir/xdg-runtime"
export TMPDIR="$job_runtime_dir/tmp"
export LOOM_WORKER_SLURM_JOB_ID="$SLURM_JOB_ID"
export LOOM_WORKER_COMPOSE_PROJECT="loom-task-builder-${SLURM_JOB_ID//[^A-Za-z0-9_-]/-}"
/usr/bin/mkdir -p "$DOCKER_CONFIG" "$XDG_CONFIG_HOME" "$XDG_RUNTIME_DIR" "$TMPDIR"
cd "$LOOM_REMOTE_WORKER_REPO_DIR"
docker compose --project-name "$LOOM_WORKER_COMPOSE_PROJECT" \
  --env-file "$LOOM_REMOTE_WORKER_ENV_FILE" \
  -f deploy/docker-compose.remote-worker.yml \
  run --rm --no-deps \
  --volume "$LOOM_TASK_IMAGE_BUILDER_DOCKER_CONFIG_DIR:/run/loom/task-image-builder-docker:ro" \
  --env DOCKER_CONFIG=/run/loom/task-image-builder-docker \
  worker python -m loom_worker.task_image_builder
"""
    return SbatchRequest(args=tuple(args), stdin=stdin)


def build_task_image_builder_sbatch_test_request(
    config: TaskImageBuilderPoolConfig,
    *,
    node: str,
) -> SbatchRequest:
    request = build_task_image_builder_sbatch_request(config, node=node)
    return SbatchRequest(
        args=(
            request.args[0],
            "--test-only",
            *(item for item in request.args[1:] if item != "--parsable"),
        ),
        stdin=request.stdin,
    )


class SubprocessTaskImageBuilderSlurmRunner:
    def __init__(self, config: TaskImageBuilderPoolConfig) -> None:
        self.config = config

    async def validate_builder_request(
        self,
        *,
        node: str,
        config: TaskImageBuilderPoolConfig,
    ) -> None:
        request = build_task_image_builder_sbatch_test_request(config, node=node)
        await _run_command(
            request.args,
            stdin=request.stdin,
            timeout=config.command_timeout_seconds,
        )

    async def submit_builder(
        self,
        *,
        node: str,
        config: TaskImageBuilderPoolConfig,
    ) -> str:
        request = build_task_image_builder_sbatch_request(config, node=node)
        result = await _run_command(
            request.args,
            stdin=request.stdin,
            timeout=config.command_timeout_seconds,
        )
        job_id = result.stdout.strip().splitlines()[0].split(";", 1)[0].strip()
        if not job_id:
            raise RuntimeError("sbatch did not return a task image builder job id")
        return job_id

    async def cancel_pending_job(self, job_id: str) -> None:
        await _run_command(
            (self.config.scancel_path, "--state=PENDING", job_id),
            timeout=self.config.command_timeout_seconds,
        )

    async def cancel_job(self, job_id: str) -> None:
        await _run_command(
            (self.config.scancel_path, job_id),
            timeout=self.config.command_timeout_seconds,
        )

    async def query_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> list[SlurmWorkerJobObservation]:
        if not job_ids:
            return []
        observations: dict[str, SlurmWorkerJobObservation] = {}

        def collect(output: str, expected_ids: tuple[str, ...]) -> None:
            expected = set(expected_ids)
            for line in output.splitlines():
                parts = line.split("|", 3)
                if len(parts) == 4 and parts[0] in expected:
                    observations[parts[0]] = SlurmWorkerJobObservation(
                        job_id=parts[0],
                        slurm_state=parts[1],
                        nodelist=parts[2] or None,
                        pending_reason=parts[3] or None,
                    )

        try:
            result = await _run_command(
                (
                    self.config.squeue_path,
                    "-h",
                    "-o",
                    "%i|%T|%N|%R",
                    "-j",
                    ",".join(job_ids),
                ),
                timeout=self.config.command_timeout_seconds,
            )
        except RuntimeError as exc:
            if "Invalid job id specified" not in str(exc):
                raise
        else:
            collect(result.stdout, job_ids)

        missing = tuple(job_id for job_id in job_ids if job_id not in observations)
        if missing:
            result = await _run_command(
                (
                    self.config.sacct_path,
                    "-n",
                    "-P",
                    "-o",
                    "JobIDRaw,State,NodeList,Reason",
                    "-j",
                    ",".join(missing),
                ),
                timeout=self.config.command_timeout_seconds,
            )
            collect(result.stdout, missing)
        return [observations[job_id] for job_id in job_ids if job_id in observations]


async def _load_active_jobs(
    session: AsyncSession,
    config: TaskImageBuilderPoolConfig,
) -> list[SlurmWorkerJob]:
    return list(
        (
            await session.scalars(
                select(SlurmWorkerJob)
                .where(
                    SlurmWorkerJob.environment == config.environment,
                    SlurmWorkerJob.pool_name == config.pool_name,
                    SlurmWorkerJob.state.in_(ACTIVE_STATES),
                )
                .order_by(SlurmWorkerJob.submitted_at, SlurmWorkerJob.id)
                .with_for_update()
            )
        ).all()
    )


async def _failure_backoff_active(
    session: AsyncSession,
    config: TaskImageBuilderPoolConfig,
    *,
    now: datetime,
) -> bool:
    latest_failure = await session.scalar(
        select(SlurmWorkerJob.finished_at)
        .where(
            SlurmWorkerJob.environment == config.environment,
            SlurmWorkerJob.pool_name == config.pool_name,
            SlurmWorkerJob.state == "failed",
            SlurmWorkerJob.finished_at.is_not(None),
        )
        .order_by(SlurmWorkerJob.finished_at.desc(), SlurmWorkerJob.id.desc())
        .limit(1)
    )
    return latest_failure is not None and latest_failure > now - timedelta(
        seconds=config.failure_backoff_seconds
    )


async def reconcile_task_image_builder_autoscaler_once(
    session: AsyncSession,
    *,
    config: TaskImageBuilderPoolConfig,
    runner: TaskImageBuilderSlurmRunner,
    scale_up_allowed: bool = True,
) -> TaskImageBuilderAutoscalerResult:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
        {"key": f"task-image-builder:{config.environment}:{config.pool_name}"},
    )
    active_jobs = await _load_active_jobs(session, config)
    query_jobs = getattr(runner, "query_jobs", None)
    active_job_ids = tuple(job.job_id for job in active_jobs if job.job_id)
    if query_jobs is not None and active_job_ids:
        observations = await query_jobs(active_job_ids)
        await reconcile_slurm_worker_jobs(
            session,
            observations,
            stale_after_seconds=300,
            slurm_cluster_id=config.slurm_cluster_id,
            environment=config.environment,
            pool_name=config.pool_name,
        )
        active_jobs = await _load_active_jobs(session, config)

    now = datetime.now(UTC)
    failure_backoff_active = await _failure_backoff_active(
        session,
        config,
        now=now,
    )
    queued = int(
        await session.scalar(
            select(func.count(TaskImageMaterialization.id)).where(
                TaskImageMaterialization.cpu_arch == config.cpu_arch,
                TaskImageMaterialization.attempt_count < TaskImageMaterialization.max_attempts,
                or_(
                    and_(
                        TaskImageMaterialization.state == "queued",
                        or_(
                            TaskImageMaterialization.next_attempt_at.is_(None),
                            TaskImageMaterialization.next_attempt_at <= now,
                        ),
                    ),
                    and_(
                        TaskImageMaterialization.state.in_(("claimed", "running")),
                        TaskImageMaterialization.lease_expires_at <= now,
                    ),
                ),
            )
        )
        or 0
    )
    target_jobs = min(config.max_jobs, queued) if scale_up_allowed else 0
    pending = [job for job in active_jobs if job.state == "pending" and job.job_id]
    cancelled: list[str] = []
    if scale_up_allowed:
        excess = max(0, len(active_jobs) - target_jobs)
        cancellation_candidates = pending[-excess:] if excess else []
    else:
        cancellation_candidates = active_jobs
    for job in reversed(cancellation_candidates):
        assert job.job_id is not None
        if scale_up_allowed:
            await runner.cancel_pending_job(job.job_id)
            cancellation_reason = "cancelled after task image backlog drained"
        else:
            await runner.cancel_job(job.job_id)
            cancellation_reason = (
                "cancelled because global execution witness forbids builder capacity"
            )
        job.state = "cancelled"
        job.slurm_state = "CANCELLED"
        job.pending_reason = cancellation_reason
        job.finished_at = now
        job.updated_at = now
        cancelled.append(job.job_id)
    active_jobs = [job for job in active_jobs if job.state in ACTIVE_STATES]
    pending_count = sum(job.state == "pending" for job in active_jobs)
    active_nodes = {job.nodelist for job in active_jobs}
    submission_count = (
        0
        if failure_backoff_active
        else min(
            max(0, target_jobs - len(active_jobs)),
            max(0, config.pending_job_cap - pending_count),
        )
    )
    nodes = [node for node in config.allowed_nodes if node not in active_nodes][:submission_count]
    submitted: list[str] = []
    for node in nodes:
        job_id = await runner.submit_builder(node=node, config=config)
        session.add(
            SlurmWorkerJob(
                slurm_cluster_id=config.slurm_cluster_id,
                environment=config.environment,
                pool_name=config.pool_name,
                nodelist=node,
                requested_cpus=config.requested_cpus,
                requested_memory_mib=config.requested_memory_mib,
                requested_concurrency=1,
                requested_gpus=0,
                job_id=job_id,
                slurm_state="PENDING",
                state="pending",
                redacted_env=redact_env(
                    {
                        "LOOM_REMOTE_WORKER_ENV_FILE": config.env_file,
                        "LOOM_REMOTE_WORKER_REPO_DIR": config.repo_dir,
                        "LOOM_WORKER_TASK_IMAGE_BUILDER_IDLE_EXIT_SECONDS": str(
                            config.idle_exit_after_seconds
                        ),
                    }
                ),
                submitted_at=now,
                updated_at=now,
            )
        )
        await session.flush()
        submitted.append(job_id)
    return TaskImageBuilderAutoscalerResult(
        queued_materializations=queued,
        submitted_job_ids=tuple(submitted),
        cancelled_job_ids=tuple(cancelled),
        failure_backoff_active=failure_backoff_active,
    )


async def run_task_image_builder_autoscaler_loop(
    *,
    session_factory: Any,
    configs: tuple[TaskImageBuilderPoolConfig, ...],
    runners: dict[str, TaskImageBuilderSlurmRunner],
    interval_seconds: float = 30.0,
) -> None:
    while True:
        for config in configs:
            async with session_factory() as session, session.begin():
                await reconcile_task_image_builder_autoscaler_once(
                    session,
                    config=config,
                    runner=runners[config.pool_name],
                )
        await asyncio.sleep(interval_seconds)
