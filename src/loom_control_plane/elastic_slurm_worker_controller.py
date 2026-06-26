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
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import SlurmWorkerJob, Trial
from loom_control_plane.slurm_worker_jobs import (
    ACTIVE_STATES,
    SlurmWorkerJobObservation,
    reconcile_slurm_worker_jobs,
    record_slurm_worker_job,
)

logger = logging.getLogger(__name__)

_QUEUE_ACTIVE_STATES = ("claimed", "running")
_QUEUE_READY_STMT = select(func.count()).select_from(Trial).where(
    Trial.state == "queued",
    or_(Trial.next_attempt_at.is_(None), Trial.next_attempt_at <= func.now()),
)
_QUEUE_RUNNING_STMT = select(func.count()).select_from(Trial).where(
    Trial.state.in_(_QUEUE_ACTIVE_STATES),
)
_SAFE_JOB_NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")


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


@dataclass(frozen=True)
class SlurmWorkerControllerDecision:
    submit_nodes: tuple[str, ...]
    cancel_job_ids: tuple[str, ...]
    reason: str


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
        """Cancel one pending Slurm job."""


def _require_nonempty(value: str, name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{name} is required when Slurm worker controller is enabled")
    return value


def _require_positive(value: int | float, name: str) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


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
) -> ElasticSlurmWorkerControllerConfig | None:
    if not enabled:
        return None

    allowed_nodes = tuple(
        node
        for node in (part.strip() for part in allowed_nodes_csv.split(","))
        if node
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

    _require_positive(requested_cpus, "requested_cpus")
    _require_positive(requested_memory_mib, "requested_memory_mib")
    _require_positive(requested_concurrency, "requested_concurrency")
    _require_positive(max_jobs, "max_jobs")
    _require_positive(pending_job_cap, "pending_job_cap")
    _require_positive(min_queued_trials, "min_queued_trials")
    _require_positive(stale_after_seconds, "stale_after_seconds")
    _require_positive(command_timeout_seconds, "command_timeout_seconds")

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
    )


def compute_controller_decision(
    config: ElasticSlurmWorkerControllerConfig,
    snapshot: SlurmWorkerCapacitySnapshot,
) -> SlurmWorkerControllerDecision:
    if snapshot.queued_trials < config.min_queued_trials:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=snapshot.cancellable_pending_job_ids,
            reason="queue_drained",
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
        )

    if snapshot.pending_jobs >= config.pending_job_cap:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=(),
            reason="pending_cap_reached",
        )

    if remaining_jobs <= 0:
        return SlurmWorkerControllerDecision(
            submit_nodes=(),
            cancel_job_ids=(),
            reason="max_jobs_reached",
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


def build_sbatch_request(
    config: ElasticSlurmWorkerControllerConfig,
    *,
    node: str,
) -> SbatchRequest:
    job_node = _SAFE_JOB_NAME_RE.sub("-", node).strip("-") or "worker"
    export = ",".join((
        "ALL",
        f"LOOM_WORKER_MAX_CONCURRENT={config.requested_concurrency}",
        f"LOOM_WORKER_POOL_NAME={config.pool_name}",
        f"LOOM_REMOTE_WORKER_ENV_FILE={config.env_file}",
        f"LOOM_REMOTE_WORKER_REPO_DIR={config.repo_dir}",
    ))
    args = [
        config.sbatch_path,
        "--parsable",
        f"--job-name=loom-worker-{job_node}",
        f"--nodelist={node}",
        "--exclusive",
        f"--time={config.time_limit}",
    ]
    if config.partition:
        args.append(f"--partition={config.partition}")
    args.extend((
        f"--cpus-per-task={config.requested_cpus}",
        f"--mem={config.requested_memory_mib}M",
        f"--export={export}",
    ))

    stdin = """#!/usr/bin/env bash
set -euo pipefail

cd "$LOOM_REMOTE_WORKER_REPO_DIR"
docker compose --env-file "$LOOM_REMOTE_WORKER_ENV_FILE" -f deploy/docker-compose.remote-worker.yml up --build
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


async def load_capacity_snapshot(
    session: AsyncSession,
    config: ElasticSlurmWorkerControllerConfig,
) -> SlurmWorkerCapacitySnapshot:
    queued_trials = int((await session.execute(_QUEUE_READY_STMT)).scalar_one())
    running_trials = int((await session.execute(_QUEUE_RUNNING_STMT)).scalar_one())
    jobs = (await session.execute(
        select(SlurmWorkerJob).where(
            SlurmWorkerJob.environment == config.environment,
            SlurmWorkerJob.pool_name == config.pool_name,
            SlurmWorkerJob.state.in_((*ACTIVE_STATES, "stale")),
        ),
    )).scalars().all()

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


async def run_elastic_slurm_worker_controller_once(
    session: AsyncSession,
    *,
    config: ElasticSlurmWorkerControllerConfig,
    runner: SlurmWorkerCommandRunner,
) -> SlurmWorkerControllerTickResult:
    snapshot = await load_capacity_snapshot(session, config)
    if snapshot.active_job_ids:
        try:
            observations = await runner.query_jobs(snapshot.active_job_ids)
            await reconcile_slurm_worker_jobs(
                session,
                observations,
                stale_after_seconds=config.stale_after_seconds,
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
        try:
            job_id = await runner.submit_worker(node=node, config=config)
            await record_slurm_worker_job(
                session,
                environment=config.environment,
                pool_name=config.pool_name,
                nodelist=node,
                requested_cpus=config.requested_cpus,
                requested_memory_mib=config.requested_memory_mib,
                requested_concurrency=config.requested_concurrency,
                job_id=job_id,
                slurm_state="PENDING",
                pending_reason=None,
                env=_worker_env(config),
                submitted_at=datetime.now(UTC),
            )
            submitted_job_ids.append(job_id)
        except Exception as exc:
            await record_slurm_worker_job(
                session,
                environment=config.environment,
                pool_name=config.pool_name,
                nodelist=node,
                requested_cpus=config.requested_cpus,
                requested_memory_mib=config.requested_memory_mib,
                requested_concurrency=config.requested_concurrency,
                job_id=None,
                slurm_state="FAILED",
                pending_reason=None,
                env=_worker_env(config),
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
    return {
        "LOOM_WORKER_MAX_CONCURRENT": str(config.requested_concurrency),
        "LOOM_WORKER_POOL_NAME": config.pool_name,
        "LOOM_REMOTE_WORKER_ENV_FILE": config.env_file,
        "LOOM_REMOTE_WORKER_REPO_DIR": config.repo_dir,
    }
