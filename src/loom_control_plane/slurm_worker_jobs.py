"""Slurm worker job registry and capacity summarization helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import SlurmWorkerJob, Worker
from loom.worker_token import (
    DEFAULT_WORKER_TOKEN_ENV_KEY,
    WORKER_AUTH_FINGERPRINT_ENV_KEY,
    worker_token_fingerprint,
)

logger = logging.getLogger(__name__)

ACTIVE_STATES = {"pending", "running"}
TERMINAL_STATES = {"completed", "failed", "cancelled", "stale"}
UNRECOVERABLE_SLURM_STATES = frozenset({"REQUEUE_HOLD"})
_SECRET_KEY_PARTS = (
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASS",
    "KEY",
    "CREDENTIAL",
)


def slurm_cluster_for_pool(pool_name: str) -> str:
    return "gb10" if pool_name == "gb10" or pool_name.endswith("-gb10") else "oldlab"


@dataclass(frozen=True)
class SlurmWorkerJobObservation:
    job_id: str
    slurm_state: str
    nodelist: str | None = None
    pending_reason: str | None = None
    worker_id: UUID | None = None
    observed_at: datetime | None = None


@dataclass
class SlurmWorkerPoolCapacity:
    environment: str
    pool_name: str
    desired_slots: int = 0
    active_slots: int = 0
    pending_slots: int = 0
    stale_slots: int = 0
    running_jobs: int = 0
    pending_jobs: int = 0
    stale_jobs: int = 0
    failed_submissions: int = 0
    cancelled_pending_jobs: int = 0
    idle_exits: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "pool_name": self.pool_name,
            "desired_slots": self.desired_slots,
            "active_slots": self.active_slots,
            "pending_slots": self.pending_slots,
            "stale_slots": self.stale_slots,
            "running_jobs": self.running_jobs,
            "pending_jobs": self.pending_jobs,
            "stale_jobs": self.stale_jobs,
            "failed_submissions": self.failed_submissions,
            "cancelled_pending_jobs": self.cancelled_pending_jobs,
            "idle_exits": self.idle_exits,
        }


@dataclass
class SlurmWorkerCapacitySummary:
    by_pool: dict[tuple[str, str], SlurmWorkerPoolCapacity] = field(
        default_factory=dict,
    )

    def as_list(self) -> list[dict[str, object]]:
        return [capacity.as_dict() for _, capacity in sorted(self.by_pool.items())]


@dataclass(frozen=True)
class SlurmWorkerJobReconcileResult:
    updated: int = 0
    stale: int = 0
    missing: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "updated": self.updated,
            "stale": self.stale,
            "missing": self.missing,
        }


def redact_env(env: dict[str, str] | None) -> dict[str, str]:
    if not env:
        return {}
    redacted: dict[str, str] = {}
    worker_token = env.get(DEFAULT_WORKER_TOKEN_ENV_KEY)
    for key, value in env.items():
        upper_key = key.upper()
        if any(part in upper_key for part in _SECRET_KEY_PARTS):
            redacted[key] = "<redacted>"
        else:
            redacted[key] = str(value)
    if worker_token:
        redacted[WORKER_AUTH_FINGERPRINT_ENV_KEY] = worker_token_fingerprint(
            str(worker_token),
        )
    return redacted


def _slurm_state_token(raw_state: str | None) -> str:
    return (raw_state or "PENDING").strip().upper().split(maxsplit=1)[0]


def is_unrecoverable_slurm_state(raw_state: str | None) -> bool:
    return _slurm_state_token(raw_state) in UNRECOVERABLE_SLURM_STATES


def _normalize_slurm_state(raw_state: str | None) -> str:
    if raw_state is None:
        return "pending"
    token = _slurm_state_token(raw_state)
    if token in {
        "PENDING",
        "CONFIGURING",
        "RESIZING",
        "REQUEUE_FED",
    }:
        return "pending"
    if token in {"RUNNING", "COMPLETING"}:
        return "running"
    if token == "COMPLETED":
        return "completed"
    if token.startswith("CANCELLED"):
        return "cancelled"
    if token in {
        "BOOT_FAIL",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "REVOKED",
        "TIMEOUT",
    }:
        return "failed"
    return "failed"


def normalize_slurm_state(raw_state: str | None) -> str:
    """Return Loom's registry state for one raw Slurm state token."""
    return _normalize_slurm_state(raw_state)


def summarize_jobs(rows: list[dict[str, Any]]) -> SlurmWorkerCapacitySummary:
    summary = SlurmWorkerCapacitySummary()
    for row in rows:
        environment = str(row["environment"])
        pool_name = str(row["pool_name"])
        key = (environment, pool_name)
        capacity = summary.by_pool.setdefault(
            key,
            SlurmWorkerPoolCapacity(environment=environment, pool_name=pool_name),
        )
        state = str(row.get("state") or "")
        requested_concurrency = int(row.get("requested_concurrency") or 0)
        worker_status = row.get("worker_status")
        if state in ACTIVE_STATES:
            capacity.desired_slots += requested_concurrency
        if state == "pending":
            capacity.pending_slots += requested_concurrency
            capacity.pending_jobs += 1
        elif state == "running":
            capacity.active_slots += requested_concurrency
            capacity.running_jobs += 1
        elif state == "stale":
            capacity.stale_slots += requested_concurrency
            capacity.stale_jobs += 1
        elif state == "failed" and (
            row.get("job_id") is None or row.get("submission_error") is not None
        ):
            capacity.failed_submissions += 1
        elif state == "cancelled" and row.get("started_at") is None:
            capacity.cancelled_pending_jobs += 1
        if worker_status == "idle-exit":
            capacity.idle_exits += 1
    return summary


def _active_capacity_query(
    *,
    environment: str,
    pool_name: str,
    nodelist: str,
    requested_cpus: int | None,
    requested_memory_mib: int | None,
    requested_pids: int | None,
    requested_gpu_tres: str | None,
    requested_gpus: int,
    requested_concurrency: int,
) -> Select[tuple[SlurmWorkerJob]]:
    stmt = select(SlurmWorkerJob).where(
        SlurmWorkerJob.environment == environment,
        SlurmWorkerJob.pool_name == pool_name,
        SlurmWorkerJob.nodelist == nodelist,
        SlurmWorkerJob.requested_concurrency == requested_concurrency,
        SlurmWorkerJob.state.in_(ACTIVE_STATES),
    )
    if requested_cpus is None:
        stmt = stmt.where(SlurmWorkerJob.requested_cpus.is_(None))
    else:
        stmt = stmt.where(SlurmWorkerJob.requested_cpus == requested_cpus)
    if requested_memory_mib is None:
        stmt = stmt.where(SlurmWorkerJob.requested_memory_mib.is_(None))
    else:
        stmt = stmt.where(SlurmWorkerJob.requested_memory_mib == requested_memory_mib)
    if requested_pids is None:
        stmt = stmt.where(SlurmWorkerJob.requested_pids.is_(None))
    else:
        stmt = stmt.where(SlurmWorkerJob.requested_pids == requested_pids)
    if requested_gpu_tres is None:
        stmt = stmt.where(SlurmWorkerJob.requested_gpu_tres.is_(None))
    else:
        stmt = stmt.where(SlurmWorkerJob.requested_gpu_tres == requested_gpu_tres)
    stmt = stmt.where(SlurmWorkerJob.requested_gpus == requested_gpus)
    return stmt


async def record_slurm_worker_job(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
    nodelist: str,
    requested_cpus: int | None,
    requested_memory_mib: int | None,
    requested_concurrency: int,
    job_id: str | None,
    slurm_state: str | None,
    pending_reason: str | None,
    env: dict[str, str] | None,
    requested_pids: int | None = None,
    requested_gpu_tres: str | None = None,
    requested_gpus: int = 0,
    sandbox_identity: str | None = None,
    candidate_sha: str | None = None,
    compose_project: str | None = None,
    submitted_at: datetime | None = None,
    submission_error: str | None = None,
    slurm_cluster_id: str | None = None,
) -> tuple[SlurmWorkerJob, bool]:
    slurm_cluster_id = slurm_cluster_id or slurm_cluster_for_pool(pool_name)
    if slurm_cluster_id not in {"oldlab", "gb10"}:
        raise ValueError("slurm_cluster_id must be oldlab or gb10")
    if slurm_cluster_id != slurm_cluster_for_pool(pool_name):
        raise ValueError("Slurm cluster does not match the worker pool")
    if job_id is not None:
        existing_by_job_id = (
            await session.execute(
                select(SlurmWorkerJob).where(
                    SlurmWorkerJob.slurm_cluster_id == slurm_cluster_id,
                    SlurmWorkerJob.job_id == job_id,
                ),
            )
        ).scalar_one_or_none()
        if existing_by_job_id is not None:
            logger.info(
                "slurm_worker_duplicate_job_id",
                extra={
                    "environment": environment,
                    "pool_name": pool_name,
                    "job_id": job_id,
                    "existing_state": existing_by_job_id.state,
                },
            )
            return existing_by_job_id, True

    existing = (
        await session.execute(
            _active_capacity_query(
                environment=environment,
                pool_name=pool_name,
                nodelist=nodelist,
                requested_cpus=requested_cpus,
                requested_memory_mib=requested_memory_mib,
                requested_pids=requested_pids,
                requested_gpu_tres=requested_gpu_tres,
                requested_gpus=requested_gpus,
                requested_concurrency=requested_concurrency,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        logger.info(
            "slurm_worker_duplicate_active_job",
            extra={
                "environment": environment,
                "pool_name": pool_name,
                "nodelist": nodelist,
                "existing_job_id": existing.job_id,
                "requested_concurrency": requested_concurrency,
            },
        )
        return existing, True

    now = datetime.now(UTC)
    state = _normalize_slurm_state(slurm_state)
    job = SlurmWorkerJob(
        slurm_cluster_id=slurm_cluster_id,
        environment=environment,
        pool_name=pool_name,
        nodelist=nodelist,
        requested_cpus=requested_cpus,
        requested_memory_mib=requested_memory_mib,
        requested_pids=requested_pids,
        requested_gpu_tres=requested_gpu_tres,
        requested_gpus=requested_gpus,
        requested_concurrency=requested_concurrency,
        sandbox_identity=sandbox_identity,
        candidate_sha=candidate_sha,
        compose_project=compose_project,
        job_id=job_id,
        slurm_state=slurm_state,
        state=state,
        pending_reason=pending_reason,
        redacted_env=redact_env(env),
        submission_error=submission_error,
        submitted_at=submitted_at or now,
        started_at=now if state == "running" else None,
        finished_at=now if state in TERMINAL_STATES else None,
        updated_at=now,
    )
    session.add(job)
    await session.flush()
    logger.info(
        "slurm_worker_job_recorded",
        extra={
            "environment": environment,
            "pool_name": pool_name,
            "nodelist": nodelist,
            "job_id": job_id,
            "state": state,
            "pending_reason": pending_reason,
            "requested_concurrency": requested_concurrency,
        },
    )
    return job, False


async def reconcile_slurm_worker_jobs(
    session: AsyncSession,
    observations: list[SlurmWorkerJobObservation],
    *,
    stale_after_seconds: int,
    environment: str | None = None,
    pool_name: str | None = None,
    now: datetime | None = None,
    slurm_cluster_id: str | None = None,
) -> SlurmWorkerJobReconcileResult:
    if (environment is None) != (pool_name is None):
        raise ValueError("environment and pool_name must be provided together")
    if slurm_cluster_id is not None and slurm_cluster_id not in {"oldlab", "gb10"}:
        raise ValueError("slurm_cluster_id must be oldlab or gb10")
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(seconds=stale_after_seconds)
    observed_job_ids = {obs.job_id for obs in observations}
    updated = 0
    stale = 0
    missing = 0
    for obs in observations:
        job_stmt = select(SlurmWorkerJob).where(
            SlurmWorkerJob.job_id == obs.job_id,
        )
        if slurm_cluster_id is not None:
            job_stmt = job_stmt.where(
                SlurmWorkerJob.slurm_cluster_id == slurm_cluster_id
            )
        if environment is not None and pool_name is not None:
            job_stmt = job_stmt.where(
                SlurmWorkerJob.environment == environment,
                SlurmWorkerJob.pool_name == pool_name,
            )
        job = (
            await session.execute(job_stmt)
        ).scalar_one_or_none()
        if job is None:
            missing += 1
            logger.info(
                "slurm_worker_observation_without_record",
                extra={
                    "job_id": obs.job_id,
                    "slurm_state": obs.slurm_state,
                    "slurm_cluster_id": slurm_cluster_id,
                },
            )
            continue
        state = _normalize_slurm_state(obs.slurm_state)
        job.slurm_state = obs.slurm_state
        job.state = state
        if obs.nodelist is not None:
            job.nodelist = obs.nodelist
        job.pending_reason = obs.pending_reason
        if obs.worker_id is not None:
            job.worker_id = obs.worker_id
        if state == "running" and job.started_at is None:
            job.started_at = now
        if state in TERMINAL_STATES and job.finished_at is None:
            job.finished_at = now
        job.last_reconciled_at = obs.observed_at or now
        job.updated_at = now
        updated += 1
        if state == "running" and job.worker_id is not None:
            worker = (
                await session.execute(
                    select(Worker).where(Worker.id == job.worker_id),
                )
            ).scalar_one_or_none()
            if worker is None or worker.status != "active" or worker.last_seen_at <= cutoff:
                job.state = "stale"
                job.pending_reason = "worker heartbeat stale"
                job.stale_at = now
                job.finished_at = now
                job.updated_at = now
                stale += 1
                logger.info(
                    "slurm_worker_job_marked_stale_worker_heartbeat",
                    extra={
                        "job_id": job.job_id,
                        "environment": job.environment,
                        "pool_name": job.pool_name,
                        "nodelist": job.nodelist,
                        "worker_id": str(job.worker_id),
                    },
                )
        if obs.pending_reason:
            logger.info(
                "slurm_worker_pending_reason",
                extra={"job_id": obs.job_id, "pending_reason": obs.pending_reason},
            )
        if state == "cancelled":
            logger.info(
                "slurm_worker_job_cancelled",
                extra={
                    "job_id": obs.job_id,
                    "environment": job.environment,
                    "pool_name": job.pool_name,
                    "nodelist": job.nodelist,
                    "pending_reason": obs.pending_reason,
                    "started": job.started_at is not None,
                },
            )

    active_stmt = select(SlurmWorkerJob).where(SlurmWorkerJob.state.in_(ACTIVE_STATES))
    if environment is not None and pool_name is not None:
        active_stmt = active_stmt.where(
            SlurmWorkerJob.environment == environment,
            SlurmWorkerJob.pool_name == pool_name,
        )
    if slurm_cluster_id is not None:
        active_stmt = active_stmt.where(SlurmWorkerJob.slurm_cluster_id == slurm_cluster_id)
    if observed_job_ids:
        active_stmt = active_stmt.where(
            or_(
                SlurmWorkerJob.job_id.is_(None),
                SlurmWorkerJob.job_id.not_in(observed_job_ids),
            )
        )
    stale_candidates = (await session.execute(active_stmt)).scalars().all()
    for job in stale_candidates:
        last_seen = job.last_reconciled_at or job.submitted_at or job.created_at
        if last_seen is not None and last_seen > cutoff:
            continue
        job.state = "stale"
        job.pending_reason = "not reported by Slurm reconcile"
        job.stale_at = now
        job.finished_at = now
        job.updated_at = now
        stale += 1
        logger.info(
            "slurm_worker_job_marked_stale",
            extra={
                "job_id": job.job_id,
                "environment": job.environment,
                "pool_name": job.pool_name,
                "nodelist": job.nodelist,
            },
        )

    await session.flush()
    return SlurmWorkerJobReconcileResult(updated=updated, stale=stale, missing=missing)


def slurm_worker_job_to_dict(job: SlurmWorkerJob) -> dict[str, object]:
    def _dt(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    return {
        "id": str(job.id),
        "slurm_cluster_id": job.slurm_cluster_id,
        "environment": job.environment,
        "pool_name": job.pool_name,
        "nodelist": job.nodelist,
        "requested_cpus": job.requested_cpus,
        "requested_memory_mib": job.requested_memory_mib,
        "requested_pids": job.requested_pids,
        "requested_gpu_tres": job.requested_gpu_tres,
        "requested_gpus": job.requested_gpus,
        "requested_concurrency": job.requested_concurrency,
        "sandbox_identity": job.sandbox_identity,
        "candidate_sha": job.candidate_sha,
        "compose_project": job.compose_project,
        "job_id": job.job_id,
        "slurm_state": job.slurm_state,
        "state": job.state,
        "pending_reason": job.pending_reason,
        "worker_id": str(job.worker_id) if job.worker_id is not None else None,
        "redacted_env": job.redacted_env,
        "submission_error": job.submission_error,
        "submitted_at": _dt(job.submitted_at),
        "started_at": _dt(job.started_at),
        "finished_at": _dt(job.finished_at),
        "last_reconciled_at": _dt(job.last_reconciled_at),
        "stale_at": _dt(job.stale_at),
        "created_at": _dt(job.created_at),
        "updated_at": _dt(job.updated_at),
    }


async def fetch_slurm_worker_job_status(
    session: AsyncSession,
    *,
    active_only: bool = False,
) -> tuple[SlurmWorkerCapacitySummary, list[dict[str, object]]]:
    stmt = (
        select(
            SlurmWorkerJob,
            Worker.status.label("worker_status"),
        )
        .outerjoin(Worker, SlurmWorkerJob.worker_id == Worker.id)
        .order_by(
            SlurmWorkerJob.environment,
            SlurmWorkerJob.pool_name,
            SlurmWorkerJob.created_at,
        )
    )
    if active_only:
        stmt = stmt.where(SlurmWorkerJob.state.not_in(TERMINAL_STATES))
    rows = (await session.execute(stmt)).all()
    summary_rows: list[dict[str, Any]] = []
    jobs: list[dict[str, object]] = []
    for job, worker_status in rows:
        summary_rows.append(
            {
                "environment": job.environment,
                "pool_name": job.pool_name,
                "state": job.state,
                "requested_concurrency": job.requested_concurrency,
                "job_id": job.job_id,
                "started_at": job.started_at,
                "submission_error": job.submission_error,
                "worker_status": worker_status,
            }
        )
        jobs.append(slurm_worker_job_to_dict(job))
    return summarize_jobs(summary_rows), jobs


async def fetch_slurm_worker_metric_rows(session: AsyncSession) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            select(
                SlurmWorkerJob.environment,
                SlurmWorkerJob.pool_name,
                SlurmWorkerJob.state,
                SlurmWorkerJob.requested_concurrency,
                SlurmWorkerJob.job_id,
                SlurmWorkerJob.started_at,
                SlurmWorkerJob.submission_error,
                Worker.status.label("worker_status"),
            ).outerjoin(Worker, SlurmWorkerJob.worker_id == Worker.id),
        )
    ).all()
    return [
        {
            "environment": environment,
            "pool_name": pool_name,
            "state": state,
            "requested_concurrency": requested_concurrency,
            "job_id": job_id,
            "started_at": started_at,
            "submission_error": submission_error,
            "worker_status": worker_status,
        }
        for (
            environment,
            pool_name,
            state,
            requested_concurrency,
            job_id,
            started_at,
            submission_error,
            worker_status,
        ) in rows
    ]


async def count_slurm_worker_jobs(session: AsyncSession) -> int:
    return int(
        (
            await session.execute(
                select(func.count()).select_from(SlurmWorkerJob),
            )
        ).scalar_one()
    )
