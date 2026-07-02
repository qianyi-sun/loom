"""Background consumer for ``task_set_materialization_jobs`` (#242 sub-plan 3)."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import Task, TaskSet, TaskSetManifest, TaskSetMaterializationJob
from loom.models.taskset import UserTaskSetManifest
from loom.taskset.materialize import MaterializeOutput, materialize_task_set

logger = logging.getLogger(__name__)

_CLAIMED_BY = f"{socket.gethostname()}:{os.getpid()}"
_ACTIVE_JOB_STATES = frozenset({"queued", "claimed", "running"})
_RETRY_BACKOFF_SEC = (30, 120, 300)


def _retry_delay_sec(attempt_count: int) -> int:
    idx = min(max(attempt_count - 1, 0), len(_RETRY_BACKOFF_SEC) - 1)
    return _RETRY_BACKOFF_SEC[idx]


async def reclaim_stale_jobs(
    session: AsyncSession,
    *,
    claim_ttl_sec: int,
) -> int:
    """Return stuck claimed/running jobs to queued with backoff."""
    cutoff = datetime.now(UTC) - timedelta(seconds=claim_ttl_sec)
    result = await session.execute(
        update(TaskSetMaterializationJob)
        .where(
            TaskSetMaterializationJob.state.in_(("claimed", "running")),
            TaskSetMaterializationJob.claimed_at.is_not(None),
            TaskSetMaterializationJob.claimed_at < cutoff,
        )
        .values(
            state="queued",
            claimed_at=None,
            claimed_by=None,
            started_at=None,
            next_attempt_at=datetime.now(UTC) + timedelta(seconds=30),
            updated_at=datetime.now(UTC),
        )
        .returning(TaskSetMaterializationJob.id),
    )
    reclaimed = len(result.all())
    if reclaimed:
        await session.commit()
    return reclaimed


async def _claim_jobs(
    session: AsyncSession,
    *,
    batch_size: int,
    claimed_by: str,
) -> list[TaskSetMaterializationJob]:
    now = datetime.now(UTC)
    jobs = (await session.execute(
        select(TaskSetMaterializationJob)
        .where(
            TaskSetMaterializationJob.state == "queued",
            (TaskSetMaterializationJob.next_attempt_at.is_(None))
            | (TaskSetMaterializationJob.next_attempt_at <= now),
        )
        .order_by(TaskSetMaterializationJob.enqueued_at)
        .with_for_update(skip_locked=True)
        .limit(batch_size),
    )).scalars().all()
    for job in jobs:
        job.state = "claimed"
        job.claimed_at = now
        job.claimed_by = claimed_by
        job.attempt_count += 1
        job.updated_at = now
    if jobs:
        await session.commit()
    return list(jobs)


async def _finalize_job(
    session: AsyncSession,
    *,
    job: TaskSetMaterializationJob,
    task_set: TaskSet,
    output: MaterializeOutput,
) -> None:
    now = datetime.now(UTC)
    if output.retry_source and job.attempt_count < job.max_attempts:
        job.state = "queued"
        job.claimed_at = None
        job.claimed_by = None
        job.started_at = None
        job.next_attempt_at = now + timedelta(seconds=_retry_delay_sec(job.attempt_count))
        job.failure_reason = output.job_failure_reason
        job.failure_message = output.job_failure_message
        job.updated_at = now
        task_set.status = "materializing"
        task_set.status_reason = output.status_reason
        task_set.updated_at = now
        await session.commit()
        return

    if output.retry_source and job.attempt_count >= job.max_attempts:
        output = MaterializeOutput(
            status="failed",
            status_reason="source_unreachable",
            job_failure_reason="source_unreachable",
            job_failure_message=output.job_failure_message,
            error_summary=output.error_summary,
        )

    job.state = "succeeded" if output.task_count > 0 else "failed"
    if output.job_failure_reason and output.task_count == 0:
        job.state = "failed"
    job.finished_at = now
    job.updated_at = now
    job.failure_reason = output.job_failure_reason
    job.failure_message = output.job_failure_message
    job.error_summary = output.error_summary

    task_set.status = output.status
    task_set.status_reason = output.status_reason
    task_set.task_count = output.task_count
    task_set.evaluation_ready = output.evaluation_ready
    task_set.updated_at = now
    await session.commit()


async def _materialize_claimed_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
) -> None:
    async with session_factory() as session:
        job = await session.get(TaskSetMaterializationJob, job_id)
        if job is None or job.state != "claimed":
            return
        task_set = await session.get(TaskSet, job.task_set_id)
        manifest_row = await session.get(TaskSetManifest, job.task_set_id)
        if task_set is None or manifest_row is None:
            job.state = "failed"
            job.failure_reason = "missing_task_set"
            job.failure_message = "task set or manifest row missing"
            job.finished_at = datetime.now(UTC)
            await session.commit()
            return

        job.state = "running"
        job.started_at = datetime.now(UTC)
        job.updated_at = datetime.now(UTC)
        await session.execute(
            delete(Task).where(Task.task_set_id == job.task_set_id),
        )
        await session.commit()

    async with session_factory() as session:
        job = await session.get(TaskSetMaterializationJob, job_id)
        task_set = await session.get(TaskSet, job.task_set_id) if job else None
        manifest_row = await session.get(TaskSetManifest, job.task_set_id) if job else None
        if job is None or task_set is None or manifest_row is None:
            return

        manifest = UserTaskSetManifest.model_validate(manifest_row.manifest)
        output = await asyncio.to_thread(
            materialize_task_set,
            manifest=manifest,
            task_set_id=task_set.id,
            owning_team_id=str(task_set.owning_team_id),
            intents=list(task_set.intents),
            verifier_blob_uri=manifest_row.verifier_blob_uri,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            upstream_cache_root=upstream_cache_root,
        )

        if output.task_rows and not output.retry_source:
            for row in output.task_rows:
                session.add(
                    Task(
                        id=row.id,
                        checksum=row.checksum,
                        config=row.config,
                        source=row.source,
                        task_set_id=task_set.id,
                        benchmark_id=None,
                    ),
                )

        await _finalize_job(session, job=job, task_set=task_set, output=output)


async def run_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    batch_size: int,
    claim_ttl_sec: int,
) -> None:
    async with session_factory() as session:
        await reclaim_stale_jobs(session, claim_ttl_sec=claim_ttl_sec)

    async with session_factory() as session:
        jobs = await _claim_jobs(
            session,
            batch_size=batch_size,
            claimed_by=_CLAIMED_BY,
        )

    for job in jobs:
        try:
            await _materialize_claimed_job(
                session_factory,
                job_id=job.id,
                minio_client=minio_client,
                artifacts_bucket=artifacts_bucket,
                upstream_cache_root=upstream_cache_root,
            )
        except Exception:
            logger.exception(
                "taskset_materializer failed job_id=%s task_set_id=%s",
                job.id,
                job.task_set_id,
            )
            async with session_factory() as session:
                db_job = await session.get(TaskSetMaterializationJob, job.id)
                task_set = await session.get(TaskSet, job.task_set_id)
                if db_job is not None:
                    now = datetime.now(UTC)
                    db_job.state = "failed"
                    db_job.failure_reason = "internal_error"
                    db_job.failure_message = "materialization worker crashed"
                    db_job.finished_at = now
                    db_job.updated_at = now
                    if task_set is not None:
                        task_set.status = "failed"
                        task_set.status_reason = "internal_error"
                        task_set.updated_at = now
                    await session.commit()


async def run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    batch_size: int,
    poll_interval_sec: int,
    claim_ttl_sec: int,
) -> None:
    upstream_cache_root.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            await run_once(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=artifacts_bucket,
                upstream_cache_root=upstream_cache_root,
                batch_size=batch_size,
                claim_ttl_sec=claim_ttl_sec,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("taskset_materializer iteration failed")
        await asyncio.sleep(poll_interval_sec)
