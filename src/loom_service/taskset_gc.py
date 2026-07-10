"""Background cleanup for retained TaskSets and abandoned staged generations."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import logging
import re
from collections import defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import Task, TaskSet, TaskSetManifest, TaskSetMaterializationJob
from loom.taskset.storage_bytes import (
    generated_tasks_prefix,
    generation_prefix,
    generation_root,
    taskset_root,
)

logger = logging.getLogger(__name__)

_ACTIVE_JOB_STATES = frozenset({"claimed", "running"})
_CANONICAL_EPOCH_RE = re.compile(r"(?:0|[1-9][0-9]*)$")
_DELETE_BATCH_SIZE = 1_000
_ROOT_GC_OBJECT_DELETE_BUDGET = 1_000
_LIVE_GC_TASK_SET_LIMIT = 100
_LIVE_GC_JOB_LIMIT = 100
_LIVE_GC_OBJECT_DELETE_BUDGET = 1_000


@dataclass(frozen=True)
class PrefixDeleteResult:
    """The bounded outcome of one S3 prefix-delete attempt."""

    attempted_objects: int = 0
    deleted_objects: int = 0
    error_objects: int = 0
    partial: bool = False


@dataclass(frozen=True)
class MaterializationGenerationGcResult:
    """Counters for one bounded live-generation reconciliation pass."""

    candidate_generations: int = 0
    protected_generations: int = 0
    attempted_objects: int = 0
    deleted_objects: int = 0
    error_objects: int = 0
    partial: bool = False


@dataclass(frozen=True)
class _LiveTaskSet:
    id: str
    owning_team_id: UUID
    slug: str


@dataclass(frozen=True)
class _LiveJob:
    id: UUID
    task_set_id: str
    state: str
    lease_epoch: int


@dataclass(frozen=True)
class _GenerationCandidate:
    task_set: _LiveTaskSet
    job: _LiveJob
    epoch: int
    prefix: str
    tasks_prefix: str


def _storage_prefix(*, team_id: UUID | str, slug: str) -> str:
    """Compatibility wrapper for the delimiter-safe TaskSet root."""
    return taskset_root(team_id=team_id, slug=slug)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"expected s3 uri, got {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri!r}")
    return bucket, key


@contextlib.contextmanager
def _with_delete_objects_content_md5(client: Any) -> Iterator[None]:
    """Add the MD5 MinIO requires after Botocore has serialized each delete body."""
    events = getattr(getattr(client, "meta", None), "events", None)
    if events is None:
        yield
        return

    def add_content_md5(*, request: Any, **_kwargs: Any) -> None:
        body = request.body
        if isinstance(body, bytes):
            digest = hashlib.md5(body, usedforsecurity=False).digest()
            request.headers["Content-MD5"] = base64.b64encode(digest).decode("ascii")

    event_name = "before-sign.s3.DeleteObjects"
    try:
        events.register(event_name, add_content_md5)
    except Exception:
        logger.warning("taskset_gc: could not register DeleteObjects MD5 hook", exc_info=True)
        yield
        return
    try:
        yield
    finally:
        events.unregister(event_name, add_content_md5)


def _delete_s3_prefix(
    client: Any,
    *,
    bucket: str,
    prefix: str,
    max_objects: int | None = None,
) -> PrefixDeleteResult:
    """Delete up to ``max_objects`` below an exact prefix and report every error."""
    if max_objects is not None and max_objects <= 0:
        return PrefixDeleteResult(partial=True)

    attempted = 0
    deleted = 0
    errors = 0
    partial = False
    remaining = max_objects
    try:
        with _with_delete_objects_content_md5(client):
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                objects = page.get("Contents", [])
                keys = [
                    {"Key": key}
                    for obj in objects
                    if isinstance((key := obj.get("Key")), str)
                ]
                if not keys:
                    continue
                if remaining is not None and remaining <= 0:
                    partial = True
                    break
                if remaining is not None and len(keys) > remaining:
                    keys = keys[:remaining]
                    partial = True

                for start in range(0, len(keys), _DELETE_BATCH_SIZE):
                    batch = keys[start:start + _DELETE_BATCH_SIZE]
                    attempted += len(batch)
                    try:
                        response = client.delete_objects(
                            Bucket=bucket,
                            Delete={"Objects": batch},
                        )
                    except Exception:
                        logger.warning(
                            "taskset_gc: delete_objects failed for prefix %s", prefix,
                            exc_info=True,
                        )
                        errors += len(batch)
                        continue

                    if not isinstance(response, Mapping):
                        logger.warning(
                            "taskset_gc: delete_objects returned an invalid result for %s",
                            prefix,
                        )
                        errors += len(batch)
                        continue
                    deleted_entries = response.get("Deleted", [])
                    error_entries = response.get("Errors", [])
                    if not isinstance(deleted_entries, list) or not isinstance(error_entries, list):
                        logger.warning(
                            "taskset_gc: delete_objects omitted object results for %s",
                            prefix,
                        )
                        errors += len(batch)
                        continue
                    deleted_count = min(len(deleted_entries), len(batch))
                    error_count = min(len(error_entries), len(batch) - deleted_count)
                    deleted += deleted_count
                    errors += error_count + (len(batch) - deleted_count - error_count)

                if remaining is not None:
                    remaining -= len(keys)
                    if remaining == 0:
                        partial = True
                        break
    except Exception:
        logger.warning(
            "taskset_gc: listing prefix %s failed", prefix,
            exc_info=True,
        )
        errors += 1

    return PrefixDeleteResult(
        attempted_objects=attempted,
        deleted_objects=deleted,
        error_objects=errors,
        partial=partial,
    )


def _delete_single_blob(client: Any, blob_uri: str | None) -> None:
    if blob_uri is None:
        return
    try:
        bucket, key = _parse_s3_uri(blob_uri)
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        logger.warning("gc: failed to delete blob %s", blob_uri, exc_info=True)


def _listed_generation_epochs(
    client: Any,
    *,
    bucket: str,
    job_prefix: str,
    max_epoch: int,
) -> set[int]:
    """Discover only canonical epoch segments under one DB-derived job prefix."""
    epochs: set[int] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=job_prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not isinstance(key, str) or not key.startswith(job_prefix):
                continue
            epoch_segment, separator, remainder = key[len(job_prefix):].partition("/")
            if not separator or not remainder or not _CANONICAL_EPOCH_RE.fullmatch(epoch_segment):
                continue
            epoch = int(epoch_segment)
            if epoch <= max_epoch:
                epochs.add(epoch)
    return epochs


def _source_protects_candidate(
    source: str | None,
    *,
    artifacts_bucket: str,
    candidate: _GenerationCandidate,
) -> bool:
    if source is None:
        return False
    try:
        bucket, key = _parse_s3_uri(source)
    except ValueError:
        return False
    return bucket == artifacts_bucket and key.startswith(candidate.tasks_prefix)


async def _candidate_is_protected_after_recheck(
    session: AsyncSession,
    *,
    candidate: _GenerationCandidate,
    artifacts_bucket: str,
) -> tuple[bool, bool]:
    """Return ``(protected, query_failed)`` from a short final DB snapshot."""
    try:
        task_set = await session.get(TaskSet, candidate.task_set.id)
        job = await session.get(TaskSetMaterializationJob, candidate.job.id)
        if (
            task_set is None
            or task_set.soft_deleted_at is not None
            or task_set.status == "deleted"
            or job is None
            or job.task_set_id != candidate.task_set.id
            or candidate.epoch > job.lease_epoch
        ):
            return True, False

        current_prefix = generation_prefix(
            team_id=task_set.owning_team_id,
            slug=task_set.slug,
            job_id=job.id,
            epoch=candidate.epoch,
        )
        if current_prefix != candidate.prefix:
            return True, False
        if job.state in _ACTIVE_JOB_STATES and candidate.epoch == job.lease_epoch:
            return True, False

        sources = (await session.execute(
            select(Task.source).where(Task.task_set_id == task_set.id),
        )).scalars().all()
        return any(
            _source_protects_candidate(
                source,
                artifacts_bucket=artifacts_bucket,
                candidate=candidate,
            )
            for source in sources
        ), False
    except Exception:
        logger.warning(
            "taskset_gc: final DB recheck failed for generation %s",
            candidate.prefix,
            exc_info=True,
        )
        return True, True
    finally:
        await session.rollback()


async def purge_abandoned_materialization_generations(
    session: AsyncSession,
    *,
    minio_client: Any,
    artifacts_bucket: str,
    task_set_limit: int = _LIVE_GC_TASK_SET_LIMIT,
    job_limit: int = _LIVE_GC_JOB_LIMIT,
    object_delete_budget: int = _LIVE_GC_OBJECT_DELETE_BUDGET,
) -> MaterializationGenerationGcResult:
    """Boundedly remove unreferenced generated output from live TaskSets.

    Object listings and deletes occur only after the candidate SQL snapshot has
    been closed. Every deletion prefix is reconstructed from a TaskSet row, a
    materialization-job UUID, and a persisted epoch; object keys never choose a
    target prefix.
    """
    if task_set_limit <= 0 or job_limit <= 0 or object_delete_budget < 0:
        raise ValueError("live generation GC bounds must be positive (budget may be zero)")

    try:
        task_set_rows = (await session.execute(
            select(TaskSet.id, TaskSet.owning_team_id, TaskSet.slug)
            .where(
                TaskSet.soft_deleted_at.is_(None),
                TaskSet.status != "deleted",
            )
            .order_by(TaskSet.created_at, TaskSet.id)
            .limit(task_set_limit),
        )).all()
        task_sets = {
            row.id: _LiveTaskSet(
                id=row.id,
                owning_team_id=row.owning_team_id,
                slug=row.slug,
            )
            for row in task_set_rows
        }
        if not task_sets:
            return MaterializationGenerationGcResult()

        job_rows = (await session.execute(
            select(
                TaskSetMaterializationJob.id,
                TaskSetMaterializationJob.task_set_id,
                TaskSetMaterializationJob.state,
                TaskSetMaterializationJob.lease_epoch,
            )
            .where(TaskSetMaterializationJob.task_set_id.in_(task_sets))
            .order_by(
                TaskSetMaterializationJob.enqueued_at,
                TaskSetMaterializationJob.id,
            )
            .limit(job_limit),
        )).all()
        source_rows = (await session.execute(
            select(Task.task_set_id, Task.source).where(Task.task_set_id.in_(task_sets)),
        )).all()
    finally:
        # Do not hold a DB transaction while traversing object storage.
        await session.rollback()

    sources_by_task_set: dict[str, list[str | None]] = defaultdict(list)
    for row in source_rows:
        sources_by_task_set[row.task_set_id].append(row.source)

    jobs = [
        _LiveJob(
            id=row.id,
            task_set_id=row.task_set_id,
            state=row.state,
            lease_epoch=row.lease_epoch,
        )
        for row in job_rows
        if row.task_set_id in task_sets
    ]
    candidates: dict[tuple[str, UUID, int], _GenerationCandidate] = {}
    errors = 0
    for job in jobs:
        task_set = task_sets[job.task_set_id]
        try:
            job_prefix = f"{generation_root(team_id=task_set.owning_team_id, slug=task_set.slug)}{job.id}/"
            epochs = await asyncio.to_thread(
                _listed_generation_epochs,
                minio_client,
                bucket=artifacts_bucket,
                job_prefix=job_prefix,
                max_epoch=job.lease_epoch,
            )
        except Exception:
            logger.warning(
                "taskset_gc: failed to list generation prefix for job %s", job.id,
                exc_info=True,
            )
            errors += 1
            continue
        for epoch in epochs:
            try:
                candidate = _GenerationCandidate(
                    task_set=task_set,
                    job=job,
                    epoch=epoch,
                    prefix=generation_prefix(
                        team_id=task_set.owning_team_id,
                        slug=task_set.slug,
                        job_id=job.id,
                        epoch=epoch,
                    ),
                    tasks_prefix=generated_tasks_prefix(
                        team_id=task_set.owning_team_id,
                        slug=task_set.slug,
                        job_id=job.id,
                        epoch=epoch,
                    ),
                )
            except ValueError:
                logger.warning(
                    "taskset_gc: skipped malformed DB generation for job %s", job.id,
                    exc_info=True,
                )
                errors += 1
                continue
            candidates[(task_set.id, job.id, epoch)] = candidate

    protected = 0
    deletable: list[_GenerationCandidate] = []
    for candidate in sorted(
        candidates.values(),
        key=lambda item: (item.task_set.id, str(item.job.id), item.epoch),
    ):
        active_epoch = (
            candidate.job.state in _ACTIVE_JOB_STATES
            and candidate.epoch == candidate.job.lease_epoch
        )
        referenced = any(
            _source_protects_candidate(
                source,
                artifacts_bucket=artifacts_bucket,
                candidate=candidate,
            )
            for source in sources_by_task_set[candidate.task_set.id]
        )
        if active_epoch or referenced:
            protected += 1
        else:
            deletable.append(candidate)

    attempted = 0
    deleted = 0
    partial = False
    remaining_budget = object_delete_budget
    for candidate in deletable:
        if remaining_budget <= 0:
            partial = True
            break
        rechecked_protected, recheck_failed = await _candidate_is_protected_after_recheck(
            session,
            candidate=candidate,
            artifacts_bucket=artifacts_bucket,
        )
        if rechecked_protected:
            protected += 1
            errors += int(recheck_failed)
            continue
        delete_result = await asyncio.to_thread(
            _delete_s3_prefix,
            minio_client,
            bucket=artifacts_bucket,
            prefix=candidate.prefix,
            max_objects=remaining_budget,
        )
        attempted += delete_result.attempted_objects
        deleted += delete_result.deleted_objects
        errors += delete_result.error_objects
        remaining_budget -= delete_result.attempted_objects
        partial = partial or delete_result.partial

    return MaterializationGenerationGcResult(
        candidate_generations=len(candidates),
        protected_generations=protected,
        attempted_objects=attempted,
        deleted_objects=deleted,
        error_objects=errors,
        partial=partial,
    )


async def purge_expired_task_sets(
    session: AsyncSession,
    *,
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
    object_delete_budget: int = _ROOT_GC_OBJECT_DELETE_BUDGET,
) -> int:
    """Purge retention-expired TaskSet roots. Returns hard-deleted row count."""
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    try:
        expired_rows = (await session.execute(
            select(TaskSet.id, TaskSet.owning_team_id, TaskSet.slug)
            .where(
                TaskSet.soft_deleted_at.is_not(None),
                TaskSet.soft_deleted_at < cutoff,
            )
            .order_by(TaskSet.soft_deleted_at, TaskSet.id)
            .limit(100),
        )).all()
    finally:
        await session.rollback()

    purged = 0
    for row in expired_rows:
        try:
            prefix = _storage_prefix(team_id=row.owning_team_id, slug=row.slug)
            manifest_row = (await session.execute(
                select(TaskSetManifest).where(TaskSetManifest.task_set_id == row.id),
            )).scalar_one_or_none()
            verifier_blob_uri = (
                manifest_row.verifier_blob_uri if manifest_row is not None else None
            )
            transform_blob_uri = (
                manifest_row.transform_blob_uri if manifest_row is not None else None
            )
            await session.rollback()

            delete_result = await asyncio.to_thread(
                _delete_s3_prefix,
                minio_client,
                bucket=artifacts_bucket,
                prefix=prefix,
                max_objects=object_delete_budget,
            )
            if delete_result.partial or delete_result.error_objects:
                logger.warning(
                    "taskset_gc: retained expired root %s after partial/error delete "
                    "attempted=%d deleted=%d errors=%d",
                    row.id,
                    delete_result.attempted_objects,
                    delete_result.deleted_objects,
                    delete_result.error_objects,
                )
                continue
            await asyncio.to_thread(_delete_single_blob, minio_client, verifier_blob_uri)
            await asyncio.to_thread(_delete_single_blob, minio_client, transform_blob_uri)

            current = await session.get(TaskSet, row.id)
            if (
                current is None
                or current.soft_deleted_at is None
                or current.soft_deleted_at >= cutoff
            ):
                await session.rollback()
                continue
            await session.execute(delete(Task).where(Task.task_set_id == row.id))
            await session.execute(
                delete(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == row.id,
                ),
            )
            await session.execute(delete(TaskSetManifest).where(TaskSetManifest.task_set_id == row.id))
            await session.execute(delete(TaskSet).where(TaskSet.id == row.id))
            await session.commit()
            purged += 1
        except Exception:
            logger.exception("gc: failed to purge task_set %s", row.id)
            await session.rollback()

    return purged


async def run_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
) -> tuple[int, MaterializationGenerationGcResult]:
    """Run both independent TaskSet cleanup contracts once."""
    async with session_factory() as session:
        purged = await purge_expired_task_sets(
            session,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            retention_days=retention_days,
        )
    async with session_factory() as session:
        generation_result = await purge_abandoned_materialization_generations(
            session,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
        )
    return purged, generation_result


async def run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
    poll_interval_sec: int,
) -> None:
    while True:
        try:
            purged, generation_result = await run_once(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=artifacts_bucket,
                retention_days=retention_days,
            )
            if purged > 0:
                logger.info("taskset_gc purged %d expired task sets", purged)
            if (
                generation_result.deleted_objects
                or generation_result.error_objects
                or generation_result.partial
            ):
                logger.info(
                    "taskset_generation_gc candidates=%d protected=%d attempted=%d "
                    "deleted=%d errors=%d partial=%s",
                    generation_result.candidate_generations,
                    generation_result.protected_generations,
                    generation_result.attempted_objects,
                    generation_result.deleted_objects,
                    generation_result.error_objects,
                    generation_result.partial,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("taskset_gc iteration failed")
        await asyncio.sleep(poll_interval_sec)
