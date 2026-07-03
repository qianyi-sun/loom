"""Background GC for soft-deleted TaskSets (#242 sub-plan 7).

Purges object-storage blobs and hard-deletes DB rows for TaskSets whose
``soft_deleted_at`` exceeds the configurable retention window (default 7 days).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import Task, TaskSet, TaskSetManifest, TaskSetMaterializationJob

logger = logging.getLogger(__name__)


def _storage_prefix(*, team_id: str, slug: str) -> str:
    return f"tasksets/user/{team_id}/{slug}"


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"expected s3 uri, got {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri!r}")
    return bucket, key


def _delete_s3_prefix(
    client: Any,
    *,
    bucket: str,
    prefix: str,
) -> int:
    """Delete all objects under a prefix. Returns count of deleted objects."""
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        keys = [{"Key": obj["Key"]} for obj in objects]
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        deleted += len(keys)
    return deleted


def _delete_single_blob(client: Any, blob_uri: str | None) -> None:
    if blob_uri is None:
        return
    try:
        bucket, key = _parse_s3_uri(blob_uri)
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        logger.warning("gc: failed to delete blob %s", blob_uri, exc_info=True)


async def purge_expired_task_sets(
    session: AsyncSession,
    *,
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
) -> int:
    """Find and purge TaskSets past the retention window. Returns purge count."""
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    expired_rows = (await session.execute(
        select(TaskSet).where(
            TaskSet.soft_deleted_at.is_not(None),
            TaskSet.soft_deleted_at < cutoff,
        ).limit(100),
    )).scalars().all()

    if not expired_rows:
        return 0

    purged = 0
    for task_set in expired_rows:
        try:
            prefix = _storage_prefix(
                team_id=str(task_set.owning_team_id),
                slug=task_set.slug,
            )
            await asyncio.to_thread(
                _delete_s3_prefix,
                minio_client,
                bucket=artifacts_bucket,
                prefix=prefix,
            )

            manifest_row = (await session.execute(
                select(TaskSetManifest).where(
                    TaskSetManifest.task_set_id == task_set.id,
                ),
            )).scalar_one_or_none()
            if manifest_row is not None:
                await asyncio.to_thread(
                    _delete_single_blob, minio_client, manifest_row.verifier_blob_uri,
                )
                await asyncio.to_thread(
                    _delete_single_blob, minio_client, manifest_row.transform_blob_uri,
                )

            await session.execute(
                delete(Task).where(Task.task_set_id == task_set.id),
            )
            await session.execute(
                delete(TaskSetMaterializationJob).where(
                    TaskSetMaterializationJob.task_set_id == task_set.id,
                ),
            )
            await session.execute(
                delete(TaskSetManifest).where(
                    TaskSetManifest.task_set_id == task_set.id,
                ),
            )
            await session.execute(
                delete(TaskSet).where(TaskSet.id == task_set.id),
            )
            await session.commit()
            purged += 1
        except Exception:
            logger.exception(
                "gc: failed to purge task_set %s", task_set.id,
            )
            await session.rollback()

    return purged


async def run_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    retention_days: int,
) -> int:
    async with session_factory() as session:
        return await purge_expired_task_sets(
            session,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            retention_days=retention_days,
        )


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
            purged = await run_once(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=artifacts_bucket,
                retention_days=retention_days,
            )
            if purged > 0:
                logger.info("taskset_gc purged %d expired task sets", purged)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("taskset_gc iteration failed")
        await asyncio.sleep(poll_interval_sec)
