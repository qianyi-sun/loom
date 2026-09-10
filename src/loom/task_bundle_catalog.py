"""Atomic catalog/source/image publication with one consistent lock order."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Task,
    TaskBundleSourceIncarnation,
    TaskBundleSourceReference,
    TaskImageMaterialization,
)
from loom.task_bundle_source_journal import (
    TaskBundleUpload,
    _source,
    admit_task_bundle_source,
    publish_task_bundle_source,
    release_task_bundle_reference,
    require_task_bundle_transaction,
)
from loom.task_image_materialization import (
    _assert_no_pending_task_image_writes,
    _lock_task_image_materializations,
    _reference_task_image_materializations,
    task_bundle_content_manifest_digest,
)


async def publish_task_bundle_catalog(
    session: AsyncSession,
    *,
    tasks: Sequence[Task],
    uploads: Mapping[str, TaskBundleUpload],
    now: datetime,
) -> dict[str, tuple[TaskImageMaterialization, ...]]:
    """Publish previously prepared uploads in the caller's catalog transaction.

    The caller first owns its benchmark or taskset/job authority and writes its
    Task rows. This function takes sorted Task, image, then logical-source locks;
    it performs no storage I/O or commit. Roll back the entire caller transaction
    on any error. No task/image/source publication becomes visible on its own.

    Existing source pins on historical image rows remain intact. Only the
    replaced catalog references are released here; image/trial retirement owns
    their separate references. This is not a producer-default or GC switch.
    """
    task_ids = [task.id for task in tasks]
    if len(set(task_ids)) != len(task_ids) or set(uploads) != set(task_ids):
        raise ValueError("catalog publication requires one exact upload per task")
    if not tasks:
        return {}
    _assert_no_pending_task_image_writes(session)
    await require_task_bundle_transaction(session)
    await session.flush()
    locked_tasks = tuple(
        await session.scalars(
            select(Task)
            .where(Task.id.in_(task_ids))
            .order_by(Task.id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    )
    if {task.id for task in locked_tasks} != set(task_ids):
        raise ValueError("catalog publication requires persisted caller-owned Task rows")
    sources: dict[str, str] = {}
    for task in locked_tasks:
        if not task.source or not task_bundle_content_manifest_digest(task.source_provenance):
            raise ValueError("catalog publication requires registered strong sources")
        source_id = hashlib.sha256(task.source.encode()).hexdigest()
        if uploads[task.id].source_id != source_id:
            raise ValueError("catalog source upload ticket conflicts")
        sources[task.id] = source_id
    # Incarnation parent identity is immutable. Validate it before taking any
    # source lock, so a forged ticket cannot introduce an out-of-order parent.
    parents: dict[UUID, str] = {
        incarnation_id: source_id
        for incarnation_id, source_id in (
            await session.execute(
                select(TaskBundleSourceIncarnation.id, TaskBundleSourceIncarnation.source_id).where(
                    TaskBundleSourceIncarnation.id.in_(
                        ticket.incarnation_id for ticket in uploads.values()
                    )
                )
            )
        ).all()
    }
    if any(
        parents.get(uploads[key].incarnation_id) != source_id for key, source_id in sources.items()
    ):
        raise ValueError("catalog source incarnation conflicts")

    result = {
        task.id: await _lock_task_image_materializations(session, task_row=task)
        for task in locked_tasks
    }
    previous = (
        await session.execute(
            select(TaskBundleSourceReference.source_id, TaskBundleSourceReference.owner_id).where(
                TaskBundleSourceReference.kind == "catalog",
                TaskBundleSourceReference.owner_id.in_(task_ids),
            )
        )
    ).all()
    for source_id in sorted(set(sources.values()) | {row.source_id for row in previous}):
        await _source(session, source_id)
    for task in sorted(locked_tasks, key=lambda task: sources[task.id]):
        await publish_task_bundle_source(
            session,
            incarnation_id=uploads[task.id].incarnation_id,
            reference_kind="catalog",
            owner_id=task.id,
            now=now,
        )
        await admit_task_bundle_source(
            session,
            task_id=task.id,
            task_checksum=task.checksum,
            task_config=task.config,
            task_source=task.source,
            task_source_provenance=task.source_provenance,
            reference_kind="catalog",
            owner_id=task.id,
        )
        await _reference_task_image_materializations(session, rows=result[task.id])
    for previous_source, owner_id in previous:
        if previous_source != sources[owner_id]:
            await release_task_bundle_reference(
                session, source_id=previous_source, reference_kind="catalog", owner_id=owner_id
            )
    await session.flush()
    return result
