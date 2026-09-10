"""Transactional source publication and retention, with no object-store I/O.

Lock order: caller catalog/trial/materialization locks, sorted logical source
locks, then incarnation/write/version rows. Retirement never takes caller locks.
Every API uses the caller transaction; commit intent issuance BEFORE storage I/O,
and commit deletion claims BEFORE exact-version deletion. No prefix delete exists.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid4

import rfc8785
from sqlalchemy import delete, exists, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskBundleSource,
    TaskBundleSourceIncarnation,
    TaskBundleSourceReference,
    TaskBundleSourceVersion,
    TaskBundleSourceWrite,
)
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_bundle_source_storage import (
    TaskBundleInventoryCursorV1,
    TaskBundleObjectIntentV1,
    TaskBundleVersionBatch,
    _version,
)
from loom.trajectory.storage import ObjectWriteResult

ReferenceKind = Literal["catalog", "materialization", "trial"]


@dataclass(frozen=True, slots=True)
class TaskBundleUpload:
    source_id: str
    incarnation_id: UUID
    intents: tuple[TaskBundleObjectIntentV1, ...]
    available: bool


@dataclass(frozen=True, slots=True)
class TaskBundleVersionDeletion:
    id: UUID
    intent: TaskBundleObjectIntentV1
    version_id: str


def _now(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("source journal time must be timezone-aware")
    return now.astimezone(UTC)


def _reference(kind: str, owner: str) -> None:
    if (
        kind not in {"catalog", "materialization", "trial"}
        or not 1 <= len(owner) <= 1024
        or any(ord(char) < 32 or ord(char) == 127 for char in owner)
    ):
        raise ValueError("source reference identity is invalid")


async def _require_transaction(session: AsyncSession) -> None:
    valid = await session.scalar(
        text(
            "SELECT pg_catalog.current_setting('transaction_isolation') = 'read committed' "
            "AND pg_catalog.pg_current_xact_id_if_assigned() IS NOT NULL"
        )
    )
    if valid is not True:
        raise ValueError("source journal requires an explicit READ COMMITTED transaction")


async def _source(session: AsyncSession, source_id: str) -> TaskBundleSource:
    await session.flush()
    source = await session.scalar(
        select(TaskBundleSource)
        .where(TaskBundleSource.id == source_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if source is None:
        raise ValueError("registered task bundle source is absent")
    # A lock on unchanged logical facts does not refresh a fixed snapshot of
    # incarnation/reference rows. AUTOCOMMIT also releases the lock too early.
    await _require_transaction(session)
    return source


async def _incarnation(
    session: AsyncSession, incarnation_id: UUID
) -> tuple[TaskBundleSource, TaskBundleSourceIncarnation]:
    source_id = await session.scalar(
        select(TaskBundleSourceIncarnation.source_id).where(
            TaskBundleSourceIncarnation.id == incarnation_id
        )
    )
    if source_id is None:
        raise ValueError("source incarnation is absent")
    source = await _source(session, source_id)
    incarnation = await session.scalar(
        select(TaskBundleSourceIncarnation)
        .where(TaskBundleSourceIncarnation.id == incarnation_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if incarnation is None:
        raise ValueError("source incarnation is absent")
    return source, incarnation


def _intent(row: TaskBundleSourceWrite) -> TaskBundleObjectIntentV1:
    return TaskBundleObjectIntentV1(
        id=row.id,
        bucket=row.bucket,
        object_key=row.object_key,
        content_sha256=row.content_sha256,
        size_bytes=row.size_bytes,
    )


async def _write(
    session: AsyncSession, intent_id: UUID
) -> tuple[TaskBundleSourceIncarnation, TaskBundleSourceWrite]:
    incarnation_id = await session.scalar(
        select(TaskBundleSourceWrite.incarnation_id).where(TaskBundleSourceWrite.id == intent_id)
    )
    if incarnation_id is None:
        raise ValueError("source write intent is absent")
    _, incarnation = await _incarnation(session, incarnation_id)
    row = await session.scalar(
        select(TaskBundleSourceWrite)
        .where(TaskBundleSourceWrite.id == intent_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if row is None:
        raise ValueError("source write intent is absent")
    return incarnation, row


async def _available(session: AsyncSession, source_id: str) -> TaskBundleSourceIncarnation | None:
    row: TaskBundleSourceIncarnation | None = await session.scalar(
        select(TaskBundleSourceIncarnation).where(
            TaskBundleSourceIncarnation.source_id == source_id,
            TaskBundleSourceIncarnation.state == "available",
        )
    )
    return row


async def _ticket(
    session: AsyncSession, spec: TaskBundleSourceSpecV1, incarnation: TaskBundleSourceIncarnation
) -> TaskBundleUpload:
    writes = {
        row.object_key: _intent(row)
        for row in await session.scalars(
            select(TaskBundleSourceWrite).where(
                TaskBundleSourceWrite.incarnation_id == incarnation.id
            )
        )
    }
    return TaskBundleUpload(
        source_id=spec.id,
        incarnation_id=incarnation.id,
        intents=tuple(writes[item.object_key] for item in spec.objects),
        available=incarnation.state == "available",
    )


async def begin_task_bundle_upload(
    session: AsyncSession,
    *,
    spec: TaskBundleSourceSpecV1,
    upload_id: UUID,
    now: datetime,
    expires_at: datetime,
) -> TaskBundleUpload:
    now, expires_at = _now(now), _now(expires_at)
    if not now < expires_at or (expires_at - now).total_seconds() > 86400:
        raise ValueError("source upload lifetime is invalid")
    # Parse again at the persistence boundary, preserving immutable config bytes.
    spec = TaskBundleSourceSpecV1.model_validate_json(spec.model_dump_json())
    # Establish and check a real transaction before the first authority INSERT.
    # Two statements intentionally distinguish AUTOCOMMIT from retained ownership.
    await session.execute(text("SELECT pg_catalog.pg_current_xact_id()"))
    await _require_transaction(session)
    await session.execute(
        pg_insert(TaskBundleSource)
        .values(
            id=spec.id,
            source_uri=spec.source_uri,
            spec_json=spec.model_dump(mode="json"),
            created_at=now,
        )
        .on_conflict_do_nothing(index_elements=["id"])
    )
    source = await _source(session, spec.id)
    if source.source_uri != spec.source_uri or source.spec_json != spec.model_dump(mode="json"):
        raise ValueError("immutable source registration conflicts")
    available = await _available(session, spec.id)
    if available is not None:
        return await _ticket(session, spec, available)
    created = await session.scalar(
        pg_insert(TaskBundleSourceIncarnation)
        .values(
            id=upload_id,
            source_id=spec.id,
            state="uploading",
            created_at=now,
            updated_at=now,
            expires_at=expires_at,
        )
        .on_conflict_do_nothing(index_elements=["id"])
        .returning(TaskBundleSourceIncarnation.id)
    )
    incarnation = await session.get(TaskBundleSourceIncarnation, upload_id)
    assert incarnation is not None
    if incarnation.source_id != spec.id or incarnation.state != "uploading":
        raise ValueError("source upload incarnation conflicts or is retiring")
    # A transport retry proposes a new deadline from its own clock. It may
    # resume the original upload but must never extend that upload's lifetime.
    if not incarnation.created_at <= now < incarnation.expires_at:
        raise ValueError("source upload incarnation is expired or not yet valid")
    if created is not None:
        await session.execute(
            pg_insert(TaskBundleSourceWrite),
            [
                dict(
                    id=uuid4(),
                    incarnation_id=upload_id,
                    bucket=spec.bucket,
                    object_key=item.object_key,
                    content_sha256=item.content_sha256,
                    size_bytes=item.size_bytes,
                )
                for item in spec.objects
            ],
        )
    return await _ticket(session, spec, incarnation)


async def issue_task_bundle_write(
    session: AsyncSession, *, intent_id: UUID, now: datetime
) -> TaskBundleObjectIntentV1:
    now = _now(now)
    incarnation, row = await _write(session, intent_id)
    if (
        incarnation.state != "uploading"
        or not incarnation.created_at <= now < incarnation.expires_at
    ):
        raise ValueError("source upload is expired or retiring")
    if row.issued_at is None:
        row.issued_at = now
        await session.flush()
    return _intent(row)


async def _record(
    session: AsyncSession, row: TaskBundleSourceWrite, receipt: ObjectWriteResult, now: datetime
) -> None:
    intent = _intent(row)
    version = _version(receipt.version_id)
    if row.issued_at is None or receipt.uri != intent.uri:
        raise ValueError("source receipt lacks an issued exact intent")
    await session.execute(
        pg_insert(TaskBundleSourceVersion)
        .values(
            id=uuid4(),
            write_id=row.id,
            bucket=row.bucket,
            object_key=row.object_key,
            version_id=version,
            state="available",
            observed_at=now,
        )
        .on_conflict_do_nothing(index_elements=["bucket", "object_key", "version_id"])
    )
    existing = await session.scalar(
        select(TaskBundleSourceVersion).where(
            TaskBundleSourceVersion.bucket == row.bucket,
            TaskBundleSourceVersion.object_key == row.object_key,
            TaskBundleSourceVersion.version_id == version,
        )
    )
    if existing is None or existing.write_id != row.id:
        raise ValueError("source version belongs to another intent")
    # Exact replay never revives an already deleting/deleted version. Late new
    # versions are evidence even for retired incarnations, never publication.


async def record_task_bundle_write(
    session: AsyncSession, *, intent_id: UUID, receipt: ObjectWriteResult, now: datetime
) -> None:
    now = _now(now)
    _, row = await _write(session, intent_id)
    await _record(session, row, receipt, now)


async def attach_task_bundle_reference(
    session: AsyncSession, *, source_id: str, reference_kind: ReferenceKind, owner_id: str
) -> UUID:
    _reference(reference_kind, owner_id)
    await _source(session, source_id)
    available = await _available(session, source_id)
    if available is None:
        raise ValueError("source has no available incarnation")
    await session.execute(
        pg_insert(TaskBundleSourceReference)
        .values(source_id=source_id, kind=reference_kind, owner_id=owner_id)
        .on_conflict_do_nothing()
    )
    return available.id


async def release_task_bundle_reference(
    session: AsyncSession, *, source_id: str, reference_kind: ReferenceKind, owner_id: str
) -> None:
    _reference(reference_kind, owner_id)
    await _source(session, source_id)
    await session.execute(
        delete(TaskBundleSourceReference).where(
            TaskBundleSourceReference.source_id == source_id,
            TaskBundleSourceReference.kind == reference_kind,
            TaskBundleSourceReference.owner_id == owner_id,
        )
    )


async def publish_task_bundle_source(
    session: AsyncSession,
    *,
    incarnation_id: UUID,
    reference_kind: ReferenceKind,
    owner_id: str,
    now: datetime,
) -> TaskBundleSourceSpecV1:
    now = _now(now)
    _reference(reference_kind, owner_id)
    source, incarnation = await _incarnation(session, incarnation_id)
    if incarnation.state in {"deleting", "retired"}:
        raise ValueError("source incarnation is retiring")
    if incarnation.state == "uploading":
        if not incarnation.created_at <= now < incarnation.expires_at:
            raise ValueError("source upload is expired")
        incomplete = await session.scalar(
            select(
                exists().where(
                    TaskBundleSourceWrite.incarnation_id == incarnation_id,
                    ~exists().where(
                        TaskBundleSourceVersion.write_id == TaskBundleSourceWrite.id,
                        TaskBundleSourceVersion.state == "available",
                    ),
                )
            )
        )
        if incomplete:
            raise ValueError("source upload is incomplete")
        available = await _available(session, source.id)
        # A concurrent complete publication wins; never replace an available
        # incarnation under references. Dispose only this losing upload's versions.
        incarnation.state = "available" if available is None else "deleting"
        incarnation.updated_at = now
        await session.flush()
    await attach_task_bundle_reference(
        session, source_id=source.id, reference_kind=reference_kind, owner_id=owner_id
    )
    return TaskBundleSourceSpecV1.model_validate_json(json.dumps(source.spec_json))


async def retire_task_bundle_source(
    session: AsyncSession, *, incarnation_id: UUID, now: datetime
) -> bool:
    now = _now(now)
    source, incarnation = await _incarnation(session, incarnation_id)
    if incarnation.state in {"deleting", "retired"}:
        return True
    if incarnation.state == "uploading" and now < incarnation.expires_at:
        return False
    if incarnation.state == "available" and await session.scalar(
        select(exists().where(TaskBundleSourceReference.source_id == source.id))
    ):
        return False
    incarnation.state, incarnation.updated_at = "deleting", now
    await session.flush()
    return True


async def claim_task_bundle_version_deletions(
    session: AsyncSession, *, incarnation_id: UUID, now: datetime, limit: int = 1000
) -> tuple[TaskBundleVersionDeletion, ...]:
    _now(now)
    if not 1 <= limit <= 1000:
        raise ValueError("source deletion batch limit is invalid")
    _, incarnation = await _incarnation(session, incarnation_id)
    if incarnation.state not in {"deleting", "retired"}:
        raise ValueError("source incarnation is not retiring")
    # Do not delete a cursor's marker while its inventory pass is unfinished.
    in_progress = await session.scalar(
        select(
            exists().where(
                TaskBundleSourceWrite.incarnation_id == incarnation_id,
                TaskBundleSourceWrite.inventory_active.is_(True),
            )
        )
    )
    if in_progress:
        return ()
    rows = (
        await session.execute(
            select(TaskBundleSourceVersion, TaskBundleSourceWrite)
            .join(
                TaskBundleSourceWrite, TaskBundleSourceWrite.id == TaskBundleSourceVersion.write_id
            )
            .where(
                TaskBundleSourceWrite.incarnation_id == incarnation_id,
                TaskBundleSourceVersion.state != "deleted",
            )
            .order_by(TaskBundleSourceVersion.id)
            .limit(limit)
        )
    ).all()
    result = []
    for version, write in rows:
        version.state = "deleting"
        result.append(
            TaskBundleVersionDeletion(
                id=version.id, intent=_intent(write), version_id=_version(version.version_id)
            )
        )
    await session.flush()
    return tuple(result)


async def finish_task_bundle_version_deletion(
    session: AsyncSession, *, deletion: TaskBundleVersionDeletion, now: datetime
) -> None:
    now = _now(now)
    incarnation, row = await _write(session, deletion.intent.id)
    version = await session.scalar(
        select(TaskBundleSourceVersion)
        .where(TaskBundleSourceVersion.id == deletion.id)
        .execution_options(populate_existing=True)
    )
    if (
        incarnation.state not in {"deleting", "retired"}
        or _intent(row) != deletion.intent
        or version is None
        or version.version_id != deletion.version_id
        or version.write_id != row.id
        or version.state not in {"deleting", "deleted"}
    ):
        raise ValueError("source deletion completion conflicts with retirement")
    if version.state == "deleting":
        version.state, version.deleted_at = "deleted", now
        await session.flush()


async def checkpoint_task_bundle_inventory(
    session: AsyncSession,
    *,
    intent_id: UUID,
    expected_epoch: int,
    batch: TaskBundleVersionBatch,
    now: datetime,
) -> int:
    now = _now(now)
    _, row = await _write(session, intent_id)
    if row.issued_at is None or not row.inventory_active or row.inventory_epoch != expected_epoch:
        raise ValueError("source inventory checkpoint epoch conflicts")
    if batch.continuation is not None:
        digest = hashlib.sha256(rfc8785.dumps(_intent(row).model_dump(mode="json"))).hexdigest()
        if batch.continuation.intent_sha256 != digest:
            raise ValueError("source inventory checkpoint intent conflicts")
    for receipt in batch.versions:
        await _record(session, row, receipt, now)
    row.inventory_epoch += 1
    row.inventory_cursor = (
        batch.continuation.model_dump(mode="json") if batch.continuation is not None else None
    )
    row.inventory_active = not batch.observed_end
    if batch.observed_end:
        row.last_observed_end_at = now
    await session.flush()
    return row.inventory_epoch


async def task_bundle_inventory_checkpoint(
    session: AsyncSession, *, intent_id: UUID, restart: bool = False
) -> tuple[TaskBundleObjectIntentV1, int, TaskBundleInventoryCursorV1 | None]:
    """Resume or explicitly restart an observation without discarding receipts.

    Restart replaces an unusable provider cursor and fences every older page
    still in flight. It is not writer-quiescence or absence evidence.
    """
    incarnation, row = await _write(session, intent_id)
    if row.issued_at is None:
        raise ValueError("source write was never issued")
    deleting = await session.scalar(
        select(
            exists().where(
                TaskBundleSourceVersion.write_id == TaskBundleSourceWrite.id,
                TaskBundleSourceWrite.incarnation_id == incarnation.id,
                TaskBundleSourceVersion.state == "deleting",
            )
        )
    )
    if deleting:
        raise ValueError("source inventory must wait for pending exact deletion")
    if restart:
        row.inventory_epoch += 1
        row.inventory_cursor = None
    row.inventory_active = True
    await session.flush()
    cursor = (
        TaskBundleInventoryCursorV1.model_validate(row.inventory_cursor)
        if row.inventory_cursor is not None
        else None
    )
    return _intent(row), row.inventory_epoch, cursor
