"""Lease-fenced control-plane operations for task-image builders."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TaskImageMaterialization

DEFAULT_TASK_IMAGE_LEASE_SECONDS = 300.0
_MAX_RETRY_BACKOFF_SECONDS = 600.0


class TaskImageLeaseConflictError(Exception):
    """The caller no longer owns the materialization lease."""


def _lease_deadline(*, now: datetime, lease_seconds: float) -> datetime:
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    return now + timedelta(seconds=lease_seconds)


async def claim_task_image_materialization(
    session: AsyncSession,
    *,
    builder_id: str,
    cpu_arch: str,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
) -> TaskImageMaterialization | None:
    """Atomically claim queued work or recover one expired lease."""
    now = datetime.now(UTC)
    await session.execute(
        update(TaskImageMaterialization)
        .where(
            TaskImageMaterialization.state.in_(("claimed", "running")),
            TaskImageMaterialization.lease_expires_at <= now,
            TaskImageMaterialization.attempt_count >= TaskImageMaterialization.max_attempts,
        )
        .values(
            state="failed",
            claimed_by=None,
            lease_expires_at=None,
            failure_reason="lease_expired",
            failure_message="task image build lease expired at the attempt limit",
            finished_at=now,
            updated_at=now,
        )
    )
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(
            TaskImageMaterialization.cpu_arch == cpu_arch,
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
        .order_by(TaskImageMaterialization.created_at, TaskImageMaterialization.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if row is None:
        return None
    row.state = "claimed"
    row.claimed_by = builder_id
    row.lease_epoch += 1
    row.attempt_count += 1
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.claimed_at = now
    row.started_at = None
    row.finished_at = None
    row.next_attempt_at = None
    row.failure_reason = None
    row.failure_message = None
    row.updated_at = now
    await session.flush()
    return row


async def _locked_owned_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    allowed_states: tuple[str, ...],
    now: datetime,
) -> TaskImageMaterialization:
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .with_for_update()
    )
    if (
        row is None
        or row.state not in allowed_states
        or row.claimed_by != builder_id
        or row.lease_epoch != lease_epoch
        or row.lease_expires_at is None
        or row.lease_expires_at <= now
    ):
        raise TaskImageLeaseConflictError("stale task image materialization lease")
    return row


async def start_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed",),
        now=now,
    )
    row.state = "running"
    row.started_at = now
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.updated_at = now
    await session.flush()
    return row


async def heartbeat_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.updated_at = now
    await session.flush()
    return row


async def complete_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    registry_images: dict[str, str],
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        allowed_states=("running",),
        now=now,
    )
    row.state = "ready"
    row.registry_images = dict(registry_images)
    row.claimed_by = None
    row.lease_expires_at = None
    row.ready_at = now
    row.finished_at = now
    row.updated_at = now
    await session.flush()
    return row


async def fail_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    retryable: bool,
    failure_reason: str,
    failure_message: str,
) -> TaskImageMaterialization:
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    row.claimed_by = None
    row.lease_expires_at = None
    row.failure_reason = failure_reason
    row.failure_message = failure_message
    row.updated_at = now
    if retryable and row.attempt_count < row.max_attempts:
        backoff_seconds = min(
            30.0 * (2 ** max(row.attempt_count - 1, 0)),
            _MAX_RETRY_BACKOFF_SECONDS,
        )
        row.state = "queued"
        row.next_attempt_at = now + timedelta(seconds=backoff_seconds)
        row.finished_at = None
    else:
        row.state = "failed"
        row.next_attempt_at = None
        row.finished_at = now
    await session.flush()
    return row


def task_image_materialization_payload(
    row: TaskImageMaterialization,
) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "materialization_key": row.materialization_key,
        "task_id": row.task_id,
        "task_checksum": row.task_checksum,
        "cpu_arch": row.cpu_arch,
        "task_config": row.task_config,
        "task_source": row.task_source,
        "task_source_provenance": row.task_source_provenance,
        "state": row.state,
        "attempt_count": row.attempt_count,
        "max_attempts": row.max_attempts,
        "lease_epoch": row.lease_epoch,
        "lease_expires_at": (row.lease_expires_at.isoformat() if row.lease_expires_at else None),
        "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
        "registry_images": row.registry_images,
        "failure_reason": row.failure_reason,
        "failure_message": row.failure_message,
    }
