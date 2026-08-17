"""Lease-fenced control-plane operations for task-image builders."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.models.task import TaskConfig
from loom.task_image_materialization import (
    required_task_image_components,
    validate_task_image_registry_images,
)

DEFAULT_TASK_IMAGE_LEASE_SECONDS = 300.0
_MAX_RETRY_BACKOFF_SECONDS = 600.0
_TERMINAL_TRIAL_STATES = ("succeeded", "failed", "cancelled")


class TaskImageLeaseConflictError(Exception):
    """The caller no longer owns the materialization lease."""


class TaskImageCompletionError(ValueError):
    """Published component evidence does not match the immutable task snapshot."""


class TaskImageRetryConflictError(RuntimeError):
    """The requested materialization cannot be safely requeued."""


def _expected_registry_image_components(task_config: dict[str, Any]) -> set[str]:
    task = TaskConfig.model_validate(task_config)
    return required_task_image_components(task)


def _record_registry_image_history(
    row: TaskImageMaterialization,
    *,
    builder_id: str,
    lease_epoch: int,
    registry_images: dict[str, str],
    now: datetime,
) -> None:
    history = list(row.registry_image_history or [])
    observed = {
        (str(entry.get("component")), str(entry.get("registry_image")))
        for entry in history
        if isinstance(entry, dict)
    }
    for component, registry_image in registry_images.items():
        if (component, registry_image) in observed:
            continue
        history.append(
            {
                "component": component,
                "registry_image": registry_image,
                "builder_id": builder_id,
                "lease_epoch": lease_epoch,
                "recorded_at": now.isoformat(),
            }
        )
    row.registry_image_history = history


async def record_task_image_publication(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    builder_id: str,
    lease_epoch: int,
    component: str,
    registry_image: str,
) -> TaskImageMaterialization:
    """Append cleanup evidence without granting stale builders readiness authority."""
    now = datetime.now(UTC)
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .with_for_update()
    )
    if row is None:
        raise TaskImageLeaseConflictError("task image materialization does not exist")
    try:
        images = validate_task_image_registry_images(
            {component: registry_image},
            expected_components=_expected_registry_image_components(row.task_config),
        )
    except ValueError as exc:
        raise TaskImageCompletionError(str(exc)) from exc
    _record_registry_image_history(
        row,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        registry_images=images,
        now=now,
    )
    if (
        row.state in {"claimed", "running"}
        and row.claimed_by == builder_id
        and row.lease_epoch == lease_epoch
        and row.lease_expires_at is not None
        and row.lease_expires_at > now
    ):
        row.registry_images = {**row.registry_images, **images}
    row.updated_at = now
    await session.flush()
    return row


def _durable_reference_exists(row: Any) -> ColumnElement[bool]:
    current_task = exists().where(
        Task.id == row.task_id,
        or_(
            Task.checksum == row.task_checksum,
            Task.checksum == func.concat("sha256:", row.task_checksum),
        ),
    )
    live_trial = exists().where(
        TrialTaskImageMaterialization.materialization_id == row.id,
        Trial.id == TrialTaskImageMaterialization.trial_id,
        Trial.state.not_in(_TERMINAL_TRIAL_STATES),
    )
    return or_(current_task, live_trial)


def _registry_publication_exists(row: Any) -> ColumnElement[bool]:
    return or_(row.registry_images != {}, row.registry_image_history != [])


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
    try:
        registry_images = validate_task_image_registry_images(
            registry_images,
            expected_components=_expected_registry_image_components(row.task_config),
            require_complete=True,
        )
    except ValueError as exc:
        raise TaskImageCompletionError(str(exc)) from exc
    _record_registry_image_history(
        row,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        registry_images=registry_images,
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
    registry_images: dict[str, str],
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
    try:
        registry_images = validate_task_image_registry_images(
            registry_images,
            expected_components=_expected_registry_image_components(row.task_config),
            require_nonempty=False,
        )
    except ValueError as exc:
        raise TaskImageCompletionError(str(exc)) from exc
    _record_registry_image_history(
        row,
        builder_id=builder_id,
        lease_epoch=lease_epoch,
        registry_images=registry_images,
        now=now,
    )
    row.claimed_by = None
    row.lease_expires_at = None
    row.failure_reason = failure_reason
    row.failure_message = failure_message
    row.registry_images = {**row.registry_images, **registry_images}
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


async def retry_task_image_materialization(
    session: AsyncSession,
    *,
    materialization_id: UUID,
) -> TaskImageMaterialization:
    """Requeue an exhausted or suspect ready image under an admin decision."""
    now = datetime.now(UTC)
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .with_for_update()
    )
    if row is None:
        raise TaskImageRetryConflictError("task image materialization does not exist")
    if row.state not in {"failed", "ready"}:
        raise TaskImageRetryConflictError(
            f"task image materialization in state {row.state!r} cannot be retried"
        )
    row.state = "queued"
    row.attempt_count = 0
    row.next_attempt_at = None
    row.claimed_by = None
    row.lease_epoch += 1
    row.lease_expires_at = None
    row.failure_reason = None
    row.failure_message = None
    row.claimed_at = None
    row.started_at = None
    row.ready_at = None
    row.finished_at = None
    row.registry_images = {}
    row.last_referenced_at = now
    row.unreferenced_at = None
    row.updated_at = now
    await session.flush()
    return row


async def claim_task_image_registry_gc(
    session: AsyncSession,
    *,
    gc_id: str,
    grace_hours: int,
    lease_seconds: float = DEFAULT_TASK_IMAGE_LEASE_SECONDS,
) -> TaskImageMaterialization | None:
    """Fence one grace-expired registry image set for external deletion."""
    if grace_hours < 0:
        raise ValueError("grace_hours must be non-negative")
    now = datetime.now(UTC)
    cutoff = now - timedelta(hours=grace_hours)
    await session.execute(
        update(TaskImageMaterialization)
        .where(
            TaskImageMaterialization.state.in_(("ready", "failed")),
            _registry_publication_exists(TaskImageMaterialization),
            _durable_reference_exists(TaskImageMaterialization),
        )
        .values(
            last_referenced_at=now,
            unreferenced_at=None,
            updated_at=now,
        )
    )
    await session.execute(
        update(TaskImageMaterialization)
        .where(
            TaskImageMaterialization.state.in_(("ready", "failed")),
            _registry_publication_exists(TaskImageMaterialization),
            TaskImageMaterialization.unreferenced_at.is_(None),
            ~_durable_reference_exists(TaskImageMaterialization),
        )
        .values(unreferenced_at=now, updated_at=now)
    )
    await session.execute(
        update(TaskImageMaterialization)
        .where(
            TaskImageMaterialization.state == "retiring",
            TaskImageMaterialization.lease_expires_at <= now,
            _durable_reference_exists(TaskImageMaterialization),
        )
        .values(
            state="queued",
            attempt_count=0,
            next_attempt_at=None,
            claimed_by=None,
            lease_expires_at=None,
            registry_images={},
            failure_reason=None,
            failure_message=None,
            ready_at=None,
            finished_at=None,
            last_referenced_at=now,
            unreferenced_at=None,
            updated_at=now,
        )
    )
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(
            or_(
                and_(
                    TaskImageMaterialization.state.in_(("ready", "failed")),
                    _registry_publication_exists(TaskImageMaterialization),
                    TaskImageMaterialization.unreferenced_at <= cutoff,
                ),
                and_(
                    TaskImageMaterialization.state == "retiring",
                    TaskImageMaterialization.lease_expires_at <= now,
                ),
            ),
            ~_durable_reference_exists(TaskImageMaterialization),
        )
        .order_by(
            TaskImageMaterialization.unreferenced_at,
            TaskImageMaterialization.id,
        )
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if row is None:
        return None
    row.state = "retiring"
    row.claimed_by = gc_id
    row.lease_epoch += 1
    row.lease_expires_at = _lease_deadline(now=now, lease_seconds=lease_seconds)
    row.claimed_at = now
    row.updated_at = now
    await session.flush()
    return row


async def complete_task_image_registry_gc(
    session: AsyncSession,
    *,
    materialization_id: UUID,
    gc_id: str,
    lease_epoch: int,
) -> TaskImageMaterialization:
    """Commit deletion only for the current owner, requeuing raced references."""
    now = datetime.now(UTC)
    row = await _locked_owned_materialization(
        session,
        materialization_id=materialization_id,
        builder_id=gc_id,
        lease_epoch=lease_epoch,
        allowed_states=("retiring",),
        now=now,
    )
    referenced = bool(await session.scalar(select(_durable_reference_exists(row))))
    row.registry_images = {}
    row.registry_image_history = []
    row.claimed_by = None
    row.lease_expires_at = None
    row.updated_at = now
    if referenced:
        row.state = "queued"
        row.attempt_count = 0
        row.next_attempt_at = None
        row.failure_reason = None
        row.failure_message = None
        row.ready_at = None
        row.finished_at = None
        row.last_referenced_at = now
        row.unreferenced_at = None
    else:
        row.state = "retired"
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
        "registry_image_history": row.registry_image_history,
        "failure_reason": row.failure_reason,
        "failure_message": row.failure_message,
    }
