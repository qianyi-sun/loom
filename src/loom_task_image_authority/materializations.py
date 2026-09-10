"""Session-authorized materialization leases for the rootless build provider."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID

import rfc8785
from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageAttemptRetention,
    TaskImageBuildGrant,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
    TaskImageBuildSessionGeneration,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageMaterializationOperationEvent,
)
from loom.security.secret_store import SecretStore
from loom.task_image_build_plan import (
    TaskImageBuildPlan,
    derive_task_image_build_plan,
    parse_task_image_build_plan,
)
from loom.task_image_materialization import admit_task_image_source
from loom_task_image_authority.bundle_capability import (
    AsyncTaskImageBundleCapabilityProvider,
    TaskImageBundleCapability,
    TaskImageBundleCapabilityError,
    TaskImageBundleCapabilityProvider,
    TaskImageBundleCapabilityV1,
    parse_task_image_bundle_capability,
)
from loom_task_image_authority.retention import attempt_is_retired

if TYPE_CHECKING:
    from loom.task_bundle_source import TaskBundleSourceSpecV1

DEFAULT_SESSION_MATERIALIZATION_LEASE_SECONDS = 300.0
MAX_SESSION_MATERIALIZATION_LEASE_SECONDS = 15 * 60.0
_MAX_RETRY_BACKOFF_SECONDS = 600.0

OperationType = Literal[
    "start",
    "heartbeat",
    "bundle",
    "release",
    "containment_release",
    "deterministic_fail",
]


class TaskImageSessionMaterializationAuthorizationError(RuntimeError):
    """The presented internal session authority is not the current generation."""


class TaskImageSessionMaterializationConflictError(RuntimeError):
    """A claim, operation identity, or lease binding no longer matches."""


class TaskImageBuildSessionAuthorization(Protocol):
    """Structural session authority contract accepted from the projection store."""

    @property
    def grant_id(self) -> UUID: ...

    @property
    def session_id(self) -> UUID: ...

    @property
    def session_generation(self) -> int: ...

    @property
    def authority_version(self) -> Literal[1, 2]: ...

    @property
    def builder_release_sha256(self) -> str | None: ...

    @property
    def supervisor_executable_sha256(self) -> str: ...

    @property
    def purpose(self) -> Literal["production", "shadow"]: ...

    @property
    def shadow_campaign_id(self) -> UUID | None: ...

    @property
    def environment(self) -> str: ...

    @property
    def pool_id(self) -> str: ...

    @property
    def cpu_arch(self) -> Literal["x86_64", "arm64"]: ...

    @property
    def attestation_generation(self) -> int: ...

    @property
    def attestation_sha256(self) -> str: ...

    @property
    def attestation_expires_at(self) -> datetime: ...

    @property
    def session_expires_at(self) -> datetime: ...

    @property
    def grant_expires_at(self) -> datetime: ...


def _utc(value: datetime) -> datetime:
    if value.utcoffset() is None:
        raise ValueError("task-image materialization time must be timezone-aware")
    return value.astimezone(UTC)


def _nonzero_id(value: UUID, *, label: str) -> UUID:
    if value.int == 0:
        raise ValueError(f"{label} must be nonzero")
    return value


def _lease_deadline(*, now: datetime, lease_seconds: float) -> datetime:
    if (
        isinstance(lease_seconds, bool)
        or not math.isfinite(lease_seconds)
        or lease_seconds <= 0
        or lease_seconds > MAX_SESSION_MATERIALIZATION_LEASE_SECONDS
    ):
        raise ValueError("session materialization lease duration is invalid")
    return now + timedelta(seconds=lease_seconds)


def _plan_snapshot(plan: TaskImageBuildPlan) -> tuple[dict[str, object], str]:
    payload = plan.model_dump(mode="json", exclude_none=False)
    return payload, hashlib.sha256(rfc8785.dumps(payload)).hexdigest()


def _stored_attempt_claim_plan(
    attempt: TaskImageMaterializationAttempt,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
) -> TaskImageBuildPlan:
    if attempt.claim_plan_json is None or attempt.claim_plan_sha256 is None:
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        )
    try:
        plan = parse_task_image_build_plan(json.dumps(attempt.claim_plan_json, ensure_ascii=False, separators=(",", ":")))
        payload, digest = _plan_snapshot(plan)
    except (TypeError, ValueError):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        ) from None
    if (
        digest != attempt.claim_plan_sha256
        or payload != attempt.claim_plan_json
        or attempt.grant_id != authorization.grant_id
        or plan.grant_id != authorization.grant_id
        or plan.session_id != attempt.session_id
        or plan.session_generation != attempt.session_generation
        or plan.builder_id != attempt.builder_id
        or attempt.materialization_id != materialization_id
        or plan.materialization_id != materialization_id
        or plan.cpu_arch != authorization.cpu_arch
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        )
    return plan


def _stored_claim_plan(
    attempt: TaskImageMaterializationAttempt,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
) -> TaskImageBuildPlan:
    plan = _stored_attempt_claim_plan(
        attempt, authorization=authorization, materialization_id=materialization_id,
    )
    # Claim replay belongs to its original session, unlike continuing an attempt
    # with a freshly authenticated successor under the same grant.
    if (
        plan.session_id != authorization.session_id
        or plan.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        )
    return plan


def _reject_pending_materialization_authority(session: AsyncSession) -> None:
    # Check before any SELECT can autoflush a later retention/parent write.
    # Refreshing locked rows must not silently discard caller-owned edits when
    # autoflush is suppressed, either. Reads require committed/flushed authority.
    models = (TaskImageAttemptRetention, TaskImageMaterialization, TaskImageMaterializationAttempt)
    if any(
        isinstance(row, models)
        for row in (*session.new, *session.dirty, *session.deleted)
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image materialization authority has unflushed changes"
        )


async def lock_current_task_image_build_session_authority(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    now: datetime,
    source_admission: bool = False,
) -> TaskImageBuildSessionGeneration:
    """Recheck and lock grant → projection → current generation in canonical order."""

    if source_admission:
        await _require_source_transaction(session)
    _reject_pending_materialization_authority(session)
    if (
        authorization.authority_version != 2
        or authorization.builder_release_sha256 is None
        or authorization.purpose != "production"
        or authorization.shadow_campaign_id is not None
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image materialization requires V2 production session authority"
        )
    grant = await session.scalar(
        select(TaskImageBuildGrant)
        .where(TaskImageBuildGrant.id == authorization.grant_id)
        .with_for_update()
    )
    authority_spec = grant.authority_spec if grant is not None else {}
    if (
        grant is None
        or grant.state != "released"
        or grant.grant_expires_at <= now
        or grant.grant_expires_at != authorization.grant_expires_at
        or grant.cpu_arch != authorization.cpu_arch
        or authority_spec.get("schema_version") != 2
        or authority_spec.get("purpose") != authorization.purpose
        or authority_spec.get("shadow_campaign_id")
        != (
            str(authorization.shadow_campaign_id)
            if authorization.shadow_campaign_id is not None
            else None
        )
        or authority_spec.get("environment") != authorization.environment
        or authority_spec.get("pool_id") != authorization.pool_id
        or authority_spec.get("cpu_arch") != authorization.cpu_arch
        or authority_spec.get("builder_release_sha256") != authorization.builder_release_sha256
        or authority_spec.get("supervisor_executable_sha256")
        != authorization.supervisor_executable_sha256
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image materialization session is unavailable"
        )
    projection = await session.scalar(
        select(TaskImageBuildProjection)
        .where(TaskImageBuildProjection.grant_id == authorization.grant_id)
        .with_for_update()
    )
    if (
        projection is None
        or projection.state != "exchanged"
        or projection.session_id != authorization.session_id
        or projection.session_generation != authorization.session_generation
        or projection.attestation_generation != authorization.attestation_generation
        or projection.attestation_sha256 != authorization.attestation_sha256
        or projection.session_expires_at is None
        or projection.attestation_expires_at is None
        or projection.session_expires_at != authorization.session_expires_at
        or projection.attestation_expires_at != authorization.attestation_expires_at
        or projection.session_expires_at <= now
        or projection.attestation_expires_at <= now
        or authorization.session_expires_at <= now
        or authorization.attestation_expires_at <= now
        or authorization.grant_expires_at <= now
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image materialization session is unavailable"
        )
    generation = await session.scalar(
        select(TaskImageBuildSessionGeneration)
        .where(
            TaskImageBuildSessionGeneration.grant_id == authorization.grant_id,
            TaskImageBuildSessionGeneration.generation == authorization.session_generation,
            TaskImageBuildSessionGeneration.session_id == authorization.session_id,
        )
        .with_for_update()
    )
    if (
        generation is None
        or generation.attestation_generation != authorization.attestation_generation
        or generation.attestation_sha256 != authorization.attestation_sha256
        or generation.expires_at != projection.session_expires_at
        or generation.expires_at <= now
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image materialization session is unavailable"
        )
    return generation


async def _require_source_transaction(session: AsyncSession) -> None:
    from loom.task_bundle_source_journal import require_task_bundle_transaction

    try:
        await require_task_bundle_transaction(session)
    except ValueError as error:
        raise TaskImageSessionMaterializationConflictError(str(error)) from None


async def _admit_source(
    session: AsyncSession, row: TaskImageMaterialization,
) -> TaskBundleSourceSpecV1 | None:
    # Call only after the caller/image/attempt locks. Cleanup deliberately does
    # not enter here: loss of input must never prevent closing an allocation.
    if row.state == "retired":
        raise TaskImageSessionMaterializationConflictError("task-image source owner is retired")
    try:
        return await admit_task_image_source(session, row=row)
    except ValueError as error:
        raise TaskImageSessionMaterializationConflictError(f"task-image source unavailable: {error}") from None


async def _admitted_plan(
    session: AsyncSession, row: TaskImageMaterialization,
    authorization: TaskImageBuildSessionAuthorization,
) -> TaskImageBuildPlan:
    source = await _admit_source(session, row)
    return derive_task_image_build_plan(row, authorization, admitted_source=source)


async def _claim_replay(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    claim_id: UUID,
) -> tuple[TaskImageMaterialization, TaskImageBuildPlan] | None:
    # Discover identity without taking the child lock. Normal lease operations
    # and retirement acquire materialization -> attempt; child-first replay would
    # deadlock with either parent owner. The observation is not authority and is
    # reloaded and revalidated after acquiring both locks in that shared order.
    observed = (
        await session.execute(
            select(
                TaskImageMaterializationAttempt.id,
                TaskImageMaterializationAttempt.materialization_id,
                TaskImageMaterializationAttempt.grant_id,
                TaskImageMaterializationAttempt.session_id,
                TaskImageMaterializationAttempt.session_generation,
            ).where(TaskImageMaterializationAttempt.claim_id == claim_id)
        )
    ).one_or_none()
    if observed is None:
        return None
    if (
        observed.grant_id != authorization.grant_id
        or observed.session_id != authorization.session_id
        or observed.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image claim identity was already used"
        )
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == observed.materialization_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if row is None:
        raise TaskImageSessionMaterializationConflictError(
            "task-image claim replay materialization is unavailable"
        )
    attempt = await session.scalar(
        select(TaskImageMaterializationAttempt)
        .where(
            TaskImageMaterializationAttempt.id == observed.id,
            TaskImageMaterializationAttempt.claim_id == claim_id,
        )
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if (
        attempt is None
        or attempt.materialization_id != row.id
        or attempt.grant_id != authorization.grant_id
        or attempt.session_id != authorization.session_id
        or attempt.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image claim replay identity changed"
        )
    if await attempt_is_retired(session, attempt_id=attempt.id):
        raise TaskImageSessionMaterializationConflictError("task-image attempt is permanently retired")
    await _admit_source(session, row)
    return row, _stored_claim_plan(
        attempt,
        authorization=authorization,
        materialization_id=row.id,
    )


async def claim_session_materialization(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    claim_id: UUID,
    now: datetime,
    lease_seconds: float,
) -> tuple[TaskImageMaterialization, TaskImageBuildPlan] | None:
    """Claim one native materialization under the exact current build session."""

    now = _utc(now)
    claim_id = _nonzero_id(claim_id, label="claim_id")
    deadline = _lease_deadline(now=now, lease_seconds=lease_seconds)
    await lock_current_task_image_build_session_authority(
        session,
        authorization=authorization,
        now=now,
        source_admission=True,
    )
    replay = await _claim_replay(
        session,
        authorization=authorization,
        claim_id=claim_id,
    )
    if replay is not None:
        return replay

    live_materialization_id = await session.scalar(
        select(TaskImageMaterialization.id)
        .join(
            TaskImageMaterializationAttempt,
            and_(
                TaskImageMaterializationAttempt.materialization_id
                == TaskImageMaterialization.id,
                TaskImageMaterializationAttempt.lease_epoch
                == TaskImageMaterialization.lease_epoch,
                TaskImageMaterializationAttempt.builder_id
                == TaskImageMaterialization.claimed_by,
            ),
        )
        .where(
            TaskImageMaterializationAttempt.grant_id == authorization.grant_id,
            TaskImageMaterializationAttempt.claim_id.is_not(None),
            TaskImageMaterialization.state.in_(("claimed", "running")),
            TaskImageMaterialization.lease_expires_at > now,
        )
        .limit(1)
        .with_for_update()
    )
    if live_materialization_id is not None:
        return None

    builder_id = f"rootless:{authorization.session_id.hex}"
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(
            TaskImageMaterialization.cpu_arch == authorization.cpu_arch,
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
        .execution_options(populate_existing=True)
        .with_for_update(skip_locked=True)
    )
    if row is None:
        return None

    # Derivation must reject malformed frozen state before any lease field changes.
    plan = await _admitted_plan(session, row, authorization)
    plan_json, plan_sha256 = _plan_snapshot(plan)
    next_attempt_number = (
        int(
            (
                await session.scalar(
                    select(func.max(TaskImageMaterializationAttempt.attempt_number)).where(
                        TaskImageMaterializationAttempt.materialization_id == row.id
                    )
                )
            )
            or 0
        )
        + 1
    )
    row.state = "claimed"
    row.claimed_by = builder_id
    row.lease_epoch += 1
    row.lease_expires_at = deadline
    row.claimed_at = now
    row.started_at = None
    row.finished_at = None
    row.next_attempt_at = None
    row.failure_reason = None
    row.failure_message = None
    row.updated_at = now
    session.add(
        TaskImageMaterializationAttempt(
            materialization_id=row.id,
            attempt_number=next_attempt_number,
            lease_epoch=row.lease_epoch,
            builder_id=builder_id,
            grant_id=authorization.grant_id,
            session_id=authorization.session_id,
            session_generation=authorization.session_generation,
            claim_id=claim_id,
            claim_deterministic_failure_count=row.attempt_count,
            claim_lease_expires_at=deadline,
            claim_plan_json=plan_json,
            claim_plan_sha256=plan_sha256,
            claimed_at=now,
        )
    )
    await session.flush()
    return row, plan


async def _operation_replay(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    operation_type: OperationType,
    operation_id: UUID,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
) -> TaskImageMaterialization | None:
    event = await session.scalar(
        select(TaskImageMaterializationOperationEvent)
        .where(TaskImageMaterializationOperationEvent.operation_id == operation_id)
        .with_for_update()
    )
    if event is None:
        return None
    if (
        event.operation_type != operation_type
        or event.materialization_id != materialization_id
        or event.materialization_attempt_id != attempt_id
        or event.lease_epoch != lease_epoch
        or event.grant_id != authorization.grant_id
        or event.session_id != authorization.session_id
        or event.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image operation identity was already used"
        )
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if row is None:
        raise TaskImageSessionMaterializationConflictError(
            "task-image operation replay materialization is unavailable"
        )
    if operation_type in ("start", "heartbeat") and await attempt_is_retired(
        session, attempt_id=attempt_id
    ):
        raise TaskImageSessionMaterializationConflictError("task-image attempt is permanently retired")
    if operation_type in ("start", "heartbeat"):
        await _admit_source(session, row)
    return row


async def lock_session_materialization_lease(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    allowed_states: tuple[str, ...],
    now: datetime,
    cleanup_only: bool = False,
) -> tuple[TaskImageMaterialization, TaskImageMaterializationAttempt]:
    # Only trusted release/failure transitions opt out: they return no bundle,
    # build plan, registry credential, candidate or publication authority.
    if not cleanup_only:
        await _require_source_transaction(session)
    _reject_pending_materialization_authority(session)
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if (
        row is None
        or row.state not in allowed_states
        or row.lease_epoch != lease_epoch
        or row.lease_expires_at is None
        or row.lease_expires_at <= now
    ):
        raise TaskImageSessionMaterializationConflictError(
            "stale task-image session materialization lease"
        )
    attempt = await session.scalar(
        select(TaskImageMaterializationAttempt)
        .where(
            TaskImageMaterializationAttempt.id == attempt_id,
            TaskImageMaterializationAttempt.materialization_id == materialization_id,
            TaskImageMaterializationAttempt.lease_epoch == lease_epoch,
            TaskImageMaterializationAttempt.builder_id == row.claimed_by,
            TaskImageMaterializationAttempt.grant_id == authorization.grant_id,
            TaskImageMaterializationAttempt.claim_id.is_not(None),
        )
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if attempt is None:
        raise TaskImageSessionMaterializationConflictError(
            "stale task-image session materialization attempt"
        )
    if not cleanup_only and await attempt_is_retired(session, attempt_id=attempt.id):
        raise TaskImageSessionMaterializationConflictError("task-image attempt is permanently retired")
    if not cleanup_only:
        await _admit_source(session, row)
    return row, attempt


def _append_operation(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    operation_type: OperationType,
    operation_id: UUID,
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    now: datetime,
) -> None:
    session.add(
        TaskImageMaterializationOperationEvent(
            operation_id=operation_id,
            operation_type=operation_type,
            materialization_attempt_id=attempt.id,
            materialization_id=row.id,
            attempt_number=attempt.attempt_number,
            lease_epoch=attempt.lease_epoch,
            builder_id=attempt.builder_id,
            grant_id=authorization.grant_id,
            session_id=authorization.session_id,
            session_generation=authorization.session_generation,
            result_state=row.state,
            result_attempt_count=row.attempt_count,
            result_lease_expires_at=row.lease_expires_at,
            recorded_at=now,
        )
    )


async def _revoke_containment_session(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    operation_id: UUID,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    now: datetime,
) -> None:
    """Revoke the exact current session after a containment failure."""

    projection = await session.scalar(
        select(TaskImageBuildProjection)
        .where(TaskImageBuildProjection.grant_id == authorization.grant_id)
        .with_for_update()
    )
    if (
        projection is None
        or projection.state != "exchanged"
        or projection.session_id != authorization.session_id
        or projection.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image materialization session is unavailable"
        )
    projection.state = "revoked"
    projection.revoked_at = now
    projection.revoke_reason = "containment_failure"
    projection.event_sequence += 1
    projection.updated_at = now
    session.add(
        TaskImageBuildProjectionEvent(
            grant_id=projection.grant_id,
            event_sequence=projection.event_sequence,
            event_type="revoked",
            event_key=f"containment:{operation_id.hex}",
            payload_json={
                "reason": "containment_failure",
                "operation_id": str(operation_id),
                "materialization_id": str(materialization_id),
                "attempt_id": str(attempt_id),
                "lease_epoch": lease_epoch,
            },
            created_at=now,
        )
    )


async def _prepare_operation(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    operation_type: OperationType,
    operation_id: UUID,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    allowed_states: tuple[str, ...],
    now: datetime,
) -> tuple[datetime, TaskImageMaterialization | None, TaskImageMaterializationAttempt | None]:
    now = _utc(now)
    _nonzero_id(operation_id, label="operation_id")
    _nonzero_id(materialization_id, label="materialization_id")
    _nonzero_id(attempt_id, label="attempt_id")
    if lease_epoch <= 0:
        raise ValueError("lease_epoch must be positive")
    await lock_current_task_image_build_session_authority(
        session,
        authorization=authorization,
        now=now,
        source_admission=operation_type in ("start", "heartbeat", "bundle"),
    )
    replay = await _operation_replay(
        session,
        authorization=authorization,
        operation_type=operation_type,
        operation_id=operation_id,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
    )
    if replay is not None:
        return now, replay, None
    row, attempt = await lock_session_materialization_lease(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=allowed_states,
        now=now,
        cleanup_only=operation_type in ("release", "containment_release", "deterministic_fail"),
    )
    return now, row, attempt


async def start_session_materialization(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    now: datetime,
) -> TaskImageMaterialization:
    now, row, attempt = await _prepare_operation(
        session,
        authorization=authorization,
        operation_type="start",
        operation_id=operation_id,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed",),
        now=now,
    )
    assert row is not None
    if attempt is None:
        return row
    row.state = "running"
    row.started_at = now
    row.lease_expires_at = _lease_deadline(
        now=now,
        lease_seconds=DEFAULT_SESSION_MATERIALIZATION_LEASE_SECONDS,
    )
    row.updated_at = now
    _append_operation(
        session,
        authorization=authorization,
        operation_type="start",
        operation_id=operation_id,
        row=row,
        attempt=attempt,
        now=now,
    )
    await session.flush()
    return row


async def heartbeat_session_materialization(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    now: datetime,
) -> TaskImageMaterialization:
    now, row, attempt = await _prepare_operation(
        session,
        authorization=authorization,
        operation_type="heartbeat",
        operation_id=operation_id,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    assert row is not None
    if attempt is None:
        return row
    row.lease_expires_at = _lease_deadline(
        now=now,
        lease_seconds=DEFAULT_SESSION_MATERIALIZATION_LEASE_SECONDS,
    )
    row.updated_at = now
    _append_operation(
        session,
        authorization=authorization,
        operation_type="heartbeat",
        operation_id=operation_id,
        row=row,
        attempt=attempt,
        now=now,
    )
    await session.flush()
    return row


async def get_session_materialization_build_plan(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    now: datetime,
) -> TaskImageBuildPlan:
    """Re-derive a plan only for the exact current live lease, without mutation."""

    now = _utc(now)
    _nonzero_id(materialization_id, label="materialization_id")
    _nonzero_id(attempt_id, label="attempt_id")
    if lease_epoch <= 0:
        raise ValueError("lease_epoch must be positive")
    await lock_current_task_image_build_session_authority(
        session,
        authorization=authorization,
        now=now,
        source_admission=True,
    )
    row, _attempt = await lock_session_materialization_lease(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    return await _admitted_plan(session, row, authorization)


@dataclass(frozen=True, slots=True)
class TaskImageBundlePreparation:
    plan: TaskImageBuildPlan
    capability: TaskImageBundleCapability | None = field(repr=False)
    checked_at: datetime
    valid_until: datetime


@dataclass(frozen=True, slots=True)
class _LockedBundleState:
    row: TaskImageMaterialization
    attempt: TaskImageMaterializationAttempt
    event: TaskImageMaterializationOperationEvent | None
    plan: TaskImageBuildPlan
    checked_at: datetime
    valid_until: datetime


def _bundle_time(clock: Callable[[], datetime], *, previous: datetime, valid_until: datetime) -> datetime:
    current = _utc(clock())
    if current < previous or current >= valid_until:
        raise TaskImageBundleCapabilityError("task-image bundle authority expired or clock regressed")
    return current


async def _lock_bundle_state(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    clock: Callable[[], datetime],
) -> _LockedBundleState:
    now = _utc(clock())
    _nonzero_id(operation_id, label="operation_id")
    _nonzero_id(materialization_id, label="materialization_id")
    _nonzero_id(attempt_id, label="attempt_id")
    if lease_epoch <= 0:
        raise ValueError("lease_epoch must be positive")
    await lock_current_task_image_build_session_authority(
        session,
        authorization=authorization,
        now=now,
        source_admission=True,
    )
    event = await session.scalar(
        select(TaskImageMaterializationOperationEvent)
        .where(TaskImageMaterializationOperationEvent.operation_id == operation_id)
        .with_for_update()
    )
    if event is not None and (
        event.operation_type != "bundle"
        or event.materialization_id != materialization_id
        or event.materialization_attempt_id != attempt_id
        or event.lease_epoch != lease_epoch
        or event.grant_id != authorization.grant_id
        or event.session_id != authorization.session_id
        or event.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image operation identity was already used"
        )
    row, attempt = await lock_session_materialization_lease(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    assert row.lease_expires_at is not None
    valid_until = min(
        authorization.grant_expires_at, authorization.attestation_expires_at,
        authorization.session_expires_at, row.lease_expires_at,
    )
    checked_at = _bundle_time(clock, previous=now, valid_until=valid_until)
    plan = await _admitted_plan(session, row, authorization)
    claimed = _stored_attempt_claim_plan(attempt, authorization=authorization, materialization_id=materialization_id)
    # Original claim identity is validated against the attempt above. A renewed
    # session changes live capability identity, not the frozen build inputs.
    live_fields = {"authorization_expires_at", "session_id", "session_generation", "builder_id"}
    if plan.model_dump(exclude=live_fields) != claimed.model_dump(exclude=live_fields):
        raise TaskImageSessionMaterializationConflictError("task-image frozen bundle plan changed")
    plan = plan.model_copy(update={"authorization_expires_at": valid_until})
    return _LockedBundleState(row, attempt, event, plan, checked_at, valid_until)


async def _bundle_preparation(
    state: _LockedBundleState, *, provider: TaskImageBundleCapabilityProvider | AsyncTaskImageBundleCapabilityProvider,
    secret_store: SecretStore, clock: Callable[[], datetime],
) -> TaskImageBundlePreparation:
    event = state.event
    now = _bundle_time(clock, previous=state.checked_at, valid_until=state.valid_until)
    capability = None
    if event is not None:
        if (
            event.secret_response_ref is None
            or event.secret_response_sha256 is None
            or event.secret_response_expires_at is None
            or event.secret_response_expires_at <= now
        ):
            raise TaskImageBundleCapabilityError(
                "task-image bundle capability replay is unavailable"
            )
        try:
            payload = await secret_store.get(event.secret_response_ref)
            if not hmac.compare_digest(
                hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                event.secret_response_sha256,
            ):
                raise ValueError("bundle capability digest changed")
            capability = parse_task_image_bundle_capability(payload)
        except Exception:
            raise TaskImageBundleCapabilityError(
                "task-image bundle capability replay is unavailable"
            ) from None
        if (
            capability.expires_at != event.secret_response_expires_at
        ):
            raise TaskImageBundleCapabilityError(
                "task-image bundle capability replay is unavailable"
            )
        now = _bundle_time(clock, previous=now, valid_until=min(state.valid_until, capability.expires_at))
        provider.validate(capability, state.plan, now=now)
    now = _bundle_time(clock, previous=now, valid_until=state.valid_until)
    return TaskImageBundlePreparation(state.plan, capability, now, state.valid_until)


async def prepare_session_materialization_bundle(
    session: AsyncSession, *, authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID, attempt_id: UUID, lease_epoch: int, operation_id: UUID,
    provider: TaskImageBundleCapabilityProvider | AsyncTaskImageBundleCapabilityProvider,
    secret_store: SecretStore, clock: Callable[[], datetime],
) -> TaskImageBundlePreparation:
    """Prepare under locks; owner must release the transaction before storage I/O."""
    state = await _lock_bundle_state(
        session, authorization=authorization, materialization_id=materialization_id,
        attempt_id=attempt_id, lease_epoch=lease_epoch, operation_id=operation_id, clock=clock,
    )
    return await _bundle_preparation(state, provider=provider, secret_store=secret_store, clock=clock)


async def finalize_session_materialization_bundle(
    session: AsyncSession, *, authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID, attempt_id: UUID, lease_epoch: int, operation_id: UUID,
    prepared: TaskImageBundlePreparation, capability: TaskImageBundleCapability,
    provider: TaskImageBundleCapabilityProvider | AsyncTaskImageBundleCapabilityProvider,
    secret_store: SecretStore, clock: Callable[[], datetime],
) -> TaskImageBundlePreparation:
    """Fresh admission and an idempotent winner after unlocked bundle I/O."""
    state = await _lock_bundle_state(
        session, authorization=authorization, materialization_id=materialization_id,
        attempt_id=attempt_id, lease_epoch=lease_epoch, operation_id=operation_id, clock=clock,
    )
    current = await _bundle_preparation(state, provider=provider, secret_store=secret_store, clock=clock)
    if current.capability is not None:
        return current
    if (
        prepared.plan.model_dump(exclude={"authorization_expires_at"})
        != current.plan.model_dump(exclude={"authorization_expires_at"})
        or capability.expires_at > prepared.plan.authorization_expires_at
    ):
        raise TaskImageSessionMaterializationConflictError("task-image frozen bundle plan changed")
    now = _bundle_time(clock, previous=max(prepared.checked_at, current.checked_at), valid_until=min(current.valid_until, capability.expires_at))
    provider.validate(capability, current.plan, now=now)
    payload = capability.model_dump_json()
    secret_ref = await secret_store.put(
        namespace="task-image-bundle-capability",
        value=payload,
    )
    if not secret_ref.startswith("loom://task-image-bundle-capability/"):
        raise TaskImageBundleCapabilityError(
            "task-image bundle capability storage is unavailable"
        )
    session.add(
        TaskImageMaterializationOperationEvent(
            operation_id=operation_id,
            operation_type="bundle",
            materialization_attempt_id=state.attempt.id,
            materialization_id=state.row.id,
            attempt_number=state.attempt.attempt_number,
            lease_epoch=state.attempt.lease_epoch,
            builder_id=state.attempt.builder_id,
            grant_id=authorization.grant_id,
            session_id=authorization.session_id,
            session_generation=authorization.session_generation,
            result_state=state.row.state,
            result_attempt_count=state.row.attempt_count,
            result_lease_expires_at=state.row.lease_expires_at,
            secret_response_ref=secret_ref,
            secret_response_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            secret_response_expires_at=capability.expires_at,
            recorded_at=now,
        )
    )
    try:
        await session.flush()
    except IntegrityError as error:
        cause = error.orig.__cause__ if error.orig is not None else None
        constraint_name = getattr(getattr(error.orig, "diag", None), "constraint_name", None)
        if constraint_name is None:
            constraint_name = getattr(cause, "constraint_name", None)
        if (
            getattr(error.orig, "sqlstate", None) == "23505"
            and constraint_name == "task_image_materialization_operation_events_operation_uidx"
        ):
            raise TaskImageSessionMaterializationConflictError("task-image operation identity was already used") from None
        raise
    now = _bundle_time(clock, previous=now, valid_until=min(current.valid_until, capability.expires_at))
    return TaskImageBundlePreparation(current.plan, capability, now, current.valid_until)


async def issue_session_materialization_bundle(
    session: AsyncSession, *, authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID, attempt_id: UUID, lease_epoch: int, operation_id: UUID,
    now: datetime, provider: TaskImageBundleCapabilityProvider, secret_store: SecretStore,
) -> TaskImageBundleCapabilityV1:
    """Legacy synchronous injected-provider helper; HTTP uses unlocked phases."""
    prepared = await prepare_session_materialization_bundle(
        session, authorization=authorization, materialization_id=materialization_id,
        attempt_id=attempt_id, lease_epoch=lease_epoch, operation_id=operation_id,
        provider=provider, secret_store=secret_store, clock=lambda: now,
    )
    if prepared.capability is not None:
        assert isinstance(prepared.capability, TaskImageBundleCapabilityV1)
        return prepared.capability
    finalized = await finalize_session_materialization_bundle(
        session, authorization=authorization, materialization_id=materialization_id,
        attempt_id=attempt_id, lease_epoch=lease_epoch, operation_id=operation_id,
        prepared=prepared, capability=provider.issue(prepared.plan, now=now),
        provider=provider, secret_store=secret_store, clock=lambda: now,
    )
    assert isinstance(finalized.capability, TaskImageBundleCapabilityV1)
    return finalized.capability


async def _release_operation(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    now: datetime,
    operation_type: Literal["release", "containment_release"],
) -> TaskImageMaterialization:
    now, row, attempt = await _prepare_operation(
        session,
        authorization=authorization,
        operation_type=operation_type,
        operation_id=operation_id,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    assert row is not None
    if attempt is None:
        return row
    row.state = "queued"
    row.claimed_by = None
    row.lease_expires_at = None
    row.next_attempt_at = now + timedelta(
        seconds=min(
            30.0 * (2 ** max(row.attempt_count - 1, 0)),
            _MAX_RETRY_BACKOFF_SECONDS,
        )
    )
    row.failure_reason = (
        "containment_failure"
        if operation_type == "containment_release"
        else "infrastructure_release"
    )
    row.failure_message = (
        "task-image containment authority was lost"
        if operation_type == "containment_release"
        else "task-image build requires retryable infrastructure work"
    )
    row.finished_at = None
    row.updated_at = now
    _append_operation(
        session,
        authorization=authorization,
        operation_type=operation_type,
        operation_id=operation_id,
        row=row,
        attempt=attempt,
        now=now,
    )
    if operation_type == "containment_release":
        await _revoke_containment_session(
            session,
            authorization=authorization,
            operation_id=operation_id,
            materialization_id=materialization_id,
            attempt_id=attempt_id,
            lease_epoch=lease_epoch,
            now=now,
        )
    await session.flush()
    return row


async def release_session_materialization(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    now: datetime,
) -> TaskImageMaterialization:
    return await _release_operation(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        operation_id=operation_id,
        now=now,
        operation_type="release",
    )


async def release_containment_failed_session_materialization(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    now: datetime,
) -> TaskImageMaterialization:
    return await _release_operation(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        operation_id=operation_id,
        now=now,
        operation_type="containment_release",
    )


async def fail_session_materialization(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    now: datetime,
) -> TaskImageMaterialization:
    """Record one deterministic build failure against the bounded budget."""

    now, row, attempt = await _prepare_operation(
        session,
        authorization=authorization,
        operation_type="deterministic_fail",
        operation_id=operation_id,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    assert row is not None
    if attempt is None:
        return row
    row.attempt_count += 1
    row.claimed_by = None
    row.lease_expires_at = None
    row.failure_reason = "deterministic_build_failure"
    row.failure_message = "task-image build failed deterministically"
    row.updated_at = now
    if row.attempt_count < row.max_attempts:
        row.state = "queued"
        row.next_attempt_at = now + timedelta(
            seconds=min(
                30.0 * (2 ** max(row.attempt_count - 1, 0)),
                _MAX_RETRY_BACKOFF_SECONDS,
            )
        )
        row.finished_at = None
    else:
        row.state = "failed"
        row.next_attempt_at = None
        row.finished_at = now
    _append_operation(
        session,
        authorization=authorization,
        operation_type="deterministic_fail",
        operation_id=operation_id,
        row=row,
        attempt=attempt,
        now=now,
    )
    await session.flush()
    return row


__all__ = [
    "DEFAULT_SESSION_MATERIALIZATION_LEASE_SECONDS",
    "MAX_SESSION_MATERIALIZATION_LEASE_SECONDS",
    "TaskImageSessionMaterializationAuthorizationError",
    "TaskImageSessionMaterializationConflictError",
    "claim_session_materialization",
    "fail_session_materialization",
    "get_session_materialization_build_plan",
    "heartbeat_session_materialization",
    "issue_session_materialization_bundle",
    "release_containment_failed_session_materialization",
    "release_session_materialization",
    "start_session_materialization",
]
