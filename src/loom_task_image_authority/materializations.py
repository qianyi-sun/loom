"""Session-authorized materialization leases for the rootless build provider."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

import rfc8785
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageBuildGrant,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
    TaskImageBuildSessionGeneration,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageMaterializationOperationEvent,
)
from loom.security.secret_store import SecretStore
from loom.task_image_build_plan import TaskImageBuildPlanV1, derive_task_image_build_plan
from loom_task_image_authority.bundle_capability import (
    TaskImageBundleCapabilityError,
    TaskImageBundleCapabilityProvider,
    TaskImageBundleCapabilityV1,
)
from loom_task_image_authority.store import TaskImageBuildSessionAuthorization

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


def _plan_snapshot(plan: TaskImageBuildPlanV1) -> tuple[dict[str, object], str]:
    payload = plan.model_dump(mode="json", exclude_none=False)
    return payload, hashlib.sha256(rfc8785.dumps(payload)).hexdigest()


def _stored_claim_plan(
    attempt: TaskImageMaterializationAttempt,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
) -> TaskImageBuildPlanV1:
    if attempt.claim_plan_json is None or attempt.claim_plan_sha256 is None:
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        )
    try:
        plan = TaskImageBuildPlanV1.model_validate_json(json.dumps(attempt.claim_plan_json))
        payload, digest = _plan_snapshot(plan)
    except (TypeError, ValueError):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        ) from None
    if (
        digest != attempt.claim_plan_sha256
        or payload != attempt.claim_plan_json
        or plan.grant_id != authorization.grant_id
        or plan.session_id != authorization.session_id
        or plan.session_generation != authorization.session_generation
        or plan.materialization_id != materialization_id
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        )
    return plan


async def _lock_current_session_authority(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    now: datetime,
) -> TaskImageBuildSessionGeneration:
    """Recheck and lock grant → projection → current generation in canonical order."""

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


async def _claim_replay(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    claim_id: UUID,
) -> tuple[TaskImageMaterialization, TaskImageBuildPlanV1] | None:
    attempt = await session.scalar(
        select(TaskImageMaterializationAttempt)
        .where(TaskImageMaterializationAttempt.claim_id == claim_id)
        .with_for_update()
    )
    if attempt is None:
        return None
    if (
        attempt.grant_id != authorization.grant_id
        or attempt.session_id != authorization.session_id
        or attempt.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image claim identity was already used"
        )
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == attempt.materialization_id)
        .with_for_update()
    )
    if row is None:
        raise TaskImageSessionMaterializationConflictError(
            "task-image claim replay materialization is unavailable"
        )
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
) -> tuple[TaskImageMaterialization, TaskImageBuildPlanV1] | None:
    """Claim one native materialization under the exact current build session."""

    now = _utc(now)
    claim_id = _nonzero_id(claim_id, label="claim_id")
    deadline = _lease_deadline(now=now, lease_seconds=lease_seconds)
    await _lock_current_session_authority(
        session,
        authorization=authorization,
        now=now,
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
        .with_for_update(skip_locked=True)
    )
    if row is None:
        return None

    # Derivation must reject malformed frozen state before any lease field changes.
    plan = derive_task_image_build_plan(row, authorization)
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
        .with_for_update()
    )
    if row is None:
        raise TaskImageSessionMaterializationConflictError(
            "task-image operation replay materialization is unavailable"
        )
    return row


async def _locked_operation_lease(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    allowed_states: tuple[str, ...],
    now: datetime,
) -> tuple[TaskImageMaterialization, TaskImageMaterializationAttempt]:
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
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
        .with_for_update()
    )
    if attempt is None:
        raise TaskImageSessionMaterializationConflictError(
            "stale task-image session materialization attempt"
        )
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
    await _lock_current_session_authority(
        session,
        authorization=authorization,
        now=now,
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
    row, attempt = await _locked_operation_lease(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=allowed_states,
        now=now,
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
) -> TaskImageBuildPlanV1:
    """Re-derive a plan only for the exact current live lease, without mutation."""

    now = _utc(now)
    _nonzero_id(materialization_id, label="materialization_id")
    _nonzero_id(attempt_id, label="attempt_id")
    if lease_epoch <= 0:
        raise ValueError("lease_epoch must be positive")
    await _lock_current_session_authority(
        session,
        authorization=authorization,
        now=now,
    )
    row, _attempt = await _locked_operation_lease(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    return derive_task_image_build_plan(row, authorization)


async def issue_session_materialization_bundle(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    operation_id: UUID,
    now: datetime,
    provider: TaskImageBundleCapabilityProvider,
    secret_store: SecretStore,
) -> TaskImageBundleCapabilityV1:
    """Issue or replay one encrypted, operation-bound bundle capability."""

    now = _utc(now)
    _nonzero_id(operation_id, label="operation_id")
    _nonzero_id(materialization_id, label="materialization_id")
    _nonzero_id(attempt_id, label="attempt_id")
    if lease_epoch <= 0:
        raise ValueError("lease_epoch must be positive")
    await _lock_current_session_authority(
        session,
        authorization=authorization,
        now=now,
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
    row, attempt = await _locked_operation_lease(
        session,
        authorization=authorization,
        materialization_id=materialization_id,
        attempt_id=attempt_id,
        lease_epoch=lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
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
            capability = TaskImageBundleCapabilityV1.model_validate_json(payload)
        except Exception:
            raise TaskImageBundleCapabilityError(
                "task-image bundle capability replay is unavailable"
            ) from None
        if (
            capability.grant_id != authorization.grant_id
            or capability.session_id != authorization.session_id
            or capability.session_generation != authorization.session_generation
            or capability.materialization_id != materialization_id
            or capability.expires_at != event.secret_response_expires_at
        ):
            raise TaskImageBundleCapabilityError(
                "task-image bundle capability replay is unavailable"
            )
        return capability

    plan = derive_task_image_build_plan(row, authorization)
    capability = provider.issue(plan, now=now)
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
            secret_response_ref=secret_ref,
            secret_response_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            secret_response_expires_at=capability.expires_at,
            recorded_at=now,
        )
    )
    await session.flush()
    return capability


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
