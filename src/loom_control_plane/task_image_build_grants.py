"""Recoverable one-invocation task-image build grant state machine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TaskImageBuildGrant, TaskImageBuildGrantEvent
from loom_control_plane.task_image_build_environment import (
    SlurmBuildGrantV1,
    SlurmBuildInventoryV1,
    SlurmBuildJobObservationV1,
)
from loom_task_image_authority.contracts import canonical_authority_sha256


class TaskImageBuildGrantConflictError(RuntimeError):
    """The requested grant transition conflicts with durable state."""


@dataclass(frozen=True)
class TaskImageBuildInventoryDecision:
    action: Literal["wait", "bind", "revoke", "cancel_then_reconcile"]
    reason: str
    bind_job_id: str | None = None
    cancel_job_ids: tuple[str, ...] = ()


def _observation_matches_grant(
    grant: SlurmBuildGrantV1,
    observation: SlurmBuildJobObservationV1,
) -> bool:
    return (
        observation.comment == grant.comment
        and observation.submitting_identity == grant.request.submitting_identity
        and observation.request == grant.request
    )


def classify_task_image_build_inventory(
    grant: SlurmBuildGrantV1,
    inventory: SlurmBuildInventoryV1,
    *,
    invocation_started_at: datetime,
    ambiguity_settle_until: datetime,
    now: datetime,
) -> TaskImageBuildInventoryDecision:
    """Classify complete controller/accounting evidence without side effects."""
    if not inventory.controller_authoritative or not inventory.accounting_authoritative:
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_incomplete",
        )
    if any(job.state == "unknown" for job in inventory.jobs):
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_state_unknown",
        )
    if inventory.observed_at > now:
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_snapshot_is_in_the_future",
        )
    if inventory.observed_at < invocation_started_at:
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_snapshot_precedes_submission",
        )
    if not inventory.jobs:
        if inventory.observed_at < ambiguity_settle_until:
            return TaskImageBuildInventoryDecision(
                action="wait",
                reason="inventory_snapshot_precedes_settle_deadline",
            )
        return TaskImageBuildInventoryDecision(
            action="revoke",
            reason="authoritative_inventory_empty",
        )

    live_jobs = tuple(job for job in inventory.jobs if job.state in {"pending", "running"})
    terminal_jobs = tuple(job for job in inventory.jobs if job.state == "terminal")
    if len(inventory.jobs) == 1:
        job = inventory.jobs[0]
        if job.state == "terminal":
            return TaskImageBuildInventoryDecision(
                action="revoke",
                reason="terminal_submission_observed",
            )
        if (
            job.state == "pending"
            and job.held
            and _observation_matches_grant(grant, job)
        ):
            return TaskImageBuildInventoryDecision(
                action="bind",
                reason="one_exact_held_submission",
                bind_job_id=job.job_id,
            )

    if live_jobs:
        return TaskImageBuildInventoryDecision(
            action="cancel_then_reconcile",
            reason="submission_protocol_violation",
            cancel_job_ids=tuple(sorted((job.job_id for job in live_jobs), key=int)),
        )
    if terminal_jobs:
        return TaskImageBuildInventoryDecision(
            action="revoke",
            reason="terminal_submission_observed",
        )
    return TaskImageBuildInventoryDecision(
        action="wait",
        reason="inventory_state_unknown",
    )


def _classify_revoked_grant_inventory(
    inventory: SlurmBuildInventoryV1,
    *,
    revoked_at: datetime,
    now: datetime,
) -> TaskImageBuildInventoryDecision:
    if not inventory.controller_authoritative or not inventory.accounting_authoritative:
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_incomplete",
        )
    if any(job.state == "unknown" for job in inventory.jobs):
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_state_unknown",
        )
    if inventory.observed_at > now:
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_snapshot_is_in_the_future",
        )
    if inventory.observed_at < revoked_at:
        return TaskImageBuildInventoryDecision(
            action="wait",
            reason="inventory_snapshot_precedes_revocation",
        )
    live_job_ids = tuple(
        sorted(
            (job.job_id for job in inventory.jobs if job.state in {"pending", "running"}),
            key=int,
        )
    )
    if live_job_ids:
        return TaskImageBuildInventoryDecision(
            action="cancel_then_reconcile",
            reason="revoked_grant_live_submission",
            cancel_job_ids=live_job_ids,
        )
    return TaskImageBuildInventoryDecision(
        action="revoke",
        reason="revoked_grant_has_no_live_submission",
    )


async def _locked_grant(
    session: AsyncSession,
    *,
    grant_id: UUID,
) -> TaskImageBuildGrant:
    row = await session.scalar(
        select(TaskImageBuildGrant)
        .where(TaskImageBuildGrant.id == grant_id)
        .with_for_update()
    )
    if row is None:
        raise TaskImageBuildGrantConflictError(f"task-image build grant {grant_id} not found")
    return row


def _append_event(
    session: AsyncSession,
    *,
    row: TaskImageBuildGrant,
    event_type: str,
    payload: dict[str, object],
    now: datetime,
) -> None:
    row.journal_sequence += 1
    session.add(
        TaskImageBuildGrantEvent(
            grant_id=row.id,
            sequence=row.journal_sequence,
            event_type=event_type,
            payload=payload,
            created_at=now,
        )
    )


def _stored_grant(row: TaskImageBuildGrant) -> SlurmBuildGrantV1:
    grant = SlurmBuildGrantV1.model_validate(
        {
            "schema": "loom.task-image-build-grant/v1",
            "grant_id": row.id,
            "request": row.request_spec,
            "request_sha256": row.request_sha256,
            "authority": row.authority_spec,
            "authority_sha256": row.authority_sha256,
            "comment": row.slurm_comment,
        }
    )
    request = grant.request
    authority = grant.authority
    if (
        row.environment != authority.environment
        or row.provider != "slurm-rootless-v1"
        or row.slurm_cluster_id != request.slurm_cluster_id
        or row.cpu_arch != request.cpu_arch
        or row.submitting_identity != request.submitting_identity
        or row.slurm_account != request.account
        or row.slurm_partition != request.partition
        or row.slurm_qos != request.qos
        or row.request_sha256 != authority.slurm_request_sha256
        or row.authority_sha256 != canonical_authority_sha256(authority)
        or row.grant_expires_at != authority.expires_at
    ):
        raise ValueError("stored task-image build grant authority changed")
    return grant


def _require_live_grant(row: TaskImageBuildGrant, *, now: datetime) -> SlurmBuildGrantV1:
    grant = _stored_grant(row)
    if now >= grant.authority.expires_at:
        raise TaskImageBuildGrantConflictError(
            f"task-image build grant {row.id} authority expired"
        )
    return grant


async def issue_task_image_build_grant(
    session: AsyncSession,
    *,
    environment: str,
    grant: SlurmBuildGrantV1,
    ambiguity_settle_seconds: int,
    now: datetime,
) -> TaskImageBuildGrant:
    """Persist a new grant and its first journal record atomically."""
    grant = SlurmBuildGrantV1.model_validate(grant.model_dump(mode="python"))
    if ambiguity_settle_seconds <= 0:
        raise ValueError("ambiguity settle duration must be positive")
    authority = grant.authority
    if authority.environment != environment:
        raise ValueError("task-image build grant environment differs from authority")
    if now < authority.issued_at:
        raise ValueError("task-image build grant authority is not yet issued")
    if now >= authority.expires_at:
        raise ValueError("task-image build grant authority expired")
    request = grant.request
    row = TaskImageBuildGrant(
        id=grant.grant_id,
        environment=environment,
        provider="slurm-rootless-v1",
        slurm_cluster_id=request.slurm_cluster_id,
        cpu_arch=request.cpu_arch,
        state="issued",
        submitting_identity=request.submitting_identity,
        slurm_account=request.account,
        slurm_partition=request.partition,
        slurm_qos=request.qos,
        request_spec=request.model_dump(mode="json"),
        request_sha256=grant.request_sha256,
        authority_spec=authority.model_dump(mode="json", exclude_none=False),
        authority_sha256=grant.authority_sha256,
        grant_expires_at=authority.expires_at,
        slurm_comment=grant.comment,
        ambiguity_settle_seconds=ambiguity_settle_seconds,
        ambiguity_settle_until=None,
        journal_sequence=0,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    _append_event(
        session,
        row=row,
        event_type="issued",
        payload={},
        now=now,
    )
    await session.flush()
    return row


async def begin_task_image_build_submission(
    session: AsyncSession,
    *,
    grant_id: UUID,
    now: datetime,
) -> TaskImageBuildGrant:
    """Consume the grant's only external submission invocation authority."""
    row = await _locked_grant(session, grant_id=grant_id)
    _require_live_grant(row, now=now)
    if row.state != "issued":
        raise TaskImageBuildGrantConflictError(
            f"task-image build grant {grant_id} was already invoked"
        )
    row.state = "submitting"
    row.invocation_started_at = now
    row.ambiguity_settle_until = now + timedelta(seconds=row.ambiguity_settle_seconds)
    row.updated_at = now
    _append_event(
        session,
        row=row,
        event_type="submission_started",
        payload={},
        now=now,
    )
    await session.flush()
    return row


async def _has_cancellation_event(
    session: AsyncSession,
    *,
    grant_id: UUID,
    job_ids: tuple[str, ...],
) -> bool:
    event_id = await session.scalar(
        select(TaskImageBuildGrantEvent.id)
        .where(
            TaskImageBuildGrantEvent.grant_id == grant_id,
            TaskImageBuildGrantEvent.event_type == "cancellation_requested",
            TaskImageBuildGrantEvent.payload == {"job_ids": list(job_ids)},
        )
        .limit(1)
    )
    return event_id is not None


async def reconcile_task_image_build_submission(
    session: AsyncSession,
    *,
    grant_id: UUID,
    inventory: SlurmBuildInventoryV1,
    now: datetime,
) -> TaskImageBuildInventoryDecision:
    """Journal the safe consequence of authoritative submission inventory."""
    row = await _locked_grant(session, grant_id=grant_id)
    grant = _require_live_grant(row, now=now)
    if row.state not in {"submitting", "revoked"}:
        raise TaskImageBuildGrantConflictError(
            f"task-image build grant {grant_id} is not awaiting reconciliation"
        )
    was_revoked = row.state == "revoked"
    if was_revoked:
        if row.revoked_at is None:
            raise TaskImageBuildGrantConflictError(
                f"task-image build grant {grant_id} lacks revocation timing evidence"
            )
        decision = _classify_revoked_grant_inventory(
            inventory,
            revoked_at=row.revoked_at,
            now=now,
        )
    elif row.invocation_started_at is None or row.ambiguity_settle_until is None:
        raise TaskImageBuildGrantConflictError(
            f"task-image build grant {grant_id} lacks submission timing evidence"
        )
    else:
        decision = classify_task_image_build_inventory(
            grant,
            inventory,
            invocation_started_at=row.invocation_started_at,
            ambiguity_settle_until=row.ambiguity_settle_until,
            now=now,
        )
    if decision.action == "bind":
        if decision.bind_job_id is None:
            raise RuntimeError("bind decision lacks a Slurm job ID")
        row.state = "bound"
        row.slurm_job_id = decision.bind_job_id
        row.bound_at = now
        row.updated_at = now
        _append_event(
            session,
            row=row,
            event_type="bound",
            payload={"job_id": decision.bind_job_id},
            now=now,
        )
    elif decision.action == "revoke" and not was_revoked:
        row.state = "revoked"
        row.revoke_reason = decision.reason
        row.revoked_at = now
        row.updated_at = now
        _append_event(
            session,
            row=row,
            event_type="revoked",
            payload={"reason": decision.reason},
            now=now,
        )
    elif decision.action == "cancel_then_reconcile" and not await _has_cancellation_event(
        session,
        grant_id=grant_id,
        job_ids=decision.cancel_job_ids,
    ):
        _append_event(
            session,
            row=row,
            event_type="cancellation_requested",
            payload={"job_ids": list(decision.cancel_job_ids)},
            now=now,
        )
        row.updated_at = now
    elif decision.action == "wait":
        _append_event(
            session,
            row=row,
            event_type="reconciliation_wait",
            payload={"reason": decision.reason},
            now=now,
        )
        row.updated_at = now
    await session.flush()
    return decision


async def record_task_image_build_release(
    session: AsyncSession,
    *,
    grant_id: UUID,
    job_id: str,
    now: datetime,
) -> TaskImageBuildGrant:
    """Record release only after the exact held Slurm job is durably bound."""
    row = await _locked_grant(session, grant_id=grant_id)
    _require_live_grant(row, now=now)
    if row.state != "bound" or row.slurm_job_id != job_id:
        raise TaskImageBuildGrantConflictError(
            f"task-image build grant {grant_id} is not bound to Slurm job {job_id}"
        )
    row.state = "released"
    row.released_at = now
    row.updated_at = now
    _append_event(
        session,
        row=row,
        event_type="released",
        payload={"job_id": job_id},
        now=now,
    )
    await session.flush()
    return row


__all__ = [
    "TaskImageBuildGrantConflictError",
    "TaskImageBuildInventoryDecision",
    "begin_task_image_build_submission",
    "classify_task_image_build_inventory",
    "issue_task_image_build_grant",
    "reconcile_task_image_build_submission",
    "record_task_image_build_release",
]
