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
    if not inventory.jobs:
        if now < ambiguity_settle_until:
            return TaskImageBuildInventoryDecision(
                action="wait",
                reason="ambiguity_settle_window_open",
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
    return SlurmBuildGrantV1.model_validate(
        {
            "schema": "loom.task-image-build-grant/v1",
            "grant_id": row.id,
            "request": row.request_spec,
            "request_sha256": row.request_sha256,
            "comment": row.slurm_comment,
        }
    )


async def issue_task_image_build_grant(
    session: AsyncSession,
    *,
    environment: str,
    grant: SlurmBuildGrantV1,
    ambiguity_settle_seconds: int,
    now: datetime,
) -> TaskImageBuildGrant:
    """Persist a new grant and its first journal record atomically."""
    if ambiguity_settle_seconds <= 0:
        raise ValueError("ambiguity settle duration must be positive")
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
        slurm_comment=grant.comment,
        ambiguity_settle_until=now + timedelta(seconds=ambiguity_settle_seconds),
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
    if row.state != "issued":
        raise TaskImageBuildGrantConflictError(
            f"task-image build grant {grant_id} was already invoked"
        )
    row.state = "submitting"
    row.invocation_started_at = now
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
    if row.state != "submitting":
        raise TaskImageBuildGrantConflictError(
            f"task-image build grant {grant_id} is not awaiting reconciliation"
        )
    decision = classify_task_image_build_inventory(
        _stored_grant(row),
        inventory,
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
    elif decision.action == "revoke":
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
