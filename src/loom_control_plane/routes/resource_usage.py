"""Worker-authenticated durable trial resource-accounting writes (#1503)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import and_, case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from loom.auth import verify_bearer_token
from loom.db.schema import Trial, TrialResourceUsage, Worker
from loom.models.resource_usage import ResourceCounters, TrialResourceUsageReport
from loom.resource_usage_store import report_values
from loom_control_plane.protected_worker_session import ProtectedBodyWorkerSession
from loom_control_plane.routes.execution_fence import (
    OptionalExecutionGenerationHeader,
    OptionalExecutionLeaseIdHeader,
    enforce_trial_execution_fence,
)

router = APIRouter()
_GAUGE_COUNTERS = frozenset({"memory_current_bytes", "pids_current"})


def _monotonic(existing: Any, incoming: Any) -> Any:
    return case(
        (incoming.is_(None), existing),
        (existing.is_(None), incoming),
        else_=func.greatest(existing, incoming),
    )


@router.put("/trials/{trial_id}/resource-usage")
async def put_trial_resource_usage(
    trial_id: UUID,
    report: TrialResourceUsageReport,
    request: Request,
    protected_worker_session: ProtectedBodyWorkerSession,
    authorization: str | None = Header(default=None),
    execution_lease_id: OptionalExecutionLeaseIdHeader = None,
    execution_generation: OptionalExecutionGenerationHeader = None,
) -> dict[str, object]:
    if report.trial_id != trial_id:
        raise HTTPException(status_code=400, detail="trial_id path/body mismatch")
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
        if ctx is None or "worker:report" not in ctx.scopes:
            raise HTTPException(status_code=401, detail="not authorized to report usage")
        trial = (
            await session.execute(select(Trial).where(Trial.id == trial_id))
        ).scalar_one_or_none()
        if trial is None:
            raise HTTPException(status_code=404, detail="trial not found")
        fence = await enforce_trial_execution_fence(
            session,
            trial_id=trial_id,
            lease_id=execution_lease_id,
            generation=execution_generation,
            surface="usage",
            lock=True,
        )
        if fence is not None and report.attempt_count != fence.attempt:
            raise HTTPException(status_code=409, detail="execution_generation_fenced")
        if report.attempt_count > trial.attempt_count:
            raise HTTPException(status_code=409, detail="resource usage attempt is ahead of trial")
        if (
            report.attempt_count == trial.attempt_count
            and trial.worker_id is not None
            and report.worker_id != trial.worker_id
        ):
            raise HTTPException(status_code=409, detail="resource usage worker lost trial claim")
        worker_exists = (
            await session.execute(select(Worker.id).where(Worker.id == report.worker_id))
        ).scalar_one_or_none()
        if worker_exists is None:
            raise HTTPException(status_code=409, detail="resource usage worker not found")

        existing = (
            await session.execute(
                select(TrialResourceUsage).where(
                    TrialResourceUsage.trial_id == trial_id,
                    TrialResourceUsage.attempt_count == report.attempt_count,
                    TrialResourceUsage.execution_key == report.execution_key,
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            immutable = (
                (existing.worker_id, report.worker_id),
                (existing.container_role, report.container_role),
                (existing.role_name, report.role_name),
                (existing.backend, report.backend),
                (existing.architecture, report.architecture),
                (existing.candidate_sha, report.candidate_sha),
            )
            if any(stored != incoming_value for stored, incoming_value in immutable):
                raise HTTPException(
                    status_code=409,
                    detail="resource usage execution identity changed",
                )

        values = report_values(
            report,
            lifecycle_authority_id=trial.lifecycle_authority_id,
        )
        insert_statement = pg_insert(TrialResourceUsage).values(**values)
        incoming = insert_statement.excluded
        current = TrialResourceUsage
        counter_updates = {
            name: (
                case(
                    (incoming.observation_seq >= current.observation_seq, getattr(incoming, name)),
                    else_=getattr(current, name),
                )
                if name in _GAUGE_COUNTERS
                else _monotonic(getattr(current, name), getattr(incoming, name))
            )
            for name in ResourceCounters.model_fields
        }
        newer = incoming.observation_seq >= current.observation_seq
        same_identity = and_(
            current.worker_id == incoming.worker_id,
            current.container_role == incoming.container_role,
            current.role_name == incoming.role_name,
            current.backend == incoming.backend,
            current.architecture.is_not_distinct_from(incoming.architecture),
            current.candidate_sha.is_not_distinct_from(incoming.candidate_sha),
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            constraint="trial_resource_usage_identity_uidx",
            set_={
                "observation_seq": func.greatest(
                    current.observation_seq,
                    incoming.observation_seq,
                ),
                "last_observed_at": func.greatest(
                    current.last_observed_at,
                    incoming.last_observed_at,
                ),
                "finalized_at": case(
                    (current.finalized_at.is_(None), incoming.finalized_at),
                    (incoming.finalized_at.is_(None), current.finalized_at),
                    else_=func.greatest(current.finalized_at, incoming.finalized_at),
                ),
                "terminal_reason": case(
                    (
                        newer & incoming.finalized_at.is_not(None),
                        incoming.terminal_reason,
                    ),
                    else_=current.terminal_reason,
                ),
                "completeness": case(
                    (current.completeness == "complete", current.completeness),
                    (incoming.completeness == "complete", incoming.completeness),
                    (current.completeness == "partial", current.completeness),
                    else_=incoming.completeness,
                ),
                "diagnostic_code": case(
                    (newer & incoming.diagnostic_code.is_not(None), incoming.diagnostic_code),
                    else_=current.diagnostic_code,
                ),
                "source": case((newer, incoming.source), else_=current.source),
                "lifecycle_authority_id": func.coalesce(
                    current.lifecycle_authority_id,
                    incoming.lifecycle_authority_id,
                ),
                "runtime_id_hash": func.coalesce(current.runtime_id_hash, incoming.runtime_id_hash),
                "image_digest": func.coalesce(current.image_digest, incoming.image_digest),
                "container_started_at": func.coalesce(
                    current.container_started_at,
                    incoming.container_started_at,
                ),
                "updated_at": func.now(),
                **counter_updates,
            },
            where=same_identity,
        ).returning(TrialResourceUsage.id)
        accepted_id = (await session.execute(upsert_statement)).scalar_one_or_none()
        if accepted_id is None:
            await session.rollback()
            raise HTTPException(
                status_code=409,
                detail="resource usage execution identity changed",
            )
        await session.commit()
    return {
        "accepted": True,
        "trial_id": str(trial_id),
        "execution_key": report.execution_key,
    }
