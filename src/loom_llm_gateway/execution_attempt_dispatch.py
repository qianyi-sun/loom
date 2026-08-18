"""Live Gateway authorization for Pipeline execution-attempt step JWTs."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import ExecutionAttempt, PipelineRun, PipelineStageRun


def _control_binding_digest(execution_spec: dict[str, object] | None) -> str | None:
    if execution_spec is None:
        raise ValueError("execution spec is unavailable")
    raw_refs = execution_spec.get("control_binding_snapshots")
    if not isinstance(raw_refs, list) or len(raw_refs) > 1:
        raise ValueError("control binding set is not closed")
    if not raw_refs:
        return None
    ref = raw_refs[0]
    if not isinstance(ref, dict):
        raise ValueError("control binding ref is invalid")
    digest = ref.get("snapshot_sha256")
    if not isinstance(digest, str):
        raise ValueError("control binding digest is unavailable")
    return digest


async def authorize_execution_attempt_dispatch(
    session: AsyncSession,
    ctx: AuthContext,
) -> None:
    """Fail closed unless an attempt JWT still names the live DB authority.

    Signature verification proves who minted the bearer.  This read proves the
    Attempt has not since been cancelled, finished, lost, re-leased, rotated,
    or rebound before any request is sent to an upstream provider.
    """

    if ctx.execution_attempt_id is None:
        return
    row = (
        await session.execute(
            select(ExecutionAttempt, PipelineStageRun, PipelineRun)
            .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
            .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
            .where(ExecutionAttempt.id == ctx.execution_attempt_id)
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=403, detail="execution attempt dispatch forbidden")
    attempt, stage, run = row
    try:
        control_binding_digest = _control_binding_digest(stage.resolved_execution_spec_json)
        container_node = (stage.resolved_execution_spec_json or {}).get("container_node")
        network_profile = (
            container_node.get("network_profile") if isinstance(container_node, dict) else None
        )
        authorized = (
            ctx.step_jwt_id is not None
            and ctx.execution_attempt_lease_epoch is not None
            and ctx.execution_spec_digest is not None
            and ctx.provider_connection_id_bound
            and run.team_id == ctx.team_id
            and run.state == "running"
            and stage.node_key == ctx.step_id
            and stage.state in {"claimed", "running"}
            and stage.execution_spec_digest == ctx.execution_spec_digest
            and stage.provider_connection_ref == ctx.provider_connection_id
            and network_profile == "gateway"
            and attempt.state in {"claimed", "running"}
            and attempt.cancellation_requested_at is None
            and attempt.lease_expires_at is not None
            and attempt.lease_expires_at > datetime.now(UTC)
            and attempt.lease_epoch == ctx.execution_attempt_lease_epoch
            and attempt.step_jwt_id == ctx.step_jwt_id
            and attempt.execution_authorization_digest
            == ctx.execution_authorization_digest
            and control_binding_digest == ctx.control_binding_snapshot_digest
        )
    except (AttributeError, TypeError, ValueError):
        authorized = False
    if not authorized:
        raise HTTPException(status_code=403, detail="execution attempt dispatch forbidden")


__all__ = ["authorize_execution_attempt_dispatch"]
