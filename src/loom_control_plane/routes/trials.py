"""Trial submission + fetch endpoints."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from loom.auth import verify_bearer_token
from loom.db.schema import Task as TaskRow
from loom.db.schema import TeamQuota
from loom.db.schema import Trial as TrialRow
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom_control_plane.scheduler.requires_caps import derive_requires_caps

router = APIRouter()


@router.post("/trials", status_code=201)
async def submit_trial(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "submit" not in ctx.scopes or ctx.team_id is None:
        raise HTTPException(status_code=401, detail="not authorized to submit")

    task_id = payload.get("task_id")
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id required")

    async with request.app.state.session_factory() as session:
        task_row = (await session.execute(
            select(TaskRow).where(TaskRow.id == task_id),
        )).scalar_one_or_none()
    if task_row is None:
        raise HTTPException(status_code=404, detail=f"unknown task {task_id}")

    task_config = normalize_steps(TaskConfig.model_validate(task_row.config))
    trial_config = TrialConfig.model_validate(payload.get("config") or {})
    requires_caps = derive_requires_caps(task_config)

    trial_id = uuid4()
    async with request.app.state.session_factory() as session:
        # Defensive: a Team can exist without a TeamQuota row. If so, the
        # §2.6 DRF claim query's JOIN team_quotas would silently exclude
        # every trial we insert, and the trial would languish in queued
        # forever. Upsert a default quota row idempotently before the
        # trial INSERT so submission and scheduling stay aligned.
        await session.execute(
            pg_insert(TeamQuota)
            .values(team_id=ctx.team_id)
            .on_conflict_do_nothing(index_elements=["team_id"]),
        )
        result = await session.execute(
            insert(TrialRow).values(
                id=trial_id, team_id=ctx.team_id, task_id=task_id,
                config=trial_config.model_dump(mode="json"),
                requires_caps=requires_caps.model_dump(mode="json"),
                state="queued",
                submit_priority=trial_config.submit_priority,
            ).returning(TrialRow.submitted_at),
        )
        submitted_at = result.scalar_one()
        await session.commit()

    return {
        "trial_id": str(trial_id),
        "state": "queued",
        "submitted_at": submitted_at.isoformat(),
    }
