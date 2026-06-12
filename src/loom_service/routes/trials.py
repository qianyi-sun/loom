"""Trials read routes + write forwarders (spec §5.1).

Read surface:
- GET /api/v1/trials             — list with cursor pagination + filters
- GET /api/v1/trials/{id}        — detail + presigned ATIF + trajectory URLs

Write forwarders (Task 8):
- POST /api/v1/trials            — proxies to Control Plane /trials
- POST /api/v1/trials/{id}/cancel — proxies to Control Plane /trials/{id}/cancel

Field extraction notes: the v0.7 `trials` table does NOT carry
`aggregate_reward`, `cost_usd`, or `batch_id` columns. Reward + cost
are extracted from `Trial.result` (the JSONB the worker writes at
finalize). Agent name + model are pulled from `Trial.config["agent"]`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import and_, or_, select

from loom.auth import verify_bearer_token
from loom.db.schema import Trial
from loom_service.agent_catalog import known_names
from loom_service.auth_guards import (
    is_admin,
    require_human_or_admin,
    require_scope,
    require_team_or_admin,
)
from loom_service.forwarders import forward, propagate
from loom_service.pagination import Cursor, decode_cursor, encode_cursor

router = APIRouter()


def _extract_reward(result: dict[str, Any] | None) -> float | None:
    """Pull aggregate reward out of the worker-written result JSONB.
    The worker stores the multi-step combined reward as
    `result["aggregate_reward"]` (Plan 3 contract); fall through to
    `result["reward"]` for single-step trials that predate that key.

    Defensive cast — `result` is JSONB whose shape is enforced by the
    worker; a malformed value should not crash the read API."""
    if not result:
        return None
    val = result.get("aggregate_reward")
    if val is None:
        val = result.get("reward")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _extract_cost(result: dict[str, Any] | None) -> float:
    """Total cost in USD across all LLM calls; 0.0 if absent or
    malformed."""
    if not result:
        return 0.0
    val = result.get("cost_usd", 0)
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def _trial_row(t: Trial) -> dict[str, Any]:
    agent = (t.config or {}).get("agent") or {}
    return {
        "id": str(t.id),
        "task_id": t.task_id,
        "team_id": str(t.team_id),
        "state": t.state,
        "failure_reason": t.failure_reason,
        "submitted_at": t.submitted_at.isoformat(),
        "started_at": t.started_at.isoformat() if t.started_at else None,
        "finished_at": (
            t.finished_at.isoformat() if t.finished_at else None
        ),
        "attempt_count": t.attempt_count,
        "aggregate_reward": _extract_reward(t.result),
        "cost_usd": _extract_cost(t.result),
        "agent_name": agent.get("name"),
        "model": agent.get("model"),
    }


@router.get("/trials")
async def list_trials(
    request: Request,
    team_id: Annotated[UUID | None, Query()] = None,
    task_id: Annotated[str | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")

        # Resolve the team filter:
        # - explicit `team_id` query → require_team_or_admin
        # - no filter + non-admin → scope to caller's own team
        # - no filter + admin → no team filter
        target_team = team_id
        if target_team is not None:
            require_team_or_admin(ctx, target_team)
        elif not is_admin(ctx):
            target_team = ctx.team_id

        stmt = select(Trial).order_by(
            Trial.submitted_at.desc(), Trial.id.desc(),
        )
        if target_team is not None:
            stmt = stmt.where(Trial.team_id == target_team)
        if task_id is not None:
            stmt = stmt.where(Trial.task_id == task_id)
        if state:
            wanted = [x.strip() for x in state.split(",") if x.strip()]
            if wanted:
                stmt = stmt.where(Trial.state.in_(wanted))
        if cursor:
            try:
                c = decode_cursor(cursor)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400, detail=str(exc),
                ) from exc
            # Composite (submitted_at, id) key: rows strictly LESS than
            # the cursor (DESC ordering).
            stmt = stmt.where(
                or_(
                    Trial.submitted_at < c.submitted_at,
                    and_(
                        Trial.submitted_at == c.submitted_at,
                        Trial.id < c.id,
                    ),
                ),
            )
        stmt = stmt.limit(limit + 1)
        rows = list((await s.execute(stmt)).scalars().all())

    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_c = encode_cursor(
            Cursor(submitted_at=last.submitted_at, id=last.id),
        )
    else:
        next_c = None
    return {
        "items": [_trial_row(r) for r in rows],
        "next_cursor": next_c,
    }


def _presign_get(
    client: Any, bucket: str, key: str, expires_sec: int,
) -> str:
    url: str = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_sec,
    )
    return url


@router.get("/trials/{trial_id}")
async def get_trial(
    request: Request,
    trial_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")
        trial = (await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )).scalar_one_or_none()
        if trial is None:
            raise HTTPException(status_code=404, detail="trial not found")
        require_team_or_admin(ctx, trial.team_id)

    base = _trial_row(trial)
    # The worker's TrajectoryWriter writes events.jsonl under
    # `<trajectories_bucket>/<team_id>/<trial_id>/events.jsonl`;
    # finalize.py writes ATIF to the same bucket at `atif.json`.
    # Both URLs are presigned-GET on the trajectories bucket.
    base["atif_url"] = _presign_get(
        request.app.state.minio_client,
        settings.trajectories_bucket,
        f"{trial.team_id}/{trial.id}/atif.json",
        settings.signed_url_expiry_sec,
    )
    base["trajectory_url"] = _presign_get(
        request.app.state.minio_client,
        settings.trajectories_bucket,
        f"{trial.team_id}/{trial.id}/events.jsonl",
        settings.signed_url_expiry_sec,
    )
    # `*_ready` flags so the SPA can avoid rendering a download link
    # that's going to 404. The trajectory exists as soon as the worker
    # starts the trial (first event flushed); ATIF only after finalize.
    is_terminal = trial.state in {"succeeded", "failed", "cancelled"}
    base["atif_ready"] = is_terminal and trial.finished_at is not None
    base["trajectory_ready"] = trial.started_at is not None
    # Per-artifact listing comes from Plan 7's `tasks/{id}/bundle` or
    # the in-flight artifacts table; v0.7 doesn't yet have an
    # `artifacts` table, so the array stays empty here. Plan 21+ can
    # add it without changing the response shape.
    base["artifacts"] = []
    return base


class _SubmitReq(BaseModel):
    task_id: str
    config: dict[str, Any]


def _validate_agent_name(config: dict[str, Any]) -> None:
    """Reject a trial_config whose `agent_name` isn't in the catalog.
    Server-side defense in depth — the SPA already restricts the
    dropdown to known names, but a direct API caller (curl, sdk) can
    still send anything. Validating here keeps a typo'd or hostile
    name from reaching the worker."""
    agent_name = config.get("agent_name")
    if not isinstance(agent_name, str) or not agent_name:
        # The Control Plane's TrialConfig.model_validate will 422 this
        # — propagate cleanly rather than 400 here so the error shape
        # stays consistent.
        return
    if agent_name not in known_names():
        raise HTTPException(
            status_code=400,
            detail=(
                f"unknown agent_name {agent_name!r}. "
                "GET /api/v1/agents for the catalog."
            ),
        )


@router.post("/trials", status_code=201)
async def submit_trial(
    request: Request,
    payload: _SubmitReq,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    """Authenticate locally, check `submit` scope, then proxy to
    Control Plane's POST /trials. The CP runs the canonical license-
    allowlist + team-quota checks; this route just keeps unauthorized
    requests from touching the upstream at all."""
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "submit")
    _validate_agent_name(payload.config)
    resp = await forward(
        request.app.state.http_client,
        method="POST", path="/trials",
        authorization=authorization,
        json_body=payload.model_dump(mode="json"),
    )
    return propagate(resp)


@router.post("/trials/{trial_id}/cancel")
async def cancel_trial(
    request: Request,
    trial_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    """Same-team check happens BEFORE the forward so we don't burn
    a CP round-trip when the caller is unauthorized."""
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "submit")
        trial = (await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )).scalar_one_or_none()
        if trial is None:
            raise HTTPException(status_code=404, detail="trial not found")
        require_team_or_admin(ctx, trial.team_id)
    resp = await forward(
        request.app.state.http_client,
        method="POST",
        path=f"/trials/{trial_id}/cancel",
        authorization=authorization,
    )
    return propagate(resp)
