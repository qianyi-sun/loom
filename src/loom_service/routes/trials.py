"""Trials read routes + write forwarders (spec §5.1).

Read surface:
- GET /api/v1/trials             — list with cursor pagination + filters
- GET /api/v1/trials/{id}        — detail + service download URLs
- GET /api/v1/trials/{id}/artifacts/download — authenticated artifact proxy

Write forwarders (Task 8):
- POST /api/v1/trials            — proxies to Control Plane /trials
- POST /api/v1/trials/{id}/cancel — proxies to Control Plane /trials/{id}/cancel

Field extraction notes: the v0.7 `trials` table does NOT carry
`aggregate_reward` or `batch_id` columns. Reward is extracted from
`Trial.result` (the JSONB the worker writes at finalize). LLM usage
is aggregated from `llm_calls` so stale rate-card snapshots do not leak
into trial read responses. Agent name + model are pulled from current top-level
`Trial.config["agent_name"]` / `Trial.config["agent_model"]`, with
legacy `Trial.config["agent"]` fallback for older rows.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, select

from loom.db.schema import LlmCall, Team, Trial
from loom.models.types import ModelSpec
from loom_service.agent_catalog import (
    known_names,
    validate_agent_model_compat,
)
from loom_service.auth_guards import (
    is_admin,
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.forwarders import forward, propagate
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.routes.object_downloads import stream_object_response

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


def _empty_usage_projection() -> dict[str, int]:
    return {
        "total_prompt_tokens": 0,
        "total_completion_tokens": 0,
        "llm_calls_count": 0,
    }


async def _usage_by_trial_ids(
    session: Any,
    trial_ids: Sequence[UUID],
) -> dict[UUID, dict[str, int]]:
    if not trial_ids:
        return {}
    rows = (await session.execute(
        select(
            LlmCall.trial_id.label("trial_id"),
            func.coalesce(
                func.sum(LlmCall.input_tokens), 0,
            ).label("total_prompt_tokens"),
            func.coalesce(
                func.sum(LlmCall.output_tokens), 0,
            ).label("total_completion_tokens"),
            func.count(LlmCall.id).label("llm_calls_count"),
        )
        .where(LlmCall.trial_id.in_(trial_ids))
        .group_by(LlmCall.trial_id),
    )).all()
    return {
        row.trial_id: {
            "total_prompt_tokens": int(row.total_prompt_tokens or 0),
            "total_completion_tokens": int(row.total_completion_tokens or 0),
            "llm_calls_count": int(row.llm_calls_count or 0),
        }
        for row in rows
    }


def _extract_agent_projection(
    config: dict[str, Any] | None,
) -> tuple[str | None, dict[str, Any] | None]:
    if not isinstance(config, dict):
        return None, None

    if "agent_name" in config or "agent_model" in config:
        name = config.get("agent_name")
        model = config.get("agent_model")
        return (
            name if isinstance(name, str) and name else None,
            model if isinstance(model, dict) else None,
        )

    agent = config.get("agent")
    if not isinstance(agent, dict):
        return None, None
    name = agent.get("name")
    model = agent.get("model")
    return (
        name if isinstance(name, str) and name else None,
        model if isinstance(model, dict) else None,
    )


def _trial_row(
    t: Trial,
    *,
    usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    agent_name, model = _extract_agent_projection(t.config)
    usage_projection = usage or _empty_usage_projection()
    return {
        "id": str(t.id),
        "task_id": t.task_id,
        "team_id": str(t.team_id),
        "state": t.state,
        "failure_reason": t.failure_reason,
        "failure_message": t.failure_message,
        "submitted_at": t.submitted_at.isoformat(),
        "started_at": t.started_at.isoformat() if t.started_at else None,
        "finished_at": (t.finished_at.isoformat() if t.finished_at else None),
        "attempt_count": t.attempt_count,
        "aggregate_reward": _extract_reward(t.result),
        "total_prompt_tokens": usage_projection["total_prompt_tokens"],
        "total_completion_tokens": usage_projection[
            "total_completion_tokens"
        ],
        "llm_calls_count": usage_projection["llm_calls_count"],
        "agent_name": agent_name,
        "model": model,
        "visibility": t.visibility,
        "share_status": t.share_status,
        "source_provenance": t.source_provenance,
    }


@router.get("/trials")
async def list_trials(
    request: Request,
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    task_id: Annotated[str | None, Query()] = None,
    batch_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    s, ctx = sc
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
        Trial.submitted_at.desc(),
        Trial.id.desc(),
    )
    if target_team is not None:
        stmt = stmt.where(Trial.team_id == target_team)
    if task_id is not None:
        stmt = stmt.where(Trial.task_id == task_id)
    if batch_id is not None:
        stmt = stmt.where(Trial.batch_id == batch_id)
    if state:
        wanted = [x.strip() for x in state.split(",") if x.strip()]
        if wanted:
            stmt = stmt.where(Trial.state.in_(wanted))
    if cursor:
        try:
            c = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
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
    usage_by_trial = await _usage_by_trial_ids(s, [r.id for r in rows])
    return {
        "items": [
            _trial_row(r, usage=usage_by_trial.get(r.id)) for r in rows
        ],
        "next_cursor": next_c,
    }


def _artifact_bucket(item: dict[str, Any], default_bucket: str) -> str:
    bucket = item.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return default_bucket
    return bucket


def _artifact_filename(key: str) -> str:
    name = key.rstrip("/").rsplit("/", 1)[-1]
    return name or "artifact"


def _projected_artifacts(
    request: Request,
    *,
    trajectory_index: dict[str, Any] | None,
    trial_id: UUID,
) -> list[dict[str, Any]]:
    if not trajectory_index:
        return []
    artifacts = trajectory_index.get("artifacts")
    if not isinstance(artifacts, list):
        return []

    out: list[dict[str, Any]] = []
    for item in artifacts:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str) or not key:
            continue
        size = item.get("size")
        if isinstance(size, int):
            size_int = size
        elif isinstance(size, str):
            try:
                size_int = int(size)
            except ValueError:
                size_int = 0
        else:
            size_int = 0
        entry: dict[str, Any] = {
            "key": key,
            "size": max(size_int, 0),
            "download_url": str(
                request.url_for(
                    "download_artifact",
                    trial_id=str(trial_id),
                ).include_query_params(key=key),
            ),
        }
        share_status = item.get("share_status")
        if share_status in {"pending_scan", "shared", "blocked"}:
            entry["share_status"] = share_status
        else:
            entry["share_status"] = "pending_scan"
        blocked_reason = item.get("blocked_reason")
        if isinstance(blocked_reason, str):
            entry["blocked_reason"] = blocked_reason
        elif entry["share_status"] == "blocked":
            entry["blocked_reason"] = "blocked by artifact sharing policy"
        else:
            entry["blocked_reason"] = None
        step_name = item.get("step_name")
        if isinstance(step_name, str):
            entry["step_name"] = step_name
        out.append(entry)
    return out


def _find_projected_artifact(
    trajectory_index: dict[str, Any] | None,
    key: str,
) -> dict[str, Any] | None:
    if not trajectory_index:
        return None
    artifacts = trajectory_index.get("artifacts")
    if not isinstance(artifacts, list):
        return None
    for item in artifacts:
        if isinstance(item, dict) and item.get("key") == key:
            return item
    return None


@router.get("/trials/{trial_id}")
async def get_trial(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)

    usage_by_trial = await _usage_by_trial_ids(s, [trial.id])
    base = _trial_row(trial, usage=usage_by_trial.get(trial.id))
    owner_team = (await s.execute(
        select(Team).where(Team.id == trial.team_id),
    )).scalar_one_or_none()
    if owner_team is not None:
        base["owner_team"] = {
            "id": str(owner_team.id),
            "name": owner_team.name,
        }
    trajectory_index = trial.trajectory_index or {}
    # The worker's TrajectoryWriter writes events.jsonl under
    # `<trajectories_bucket>/<team_id>/<trial_id>/events.jsonl`;
    # finalize.py writes ATIF to the same bucket at `atif.json`.
    # Both user-facing URLs stay on loom_service so remote browser
    # clients do not need direct MinIO reachability.
    base["atif_url"] = str(
        request.url_for("download_atif", trial_id=str(trial.id)),
    )
    base["trajectory_url"] = str(
        request.url_for("download_trajectory", trial_id=str(trial.id)),
    )
    # `*_ready` flags so the SPA can avoid rendering a download link
    # that's going to 404. The trajectory exists as soon as the worker
    # starts the trial (first event flushed); ATIF only after finalize.
    is_terminal = trial.state in {"succeeded", "failed", "cancelled"}
    base["atif_ready"] = bool(trajectory_index.get("atif_uri")) or (
        is_terminal and trial.finished_at is not None
    )
    base["trajectory_ready"] = bool(trajectory_index.get("trajectory_uri")) or (
        trial.started_at is not None
    )
    base["artifacts"] = _projected_artifacts(
        request,
        trajectory_index=trajectory_index,
        trial_id=trial.id,
    )
    return base


@router.get("/trials/{trial_id}/artifacts/download")
async def download_artifact(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    key: Annotated[str, Query(min_length=1)],
) -> StreamingResponse:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)

    artifact = _find_projected_artifact(trial.trajectory_index, key)
    if artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")

    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=_artifact_bucket(artifact, settings.artifacts_bucket),
        key=key,
        filename=_artifact_filename(key),
        artifact_kind="artifact",
    )


class _SubmitReq(BaseModel):
    task_id: str
    config: dict[str, Any]
    # cluster-deploy.md §Schema additions: optional per-trial provider
    # override. When set, gateway uses this connection's decrypted
    # api_key + base_url; otherwise platform default. UUID is
    # validated against the caller's team before forwarding.
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


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
            detail=(f"unknown agent_name {agent_name!r}. GET /api/v1/agents for the catalog."),
        )
    model_raw = config.get("agent_model")
    model: ModelSpec | None
    if model_raw is None:
        model = None
    else:
        try:
            model = ModelSpec.model_validate(model_raw)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"agent_model failed to validate: {exc}",
            ) from exc
    err = validate_agent_model_compat(agent_name, model)
    if err is not None:
        raise HTTPException(status_code=400, detail=err)


@router.post("/trials", status_code=201)
async def submit_trial(
    request: Request,
    sc: SessionAndCtx,
    payload: _SubmitReq,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    """Authenticate locally, check `submit` scope, then proxy to
    Control Plane's POST /trials. The CP runs the canonical license-
    allowlist + team-quota checks; this route just keeps unauthorized
    requests from touching the upstream at all."""
    s, ctx = sc
    require_scope(ctx, "submit")
    _validate_agent_name(payload.config)

    # Validate the optional provider_connection_id before forwarding.
    # Doing this in loom_service (not control-plane) avoids a
    # round-trip + keeps the user-facing error close to the user-
    # supplied input.
    if payload.provider_connection_id is not None and ctx.team_id is not None:
        await validate_provider_connection(
            s,
            payload.provider_connection_id,
            team_id=ctx.team_id,
        )

    resp = await forward(
        request.app.state.http_client,
        method="POST",
        path="/trials",
        authorization=authorization,
        json_body=payload.model_dump(mode="json"),
    )
    return propagate(resp)


@router.post("/trials/{trial_id}/cancel")
async def cancel_trial(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    """Same-team check happens BEFORE the forward so we don't burn
    a CP round-trip when the caller is unauthorized."""
    s, ctx = sc
    require_scope(ctx, "submit")
    trial = (
        await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )
    ).scalar_one_or_none()
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
