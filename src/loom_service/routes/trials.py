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

from loom.db.schema import LlmCall, Task, Team, Trial, User
from loom.models.types import ModelSpec
from loom_llm_gateway.rate_card import (
    COST_META_CONFIDENCE_KEY,
    COST_META_SOURCE_KEY,
)
from loom_service.agent_catalog import (
    known_names,
    validate_agent_model_compat,
)
from loom_service.auth_guards import (
    require_scope,
    require_submitting_user,
    require_team_or_admin,
)
from loom_service.debug_evidence import build_trial_debug_evidence
from loom_service.dependencies import SessionAndCtx
from loom_service.diagnosis import build_trial_diagnosis
from loom_service.forwarders import forward, propagate
from loom_service.monitor_filters import (
    apply_trial_monitor_filters,
    resolve_monitor_team_filter,
)
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.routes.object_downloads import stream_object_response
from loom_service.stale_running_debug import trial_stale_running_debug_context
from loom_service.usage_accounting import (
    empty_usage_projection,
    price_snapshots_for_trials,
    project_trial_llm_evidence,
    summarize_usage_counts,
    usage_status_filter,
)

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


def _empty_usage_projection() -> dict[str, Any]:
    return empty_usage_projection()


def _priced_call_filter() -> Any:
    return (
        ~LlmCall.rate_card_hash.like("facade:tokens-only%")
        & ~_price_unknown_call_filter()
        & (LlmCall.rate_card_hash != "failed-upstream")
    )


def _price_unknown_call_filter() -> Any:
    return (
        LlmCall.rate_card_hash.like("facade:rate-card:missing%")
        | _cost_meta_filter(COST_META_SOURCE_KEY, "unpriced")
    )


def _cost_meta_filter(key: str, value: str) -> Any:
    return func.coalesce(LlmCall.provider_extras.op("->>")(key), "") == value


def _cost_source_counts(row: Any) -> dict[str, int]:
    return {
        "operator-supplied": int(row.cost_source_operator_supplied_count or 0),
        "rate-card": int(row.cost_source_rate_card_count or 0),
        "tokens-only": int(row.cost_source_tokens_only_count or 0),
        "unpriced": int(row.cost_source_unpriced_count or 0),
    }


def _cost_confidence_counts(row: Any) -> dict[str, int]:
    return {
        "configured": int(row.cost_confidence_configured_count or 0),
        "not_applicable": int(row.cost_confidence_not_applicable_count or 0),
        "unavailable": int(row.cost_confidence_unavailable_count or 0),
    }


async def _usage_by_trial_ids(
    session: Any,
    trial_ids: Sequence[UUID],
) -> dict[UUID, dict[str, Any]]:
    if not trial_ids:
        return {}
    rows = (
        await session.execute(
            select(
                LlmCall.trial_id.label("trial_id"),
                func.coalesce(
                    func.sum(LlmCall.input_tokens),
                    0,
                ).label("total_prompt_tokens"),
                func.coalesce(
                    func.sum(LlmCall.output_tokens),
                    0,
                ).label("total_completion_tokens"),
                func.count(LlmCall.id).label("llm_calls_count"),
                func.coalesce(
                    func.sum(LlmCall.cost_usd),
                    0,
                ).label("total_cost_usd"),
                func.count(LlmCall.id)
                .filter(_priced_call_filter())
                .label("priced_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash.like("facade:tokens-only%"))
                .label("token_only_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_price_unknown_call_filter())
                .label("price_unknown_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash == "failed-upstream")
                .label("failed_upstream_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "operator-supplied"))
                .label("cost_source_operator_supplied_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "rate-card"))
                .label("cost_source_rate_card_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "tokens-only"))
                .label("cost_source_tokens_only_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "unpriced"))
                .label("cost_source_unpriced_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "configured"))
                .label("cost_confidence_configured_count"),
                func.count(LlmCall.id)
                .filter(
                    _cost_meta_filter(COST_META_CONFIDENCE_KEY, "not_applicable"),
                )
                .label("cost_confidence_not_applicable_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "unavailable"))
                .label("cost_confidence_unavailable_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("partial"))
                .label("partial_usage_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("missing"))
                .label("missing_usage_llm_calls_count"),
            )
            .where(LlmCall.trial_id.in_(trial_ids))
            .group_by(LlmCall.trial_id),
        )
    ).all()
    return {
        row.trial_id: summarize_usage_counts(
            llm_calls_count=int(row.llm_calls_count or 0),
            total_prompt_tokens=int(row.total_prompt_tokens or 0),
            total_completion_tokens=int(row.total_completion_tokens or 0),
            total_cost_usd=row.total_cost_usd,
            priced_llm_calls_count=int(row.priced_llm_calls_count or 0),
            token_only_llm_calls_count=int(
                row.token_only_llm_calls_count or 0,
            ),
            price_unknown_llm_calls_count=int(
                row.price_unknown_llm_calls_count or 0,
            ),
            failed_upstream_llm_calls_count=int(
                row.failed_upstream_llm_calls_count or 0,
            ),
            partial_usage_llm_calls_count=int(
                row.partial_usage_llm_calls_count or 0,
            ),
            missing_usage_llm_calls_count=int(
                row.missing_usage_llm_calls_count or 0,
            ),
            cost_source_counts=_cost_source_counts(row),
            cost_confidence_counts=_cost_confidence_counts(row),
        )
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
    usage: dict[str, Any] | None = None,
    owner_team: Team | None = None,
    submitted_by_user: User | None = None,
) -> dict[str, Any]:
    agent_name, model = _extract_agent_projection(t.config)
    usage_projection = usage or _empty_usage_projection()
    llm_evidence = project_trial_llm_evidence(
        t,
        llm_calls_count=int(usage_projection["llm_calls_count"]),
    )
    out: dict[str, Any] = {
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
        "total_completion_tokens": usage_projection["total_completion_tokens"],
        "total_tokens": usage_projection["total_tokens"],
        "llm_calls_count": usage_projection["llm_calls_count"],
        "estimated_cost_usd": usage_projection["estimated_cost_usd"],
        "cost_currency": usage_projection["cost_currency"],
        "cost_status": usage_projection["cost_status"],
        "cost_estimate_source": usage_projection["cost_estimate_source"],
        "cost_estimate_confidence": usage_projection["cost_estimate_confidence"],
        "pricing_modes": usage_projection["pricing_modes"],
        "priced_llm_calls_count": usage_projection["priced_llm_calls_count"],
        "token_only_llm_calls_count": usage_projection["token_only_llm_calls_count"],
        "price_unknown_llm_calls_count": usage_projection["price_unknown_llm_calls_count"],
        "failed_upstream_llm_calls_count": usage_projection["failed_upstream_llm_calls_count"],
        "partial_usage_llm_calls_count": usage_projection["partial_usage_llm_calls_count"],
        "missing_usage_llm_calls_count": usage_projection["missing_usage_llm_calls_count"],
        "usage_reporting_status": usage_projection["usage_reporting_status"],
        "usage_estimate_confidence": usage_projection["usage_estimate_confidence"],
        "llm_evidence_status": llm_evidence["llm_evidence_status"],
        "no_call": llm_evidence["no_call"],
        "agent_name": agent_name,
        "model": model,
        "visibility": t.visibility,
        "share_status": t.share_status,
        "source_provenance": t.source_provenance,
        "submitted_by_user": (
            {
                "id": str(submitted_by_user.id),
                "username": submitted_by_user.username,
                "team_id": str(t.team_id),
                "team_name": owner_team.name if owner_team else None,
            }
            if submitted_by_user is not None
            else None
        ),
    }
    if owner_team is not None:
        out["team_name"] = owner_team.name
        out["owner_team"] = {
            "id": str(owner_team.id),
            "name": owner_team.name,
        }
    return out


@router.get("/trials")
async def list_trials(
    request: Request,
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    task_id: Annotated[str | None, Query()] = None,
    batch_id: Annotated[UUID | None, Query()] = None,
    benchmark_id: Annotated[str | None, Query()] = None,
    agent_name: Annotated[str | None, Query()] = None,
    agent: Annotated[str | None, Query()] = None,
    model_provider: Annotated[str | None, Query()] = None,
    model_name: Annotated[str | None, Query()] = None,
    model: Annotated[str | None, Query()] = None,
    provider_connection_id: Annotated[UUID | None, Query()] = None,
    provider_model_id: Annotated[str | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")

    target_team = resolve_monitor_team_filter(ctx, team_id)

    stmt = select(Trial).order_by(
        Trial.submitted_at.desc(),
        Trial.id.desc(),
    )
    stmt = apply_trial_monitor_filters(
        stmt,
        target_team=target_team,
        task_id=task_id,
        batch_id=batch_id,
        benchmark_id=benchmark_id,
        agent_name=agent_name,
        agent=agent,
        model_provider=model_provider,
        model_name=model_name,
        model=model,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=state,
    )
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
    teams_by_id: dict[UUID, Team] = {}
    users_by_id: dict[UUID, User] = {}
    if rows:
        team_rows = (
            (
                await s.execute(
                    select(Team).where(Team.id.in_({r.team_id for r in rows})),
                )
            )
            .scalars()
            .all()
        )
        teams_by_id = {team.id: team for team in team_rows}
        user_ids = {r.submitted_by_user_id for r in rows if r.submitted_by_user_id is not None}
        if user_ids:
            user_rows = (
                (
                    await s.execute(
                        select(User).where(User.id.in_(user_ids)),
                    )
                )
                .scalars()
                .all()
            )
            users_by_id = {user.id: user for user in user_rows}
    return {
        "items": [
            _trial_row(
                r,
                usage=usage_by_trial.get(r.id),
                owner_team=teams_by_id.get(r.team_id),
                submitted_by_user=(
                    users_by_id.get(r.submitted_by_user_id)
                    if r.submitted_by_user_id is not None
                    else None
                ),
            )
            for r in rows
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
    owner_team = (
        await s.execute(
            select(Team).where(Team.id == trial.team_id),
        )
    ).scalar_one_or_none()
    submitted_by_user = None
    if trial.submitted_by_user_id is not None:
        submitted_by_user = (
            await s.execute(
                select(User).where(User.id == trial.submitted_by_user_id),
            )
        ).scalar_one_or_none()
    task = (
        await s.execute(
            select(Task).where(Task.id == trial.task_id),
        )
    ).scalar_one_or_none()
    llm_calls = list(
        (
            await s.execute(
                select(LlmCall)
                .where(LlmCall.trial_id == trial.id)
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    debug_context = await trial_stale_running_debug_context(
        s,
        trial,
        task=task,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    base = _trial_row(
        trial,
        usage=usage_by_trial.get(trial.id),
        owner_team=owner_team,
        submitted_by_user=submitted_by_user,
    )
    base["price_snapshots"] = await price_snapshots_for_trials(s, [trial.id])
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
    debug_evidence = build_trial_debug_evidence(
        request,
        trial,
        task=task,
        llm_calls=llm_calls,
        **debug_context,
    )
    base["debug_evidence"] = debug_evidence
    base["diagnosis"] = build_trial_diagnosis(debug_evidence)
    return base


@router.get("/trials/{trial_id}/debug")
async def get_trial_debug(
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
    task = (
        await s.execute(
            select(Task).where(Task.id == trial.task_id),
        )
    ).scalar_one_or_none()
    llm_calls = list(
        (
            await s.execute(
                select(LlmCall)
                .where(LlmCall.trial_id == trial.id)
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    debug_context = await trial_stale_running_debug_context(
        s,
        trial,
        task=task,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    return build_trial_debug_evidence(
        request,
        trial,
        task=task,
        llm_calls=llm_calls,
        **debug_context,
    )


@router.get("/trials/{trial_id}/diagnosis")
async def get_trial_diagnosis(
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
    task = (
        await s.execute(
            select(Task).where(Task.id == trial.task_id),
        )
    ).scalar_one_or_none()
    llm_calls = list(
        (
            await s.execute(
                select(LlmCall)
                .where(LlmCall.trial_id == trial.id)
                .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    debug_context = await trial_stale_running_debug_context(
        s,
        trial,
        task=task,
        llm_calls=llm_calls,
        settings=request.app.state.settings,
    )
    debug_evidence = build_trial_debug_evidence(
        request,
        trial,
        task=task,
        llm_calls=llm_calls,
        **debug_context,
    )
    return build_trial_diagnosis(debug_evidence)


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
    require_submitting_user(ctx)
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
