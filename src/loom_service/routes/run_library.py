"""Org-wide Run Library for completed shared work (#336)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, or_, select

from loom.db.schema import Batch, LlmCall, Team, Trial
from loom.security.redaction import redact_text
from loom_service.auth_guards import (
    is_admin,
    require_scope,
    require_team_or_admin,
)
from loom_service.debug_evidence import build_batch_debug_evidence
from loom_service.dependencies import SessionAndCtx
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.routes.object_downloads import stream_object_response

router = APIRouter()

_ORG_VISIBLE_BATCH_STATES = frozenset({"finished", "cancelled"})
_ORG_VISIBLE_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
_ARTIFACT_GROUPS = (
    "reports",
    "trajectories",
    "reusable_outputs",
    "logs_diagnostics",
    "raw_diagnostics",
)


class _CloneConfigRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


class _ReuseArtifactRequest(BaseModel):
    key: str = Field(min_length=1)
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


class _VisibilityPatch(BaseModel):
    visibility: str
    share_status: str


def _is_owner_or_admin(ctx: Any, team_id: UUID) -> bool:
    return is_admin(ctx) or ctx.team_id == team_id


def _batch_is_org_visible(batch: Batch) -> bool:
    return (
        batch.visibility == "org"
        and batch.share_status == "shared"
        and batch.state in _ORG_VISIBLE_BATCH_STATES
    )


def _trial_is_org_visible(
    trial: Trial,
    batch: Batch | None = None,
) -> bool:
    if batch is not None:
        return _batch_is_org_visible(batch) and trial.state in _ORG_VISIBLE_TRIAL_STATES

    trial_shared = (
        trial.visibility == "org"
        and trial.share_status == "shared"
        and trial.state in _ORG_VISIBLE_TRIAL_STATES
    )
    return trial_shared


def _can_read_batch(ctx: Any, batch: Batch) -> bool:
    return _is_owner_or_admin(ctx, batch.team_id) or _batch_is_org_visible(batch)


def _can_read_trial(
    ctx: Any,
    trial: Trial,
    batch: Batch | None = None,
) -> bool:
    return _is_owner_or_admin(ctx, trial.team_id) or _trial_is_org_visible(
        trial, batch,
    )


def _artifact_role(item: dict[str, Any]) -> str:
    raw = item.get("role") or item.get("artifact_role")
    if isinstance(raw, str):
        normalized = raw.strip().lower().replace("-", "_")
        aliases = {
            "report": "reports",
            "reports": "reports",
            "atif": "reports",
            "trajectory": "trajectories",
            "trajectories": "trajectories",
            "output": "reusable_outputs",
            "outputs": "reusable_outputs",
            "reusable_output": "reusable_outputs",
            "reusable_outputs": "reusable_outputs",
            "log": "logs_diagnostics",
            "logs": "logs_diagnostics",
            "diagnostic": "logs_diagnostics",
            "diagnostics": "logs_diagnostics",
            "logs_diagnostics": "logs_diagnostics",
            "raw": "raw_diagnostics",
            "raw_diagnostic": "raw_diagnostics",
            "raw_diagnostics": "raw_diagnostics",
            "internal_diagnostics": "raw_diagnostics",
        }
        role = aliases.get(normalized)
        if role is not None:
            return role

    key = item.get("key")
    key_text = key.lower() if isinstance(key, str) else ""
    if key_text.endswith("atif.json") or "report" in key_text:
        return "reports"
    if key_text.endswith("events.jsonl") or "trajectory" in key_text:
        return "trajectories"
    if "debug" in key_text or "raw" in key_text or "internal" in key_text:
        return "raw_diagnostics"
    if "log" in key_text or "diagnostic" in key_text:
        return "logs_diagnostics"
    return "reusable_outputs"


def _artifact_bucket(item: dict[str, Any], default_bucket: str) -> str:
    bucket = item.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return default_bucket
    return bucket


def _artifact_filename(key: str) -> str:
    name = key.rstrip("/").rsplit("/", 1)[-1]
    return name or "artifact"


def _artifact_items(
    trajectory_index: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not trajectory_index:
        return []
    artifacts = trajectory_index.get("artifacts")
    if not isinstance(artifacts, list):
        return []
    return [item for item in artifacts if isinstance(item, dict)]


def _find_artifact(
    trajectory_index: dict[str, Any] | None,
    key: str,
) -> dict[str, Any] | None:
    for item in _artifact_items(trajectory_index):
        if item.get("key") == key:
            return item
    return None


def _share_status(item: dict[str, Any]) -> str:
    status = item.get("share_status")
    if status in {"pending_scan", "shared", "blocked"}:
        return str(status)
    return "pending_scan"


def _blocked_reason(item: dict[str, Any]) -> str:
    reason = item.get("blocked_reason")
    if isinstance(reason, str) and reason.strip():
        return redact_text(reason)
    return "blocked by artifact sharing policy"


def _artifact_summary(trials: Sequence[Trial]) -> dict[str, int]:
    summary = {role: 0 for role in _ARTIFACT_GROUPS}
    for trial in trials:
        for item in _artifact_items(trial.trajectory_index):
            summary[_artifact_role(item)] += 1
    return summary


def _artifact_inventory(
    request: Request,
    trials: Sequence[Trial],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {
        role: [] for role in _ARTIFACT_GROUPS
    }
    for trial in trials:
        for item in _artifact_items(trial.trajectory_index):
            key = item.get("key")
            if not isinstance(key, str) or not key:
                continue
            size = item.get("size")
            try:
                size_int = int(size) if size is not None else 0
            except (TypeError, ValueError):
                size_int = 0
            role = _artifact_role(item)
            status = _share_status(item)
            grouped[role].append({
                "trial_id": str(trial.id),
                "key": key,
                "size": max(size_int, 0),
                "role": role,
                "share_status": status,
                "blocked_reason": (
                    _blocked_reason(item) if status == "blocked" else None
                ),
                "download_url": str(
                    request.url_for(
                        "download_run_library_artifact",
                        trial_id=str(trial.id),
                    ).include_query_params(key=key),
                ),
            })
    return grouped


def _trial_summary(trials: Sequence[Trial]) -> dict[str, int]:
    summary = {
        k: 0 for k in (
            "queued",
            "claimed",
            "running",
            "succeeded",
            "failed",
            "cancelled",
        )
    }
    for trial in trials:
        state = str(trial.state)
        summary[state] = summary.get(state, 0) + 1
    return summary


def _rollup_result(result: dict[str, Any] | None) -> tuple[float | None, float]:
    if not result:
        return None, 0.0
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        reward_f = float(reward) if reward is not None else None
    except (TypeError, ValueError):
        reward_f = None
    try:
        cost_f = float(result.get("cost_usd", 0) or 0)
    except (TypeError, ValueError):
        cost_f = 0.0
    return reward_f, cost_f


def _trial_rollup(trials: Sequence[Trial]) -> tuple[float | None, float]:
    reward_sum = 0.0
    reward_count = 0
    cost_total = 0.0
    for trial in trials:
        if trial.state not in _ORG_VISIBLE_TRIAL_STATES:
            continue
        reward, cost = _rollup_result(trial.result)
        cost_total += cost
        if reward is not None:
            reward_sum += reward
            reward_count += 1
    return (
        reward_sum / reward_count if reward_count else None,
        cost_total,
    )


async def _batch_trials(session: Any, batch_id: UUID) -> list[Trial]:
    return list((await session.execute(
        select(Trial).where(Trial.batch_id == batch_id),
    )).scalars().all())


async def _llm_calls_for_trials(
    session: Any,
    trials: Sequence[Trial],
) -> list[LlmCall]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return []
    return list((await session.execute(
        select(LlmCall)
        .where(LlmCall.trial_id.in_(trial_ids))
        .order_by(LlmCall.captured_at.asc(), LlmCall.id.asc()),
    )).scalars().all())


async def _serialize_batch(
    request: Request,
    session: Any,
    batch: Batch,
    owner_team: Team,
    *,
    include_inventory: bool = False,
) -> dict[str, Any]:
    trials = await _batch_trials(session, batch.id)
    llm_calls = await _llm_calls_for_trials(session, trials)
    reward, cost = _trial_rollup(trials)
    out: dict[str, Any] = {
        "id": str(batch.id),
        "team_id": str(batch.team_id),
        "owner_team": {
            "id": str(owner_team.id),
            "name": owner_team.name,
        },
        "name": batch.name,
        "description": batch.description,
        "task_filter": batch.task_filter,
        "trial_config": batch.trial_config,
        "backend": batch.backend,
        "combinations": batch.combinations,
        "provider_connection_id": (
            str(batch.provider_connection_id)
            if batch.provider_connection_id else None
        ),
        "provider_model_id": batch.provider_model_id,
        "state": batch.state,
        "result_status": batch.result_status,
        "visibility": batch.visibility,
        "share_status": batch.share_status,
        "source_provenance": batch.source_provenance,
        "expected_trial_count": batch.expected_trial_count,
        "created_by_token_prefix": batch.created_by_token_prefix,
        "created_at": batch.created_at.isoformat(),
        "finished_at": batch.finished_at.isoformat() if batch.finished_at else None,
        "trial_summary": _trial_summary(trials),
        "aggregate_reward": reward,
        "total_cost_usd": cost,
        "artifact_summary": _artifact_summary(trials),
        "debug_evidence": build_batch_debug_evidence(
            batch,
            trials=trials,
            llm_calls=llm_calls,
        ),
    }
    if include_inventory:
        out["artifact_inventory"] = _artifact_inventory(request, trials)
    return out


async def _load_batch_with_team(
    session: Any,
    batch_id: UUID,
) -> tuple[Batch, Team]:
    row = (await session.execute(
        select(Batch, Team)
        .join(Team, Team.id == Batch.team_id)
        .where(Batch.id == batch_id),
    )).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="batch not found")
    batch, team = row
    return batch, team


async def _load_trial_with_batch(
    session: Any,
    trial_id: UUID,
) -> tuple[Trial, Batch | None]:
    trial = (await session.execute(
        select(Trial).where(Trial.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    batch: Batch | None = None
    if trial.batch_id is not None:
        batch = (await session.execute(
            select(Batch).where(Batch.id == trial.batch_id),
        )).scalar_one_or_none()
    return trial, batch


def _apply_read_filter(
    stmt: Any,
    *,
    ctx: Any,
    scope: str,
    team_id: UUID | None,
) -> Any:
    if team_id is not None:
        if _is_owner_or_admin(ctx, team_id):
            return stmt.where(Batch.team_id == team_id)
        return stmt.where(
            and_(
                Batch.team_id == team_id,
                Batch.visibility == "org",
                Batch.share_status == "shared",
                Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
            ),
        )

    if scope == "all":
        if is_admin(ctx):
            return stmt
        return stmt.where(
            or_(
                Batch.team_id == ctx.team_id,
                and_(
                    Batch.visibility == "org",
                    Batch.share_status == "shared",
                    Batch.state.in_(sorted(_ORG_VISIBLE_BATCH_STATES)),
                ),
            ),
        )

    if ctx.team_id is None:
        return stmt
    return stmt.where(Batch.team_id == ctx.team_id)


@router.get("/run-library/batches")
async def list_run_library_batches(
    request: Request,
    sc: SessionAndCtx,
    scope: Annotated[str, Query(pattern="^(my|all)$")] = "my",
    team_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    visibility: Annotated[str | None, Query(pattern="^(team|org|private)$")] = None,
    artifact_type: Annotated[str | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")

    stmt = (
        select(Batch, Team)
        .join(Team, Team.id == Batch.team_id)
        .order_by(Batch.created_at.desc(), Batch.id.desc())
    )
    stmt = _apply_read_filter(stmt, ctx=ctx, scope=scope, team_id=team_id)
    if state:
        wanted = [item.strip() for item in state.split(",") if item.strip()]
        if wanted:
            stmt = stmt.where(Batch.state.in_(wanted))
    if visibility:
        stmt = stmt.where(Batch.visibility == visibility)
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stmt = stmt.where(
            or_(
                Batch.created_at < cur.submitted_at,
                and_(Batch.created_at == cur.submitted_at, Batch.id < cur.id),
            ),
        )
    stmt = stmt.limit(limit + 1)
    rows = list((await session.execute(stmt)).all())

    serialized: list[dict[str, Any]] = []
    for batch, team in rows:
        item = await _serialize_batch(request, session, batch, team)
        if artifact_type:
            if item["artifact_summary"].get(artifact_type, 0) <= 0:
                continue
        serialized.append(item)

    next_cursor: str | None = None
    if len(serialized) > limit:
        serialized = serialized[:limit]
        last_id = UUID(serialized[-1]["id"])
        last_created = next(
            batch.created_at for batch, _team in rows if batch.id == last_id
        )
        next_cursor = encode_cursor(
            Cursor(submitted_at=last_created, id=last_id),
        )
    return {"items": serialized, "next_cursor": next_cursor}


@router.get("/run-library/batches/{batch_id}")
async def get_run_library_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    batch, team = await _load_batch_with_team(session, batch_id)
    if not _can_read_batch(ctx, batch):
        raise HTTPException(status_code=403, detail="batch is not shared")
    return await _serialize_batch(
        request, session, batch, team, include_inventory=True,
    )


@router.patch("/run-library/batches/{batch_id}/visibility")
async def update_run_library_batch_visibility(
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _VisibilityPatch,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    if payload.visibility not in {"team", "org", "private"}:
        raise HTTPException(status_code=400, detail="invalid visibility")
    if payload.share_status not in {"pending_scan", "shared", "blocked"}:
        raise HTTPException(status_code=400, detail="invalid share_status")
    batch, _team = await _load_batch_with_team(session, batch_id)
    require_team_or_admin(ctx, batch.team_id)
    batch.visibility = payload.visibility
    batch.share_status = payload.share_status
    await session.commit()
    await session.refresh(batch)
    return {
        "batch_id": str(batch.id),
        "visibility": batch.visibility,
        "share_status": batch.share_status,
    }


@router.post("/run-library/batches/{batch_id}/clone-config", status_code=201)
async def clone_run_library_batch_config(
    sc: SessionAndCtx,
    batch_id: UUID,
    payload: _CloneConfigRequest,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    if ctx.team_id is None:
        raise HTTPException(status_code=400, detail="team context required")
    source, _team = await _load_batch_with_team(session, batch_id)
    if not _can_read_batch(ctx, source):
        raise HTTPException(status_code=403, detail="batch is not shared")
    if source.provider_connection_id is not None and payload.provider_connection_id is None:
        raise HTTPException(
            status_code=400,
            detail="choose a provider_connection_id owned by your team",
        )
    if payload.provider_connection_id is not None:
        await validate_provider_connection(
            session, payload.provider_connection_id, team_id=ctx.team_id,
        )

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    provenance = [{
        "kind": "cloned_batch_config",
        "source_batch_id": str(source.id),
        "source_team_id": str(source.team_id),
        "source_visibility": source.visibility,
    }]
    clone = Batch(
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description or (
            f"Cloned config from shared batch {source.id}."
        ),
        task_filter=dict(source.task_filter),
        trial_config=dict(source.trial_config),
        state="submitted",
        created_by_token_prefix=token_prefix,
        expected_trial_count=source.expected_trial_count,
        n_per_task=source.n_per_task,
        backend=source.backend,
        combinations=list(source.combinations or []),
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=payload.provider_model_id or source.provider_model_id,
        source_provenance=provenance,
    )
    session.add(clone)
    await session.commit()
    await session.refresh(clone)
    return {
        "batch_id": str(clone.id),
        "cloned_from_batch_id": str(source.id),
        "provider_connection_id": (
            str(clone.provider_connection_id)
            if clone.provider_connection_id else None
        ),
        "provider_model_id": clone.provider_model_id,
        "source_provenance": clone.source_provenance,
        "state": clone.state,
        "created_at": clone.created_at.isoformat(),
    }


@router.get("/run-library/trials/{trial_id}/artifacts/download")
async def download_run_library_artifact(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    key: Annotated[str, Query(min_length=1)],
) -> StreamingResponse:
    settings = request.app.state.settings
    session, ctx = sc
    require_scope(ctx, "read:own")
    trial, batch = await _load_trial_with_batch(session, trial_id)
    if not _can_read_trial(ctx, trial, batch):
        raise HTTPException(status_code=403, detail="trial is not shared")
    artifact = _find_artifact(trial.trajectory_index, key)
    if artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if _share_status(artifact) != "shared":
        raise HTTPException(status_code=403, detail=_blocked_reason(artifact))
    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=_artifact_bucket(artifact, settings.artifacts_bucket),
        key=key,
        filename=_artifact_filename(key),
        artifact_kind="artifact",
    )


@router.post("/run-library/trials/{trial_id}/artifacts/reuse", status_code=201)
async def reuse_run_library_artifact(
    sc: SessionAndCtx,
    trial_id: UUID,
    payload: _ReuseArtifactRequest,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "submit")
    if ctx.team_id is None:
        raise HTTPException(status_code=400, detail="team context required")
    trial, batch = await _load_trial_with_batch(session, trial_id)
    if not _can_read_trial(ctx, trial, batch):
        raise HTTPException(status_code=403, detail="trial is not shared")
    artifact = _find_artifact(trial.trajectory_index, payload.key)
    if artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if _share_status(artifact) != "shared":
        raise HTTPException(status_code=403, detail=_blocked_reason(artifact))
    if payload.provider_connection_id is not None:
        await validate_provider_connection(
            session, payload.provider_connection_id, team_id=ctx.team_id,
        )

    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    role = _artifact_role(artifact)
    source_batch_id = str(batch.id) if batch is not None else None
    provenance = [{
        "kind": "reused_artifact",
        "source_batch_id": source_batch_id,
        "source_trial_id": str(trial.id),
        "source_team_id": str(trial.team_id),
        "source_artifact_key": payload.key,
        "source_artifact_role": role,
    }]
    task_filter = (
        dict(batch.task_filter)
        if batch is not None
        else {"subset_kind": "explicit", "task_ids": [trial.task_id]}
    )
    trial_config = dict(batch.trial_config) if batch is not None else dict(trial.config)
    derived = Batch(
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description or (
            f"Reuses shared artifact {payload.key} from trial {trial.id}."
        ),
        task_filter=task_filter,
        trial_config=trial_config,
        state="submitted",
        created_by_token_prefix=token_prefix,
        expected_trial_count=batch.expected_trial_count if batch else 1,
        n_per_task=batch.n_per_task if batch else 1,
        backend=batch.backend if batch else "docker",
        combinations=list(batch.combinations or []) if batch else [],
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=(
            payload.provider_model_id
            or trial.provider_model_id
            or (batch.provider_model_id if batch else None)
        ),
        source_provenance=provenance,
    )
    session.add(derived)
    await session.commit()
    await session.refresh(derived)
    return {
        "batch_id": str(derived.id),
        "source_artifact": {
            "trial_id": str(trial.id),
            "key": payload.key,
            "role": role,
        },
        "source_provenance": derived.source_provenance,
        "state": derived.state,
        "created_at": derived.created_at.isoformat(),
    }
