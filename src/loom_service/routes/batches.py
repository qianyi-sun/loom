"""Batches CRUD (spec §5.3 / Plan 19, renamed in Plan 28).

Routes:
- POST /api/v1/batches          — create + immediately materialize
                                  expected_trial_count from task_filter
- GET  /api/v1/batches          — list with cursor pagination
- GET  /api/v1/batches/{id}     — detail + trial roll-up (state summary,
                                  aggregate reward from Trial.result JSONB,
                                  and token totals from llm_calls)
- POST /api/v1/batches/{id}/cancel — terminate the batch + cascade-cancel
                                  its still-active trials
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Any, NoReturn
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select, update

from loom.db.schema import (
    Batch,
    Benchmark,
    LlmCall,
    ProviderModelCache,
    Task,
    Team,
    Trial,
)
from loom.models.batch import Combination
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
from loom_service.metrics import SUBMISSION_REJECTS_TOTAL
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.task_config_validation import (
    expected_trial_count,
    invalid_task_config_detail,
    split_valid_task_configs,
)
from loom_service.task_filter import resolve_task_filter_with_diagnostics
from loom_service.worker_backends import get_active_backends

router = APIRouter()

_RERUNNABLE_FAILURE_REASONS: frozenset[str] = frozenset({
    "gateway_error",
    "retry_exhausted",
    "exhausted_retries",
})


class _CreateBatch(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    task_filter: dict[str, Any]
    trial_config: dict[str, Any]
    # Plan 23: n-sampling for single-combination batches. Ignored
    # when `combinations` is non-empty (each Combination carries
    # its own n_per_task).
    n_per_task: int = Field(default=1, ge=1, le=100)
    # Plan 28 PR-3: backend selection at the batch level. Optional;
    # defaults to "docker" so single-backend deployments don't have
    # to send it.
    backend: str = "docker"
    # Plan 28 PR-3: multi-(agent, model) combinations. Empty list
    # ⇒ single-combination behavior (agent + model come from
    # trial_config).
    combinations: list[Combination] = Field(default_factory=list)
    # cluster-deploy.md §Schema additions: batch-level provider
    # override. Carries through fan-out into Trial.provider_connection_id
    # for every trial spawned from this batch (unless overridden on
    # a per-trial basis at submission). Validated against the team
    # before insertion.
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


def _reject_submission(
    *,
    reason: str,
    status_code: int,
    detail: str,
) -> NoReturn:
    SUBMISSION_REJECTS_TOTAL.labels(reason=reason).inc()
    raise HTTPException(status_code=status_code, detail=detail)


async def _reject_if_team_paused(session: Any, team_id: UUID) -> None:
    paused_at = (await session.execute(
        select(Team.submissions_paused_at).where(Team.id == team_id),
    )).scalar_one_or_none()
    if paused_at is not None:
        _reject_submission(
            reason="team_paused",
            status_code=403,
            detail="team submissions are paused",
        )


async def _reject_if_known_failed_provider_model(
    session: Any,
    *,
    provider_connection_id: UUID | None,
    provider_model_id: str | None,
) -> None:
    if provider_connection_id is None or not provider_model_id:
        return
    row = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == provider_connection_id,
            ProviderModelCache.model_id == provider_model_id,
        ),
    )).scalar_one_or_none()
    if row is None or row.last_preflight_status != "failed":
        return
    detail = (
        f"provider model {provider_model_id!r} last preflight failed "
        f"for this provider connection"
    )
    if row.last_preflight_error_code:
        detail += f" ({row.last_preflight_error_code})"
    detail += (
        "; run provider model preflight again or choose another model"
    )
    _reject_submission(
        reason="provider_model_preflight",
        status_code=400,
        detail=detail,
    )


def _serialize(
    b: Batch,
    *,
    summary: dict[str, int] | None = None,
    aggregate_reward: float | None = None,
    usage: dict[str, int] | None = None,
    owner_team: Team | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(b.id),
        "team_id": str(b.team_id),
        "name": b.name,
        "description": b.description,
        "task_filter": b.task_filter,
        "trial_config": b.trial_config,
        "state": b.state,
        "result_status": b.result_status,
        "failure_reason": b.failure_reason,
        "failure_message": b.failure_message,
        "fanout_errors": b.fanout_errors,
        "rerun_of_batch_id": (
            str(b.rerun_of_batch_id) if b.rerun_of_batch_id else None
        ),
        "rerun_targets": b.rerun_targets,
        "created_at": b.created_at.isoformat(),
        "finished_at": (
            b.finished_at.isoformat() if b.finished_at else None
        ),
        "created_by_token_prefix": b.created_by_token_prefix,
        "expected_trial_count": b.expected_trial_count,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "combinations": b.combinations,
        "visibility": b.visibility,
        "share_status": b.share_status,
        "source_provenance": b.source_provenance,
    }
    if owner_team is not None:
        out["owner_team"] = {
            "id": str(owner_team.id),
            "name": owner_team.name,
        }
    out.update(usage or _empty_usage_projection())
    if summary is not None:
        out["trial_summary"] = summary
        out["aggregate_reward"] = aggregate_reward
    if extra:
        out.update(extra)
    return out


@router.post("/batches", status_code=201)
async def create_batch(
    request: Request,
    sc: SessionAndCtx,
    payload: _CreateBatch,
) -> dict[str, Any]:
    s, ctx = sc
    try:
        require_scope(ctx, "submit")
    except HTTPException:
        SUBMISSION_REJECTS_TOTAL.labels(reason="permission").inc()
        raise
    if ctx.team_id is None:
        _reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="admin tokens must scope batches to a team — "
                   "use the service's per-team admin token",
        )
    await _reject_if_team_paused(s, ctx.team_id)

    catalog = known_names()

    if payload.combinations:
        # Multi-combination batch. trial_config MUST NOT carry
        # agent_name / agent_model / n_per_task in this shape —
        # those live on each Combination.
        for forbidden in ("agent_name", "agent_model"):
            if forbidden in payload.trial_config:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"trial_config.{forbidden} must be absent when "
                        "`combinations` is supplied — each combination "
                        "carries its own agent/model"
                    ),
                )
        # Catalog membership + agent⇄model compatibility per
        # Combination.
        for i, combo in enumerate(payload.combinations):
            if combo.agent_name not in catalog:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"combinations[{i}].agent_name "
                        f"{combo.agent_name!r} is not in the agent "
                        "catalog. GET /api/v1/agents."
                    ),
                )
            err = validate_agent_model_compat(
                combo.agent_name, combo.agent_model,
            )
            if err is not None:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=f"combinations[{i}]: {err}",
                )
        # Labels unique within the batch (after computing the
        # derived label for those without one).
        seen_labels: set[str] = set()
        for i, combo in enumerate(payload.combinations):
            label = combo.label or _derive_combination_label(combo)
            if label in seen_labels:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"combinations[{i}] label {label!r} is "
                        "duplicated within the batch"
                    ),
                )
            seen_labels.add(label)
    else:
        # Single-combination batch. Catalog check on the agent
        # embedded in trial_config + agent⇄model compatibility.
        agent_name = payload.trial_config.get("agent_name")
        if isinstance(agent_name, str) and agent_name:
            if agent_name not in catalog:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        f"unknown agent_name {agent_name!r} in "
                        "trial_config. GET /api/v1/agents."
                    ),
                )
            if "agent_model" not in payload.trial_config:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=(
                        "trial_config.agent_model is required when "
                        "trial_config.agent_name is supplied; use null "
                        "for agents that do not call a model"
                    ),
                )
            model_raw = payload.trial_config["agent_model"]
            model: ModelSpec | None
            if model_raw is None:
                model = None
            else:
                try:
                    model = ModelSpec.model_validate(model_raw)
                except Exception as exc:
                    _reject_submission(
                        reason="invalid_input",
                        status_code=400,
                        detail=(
                            "trial_config.agent_model failed to "
                            f"validate: {exc}"
                        ),
                    )
            err = validate_agent_model_compat(agent_name, model)
            if err is not None:
                _reject_submission(
                    reason="invalid_input",
                    status_code=400,
                    detail=f"trial_config: {err}",
                )

    # cluster-deploy.md §POST /batches: reject when no live worker
    # advertises the requested backend. Saves operators from creating
    # batches that would stall in 'submitted' forever (no worker will
    # ever claim them). Backend catalog is owned by /api/v1/backends;
    # this check uses the same predicate so a backend that shows
    # `available=false` there also rejects here.
    active_backends = await get_active_backends(s)
    if payload.backend not in active_backends:
        available_str = (
            ", ".join(sorted(active_backends)) if active_backends
            else "(none — no active workers)"
        )
        _reject_submission(
            reason="no_workers",
            status_code=400,
            detail=(
                f"no active worker advertises backend "
                f"{payload.backend!r}. Currently available: "
                f"{available_str}. See `GET /api/v1/backends`."
            ),
        )

    # Validate provider_connection_id before task materialization/fan-out
    # work so known bad provider/model input returns a direct actionable error.
    if payload.provider_connection_id is not None and ctx.team_id is not None:
        try:
            await validate_provider_connection(
                s, payload.provider_connection_id, team_id=ctx.team_id,
            )
        except HTTPException:
            SUBMISSION_REJECTS_TOTAL.labels(
                reason="provider_connection",
            ).inc()
            raise
        await _reject_if_known_failed_provider_model(
            s,
            provider_connection_id=payload.provider_connection_id,
            provider_model_id=payload.provider_model_id,
        )

    task_result = await resolve_task_filter_with_diagnostics(
        s,
        payload.task_filter,
    )
    task_ids = task_result.task_ids
    # Audit M2: a filter materializing to zero tasks creates a
    # batch stuck in `submitted` forever — reject up front.
    if not task_ids:
        _reject_submission(
            reason="empty_filter",
            status_code=400,
            detail=(
                f"task_filter {payload.task_filter} matched zero "
                "tasks; refusing to create empty batch"
            ),
        )

    valid_task_ids, invalid_tasks = await split_valid_task_configs(
        s, task_ids,
    )
    if invalid_tasks:
        _reject_submission(
            reason="invalid_task_config",
            status_code=400,
            detail=invalid_task_config_detail(invalid_tasks),
        )

    token_prefix = (
        ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    )

    # expected_trial_count = sum over combinations × tasks.
    if payload.combinations:
        combinations_jsonb = [
            c.model_dump(mode="json") for c in payload.combinations
        ]
        expected = expected_trial_count(
            task_count=len(valid_task_ids),
            n_per_task=payload.n_per_task,
            combinations=combinations_jsonb,
        )
    else:
        expected = expected_trial_count(
            task_count=len(valid_task_ids),
            n_per_task=payload.n_per_task,
            combinations=None,
        )
        combinations_jsonb = []

    b = Batch(
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description,
        task_filter=payload.task_filter,
        trial_config=payload.trial_config,
        state="submitted",
        created_by_token_prefix=token_prefix,
        expected_trial_count=expected,
        n_per_task=payload.n_per_task,
        backend=payload.backend,
        combinations=combinations_jsonb,
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=payload.provider_model_id,
    )
    s.add(b)
    await s.commit()
    await s.refresh(b)
    return {
        "batch_id": str(b.id),
        "expected_trial_count": expected,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "combinations": b.combinations,
        "state": b.state,
        "created_at": b.created_at.isoformat(),
    }


def _derive_combination_label(combo: Combination) -> str:
    """Default label `"{agent_name}"` or
    `"{agent_name}/{provider}/{name}"` when a model is set."""
    if combo.agent_model is None:
        return combo.agent_name
    return (
        f"{combo.agent_name}/{combo.agent_model.provider}/"
        f"{combo.agent_model.name}"
    )


@router.get("/batches")
async def list_batches(
    request: Request,
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")

    target_team = team_id
    if target_team is not None:
        require_team_or_admin(ctx, target_team)
    elif not is_admin(ctx):
        target_team = ctx.team_id

    stmt = select(Batch).order_by(
        Batch.created_at.desc(), Batch.id.desc(),
    )
    if target_team is not None:
        stmt = stmt.where(Batch.team_id == target_team)
    if state:
        wanted = [x.strip() for x in state.split(",") if x.strip()]
        if wanted:
            stmt = stmt.where(Batch.state.in_(wanted))
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            ) from exc
        stmt = stmt.where(
            or_(
                Batch.created_at < cur.submitted_at,
                and_(
                    Batch.created_at == cur.submitted_at,
                    Batch.id < cur.id,
                ),
            ),
        )
    stmt = stmt.limit(limit + 1)
    rows: Sequence[Batch] = (
        await s.execute(stmt)
    ).scalars().all()

    items = list(rows)
    if len(items) > limit:
        items = items[:limit]
        last = items[-1]
        next_cursor: str | None = encode_cursor(
            Cursor(submitted_at=last.created_at, id=last.id),
        )
    else:
        next_cursor = None
    usage_by_batch = await _usage_by_batch_ids(s, [r.id for r in items])
    return {
        "items": [
            _serialize(r, usage=usage_by_batch.get(r.id)) for r in items
        ],
        "next_cursor": next_cursor,
    }


def _rollup_from_result(result: dict[str, Any] | None) -> float | None:
    """Pull reward out of a Trial.result JSONB. Same logic as
    routes/trials.py — kept inline here so the rollup query can flow
    naturally."""
    if not result:
        return None
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        return float(reward) if reward is not None else None
    except (TypeError, ValueError):
        return None


def _empty_usage_projection() -> dict[str, int]:
    return {
        "total_prompt_tokens": 0,
        "total_completion_tokens": 0,
        "llm_calls_count": 0,
    }


async def _usage_totals_for_trials(
    session: Any,
    trials: Sequence[Trial],
) -> dict[str, int]:
    trial_ids = [trial.id for trial in trials]
    if not trial_ids:
        return _empty_usage_projection()
    row = (await session.execute(
        select(
            func.coalesce(
                func.sum(LlmCall.input_tokens), 0,
            ).label("total_prompt_tokens"),
            func.coalesce(
                func.sum(LlmCall.output_tokens), 0,
            ).label("total_completion_tokens"),
            func.count(LlmCall.id).label("llm_calls_count"),
        ).where(LlmCall.trial_id.in_(trial_ids)),
    )).one()
    return {
        "total_prompt_tokens": int(row.total_prompt_tokens or 0),
        "total_completion_tokens": int(row.total_completion_tokens or 0),
        "llm_calls_count": int(row.llm_calls_count or 0),
    }


async def _usage_by_batch_ids(
    session: Any,
    batch_ids: Sequence[UUID],
) -> dict[UUID, dict[str, int]]:
    if not batch_ids:
        return {}
    rows = (await session.execute(
        select(
            Trial.batch_id.label("batch_id"),
            func.coalesce(
                func.sum(LlmCall.input_tokens), 0,
            ).label("total_prompt_tokens"),
            func.coalesce(
                func.sum(LlmCall.output_tokens), 0,
            ).label("total_completion_tokens"),
            func.count(LlmCall.id).label("llm_calls_count"),
        )
        .join(LlmCall, LlmCall.trial_id == Trial.id)
        .where(Trial.batch_id.in_(batch_ids))
        .group_by(Trial.batch_id),
    )).all()
    return {
        row.batch_id: {
            "total_prompt_tokens": int(row.total_prompt_tokens or 0),
            "total_completion_tokens": int(row.total_completion_tokens or 0),
            "llm_calls_count": int(row.llm_calls_count or 0),
        }
        for row in rows
    }


def _empty_trial_summary() -> dict[str, int]:
    return {
        k: 0 for k in (
            "queued", "claimed", "running",
            "succeeded", "failed", "cancelled",
        )
    }


def _summary_from_trials(trials: Sequence[Trial]) -> dict[str, int]:
    summary = _empty_trial_summary()
    for trial in trials:
        state = str(trial.state)
        summary[state] = summary.get(state, 0) + 1
    return summary


def _rollup_from_trials(trials: Sequence[Trial]) -> float | None:
    reward_sum = 0.0
    reward_n = 0
    for trial in trials:
        if str(trial.state) not in {"succeeded", "failed"}:
            continue
        rew = _rollup_from_result(trial.result)
        if rew is not None:
            reward_sum += rew
            reward_n += 1
    return (reward_sum / reward_n) if reward_n > 0 else None


async def _benchmark_summary_from_trials(
    session: Any,
    trials: Sequence[Trial],
) -> list[dict[str, Any]]:
    task_ids = sorted({trial.task_id for trial in trials})
    if not task_ids:
        return []

    rows = (await session.execute(
        select(
            Task.id,
            Task.benchmark_id,
            Benchmark.display_name,
        )
        .outerjoin(Benchmark, Benchmark.id == Task.benchmark_id)
        .where(Task.id.in_(task_ids)),
    )).all()
    task_lookup = {
        str(row.id): {
            "benchmark_id": row.benchmark_id,
            "display_name": row.display_name,
        }
        for row in rows
    }

    grouped: dict[str, list[Trial]] = defaultdict(list)
    labels: dict[str, tuple[str | None, str]] = {}
    for trial in trials:
        meta = task_lookup.get(str(trial.task_id), {})
        benchmark_id = meta.get("benchmark_id")
        group_key = str(benchmark_id) if benchmark_id else "__unbenchmarked__"
        display_name = (
            str(meta.get("display_name"))
            if meta.get("display_name")
            else (str(benchmark_id) if benchmark_id else "Unbenchmarked tasks")
        )
        grouped[group_key].append(trial)
        labels[group_key] = (
            str(benchmark_id) if benchmark_id else None,
            display_name,
        )

    summaries: list[dict[str, Any]] = []
    for group_key, group_trials in grouped.items():
        summary = _summary_from_trials(group_trials)
        benchmark_id, display_name = labels[group_key]
        summaries.append({
            "benchmark_id": benchmark_id,
            "display_name": display_name,
            "metric_name": "score",
            "expected_trial_count": len(group_trials),
            "completed_trial_count": sum(
                summary.get(state, 0)
                for state in ("succeeded", "failed", "cancelled")
            ),
            "platform_failed_count": summary.get("failed", 0),
            "trial_summary": summary,
            "aggregate_reward": _rollup_from_trials(group_trials),
        })

    return sorted(
        summaries,
        key=lambda row: (
            str(row["display_name"]).casefold(),
            str(row["benchmark_id"] or ""),
        ),
    )


def _trial_key(trial: Trial) -> tuple[str, int, int]:
    return (trial.task_id, int(trial.sample_idx), int(trial.combination_idx))


def _is_rerunnable_failure(trial: Trial) -> bool:
    return (
        str(trial.state) == "failed"
        and trial.failure_reason in _RERUNNABLE_FAILURE_REASONS
    )


def _effective_trials(
    original_trials: Sequence[Trial],
    rerun_trials: Sequence[Trial],
) -> list[Trial]:
    original_by_key = {_trial_key(trial): trial for trial in original_trials}
    effective = dict(original_by_key)
    for trial in rerun_trials:
        if str(trial.state) != "succeeded":
            continue
        key = _trial_key(trial)
        original = original_by_key.get(key)
        if original is None or not _is_rerunnable_failure(original):
            continue
        effective[key] = trial
    return list(effective.values())


def _result_status_from_trials(trials: Sequence[Trial]) -> str | None:
    if not trials:
        return None
    states = [str(trial.state) for trial in trials]
    if any(state in {"queued", "claimed", "running"} for state in states):
        return None
    succeeded = sum(1 for state in states if state == "succeeded")
    failed = len(states) - succeeded
    if failed == 0 and succeeded > 0:
        return "succeeded"
    if succeeded == 0:
        return "all_failed"
    return "partial_failed"


@router.get("/batches/{batch_id}")
async def get_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (await s.execute(
        select(Batch).where(Batch.id == batch_id),
    )).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404, detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    owner_team = (await s.execute(
        select(Team).where(Team.id == b.team_id),
    )).scalar_one_or_none()

    original_trials = (await s.execute(
        select(Trial).where(Trial.batch_id == batch_id),
    )).scalars().all()
    summary = _summary_from_trials(original_trials)
    avg_reward = _rollup_from_trials(original_trials)
    usage = await _usage_totals_for_trials(s, original_trials)
    benchmark_summary = await _benchmark_summary_from_trials(
        s, original_trials,
    )

    rerun_batches = (await s.execute(
        select(Batch)
        .where(Batch.rerun_of_batch_id == batch_id)
        .order_by(Batch.created_at.asc(), Batch.id.asc()),
    )).scalars().all()
    rerun_batch_ids = [child.id for child in rerun_batches]
    rerun_trials: list[Trial] = []
    if rerun_batch_ids:
        rerun_trials = list((await s.execute(
            select(Trial).where(Trial.batch_id.in_(rerun_batch_ids)),
        )).scalars().all())
    effective_trials = _effective_trials(original_trials, rerun_trials)
    effective_summary = _summary_from_trials(effective_trials)
    effective_reward = _rollup_from_trials(effective_trials)
    effective_usage = await _usage_totals_for_trials(s, effective_trials)
    rerunnable_failed_count = sum(
        1 for trial in original_trials if _is_rerunnable_failure(trial)
    )
    extra = {
        "rerun_batches": [
            {
                "id": str(child.id),
                "name": child.name,
                "state": child.state,
                "result_status": child.result_status,
                "expected_trial_count": child.expected_trial_count,
                "created_at": child.created_at.isoformat(),
                "finished_at": (
                    child.finished_at.isoformat() if child.finished_at else None
                ),
            }
            for child in rerun_batches
        ],
        "rerunnable_failed_count": rerunnable_failed_count,
        "effective_trial_summary": effective_summary,
        "effective_result_status": _result_status_from_trials(effective_trials),
        "effective_aggregate_reward": effective_reward,
        "effective_total_prompt_tokens": effective_usage[
            "total_prompt_tokens"
        ],
        "effective_total_completion_tokens": effective_usage[
            "total_completion_tokens"
        ],
        "effective_llm_calls_count": effective_usage["llm_calls_count"],
        "benchmark_summary": benchmark_summary,
    }

    return _serialize(
        b,
        summary=summary,
        aggregate_reward=avg_reward,
        usage=usage,
        owner_team=owner_team,
        extra=extra,
    )


@router.post("/batches/{batch_id}/rerun-failed", status_code=201)
async def rerun_failed_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    try:
        require_scope(ctx, "submit")
    except HTTPException:
        SUBMISSION_REJECTS_TOTAL.labels(reason="permission").inc()
        raise
    b = (await s.execute(
        select(Batch).where(Batch.id == batch_id),
    )).scalar_one_or_none()
    if b is None:
        _reject_submission(
            reason="invalid_input",
            status_code=404,
            detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    await _reject_if_team_paused(s, b.team_id)

    active_backends = await get_active_backends(s)
    if b.backend not in active_backends:
        available_str = (
            ", ".join(sorted(active_backends)) if active_backends
            else "(none -- no active workers)"
        )
        _reject_submission(
            reason="no_workers",
            status_code=400,
            detail=(
                f"no active worker advertises backend {b.backend!r}. "
                f"Currently available: {available_str}."
            ),
        )

    failed_trials = (await s.execute(
        select(Trial)
        .where(
            and_(
                Trial.batch_id == batch_id,
                Trial.state == "failed",
                Trial.failure_reason.in_(sorted(_RERUNNABLE_FAILURE_REASONS)),
            ),
        )
        .order_by(Trial.task_id.asc(), Trial.combination_idx.asc(), Trial.sample_idx.asc()),
    )).scalars().all()
    child_batch_ids = (await s.execute(
        select(Batch.id).where(Batch.rerun_of_batch_id == batch_id),
    )).scalars().all()
    successful_rerun_keys: set[tuple[str, int, int]] = set()
    if child_batch_ids:
        successful_rerun_keys = {
            _trial_key(trial)
            for trial in (await s.execute(
                select(Trial).where(
                    and_(
                        Trial.batch_id.in_(child_batch_ids),
                        Trial.state == "succeeded",
                    ),
                ),
            )).scalars().all()
        }
        failed_trials = [
            trial for trial in failed_trials
            if _trial_key(trial) not in successful_rerun_keys
        ]
    if not failed_trials:
        _reject_submission(
            reason="invalid_input",
            status_code=400,
            detail="batch has no rerunnable failed trials",
        )

    targets = [
        {
            "task_id": trial.task_id,
            "sample_idx": int(trial.sample_idx),
            "combination_idx": int(trial.combination_idx),
            "original_trial_id": str(trial.id),
            "failure_reason": trial.failure_reason,
        }
        for trial in failed_trials
    ]
    task_ids = sorted({trial.task_id for trial in failed_trials})
    token_prefix = ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    rerun = Batch(
        team_id=b.team_id,
        name=f"{b.name} failed-case rerun",
        description=(
            f"Reruns {len(targets)} transient failed case(s) from batch {b.id}."
        ),
        task_filter={"subset_kind": "explicit", "task_ids": task_ids},
        trial_config=dict(b.trial_config),
        state="submitted",
        created_by_token_prefix=token_prefix,
        expected_trial_count=len(targets),
        n_per_task=1,
        backend=b.backend,
        combinations=list(b.combinations or []),
        provider_connection_id=b.provider_connection_id,
        provider_model_id=b.provider_model_id,
        rerun_of_batch_id=b.id,
        rerun_targets=targets,
    )
    s.add(rerun)
    await s.commit()
    await s.refresh(rerun)
    return {
        "batch_id": str(rerun.id),
        "rerun_of_batch_id": str(b.id),
        "expected_trial_count": rerun.expected_trial_count,
        "state": rerun.state,
        "created_at": rerun.created_at.isoformat(),
        "rerun_target_count": len(targets),
    }


@router.post("/batches/{batch_id}/cancel")
async def cancel_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "submit")
    b = (await s.execute(
        select(Batch).where(Batch.id == batch_id),
    )).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404, detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    now = datetime.now(UTC)
    await s.execute(
        update(Batch)
        .where(Batch.id == batch_id)
        .values(state="cancelled", finished_at=now),
    )
    # Cascade-cancel still-active trials in this batch. We do
    # NOT cancel queued trials whose worker may already be partway
    # through claim; the CP's existing cancel endpoint (Plan 5)
    # handles graceful interruption when called per-trial. Here we
    # just transition the rows to `cancelled` so the SPA stops
    # showing them as in-flight.
    await s.execute(
        update(Trial)
        .where(
            and_(
                Trial.batch_id == batch_id,
                Trial.state.in_(["queued", "claimed", "running"]),
            ),
        )
        .values(
            state="cancelled",
            cancellation_requested_at=now,
            finished_at=now,
        ),
    )
    await s.commit()
    return {"batch_id": str(batch_id), "state": "cancelled"}
