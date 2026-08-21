"""Trial submission + fetch endpoints."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import is_admin, verify_bearer_token
from loom.benchmark_profiles import reject_non_runnable_benchmark_profiles
from loom.data_lifecycle_registry import ensure_trial_lifecycle_authority
from loom.db.schema import (
    Batch,
    LlmCall,
    ProviderConnection,
    TeamQuota,
    TrialTaskImageMaterialization,
)
from loom.db.schema import Task as TaskRow
from loom.db.schema import Trial as TrialRow
from loom.db.task_set_visibility import visible_tasks
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom.request_params import coerce_request_params
from loom.submission_identity import require_submitting_user
from loom.task_image_materialization import ensure_task_image_materializations
from loom_control_plane.scheduler.requires_caps import derive_requires_caps
from loom_service.submission_compat import validate_submission_agent_task_compatibility

router = APIRouter()


async def _ensure_trial_task_image_links(
    session: AsyncSession,
    *,
    trial_id: UUID,
    task_row: TaskRow,
) -> None:
    materializations = await ensure_task_image_materializations(
        session,
        task_row=task_row,
    )
    if not materializations:
        return
    await session.execute(
        pg_insert(TrialTaskImageMaterialization)
        .values(
            [
                {
                    "trial_id": trial_id,
                    "materialization_id": materialization.id,
                }
                for materialization in materializations
            ]
        )
        .on_conflict_do_nothing(index_elements=["trial_id", "materialization_id"])
    )


def _required_worker_pool(payload: dict[str, Any]) -> str | None:
    raw = payload.get("required_worker_pool")
    if raw is None:
        return None
    pool = str(raw).strip()
    if not pool:
        raise HTTPException(
            status_code=400,
            detail="required_worker_pool must be a non-empty string",
        )
    if len(pool) > 80 or any(ch.isspace() for ch in pool):
        raise HTTPException(
            status_code=400,
            detail=("required_worker_pool must be 1-80 characters and contain no whitespace"),
        )
    return pool


@router.post("/trials", status_code=201)
async def submit_trial(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized to submit")

    task_id = payload.get("task_id")
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id required")
    idempotency_key = payload.get("idempotency_key")

    # Plan 19 validated only batch existence. Multi-team deployments need
    # ownership validation too: normal team tokens may only submit into their
    # own batches, while the internal batch-runner token derives the target
    # tenant from the parent batch row.
    batch_id = payload.get("batch_id")
    batch_team_id: UUID | None = None
    batch_backend = "docker"
    batch_submitter_user_id: UUID | None = None
    batch_usage_user_id: UUID | None = None
    batch_usage_actor: str | None = None
    if batch_id is not None:
        async with request.app.state.session_factory() as session:
            batch_row = (
                await session.execute(
                    select(
                        Batch.team_id,
                        Batch.backend,
                        Batch.submitted_by_user_id,
                        Batch.usage_attributed_user_id,
                        Batch.usage_attributed_actor,
                    ).where(Batch.id == batch_id),
                )
            ).first()
        if batch_row is None:
            raise HTTPException(
                status_code=400,
                detail=f"unknown batch {batch_id}",
            )
        batch_team_id = batch_row.team_id
        batch_backend = batch_row.backend
        batch_submitter_user_id = batch_row.submitted_by_user_id
        batch_usage_user_id = batch_row.usage_attributed_user_id
        batch_usage_actor = batch_row.usage_attributed_actor

    if ctx.team_id is not None:
        if "submit" not in ctx.scopes:
            raise HTTPException(
                status_code=401,
                detail="not authorized to submit",
            )
        if batch_team_id is not None and batch_team_id != ctx.team_id:
            raise HTTPException(
                status_code=403,
                detail="batch belongs to another team",
            )
        require_submitting_user(ctx)
        submit_team_id = ctx.team_id
        submitter_user_id = ctx.user_id
        usage_user_id = ctx.user_id
        usage_actor = f"user:{ctx.user_id}" if ctx.user_id is not None else None
    elif "submit:batch" in ctx.scopes and batch_team_id is not None:
        submit_team_id = batch_team_id
        submitter_user_id = batch_submitter_user_id
        usage_user_id = batch_usage_user_id
        usage_actor = batch_usage_actor
    else:
        raise HTTPException(status_code=401, detail="not authorized to submit")

    # Plan 19: if `idempotency_key` was supplied and a trial with that
    # key already exists FOR THIS TEAM, return its trial_id without
    # minting a new row. The team-scoping is important — without it,
    # a cross-team idempotency_key collision would leak the existence
    # of the other team's trial (audit H1). The follow-up INSERT below
    # has `ON CONFLICT DO NOTHING` which closes the race window; the
    # early read just skips the (heavier) license + quota work on the
    # common "runner re-submits an already-emitted trial" path.
    if idempotency_key is not None:
        async with request.app.state.session_factory() as session:
            existing_row = (
                await session.execute(
                    select(TrialRow, TaskRow)
                    .join(TaskRow, TaskRow.id == TrialRow.task_id)
                    .where(
                        TrialRow.idempotency_key == idempotency_key,
                        TrialRow.team_id == submit_team_id,
                    ),
                )
            ).one_or_none()
            if existing_row is not None:
                existing, existing_task = existing_row
                await _ensure_trial_task_image_links(
                    session,
                    trial_id=existing.id,
                    task_row=existing_task,
                )
                await session.commit()
                return {
                    "trial_id": str(existing.id),
                    "state": existing.state,
                    "submitted_at": existing.submitted_at.isoformat(),
                }

    async with request.app.state.session_factory() as session:
        task_row = (
            await session.execute(
                visible_tasks(team_id=submit_team_id).where(TaskRow.id == task_id),
            )
        ).scalar_one_or_none()
        if task_row is not None and task_row.benchmark_id is not None:
            await reject_non_runnable_benchmark_profiles(
                session,
                [task_row.benchmark_id],
            )
    if task_row is None:
        raise HTTPException(status_code=404, detail="task not found")

    try:
        task_config = normalize_steps(
            TaskConfig.model_validate(task_row.config),
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid task config for {task_id}: {exc}",
        ) from exc
    # Snapshot deployment RetryPolicy defaults into the trial payload at submit
    # time when the submitter didn't set an explicit `retry` block (#401). Persisting
    # the resolved policy keeps clone/re-run reproducible after an operator retunes
    # the deployment defaults.
    settings = request.app.state.settings
    raw_config = dict(payload.get("config") or {})
    if "retry" not in raw_config:
        raw_config["retry"] = {
            "max_attempts": settings.trial_retry_default_max_attempts,
            "retry_on": list(settings.trial_retry_default_retry_on),
            "backoff": {
                "base_sec": settings.trial_retry_default_backoff_base_sec,
                "max_sec": settings.trial_retry_default_backoff_max_sec,
                "multiplier": settings.trial_retry_default_backoff_multiplier,
                "jitter": settings.trial_retry_default_backoff_jitter,
            },
        }
    try:
        trial_config = TrialConfig.model_validate(raw_config)
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid trial config: {exc}",
        ) from exc
    if trial_config.multi_model is not None and trial_config.multi_model.enabled:
        from loom.models.trial import (
            MultiModelSwitchSpec,
            materialize_multi_model_switch_episode,
        )

        materialized = materialize_multi_model_switch_episode(trial_config.multi_model)
        trial_config = trial_config.model_copy(
            update={
                "multi_model": MultiModelSwitchSpec.model_validate(materialized),
            },
        )
    async with request.app.state.session_factory() as session:
        await validate_submission_agent_task_compatibility(
            session,
            team_id=submit_team_id,
            task_ids=[task_id],
            trial_config=trial_config.model_dump(mode="json"),
        )
    requires_caps = derive_requires_caps(task_config)
    requires_caps_json = requires_caps.model_dump(mode="json")
    requires_caps_json["backend"] = batch_backend
    if trial_config.multi_model is not None and trial_config.multi_model.enabled:
        requires_caps_json["terminus2_model_switch"] = True
    required_worker_pool = _required_worker_pool(payload)
    if required_worker_pool is not None:
        requires_caps_json["worker_pool"] = required_worker_pool

    trial_id = uuid4()
    async with request.app.state.session_factory() as session:
        # Defensive: a Team can exist without a TeamQuota row. If so, the
        # §2.6 DRF claim query's JOIN team_quotas would silently exclude
        # every trial we insert, and the trial would languish in queued
        # forever. Upsert a default quota row idempotently before the
        # trial INSERT so submission and scheduling stay aligned.
        await session.execute(
            pg_insert(TeamQuota)
            .values(team_id=submit_team_id)
            .on_conflict_do_nothing(index_elements=["team_id"]),
        )
        # Clamp requested max_attempts to the team's admin-set ceiling (#401).
        # The scheduler's claim query enforces this too, but persisting the
        # clamped value keeps the snapshot honest: what the trial payload says
        # matches what will actually run.
        ceiling = (
            await session.execute(
                select(TeamQuota.max_attempts_ceiling).where(
                    TeamQuota.team_id == submit_team_id,
                ),
            )
        ).scalar_one()
        if trial_config.retry.max_attempts > ceiling:
            trial_config = trial_config.model_copy(
                update={
                    "retry": trial_config.retry.model_copy(
                        update={
                            "max_attempts": ceiling,
                        }
                    ),
                }
            )
        # Plan 19: batch_id + idempotency_key are optional. When
        # `idempotency_key` is set we use pg_insert + ON CONFLICT DO
        # NOTHING so a concurrent race (two runner instances picking
        # the same batch row before the SELECT-skip-locked is held)
        # doesn't produce duplicate trial rows. `batch_id` was
        # already FK-validated upstream.
        # Plan 23: sample_idx is the n-sampling index. Defaults to 0 so
        # hand-submitted trials and pre-migration callers Just Work.
        sample_idx = int(payload.get("sample_idx") or 0)
        # Plan 28 PR-3: combination_idx is the multi-combination index.
        # Defaults to 0 (single-combination batches + hand-submitted
        # trials).
        combination_idx = int(payload.get("combination_idx") or 0)
        # cluster-deploy.md §Schema additions: per-trial provider
        # override. loom_service validated team-scope before forwarding;
        # control-plane trusts the validated payload (it doesn't have
        # a session_factory wired to provider_connections specifically,
        # and the FK enforces existence at INSERT time).
        provider_connection_id = payload.get("provider_connection_id")
        provider_model_id = payload.get("provider_model_id")
        # #672 PR-3: family_key is set by the batch_runner when the
        # parent batch opted into family-run mode. NULL for classic
        # batches; the scheduler's claim query gates trials whose
        # family_key is set on the matching batch_family_state row.
        family_key = payload.get("family_key")
        if family_key is not None and not isinstance(family_key, str):
            raise HTTPException(
                status_code=400,
                detail="family_key must be a string when supplied",
            )
        insert_values: dict[str, Any] = {
            "id": trial_id,
            "team_id": submit_team_id,
            "task_id": task_id,
            "config": trial_config.model_dump(mode="json"),
            "requires_caps": requires_caps_json,
            "state": "queued",
            "submit_priority": trial_config.submit_priority,
            "batch_id": batch_id,
            "submitted_by_user_id": submitter_user_id,
            "usage_attributed_user_id": usage_user_id,
            "usage_attributed_actor": usage_actor,
            "idempotency_key": idempotency_key,
            "sample_idx": sample_idx,
            "combination_idx": combination_idx,
            "provider_connection_id": provider_connection_id,
            "provider_model_id": provider_model_id,
            "family_key": family_key,
        }
        if idempotency_key is not None:
            # The partial unique index is `WHERE idempotency_key IS NOT
            # NULL`; the ON CONFLICT predicate must match it for
            # Postgres to use the index as a conflict target.
            stmt = (
                pg_insert(TrialRow)
                .values(**insert_values)
                .on_conflict_do_nothing(
                    index_elements=["idempotency_key"],
                    index_where=text(
                        "idempotency_key IS NOT NULL",
                    ),
                )
                .returning(TrialRow.id, TrialRow.submitted_at)
            )
            result = await session.execute(stmt)
            row = result.one_or_none()
            if row is None:
                # ON CONFLICT fired — another caller won the race.
                # Re-read the canonical row scoped to this team (a
                # cross-team idempotency-key collision should never
                # reach here because the partial unique index is
                # global, but we never expose another team's trial:
                # if the canonical row belongs to a different team
                # we 409 the caller).
                existing = (
                    await session.execute(
                        select(TrialRow).where(
                            TrialRow.idempotency_key == idempotency_key,
                        ),
                    )
                ).scalar_one()
                if existing.team_id != submit_team_id:
                    raise HTTPException(
                        status_code=409,
                        detail=("idempotency_key collision with another team's trial"),
                    )
                existing_task = (
                    await session.execute(
                        select(TaskRow).where(TaskRow.id == existing.task_id),
                    )
                ).scalar_one()
                await _ensure_trial_task_image_links(
                    session,
                    trial_id=existing.id,
                    task_row=existing_task,
                )
                await session.commit()
                return {
                    "trial_id": str(existing.id),
                    "state": existing.state,
                    "submitted_at": existing.submitted_at.isoformat(),
                }
            trial_id = row.id
            submitted_at = row.submitted_at
        else:
            result = await session.execute(
                insert(TrialRow).values(**insert_values).returning(TrialRow.submitted_at),
            )
            submitted_at = result.scalar_one()
        lifecycle_authority_id = await ensure_trial_lifecycle_authority(
            session,
            trial_id=trial_id,
            team_id=submit_team_id,
            created_at=submitted_at,
        )
        lifecycle_result = await session.execute(
            update(TrialRow)
            .where(
                TrialRow.id == trial_id,
                TrialRow.lifecycle_authority_id.is_(None),
            )
            .values(lifecycle_authority_id=lifecycle_authority_id)
        )
        if lifecycle_result.rowcount != 1:
            raise RuntimeError("trial lifecycle authority binding is stale")
        await _ensure_trial_task_image_links(
            session,
            trial_id=trial_id,
            task_row=task_row,
        )
        if trial_config.multi_model is not None and trial_config.multi_model.enabled:
            from loom.model_switch_store import persist_model_switch_plan

            conn_id = None
            if provider_connection_id:
                conn_id = UUID(str(provider_connection_id))
            inherit_raw = payload.get("inherit_model_switch_plan_from_trial_id")
            inherit_from = UUID(str(inherit_raw)) if inherit_raw else None
            conn_row = None
            if conn_id is not None:
                conn_row = (
                    await session.execute(
                        select(ProviderConnection).where(
                            ProviderConnection.id == conn_id,
                        ),
                    )
                ).scalar_one_or_none()
            dumped = trial_config.model_dump(mode="json")
            await persist_model_switch_plan(
                session,
                trial_id=trial_id,
                trial_config=dumped,
                agent_model=trial_config.agent_model,
                provider_connection_id=conn_id,
                combination_idx=combination_idx,
                inherit_from_trial_id=inherit_from,
                provider_connection=conn_row,
            )
        await session.commit()

    return {
        "trial_id": str(trial_id),
        "state": "queued",
        "submitted_at": submitted_at.isoformat(),
    }


_CANCEL_SQL = text("""
UPDATE trials
   SET state = 'cancelled',
       cancellation_requested_at = NOW(),
       cancellation_observed_at = CASE WHEN state = 'queued' THEN NOW()
                                       ELSE cancellation_observed_at END,
       finished_at = COALESCE(finished_at, NOW())
 WHERE id = (:trial_id)::uuid
   AND ((:team_id)::uuid IS NULL OR team_id = (:team_id)::uuid)
   AND state IN ('queued', 'claimed', 'running')
 RETURNING id, state, finished_at;
""")


@router.post("/trials/{trial_id}/cancel")
async def cancel_trial(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(request.app.state, "admin_secret_verifier", None),
        )
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    caller_is_admin = is_admin(ctx)
    # A platform admin may cancel any trial; a team caller is scoped to its own
    # team. A non-admin token with no team has nothing it may act on.
    if not caller_is_admin and ctx.team_id is None:
        raise HTTPException(status_code=401, detail="not authorized")
    scoped_team_id = None if caller_is_admin else ctx.team_id

    async with request.app.state.session_factory() as session:
        row = (
            (
                await session.execute(
                    _CANCEL_SQL,
                    {
                        "trial_id": trial_id,
                        "team_id": scoped_team_id,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        await session.commit()

    if row is None:
        raise HTTPException(
            status_code=409,
            detail="trial is in a terminal state (succeeded/failed/cancelled)",
        )
    return {"trial_id": str(row["id"]), "state": row["state"]}


@router.get("/trials/{trial_id}")
async def get_trial(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")

    async with request.app.state.session_factory() as session:
        row = (
            await session.execute(
                select(TrialRow).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="trial not found")
    if ctx.team_id is not None and row.team_id != ctx.team_id:
        raise HTTPException(status_code=403, detail="trial belongs to another team")

    return {
        "id": str(row.id),
        "team_id": str(row.team_id),
        "task_id": row.task_id,
        "state": row.state,
        "failure_reason": row.failure_reason,
        "submitted_at": row.submitted_at.isoformat(),
        "claimed_at": row.claimed_at.isoformat() if row.claimed_at else None,
        "pre_start_heartbeat_at": (
            row.pre_start_heartbeat_at.isoformat() if row.pre_start_heartbeat_at else None
        ),
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "attempt_count": row.attempt_count,
        "result": row.result,
        "trajectory_index": row.trajectory_index,
    }


@router.get("/trials/{trial_id}/llm-calls")
async def get_trial_llm_calls(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """List every llm_calls row for this trial, ordered by capture time.
    Read by the worker at finalize to project LLMCallEvents into the
    trajectory before ATIF projection runs (Plan 9 amendment A9.2)."""
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")

    # Worker scope OR same-team team-token can read.
    async with request.app.state.session_factory() as session:
        trial_row = (
            await session.execute(
                select(TrialRow.team_id).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()
    if trial_row is None:
        raise HTTPException(status_code=404, detail="trial not found")
    if ctx.team_id is not None and trial_row != ctx.team_id:
        raise HTTPException(
            status_code=403,
            detail="trial belongs to another team",
        )

    async with request.app.state.session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(LlmCall)
                    .where(LlmCall.trial_id == trial_id)
                    .order_by(LlmCall.captured_at),
                )
            )
            .scalars()
            .all()
        )
    return {
        "items": [
            {
                "id": str(r.id),
                "trial_id": str(r.trial_id),
                "step_id": r.step_id,
                "dialect": r.dialect,
                "model": r.model,
                "input_tokens": r.input_tokens,
                "output_tokens": r.output_tokens,
                "provider_extras": r.provider_extras,
                "request_params": coerce_request_params(r.request_params),
                "cost_usd": float(r.cost_usd),
                "rate_card_hash": r.rate_card_hash,
                "captured_at": r.captured_at.isoformat(),
                # #298 Slice B: gateway-internal retry attempt that
                # produced this row. Defaults to 1 for pre-#298 rows.
                "attempt": r.attempt,
                "client_call_id": str(r.client_call_id) if r.client_call_id else None,
                "episode": r.episode,
                "call_ordinal": r.call_ordinal,
                "requested_model": r.requested_model,
                "response_model": r.response_model,
                "role": r.role,
                "correlation_status": r.correlation_status,
            }
            for r in rows
        ],
    }


class _TerminusReclaimBody(BaseModel):
    step_id: str = Field(min_length=1)
    worker_id: UUID | None = None


class _EpisodeCheckpointBody(BaseModel):
    execution_id: UUID
    run_attempt_id: UUID
    episode: int = Field(ge=1)
    active_role: str
    last_call_ordinal: int = Field(ge=0)
    last_seq: int = Field(ge=0)
    tmux_session_id: str | None = None


@router.post("/trials/{trial_id}/terminus/reclaim")
async def reclaim_terminus(
    trial_id: UUID,
    payload: _TerminusReclaimBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    from loom_control_plane.terminus_recovery import reclaim_terminus_execution

    async with request.app.state.session_factory() as session:
        trial_team = (
            await session.execute(
                select(TrialRow.team_id).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()
        if trial_team is None:
            raise HTTPException(status_code=404, detail="trial not found")
        if ctx.team_id is not None and trial_team != ctx.team_id:
            raise HTTPException(status_code=403, detail="trial belongs to another team")
        state = await reclaim_terminus_execution(
            session,
            trial_id=trial_id,
            step_id=payload.step_id,
            worker_id=payload.worker_id,
        )
        await session.commit()
    return state.model_dump(mode="json")


@router.post("/trials/{trial_id}/terminus/episode-checkpoints")
async def post_episode_checkpoint(
    trial_id: UUID,
    payload: _EpisodeCheckpointBody,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    from loom_control_plane.terminus_recovery import write_episode_checkpoint

    async with request.app.state.session_factory() as session:
        trial_team = (
            await session.execute(
                select(TrialRow.team_id).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()
        if trial_team is None:
            raise HTTPException(status_code=404, detail="trial not found")
        if ctx.team_id is not None and trial_team != ctx.team_id:
            raise HTTPException(status_code=403, detail="trial belongs to another team")
        row = await write_episode_checkpoint(
            session,
            execution_id=payload.execution_id,
            run_attempt_id=payload.run_attempt_id,
            episode=payload.episode,
            active_role=payload.active_role,
            last_call_ordinal=payload.last_call_ordinal,
            last_seq=payload.last_seq,
            tmux_session_id=payload.tmux_session_id,
        )
        await session.commit()
    return {"id": str(row.id), "version": row.version, "checksum": row.checksum}
