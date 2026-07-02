"""Trial submission + fetch endpoints."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy import insert, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from loom.auth import verify_bearer_token
from loom.db.schema import Batch, LlmCall, TeamQuota
from loom.db.schema import Task as TaskRow
from loom.db.schema import Trial as TrialRow
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom.request_params import coerce_request_params
from loom.submission_identity import require_submitting_user
from loom_control_plane.scheduler.requires_caps import derive_requires_caps

router = APIRouter()


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
            detail=(
                "required_worker_pool must be 1-80 characters "
                "and contain no whitespace"
            ),
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
    batch_submitter_user_id: UUID | None = None
    if batch_id is not None:
        async with request.app.state.session_factory() as session:
            batch_row = (
                await session.execute(
                    select(Batch.team_id, Batch.submitted_by_user_id).where(
                        Batch.id == batch_id,
                    ),
                )
            ).first()
        if batch_row is None:
            raise HTTPException(
                status_code=400,
                detail=f"unknown batch {batch_id}",
            )
        batch_team_id = batch_row.team_id
        batch_submitter_user_id = batch_row.submitted_by_user_id

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
    elif "submit:batch" in ctx.scopes and batch_team_id is not None:
        submit_team_id = batch_team_id
        submitter_user_id = batch_submitter_user_id
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
            existing = (
                await session.execute(
                    select(TrialRow).where(
                        TrialRow.idempotency_key == idempotency_key,
                        TrialRow.team_id == submit_team_id,
                    ),
                )
            ).scalar_one_or_none()
        if existing is not None:
            return {
                "trial_id": str(existing.id),
                "state": existing.state,
                "submitted_at": existing.submitted_at.isoformat(),
            }

    async with request.app.state.session_factory() as session:
        task_row = (
            await session.execute(
                select(TaskRow).where(TaskRow.id == task_id),
            )
        ).scalar_one_or_none()
    if task_row is None:
        raise HTTPException(status_code=404, detail=f"unknown task {task_id}")

    try:
        task_config = normalize_steps(
            TaskConfig.model_validate(task_row.config),
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid task config for {task_id}: {exc}",
        ) from exc
    try:
        trial_config = TrialConfig.model_validate(payload.get("config") or {})
    except ValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid trial config: {exc}",
        ) from exc
    requires_caps = derive_requires_caps(task_config)
    requires_caps_json = requires_caps.model_dump(mode="json")
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
            "idempotency_key": idempotency_key,
            "sample_idx": sample_idx,
            "combination_idx": combination_idx,
            "provider_connection_id": provider_connection_id,
            "provider_model_id": provider_model_id,
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
                await session.commit()
                if existing.team_id != submit_team_id:
                    raise HTTPException(
                        status_code=409,
                        detail=("idempotency_key collision with another team's trial"),
                    )
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
        await session.commit()

    return {
        "trial_id": str(trial_id),
        "state": "queued",
        "submitted_at": submitted_at.isoformat(),
    }


_CANCEL_SQL = text("""
UPDATE trials
   SET state = 'cancelled',
       cancellation_requested_at = NOW()
 WHERE id = (:trial_id)::uuid
   AND team_id = (:team_id)::uuid
   AND state IN ('queued', 'claimed', 'running')
 RETURNING id, state;
""")


@router.post("/trials/{trial_id}/cancel")
async def cancel_trial(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or ctx.team_id is None:
        raise HTTPException(status_code=401, detail="not authorized")

    async with request.app.state.session_factory() as session:
        row = (
            (
                await session.execute(
                    _CANCEL_SQL,
                    {
                        "trial_id": trial_id,
                        "team_id": ctx.team_id,
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
            }
            for r in rows
        ],
    }
