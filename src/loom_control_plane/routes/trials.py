"""Trial submission + fetch endpoints."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import insert, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from loom.auth import verify_bearer_token
from loom.db.schema import LlmCall, TeamQuota
from loom.db.schema import Task as TaskRow
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
    idempotency_key = payload.get("idempotency_key")

    # Plan 19: if `idempotency_key` was supplied and a trial with that
    # key already exists, return its trial_id without minting a new
    # row. The check + insert are not atomic against a true race; the
    # follow-up INSERT below has `ON CONFLICT (idempotency_key) DO
    # NOTHING` which closes the window — we still do the early read so
    # the common "campaign runner re-submits an already-emitted trial"
    # case skips the (heavier) license + quota work.
    if idempotency_key is not None:
        async with request.app.state.session_factory() as session:
            existing = (await session.execute(
                select(TrialRow).where(
                    TrialRow.idempotency_key == idempotency_key,
                ),
            )).scalar_one_or_none()
        if existing is not None:
            return {
                "trial_id": str(existing.id),
                "state": existing.state,
                "submitted_at": existing.submitted_at.isoformat(),
            }

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
        # trial INSERT so submission and scheduling stay aligned. The
        # DB-level default on team_quotas.license_allowlist (Plan 13
        # A13.1) means the new row carries the v1 allowlist automatically.
        await session.execute(
            pg_insert(TeamQuota)
            .values(team_id=ctx.team_id)
            .on_conflict_do_nothing(index_elements=["team_id"]),
        )
        # Plan 13 Task 4: license-allowlist enforcement. Re-read the
        # quota row (which may have just been upserted with defaults) to
        # check the task's license against the team's allowlist. A NULL
        # task license is treated as "allowed" — only benchmark-imported
        # tasks have license tags; hand-authored tasks pass through.
        # NOTE: explicit `is not None` — an empty-string license (which a
        # buggy benchmark importer could produce) must NOT bypass; it
        # should fall through to the allowlist check and 403.
        if task_row.license is not None:
            quota = (await session.execute(
                select(TeamQuota).where(TeamQuota.team_id == ctx.team_id),
            )).scalar_one()
            if task_row.license not in quota.license_allowlist:
                raise HTTPException(
                    status_code=403,
                    detail=(
                        f"task license {task_row.license!r} not in team's "
                        f"allowlist {sorted(quota.license_allowlist)}"
                    ),
                )
        # Plan 19: campaign_id + idempotency_key are optional. When
        # `idempotency_key` is set we use pg_insert + ON CONFLICT DO
        # NOTHING so a concurrent race (two runner instances picking
        # the same campaign row before the SELECT-skip-locked is held)
        # doesn't produce duplicate trial rows.
        campaign_id = payload.get("campaign_id")
        insert_values: dict[str, Any] = {
            "id": trial_id, "team_id": ctx.team_id, "task_id": task_id,
            "config": trial_config.model_dump(mode="json"),
            "requires_caps": requires_caps.model_dump(mode="json"),
            "state": "queued",
            "submit_priority": trial_config.submit_priority,
            "campaign_id": campaign_id,
            "idempotency_key": idempotency_key,
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
                # Re-read the canonical row.
                existing = (await session.execute(
                    select(TrialRow).where(
                        TrialRow.idempotency_key == idempotency_key,
                    ),
                )).scalar_one()
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
                insert(TrialRow).values(**insert_values)
                .returning(TrialRow.submitted_at),
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
        row = (await session.execute(_CANCEL_SQL, {
            "trial_id": trial_id, "team_id": ctx.team_id,
        })).mappings().one_or_none()
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
        row = (await session.execute(
            select(TrialRow).where(TrialRow.id == trial_id),
        )).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="trial not found")
    if ctx.team_id is not None and row.team_id != ctx.team_id:
        raise HTTPException(status_code=403, detail="trial belongs to another team")

    return {
        "id": str(row.id), "team_id": str(row.team_id), "task_id": row.task_id,
        "state": row.state, "failure_reason": row.failure_reason,
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
        trial_row = (await session.execute(
            select(TrialRow.team_id).where(TrialRow.id == trial_id),
        )).scalar_one_or_none()
    if trial_row is None:
        raise HTTPException(status_code=404, detail="trial not found")
    if ctx.team_id is not None and trial_row != ctx.team_id:
        raise HTTPException(
            status_code=403, detail="trial belongs to another team",
        )

    async with request.app.state.session_factory() as session:
        rows = (await session.execute(
            select(LlmCall)
            .where(LlmCall.trial_id == trial_id)
            .order_by(LlmCall.captured_at),
        )).scalars().all()
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
                "cost_usd": float(r.cost_usd),
                "rate_card_hash": r.rate_card_hash,
                "captured_at": r.captured_at.isoformat(),
            }
            for r in rows
        ],
    }
