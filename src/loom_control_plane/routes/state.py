"""Fenced state PATCH (spec §2.8).

The fencing predicate (`worker_id = :worker_id` in the UPDATE WHERE clause)
makes this safe under crash-detector reclaim: if the trial has been
reassigned to a different worker, the UPDATE matches 0 rows → the route
returns 409 + `WorkerLostClaim` semantics.

Also enforces:
- Bug 1: state/failure_reason values are validated against TrialState +
  FailureReason enums (no arbitrary strings into the DB).
- Bug 4: source state is restricted per target — succeeded/failed only from
  claimed/running, etc. — no `succeeded → queued` reversals.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from loom.auth import verify_bearer_token
from loom.family_run.orchestration import apply_advance_decision
from loom.family_run.registry import resolve_plugin
from loom.family_run.spec import AdvanceDecision, ResolvedFamilyRunSpec
from loom.models.result import FailureReason, TrialState
from loom_control_plane.metrics import STATE_PATCH_TOTAL
from loom_control_plane.protected_worker_session import ProtectedBodyWorkerStateSession
from loom_control_plane.routes.execution_fence import (
    OptionalExecutionGenerationHeader,
    OptionalExecutionLeaseIdHeader,
    enforce_trial_execution_fence,
)

router = APIRouter()

# Allowed source states for each target state. Targets not in this map
# (e.g., queued, claimed) cannot be reached via PATCH /state.
_ALLOWED_FROM: dict[TrialState, set[TrialState]] = {
    TrialState.RUNNING: {TrialState.CLAIMED, TrialState.RUNNING},
    TrialState.MATERIALIZING: {TrialState.CLAIMED, TrialState.RUNNING},
    TrialState.SUCCEEDED: {
        TrialState.CLAIMED,
        TrialState.RUNNING,
        TrialState.MATERIALIZING,
    },
    TrialState.FAILED: {
        TrialState.CLAIMED,
        TrialState.RUNNING,
        TrialState.MATERIALIZING,
    },
    TrialState.CANCELLED: {
        TrialState.QUEUED,
        TrialState.CLAIMED,
        TrialState.RUNNING,
    },
}

_PATCH_SQL = text("""
UPDATE trials
   SET state = (:new_state)::text,
       result = CASE WHEN (:has_result)::boolean
                     THEN :result_payload ELSE result END,
       failure_reason = (:failure_reason)::text,
       failure_message = CASE WHEN (:failure_message)::text IS NOT NULL
                               THEN (:failure_message)::text
                               ELSE failure_message END,
       finished_at = CASE WHEN (:is_terminal)::boolean
                           THEN NOW() ELSE finished_at END,
       started_at = CASE WHEN (:new_state)::text = 'running' AND started_at IS NULL
                          THEN NOW() ELSE started_at END
 WHERE id = (:trial_id)::uuid
   AND worker_id = (:worker_id)::uuid
   AND state = ANY(:allowed_from)
 RETURNING id, state;
""").bindparams(bindparam("result_payload", type_=JSONB))

_SUCCEEDED_RESULT_GUARD_SQL = text("""
SELECT id
  FROM trials
 WHERE id = (:trial_id)::uuid
   AND worker_id = (:worker_id)::uuid
   AND state = ANY(:allowed_from)
   AND result IS NULL
""")

_TERMINAL = {
    TrialState.SUCCEEDED,
    TrialState.FAILED,
    TrialState.CANCELLED,
}

# #672 family-runs: at finalize, load the batch's family_run_spec and the
# trial's family row so the advance predicate can decide next steps.
_FAMILY_FINALIZE_LOAD_SQL = text("""
SELECT t.family_key,
       t.batch_id,
       t.task_id,
       t.attempt_count,
       t.state           AS trial_state,
       t.result,
       b.family_run_spec AS spec,
       bfs.task_sequence,
       bfs.current_index,
       bfs.attempt_count AS family_attempt_count,
       bfs.state         AS family_state
  FROM trials t
  JOIN batches b ON b.id = t.batch_id
  JOIN batch_family_state bfs
    ON bfs.batch_id = t.batch_id
   AND bfs.family_key = t.family_key
 WHERE t.id = (:trial_id)::uuid
   AND t.family_key IS NOT NULL
   AND b.family_run_spec IS NOT NULL
   FOR UPDATE OF bfs
""")

_FAMILY_FINALIZE_UPDATE_SQL = text("""
UPDATE batch_family_state
   SET state = (:new_state)::text,
       current_index = (:new_current_index)::int,
       attempt_count = (:new_attempt_count)::int,
       updated_at = NOW()
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
""")

# #672 PR-1 shortcut: SKIP/ABORT decisions cancel any remaining queued
# trials in the same family so the batch settles cleanly.
_FAMILY_CANCEL_REMAINING_SQL = text("""
UPDATE trials
   SET state = 'cancelled',
       finished_at = NOW()
 WHERE batch_id = (:batch_id)::uuid
   AND family_key = (:family_key)::text
   AND state = 'queued'
""")


def _reward_from_result(result: Any) -> float | None:
    if not isinstance(result, dict):
        return None
    reward = result.get("reward")
    if isinstance(reward, (int, float)):
        return float(reward)
    return None


@dataclass
class _TrialShim:
    id: UUID
    task_id: str
    state: str
    reward: float | None
    attempt_count: int


@dataclass
class _FamilyShim:
    batch_id: UUID
    family_key: str
    task_sequence: list[str]
    current_index: int
    attempt_count: int


async def _finalize_family(
    session: Any,
    *,
    trial_id: UUID,
    new_state: TrialState,
) -> None:
    """Evaluate the family's advance predicate + persist the new state.

    Called from ``patch_state`` after a trial transitions to a terminal
    state (succeeded/failed/cancelled). No-op when the trial isn't
    part of a family-run batch. On ADVANCE, transitions the family to
    ``adapting`` - the ``loom_family_orchestrator`` service picks it
    up and runs the adapter, uniformly across all adapter names
    including ``noop``.
    """
    row = (
        (
            await session.execute(
                _FAMILY_FINALIZE_LOAD_SQL,
                {"trial_id": trial_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return

    spec = ResolvedFamilyRunSpec.model_validate(row["spec"])
    predicate = resolve_plugin("loom.family.advance", spec.advance_predicate)
    trial_shim = _TrialShim(
        id=trial_id,
        task_id=row["task_id"],
        state=new_state.value,
        reward=_reward_from_result(row["result"]),
        attempt_count=row["attempt_count"],
    )
    family_shim = _FamilyShim(
        batch_id=row["batch_id"],
        family_key=row["family_key"],
        task_sequence=list(row["task_sequence"]),
        current_index=row["current_index"],
        attempt_count=row["family_attempt_count"],
    )
    decision: AdvanceDecision = predicate.decide(
        trial=trial_shim,
        family=family_shim,
        spec=spec,
        params=spec.advance_predicate.params,
    )
    next_state = apply_advance_decision(family_shim, decision)

    # #672 PR-2: no more per-adapter shortcut. Every ADVANCE decision
    # transitions to 'adapting'; the loom_family_orchestrator service
    # runs the adapter (including noop) and applies the bump to
    # pending/done. This keeps the finalize path uniform across
    # adapters and prevents the CP finalize hook from silently
    # short-circuiting orchestrator observability.
    persist_state = next_state.state
    persist_index = next_state.current_index
    persist_attempt = next_state.attempt_count

    await session.execute(
        _FAMILY_FINALIZE_UPDATE_SQL,
        {
            "batch_id": row["batch_id"],
            "family_key": row["family_key"],
            "new_state": persist_state,
            "new_current_index": persist_index,
            "new_attempt_count": persist_attempt,
        },
    )

    if decision in (AdvanceDecision.SKIP, AdvanceDecision.ABORT):
        await session.execute(
            _FAMILY_CANCEL_REMAINING_SQL,
            {
                "batch_id": row["batch_id"],
                "family_key": row["family_key"],
            },
        )


@router.patch("/trials/{trial_id}/state")
async def patch_state(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    protected_worker_session: ProtectedBodyWorkerStateSession,
    authorization: str | None = Header(default=None),
    execution_lease_id: OptionalExecutionLeaseIdHeader = None,
    execution_generation: OptionalExecutionGenerationHeader = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        new_state_str = payload["state"]
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"state + worker_id required: {exc}",
        ) from exc

    # Bug 1 fix: validate state against the TrialState enum upfront so we
    # never write garbage to the DB.
    try:
        new_state = TrialState(new_state_str)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid state {new_state_str!r}; "
            f"must be one of {sorted(s.value for s in TrialState)}",
        ) from exc
    if new_state not in _ALLOWED_FROM:
        raise HTTPException(
            status_code=400,
            detail=f"state {new_state_str!r} cannot be reached via PATCH",
        )

    failure_reason_str = payload.get("failure_reason")
    if failure_reason_str is not None:
        try:
            FailureReason(failure_reason_str)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"invalid failure_reason {failure_reason_str!r}",
            ) from exc

    failure_message_str = payload.get("failure_message")
    if failure_message_str is not None and not isinstance(failure_message_str, str):
        raise HTTPException(
            status_code=400,
            detail="failure_message must be a string",
        )

    allowed_from = sorted(s.value for s in _ALLOWED_FROM[new_state])
    result_payload = payload.get("result")
    has_result = result_payload is not None

    async with request.app.state.session_factory() as session:
        await enforce_trial_execution_fence(
            session,
            trial_id=trial_id,
            lease_id=execution_lease_id,
            generation=execution_generation,
            surface="result",
            lock=True,
        )
        if new_state == TrialState.SUCCEEDED and not has_result:
            missing_result = (
                await session.execute(
                    _SUCCEEDED_RESULT_GUARD_SQL,
                    {
                        "trial_id": trial_id,
                        "worker_id": worker_id,
                        "allowed_from": allowed_from,
                    },
                )
            ).first()
            if missing_result is not None:
                STATE_PATCH_TOTAL.labels(
                    endpoint="state",
                    result="invalid",
                ).inc()
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "state 'succeeded' requires result to be supplied or already persisted"
                    ),
                )
        row = (
            (
                await session.execute(
                    _PATCH_SQL,
                    {
                        "trial_id": trial_id,
                        "worker_id": worker_id,
                        "new_state": new_state.value,
                        "result_payload": result_payload,
                        "has_result": has_result,
                        "failure_reason": failure_reason_str,
                        "failure_message": failure_message_str,
                        "is_terminal": new_state in _TERMINAL,
                        "allowed_from": allowed_from,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )

        # #672 family-runs: on terminal state, evaluate the advance
        # predicate + persist the family's new position within the same
        # transaction. No-op for non-family trials.
        if row is not None and new_state in _TERMINAL:
            await _finalize_family(
                session,
                trial_id=trial_id,
                new_state=new_state,
            )

        await session.commit()

    if row is None:
        STATE_PATCH_TOTAL.labels(endpoint="state", result="fenced").inc()
        raise HTTPException(
            status_code=409,
            detail=(
                "worker lost claim, or trial is not in a state from which "
                f"the {new_state.value} transition is allowed"
            ),
        )

    STATE_PATCH_TOTAL.labels(endpoint="state", result="ok").inc()
    return {"trial_id": str(row["id"]), "state": row["state"]}
