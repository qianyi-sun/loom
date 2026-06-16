"""POST /admin/step-tokens — mint a step-scoped JWT (Plan 9 Task 4).

Worker calls this once per step to obtain a bearer token the agent
presents at the Gateway. The Gateway verifies the JWT and extracts
`(team_id, trial_id, step_id)` for cost attribution.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from loom.auth import mint_step_jwt, verify_bearer_token
from loom.db.schema import Trial as TrialRow

router = APIRouter(prefix="/admin")


class _IssueStepTokenRequest(BaseModel):
    team_id: UUID
    trial_id: UUID
    step_id: str = Field(min_length=1, max_length=64)
    ttl_sec: int = Field(gt=0, le=7200)


@router.post("/step-tokens", status_code=201)
async def issue_step_token(
    request: Request,
    payload: _IssueStepTokenRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session, authorization, signing_key=signing_key,
        )
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(
            status_code=403, detail="missing scope worker:report",
        )

    # Plan 9 audit fix: verify the trial exists and matches the team
    # the worker claims to be acting on behalf of. Without this, a
    # buggy/compromised worker could mint step tokens for fictional
    # trial_ids and the Gateway would silently attribute orphan
    # llm_calls rows.
    #
    # Issue #72: also pull `provider_connection_id` from the trial row
    # so the JWT scope binds the bearer to one specific connection.
    # The worker doesn't supply it — the CP is the source of truth
    # (defense against a compromised worker forging an unrelated
    # connection_id and routing through another team's credentials).
    async with request.app.state.session_factory() as session:
        trial_row = (await session.execute(
            select(TrialRow.team_id, TrialRow.provider_connection_id)
            .where(TrialRow.id == payload.trial_id),
        )).one_or_none()
    if trial_row is None:
        raise HTTPException(
            status_code=404, detail=f"trial {payload.trial_id} not found",
        )
    trial_team, trial_provider_connection_id = trial_row
    if trial_team != payload.team_id:
        raise HTTPException(
            status_code=400,
            detail=f"team_id {payload.team_id} does not own trial "
                   f"{payload.trial_id}",
        )

    token = mint_step_jwt(
        team_id=payload.team_id,
        trial_id=payload.trial_id,
        step_id=payload.step_id,
        ttl_sec=payload.ttl_sec,
        signing_key=signing_key,
        provider_connection_id=trial_provider_connection_id,
    )
    expires_at = datetime.now(UTC) + timedelta(seconds=payload.ttl_sec)
    return {
        "token": token,
        "expires_at": expires_at.isoformat(),
    }
