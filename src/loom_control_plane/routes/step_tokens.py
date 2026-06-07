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

from loom.auth import mint_step_jwt, verify_bearer_token

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

    token = mint_step_jwt(
        team_id=payload.team_id,
        trial_id=payload.trial_id,
        step_id=payload.step_id,
        ttl_sec=payload.ttl_sec,
        signing_key=signing_key,
    )
    expires_at = datetime.now(UTC) + timedelta(seconds=payload.ttl_sec)
    return {
        "token": token,
        "expires_at": expires_at.isoformat(),
    }
