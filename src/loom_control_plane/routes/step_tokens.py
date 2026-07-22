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
from loom.db.schema import (
    ProviderConnection,
    ProviderConnectionShare,
)
from loom.db.schema import (
    Trial as TrialRow,
)

router = APIRouter(prefix="/admin")


class _IssueStepTokenRequest(BaseModel):
    team_id: UUID
    trial_id: UUID
    step_id: str = Field(min_length=1, max_length=64)
    ttl_sec: int = Field(gt=0, le=7200)
    # Only the dedicated family-orchestrator credential may set this field.
    # Presence is significant: explicit null means the evolver must use the
    # platform route even when the completed trial used a BYO connection.
    provider_connection_id: UUID | None = None


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
            session,
            authorization,
            signing_key=signing_key,
            allow_family_orchestrator=True,
        )
    explicit_provider = "provider_connection_id" in payload.model_fields_set
    family_evolver_request = payload.step_id == "family_evolver"
    if ctx is None:
        raise HTTPException(
            status_code=403,
            detail="missing step-token scope",
        )
    if family_evolver_request:
        if (
            ctx.type != "family_orchestrator"
            or ctx.team_id is not None
            or ctx.user_id is not None
            or ctx.scopes != ["family:evolve"]
            or not explicit_provider
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "family_evolver step tokens require the dedicated principal and "
                    "an explicit provider_connection_id field"
                ),
            )
    elif explicit_provider or "worker:report" not in ctx.scopes:
        raise HTTPException(
            status_code=403,
            detail="provider override requires the family:evolve scope",
        )

    # Plan 9 audit fix: verify the trial exists and matches the team
    # the worker claims to be acting on behalf of. Without this, a
    # buggy/compromised worker could mint step tokens for fictional
    # trial_ids and the Gateway would silently attribute orphan
    # llm_calls rows.
    #
    # Pull the agent provider from the trial row by default. Only a dedicated
    # family:evolve credential may override this value (explicit null selects
    # the platform route); the CP still authorizes a configured override
    # against the authoritative trial team before minting the JWT.
    async with request.app.state.session_factory() as session:
        trial_row = (
            await session.execute(
                select(TrialRow.team_id, TrialRow.provider_connection_id).where(
                    TrialRow.id == payload.trial_id
                ),
            )
        ).one_or_none()
    if trial_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"trial {payload.trial_id} not found",
        )
    trial_team, trial_provider_connection_id = trial_row
    if trial_team != payload.team_id:
        raise HTTPException(
            status_code=400,
            detail=f"team_id {payload.team_id} does not own trial {payload.trial_id}",
        )

    effective_provider_connection_id = trial_provider_connection_id
    if family_evolver_request:
        effective_provider_connection_id = payload.provider_connection_id
        if effective_provider_connection_id is not None:
            async with request.app.state.session_factory() as provider_session:
                connection_team = (
                    await provider_session.execute(
                        select(ProviderConnection.team_id).where(
                            ProviderConnection.id == effective_provider_connection_id,
                            ProviderConnection.deleted_at.is_(None),
                        ),
                    )
                ).scalar_one_or_none()
                if connection_team is None:
                    raise HTTPException(
                        status_code=404,
                        detail="provider_connection not found",
                    )
                if connection_team != trial_team:
                    shared = (
                        await provider_session.execute(
                            select(ProviderConnectionShare.provider_connection_id).where(
                                ProviderConnectionShare.provider_connection_id
                                == effective_provider_connection_id,
                                ProviderConnectionShare.target_team_id == trial_team,
                            ),
                        )
                    ).scalar_one_or_none()
                    if shared is None:
                        raise HTTPException(
                            status_code=404,
                            detail="provider_connection not found",
                        )

    token = mint_step_jwt(
        team_id=payload.team_id,
        trial_id=payload.trial_id,
        step_id=payload.step_id,
        ttl_sec=payload.ttl_sec,
        signing_key=signing_key,
        provider_connection_id=effective_provider_connection_id,
        provider_connection_id_bound=family_evolver_request,
    )
    expires_at = datetime.now(UTC) + timedelta(seconds=payload.ttl_sec)
    return {
        "token": token,
        "expires_at": expires_at.isoformat(),
    }
