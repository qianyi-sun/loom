"""Team detail route (spec §5.7).

Returns the team row + its TeamQuota row (fair_share_weight, max
attempts, in-flight count, license_allowlist) + member tokens
(token_hash prefix only — the raw secret is never recoverable).

Members are joined from `tokens` filtered to this team. The list
includes revoked tokens; the SPA can filter them client-side.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Team, TeamQuota, Token
from loom_service.auth_guards import (
    require_human_or_admin,
    require_team_or_admin,
)

router = APIRouter()


@router.get("/teams/{team_id}")
async def get_team(
    request: Request,
    team_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        # Cross-team check fires BEFORE the not-found probe so a team
        # token can't enumerate other teams' existence.
        require_team_or_admin(ctx, team_id)

        team = (await s.execute(
            select(Team).where(Team.id == team_id),
        )).scalar_one_or_none()
        if team is None:
            raise HTTPException(status_code=404, detail="team not found")

        quota = (await s.execute(
            select(TeamQuota).where(TeamQuota.team_id == team_id),
        )).scalar_one_or_none()

        members = (await s.execute(
            select(Token).where(Token.team_id == team_id)
            .order_by(Token.issued_at.desc()),
        )).scalars().all()

    quota_payload: dict[str, Any] | None = None
    if quota is not None:
        quota_payload = {
            "fair_share_weight": float(quota.fair_share_weight),
            "max_attempts": quota.max_attempts,
            "in_flight_count": quota.in_flight_count,
            "license_allowlist": list(quota.license_allowlist),
        }

    return {
        "id": str(team.id),
        "name": team.name,
        "created_at": team.created_at.isoformat(),
        "quota": quota_payload,
        "members": [
            {
                "token_hash_prefix": m.token_hash.hex()[:8],
                "type": m.type,
                "scopes": list(m.scopes),
                "issued_at": m.issued_at.isoformat(),
                "expires_at": (
                    m.expires_at.isoformat() if m.expires_at else None
                ),
                "revoked_at": (
                    m.revoked_at.isoformat() if m.revoked_at else None
                ),
                "last_seen_at": (
                    m.last_seen_at.isoformat() if m.last_seen_at else None
                ),
            }
            for m in members
        ],
    }
