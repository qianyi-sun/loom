"""Team detail route (spec §5.7).

Returns the team row, its legacy TeamQuota row, API-token metadata
(`members`, token_hash prefix only), and browser-session user memberships
(`user_members`). Raw token secrets are never recoverable.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import select

from loom.db.schema import Team, TeamMembership, TeamQuota, Token, User
from loom_service.auth_guards import (
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


@router.get("/teams/{team_id}")
async def get_team(
    request: Request,
    sc: SessionAndCtx,
    team_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
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
    user_member_rows = (await s.execute(
        select(TeamMembership, User)
        .join(User, User.id == TeamMembership.user_id)
        .where(TeamMembership.team_id == team_id)
        .order_by(User.email.asc(), User.id.asc()),
    )).all()

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
        "user_members": [
            {
                "user_id": str(user.id),
                "email": user.email,
                "display_name": user.display_name,
                "role": membership.role,
                "joined_at": membership.created_at.isoformat(),
            }
            for membership, user in user_member_rows
        ],
    }
