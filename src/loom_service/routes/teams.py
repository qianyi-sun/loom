"""Team detail route (spec §5.7).

Returns the team row, its legacy TeamQuota row, API-token metadata
(`members`, token_hash prefix only), and browser-session user memberships
(`user_members`). Raw token secrets are never recoverable.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Team, TeamMembership, TeamQuota, Token, User
from loom_service.admin_audit import require_admin_actor, write_admin_audit_event
from loom_service.auth_guards import (
    require_team_or_admin,
)
from loom_service.dependencies import AdminSessionAndCtx, SessionAndCtx
from loom_service.metrics import TEAM_EMERGENCY_ACTIONS_TOTAL

router = APIRouter()


class _TeamControlRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=500)

    @field_validator("reason", mode="before")
    @classmethod
    def _strip_reason(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class _AdminTeamCreateRequest(BaseModel):
    name: str = Field(min_length=2, max_length=128)

    @field_validator("name", mode="before")
    @classmethod
    def _strip_name(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


class _AdminTeamUpdateRequest(BaseModel):
    name: str = Field(min_length=2, max_length=128)

    @field_validator("name", mode="before")
    @classmethod
    def _strip_name(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


async def _load_team(session: AsyncSession, team_id: UUID) -> Team:
    team = (await session.execute(
        select(Team).where(Team.id == team_id),
    )).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="team not found")
    return team


async def _team_name_exists(
    session: AsyncSession,
    *,
    name: str,
    exclude_team_id: UUID | None = None,
) -> bool:
    query = select(Team.id).where(func.lower(Team.name) == name.lower())
    if exclude_team_id is not None:
        query = query.where(Team.id != exclude_team_id)
    return (await session.execute(query.limit(1))).scalar_one_or_none() is not None


async def _serialize_team(session: AsyncSession, team: Team) -> dict[str, Any]:
    quota = (await session.execute(
        select(TeamQuota).where(TeamQuota.team_id == team.id),
    )).scalar_one_or_none()

    members = (await session.execute(
        select(Token).where(Token.team_id == team.id)
        .order_by(Token.issued_at.desc()),
    )).scalars().all()
    user_member_rows = (await session.execute(
        select(TeamMembership, User)
        .join(User, User.id == TeamMembership.user_id)
        .where(TeamMembership.team_id == team.id)
        .order_by(User.email.asc(), User.id.asc()),
    )).all()

    quota_payload: dict[str, Any] | None = None
    if quota is not None:
        quota_payload = {
            "fair_share_weight": float(quota.fair_share_weight),
            "max_attempts_ceiling": quota.max_attempts_ceiling,
            "in_flight_count": quota.in_flight_count,
            "license_allowlist": list(quota.license_allowlist),
        }

    return {
        "id": str(team.id),
        "name": team.name,
        "created_at": team.created_at.isoformat(),
        "disabled_at": (
            team.disabled_at.isoformat() if team.disabled_at else None
        ),
        "disabled_reason": team.disabled_reason,
        "submissions_paused_at": (
            team.submissions_paused_at.isoformat()
            if team.submissions_paused_at else None
        ),
        "submissions_paused_reason": team.submissions_paused_reason,
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


async def _set_team_control(
    *,
    request: Request,
    session: AsyncSession,
    actor: str,
    team_id: UUID,
    action: str,
    payload: _TeamControlRequest | None,
) -> dict[str, Any]:
    team = await _load_team(session, team_id)
    now = datetime.now(UTC)
    reason = payload.reason if payload else None

    metric_action = action.replace(".", "_")
    if action == "disable":
        team.disabled_at = now
        team.disabled_reason = reason
        audit_action = "team.disable"
    elif action == "enable":
        team.disabled_at = None
        team.disabled_reason = None
        audit_action = "team.enable"
    elif action == "pause_submissions":
        team.submissions_paused_at = now
        team.submissions_paused_reason = reason
        audit_action = "team.submissions.pause"
    elif action == "resume_submissions":
        team.submissions_paused_at = None
        team.submissions_paused_reason = None
        audit_action = "team.submissions.resume"
    else:  # pragma: no cover - internal caller bug
        raise RuntimeError(f"unknown team control action: {action}")

    await session.flush()
    await write_admin_audit_event(
        session,
        actor=actor,
        action=audit_action,
        target_type="team",
        target_id=str(team_id),
        request=request,
        metadata={
            "reason_present": bool(reason),
        },
    )
    TEAM_EMERGENCY_ACTIONS_TOTAL.labels(action=metric_action).inc()
    await session.commit()
    return await _serialize_team(session, team)


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
    team = await _load_team(s, team_id)
    return await _serialize_team(s, team)


@router.get("/admin/teams")
async def list_admin_teams(sc: AdminSessionAndCtx) -> dict[str, list[dict[str, Any]]]:
    session, _ctx = sc
    rows = (await session.execute(
        select(Team).order_by(func.lower(Team.name).asc(), Team.id.asc()),
    )).scalars().all()
    return {"items": [await _serialize_team(session, team) for team in rows]}


@router.post("/admin/teams", status_code=status.HTTP_201_CREATED)
async def create_admin_team(
    request: Request,
    sc: AdminSessionAndCtx,
    payload: _AdminTeamCreateRequest,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, _ctx = sc
    actor = require_admin_actor(x_loom_admin_actor)
    if await _team_name_exists(session, name=payload.name):
        raise HTTPException(status_code=409, detail="team name already exists")

    now = datetime.now(UTC)
    team = Team(id=uuid4(), name=payload.name, created_at=now)
    session.add(team)
    await session.flush()
    # #401: seed the admin ceiling from the schema-driven default so operators
    # can retune per environment without touching code or the DB default.
    settings = request.app.state.settings
    session.add(TeamQuota(
        team_id=team.id,
        max_attempts_ceiling=settings.team_quota_max_attempts_ceiling_default,
    ))
    await write_admin_audit_event(
        session,
        actor=actor,
        action="team.create",
        target_type="team",
        target_id=str(team.id),
        request=request,
        metadata={"team_name": team.name},
    )
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="team name already exists") from exc
    return await _serialize_team(session, team)


@router.patch("/admin/teams/{team_id}")
async def update_admin_team(
    request: Request,
    sc: AdminSessionAndCtx,
    team_id: UUID,
    payload: _AdminTeamUpdateRequest,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, _ctx = sc
    actor = require_admin_actor(x_loom_admin_actor)
    team = await _load_team(session, team_id)
    if await _team_name_exists(session, name=payload.name, exclude_team_id=team_id):
        raise HTTPException(status_code=409, detail="team name already exists")

    old_name = team.name
    team.name = payload.name
    await write_admin_audit_event(
        session,
        actor=actor,
        action="team.update",
        target_type="team",
        target_id=str(team.id),
        request=request,
        metadata={"old_name": old_name, "new_name": team.name},
    )
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="team name already exists") from exc
    return await _serialize_team(session, team)


@router.post("/admin/teams/{team_id}/disable")
async def disable_team(
    request: Request,
    sc: AdminSessionAndCtx,
    team_id: UUID,
    payload: _TeamControlRequest | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, _ctx = sc
    actor = require_admin_actor(x_loom_admin_actor)
    return await _set_team_control(
        request=request,
        session=session,
        actor=actor,
        team_id=team_id,
        action="disable",
        payload=payload,
    )


@router.post("/admin/teams/{team_id}/enable")
async def enable_team(
    request: Request,
    sc: AdminSessionAndCtx,
    team_id: UUID,
    payload: _TeamControlRequest | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, _ctx = sc
    actor = require_admin_actor(x_loom_admin_actor)
    return await _set_team_control(
        request=request,
        session=session,
        actor=actor,
        team_id=team_id,
        action="enable",
        payload=payload,
    )


@router.post("/admin/teams/{team_id}/pause-submissions")
async def pause_team_submissions(
    request: Request,
    sc: AdminSessionAndCtx,
    team_id: UUID,
    payload: _TeamControlRequest | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, _ctx = sc
    actor = require_admin_actor(x_loom_admin_actor)
    return await _set_team_control(
        request=request,
        session=session,
        actor=actor,
        team_id=team_id,
        action="pause_submissions",
        payload=payload,
    )


@router.post("/admin/teams/{team_id}/resume-submissions")
async def resume_team_submissions(
    request: Request,
    sc: AdminSessionAndCtx,
    team_id: UUID,
    payload: _TeamControlRequest | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    session, _ctx = sc
    actor = require_admin_actor(x_loom_admin_actor)
    return await _set_team_control(
        request=request,
        session=session,
        actor=actor,
        team_id=team_id,
        action="resume_submissions",
        payload=payload,
    )
