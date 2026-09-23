"""Audited platform-admin grant/revoke operations for human accounts (#802)."""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import Team, TeamMembership, Token, User, UserSession
from loom.system_identities import PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID
from loom_service.admin_audit import actor_from_context, write_admin_audit_event
from loom_service.dependencies import AdminSessionAndCtx

router = APIRouter()


class GrantRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ensure_admin_team: bool = True


class RevokeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # No implicit credential policy: clients must acknowledge the logout.
    credential_policy: Literal["revoke_all"]


async def _change_platform_admin(
    user_id: UUID,
    *,
    grant: bool,
    ensure_admin_team: bool,
    request: Request,
    sc: tuple[AsyncSession, AuthContext],
    fallback_actor: str | None,
) -> dict[str, Any]:
    session, ctx = sc
    if ctx.user_id is not None:
        actor_user = (await session.execute(
            select(User).where(User.id == ctx.user_id),
        )).scalar_one_or_none()
        if (
            actor_user is None or actor_user.status != "active"
            or actor_user.disabled_at is not None or not actor_user.is_platform_admin
        ):
            raise HTTPException(status_code=403, detail="active platform admin required")
    actor = await actor_from_context(session, ctx, fallback_actor)
    # A full primary key is the only selector: never resolve names or emails
    # with first()/LIMIT 1, which could promote the wrong human.
    user = (await session.execute(
        select(User).where(User.id == user_id).with_for_update()
        .execution_options(populate_existing=True),
    )).scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    if user_id == PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID:
        raise HTTPException(status_code=403, detail="deployment-only user")
    if user.status != "active" or user.disabled_at is not None:
        raise HTTPException(status_code=409, detail="user is disabled or inactive")

    teams = (await session.execute(
        select(Team).where(func.lower(Team.name) == "admin").with_for_update(),
    )).scalars().all()
    if len(teams) > 1:
        raise HTTPException(status_code=409, detail="reserved admin team is ambiguous")
    team = teams[0] if teams else None
    if grant and ensure_admin_team and (team is None or team.disabled_at is not None):
        raise HTTPException(status_code=409, detail="one enabled reserved admin team is required")
    membership = None
    if team is not None:
        membership = (await session.execute(
            select(TeamMembership).where(
                TeamMembership.user_id == user_id, TeamMembership.team_id == team.id,
            ).with_for_update(),
        )).scalar_one_or_none()
    before = {
        "is_platform_admin": user.is_platform_admin,
        "admin_team_role": membership.role if membership else None,
    }
    user.is_platform_admin = grant
    admin_team_role = before["admin_team_role"]
    revoked_sessions = revoked_tokens = 0
    if grant and ensure_admin_team:
        assert team is not None
        if membership is None:
            session.add(TeamMembership(team_id=team.id, user_id=user_id, role="owner"))
        else:
            membership.role = "owner"
        admin_team_role = "owner"
    elif not grant:
        if membership is not None:
            await session.delete(membership)
        admin_team_role = None
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        revoked_sessions = len((await session.execute(
            update(UserSession).where(
                UserSession.user_id == user_id, UserSession.revoked_at.is_(None),
            ).values(revoked_at=now).returning(UserSession.session_hash),
        )).all())
        revoked_tokens = len((await session.execute(
            update(Token).where(
                Token.created_by_user_id == user_id, Token.revoked_at.is_(None),
            ).values(revoked_at=now).returning(Token.token_hash),
        )).all())
    after = {"is_platform_admin": grant, "admin_team_role": admin_team_role}
    metadata = {
        "actor_user_id": str(ctx.user_id) if ctx.user_id else None,
        "before": before,
        "after": after,
        "admin_team_id": str(team.id) if team else None,
        "credential_policy": "preserve" if grant else "revoke_all",
        "revoked_sessions": revoked_sessions,
        "revoked_user_tokens": revoked_tokens,
        "changed": before != after or bool(revoked_sessions or revoked_tokens),
    }
    await write_admin_audit_event(
        session, actor=actor,
        action=f"user.platform_admin.{'grant' if grant else 'revoke'}",
        target_type="user", target_id=str(user_id), request=request, metadata=metadata,
    )
    # Audit and all authority/credential changes succeed or roll back together.
    await session.commit()
    return {"user_id": str(user_id), "username": user.username, **metadata}


@router.post("/admin/users/{user_id}/platform-admin/grant")
async def grant_platform_admin(
    user_id: UUID, body: GrantRequest, request: Request, sc: AdminSessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _change_platform_admin(
        user_id, grant=True, ensure_admin_team=body.ensure_admin_team,
        request=request, sc=sc, fallback_actor=x_loom_admin_actor,
    )


@router.post("/admin/users/{user_id}/platform-admin/revoke")
async def revoke_platform_admin(
    user_id: UUID, body: RevokeRequest, request: Request, sc: AdminSessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _change_platform_admin(
        user_id, grant=False, ensure_admin_team=False,
        request=request, sc=sc, fallback_actor=x_loom_admin_actor,
    )
