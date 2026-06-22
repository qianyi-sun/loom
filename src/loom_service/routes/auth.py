"""Browser user session routes for invite-only public Loom (#326)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import Team, TeamMembership, User
from loom_service.dependencies import SessionAndCtx
from loom_service.session_auth import (
    consume_login_challenge,
    create_login_challenge,
    refresh_session,
    revoke_session,
    rotate_csrf_token,
    session_cookie_options,
    switch_session_team,
    verify_session_cookie,
)

router = APIRouter(prefix="/auth", tags=["auth"])


class _LoginStartReq(BaseModel):
    email: str = Field(min_length=3, max_length=320)

    @field_validator("email", mode="before")
    @classmethod
    def _strip(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("email")
    @classmethod
    def _require_email_shape(cls, value: str) -> str:
        if "@" not in value or value.startswith("@") or value.endswith("@"):
            raise ValueError("email must look like an email address")
        return value


class _LoginCompleteReq(BaseModel):
    token: str = Field(min_length=16)


class _SwitchTeamReq(BaseModel):
    team_id: UUID


async def _require_user(session: AsyncSession, ctx: AuthContext) -> User:
    if ctx.type != "user" or ctx.user_id is None:
        raise HTTPException(status_code=403, detail="browser session required")
    user = (await session.execute(
        select(User).where(User.id == ctx.user_id),
    )).scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=401, detail="missing user")
    return user


async def _serialize_me(
    session: AsyncSession,
    ctx: AuthContext,
    *,
    csrf_token: str | None = None,
) -> dict[str, Any]:
    user = await _require_user(session, ctx)

    if user.is_platform_admin:
        team_rows = (await session.execute(
            select(Team).order_by(Team.name.asc(), Team.id.asc()),
        )).scalars().all()
        teams = [
            {"id": str(team.id), "name": team.name, "role": "platform_admin"}
            for team in team_rows
        ]
    else:
        rows = (await session.execute(
            select(TeamMembership, Team)
            .join(Team, Team.id == TeamMembership.team_id)
            .where(TeamMembership.user_id == user.id)
            .order_by(Team.name.asc(), Team.id.asc()),
        )).all()
        teams = [
            {"id": str(team.id), "name": team.name, "role": membership.role}
            for membership, team in rows
        ]

    current_team = None
    if ctx.team_id is not None:
        for team in teams:
            if team["id"] == str(ctx.team_id):
                current_team = team
                break

    payload: dict[str, Any] = {
        "user": {
            "id": str(user.id),
            "email": user.email,
            "display_name": user.display_name,
            "is_platform_admin": user.is_platform_admin,
        },
        "teams": teams,
        "current_team": current_team,
        "role": ctx.role,
        "scopes": list(ctx.scopes),
        "is_platform_admin": user.is_platform_admin,
    }
    if csrf_token is not None:
        payload["csrf_token"] = csrf_token
    return payload


def _set_auth_cookies(
    response: Response, request: Request, *, raw_session: str,
) -> None:
    settings = request.app.state.settings
    response.set_cookie(
        value=raw_session, **session_cookie_options(settings),
    )


def _clear_auth_cookies(response: Response, request: Request) -> None:
    settings = request.app.state.settings
    response.delete_cookie(settings.auth_session_cookie_name, path="/")
    response.delete_cookie(settings.auth_csrf_cookie_name, path="/")


@router.post("/login/start")
async def login_start(
    request: Request, payload: _LoginStartReq,
) -> dict[str, Any]:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as session:
        raw = await create_login_challenge(
            session,
            email=payload.email,
            ttl_seconds=settings.auth_login_challenge_ttl_sec,
        )
        await session.commit()
    body: dict[str, Any] = {"status": "sent"}
    if settings.auth_return_login_token and raw is not None:
        body["login_token"] = raw
    return body


@router.post("/login/complete")
async def login_complete(
    request: Request, response: Response, payload: _LoginCompleteReq,
) -> dict[str, Any]:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as session:
        created = await consume_login_challenge(
            session,
            raw_token=payload.token,
            session_ttl_seconds=settings.auth_session_ttl_sec,
        )
        await session.flush()
        body = await _serialize_me(
            session, created.ctx, csrf_token=created.raw_csrf,
        )
        await session.commit()
    _set_auth_cookies(
        response, request,
        raw_session=created.raw_session,
    )
    return body


@router.get("/me")
async def me(sc: SessionAndCtx) -> dict[str, Any]:
    session, ctx = sc
    csrf_token = await rotate_csrf_token(session, ctx)
    await session.commit()
    return await _serialize_me(session, ctx, csrf_token=csrf_token)


@router.get("/whoami")
async def whoami(sc: SessionAndCtx) -> dict[str, Any]:
    session, ctx = sc
    team_name = None
    if ctx.team_id is not None:
        team = (await session.execute(
            select(Team).where(Team.id == ctx.team_id),
        )).scalar_one_or_none()
        team_name = team.name if team is not None else None

    await session.commit()
    return {
        "auth_kind": ctx.auth_kind,
        "principal_type": ctx.type,
        "team_id": str(ctx.team_id) if ctx.team_id else None,
        "team_name": team_name,
        "role": ctx.role,
        "scopes": list(ctx.scopes),
        "token_prefix": (
            ctx.token_hash.hex()[:8] if ctx.token_hash else None
        ),
        "expires_at": (
            ctx.expires_at.isoformat() if ctx.expires_at else None
        ),
    }


@router.post("/team")
async def switch_team(
    request: Request, payload: _SwitchTeamReq, sc: SessionAndCtx,
) -> dict[str, Any]:
    session, ctx = sc
    await switch_session_team(session, ctx=ctx, team_id=payload.team_id)
    await session.flush()
    refreshed = await verify_session_cookie(
        session, request.cookies.get(request.app.state.settings.auth_session_cookie_name),
    )
    if refreshed is None:
        raise HTTPException(status_code=401, detail="missing or invalid session")
    csrf_token = await rotate_csrf_token(session, refreshed)
    await session.commit()
    return await _serialize_me(session, refreshed, csrf_token=csrf_token)


@router.post("/refresh")
async def refresh(
    request: Request, response: Response, sc: SessionAndCtx,
) -> dict[str, Any]:
    session, ctx = sc
    settings = request.app.state.settings
    refreshed_tokens = await refresh_session(
        session, ctx=ctx, session_ttl_seconds=settings.auth_session_ttl_sec,
    )
    await session.commit()
    _set_auth_cookies(
        response, request, raw_session=refreshed_tokens.raw_session,
    )
    refreshed = await verify_session_cookie(session, refreshed_tokens.raw_session)
    if refreshed is None:
        raise HTTPException(status_code=401, detail="missing or invalid session")
    return await _serialize_me(
        session, refreshed, csrf_token=refreshed_tokens.raw_csrf,
    )


@router.post("/logout", status_code=204)
async def logout(
    request: Request, response: Response, sc: SessionAndCtx,
) -> None:
    session, ctx = sc
    await revoke_session(session, ctx)
    await session.commit()
    _clear_auth_cookies(response, request)
