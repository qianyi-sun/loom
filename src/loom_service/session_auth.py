"""Browser session authentication helpers for loom_service (#326)."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, TypedDict
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext, role_scopes
from loom.db.schema import LoginChallenge, Team, TeamMembership, User, UserSession
from loom_service.config import LoomServiceSettings


@dataclass(frozen=True)
class CreatedSession:
    raw_session: str
    raw_csrf: str
    ctx: AuthContext


@dataclass(frozen=True)
class RefreshedSession:
    raw_session: str
    raw_csrf: str


class CookieOptions(TypedDict):
    key: str
    httponly: bool
    secure: bool
    samesite: Literal["lax", "strict", "none"]
    max_age: int
    path: str


def hash_secret(raw: str) -> bytes:
    """Hash raw session, CSRF, and login challenge secrets for storage."""
    return hashlib.sha256(raw.encode()).digest()


def normalize_email(email: str) -> str:
    return email.strip().lower()


def _raw_secret(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def cookie_secure() -> bool:
    return os.environ.get("LOOM_ENV", "").lower() == "production"


def session_cookie_options(settings: LoomServiceSettings) -> CookieOptions:
    return {
        "key": settings.auth_session_cookie_name,
        "httponly": True,
        "secure": cookie_secure(),
        "samesite": "lax",
        "max_age": settings.auth_session_ttl_sec,
        "path": "/",
    }


def verify_csrf(ctx: AuthContext, header_value: str | None) -> None:
    """Require a matching CSRF header for browser-session mutations.

    Bearer-token callers remain exempt because they are not authenticated by
    ambient cookies. Route dependencies call this only on unsafe HTTP methods.
    """
    if ctx.auth_kind != "session":
        return
    if ctx.csrf_hash is None or not header_value:
        raise HTTPException(status_code=403, detail="CSRF token required")
    if not hmac.compare_digest(hash_secret(header_value), ctx.csrf_hash):
        raise HTTPException(status_code=403, detail="CSRF token invalid")


async def create_login_challenge(
    session: AsyncSession,
    *,
    email: str,
    ttl_seconds: int,
) -> str | None:
    """Create a one-time login challenge for an existing user.

    Unknown emails return None so the route can keep the same outward
    response without disclosing account existence.
    """
    normalized = normalize_email(email)
    user = (await session.execute(
        select(User).where(func.lower(User.email) == normalized),
    )).scalar_one_or_none()
    if user is None:
        return None

    raw = _raw_secret("loom_login")
    now = datetime.now(UTC)
    await session.execute(insert(LoginChallenge).values(
        challenge_hash=hash_secret(raw),
        user_id=user.id,
        issued_at=now,
        expires_at=now + timedelta(seconds=ttl_seconds),
    ))
    return raw


async def _first_membership(
    session: AsyncSession, user_id: UUID,
) -> tuple[TeamMembership, Team] | None:
    row = (await session.execute(
        select(TeamMembership, Team)
        .join(Team, Team.id == TeamMembership.team_id)
        .where(TeamMembership.user_id == user_id)
        .order_by(Team.name.asc(), Team.id.asc()),
    )).first()
    if row is None:
        return None
    membership, team = row
    return membership, team


async def _membership_for_team(
    session: AsyncSession, *, user_id: UUID, team_id: UUID,
) -> tuple[TeamMembership, Team] | None:
    row = (await session.execute(
        select(TeamMembership, Team)
        .join(Team, Team.id == TeamMembership.team_id)
        .where(
            TeamMembership.user_id == user_id,
            TeamMembership.team_id == team_id,
        ),
    )).first()
    if row is None:
        return None
    membership, team = row
    return membership, team


def _ctx_from_session(
    *,
    user: User,
    user_session: UserSession,
    role: str,
    team_id: UUID | None,
) -> AuthContext:
    return AuthContext(
        token_hash=b"",
        type="user",
        scopes=role_scopes(role),
        team_id=team_id,
        expires_at=user_session.expires_at,
        user_id=user.id,
        role=role,
        session_hash=user_session.session_hash,
        csrf_hash=user_session.csrf_hash,
        auth_kind="session",
    )


async def consume_login_challenge(
    session: AsyncSession,
    *,
    raw_token: str,
    session_ttl_seconds: int,
) -> CreatedSession:
    now = datetime.now(UTC)
    challenge = (await session.execute(
        select(LoginChallenge).where(
            LoginChallenge.challenge_hash == hash_secret(raw_token),
            LoginChallenge.consumed_at.is_(None),
        ),
    )).scalar_one_or_none()
    if challenge is None or challenge.expires_at < now:
        raise HTTPException(status_code=400, detail="invalid login token")

    user = (await session.execute(
        select(User).where(User.id == challenge.user_id),
    )).scalar_one()
    role = "platform_admin" if user.is_platform_admin else None
    team_id: UUID | None = None
    if role is None:
        first = await _first_membership(session, user.id)
        if first is None:
            raise HTTPException(status_code=403, detail="user has no teams")
        membership, _team = first
        role = membership.role
        team_id = membership.team_id

    raw_session = _raw_secret("loom_session")
    raw_csrf = _raw_secret("loom_csrf")
    session_hash = hash_secret(raw_session)
    user_session = UserSession(
        session_hash=session_hash,
        user_id=user.id,
        current_team_id=team_id,
        csrf_hash=hash_secret(raw_csrf),
        issued_at=now,
        expires_at=now + timedelta(seconds=session_ttl_seconds),
        revoked_at=None,
        last_seen_at=now,
    )
    session.add(user_session)
    await session.execute(
        update(LoginChallenge)
        .where(LoginChallenge.challenge_hash == challenge.challenge_hash)
        .values(consumed_at=now),
    )
    await session.execute(
        update(User).where(User.id == user.id).values(last_login_at=now),
    )
    ctx = _ctx_from_session(
        user=user, user_session=user_session, role=role, team_id=team_id,
    )
    return CreatedSession(raw_session=raw_session, raw_csrf=raw_csrf, ctx=ctx)


async def create_session_for_user(
    session: AsyncSession,
    *,
    user: User,
    session_ttl_seconds: int,
    current_team_id: UUID | None = None,
) -> CreatedSession:
    """Create a browser session after a trusted onboarding action.

    Invite acceptance uses the invite secret as the bootstrap proof. It creates
    or finds the user, creates the membership, then calls this helper so the
    user lands in the invited team without receiving a raw team token.
    """
    now = datetime.now(UTC)
    role = "platform_admin" if user.is_platform_admin else None
    team_id: UUID | None = None
    if current_team_id is not None:
        membership_row = await _membership_for_team(
            session, user_id=user.id, team_id=current_team_id,
        )
    else:
        membership_row = await _first_membership(session, user.id)
    if membership_row is None:
        raise HTTPException(status_code=403, detail="user has no teams")
    membership, _team = membership_row
    team_id = membership.team_id
    if role is None:
        role = membership.role

    raw_session = _raw_secret("loom_session")
    raw_csrf = _raw_secret("loom_csrf")
    user_session = UserSession(
        session_hash=hash_secret(raw_session),
        user_id=user.id,
        current_team_id=team_id,
        csrf_hash=hash_secret(raw_csrf),
        issued_at=now,
        expires_at=now + timedelta(seconds=session_ttl_seconds),
        revoked_at=None,
        last_seen_at=now,
    )
    session.add(user_session)
    await session.execute(
        update(User).where(User.id == user.id).values(last_login_at=now),
    )
    ctx = _ctx_from_session(
        user=user, user_session=user_session, role=role, team_id=team_id,
    )
    return CreatedSession(raw_session=raw_session, raw_csrf=raw_csrf, ctx=ctx)


async def verify_session_cookie(
    session: AsyncSession, raw_cookie: str | None,
) -> AuthContext | None:
    if not raw_cookie:
        return None
    now = datetime.now(UTC)
    row = (await session.execute(
        select(UserSession, User)
        .join(User, User.id == UserSession.user_id)
        .where(UserSession.session_hash == hash_secret(raw_cookie)),
    )).first()
    if row is None:
        return None
    user_session, user = row
    if user_session.revoked_at is not None or user_session.expires_at < now:
        return None

    role = "platform_admin" if user.is_platform_admin else None
    team_id = user_session.current_team_id
    if team_id is None:
        first = await _first_membership(session, user.id)
        if first is None:
            return None
        membership, _team = first
        team_id = membership.team_id
        if role is None:
            role = membership.role
        await session.execute(
            update(UserSession)
            .where(UserSession.session_hash == user_session.session_hash)
            .values(current_team_id=team_id),
        )
    else:
        membership_row = await _membership_for_team(
            session, user_id=user.id, team_id=team_id,
        )
        if membership_row is None:
            return None
        if role is None:
            role = membership_row[0].role
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == user_session.session_hash)
        .values(last_seen_at=now),
    )
    return _ctx_from_session(
        user=user, user_session=user_session, role=role, team_id=team_id,
    )


async def switch_session_team(
    session: AsyncSession, *, ctx: AuthContext, team_id: UUID,
) -> None:
    if ctx.session_hash is None or ctx.user_id is None:
        raise HTTPException(status_code=401, detail="missing browser session")
    if ctx.role == "platform_admin":
        exists = (await session.execute(
            select(Team.id).where(Team.id == team_id),
        )).scalar_one_or_none()
        if exists is None:
            raise HTTPException(status_code=404, detail="team not found")
    else:
        membership = await _membership_for_team(
            session, user_id=ctx.user_id, team_id=team_id,
        )
        if membership is None:
            raise HTTPException(status_code=403, detail="user is not a team member")
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(current_team_id=team_id),
    )


async def refresh_session(
    session: AsyncSession,
    *,
    ctx: AuthContext,
    session_ttl_seconds: int,
) -> RefreshedSession:
    if ctx.session_hash is None:
        raise HTTPException(status_code=401, detail="missing browser session")
    raw_session = _raw_secret("loom_session")
    raw_csrf = _raw_secret("loom_csrf")
    now = datetime.now(UTC)
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(
            session_hash=hash_secret(raw_session),
            csrf_hash=hash_secret(raw_csrf),
            expires_at=now + timedelta(seconds=session_ttl_seconds),
            last_seen_at=now,
        ),
    )
    return RefreshedSession(raw_session=raw_session, raw_csrf=raw_csrf)


async def rotate_csrf_token(session: AsyncSession, ctx: AuthContext) -> str:
    if ctx.session_hash is None:
        raise HTTPException(status_code=401, detail="missing browser session")
    raw_csrf = _raw_secret("loom_csrf")
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(csrf_hash=hash_secret(raw_csrf), last_seen_at=datetime.now(UTC)),
    )
    return raw_csrf


async def revoke_session(session: AsyncSession, ctx: AuthContext) -> None:
    if ctx.session_hash is None:
        return
    await session.execute(
        update(UserSession)
        .where(UserSession.session_hash == ctx.session_hash)
        .values(revoked_at=datetime.now(UTC)),
    )
