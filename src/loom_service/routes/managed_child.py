"""Owner enrollment and opaque one-use login proof, only in a bound child stack."""

from __future__ import annotations

import secrets
from datetime import timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import LoginChallenge, Team, TeamMembership, User
from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_service.admin_audit import write_admin_audit_event
from loom_service.dependencies import AdminSessionAndCtx
from loom_service.session_auth import hash_secret

router = APIRouter(prefix="/admin/managed-environment", tags=["managed-environment"])


class ChildIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment_id: UUID
    incarnation: UUID


def _binding(request: Request, payload: ChildIdentity, ctx: AuthContext) -> EnvironmentRegistrationV1:
    if ctx.type != "admin" or ctx.auth_kind != "bearer":
        raise HTTPException(403, "singleton admin bearer required")
    binding = getattr(request.app.state, "managed_environment", None)
    if not isinstance(binding, EnvironmentRegistrationV1):
        raise HTTPException(503, "managed child is not configured")
    if (binding.environment_id, binding.incarnation) != (payload.environment_id, payload.incarnation):
        raise HTTPException(409, "managed child identity mismatch")
    return binding


async def _lock(session: AsyncSession, row: EnvironmentRegistrationV1) -> None:
    # Serializes enrollment and issuance even before the owner/team rows exist.
    await session.execute(select(func.pg_advisory_xact_lock(row.environment_id.int % (2**63))))


async def _owner(session: AsyncSession, row: EnvironmentRegistrationV1) -> User:
    user = await session.get(User, row.owner_user_id, with_for_update=True)
    team = await session.get(Team, row.owner_team_id, with_for_update=True)
    membership = await session.scalar(select(TeamMembership).where(
        TeamMembership.user_id == row.owner_user_id, TeamMembership.team_id == row.owner_team_id,
    ))
    if (user is None or team is None or membership is None or membership.role != "owner"
            or user.status != "active" or user.disabled_at is not None or user.is_platform_admin
            or team.disabled_at is not None):
        raise HTTPException(403, "managed child owner unavailable")
    return user


def _identity(row: EnvironmentRegistrationV1) -> dict[str, str]:
    return {"environment_id": str(row.environment_id), "incarnation": str(row.incarnation),
            "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
            "origin": "https://" + row.public_host}


@router.get("/owner")
async def read_owner(
    request: Request, response: Response, environment_id: UUID, incarnation: UUID, sc: AdminSessionAndCtx,
) -> dict[str, str]:
    session, ctx = sc
    row = _binding(request, ChildIdentity(environment_id=environment_id, incarnation=incarnation), ctx)
    await _owner(session, row)
    await session.commit()
    response.headers["Cache-Control"] = "no-store"
    return _identity(row)


@router.post("/owner")
async def enroll_owner(request: Request, response: Response, payload: ChildIdentity, sc: AdminSessionAndCtx) -> dict[str, str]:
    session, ctx = sc
    row = _binding(request, payload, ctx)
    await _lock(session, row)
    if await session.get(User, row.owner_user_id) is None:
        assert row.owner_user_id is not None
        name = "owner-" + row.owner_user_id.hex
        session.add(User(id=row.owner_user_id, username=name, username_normalized=name,
                         display_name=row.slug, status="active", is_platform_admin=False))
    if await session.get(Team, row.owner_team_id) is None:
        session.add(Team(id=row.owner_team_id, name="Development " + row.slug))
    await session.flush()
    membership = await session.scalar(select(TeamMembership).where(
        TeamMembership.user_id == row.owner_user_id, TeamMembership.team_id == row.owner_team_id,
    ))
    if membership is None:
        session.add(TeamMembership(user_id=row.owner_user_id, team_id=row.owner_team_id, role="owner"))
    await session.flush()
    await _owner(session, row)
    await write_admin_audit_event(session, actor="managed-environment:" + str(row.environment_id), action="managed_environment.owner.enroll",
                                 target_type="environment", target_id=str(row.environment_id), request=request,
                                 metadata={"incarnation": str(row.incarnation), "owner_user_id": str(row.owner_user_id)})
    await session.commit()
    response.headers["Cache-Control"] = "no-store"
    return _identity(row)


@router.post("/login")
async def issue_owner_login(request: Request, response: Response, payload: ChildIdentity, sc: AdminSessionAndCtx) -> dict[str, Any]:
    session, ctx = sc
    row = _binding(request, payload, ctx)
    await _lock(session, row)
    user = await _owner(session, row)
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    raw = "loom_env_login_" + secrets.token_urlsafe(32)
    session.add(LoginChallenge(challenge_hash=hash_secret(raw), user_id=user.id, issued_at=now,
                               expires_at=now + timedelta(seconds=90)))
    await write_admin_audit_event(session, actor="managed-environment:" + str(row.environment_id), action="managed_environment.login.issue",
                                 target_type="environment", target_id=str(row.environment_id), request=request,
                                 metadata={"incarnation": str(row.incarnation), "owner_user_id": str(user.id)})
    await session.commit()
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return {**_identity(row), "login_token": raw, "expires_in": 90}
