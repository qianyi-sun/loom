"""Public team registration and admin approval routes for issue #10."""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PendingTeamRegistration, Team, TeamQuota, Token
from loom_service.admin_audit import (
    hash_optional,
    require_admin_actor,
    write_admin_audit_event,
)
from loom_service.dependencies import AdminSessionAndCtx

router = APIRouter()

_ACTIVE_REGISTRATION_STATUSES = ("pending", "approved")
_REGISTRATION_STATUSES = Literal["pending", "approved", "rejected", "expired"]


class _TeamRegistrationReq(BaseModel):
    name: str = Field(min_length=3, max_length=128)
    contact_email: str = Field(min_length=3, max_length=320)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name", "contact_email", mode="before")
    @classmethod
    def _strip(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("contact_email")
    @classmethod
    def _require_email_shape(cls, value: str) -> str:
        if "@" not in value or value.startswith("@") or value.endswith("@"):
            raise ValueError("contact_email must look like an email address")
        return value


class _RejectRegistrationReq(BaseModel):
    reason: str | None = Field(default=None, max_length=500)

    @field_validator("reason", mode="before")
    @classmethod
    def _strip_reason(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


def _hash_prefix(token_hash: bytes) -> str:
    return token_hash.hex()[:8]


def _serialize_registration(row: PendingTeamRegistration) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "name": row.name,
        "contact_email": row.contact_email,
        "status": row.status,
        "requested_at": row.requested_at.isoformat(),
        "reviewed_at": (
            row.reviewed_at.isoformat() if row.reviewed_at else None
        ),
        "reviewed_by_actor": row.reviewed_by_actor,
        "approved_team_id": (
            str(row.approved_team_id) if row.approved_team_id else None
        ),
    }


async def _active_registration_exists(
    session: AsyncSession,
    *,
    name: str,
) -> bool:
    normalized_name = name.lower()
    return (await session.execute(
        select(PendingTeamRegistration.id)
        .where(func.lower(PendingTeamRegistration.name) == normalized_name)
        .where(PendingTeamRegistration.status.in_(_ACTIVE_REGISTRATION_STATUSES))
        .limit(1),
    )).scalar_one_or_none() is not None


async def _team_exists(
    session: AsyncSession,
    *,
    name: str,
) -> bool:
    normalized_name = name.lower()

    return (await session.execute(
        select(Team.id)
        .where(func.lower(Team.name) == normalized_name)
        .limit(1),
    )).scalar_one_or_none() is not None


def _mint_team_token(team_id: UUID) -> tuple[str, bytes, datetime]:
    raw = f"loom_team_{secrets.token_urlsafe(32)}"
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at = datetime.now(UTC) + timedelta(days=365)
    return raw, token_hash, expires_at


@router.post("/teams/register", status_code=202)
async def register_team(
    request: Request,
    payload: _TeamRegistrationReq,
) -> dict[str, Any]:
    settings = request.app.state.settings
    if settings.team_registration_open:
        raise HTTPException(
            status_code=501,
            detail="open team registration requires a challenge hook first",
        )

    async with request.app.state.session_factory() as session:
        active_registration = await _active_registration_exists(
            session,
            name=payload.name,
        )
        if active_registration or await _team_exists(session, name=payload.name):
            raise HTTPException(
                status_code=409,
                detail="team name already has an active registration or team",
            )

        now = datetime.now(UTC)
        registration = PendingTeamRegistration(
            id=uuid4(),
            name=payload.name,
            contact_email=payload.contact_email,
            status="pending",
            requested_at=now,
            source_ip_hash=hash_optional(
                request.client.host if request.client else None,
            ),
            user_agent_hash=hash_optional(request.headers.get("user-agent")),
            request_metadata=payload.metadata,
        )
        session.add(registration)
        try:
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            raise HTTPException(
                status_code=409,
                detail="team name already has an active registration or team",
            ) from exc

    return _serialize_registration(registration)


@router.get("/admin/team-registrations")
async def list_team_registrations(
    sc: AdminSessionAndCtx,
    status: Annotated[_REGISTRATION_STATUSES, Query()] = "pending",
) -> dict[str, list[dict[str, Any]]]:
    session, _ctx = sc
    rows = (await session.execute(
        select(PendingTeamRegistration)
        .where(PendingTeamRegistration.status == status)
        .order_by(PendingTeamRegistration.requested_at.asc()),
    )).scalars().all()
    return {"items": [_serialize_registration(row) for row in rows]}


@router.post("/admin/team-registrations/{registration_id}/approve")
async def approve_team_registration(
    request: Request,
    sc: AdminSessionAndCtx,
    registration_id: UUID,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    actor = require_admin_actor(x_loom_admin_actor)
    session, _ctx = sc
    registration = (await session.execute(
        select(PendingTeamRegistration)
        .where(PendingTeamRegistration.id == registration_id)
        .with_for_update(),
    )).scalar_one_or_none()
    if registration is None:
        raise HTTPException(status_code=404, detail="registration not found")
    if registration.status != "pending":
        raise HTTPException(
            status_code=409,
            detail=f"registration is already {registration.status}",
        )
    if await _team_exists(session, name=registration.name):
        raise HTTPException(
            status_code=409,
            detail="team name already exists",
        )

    team_id = uuid4()
    raw_token, token_hash, expires_at = _mint_team_token(team_id)
    now = datetime.now(UTC)
    team = Team(id=team_id, name=registration.name, created_at=now)
    session.add(team)
    await session.flush()
    session.add(TeamQuota(team_id=team_id))
    session.add(Token(
        token_hash=token_hash,
        type="team",
        scopes=["read:own", "submit"],
        team_id=team_id,
        issued_at=now,
        expires_at=expires_at,
    ))
    registration.status = "approved"
    registration.reviewed_at = now
    registration.reviewed_by_actor = actor
    registration.approved_team_id = team_id
    await write_admin_audit_event(
        session,
        actor=actor,
        action="team_registration.approve",
        target_type="team_registration",
        target_id=str(registration.id),
        request=request,
        metadata={
            "team_id": str(team_id),
            "team_name": team.name,
            "team_token_hash_prefix": _hash_prefix(token_hash),
        },
    )
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="team name already exists") from exc

    return {
        "registration": _serialize_registration(registration),
        "team": {"id": str(team_id), "name": team.name},
        "team_token": raw_token,
        "token_hash_prefix": _hash_prefix(token_hash),
        "expires_at": expires_at.isoformat(),
    }


@router.post("/admin/team-registrations/{registration_id}/reject")
async def reject_team_registration(
    request: Request,
    sc: AdminSessionAndCtx,
    registration_id: UUID,
    payload: _RejectRegistrationReq | None = None,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, Any]:
    actor = require_admin_actor(x_loom_admin_actor)
    session, _ctx = sc
    registration = (await session.execute(
        select(PendingTeamRegistration)
        .where(PendingTeamRegistration.id == registration_id)
        .with_for_update(),
    )).scalar_one_or_none()
    if registration is None:
        raise HTTPException(status_code=404, detail="registration not found")
    if registration.status != "pending":
        raise HTTPException(
            status_code=409,
            detail=f"registration is already {registration.status}",
        )

    metadata = dict(registration.request_metadata)
    if payload is not None and payload.reason:
        metadata["rejection_reason"] = payload.reason
    now = datetime.now(UTC)
    registration.status = "rejected"
    registration.reviewed_at = now
    registration.reviewed_by_actor = actor
    registration.request_metadata = metadata
    await write_admin_audit_event(
        session,
        actor=actor,
        action="team_registration.reject",
        target_type="team_registration",
        target_id=str(registration.id),
        request=request,
        metadata={"reason_present": bool(payload and payload.reason)},
    )
    await session.commit()
    return _serialize_registration(registration)
