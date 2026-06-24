"""Public team registration and admin approval routes for issue #10."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PendingTeamRegistration, Team, TeamInvite
from loom_service.admin_audit import (
    hash_optional,
    require_admin_actor,
    write_admin_audit_event,
)
from loom_service.dependencies import AdminSessionAndCtx
from loom_service.routes.invites import (
    _invite_link,
    _mint_invite_code,
    _serialize_invite,
)

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


class _ApproveRegistrationReq(BaseModel):
    team_id: UUID
    role: Literal["owner", "member", "viewer"] = "member"


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
    contact_email: str,
) -> bool:
    normalized_email = contact_email.lower()
    return (await session.execute(
        select(PendingTeamRegistration.id)
        .where(func.lower(PendingTeamRegistration.contact_email) == normalized_email)
        .where(PendingTeamRegistration.status.in_(_ACTIVE_REGISTRATION_STATUSES))
        .limit(1),
    )).scalar_one_or_none() is not None


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
            contact_email=payload.contact_email,
        )
        if active_registration:
            raise HTTPException(
                status_code=409,
                detail="contact email already has an active registration",
            )

        now = datetime.now(UTC)
        registration = PendingTeamRegistration(
            id=uuid4(),
            name=payload.name,
            contact_email=payload.contact_email.lower(),
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
                detail="contact email already has an active registration",
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
    payload: _ApproveRegistrationReq,
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
    team = (await session.execute(
        select(Team)
        .where(Team.id == payload.team_id)
        .with_for_update(),
    )).scalar_one_or_none()
    if team is None:
        raise HTTPException(status_code=404, detail="team not found")

    now = datetime.now(UTC)
    raw_invite, code_hash, code_prefix = _mint_invite_code()
    invite = TeamInvite(
        id=uuid4(),
        team_id=team.id,
        email=registration.contact_email.lower(),
        allowed_domain=None,
        role=payload.role,
        status="pending",
        code_hash=code_hash,
        code_prefix=code_prefix,
        max_uses=1,
        accepted_uses=0,
        created_by_actor=actor,
        created_by_user_id=None,
        created_at=now,
        expires_at=now + timedelta(days=14),
        last_sent_at=now,
    )
    session.add(invite)
    registration.status = "approved"
    registration.reviewed_at = now
    registration.reviewed_by_actor = actor
    registration.approved_team_id = team.id
    await write_admin_audit_event(
        session,
        actor=actor,
        action="team_registration.approve",
        target_type="team_registration",
        target_id=str(registration.id),
        request=request,
        metadata={
            "team_id": str(team.id),
            "team_name": team.name,
            "invite_prefix": invite.code_prefix,
            "invite_role": invite.role,
        },
    )
    await session.commit()

    return {
        "registration": _serialize_registration(registration),
        "team": {"id": str(team.id), "name": team.name},
        "invite": _serialize_invite(invite, team=team, now=now),
        "invite_code": raw_invite,
        "invite_link": _invite_link(request, raw_invite),
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
