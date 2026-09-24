"""Admin audit event read API for issue #10."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import and_, or_, select

from loom.db.schema import AdminAuditEvent
from loom_service.dependencies import AdminSessionAndCtx

router = APIRouter()


def _serialize(row: AdminAuditEvent) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "created_at": row.created_at.isoformat(),
        "actor": row.actor,
        "action": row.action,
        "target_type": row.target_type,
        "target_id": row.target_id,
        "request_id": row.request_id,
        "source_ip_hash": row.source_ip_hash,
        "user_agent_hash": row.user_agent_hash,
        "metadata": row.event_metadata,
    }


@router.get("/admin/audit-events")
async def list_admin_audit_events(
    sc: AdminSessionAndCtx,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[str | None, Query()] = None,
    scope: Literal["all", "access"] = "all",
    actor: Annotated[str | None, Query(max_length=200)] = None,
    action: Annotated[str | None, Query(max_length=200)] = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, Any]:
    session, _ctx = sc
    stmt = select(AdminAuditEvent).order_by(
        AdminAuditEvent.created_at.desc(),
        AdminAuditEvent.id.desc(),
    )
    # Date/time inputs without an offset are interpreted as UTC, matching the UI.
    if start is not None:
        start = start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC)
    if end is not None:
        end = end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC)
    if start is not None and end is not None and start > end:
        raise HTTPException(status_code=400, detail="start must not be after end")
    if scope == "access":
        # Human access emitters use these target types. Managed owner/login
        # events target an environment, so include their explicit access actions.
        stmt = stmt.where(or_(
            AdminAuditEvent.target_type.in_([
                "user", "team", "token", "invite", "team_registration",
                "registration_request", "password_reset_request",
            ]),
            AdminAuditEvent.action.in_([
                "managed_environment.owner.enroll", "managed_environment.owner.revoke",
                "managed_environment.login.issue",
            ]),
        ))
    if actor:
        stmt = stmt.where(AdminAuditEvent.actor.contains(actor, autoescape=True))
    if action:
        stmt = stmt.where(AdminAuditEvent.action.contains(action, autoescape=True))
    if start is not None:
        stmt = stmt.where(AdminAuditEvent.created_at >= start)
    if end is not None:
        stmt = stmt.where(AdminAuditEvent.created_at <= end)
    if cursor is not None:
        try:
            cursor_id = UUID(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid cursor") from exc
        cursor_row = (
            await session.execute(
                select(AdminAuditEvent).where(AdminAuditEvent.id == cursor_id),
            )
        ).scalar_one_or_none()
        if cursor_row is None:
            raise HTTPException(status_code=400, detail="invalid cursor")
        stmt = stmt.where(
            or_(
                AdminAuditEvent.created_at < cursor_row.created_at,
                and_(
                    AdminAuditEvent.created_at == cursor_row.created_at,
                    AdminAuditEvent.id < cursor_row.id,
                ),
            )
        )

    rows = (await session.execute(stmt.limit(limit + 1))).scalars().all()
    has_more = len(rows) > limit
    page = rows[:limit]
    return {
        "items": [_serialize(row) for row in page],
        "next_cursor": str(page[-1].id) if has_more and page else None,
    }
