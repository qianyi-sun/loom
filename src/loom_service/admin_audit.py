"""Helpers for durable admin audit events."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import AdminAuditEvent

_SECRET_MARKERS = (
    "loom_admin_",
    "loom_api_",
    "loom_invite_",
    "loom_team_",
    "loom_w_",
    "sk-",
)


def hash_optional(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode()).hexdigest()


def require_admin_actor(actor: str | None) -> str:
    cleaned = actor.strip() if actor else ""
    if not cleaned:
        raise HTTPException(
            status_code=400,
            detail="X-Loom-Admin-Actor header is required",
        )
    if len(cleaned) > 128:
        raise HTTPException(
            status_code=400,
            detail="X-Loom-Admin-Actor must be at most 128 characters",
        )
    return cleaned


def _metadata_contains_secret(value: object) -> bool:
    if isinstance(value, str):
        return any(marker in value for marker in _SECRET_MARKERS)
    if isinstance(value, Mapping):
        return any(_metadata_contains_secret(v) for v in value.values())
    if isinstance(value, list | tuple):
        return any(_metadata_contains_secret(v) for v in value)
    return False


async def write_admin_audit_event(
    session: AsyncSession,
    *,
    actor: str,
    action: str,
    target_type: str,
    target_id: str,
    request: Request | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    safe_metadata = metadata or {}
    if _metadata_contains_secret(safe_metadata):
        raise ValueError("admin audit metadata contains secret-looking value")

    request_id = None
    source_ip_hash = None
    user_agent_hash = None
    if request is not None:
        request_id = request.headers.get("x-request-id")
        source_ip_hash = hash_optional(
            request.client.host if request.client else None,
        )
        user_agent_hash = hash_optional(request.headers.get("user-agent"))

    session.add(AdminAuditEvent(
        id=uuid4(),
        actor=actor,
        action=action,
        target_type=target_type,
        target_id=target_id,
        request_id=request_id,
        source_ip_hash=source_ip_hash,
        user_agent_hash=user_agent_hash,
        event_metadata=safe_metadata,
    ))
