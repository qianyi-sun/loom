"""Bearer token authentication for the Gateway.

Tokens were issued by the Control Plane (Plan 5); the Gateway only validates
them by hash lookup against the shared `tokens` table.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Token


@dataclass(frozen=True)
class AuthContext:
    token_hash: bytes
    type: str
    scopes: list[str]
    team_id: UUID | None
    expires_at: datetime | None


async def verify_bearer_token(
    session: AsyncSession,
    header_value: str | None,
) -> AuthContext | None:
    if not header_value or not header_value.lower().startswith("bearer "):
        return None
    raw = header_value.split(" ", 1)[1].strip()
    if not raw:
        return None

    token_hash = hashlib.sha256(raw.encode()).digest()
    row = (await session.execute(
        select(Token).where(Token.token_hash == token_hash),
    )).scalar_one_or_none()
    if row is None:
        return None
    if row.revoked_at is not None:
        return None
    if row.expires_at is not None and row.expires_at < datetime.now(UTC):
        return None

    return AuthContext(
        token_hash=row.token_hash,
        type=row.type,
        scopes=list(row.scopes),
        team_id=row.team_id,
        expires_at=row.expires_at,
    )
