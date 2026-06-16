"""Validate a `provider_connection_id` on a submitted Trial / Batch
payload.

The Trial / Batch routes accept an optional `provider_connection_id`.
The user supplies a UUID; the route must check that:

1. The UUID parses (handled by Pydantic before this is called).
2. The row exists.
3. The row belongs to the caller's team (cross-team → 400 — NOT 404,
   because the caller chose the id deliberately and a clear error
   helps debugging; the cross-team-leak concern only applies to lookup
   by id, not validation of a user-supplied id).
4. The row is not soft-deleted (deleted_at IS NULL).

Centralized here so the Trial and Batch routes use the same shape +
error messages.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ProviderConnection


async def validate_provider_connection(
    session: AsyncSession,
    provider_connection_id: UUID,
    *,
    team_id: UUID,
) -> None:
    """Raise HTTPException(400) if the connection is missing, soft-
    deleted, or owned by a different team. Returns None on success."""
    row = (await session.execute(
        select(
            ProviderConnection.team_id, ProviderConnection.deleted_at,
        ).where(ProviderConnection.id == provider_connection_id),
    )).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} not found. "
                f"Run `loom providers list` to see what's available."
            ),
        )
    found_team_id, deleted_at = row
    if deleted_at is not None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} has been "
                f"deleted. Re-create with `loom providers create` or "
                f"pick a different one."
            ),
        )
    if found_team_id != team_id:
        # Different message from "not found" — the id was clearly
        # supplied deliberately and we want operators to notice the
        # cross-team mistake quickly. Existence isn't leaked because
        # any non-team caller would already know the id (it came
        # from them).
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection {provider_connection_id} belongs to "
                f"a different team."
            ),
        )
