"""Permanent attempt fencing shared by builder and publication admission."""

from uuid import UUID

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TaskImageAttemptRetention


async def attempt_is_retired(session: AsyncSession, *, attempt_id: UUID) -> bool:
    """Read fresh scalar state without acquiring a later retention-row lock.

    Caller must hold the materialization parent lock used by the retirement
    writer (normally also the exact attempt lock). That fence serializes the
    marker check with retirement. This lookup is not a substitute for ownership,
    reference eligibility, inventory validation, or execution-start admission.
    """
    return bool(
        await session.scalar(
            select(
                exists().where(
                    TaskImageAttemptRetention.attempt_id == attempt_id,
                    TaskImageAttemptRetention.retired_at.is_not(None),
                )
            )
        )
    )
