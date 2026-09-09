"""Permanent attempt fencing shared by builder and publication admission."""

from uuid import UUID

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TaskImageAttemptRetention


class RetirementAdmissionTransactionError(RuntimeError):
    """Admission cannot establish a fresh retirement read under its parent fence."""


async def attempt_is_retired(session: AsyncSession, *, attempt_id: UUID) -> bool:
    """Read fresh scalar state without acquiring a later retention-row lock.

    Caller must hold the materialization parent lock used by the retirement
    writer (normally also the exact attempt lock). That fence serializes the
    marker check with retirement only in an enclosing READ COMMITTED transaction.
    Parent FOR UPDATE assigns an XID; AUTOCOMMIT loses it before this SELECT.
    Checking that ID rejects released fences, but cannot prove exact lock ownership.
    This lookup is not a substitute for ownership,
    reference eligibility, inventory validation, or execution-start admission.
    """
    isolation, transaction_id, retired = (
        await session.execute(
            select(
                func.pg_catalog.current_setting("transaction_isolation"),
                func.pg_catalog.pg_current_xact_id_if_assigned(),
                exists().where(
                    TaskImageAttemptRetention.attempt_id == attempt_id,
                    TaskImageAttemptRetention.retired_at.is_not(None),
                )
            )
        )
    ).one()
    if isolation != "read committed":
        raise RetirementAdmissionTransactionError("retirement admission requires READ COMMITTED")
    if transaction_id is None:
        raise RetirementAdmissionTransactionError("retirement admission requires a locked transaction")
    return bool(retired)
