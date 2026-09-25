"""Both management models share one cluster allowance, not per-owner budgets."""
from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_operation_schema import NebiusApplicationReservation
from loom.db.nebius_environment_schema import NebiusPlatformReservation

ENVELOPE_FIELDS = ("cpu_millis", "memory_mib", "storage_mib", "ephemeral_storage_mib")


async def platform_usage(session: AsyncSession, cluster_id: str) -> dict[str, int]:
    """Caller holds the NebiusPlatformBudget row lock through its mutation."""
    totals = dict.fromkeys(ENVELOPE_FIELDS, 0)
    for model in (NebiusPlatformReservation, NebiusApplicationReservation):
        used = (await session.execute(select(*[
            func.coalesce(func.sum(getattr(model, name)), 0).label(name) for name in ENVELOPE_FIELDS
        ]).where(model.cluster_id == cluster_id))).mappings().one()
        for name in ENVELOPE_FIELDS:
            totals[name] += int(used[name])
    return totals
