"""Modal compute-seconds + cost capture.

Uses the provider-neutral ``cloud_compute_records`` schema. Modal's
calculator (:func:`loom.cost.cloud.calc_modal_cost`) is GPU-aware.
"""

from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from loom.cost.cloud import calc_modal_cost
from loom.cost.cloud_records import CloudComputeRecord, persist_record

_6DP = Decimal("0.000001")


def modal_compute_record(
    *,
    team_id: UUID,
    trial_id: UUID,
    sandbox_id: str,
    image: str,
    started_at: datetime,
    stopped_at: datetime,
    cpu: float,
    memory_mb: int,
    gpu: str | None,
) -> CloudComputeRecord:
    """Build a CloudComputeRecord tagged ``cloud_provider='modal'``.

    Cost is computed from Modal's per-SKU rate table (CPU + RAM + GPU)
    times the billed seconds. The GPU spec accepts Modal's multi-GPU
    syntax (e.g. ``"H100:8"``).
    """
    billed_seconds = (stopped_at - started_at).total_seconds()
    dollars = calc_modal_cost(
        billed_seconds=billed_seconds,
        cpu=cpu,
        memory_mb=memory_mb,
        gpu=gpu,
    )
    cost = Decimal(repr(dollars)).quantize(_6DP, rounding=ROUND_HALF_EVEN)
    return CloudComputeRecord(
        team_id=team_id,
        trial_id=trial_id,
        cloud_provider="modal",
        sandbox_id=sandbox_id,
        image=image,
        started_at=started_at,
        stopped_at=stopped_at,
        compute_seconds=billed_seconds,
        cost_usd=cost,
    )


async def persist_modal_record(
    session: AsyncSession, record: CloudComputeRecord,
) -> None:
    """Insert one ``cloud_compute_records`` row for a Modal sandbox."""
    await persist_record(session, record)
