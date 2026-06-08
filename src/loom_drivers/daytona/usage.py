"""Daytona compute-seconds + cost capture (per amendment A26.1, the
underlying table `cloud_compute_records` is multi-provider; Plan 27's
Modal driver shares it via cloud_provider='modal').

compute_seconds = (stopped_at - started_at).total_seconds()
cost_usd        = compute_seconds * per_second_usd  (Numeric, 6 dp)

per_second_usd defaults to a placeholder; real Daytona billing happens
centrally on their side, this rollup is for surfacing spend in Loom's
UI alongside LLM spend.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

DEFAULT_PER_SECOND_USD: Decimal = Decimal("0.0001")
_6DP = Decimal("0.000001")


@dataclass(frozen=True)
class CloudComputeRecord:
    team_id: UUID
    trial_id: UUID
    cloud_provider: str
    sandbox_id: str
    image: str
    started_at: datetime
    stopped_at: datetime
    compute_seconds: float
    cost_usd: Decimal


def compute_record(
    *,
    team_id: UUID,
    trial_id: UUID,
    sandbox_id: str,
    image: str,
    started_at: datetime,
    stopped_at: datetime,
    per_second_usd: Decimal,
    cloud_provider: str = "daytona",
) -> CloudComputeRecord:
    compute_seconds = (stopped_at - started_at).total_seconds()
    cost = (Decimal(str(compute_seconds)) * per_second_usd).quantize(
        _6DP, rounding=ROUND_HALF_EVEN,
    )
    return CloudComputeRecord(
        team_id=team_id,
        trial_id=trial_id,
        cloud_provider=cloud_provider,
        sandbox_id=sandbox_id,
        image=image,
        started_at=started_at,
        stopped_at=stopped_at,
        compute_seconds=compute_seconds,
        cost_usd=cost,
    )


async def persist_record(
    session: AsyncSession, record: CloudComputeRecord,
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO cloud_compute_records
                (team_id, trial_id, cloud_provider, sandbox_id, image,
                 started_at, stopped_at, compute_seconds, cost_usd)
            VALUES
                (:team_id, :trial_id, :cloud_provider, :sandbox_id, :image,
                 :started_at, :stopped_at, :compute_seconds, :cost_usd)
            """,
        ),
        {
            "team_id": str(record.team_id),
            "trial_id": str(record.trial_id),
            "cloud_provider": record.cloud_provider,
            "sandbox_id": record.sandbox_id,
            "image": record.image,
            "started_at": record.started_at,
            "stopped_at": record.stopped_at,
            "compute_seconds": record.compute_seconds,
            "cost_usd": record.cost_usd,
        },
    )
