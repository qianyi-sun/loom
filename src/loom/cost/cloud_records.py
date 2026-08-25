"""Provider-neutral cloud compute usage records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


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


async def persist_record(
    session: AsyncSession,
    record: CloudComputeRecord,
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
