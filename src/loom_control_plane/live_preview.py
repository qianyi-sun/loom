"""Claim-bound ephemeral Stage 1 preview persistence and reconciliation."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionAttempt,
    PipelineLivePreviewFrame,
    PipelineLivePreviewGeneration,
    PipelineRun,
    PipelineStageRun,
)
from loom.pipeline.live_preview import (
    PREVIEW_GLOBAL_MAX_BYTES,
    PREVIEW_GLOBAL_MAX_GENERATIONS,
    PREVIEW_MAX_BYTES,
    PREVIEW_MAX_FRAMES,
    PREVIEW_MIN_INTERVAL,
    PREVIEW_TTL,
)
from loom_control_plane.metrics import PIPELINE_LIVE_PREVIEW_PURGES_TOTAL

_TERMINAL_ATTEMPT_STATES = frozenset({"succeeded", "failed", "cancelled", "lost"})
logger = logging.getLogger(__name__)


async def purge_live_preview(
    session: AsyncSession,
    *,
    attempt_id: UUID,
    reason: str,
    retained_state: Literal["handoff", "ended"] = "ended",
    now: datetime | None = None,
) -> int:
    if reason not in {
        "attempt_cancelled",
        "attempt_failed",
        "attempt_terminal",
        "cancelled",
        "claim_replaced",
        "lease_lost",
        "output_committed",
        "ttl_expired",
        "worker_lost",
    }:
        raise ValueError("unbounded live preview purge reason")
    observed = now or datetime.now(UTC)
    generation = await session.get(PipelineLivePreviewGeneration, attempt_id, with_for_update=True)
    if generation is None:
        return 0
    deleted = len(
        (
            await session.execute(
                delete(PipelineLivePreviewFrame)
                .where(PipelineLivePreviewFrame.execution_attempt_id == attempt_id)
                .returning(PipelineLivePreviewFrame.sequence)
            )
        ).all()
    )
    generation.state = retained_state
    generation.latest_sequence = None
    generation.latest_step_idx = None
    generation.received_at = None
    generation.frame_count = 0
    generation.total_bytes = 0
    generation.purge_reason = reason
    generation.purged_at = observed
    generation.expires_at = observed
    generation.updated_at = observed
    PIPELINE_LIVE_PREVIEW_PURGES_TOTAL.labels(reason=reason).inc()
    return deleted


async def reconcile_live_previews(session: AsyncSession, *, now: datetime | None = None) -> int:
    observed = now or datetime.now(UTC)
    rows = (
        await session.execute(
            select(PipelineLivePreviewGeneration, ExecutionAttempt, PipelineStageRun, PipelineRun)
            .join(
                ExecutionAttempt,
                ExecutionAttempt.id == PipelineLivePreviewGeneration.execution_attempt_id,
            )
            .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
            .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
            .where(PipelineLivePreviewGeneration.purged_at.is_(None))
        )
    ).all()
    purged = 0
    for generation, attempt, _stage, run in rows:
        reason: str | None = None
        if generation.expires_at <= observed:
            reason = "ttl_expired"
        elif (
            attempt.cancellation_requested_at is not None
            or run.cancellation_requested_at is not None
        ):
            reason = "cancelled"
        elif attempt.state in _TERMINAL_ATTEMPT_STATES:
            reason = "attempt_terminal"
        elif (
            attempt.worker_id != generation.worker_id
            or attempt.claim_id != generation.claim_id
            or attempt.lease_epoch != generation.lease_epoch
        ):
            reason = "claim_replaced"
        elif attempt.lease_expires_at is None or attempt.lease_expires_at <= observed:
            reason = "lease_lost"
        if reason is not None:
            await purge_live_preview(session, attempt_id=attempt.id, reason=reason, now=observed)
            purged += 1
    return purged


async def enforce_generation_bounds(
    session: AsyncSession,
    *,
    generation: PipelineLivePreviewGeneration,
    incoming_bytes: int = 0,
) -> None:
    while (
        generation.frame_count >= PREVIEW_MAX_FRAMES
        or generation.total_bytes + incoming_bytes > PREVIEW_MAX_BYTES
    ):
        oldest = (
            await session.execute(
                select(PipelineLivePreviewFrame)
                .where(
                    PipelineLivePreviewFrame.execution_attempt_id == generation.execution_attempt_id
                )
                .order_by(PipelineLivePreviewFrame.sequence)
                .limit(1)
                .with_for_update()
            )
        ).scalar_one()
        generation.frame_count -= 1
        generation.total_bytes -= oldest.jpeg_size_bytes
        await session.delete(oldest)


def publish_due(*, last_received_at: datetime | None, now: datetime) -> bool:
    return last_received_at is None or now - last_received_at >= PREVIEW_MIN_INTERVAL


async def run_live_preview_reconciler_loop(*, session_factory: Any, interval_sec: int = 30) -> None:
    while True:
        try:
            async with session_factory() as session:
                await reconcile_live_previews(session)
                await session.commit()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            # Existing CP background loops own structured logging. This sweep
            # stays fail-closed: failures retain no read authorization because
            # every read independently revalidates lifecycle and expiry.
            logger.warning(
                "pipeline_live_preview_reconcile_failed", extra={"reason": type(exc).__name__}
            )
        await asyncio.sleep(interval_sec)


def generation_expiry(now: datetime) -> datetime:
    return now + PREVIEW_TTL


async def active_team_preview_totals(session: AsyncSession, *, team_id: UUID) -> tuple[int, int]:
    row = (
        await session.execute(
            select(
                func.count(PipelineLivePreviewGeneration.execution_attempt_id),
                func.coalesce(func.sum(PipelineLivePreviewGeneration.total_bytes), 0),
            ).where(
                PipelineLivePreviewGeneration.team_id == team_id,
                PipelineLivePreviewGeneration.purged_at.is_(None),
            )
        )
    ).one()
    return int(row[0]), int(row[1])


async def active_global_preview_totals(session: AsyncSession) -> tuple[int, int]:
    row = (
        await session.execute(
            select(
                func.count(PipelineLivePreviewGeneration.execution_attempt_id),
                func.coalesce(func.sum(PipelineLivePreviewGeneration.total_bytes), 0),
            ).where(PipelineLivePreviewGeneration.purged_at.is_(None))
        )
    ).one()
    return int(row[0]), int(row[1])


async def acquire_preview_capacity_locks(session: AsyncSession, *, team_id: UUID) -> None:
    """Serialize global then team admission without mutable quota rows."""

    await session.execute(text("SELECT pg_advisory_xact_lock(1366001)"))
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:team_id, 1366))"),
        {"team_id": str(team_id)},
    )


def global_preview_bound_exceeded(*, generations: int, bytes_used: int, incoming: int) -> bool:
    return (
        generations >= PREVIEW_GLOBAL_MAX_GENERATIONS
        or bytes_used + incoming > PREVIEW_GLOBAL_MAX_BYTES
    )


__all__ = [
    "acquire_preview_capacity_locks",
    "active_global_preview_totals",
    "active_team_preview_totals",
    "enforce_generation_bounds",
    "generation_expiry",
    "global_preview_bound_exceeded",
    "publish_due",
    "purge_live_preview",
    "reconcile_live_previews",
    "run_live_preview_reconciler_loop",
]
