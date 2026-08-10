"""Fenced standalone Pipeline controller polling loop (#1212)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loom_pipeline_orchestrator.repository import PipelineRepository, RunLease

logger = logging.getLogger(__name__)

Reconcile = Callable[[RunLease], Awaitable[None]]


@dataclass(slots=True)
class OrchestratorContext:
    repository: PipelineRepository
    controller_id: str
    reconcile: Reconcile
    poll_seconds: float = 2.0
    picker_batch: int = 50


async def run_once(ctx: OrchestratorContext) -> int:
    leases = await ctx.repository.claim_runs(
        controller_id=ctx.controller_id,
        limit=ctx.picker_batch,
    )
    await asyncio.gather(*(_reconcile_claimed(ctx, lease) for lease in leases))
    return len(leases)


async def _reconcile_claimed(ctx: OrchestratorContext, original_lease: RunLease) -> None:
    """Reconcile each claimed run concurrently so batch leases cannot age in a queue."""

    lease = original_lease
    task: asyncio.Future[None] = asyncio.ensure_future(ctx.reconcile(lease))
    try:
        while not task.done():
            done, _pending = await asyncio.wait({task}, timeout=10.0)
            if not done:
                lease = await ctx.repository.renew(lease)
        await task
    except Exception:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.exception(
            "pipeline_reconcile_failed run_id=%s lease_epoch=%s",
            lease.pipeline_run_id,
            lease.lease_epoch,
        )
    finally:
        try:
            await ctx.repository.release(lease)
        except Exception:
            logger.exception(
                "pipeline_lease_release_failed run_id=%s lease_epoch=%s",
                lease.pipeline_run_id,
                lease.lease_epoch,
            )


async def run(ctx: OrchestratorContext, *, stop_event: asyncio.Event | None = None) -> None:
    stop = stop_event or asyncio.Event()
    while not stop.is_set():
        picked = await run_once(ctx)
        if picked:
            continue
        try:
            await asyncio.wait_for(stop.wait(), timeout=ctx.poll_seconds)
        except TimeoutError:
            pass
