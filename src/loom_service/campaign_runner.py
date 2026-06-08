"""Campaign runner background loop (spec §7 / Plan 19 Task 5).

The runner is a single asyncio task spawned from the service lifespan.
On each tick it:

1. `SELECT FOR UPDATE SKIP LOCKED` rows from `campaigns` where
   `state IN ('submitted', 'running')`. SKIP LOCKED makes concurrent
   instances safe — each replica processes a disjoint slice.
2. For each campaign, resolves the `task_filter` into the live task
   list, subtracts task_ids already submitted under this campaign,
   and POSTs the remainder to Control Plane. Each submission carries
   an `idempotency_key = "{campaign_id}::{task_id}"` so re-running
   the loop (or a CP retry) never produces duplicate trial rows.
3. Recomputes the campaign's state from current trial counts and
   advances the row.

`next_campaign_state` is split out so the state machine is
unit-testable without standing up Postgres.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import Campaign, Task, Trial

logger = logging.getLogger(__name__)

_TERMINAL: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})
_IN_FLIGHT: frozenset[str] = frozenset({"queued", "claimed", "running"})


def next_campaign_state(
    *,
    current: str,
    expected: int,
    counts: Mapping[str, int],
) -> str:
    """Pure state-transition function.

    Inputs:
    - current: existing campaign.state
    - expected: campaign.expected_trial_count (materialized at create)
    - counts: {trial_state: count_of_trials_in_that_state}

    Outputs the new state. Rules:
    - cancelled is absorbing.
    - all expected trials terminal + 0 in flight → finished.
    - any trial submitted (in-flight or terminal) → running.
    - otherwise stay in current.
    """
    if current == "cancelled":
        return "cancelled"
    terminal_count = sum(counts.get(k, 0) for k in _TERMINAL)
    in_flight = sum(counts.get(k, 0) for k in _IN_FLIGHT)
    if expected > 0 and terminal_count >= expected and in_flight == 0:
        return "finished"
    if in_flight > 0 or terminal_count > 0:
        return "running"
    return current


# ---------------------------------------------------------------------
# Runner implementation (Plan 19 Task 5)
# ---------------------------------------------------------------------


async def _resolve_task_filter(
    session: AsyncSession, task_filter: Mapping[str, Any],
) -> list[str]:
    """Same shape as routes/campaigns.py._resolve_task_filter — kept
    duplicated here so the runner has no dependency on the route
    module (lifespan can spawn runner without importing routes)."""
    stmt = select(Task.id)
    if "license" in task_filter:
        stmt = stmt.where(Task.license == task_filter["license"])
    if "task_ids" in task_filter:
        ids = [str(x) for x in task_filter["task_ids"]]
        stmt = stmt.where(Task.id.in_(ids))
    if "benchmark_id" in task_filter:
        stmt = stmt.where(Task.benchmark_id == task_filter["benchmark_id"])
    return [row[0] for row in (await session.execute(stmt)).all()]


def _idempotency_key(campaign_id: UUID, task_id: str) -> str:
    """Stable, inspectable key — operators can grep for it in logs."""
    return f"{campaign_id}::{task_id}"


async def _submit_one(
    http_client: httpx.AsyncClient,
    *,
    authorization: str | None,
    campaign_id: UUID,
    task_id: str,
    trial_config: dict[str, Any],
) -> None:
    payload: dict[str, Any] = {
        "task_id": task_id,
        "config": trial_config,
        "campaign_id": str(campaign_id),
        "idempotency_key": _idempotency_key(campaign_id, task_id),
    }
    headers: dict[str, str] = {}
    if authorization:
        headers["Authorization"] = authorization
    try:
        resp = await http_client.post(
            "/trials", json=payload, headers=headers,
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "campaign %s task %s submit error: %s",
            campaign_id, task_id, exc,
        )
        return
    if resp.status_code >= 400:
        logger.warning(
            "campaign %s task %s submit failed: %s %s",
            campaign_id, task_id, resp.status_code, resp.text,
        )


async def _advance_campaign_state(
    session: AsyncSession, campaign: Campaign,
) -> None:
    rows = (await session.execute(
        select(Trial.state).where(Trial.campaign_id == campaign.id),
    )).scalars().all()
    counts: dict[str, int] = {}
    for st in rows:
        counts[str(st)] = counts.get(str(st), 0) + 1
    new_state = next_campaign_state(
        current=campaign.state,
        expected=campaign.expected_trial_count,
        counts=counts,
    )
    if new_state != campaign.state:
        finished_at = (
            datetime.now(UTC) if new_state == "finished" else None
        )
        await session.execute(
            update(Campaign)
            .where(Campaign.id == campaign.id)
            .values(state=new_state, finished_at=finished_at),
        )


async def run_once(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    http_client: httpx.AsyncClient,
    batch_size: int,
    submit_rate_per_sec: int,
    cp_authorization: str | None = None,
) -> None:
    """Process all non-terminal campaigns once. Safe to call from a loop.

    The locking strategy splits each tick into three short transactions:

    1. SELECT … FOR UPDATE SKIP LOCKED the campaign rows to claim them
       for this runner instance, materialize the pending task list,
       COMMIT (release the lock).
    2. HTTP fanout to Control Plane. No DB locks held — trial INSERTs
       on the CP side need a key-share lock on the parent campaign row,
       which would deadlock against a held FOR UPDATE.
    3. Re-open a transaction, advance each campaign's state from the
       current trial counts.

    Concurrent runner safety: the idempotency_key
    `{campaign_id}::{task_id}` is the cross-process dedupe key —
    a second runner that picks up the same campaign mid-tick (after we
    released our SKIP-LOCKED claim) submits the same payloads, and
    the CP's ON CONFLICT DO NOTHING on the partial unique index
    collapses the duplicates.

    `cp_authorization` is the bearer token the runner sends upstream to
    Control Plane — in production this is a service-owned token with
    `submit` scope.
    """
    delay = 1.0 / max(submit_rate_per_sec, 1)

    # Phase 1: pick + materialize work, then release the lock.
    work: list[tuple[UUID, dict[str, Any], list[str]]] = []
    async with session_factory() as s:
        campaigns_to_process = (await s.execute(
            select(Campaign)
            .where(Campaign.state.in_(["submitted", "running"]))
            .with_for_update(skip_locked=True),
        )).scalars().all()
        for c in campaigns_to_process:
            task_ids = await _resolve_task_filter(s, c.task_filter)
            already_submitted = {
                row[0]
                for row in (await s.execute(
                    select(Trial.task_id).where(
                        Trial.campaign_id == c.id,
                    ),
                )).all()
            }
            pending = [
                t for t in task_ids if t not in already_submitted
            ]
            work.append((c.id, dict(c.trial_config), pending))
        await s.commit()

    # Phase 2: HTTP fanout. No DB locks.
    for campaign_id, trial_config, pending in work:
        for chunk_start in range(0, len(pending), batch_size):
            chunk = pending[chunk_start:chunk_start + batch_size]
            for tid in chunk:
                await _submit_one(
                    http_client,
                    authorization=cp_authorization,
                    campaign_id=campaign_id,
                    task_id=tid,
                    trial_config=trial_config,
                )
                await asyncio.sleep(delay)

    # Phase 3: advance state for the campaigns we processed.
    async with session_factory() as s:
        for campaign_id, _, _ in work:
            row = (await s.execute(
                select(Campaign).where(Campaign.id == campaign_id),
            )).scalar_one_or_none()
            if row is None:
                continue
            await _advance_campaign_state(s, row)
        await s.commit()


async def run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    http_client: httpx.AsyncClient,
    batch_size: int,
    submit_rate_per_sec: int,
    poll_interval_sec: int,
    cp_authorization: str | None = None,
) -> None:
    """Forever-loop entrypoint for the service lifespan. Logs +
    swallows per-iteration exceptions so the runner never dies."""
    while True:
        try:
            await run_once(
                session_factory=session_factory,
                http_client=http_client,
                batch_size=batch_size,
                submit_rate_per_sec=submit_rate_per_sec,
                cp_authorization=cp_authorization,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("campaign_runner iteration failed")
        await asyncio.sleep(poll_interval_sec)
