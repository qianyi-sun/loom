"""Batch runner background loop (spec §7 / Plan 19 Task 5, renamed
in Plan 28).

The runner is a single asyncio task spawned from the service lifespan.
On each tick it:

1. `SELECT FOR UPDATE SKIP LOCKED` rows from `batches` where
   `state IN ('submitted', 'running')`. SKIP LOCKED makes concurrent
   instances safe — each replica processes a disjoint slice.
2. For each batch, resolves the `task_filter` into the live task
   list, subtracts task_ids already submitted under this batch,
   and POSTs the remainder to Control Plane. Each submission carries
   an `idempotency_key = "{batch_id}::{task_id}::{sample_idx}"` so
   re-running the loop (or a CP retry) never produces duplicate
   trial rows.
3. Recomputes the batch's state from current trial counts and
   advances the row.

`next_batch_state` is split out so the state machine is
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

from loom.db.schema import Batch, Task, Trial

logger = logging.getLogger(__name__)

_TERMINAL: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})
_IN_FLIGHT: frozenset[str] = frozenset({"queued", "claimed", "running"})


def next_batch_state(
    *,
    current: str,
    expected: int,
    counts: Mapping[str, int],
) -> str:
    """Pure state-transition function.

    Inputs:
    - current: existing batch.state
    - expected: batch.expected_trial_count (materialized at create)
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
    """Same shape as routes/batches.py._resolve_task_filter — kept
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


def _idempotency_key(
    batch_id: UUID, task_id: str, sample_idx: int,
) -> str:
    """Stable, inspectable key — operators can grep for it in logs.

    Format changed in Plan 23 to include the sample index. The old
    `{batch}::{task}` keys are obsolete; any batch that predates
    the migration is already finished (its idempotency keys never need
    to be derived again).
    """
    return f"{batch_id}::{task_id}::{sample_idx}"


async def _submit_one(
    http_client: httpx.AsyncClient,
    *,
    authorization: str | None,
    batch_id: UUID,
    task_id: str,
    sample_idx: int,
    trial_config: dict[str, Any],
) -> None:
    payload: dict[str, Any] = {
        "task_id": task_id,
        "config": trial_config,
        # The CP route still accepts the legacy `campaign_id` key for
        # now (it's the column name on its end too until migration
        # 0011 lands in the CP's schema view). Pass batch_id under
        # that key so the rename is purely service-side here; the CP
        # rename happens in lockstep with this PR via the shared
        # schema module.
        "batch_id": str(batch_id),
        "sample_idx": sample_idx,
        "idempotency_key": _idempotency_key(
            batch_id, task_id, sample_idx,
        ),
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
            "batch %s task %s submit error: %s",
            batch_id, task_id, exc,
        )
        return
    if resp.status_code >= 400:
        logger.warning(
            "batch %s task %s submit failed: %s %s",
            batch_id, task_id, resp.status_code, resp.text,
        )


async def _advance_batch_state(
    session: AsyncSession, batch: Batch,
) -> None:
    rows = (await session.execute(
        select(Trial.state).where(Trial.batch_id == batch.id),
    )).scalars().all()
    counts: dict[str, int] = {}
    for st in rows:
        counts[str(st)] = counts.get(str(st), 0) + 1
    new_state = next_batch_state(
        current=batch.state,
        expected=batch.expected_trial_count,
        counts=counts,
    )
    if new_state != batch.state:
        finished_at = (
            datetime.now(UTC) if new_state == "finished" else None
        )
        await session.execute(
            update(Batch)
            .where(Batch.id == batch.id)
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
    """Process all non-terminal batches once. Safe to call from a loop.

    The locking strategy splits each tick into three short transactions:

    1. SELECT … FOR UPDATE SKIP LOCKED the batch rows to claim them
       for this runner instance, materialize the pending task list,
       COMMIT (release the lock).
    2. HTTP fanout to Control Plane. No DB locks held — trial INSERTs
       on the CP side need a key-share lock on the parent batch row,
       which would deadlock against a held FOR UPDATE.
    3. Re-open a transaction, advance each batch's state from the
       current trial counts.

    Concurrent runner safety: the idempotency_key
    `{batch_id}::{task_id}::{sample_idx}` is the cross-process dedupe
    key — a second runner that picks up the same batch mid-tick
    (after we released our SKIP-LOCKED claim) submits the same
    payloads, and the CP's ON CONFLICT DO NOTHING on the partial
    unique index collapses the duplicates.

    `cp_authorization` is the bearer token the runner sends upstream to
    Control Plane — in production this is a service-owned token with
    `submit` scope.
    """
    delay = 1.0 / max(submit_rate_per_sec, 1)

    # Phase 1: pick + materialize work, then release the lock.
    # Pending unit is (task_id, sample_idx) — n_per_task > 1 fans out
    # multiple samples per matched task. Existing (task_id, sample_idx)
    # pairs are subtracted so a re-tick never re-submits a sample the
    # CP already accepted.
    work: list[tuple[UUID, dict[str, Any], list[tuple[str, int]]]] = []
    async with session_factory() as s:
        batches_to_process = (await s.execute(
            select(Batch)
            .where(Batch.state.in_(["submitted", "running"]))
            .with_for_update(skip_locked=True),
        )).scalars().all()
        for b in batches_to_process:
            task_ids = await _resolve_task_filter(s, b.task_filter)
            existing = {
                (row[0], row[1])
                for row in (await s.execute(
                    select(Trial.task_id, Trial.sample_idx).where(
                        Trial.batch_id == b.id,
                    ),
                )).all()
            }
            pending: list[tuple[str, int]] = []
            for t in task_ids:
                for s_idx in range(b.n_per_task):
                    if (t, s_idx) not in existing:
                        pending.append((t, s_idx))
            work.append((b.id, dict(b.trial_config), pending))
        await s.commit()

    # Phase 2: HTTP fanout. No DB locks.
    for batch_id, trial_config, pending in work:
        for chunk_start in range(0, len(pending), batch_size):
            chunk = pending[chunk_start:chunk_start + batch_size]
            for tid, s_idx in chunk:
                await _submit_one(
                    http_client,
                    authorization=cp_authorization,
                    batch_id=batch_id,
                    task_id=tid,
                    sample_idx=s_idx,
                    trial_config=trial_config,
                )
                await asyncio.sleep(delay)

    # Phase 3: advance state for the batches we processed.
    async with session_factory() as s:
        for batch_id, _, _ in work:
            row = (await s.execute(
                select(Batch).where(Batch.id == batch_id),
            )).scalar_one_or_none()
            if row is None:
                continue
            await _advance_batch_state(s, row)
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
    """Forever-loop entrypoint for the service lifespan.

    If `cp_authorization` is None the loop logs ONE warning and then
    skips submitting (still ticks the poll). Without a token every
    CP submit would 401 — better to surface the misconfig once than
    to spam the CP with failed POSTs."""
    warned_missing_token = False
    while True:
        try:
            if cp_authorization is None:
                if not warned_missing_token:
                    logger.warning(
                        "batch_runner has no CP token "
                        "(LOOM_SVC_BATCH_RUNNER_CP_TOKEN unset); "
                        "batches will queue but not fan out",
                    )
                    warned_missing_token = True
            else:
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
            logger.exception("batch_runner iteration failed")
        await asyncio.sleep(poll_interval_sec)
