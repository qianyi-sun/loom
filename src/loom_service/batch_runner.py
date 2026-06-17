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

from loom.db.schema import Batch, Trial
from loom_service.task_config_validation import (
    expected_trial_count,
    split_valid_task_configs,
)
from loom_service.task_filter import resolve_task_filter

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


def _idempotency_key(
    batch_id: UUID,
    task_id: str,
    sample_idx: int,
    combination_idx: int | None = None,
) -> str:
    """Stable, inspectable key — operators can grep for it in logs.

    Plan 28 PR-3 grew the key when the batch has Combinations:
    single-combination batches keep the 3-segment
    `{batch}::{task}::{sample}` shape (preserves in-flight keys);
    multi-combination batches use 4 segments,
    `{batch}::{task}::{combination}::{sample}`.
    """
    if combination_idx is None:
        return f"{batch_id}::{task_id}::{sample_idx}"
    return f"{batch_id}::{task_id}::{combination_idx}::{sample_idx}"


def _materialize_trial_config(
    shared: dict[str, Any], combination: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the per-trial config for one Combination.

    Shared trial_config supplies the common knobs (timeouts, retry,
    skip_verifier, etc.); the Combination supplies agent_name +
    agent_model. The merge clobbers `agent_name` / `agent_model` on
    the shared config (route forbids them when combinations is
    non-empty, but defense in depth).
    """
    if combination is None:
        return shared
    import copy as _copy
    out: dict[str, Any] = _copy.deepcopy(shared)
    out["agent_name"] = combination["agent_name"]
    out["agent_model"] = combination.get("agent_model")
    return out


async def _submit_one(
    http_client: httpx.AsyncClient,
    *,
    authorization: str | None,
    batch_id: UUID,
    task_id: str,
    sample_idx: int,
    trial_config: dict[str, Any],
    provider_connection_id: UUID | None = None,
    provider_model_id: str | None = None,
    combination_idx: int | None = None,
) -> None:
    payload: dict[str, Any] = {
        "task_id": task_id,
        "config": trial_config,
        "batch_id": str(batch_id),
        "sample_idx": sample_idx,
        "idempotency_key": _idempotency_key(
            batch_id, task_id, sample_idx,
            combination_idx=combination_idx,
        ),
    }
    if combination_idx is not None:
        payload["combination_idx"] = combination_idx
    if provider_connection_id is not None:
        payload["provider_connection_id"] = str(provider_connection_id)
    if provider_model_id is not None:
        payload["provider_model_id"] = provider_model_id
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


def _compute_result_status(
    terminal_states: list[str],
) -> str:
    """Outcome classifier for a finished batch.

    Per the spec: succeeded / partial_failed / all_failed.
    Cancelled is set elsewhere (lifecycle override).

    A trial's platform outcome is its terminal state. Reward is model/evaluator
    outcome data and can be absent or zero without making a completed trial a
    platform failure.
    """
    succeeded = sum(1 for state in terminal_states if state == "succeeded")
    failed = len(terminal_states) - succeeded
    if failed == 0 and succeeded > 0:
        return "succeeded"
    if succeeded == 0:
        return "all_failed"
    return "partial_failed"


async def _advance_batch_state(
    session: AsyncSession, batch: Batch,
) -> None:
    state_rows = (await session.execute(
        select(Trial.state).where(Trial.batch_id == batch.id),
    )).scalars().all()
    counts: dict[str, int] = {}
    for st in state_rows:
        counts[str(st)] = counts.get(str(st), 0) + 1
    new_state = next_batch_state(
        current=batch.state,
        expected=batch.expected_trial_count,
        counts=counts,
    )

    values: dict[str, Any] = {}
    if new_state != batch.state:
        values["state"] = new_state
        if new_state in ("finished", "cancelled"):
            values["finished_at"] = datetime.now(UTC)
        if new_state == "cancelled":
            values["result_status"] = "cancelled"
        elif new_state == "finished":
            computed = _compute_result_status([str(st) for st in state_rows])
            if batch.result_status == "partial_failed" and (
                computed == "succeeded"
            ):
                values["result_status"] = "partial_failed"
            else:
                values["result_status"] = computed

    if values:
        await session.execute(
            update(Batch).where(Batch.id == batch.id).values(**values),
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
    # Pending unit is (task_id, combination_idx, sample_idx). For
    # single-combination batches combination_idx is None — the
    # idempotency key shape matches the 3-segment form that already
    # exists in the DB. For multi-combination, combination_idx is
    # 0..len(combinations)-1.
    #
    # Each work item is (batch_id, [(task_id, combination_or_None,
    # trial_config, sample_idx), ...]).
    PendingUnit = tuple[str, int | None, dict[str, Any], int]  # noqa: N806
    work: list[tuple[UUID, UUID | None, str | None, list[PendingUnit]]] = []
    async with session_factory() as s:
        batches_to_process = (await s.execute(
            select(Batch)
            .where(Batch.state.in_(["submitted", "running"]))
            .with_for_update(skip_locked=True),
        )).scalars().all()
        for b in batches_to_process:
            task_ids = await resolve_task_filter(s, b.task_filter)
            task_ids, invalid_tasks = await split_valid_task_configs(
                s, task_ids,
            )
            if invalid_tasks:
                adjusted_expected = expected_trial_count(
                    task_count=len(task_ids),
                    n_per_task=b.n_per_task,
                    combinations=b.combinations,
                )
                values: dict[str, Any] = {
                    "expected_trial_count": adjusted_expected,
                }
                if adjusted_expected == 0:
                    values.update({
                        "state": "finished",
                        "result_status": "all_failed",
                        "finished_at": datetime.now(UTC),
                    })
                elif b.result_status is None:
                    values["result_status"] = "partial_failed"
                if (
                    b.expected_trial_count != adjusted_expected
                    or any(
                        getattr(b, key) != value
                        for key, value in values.items()
                        if hasattr(b, key)
                    )
                ):
                    await s.execute(
                        update(Batch).where(Batch.id == b.id).values(**values),
                    )
                logger.warning(
                    "batch %s skipped %d invalid task configs: %s",
                    b.id,
                    len(invalid_tasks),
                    [item.task_id for item in invalid_tasks],
                )
                b.expected_trial_count = adjusted_expected
                if adjusted_expected == 0:
                    continue
                if b.result_status is None:
                    b.result_status = "partial_failed"
            if b.combinations:
                # Multi-combination: existing key is
                # (task_id, combination_idx, sample_idx).
                existing_multi = {
                    (row[0], row[1], row[2])
                    for row in (await s.execute(
                        select(
                            Trial.task_id,
                            Trial.combination_idx,
                            Trial.sample_idx,
                        ).where(Trial.batch_id == b.id),
                    )).all()
                }
                pending_units: list[PendingUnit] = []
                shared_config = dict(b.trial_config)
                for c_idx, combo in enumerate(b.combinations):
                    combo_config = _materialize_trial_config(
                        shared_config, combo,
                    )
                    n = int(combo.get("n_per_task", 1))
                    for t in task_ids:
                        for s_idx in range(n):
                            if (t, c_idx, s_idx) in existing_multi:
                                continue
                            pending_units.append(
                                (t, c_idx, combo_config, s_idx),
                            )
                work.append((
                    b.id, b.provider_connection_id, b.provider_model_id,
                    pending_units,
                ))
            else:
                # Single-combination: keep the 2-tuple key shape and
                # the None combination_idx so the resulting
                # idempotency_key uses the 3-segment format.
                existing_single = {
                    (row[0], row[1])
                    for row in (await s.execute(
                        select(Trial.task_id, Trial.sample_idx).where(
                            Trial.batch_id == b.id,
                        ),
                    )).all()
                }
                pending_units = []
                cfg = dict(b.trial_config)
                for t in task_ids:
                    for s_idx in range(b.n_per_task):
                        if (t, s_idx) in existing_single:
                            continue
                        pending_units.append((t, None, cfg, s_idx))
                work.append((
                    b.id, b.provider_connection_id, b.provider_model_id,
                    pending_units,
                ))
        await s.commit()

    # Phase 2: HTTP fanout. No DB locks.
    for batch_id, provider_connection_id, provider_model_id, pending_units in work:
        for chunk_start in range(0, len(pending_units), batch_size):
            chunk = pending_units[chunk_start:chunk_start + batch_size]
            for tid, combo_idx, cfg, s_idx in chunk:
                await _submit_one(
                    http_client,
                    authorization=cp_authorization,
                    batch_id=batch_id,
                    task_id=tid,
                    sample_idx=s_idx,
                    trial_config=cfg,
                    provider_connection_id=provider_connection_id,
                    provider_model_id=provider_model_id,
                    combination_idx=combo_idx,
                )
                await asyncio.sleep(delay)

    # Phase 3: advance state for the batches we processed.
    async with session_factory() as s:
        for batch_id, _provider_connection_id, _provider_model_id, _ in work:
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
