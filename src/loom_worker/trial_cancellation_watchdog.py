"""Trial cancellation + ownership-revoke + hard-deadline watchdog (#360, #378, #1491).

The worker's trial runner is long-lived (agent step + verifier + artifact
collection can legitimately take minutes to hours). Failure modes:

* **#360 — operator-driven cancellation.** The control plane marks a
  trial ``cancelled`` (batch cancel, admin action) but the worker only
  observes that at heartbeat/state-PATCH failure time. Meanwhile the
  in-container subprocess-agent keeps making gateway calls, spending
  budget on a cancelled run.
* **#378 — stuck trials past agent timeout.** ``asyncio.wait_for``
  around ``agent.run`` is supposed to cancel the coroutine at the
  configured timeout, but on GB10/opencode we saw four trials still
  ``running`` 5+ hours past a 2400s timeout. Whatever swallowed the
  timeout cancellation, the worker needs a second-line backstop.
* **#1491 — live-worker ownership revoke.** Heartbeat reclaim can set
  ``state=queued`` / clear ``worker_id`` (or another worker can re-claim
  with a higher ``attempt_count``) while this process still holds a
  ``RunnerPool`` seat. Treat ownership loss like cancel so ``in_flight``
  drops without requiring a worker restart.

This module supplies those defenses in one primitive. ``run_with_watchdog``
wraps a trial coroutine with:

1. A poll every ``poll_interval_sec`` against the ownership snapshot. If
   the lease is revoked (cancelled / terminal / queued / other owner /
   newer attempt), we ``cancel()`` the wrapped task and optionally run
   ``on_cancel`` (transport / attempt interrupt).
2. A monotonic elapsed-time check against ``hard_deadline_sec``.

Both conditions cancel the same wrapped task; the trial's own cleanup
handles container teardown and state PATCH (which may 409 after reclaim).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from loom.trial.stale_running import effective_agent_timeout_sec
from loom.trial.watchdog_cancellation import WatchdogCancellation, WatchdogTriggerReason

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SEC = 30.0

#: Sentinel returned by :func:`run_with_watchdog` describing what stopped
#: the wrapped coroutine. ``None`` means the coroutine completed under its
#: own control.
WatchdogTrigger = str | None

OnCancelHook = Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class TrialOwnershipSnapshot:
    """CP ownership fields used by the live-worker revoke watchdog."""

    state: str
    worker_id: UUID | None = None
    attempt_count: int | None = None


class _TrialOwnershipSource(Protocol):
    async def get_trial_ownership(self, trial_id: UUID) -> TrialOwnershipSnapshot: ...


def resolve_hard_deadline_sec(
    *,
    task_config: Any,
    trial_config: Any,
    multiplier: float,
    grace_sec: float,
) -> float | None:
    """Derive the watchdog budget from the execution timeout resolver."""
    if multiplier <= 0:
        return None
    agent_timeout_sec = effective_agent_timeout_sec(
        task_config=task_config,
        trial_config=trial_config,
    )
    if agent_timeout_sec is None:
        return None
    return agent_timeout_sec * multiplier + grace_sec


def ownership_revoke_reason(
    snapshot: TrialOwnershipSnapshot,
    *,
    owner_worker_id: UUID,
    claimed_attempt_count: int,
) -> WatchdogTriggerReason | None:
    """Return why local work must stop, or ``None`` if ownership is still valid."""

    state = snapshot.state
    if state == "cancelled":
        return WatchdogTriggerReason.CP_CANCELLED
    if state in {"failed", "succeeded"}:
        return WatchdogTriggerReason.CP_TERMINAL
    if state == "queued":
        return WatchdogTriggerReason.OWNERSHIP_REVOKED
    if (
        snapshot.worker_id is not None
        and snapshot.worker_id != owner_worker_id
    ):
        return WatchdogTriggerReason.OWNERSHIP_REVOKED
    if (
        snapshot.attempt_count is not None
        and snapshot.attempt_count > claimed_attempt_count
    ):
        return WatchdogTriggerReason.OWNERSHIP_REVOKED
    return None


async def run_with_watchdog(
    coro: Coroutine[Any, Any, Any],
    *,
    trial_id: UUID,
    cp_client: _TrialOwnershipSource,
    owner_worker_id: UUID,
    claimed_attempt_count: int,
    poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC,
    hard_deadline_sec: float | None = None,
    on_cancel: OnCancelHook | None = None,
) -> WatchdogTrigger:
    """Run ``coro`` under the trial-cancellation / ownership watchdog.

    Returns:
      ``None`` if ``coro`` completed under its own control.
      ``"cp_cancelled"`` if the CP marked the trial ``cancelled``.
      ``"cp_terminal"`` if the CP already recorded a terminal state.
      ``"ownership_revoked"`` if reclaim / re-claim cleared this worker's lease.
      ``"hard_deadline"`` if ``hard_deadline_sec`` was exceeded.
    """
    task: asyncio.Task[Any] = asyncio.create_task(coro)
    trigger: dict[str, str | None] = {"reason": None}
    watcher = asyncio.create_task(
        _watch(
            task=task,
            trial_id=trial_id,
            cp_client=cp_client,
            owner_worker_id=owner_worker_id,
            claimed_attempt_count=claimed_attempt_count,
            poll_interval_sec=poll_interval_sec,
            hard_deadline_sec=hard_deadline_sec,
            on_cancel=on_cancel,
            trigger=trigger,
        )
    )
    try:
        await task
        return trigger["reason"]
    except asyncio.CancelledError:
        raise
    finally:
        watcher.cancel()
        try:
            await watcher
        except (asyncio.CancelledError, Exception):
            pass


async def _watch(
    *,
    task: asyncio.Task[Any],
    trial_id: UUID,
    cp_client: _TrialOwnershipSource,
    owner_worker_id: UUID,
    claimed_attempt_count: int,
    poll_interval_sec: float,
    hard_deadline_sec: float | None,
    on_cancel: OnCancelHook | None,
    trigger: dict[str, str | None],
) -> None:
    started = time.monotonic()
    while not task.done():
        try:
            await asyncio.sleep(poll_interval_sec)
        except asyncio.CancelledError:
            return
        if task.done():
            return

        elapsed = time.monotonic() - started
        if (
            hard_deadline_sec is not None
            and elapsed > hard_deadline_sec
        ):
            logger.warning(
                "trial_hard_deadline_watchdog_fired trial_id=%s "
                "elapsed_sec=%.1f deadline_sec=%.1f — force-cancelling",
                trial_id, elapsed, hard_deadline_sec,
            )
            trigger["reason"] = "hard_deadline"
            await _cancel_task(
                task,
                on_cancel=on_cancel,
                cancellation=WatchdogCancellation(
                    reason=WatchdogTriggerReason.HARD_DEADLINE,
                    message="worker hard deadline elapsed",
                    elapsed_sec=elapsed,
                    hard_deadline_sec=hard_deadline_sec,
                ),
            )
            return

        try:
            snapshot = await cp_client.get_trial_ownership(trial_id)
        except Exception as exc:
            logger.debug(
                "trial_ownership_poll_failed trial_id=%s err=%r", trial_id, exc,
            )
            continue

        reason = ownership_revoke_reason(
            snapshot,
            owner_worker_id=owner_worker_id,
            claimed_attempt_count=claimed_attempt_count,
        )
        if reason is None:
            continue

        logger.info(
            "trial_ownership_revoked_by_watchdog trial_id=%s reason=%s "
            "cp_state=%s cp_worker_id=%s cp_attempt_count=%s "
            "owner_worker_id=%s claimed_attempt_count=%s — cancelling runner task",
            trial_id,
            reason.value,
            snapshot.state,
            snapshot.worker_id,
            snapshot.attempt_count,
            owner_worker_id,
            claimed_attempt_count,
        )
        trigger["reason"] = reason.value
        await _cancel_task(
            task,
            on_cancel=on_cancel,
            cancellation=WatchdogCancellation(
                reason=reason,
                message=f"control plane ownership revoke ({reason.value})",
                cp_state=snapshot.state,
            ),
        )
        return


async def _cancel_task(
    task: asyncio.Task[Any],
    *,
    on_cancel: OnCancelHook | None,
    cancellation: WatchdogCancellation,
) -> None:
    if on_cancel is not None:
        try:
            await on_cancel()
        except Exception:
            logger.exception("trial_watchdog_on_cancel_failed")
    task.cancel(cancellation)
