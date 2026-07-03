"""Trial cancellation + hard-deadline watchdog (#360, #378).

The worker's trial runner is long-lived (agent step + verifier + artifact
collection can legitimately take minutes to hours). Two failure modes
were surfaced by the 2026-07-02 / 2026-07-03 public-beta canaries and
had no defense in place:

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

This module supplies both defenses in one primitive. ``run_with_watchdog``
wraps a trial coroutine with:

1. A poll every ``poll_interval_sec`` against
   :meth:`HttpControlPlaneClient.get_trial_state`. If the state is
   ``cancelled``, we ``cancel()`` the wrapped task. The trial's existing
   ``except asyncio.CancelledError`` path then records the terminal
   state, kills the in-container exec, and returns.
2. A monotonic elapsed-time check against ``hard_deadline_sec``. If the
   trial runs longer than that budget, we cancel it and log the
   backstop trigger so evidence attributes the terminal state to the
   watchdog rather than a mystery hang.

Both conditions cancel the same wrapped task; the trial's own cleanup
handles container teardown and state PATCH.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Coroutine
from typing import Any, Protocol
from uuid import UUID

from loom.trial.watchdog_cancellation import WatchdogCancellation, WatchdogTriggerReason

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SEC = 30.0

#: Sentinel returned by :func:`run_with_watchdog` describing what stopped
#: the wrapped coroutine. ``None`` means the coroutine completed under its
#: own control.
WatchdogTrigger = str | None


class _TrialStateSource(Protocol):
    async def get_trial_state(self, trial_id: UUID) -> str: ...


async def run_with_watchdog(
    coro: Coroutine[Any, Any, Any],
    *,
    trial_id: UUID,
    cp_client: _TrialStateSource,
    poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC,
    hard_deadline_sec: float | None = None,
) -> WatchdogTrigger:
    """Run ``coro`` under the trial-cancellation watchdog.

    Returns:
      ``None`` if ``coro`` completed under its own control.
      ``"cp_cancelled"`` if the CP marked the trial ``cancelled`` and the
        watchdog cancelled the wrapped task.
      ``"hard_deadline"`` if ``hard_deadline_sec`` was exceeded and the
        watchdog force-cancelled the wrapped task.

    ``coro`` is scheduled with :func:`asyncio.create_task` so the
    watchdog and the trial run concurrently. Any exception from ``coro``
    (including ``CancelledError`` from a watchdog trigger) propagates
    out.
    """
    task: asyncio.Task[Any] = asyncio.create_task(coro)
    trigger: dict[str, str | None] = {"reason": None}
    watcher = asyncio.create_task(
        _watch(
            task=task,
            trial_id=trial_id,
            cp_client=cp_client,
            poll_interval_sec=poll_interval_sec,
            hard_deadline_sec=hard_deadline_sec,
            trigger=trigger,
        )
    )
    try:
        await task
        return trigger["reason"]
    except asyncio.CancelledError:
        # The trial coro was cancelled by the watchdog OR by an outer
        # cancellation (worker shutdown). Propagate but still surface the
        # trigger so callers can label evidence.
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
    cp_client: _TrialStateSource,
    poll_interval_sec: float,
    hard_deadline_sec: float | None,
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
            task.cancel(
                WatchdogCancellation(
                    reason=WatchdogTriggerReason.HARD_DEADLINE,
                    message="worker hard deadline elapsed",
                    elapsed_sec=elapsed,
                    hard_deadline_sec=hard_deadline_sec,
                )
            )
            return

        try:
            state = await cp_client.get_trial_state(trial_id)
        except Exception as exc:
            logger.debug(
                "trial_state_poll_failed trial_id=%s err=%r", trial_id, exc,
            )
            continue

        if state == "cancelled":
            logger.info(
                "trial_cancel_observed_by_watchdog trial_id=%s — "
                "cancelling runner task",
                trial_id,
            )
            trigger["reason"] = "cp_cancelled"
            task.cancel(
                WatchdogCancellation(
                    reason=WatchdogTriggerReason.CP_CANCELLED,
                    message="control plane reported trial cancelled",
                    cp_state=state,
                )
            )
            return

        if state in {"failed", "succeeded"}:
            logger.info(
                "trial_terminal_observed_by_watchdog trial_id=%s state=%s — "
                "cancelling runner task",
                trial_id,
                state,
            )
            trigger["reason"] = "cp_terminal"
            task.cancel(
                WatchdogCancellation(
                    reason=WatchdogTriggerReason.CP_TERMINAL,
                    message="control plane reported trial terminal",
                    cp_state=state,
                )
            )
            return
