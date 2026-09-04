"""Explicit absolute-deadline supervision for one agent attempt."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.trajectory.attempt_guard import AttemptFence, AttemptTrajectoryGuard
from loom.trajectory.writer import TrajectoryWriter

logger = logging.getLogger(__name__)

DEFAULT_CANCELLATION_DRAIN_SEC = 30.0
_BACKGROUND_CLEANUP_TASKS: set[asyncio.Task[Any]] = set()


class _AttemptAgent(Protocol):
    def begin_attempt(self, deadline: AttemptDeadline) -> object: ...

    async def aclose_attempt(self) -> None: ...


class _WorkerHealthSignal(Protocol):
    _loom_worker_unhealthy: bool


@dataclass(frozen=True, slots=True)
class AttemptTimeoutDiagnostic:
    """Supervisor observations captured after the timeout cause is latched."""

    configured_timeout_sec: float
    elapsed_monotonic_sec: float
    cancellation_drain_sec: float
    transport_close_required: bool
    task_stopped: bool


async def supervise_agent_attempt(
    *,
    agent: object,
    configured_timeout_sec: float,
    trajectory: TrajectoryWriter,
    run: Callable[[TrajectoryWriter], Awaitable[None]],
    cancellation_drain_sec: float = DEFAULT_CANCELLATION_DRAIN_SEC,
) -> AttemptTimeoutDiagnostic | None:
    """Run one attempt and make an absolute deadline the first-cause arbiter.

    ``run`` receives a trajectory guard cast to the established writer type so
    existing agent runtimes remain source-compatible. Platform code retains
    the original writer and can therefore emit timeout/retry/step-end events.
    """

    if (
        not math.isfinite(cancellation_drain_sec)
        or cancellation_drain_sec < 0
        or cancellation_drain_sec > DEFAULT_CANCELLATION_DRAIN_SEC
    ):
        raise ValueError("cancellation drain must be finite and between 0 and 30 seconds")
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    deadline = AttemptDeadline.after(configured_timeout_sec, clock=loop.time)
    fence = AttemptFence()
    guarded_trajectory = cast(
        TrajectoryWriter,
        AttemptTrajectoryGuard(trajectory, fence),
    )

    await _begin_attempt(agent, deadline)

    async def run_and_latch() -> None:
        try:
            await run(guarded_trajectory)
        except AttemptDeadlineExceededError:
            fence.latch("agent_timeout")
            raise
        except BaseException:
            fence.latch(
                "agent_timeout" if deadline.reached else "agent_finished"
            )
            raise
        else:
            fence.latch(
                "agent_timeout" if deadline.reached else "agent_finished"
            )

    task = asyncio.create_task(run_and_latch())
    close_started = False
    close_task: asyncio.Task[None] | None = None

    def start_close_once() -> asyncio.Task[None] | None:
        nonlocal close_started, close_task
        if not close_started:
            close_started = True
            close_task = _start_close_attempt(agent)
        return close_task

    try:
        while not task.done():
            remaining = deadline.remaining()
            if remaining <= 0:
                break
            done, _ = await asyncio.wait({task}, timeout=remaining)
            if done:
                break

        if fence.terminal_cause == "agent_finished":
            try:
                await task
            finally:
                await _close_completed_attempt(start_close_once())
            return None

        # The deadline wins before cancellation or transport teardown begins.
        # Later exceptions and auth responses can no longer replace this cause.
        deadline_won = fence.terminal_cause == "agent_timeout" or fence.latch(
            "agent_timeout"
        )
        if not deadline_won:
            try:
                await task
            finally:
                await _close_completed_attempt(start_close_once())
            return None

        task.cancel()
        close_task = start_close_once()
        drain_started_at = loop.time()
        waiters: set[asyncio.Task[Any]] = {task}
        if close_task is not None:
            waiters.add(close_task)
        if waiters and cancellation_drain_sec > 0:
            await asyncio.wait(waiters, timeout=cancellation_drain_sec)
        cancellation_elapsed = min(
            cancellation_drain_sec,
            max(0.0, loop.time() - drain_started_at),
        )
        task_stopped = task.done()
        for waiter in waiters:
            if waiter.done():
                _consume_task_result(
                    waiter,
                    log_failure=waiter is close_task,
                )
            else:
                _retain_background_task(
                    waiter,
                    log_failure=waiter is close_task,
                )
        if not task_stopped:
            _mark_worker_unhealthy(agent)
        return AttemptTimeoutDiagnostic(
            configured_timeout_sec=configured_timeout_sec,
            elapsed_monotonic_sec=max(0.0, loop.time() - started_at),
            cancellation_drain_sec=cancellation_elapsed,
            transport_close_required=close_task is not None,
            task_stopped=task_stopped,
        )
    except asyncio.CancelledError:
        fence.latch("supervisor_cancelled")
        task.cancel()
        close_task = start_close_once()
        waiters = {task}
        if close_task is not None:
            waiters.add(close_task)
        if cancellation_drain_sec > 0:
            done, pending = await asyncio.wait(
                waiters,
                timeout=cancellation_drain_sec,
            )
            for finished in done:
                _consume_task_result(
                    finished,
                    log_failure=finished is close_task,
                )
            for unfinished in pending:
                _retain_background_task(
                    unfinished,
                    log_failure=unfinished is close_task,
                )
        else:
            for unfinished in waiters:
                _retain_background_task(
                    unfinished,
                    log_failure=unfinished is close_task,
                )
        raise


async def _begin_attempt(agent: object, deadline: AttemptDeadline) -> None:
    begin = getattr(agent, "begin_attempt", None)
    if not callable(begin):
        return
    result = cast(_AttemptAgent, agent).begin_attempt(deadline)
    if inspect.isawaitable(result):
        await result


def _start_close_attempt(agent: object) -> asyncio.Task[None] | None:
    close = getattr(agent, "aclose_attempt", None)
    if not callable(close):
        return None
    return asyncio.create_task(cast(_AttemptAgent, agent).aclose_attempt())


async def _close_completed_attempt(close_task: asyncio.Task[None] | None) -> None:
    if close_task is not None:
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("attempt transport close failed", exc_info=True)


def _retain_background_task(
    task: asyncio.Task[Any],
    *,
    log_failure: bool,
) -> None:
    _BACKGROUND_CLEANUP_TASKS.add(task)
    task.add_done_callback(
        lambda finished: _background_task_done(
            finished,
            log_failure=log_failure,
        )
    )


def _background_task_done(
    task: asyncio.Task[Any],
    *,
    log_failure: bool,
) -> None:
    _BACKGROUND_CLEANUP_TASKS.discard(task)
    _consume_task_result(task, log_failure=log_failure)


def _consume_task_result(
    task: asyncio.Task[Any],
    *,
    log_failure: bool,
) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        if not log_failure:
            return
        logger.warning("attempt cleanup task failed", exc_info=True)


def _mark_worker_unhealthy(agent: object) -> None:
    """Expose a low-coupling fatal signal for the worker ownership layer."""

    try:
        cast(_WorkerHealthSignal, agent)._loom_worker_unhealthy = True
    except Exception:
        logger.exception("failed to mark cancellation-resistant agent unhealthy")
