"""Per-worker semaphore-gated runner pool."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from loom_worker.worker_health import WorkerUnhealthyError


class RunnerPool:
    """`max_concurrent` Trial or ExecutionAttempt work items may execute.

    spawn() registers a coroutine that will acquire the semaphore before its
    body runs; concurrent spawns past the limit park inside the semaphore
    until earlier work completes. `in_flight` reflects total registered
    (running + parked) work.

    Optional ``key`` (e.g. trial_id) prevents two live Tasks for the same
    ownership unit: a new spawn with a duplicate key cancels the prior Task
    before registering (#1491 duplicate-seat guard).
    """

    def __init__(
        self,
        max_concurrent: int,
        *,
        unhealthy_callback: Callable[[WorkerUnhealthyError], None] | None = None,
    ) -> None:
        self._sem = asyncio.Semaphore(max_concurrent)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._tasks_by_key: dict[str, asyncio.Task[Any]] = {}
        self._fatal_error: WorkerUnhealthyError | None = None
        self._unhealthy_callback = unhealthy_callback

    @property
    def in_flight(self) -> int:
        return len(self._tasks)

    async def spawn(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        key: str | None = None,
    ) -> None:
        if key is not None:
            prior = self._tasks_by_key.get(key)
            if prior is not None and not prior.done():
                prior.cancel()
                try:
                    await prior
                except (asyncio.CancelledError, Exception):
                    pass

        async def _wrapped() -> None:
            async with self._sem:
                await coro

        task: asyncio.Task[None] = asyncio.create_task(_wrapped())
        self._tasks.add(task)
        if key is not None:
            self._tasks_by_key[key] = task
        task.add_done_callback(lambda done: self._on_task_done(done, key=key))

    def _on_task_done(self, task: asyncio.Task[Any], *, key: str | None) -> None:
        self._tasks.discard(task)
        if key is not None and self._tasks_by_key.get(key) is task:
            del self._tasks_by_key[key]
        if task.cancelled():
            return
        error = task.exception()
        if isinstance(error, WorkerUnhealthyError) and self._fatal_error is None:
            self._fatal_error = error
            if self._unhealthy_callback is not None:
                self._unhealthy_callback(error)

    def raise_if_unhealthy(self) -> None:
        """Stop claim admission after a fatal attempt-supervision failure."""

        if self._fatal_error is not None:
            raise self._fatal_error

    async def wait_all(self, timeout: float | None = None) -> None:
        if not self._tasks:
            return
        await asyncio.wait(self._tasks, timeout=timeout)

    def cancel_all(self) -> None:
        for t in self._tasks:
            t.cancel()
