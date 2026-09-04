"""Per-worker semaphore-gated runner pool."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

from loom_worker.worker_health import WorkerUnhealthyError


class RunnerPool:
    """`max_concurrent` Trial or ExecutionAttempt work items may execute.

    spawn() registers a coroutine that will acquire the semaphore before its
    body runs; concurrent spawns past the limit park inside the semaphore
    until earlier work completes. `in_flight` reflects total registered
    (running + parked) work.
    """

    def __init__(self, max_concurrent: int) -> None:
        self._sem = asyncio.Semaphore(max_concurrent)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._fatal_error: WorkerUnhealthyError | None = None

    @property
    def in_flight(self) -> int:
        return len(self._tasks)

    async def spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        async def _wrapped() -> None:
            async with self._sem:
                await coro
        task: asyncio.Task[None] = asyncio.create_task(_wrapped())
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if isinstance(error, WorkerUnhealthyError) and self._fatal_error is None:
            self._fatal_error = error

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
