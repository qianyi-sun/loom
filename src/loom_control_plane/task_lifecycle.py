"""Shared background-task shutdown, preserving join-before-resource-disposal."""

import asyncio
from collections.abc import Sequence


async def cancel_and_drain_tasks(
    tasks: Sequence[asyncio.Task[None] | None],
    *,
    grace_seconds: float = 5.0,
) -> None:
    active_tasks = tuple(task for task in tasks if task is not None)
    if not active_tasks:
        return
    for task in active_tasks:
        task.cancel()
    _done, pending = await asyncio.wait(active_tasks, timeout=grace_seconds)
    for task in pending:
        task.cancel()
    results = await asyncio.gather(*active_tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            raise result
