"""Fatal worker-health signals that must stop claim admission."""

from __future__ import annotations

import os

WORKER_UNHEALTHY_EXIT_CODE = 70


class WorkerUnhealthyError(RuntimeError):
    """The current process cannot safely execute another claimed item."""


def hard_exit_unhealthy_worker(_error: WorkerUnhealthyError) -> None:
    """Exit without asyncio shutdown waiting on an uncancellable agent task."""

    os._exit(WORKER_UNHEALTHY_EXIT_CODE)
