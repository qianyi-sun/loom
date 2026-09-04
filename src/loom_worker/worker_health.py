"""Fatal worker-health signals that must stop claim admission."""

from __future__ import annotations


class WorkerUnhealthyError(RuntimeError):
    """The current process cannot safely execute another claimed item."""
