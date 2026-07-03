"""Structured cancellation metadata for worker watchdogs.

The worker watchdog cancels the running trial task to stop in-container
subprocesses. A plain ``CancelledError`` is ambiguous, so the watchdog passes a
small object through ``Task.cancel(msg=...)`` and Trial.run can persist the
right terminal diagnostic.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WatchdogTriggerReason(StrEnum):
    CP_CANCELLED = "cp_cancelled"
    CP_TERMINAL = "cp_terminal"
    HARD_DEADLINE = "hard_deadline"


@dataclass(frozen=True)
class WatchdogCancellation:
    reason: WatchdogTriggerReason
    message: str
    elapsed_sec: float | None = None
    hard_deadline_sec: float | None = None
    cp_state: str | None = None


def extract_watchdog_cancellation(exc: BaseException) -> WatchdogCancellation | None:
    if not exc.args:
        return None
    value = exc.args[0]
    if isinstance(value, WatchdogCancellation):
        return value
    return None


def watchdog_timeout_failure_message(cancellation: WatchdogCancellation) -> str:
    parts = ["watchdog hard deadline exceeded"]
    if cancellation.elapsed_sec is not None:
        parts.append(f"elapsed_sec={cancellation.elapsed_sec:g}")
    if cancellation.hard_deadline_sec is not None:
        parts.append(f"hard_deadline_sec={cancellation.hard_deadline_sec:g}")
    if cancellation.message:
        parts.append(f"detail={cancellation.message}")
    return " ".join(parts)
