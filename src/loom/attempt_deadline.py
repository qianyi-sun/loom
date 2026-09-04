"""Monotonic absolute deadlines shared by one agent attempt."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field


class AttemptDeadlineExceededError(TimeoutError):
    """The owning agent attempt can no longer start or finish work."""


@dataclass(frozen=True, slots=True)
class AttemptDeadline:
    """An immutable absolute deadline measured on a monotonic clock.

    The clock is injectable so boundary behavior can be tested without using
    wall-clock time. Only ``monotonic_deadline`` is part of the deadline's
    identity; the clock itself is an observation mechanism.
    """

    monotonic_deadline: float
    _clock: Callable[[], float] = field(
        default=time.monotonic,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not math.isfinite(self.monotonic_deadline):
            raise ValueError("attempt deadline must be finite")

    @classmethod
    def after(
        cls,
        timeout_sec: float,
        *,
        clock: Callable[[], float] | None = None,
    ) -> AttemptDeadline:
        """Create a deadline ``timeout_sec`` from one monotonic observation.

        Async callers use the running event loop's clock. Synchronous callers
        fall back to :func:`time.monotonic`; tests may inject either clock.
        """

        if not math.isfinite(timeout_sec) or timeout_sec < 0:
            raise ValueError("attempt timeout must be finite and non-negative")
        if clock is None:
            try:
                clock = asyncio.get_running_loop().time
            except RuntimeError:
                clock = time.monotonic
        return cls(monotonic_deadline=clock() + timeout_sec, _clock=clock)

    def remaining(self) -> float:
        """Return the non-negative duration remaining on this attempt."""

        return max(0.0, self.monotonic_deadline - self._clock())

    def require_remaining(self) -> float:
        """Return remaining time or fail before downstream work can start."""

        remaining = self.remaining()
        if remaining <= 0:
            raise AttemptDeadlineExceededError("agent attempt deadline has been reached")
        return remaining

    @property
    def reached(self) -> bool:
        return self.remaining() <= 0
