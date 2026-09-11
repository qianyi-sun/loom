"""One watchdog's suspend-aware permission state; not a watchdog or launch lock.

The installed runtime must run its deadline monitor independently of blocking
build/transport IO and kill exact runtime children when this state stops. Neither
a live return nor a stopped latch proves child containment or physical release.
A new instance does not authorize restarting an already consumed allocation.
"""

from __future__ import annotations

import time
from threading import Lock
from typing import NoReturn
from uuid import uuid4

from loom_capacity_agent.build_admission import (
    BuildClaimRequestV1,
    BuildExecutionPermitV1,
    BuildExecutionRequestV1,
)

_MAX_LIFETIME_NS = 10_000_000_000
_STOP_REASONS = frozenset({"cancelled", "renewal-failed", "cleanup", "completed",
    "expired", "clock-failed", "protocol-error"})


class NativeExecutionDeadline:
    """Serialized local transitions for one immutable claim/source identity.

    Transport retries reuse the request from begin_request, never begin again.
    Any failed renewal must call stop('renewal-failed'); a pending renewal does
    not extend the previous permission. The monitor must call require_live on
    its own deadline, even while the request's network operation is blocked.
    """

    def __init__(self, claim: BuildClaimRequestV1, *, source_binding_sha256: str) -> None:
        self._template = BuildExecutionRequestV1.model_validate_json(BuildExecutionRequestV1(
            claim=claim, challenge=uuid4(), source_binding_sha256=source_binding_sha256).model_dump_json())
        self._lock = Lock()
        self._stopped: str | None = None
        self._last_clock: int | None = None
        self._deadline: int | None = None
        self._pending: tuple[BuildExecutionRequestV1, int] | None = None
        self._read_clock()

    def _fail(self, reason: str) -> NoReturn:
        self._stopped = self._stopped or reason
        self._pending = None
        raise RuntimeError(f"native execution stopped: {self._stopped}")

    def _read_clock(self) -> int:
        try:
            now = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
        except (AttributeError, OSError, ValueError):
            self._fail("clock-failed")
        if type(now) is not int or now < 0 or (self._last_clock is not None and now < self._last_clock):
            self._fail("clock-failed")
        self._last_clock = now
        return now

    def _check(self) -> int:
        if self._stopped is not None:
            self._fail(self._stopped)
        now = self._read_clock()
        if (self._deadline is not None and now >= self._deadline) or (
            self._pending is not None and now >= self._pending[1] + _MAX_LIFETIME_NS
        ):
            self._fail("expired")
        return now

    def begin_request(self) -> BuildExecutionRequestV1:
        with self._lock:
            now = self._check()
            if self._pending is not None:
                self._fail("protocol-error")
            request = self._template.model_copy(update={"challenge": uuid4()})
            self._pending = (request, now)
            return request

    def accept(self, permit: BuildExecutionPermitV1) -> None:
        with self._lock:
            self._check()
            try:
                permit = BuildExecutionPermitV1.model_validate_json(permit.model_dump_json())
            except (AttributeError, ValueError):
                self._fail("protocol-error")
            if self._pending is None or permit.request != self._pending[0]:
                self._fail("protocol-error")
            lifetime = permit.not_after - permit.issued_at
            lifetime_ns = (lifetime.seconds * 1_000_000 + lifetime.microseconds) * 1000
            deadline = self._pending[1] + lifetime_ns
            # Include validation/lock/transport delay, and never bridge an
            # expired old permission even if a new reply offers a later time.
            if self._check() >= deadline:
                self._fail("expired")
            self._deadline = deadline
            self._pending = None

    def require_live(self) -> int:
        """Return the absolute BOOTTIME deadline, not child-start permission."""
        with self._lock:
            self._check()
            if self._deadline is None:
                self._fail("protocol-error")
            return self._deadline

    def poll_deadline(self) -> int:
        """Poll expiry while awaiting a reply; this never grants execution."""
        with self._lock:
            self._check()
            deadlines = ([] if self._deadline is None else [self._deadline])
            if self._pending is not None:
                deadlines.append(self._pending[1] + _MAX_LIFETIME_NS)
            if not deadlines:
                self._fail("protocol-error")
            return min(deadlines)

    def stop(self, reason: str) -> None:
        with self._lock:
            if reason not in _STOP_REASONS:
                self._fail("protocol-error")
            self._stopped = self._stopped or reason
            self._pending = None

    @property
    def stopped_reason(self) -> str | None:
        """Last observed terminal reason; reading this does not poll the clock."""
        with self._lock:
            return self._stopped
