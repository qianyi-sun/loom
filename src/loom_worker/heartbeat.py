"""Heartbeat OS thread (spec §3.2).

Runs in a dedicated daemon thread, NOT the asyncio loop. This insulates
heartbeats from agent code that may transiently block the loop. The thread
polls a `stop` Event and exits cleanly on shutdown.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from uuid import UUID

logger = logging.getLogger(__name__)


class HeartbeatThread(threading.Thread):
    def __init__(
        self,
        *,
        worker_id: UUID,
        interval_sec: float,
        tick_fn: Callable[[], None],
    ) -> None:
        super().__init__(daemon=True, name=f"loom-heartbeat-{worker_id}")
        self._worker_id = worker_id
        self._interval_sec = interval_sec
        self._tick_fn = tick_fn
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._tick_fn()
            except Exception as exc:
                logger.warning(
                    "heartbeat_tick_failed worker=%s err=%s",
                    self._worker_id, exc,
                )
            self._stop_event.wait(self._interval_sec)
