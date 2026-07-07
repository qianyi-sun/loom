"""NOTIFY round-trip probe for LISTEN watchers (#609).

Watchers connecting to Postgres via a pooler in transaction mode
have their backend recycled between transactions, which silently
breaks LISTEN/NOTIFY delivery. This probe verifies at startup that
the watcher's connection preserves session state by issuing a NOTIFY
on a synthetic channel and observing it round-trip.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Protocol

logger = logging.getLogger(__name__)

_SELF_TEST_CHANNEL = "loom_watcher_selftest"


class _AsyncListenConnection(Protocol):
    async def execute(self, sql: str) -> Any: ...
    def notifies(self) -> Any: ...  # returns AsyncIterator


async def notify_round_trip(
    conn: _AsyncListenConnection,
    *,
    timeout_sec: float = 1.0,
) -> bool:
    """Issue a NOTIFY on a synthetic channel and wait for it to arrive
    on the same connection. Returns True if the round-trip completed
    within timeout_sec, False otherwise.

    Callers should treat False as "connection does not preserve session
    semantics" (i.e., misconfigured pooler in the path) and fall back
    to poll mode with a Prometheus alert.
    """
    payload = str(uuid.uuid4())
    await conn.execute(f"LISTEN {_SELF_TEST_CHANNEL}")
    await conn.execute(f"NOTIFY {_SELF_TEST_CHANNEL}, '{payload}'")

    async def _wait_for_payload() -> bool:
        async for note in conn.notifies():
            if getattr(note, "payload", "") == payload:
                return True
        return False

    try:
        return await asyncio.wait_for(_wait_for_payload(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        logger.warning(
            "listen_watcher_selftest_timeout channel=%s payload=%s timeout_sec=%s "
            "— watcher will fall back to poll-only mode",
            _SELF_TEST_CHANNEL, payload, timeout_sec,
        )
        return False
    finally:
        try:
            await conn.execute(f"UNLISTEN {_SELF_TEST_CHANNEL}")
        except Exception:  # noqa: BLE001
            logger.debug("listen_watcher_selftest_unlisten_failed", exc_info=True)
