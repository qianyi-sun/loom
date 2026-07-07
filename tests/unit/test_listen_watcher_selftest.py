from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest


class _FakeAsyncConnection:
    """Minimal fake matching the psycopg.AsyncConnection surface used
    by notify_round_trip: execute + notifies."""

    def __init__(self, *, deliver_notification: bool = True) -> None:
        self._deliver = deliver_notification
        self._notify_queue: asyncio.Queue[_FakeNotify] = asyncio.Queue()
        self.executed_sql: list[str] = []

    async def execute(self, sql: str) -> None:
        self.executed_sql.append(sql)
        if sql.startswith("NOTIFY") and self._deliver:
            import re
            m = re.search(r"'([^']+)'", sql)
            if m:
                await self._notify_queue.put(_FakeNotify(m.group(1)))

    def notifies(self) -> AsyncIterator[Any]:
        return self._iter_notifies()

    async def _iter_notifies(self) -> AsyncIterator[Any]:
        while True:
            n = await self._notify_queue.get()
            yield n


class _FakeNotify:
    def __init__(self, payload: str) -> None:
        self.payload = payload


@pytest.mark.asyncio
async def test_notify_round_trip_returns_true_on_delivery() -> None:
    from loom_listen.self_test import notify_round_trip

    conn = _FakeAsyncConnection(deliver_notification=True)
    result = await notify_round_trip(conn, timeout_sec=1.0)
    assert result is True
    assert any(sql.startswith("LISTEN loom_watcher_selftest") for sql in conn.executed_sql)
    assert any(sql.startswith("NOTIFY loom_watcher_selftest") for sql in conn.executed_sql)


@pytest.mark.asyncio
async def test_notify_round_trip_returns_false_on_timeout() -> None:
    from loom_listen.self_test import notify_round_trip

    conn = _FakeAsyncConnection(deliver_notification=False)
    result = await notify_round_trip(conn, timeout_sec=0.1)
    assert result is False


@pytest.mark.asyncio
async def test_notify_round_trip_ignores_unrelated_payloads() -> None:
    """Notifications from a different UUID on the same channel must
    not be counted — otherwise misconfiguration is masked."""
    from loom_listen.self_test import notify_round_trip

    conn = _FakeAsyncConnection(deliver_notification=False)
    # Pre-inject a mismatched-payload notification.
    await conn._notify_queue.put(_FakeNotify("different-payload"))
    result = await notify_round_trip(conn, timeout_sec=0.1)
    assert result is False


@pytest.mark.asyncio
async def test_notify_round_trip_issues_unlisten_on_success() -> None:
    """Best-effort cleanup: probe should UNLISTEN after itself to avoid
    leaking the synthetic channel subscription."""
    from loom_listen.self_test import notify_round_trip

    conn = _FakeAsyncConnection(deliver_notification=True)
    await notify_round_trip(conn, timeout_sec=1.0)
    assert any(sql.startswith("UNLISTEN loom_watcher_selftest") for sql in conn.executed_sql)
