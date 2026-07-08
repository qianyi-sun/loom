"""ProviderConnectionsWatcher: LISTEN + poll + reconnect (#190).

Uses an injected fake connection so no real Postgres is required.
The fake's `notifies()` yields off an asyncio.Queue tests can push
to; `close()` is recorded for cleanup assertions.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

import pytest

from loom_egress_xds.config_builder import (
    ProviderConnectionRow,
    Snapshot,
)
from loom_egress_xds.provider_connections_watcher import (
    ProviderConnectionsWatcher,
    WatcherConnection,
    WatcherSettings,
)


@dataclass
class _Row:
    id: UUID
    resolved_egress_ips: list[str] = field(default_factory=list)
    upstream_host: str = "example.com"
    base_url: str = "https://example.com/v1"
    deleted_at: datetime | None = None


_C1 = UUID("00000000-0000-0000-0000-000000000001")
_C2 = UUID("00000000-0000-0000-0000-000000000002")


class _FakeNotify:
    """Minimal NOTIFY event with a `payload` attribute, matching the
    shape the self-test probe's `getattr(note, 'payload', '')` check
    expects."""

    def __init__(self, payload: str) -> None:
        self.payload = payload


class _FakeConn(WatcherConnection):
    """Records close + serves notifications from an asyncio.Queue.

    `execute()` handles NOTIFY statements by auto-delivering the payload
    so the startup self-test (notify_round_trip) succeeds without a
    real Postgres connection.
    """

    def __init__(self) -> None:
        self.notify_queue: asyncio.Queue[object] = asyncio.Queue()
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def execute(self, sql: str) -> None:
        """Accept LISTEN/UNLISTEN silently; auto-deliver NOTIFY payloads
        so the self-test probe sees its own notification round-trip."""
        import re
        if sql.startswith("NOTIFY"):
            m = re.search(r"'([^']+)'", sql)
            if m:
                await self.notify_queue.put(_FakeNotify(m.group(1)))

    async def notifies(self) -> AsyncIterator[object]:
        while True:
            yield await self.notify_queue.get()


async def _drive_until(
    cond: asyncio.Event,
    timeout: float = 2.0,
) -> None:
    """Wait for `cond` with timeout; pytest.fail on timeout for clear
    error message rather than asyncio.TimeoutError."""
    try:
        await asyncio.wait_for(cond.wait(), timeout=timeout)
    except TimeoutError:
        pytest.fail(f"condition did not fire within {timeout}s")


# ─── happy path: rebuilds on notify ──────────────────────────────────


async def test_initial_snapshot_published_at_startup() -> None:
    rows: list[ProviderConnectionRow] = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
    ]
    published: list[Snapshot] = []
    got = asyncio.Event()

    async def callback(snap: Snapshot) -> None:
        published.append(snap)
        got.set()

    conn = _FakeConn()
    watcher = ProviderConnectionsWatcher(
        connection_factory=_factory(conn),
        row_fetcher=_fetcher(rows),
        on_snapshot=callback,
        settings=WatcherSettings(poll_interval_sec=10.0),
    )
    task = asyncio.create_task(watcher.run())
    try:
        await _drive_until(got)
        assert len(published) == 1
        assert published[0].lookup(_C1) is not None
    finally:
        await watcher.stop()
        await task


async def test_notify_triggers_rebuild() -> None:
    # Start with one connection; after the first publish, mutate the
    # row list + push a NOTIFY; expect a second publish with the new row.
    rows: list[ProviderConnectionRow] = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
    ]
    published: list[Snapshot] = []
    pub_event = asyncio.Event()

    async def callback(snap: Snapshot) -> None:
        published.append(snap)
        pub_event.set()

    conn = _FakeConn()
    watcher = ProviderConnectionsWatcher(
        connection_factory=_factory(conn),
        row_fetcher=_fetcher(rows),
        on_snapshot=callback,
        settings=WatcherSettings(poll_interval_sec=10.0),
    )
    task = asyncio.create_task(watcher.run())
    try:
        await _drive_until(pub_event)
        pub_event.clear()

        rows.append(_Row(id=_C2, resolved_egress_ips=["5.6.7.8"]))
        await conn.notify_queue.put(object())

        await _drive_until(pub_event)
        assert len(published) == 2
        assert published[1].lookup(_C2) is not None
    finally:
        await watcher.stop()
        await task


async def test_noop_notify_does_not_republish() -> None:
    # If the snapshot version hasn't changed, the callback must NOT
    # fire again. Otherwise Envoy thrashes its cluster pool.
    rows: list[ProviderConnectionRow] = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
    ]
    published: list[Snapshot] = []
    pub_event = asyncio.Event()

    async def callback(snap: Snapshot) -> None:
        published.append(snap)
        pub_event.set()

    conn = _FakeConn()
    watcher = ProviderConnectionsWatcher(
        connection_factory=_factory(conn),
        row_fetcher=_fetcher(rows),
        on_snapshot=callback,
        settings=WatcherSettings(poll_interval_sec=10.0),
    )
    task = asyncio.create_task(watcher.run())
    try:
        await _drive_until(pub_event)
        assert len(published) == 1
        pub_event.clear()

        # Push 3 NOTIFY events with no row change.
        for _ in range(3):
            await conn.notify_queue.put(object())
        # Give the run loop time to process.
        await asyncio.sleep(0.1)
        assert len(published) == 1  # still one
    finally:
        await watcher.stop()
        await task


# ─── poll fallback ──────────────────────────────────────────────────


async def test_poll_fallback_triggers_rebuild() -> None:
    # With a 50ms poll interval and NO notifications, the poll loop
    # alone should drive at least 2 rebuild ticks within ~200ms.
    # Combined with the no-op suppression, only the FIRST publish
    # actually fires (subsequent rebuilds produce the same version).
    # So we assert: callback fired once + poll task is alive (didn't crash).
    rows: list[ProviderConnectionRow] = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
    ]
    published: list[Snapshot] = []
    pub_event = asyncio.Event()
    fetch_count = 0

    async def callback(snap: Snapshot) -> None:
        published.append(snap)
        pub_event.set()

    async def counting_fetcher(_conn: WatcherConnection) -> list[ProviderConnectionRow]:
        nonlocal fetch_count
        fetch_count += 1
        return rows

    watcher = ProviderConnectionsWatcher(
        connection_factory=_factory(_FakeConn()),
        row_fetcher=counting_fetcher,
        on_snapshot=callback,
        settings=WatcherSettings(poll_interval_sec=0.05),
    )
    task = asyncio.create_task(watcher.run())
    try:
        await _drive_until(pub_event)
        # Wait ~3 poll intervals; expect at least 2 additional fetches
        # to confirm the poll loop is wired through.
        await asyncio.sleep(0.2)
        assert fetch_count >= 3, (
            f"poll fallback fired only {fetch_count} times in 200ms "
            f"with 50ms interval — poll loop wiring broken"
        )
    finally:
        await watcher.stop()
        await task


# ─── reconnect ───────────────────────────────────────────────────────


async def test_reconnects_after_listen_failure() -> None:
    # connection_factory returns conn1 first (will fail mid-stream),
    # then conn2 (succeeds). Expect at least one publish AFTER the
    # reconnect, proving the watcher recovered.
    conn1 = _FakeConn()
    conn2 = _FakeConn()
    conns = [conn1, conn2]
    factory_calls = 0

    async def factory() -> WatcherConnection:
        nonlocal factory_calls
        factory_calls += 1
        return conns.pop(0)

    rows: list[ProviderConnectionRow] = [
        _Row(id=_C1, resolved_egress_ips=["1.2.3.4"]),
    ]
    published: list[Snapshot] = []
    pub_event = asyncio.Event()

    async def callback(snap: Snapshot) -> None:
        published.append(snap)
        pub_event.set()

    watcher = ProviderConnectionsWatcher(
        connection_factory=factory,
        row_fetcher=_fetcher(rows),
        on_snapshot=callback,
        settings=WatcherSettings(
            poll_interval_sec=10.0,
            reconnect_backoff_base_sec=0.05,
            reconnect_backoff_max_sec=0.1,
        ),
    )
    task = asyncio.create_task(watcher.run())
    try:
        await _drive_until(pub_event)
        pub_event.clear()

        # Sentinel: pushing an Exception onto conn1's queue causes
        # `notifies()` to raise, which the listener loop converts
        # into a reconnect.
        await conn1.notify_queue.put(
            RuntimeError("simulated listen failure"),
        )

        # Let reconnect happen + its automatic post-reconnect publish
        # fire. Then clear the event so the next _drive_until waits
        # specifically for the row-change-driven publish.
        await asyncio.sleep(0.2)
        pub_event.clear()
        rows.append(_Row(id=_C2, resolved_egress_ips=["5.6.7.8"]))
        await conn2.notify_queue.put(object())

        await _drive_until(pub_event, timeout=3.0)
        assert factory_calls == 2, f"expected 2 factory calls (reconnect), got {factory_calls}"
        assert any(s.lookup(_C2) is not None for s in published), (
            "expected at least one snapshot containing the new row"
        )
        assert conn1.closed, "old connection must be closed on reconnect"
    finally:
        await watcher.stop()
        await task


# Make _FakeConn's `notifies()` raise when an exception is enqueued —
# overrides the default behavior for the reconnect test.
async def _exception_aware_notifies(self: _FakeConn) -> AsyncIterator[object]:
    while True:
        item = await self.notify_queue.get()
        if isinstance(item, Exception):
            raise item
        yield item


_FakeConn.notifies = _exception_aware_notifies  # type: ignore[method-assign,assignment]


# ─── factories used by the tests ────────────────────────────────────


def _factory(conn: WatcherConnection):
    async def make() -> WatcherConnection:
        return conn

    return make


def _fetcher(rows: list[ProviderConnectionRow]):
    async def fetch(_conn: WatcherConnection) -> list[ProviderConnectionRow]:
        return list(rows)

    return fetch
