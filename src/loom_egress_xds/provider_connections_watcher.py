"""Postgres LISTEN watcher for `provider_connections_changed` (#190).

Triggers a snapshot rebuild on every NOTIFY, with a periodic poll
fallback so we don't sit with stale data if the LISTEN connection
silently drops. Reconnects on `psycopg.OperationalError` /
`PostgresError` with capped backoff.

Design:
- Holds ONE long-lived async connection in autocommit mode (LISTEN
  doesn't work inside a transaction in Postgres).
- A background task awaits `conn.notifies()` and pushes a sentinel
  onto an asyncio.Event whenever a notification arrives.
- A second background task ticks the same event every `poll_interval`
  as a safety net.
- The main coroutine awaits the event, rebuilds the snapshot via
  `_query_rows + build_snapshot`, and invokes the caller's
  `on_snapshot(snapshot)` callback.

Why an Event + push model rather than streaming snapshots out via
queue: the consumer (xDS server, PR-C1b) only cares about the latest
snapshot — old ones become noise. Push semantics avoid the
ABA/coalescing logic a queue would need.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from loom_egress_xds.config_builder import (
    ProviderConnectionRow,
    Snapshot,
    build_snapshot,
)

logger = logging.getLogger(__name__)

_CHANNEL = "provider_connections_changed"
_DEFAULT_POLL_INTERVAL_SEC = 30.0
_RECONNECT_BACKOFF_BASE_SEC = 0.5
_RECONNECT_BACKOFF_MAX_SEC = 30.0

# Snapshot callback. Awaited; if it raises, the watcher logs and
# carries on (one bad consumer shouldn't kill the watcher).
SnapshotCallback = Callable[[Snapshot], Awaitable[None]]

# Connection factory + row fetcher are injected so the watcher is
# testable without a live Postgres. Production wires these to psycopg.
ConnectionFactory = Callable[[], Awaitable["WatcherConnection"]]
RowFetcher = Callable[
    ["WatcherConnection"],
    Awaitable[list[ProviderConnectionRow]],
]


class WatcherConnection:
    """Abstract interface the watcher needs from a Postgres connection.

    Production impl wraps `psycopg.AsyncConnection` (autocommit on,
    `LISTEN <channel>` executed at acquire time). Tests substitute
    an in-memory fake.
    """

    async def close(self) -> None:  # pragma: no cover - protocol stub
        raise NotImplementedError

    def notifies(self) -> AsyncIterator[Any]:  # pragma: no cover
        """Async iterator yielding NOTIFY events. The watcher does
        not inspect the events; presence-of-event is the signal.

        Implementations return an async iterator (e.g. by being a
        function that contains an `async for` + `yield`). Declared
        as a regular method, not `async def`, so the body can
        `return self._stream` from a stored iterator."""
        raise NotImplementedError


@dataclass
class WatcherSettings:
    poll_interval_sec: float = _DEFAULT_POLL_INTERVAL_SEC
    reconnect_backoff_base_sec: float = _RECONNECT_BACKOFF_BASE_SEC
    reconnect_backoff_max_sec: float = _RECONNECT_BACKOFF_MAX_SEC


class ProviderConnectionsWatcher:
    """Long-lived watcher that drives snapshot rebuilds.

    Use as `async with watcher:` to ensure the LISTEN connection is
    closed cleanly on shutdown, OR `await watcher.run()` to block the
    current coroutine until cancelled.
    """

    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory,
        row_fetcher: RowFetcher,
        on_snapshot: SnapshotCallback,
        settings: WatcherSettings | None = None,
    ) -> None:
        self._connection_factory = connection_factory
        self._row_fetcher = row_fetcher
        self._on_snapshot = on_snapshot
        self._settings = settings or WatcherSettings()
        self._wake = asyncio.Event()
        # Last-published version, for short-circuiting no-op pushes.
        self._last_version: str | None = None
        self._stop = asyncio.Event()

    def request_refresh(self) -> None:
        """Cause the run loop to rebuild the snapshot on its next
        iteration. Idempotent; useful for tests and operator nudges."""
        self._wake.set()

    async def run(self) -> None:
        """Run until `stop()` is called. Reconnects forever on
        Postgres errors."""
        # Always do one rebuild at startup so consumers don't sit
        # with an empty snapshot waiting for the first NOTIFY.
        self._wake.set()

        poll_task = asyncio.create_task(self._poll_loop())
        try:
            backoff = self._settings.reconnect_backoff_base_sec
            while not self._stop.is_set():
                conn: WatcherConnection | None = None
                listener_task: asyncio.Task[None] | None = None
                try:
                    conn = await self._connection_factory()
                    backoff = self._settings.reconnect_backoff_base_sec
                    listener_task = asyncio.create_task(
                        self._listen_loop(conn),
                    )
                    await self._consume_until_disconnect(
                        conn,
                        listener_task,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "provider_connections_watcher_reconnect backoff=%.2fs err=%s",
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(
                        backoff * 2,
                        self._settings.reconnect_backoff_max_sec,
                    )
                finally:
                    if listener_task is not None:
                        listener_task.cancel()
                        # Drain the listener task. It may complete
                        # via CancelledError (we just cancelled it)
                        # OR via the connection-drop exception that
                        # caused this iteration to exit — both are
                        # already handled. Catch (Exception,
                        # CancelledError) but NOT BaseException so
                        # SystemExit / KeyboardInterrupt still
                        # propagate cleanly during process shutdown.
                        try:
                            await listener_task
                        except (Exception, asyncio.CancelledError):
                            pass
                    if conn is not None:
                        async with _suppress_exceptions():
                            await conn.close()
                    # Force a fresh rebuild after reconnect — the
                    # next iteration acquires a new connection AND
                    # publishes a snapshot built from the freshly
                    # read row state.
                    self._last_version = None
                    self._wake.set()
        finally:
            poll_task.cancel()
            async with _suppress_cancelled():
                await poll_task

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()  # let any awaiter unblock

    async def __aenter__(self) -> ProviderConnectionsWatcher:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    # ─── internal ────────────────────────────────────────────────

    async def _consume_until_disconnect(
        self,
        conn: WatcherConnection,
        listener_task: asyncio.Task[None],
    ) -> None:
        """Loop on the wake event; rebuild snapshot each tick. Exits
        when the listener task fails (re-raising its exception so the
        outer loop reconnects).

        The wake event is set by the listener loop (on NOTIFY) and by
        the poll loop (every poll_interval). The connection isn't
        used directly here — the row_fetcher might use it or open its
        own pool; that's its choice.
        """
        wake_task = asyncio.create_task(self._wake.wait())
        try:
            while not self._stop.is_set():
                # Race the wake event against the listener task. If
                # the listener died (connection dropped, network blip,
                # Postgres restart) we tear down and reconnect.
                done, _ = await asyncio.wait(
                    {wake_task, listener_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if listener_task in done:
                    exc = listener_task.exception()
                    if exc is None:
                        raise RuntimeError(
                            "listener task exited cleanly — unexpected",
                        )
                    raise exc
                # wake_task fired.
                self._wake.clear()
                wake_task = asyncio.create_task(self._wake.wait())
                if self._stop.is_set():
                    return
                await self._rebuild_and_publish(conn)
        finally:
            wake_task.cancel()
            async with _suppress_cancelled():
                await wake_task

    async def _rebuild_and_publish(self, conn: WatcherConnection) -> None:
        try:
            rows = await self._row_fetcher(conn)
        except Exception:
            logger.exception(
                "provider_connections_watcher_fetch_failed",
            )
            return
        snapshot = build_snapshot(rows)
        if snapshot.version == self._last_version:
            return
        self._last_version = snapshot.version
        try:
            await self._on_snapshot(snapshot)
        except Exception:
            logger.exception(
                "provider_connections_watcher_callback_failed version=%s",
                snapshot.version,
            )

    async def _listen_loop(self, conn: WatcherConnection) -> None:
        """Consume the connection's NOTIFY stream forever. Each
        notification sets the wake event; we don't carry payload.

        Re-raises any exception so the outer reconnect loop sees it.
        Logging happens ONCE in `run()`'s reconnect branch; this
        method intentionally does NOT log to avoid the same error
        landing in the logs twice with the same stack.
        """
        async for _ in conn.notifies():
            self._wake.set()

    async def _poll_loop(self) -> None:
        """Tick the wake event every poll_interval. Belt-and-braces
        for the case where the LISTEN connection is alive but silently
        dropped notifications."""
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self._settings.poll_interval_sec)
                self._wake.set()
        except asyncio.CancelledError:
            raise


# ─── helpers ──────────────────────────────────────────────────────────


@asynccontextmanager
async def _suppress_cancelled() -> AsyncIterator[None]:
    try:
        yield
    except asyncio.CancelledError:
        pass


@asynccontextmanager
async def _suppress_exceptions() -> AsyncIterator[None]:
    try:
        yield
    except Exception:
        logger.exception("watcher_cleanup_suppressed")


# Concrete consumers will pass the migration-installed channel name to
# their psycopg connection so this stays a string-only public constant.
CHANNEL_NAME = _CHANNEL


def make_row_fetcher_query() -> str:
    """SQL the production row_fetcher runs against `provider_connections`.
    Exported so tests can pin the query against the live schema; the
    watcher itself doesn't run SQL (so it stays driver-agnostic).

    Column set matches the `ProviderConnectionRow` Protocol exactly
    — adding columns here without updating the Protocol risks the
    fetcher returning fields nothing consumes, which is exactly the
    bug this query had pre-fixup (phantom `team_id` left over from
    an earlier draft)."""
    return (
        "SELECT id, resolved_egress_ips, upstream_host, base_url, deleted_at "
        "FROM provider_connections"
    )


__all__ = [
    "CHANNEL_NAME",
    "ProviderConnectionRow",
    "ProviderConnectionsWatcher",
    "Snapshot",
    "WatcherConnection",
    "WatcherSettings",
    "make_row_fetcher_query",
]
