"""Entry point: wires the Postgres watcher + xDS server + signal
handling into one long-lived process (#190 PR-C1b).

Env vars:
    LOOM_EGRESS_XDS_DB_URL          psycopg-style URL for the Loom
                                    Postgres (e.g. `postgresql://...`).
                                    Required.
    LOOM_EGRESS_XDS_LISTEN_ADDR     gRPC bind address (default
                                    0.0.0.0:18000).
    LOOM_EGRESS_XDS_LOG_LEVEL       Python log level (default INFO).

Lifecycle:
1. Open a long-lived `psycopg.AsyncConnection` in autocommit mode +
   issue `LISTEN provider_connections_changed`.
2. Construct `ProviderConnectionsWatcher` with the connection + a
   row-fetcher that re-queries `provider_connections` on each wake.
3. Construct `XdsSnapshotCache` + register CDS/RDS servicers on a
   grpc.aio server.
4. Wire `watcher.on_snapshot = cache.publish_snapshot`.
5. `await asyncio.gather(server.start(), watcher.run())`.

Shutdown:
- SIGTERM / SIGINT triggers `watcher.stop()` + `server.stop(grace)`.
- The watcher's `_listen_loop` exits cleanly via CancelledError; the
  gRPC server stops new streams + drains in-flight RPCs for `grace`
  seconds before hard-closing.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg import AsyncConnection

from loom_egress_xds.config_builder import ProviderConnectionRow
from loom_egress_xds.provider_connections_watcher import (
    CHANNEL_NAME,
    ProviderConnectionsWatcher,
    WatcherConnection,
    make_row_fetcher_query,
)
from loom_egress_xds.xds_server import XdsSnapshotCache, build_grpc_server

logger = logging.getLogger(__name__)

_DEFAULT_LISTEN_ADDR = "0.0.0.0:18000"
_GRACEFUL_STOP_SECONDS = 5.0


@dataclass
class _PgRow:
    """ProviderConnectionRow Protocol impl wrapping a psycopg row.
    Lives here (not in config_builder) so the egress-xds package's
    pure logic stays driver-agnostic.

    Intentionally NOT frozen: the ProviderConnectionRow Protocol
    declares its fields as plain attributes (writable from the
    Protocol's POV); frozen=True makes mypy reject the assignment
    of `_PgRow` instances to a `list[ProviderConnectionRow]`."""

    id: UUID
    resolved_egress_ips: list[str]
    upstream_host: str
    deleted_at: datetime | None


class _PsycopgWatcherConnection(WatcherConnection):
    """Adapts `psycopg.AsyncConnection` to the WatcherConnection
    Protocol. The connection is held in autocommit mode + has issued
    `LISTEN <channel>` before being handed over."""

    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def close(self) -> None:
        await self._conn.close()

    def notifies(self) -> AsyncIterator[Any]:
        # psycopg exposes notifies as an async generator on the
        # connection. Each iteration yields a `Notify` object.
        return self._conn.notifies()


async def _open_listen_connection(db_url: str) -> _PsycopgWatcherConnection:
    """Open + configure a psycopg async connection for LISTEN.
    Caller (the watcher's connection_factory) handles reconnect on
    error — this just builds one connection ready to consume
    notifications."""
    conn = await psycopg.AsyncConnection.connect(db_url, autocommit=True)
    await conn.execute(f"LISTEN {CHANNEL_NAME}")
    return _PsycopgWatcherConnection(conn)


async def _fetch_rows(
    _conn: WatcherConnection,
) -> list[ProviderConnectionRow]:
    """Open a SHORT-LIVED connection per fetch.

    Why not reuse the LISTEN connection: psycopg's autocommit + the
    notify channel make the LISTEN connection a streaming consumer.
    Reusing it for SELECTs would interleave query results with
    notifications and complicate the async iteration. A fresh
    connection per fetch is cheap (postgres connection pool will
    cache) and operationally cleaner.

    The `_conn` argument is the WatcherConnection the watcher was
    notified on; we ignore it and open a new connection from env."""
    db_url = os.environ["LOOM_EGRESS_XDS_DB_URL"]
    async with await psycopg.AsyncConnection.connect(db_url) as conn:
        async with conn.cursor() as cur:
            await cur.execute(make_row_fetcher_query())
            rows = await cur.fetchall()
    return [
        _PgRow(
            id=row[0],
            # ARRAY(INET) comes back as list of `ipaddress.IPv4Address`
            # objects; coerce to str so the translator and snapshot
            # builder see the same shape as our test fakes.
            resolved_egress_ips=[str(ip) for ip in row[1]],
            upstream_host=row[2],
            deleted_at=row[3],
        )
        for row in rows
    ]


async def main() -> None:
    log_level = os.environ.get("LOOM_EGRESS_XDS_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    db_url = os.environ["LOOM_EGRESS_XDS_DB_URL"]
    listen_addr = os.environ.get(
        "LOOM_EGRESS_XDS_LISTEN_ADDR", _DEFAULT_LISTEN_ADDR,
    )

    cache = XdsSnapshotCache()
    server = build_grpc_server(cache, listen_addr=listen_addr)

    async def connection_factory() -> WatcherConnection:
        return await _open_listen_connection(db_url)

    watcher = ProviderConnectionsWatcher(
        connection_factory=connection_factory,
        row_fetcher=_fetch_rows,
        on_snapshot=cache.publish_snapshot,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    await server.start()
    logger.info("loom_egress_xds_listening addr=%s", listen_addr)

    watcher_task = asyncio.create_task(watcher.run())
    stop_wait = asyncio.create_task(stop_event.wait())

    try:
        done, _ = await asyncio.wait(
            {watcher_task, stop_wait},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if watcher_task in done and not stop_wait.done():
            # Watcher exited on its own (shouldn't happen unless
            # cancelled or unhandled exception). Surface it.
            exc = watcher_task.exception()
            if exc is not None:
                logger.exception(
                    "watcher_exited_unexpectedly", exc_info=exc,
                )
    finally:
        logger.info("loom_egress_xds_shutting_down")
        # `watcher.stop()` sets the stop event; the run loop checks
        # it on every iteration AND on wake, so the task exits
        # cleanly on its own. No explicit `.cancel()` needed.
        # Bound the wait so a wedged watcher doesn't block the
        # server's graceful drain.
        await watcher.stop()
        try:
            await asyncio.wait_for(watcher_task, timeout=5.0)
        except (TimeoutError, Exception):
            watcher_task.cancel()
            try:
                await watcher_task
            except (Exception, asyncio.CancelledError):
                pass
        await server.stop(grace=_GRACEFUL_STOP_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
