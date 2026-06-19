"""End-to-end: Postgres → watcher → xds-server gRPC (#190 / #201 partial).

Exercises the Loom-owned chain in one process against a real
Postgres:

1. Spin up Postgres (testcontainers) + run migrations (creates
   `provider_connections` + the NOTIFY trigger from #192).
2. Insert a fixture `provider_connections` row.
3. Start the production xds-server's components (watcher + snapshot
   cache + grpc.aio CDS/RDS servicers) in-process pointing at the
   test Postgres.
4. Open a gRPC client connection (using the same envoy-data-plane
   protobufs the production Envoy would use) and call
   `StreamClusters` + `StreamRoutes`.
5. Assert the responses contain the expected per-connection
   `Cluster` + `RouteConfiguration` shapes.

What it does NOT exercise:
- Envoy's actual CDS/RDS consumption — validated by the spike
  (#196) and continuously exercised by dev compose
  (`deploy/docker-compose.dev.yml`). Putting an Envoy container in
  this test repeatedly tripped on host/container network setup that
  varies across CI / dev machines; the value vs. the brittleness
  didn't pencil. PR-201 may revisit with a full compose-stack test
  once dependencies stabilize.
- Forward-proxy behavior through Envoy — Envoy's responsibility,
  not ours.

Skipped automatically when no docker daemon is reachable or
envoy-data-plane isn't installed.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

pytest.importorskip("envoy.config.cluster.v3.cluster_pb2")
pytest.importorskip("docker")


import grpc
import psycopg
from envoy.config.cluster.v3.cluster_pb2 import Cluster
from envoy.config.route.v3.route_pb2 import RouteConfiguration
from envoy.service.cluster.v3 import cds_pb2_grpc
from envoy.service.discovery.v3.discovery_pb2 import (
    DiscoveryRequest,
)
from envoy.service.route.v3 import rds_pb2_grpc
from testcontainers.postgres import PostgresContainer

from loom_egress_xds.config_builder import ProviderConnectionRow
from loom_egress_xds.provider_connections_watcher import (
    CHANNEL_NAME,
    ProviderConnectionsWatcher,
    WatcherConnection,
    WatcherSettings,
    make_row_fetcher_query,
)
from loom_egress_xds.xds_server import (
    XdsSnapshotCache,
    build_grpc_server,
)

pytestmark = pytest.mark.docker

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _pick_free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=_REPO_ROOT,
            check=True,
        )
        yield url


class _PsycopgWatcherConnection(WatcherConnection):
    def __init__(self, conn: psycopg.AsyncConnection) -> None:
        self._conn = conn

    async def close(self) -> None:
        await self._conn.close()

    def notifies(self) -> AsyncIterator[object]:
        return self._conn.notifies()


@dataclass
class _Row:
    id: UUID
    resolved_egress_ips: list[str]
    upstream_host: str
    base_url: str
    deleted_at: datetime | None


async def _open_listen_conn(db_url: str) -> _PsycopgWatcherConnection:
    conn = await psycopg.AsyncConnection.connect(db_url, autocommit=True)
    await conn.execute(f"LISTEN {CHANNEL_NAME}")
    return _PsycopgWatcherConnection(conn)


def _make_row_fetcher(db_url: str):
    async def _fetch(_conn: WatcherConnection) -> list[ProviderConnectionRow]:
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(make_row_fetcher_query())
                rows = await cur.fetchall()
        return [
            _Row(
                id=r[0],
                resolved_egress_ips=[str(ip) for ip in r[1]],
                upstream_host=r[2],
                base_url=r[3],
                deleted_at=r[4],
            )
            for r in rows
        ]

    return _fetch


@pytest.mark.timeout(60)
async def test_xds_server_publishes_cluster_and_route_from_postgres(
    postgres_url: str,
) -> None:
    """Insert one provider_connection row; open a gRPC client to the
    in-process xds-server; assert the Cluster (CDS) and
    RouteConfiguration (RDS) responses carry the expected per-tenant
    shape."""
    team_id = uuid4()
    connection_id = uuid4()
    upstream_host = "test-upstream.example.com"
    allowed_ip = "10.99.99.99"
    http_connection_id = uuid4()
    http_upstream_host = "192.168.32.1"
    http_allowed_ip = "192.168.32.1"

    psycopg_url = postgres_url.replace(
        "postgresql+psycopg://",
        "postgresql://",
    )

    # ── 1. Insert fixture rows ──────────────────────────────────────
    with psycopg.connect(psycopg_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO teams (id, name, created_at) VALUES (%s, %s, NOW())",
                (str(team_id), f"egress-xds-test-{team_id}"),
            )
            cur.execute(
                """
                INSERT INTO provider_connections (
                    id, team_id, provider_type, display_name,
                    base_url, upstream_host, resolved_egress_ips,
                    encrypted_api_key_ref, created_by,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, 'openai-compatible',
                    'xds-test', 'https://test-upstream.example.com',
                    %s, ARRAY[%s]::inet[], 'loom://test',
                    'test:egress-xds-integration',
                    NOW(), NOW()
                )
                """,
                (str(connection_id), str(team_id), upstream_host, allowed_ip),
            )
            cur.execute(
                """
                INSERT INTO provider_connections (
                    id, team_id, provider_type, display_name,
                    base_url, upstream_host, resolved_egress_ips,
                    encrypted_api_key_ref, created_by,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, 'openai-compatible',
                    'xds-http-test', 'http://192.168.32.1:28001/v1',
                    %s, ARRAY[%s]::inet[], 'loom://test',
                    'test:egress-xds-integration',
                    NOW(), NOW()
                )
                """,
                (
                    str(http_connection_id),
                    str(team_id),
                    http_upstream_host,
                    http_allowed_ip,
                ),
            )

    # ── 2. Build the in-process xds-server ─────────────────────────
    xds_port = _pick_free_port()
    cache = XdsSnapshotCache()
    grpc_server = build_grpc_server(cache, listen_addr=f"127.0.0.1:{xds_port}")

    async def conn_factory() -> WatcherConnection:
        return await _open_listen_conn(psycopg_url)

    watcher = ProviderConnectionsWatcher(
        connection_factory=conn_factory,
        row_fetcher=_make_row_fetcher(psycopg_url),
        on_snapshot=cache.publish_snapshot,
        settings=WatcherSettings(poll_interval_sec=2.0),
    )

    await grpc_server.start()
    watcher_task = asyncio.create_task(watcher.run())

    try:
        # Wait for the watcher to publish its initial snapshot.
        for _ in range(100):
            if cache._config is not None:
                break
            await asyncio.sleep(0.05)
        assert cache._config is not None, "watcher never published initial snapshot — fetch broken"

        # ── 3. Open a gRPC client + call StreamClusters ────────────
        async with grpc.aio.insecure_channel(f"127.0.0.1:{xds_port}") as ch:
            cds_stub = cds_pb2_grpc.ClusterDiscoveryServiceStub(ch)
            rds_stub = rds_pb2_grpc.RouteDiscoveryServiceStub(ch)

            cds_resp = await _request_once(cds_stub.StreamClusters)
            rds_resp = await _request_once(rds_stub.StreamRoutes)

        # ── 4. Assertions: CDS response shape ──────────────────────
        assert cds_resp.type_url == ("type.googleapis.com/envoy.config.cluster.v3.Cluster")
        clusters = [_unpack(r, Cluster) for r in cds_resp.resources]
        assert len(clusters) == 2
        clusters_by_name = {c.name: c for c in clusters}
        cluster = clusters_by_name[f"egress-{connection_id}"]
        assert cluster.name == f"egress-{connection_id}"
        assert cluster.type == Cluster.DiscoveryType.STATIC
        endpoints = cluster.load_assignment.endpoints[0].lb_endpoints
        addrs = [e.endpoint.address.socket_address.address for e in endpoints]
        assert addrs == [allowed_ip]
        ports = [e.endpoint.address.socket_address.port_value for e in endpoints]
        assert ports == [443]

        http_cluster = clusters_by_name[f"egress-{http_connection_id}"]
        http_endpoint = (
            http_cluster.load_assignment.endpoints[0]
            .lb_endpoints[0]
            .endpoint.address.socket_address
        )
        assert http_endpoint.address == http_allowed_ip
        assert http_endpoint.port_value == 28001

        # ── 5. Assertions: RDS response shape ──────────────────────
        assert rds_resp.type_url == ("type.googleapis.com/envoy.config.route.v3.RouteConfiguration")
        assert len(rds_resp.resources) == 1
        rc = _unpack(rds_resp.resources[0], RouteConfiguration)
        assert rc.name == "loom_egress_routes"
        assert len(rc.virtual_hosts) == 1
        routes = rc.virtual_hosts[0].routes
        assert len(routes) == 2
        routes_by_cluster = {r.route.cluster: r for r in routes}
        route = routes_by_cluster[f"egress-{connection_id}"]
        by_name = {h.name: h.string_match.exact for h in route.match.headers}
        assert by_name == {
            "x-loom-connection-id": str(connection_id),
            ":authority": f"{upstream_host}:443",
        }
        assert route.match.HasField("connect_matcher")
        assert route.route.cluster == f"egress-{connection_id}"

        http_route = routes_by_cluster[f"egress-{http_connection_id}"]
        http_by_name = {h.name: h.string_match.exact for h in http_route.match.headers}
        assert http_by_name == {
            "x-loom-connection-id": str(http_connection_id),
            ":authority": f"{http_upstream_host}:28001",
        }
        assert not http_route.match.HasField("connect_matcher")
        assert http_route.match.prefix == "/"
        assert list(http_route.request_headers_to_remove) == [
            "x-loom-connection-id",
        ]
        assert http_route.route.cluster == f"egress-{http_connection_id}"
    finally:
        await watcher.stop()
        watcher_task.cancel()
        try:
            await watcher_task
        except (Exception, asyncio.CancelledError):
            pass
        await grpc_server.stop(grace=2.0)


async def _request_once(stream_method):
    """SOTW xDS request/response: send one empty DiscoveryRequest,
    read the first DiscoveryResponse, close. Mimics what Envoy does
    on initial connect."""

    async def _gen():
        yield DiscoveryRequest()

    call = stream_method(_gen())
    response = await asyncio.wait_for(call.__aiter__().__anext__(), timeout=10)
    call.cancel()
    return response


def _unpack(packed, expected_type):
    msg = expected_type()
    packed.Unpack(msg)
    return msg
