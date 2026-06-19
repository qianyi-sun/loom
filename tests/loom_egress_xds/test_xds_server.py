"""xds_server: snapshot-cache publish + CDS/RDS streaming (#190 PR-C1b).

Tests construct the snapshot cache directly and drive subscribed
streams via fake request iterators. No real gRPC sockets — the
servicers are awaitable Python objects independent of the wire.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("envoy.config.cluster.v3.cluster_pb2")


from envoy.config.cluster.v3.cluster_pb2 import Cluster
from envoy.config.route.v3.route_pb2 import RouteConfiguration
from envoy.service.discovery.v3.discovery_pb2 import (
    DiscoveryRequest,
    DiscoveryResponse,
)

from loom_egress_xds.config_builder import build_snapshot
from loom_egress_xds.xds_server import (
    ClusterDiscoveryServicer,
    RouteDiscoveryServicer,
    XdsSnapshotCache,
)


@dataclass
class _Row:
    id: UUID
    resolved_egress_ips: list[str] = field(default_factory=list)
    upstream_host: str = "api.openai.com"
    base_url: str = "https://api.openai.com/v1"
    deleted_at: datetime | None = None


_C1 = UUID("00000000-0000-0000-0000-000000000001")
_C2 = UUID("00000000-0000-0000-0000-000000000002")


async def _request_stream(*items: Any) -> AsyncIterator[DiscoveryRequest]:
    """Yield each item (DiscoveryRequest or sentinel), then block
    forever — mimics a long-lived Envoy client connection."""
    for item in items:
        yield item
    # Block forever so the servicer's run loop doesn't get an early
    # StopAsyncIteration which it interprets as 'client disconnected'.
    await asyncio.Event().wait()


# ─── XdsSnapshotCache ────────────────────────────────────────────────


async def test_get_or_wait_returns_immediately_when_config_present() -> None:
    cache = XdsSnapshotCache()
    snap = build_snapshot([_Row(id=_C1, resolved_egress_ips=["1.1.1.1"])])
    await cache.publish_snapshot(snap)
    got = await cache.get_or_wait(last_version=None)
    assert got.version_info == snap.version
    assert len(got.clusters) == 1


async def test_get_or_wait_blocks_until_publish() -> None:
    cache = XdsSnapshotCache()

    # Start a waiter; it should hang until publish.
    waiter = asyncio.create_task(cache.get_or_wait(last_version=None))
    await asyncio.sleep(0.05)
    assert not waiter.done(), "waiter completed before publish"

    snap = build_snapshot([_Row(id=_C1, resolved_egress_ips=["1.1.1.1"])])
    await cache.publish_snapshot(snap)
    got = await asyncio.wait_for(waiter, timeout=1.0)
    assert got.version_info == snap.version


async def test_publish_dedupe_skips_same_version() -> None:
    cache = XdsSnapshotCache()
    snap = build_snapshot([_Row(id=_C1, resolved_egress_ips=["1.1.1.1"])])
    await cache.publish_snapshot(snap)

    # Subscribe a waiter with the just-published version as cursor.
    # It should NOT fire when we re-publish the same snapshot — the
    # dedupe check in publish_envoy_config returns early without
    # notifying.
    waiter = asyncio.create_task(cache.get_or_wait(last_version=snap.version))
    await cache.publish_snapshot(snap)  # same version
    await asyncio.sleep(0.05)
    assert not waiter.done(), "waiter fired despite no version change"

    # New version: waiter fires.
    snap2 = build_snapshot(
        [
            _Row(
                id=_C1,
                resolved_egress_ips=["1.1.1.1", "2.2.2.2"],
            )
        ]
    )
    await cache.publish_snapshot(snap2)
    got = await asyncio.wait_for(waiter, timeout=1.0)
    assert got.version_info == snap2.version


async def test_publish_notifies_multiple_waiters() -> None:
    cache = XdsSnapshotCache()
    waiters = [asyncio.create_task(cache.get_or_wait(last_version=None)) for _ in range(5)]
    await asyncio.sleep(0.05)
    assert all(not w.done() for w in waiters)

    snap = build_snapshot([_Row(id=_C1, resolved_egress_ips=["1.1.1.1"])])
    await cache.publish_snapshot(snap)

    results = await asyncio.gather(*waiters)
    assert all(r.version_info == snap.version for r in results)


# ─── CDS streaming ───────────────────────────────────────────────────


async def test_cds_emits_clusters_after_publish() -> None:
    cache = XdsSnapshotCache()
    servicer = ClusterDiscoveryServicer(cache)

    snap = build_snapshot(
        [
            _Row(id=_C1, resolved_egress_ips=["1.1.1.1"]),
            _Row(id=_C2, resolved_egress_ips=["2.2.2.2"]),
        ]
    )
    await cache.publish_snapshot(snap)

    stream = servicer.StreamClusters(
        _request_stream(DiscoveryRequest()),
        context=None,  # type: ignore[arg-type]
    )
    first = await asyncio.wait_for(_anext(stream), timeout=1.0)
    assert isinstance(first, DiscoveryResponse)
    assert first.version_info == snap.version
    assert first.type_url == ("type.googleapis.com/envoy.config.cluster.v3.Cluster")
    assert len(first.resources) == 2

    # Unpack the resources back to Cluster and verify names.
    clusters = [_unpack(r, Cluster) for r in first.resources]
    names = sorted(c.name for c in clusters)
    assert names == [
        "egress-00000000-0000-0000-0000-000000000001",
        "egress-00000000-0000-0000-0000-000000000002",
    ]


async def test_cds_streams_update_on_new_snapshot() -> None:
    cache = XdsSnapshotCache()
    servicer = ClusterDiscoveryServicer(cache)

    snap1 = build_snapshot([_Row(id=_C1, resolved_egress_ips=["1.1.1.1"])])
    await cache.publish_snapshot(snap1)

    stream = servicer.StreamClusters(
        _request_stream(DiscoveryRequest()),
        context=None,  # type: ignore[arg-type]
    )
    first = await asyncio.wait_for(_anext(stream), timeout=1.0)
    assert first.version_info == snap1.version
    assert len(first.resources) == 1

    # Publish a NEW snapshot; the same stream should emit again with
    # the new version.
    snap2 = build_snapshot(
        [
            _Row(id=_C1, resolved_egress_ips=["1.1.1.1"]),
            _Row(id=_C2, resolved_egress_ips=["2.2.2.2"]),
        ]
    )
    await cache.publish_snapshot(snap2)
    second = await asyncio.wait_for(_anext(stream), timeout=1.0)
    assert second.version_info == snap2.version
    assert len(second.resources) == 2


# ─── RDS streaming ───────────────────────────────────────────────────


async def test_rds_emits_single_routeconfiguration() -> None:
    cache = XdsSnapshotCache()
    servicer = RouteDiscoveryServicer(cache)

    snap = build_snapshot(
        [
            _Row(id=_C1, resolved_egress_ips=["1.1.1.1"], upstream_host="api.openai.com"),
        ]
    )
    await cache.publish_snapshot(snap)

    stream = servicer.StreamRoutes(
        _request_stream(DiscoveryRequest()),
        context=None,  # type: ignore[arg-type]
    )
    first = await asyncio.wait_for(_anext(stream), timeout=1.0)
    # RDS always sends exactly one RouteConfiguration (by name).
    assert len(first.resources) == 1
    rc = _unpack(first.resources[0], RouteConfiguration)
    assert rc.name == "loom_egress_routes"
    assert len(rc.virtual_hosts) == 1
    assert len(rc.virtual_hosts[0].routes) == 1
    headers = list(rc.virtual_hosts[0].routes[0].match.headers)
    by_name = {h.name: h.string_match.exact for h in headers}
    assert by_name == {
        "x-loom-connection-id": "00000000-0000-0000-0000-000000000001",
        ":authority": "api.openai.com:443",
    }


# ─── helpers ─────────────────────────────────────────────────────────


async def _anext(it: AsyncIterator[Any]) -> Any:
    """Get next item from an async iterator. Python's built-in
    `anext()` exists in 3.10+ but using a helper keeps the test code
    explicit about awaiting."""
    return await it.__anext__()


def _unpack(packed: Any, expected_type: Any) -> Any:
    msg = expected_type()
    packed.Unpack(msg)
    return msg
