"""gRPC xDS server: CDS + RDS for Envoy egress-proxy (#190 PR-C1b).

Implements the state-of-the-world (SOTW) xDS protocol on top of the
snapshot-cache abstraction from #192:

- `XdsSnapshotCache.publish(EnvoyConfig)` stores the latest config
  and wakes every subscribed Envoy stream.
- `ClusterDiscoveryServicer` + `RouteDiscoveryServicer` each open a
  bidirectional stream per Envoy client; on every wake (or every
  incoming DiscoveryRequest) they send the current snapshot's
  Clusters / RouteConfiguration with `version_info` set to the
  snapshot's content hash. Envoy short-circuits no-op updates by
  comparing `version_info`.

Design simplifications vs full Envoy xDS:
- No per-stream resource subscription tracking. We always send the
  full state of the type — fine because the resource set is small
  (one Cluster per provider_connection, <100s realistic) and the
  update rate is low (mutations are operator-driven, not per-
  request).
- ACK/NACK from Envoy is logged but not acted on. NACK means the
  config was rejected as syntactically invalid; if that happens
  there's a translator bug and we want it surfaced loudly. Future
  work: surface NACKs as a Prometheus counter.
- No ADS (aggregated discovery). Envoy uses two separate gRPC
  streams (one for CDS, one for RDS). Slightly more sockets but
  simpler to reason about.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Callable

import grpc
from envoy.service.cluster.v3 import cds_pb2_grpc
from envoy.service.discovery.v3.discovery_pb2 import (
    DiscoveryRequest,
    DiscoveryResponse,
)
from envoy.service.route.v3 import rds_pb2_grpc
from google.protobuf.any_pb2 import Any as ProtoAny
from google.protobuf.message import Message

from loom_egress_xds.config_builder import Snapshot
from loom_egress_xds.envoy_translator import (
    ROUTE_CONFIGURATION_NAME,
    EnvoyConfig,
    build_envoy_config,
)

logger = logging.getLogger(__name__)

_CLUSTER_TYPE_URL = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
_ROUTE_CONFIG_TYPE_URL = (
    "type.googleapis.com/envoy.config.route.v3.RouteConfiguration"
)


class XdsSnapshotCache:
    """Single-snapshot cache + fan-out wake events.

    Holds the latest `EnvoyConfig` (rebuilt from each `Snapshot`
    pushed by the watcher) and an asyncio.Condition every active
    stream waits on. `publish` updates state + notifies all waiters
    atomically — no stream misses an update because of timing.

    NOT a multi-version cache (Envoy's official Python control-plane
    SDK ships one of those). Our update rate is low enough that the
    single-snapshot variant has no observable downside.
    """

    def __init__(self) -> None:
        self._config: EnvoyConfig | None = None
        self._cond = asyncio.Condition()

    async def publish_envoy_config(self, config: EnvoyConfig) -> None:
        """Called by the watcher's `on_snapshot` callback. Wakes all
        subscribed streams; each one reads the new config and pushes
        to its Envoy client."""
        async with self._cond:
            if self._config is not None and \
                    self._config.version_info == config.version_info:
                return
            self._config = config
            self._cond.notify_all()
        logger.info(
            "xds_snapshot_published version=%s clusters=%d routes=%d",
            config.version_info,
            len(config.clusters),
            len(config.route_configuration.virtual_hosts[0].routes),
        )

    async def publish_snapshot(self, snapshot: Snapshot) -> None:
        """Convenience: build the EnvoyConfig + publish in one call.
        Wired into the watcher's `on_snapshot` callback in __main__."""
        await self.publish_envoy_config(build_envoy_config(snapshot))

    async def get_or_wait(
        self,
        last_version: str | None,
    ) -> EnvoyConfig:
        """Return the current config IF its version differs from
        `last_version`, else block until publish wakes us.

        Streams call this in a loop, passing their own `last_version`
        as cursor; this naturally handles both "send initial state"
        (last_version=None) and "wait for change" (last_version=most
        recently sent version)."""
        async with self._cond:
            while (
                self._config is None
                or self._config.version_info == last_version
            ):
                await self._cond.wait()
            return self._config


class _StreamHandler:
    """Shared logic between CDS and RDS streams. Both follow the same
    pattern: read incoming DiscoveryRequests (mostly ignore, log
    NACKs), push DiscoveryResponses on every snapshot change."""

    def __init__(
        self,
        cache: XdsSnapshotCache,
        type_url: str,
        nonce_prefix: str,
    ) -> None:
        self._cache = cache
        self._type_url = type_url
        # Each stream gets its own monotonic nonce counter. Envoy
        # echoes the nonce in its ACK/NACK — used to correlate, not
        # for ordering.
        self._nonce_prefix = nonce_prefix
        self._nonce_counter = 0

    def _next_nonce(self) -> str:
        self._nonce_counter += 1
        return f"{self._nonce_prefix}-{self._nonce_counter}"

    async def run(
        self,
        request_iterator: AsyncIterable[DiscoveryRequest],
        resources_for: Callable[[EnvoyConfig], list[ProtoAny]],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[DiscoveryResponse]:
        """`resources_for(config) -> list[ProtoAny]` builds the
        per-type payload from the current config. Different for CDS
        (Cluster list) and RDS (one RouteConfiguration)."""
        # Drain incoming requests concurrently — every received
        # message is either an initial request or an ACK/NACK. We
        # don't need to act on them to be protocol-compliant for
        # SOTW + a low-volume control plane; we just log NACKs.
        request_task = asyncio.create_task(
            self._drain_requests(request_iterator),
        )
        last_version: str | None = None
        try:
            while True:
                config = await self._cache.get_or_wait(last_version)
                resources = resources_for(config)
                yield DiscoveryResponse(
                    version_info=config.version_info,
                    resources=resources,
                    type_url=self._type_url,
                    nonce=self._next_nonce(),
                )
                last_version = config.version_info
        except asyncio.CancelledError:
            raise
        finally:
            request_task.cancel()
            try:
                await request_task
            except (Exception, asyncio.CancelledError):
                pass

    async def _drain_requests(
        self,
        request_iterator: AsyncIterable[DiscoveryRequest],
    ) -> None:
        async for req in request_iterator:
            if req.error_detail.code != 0:
                # Envoy NACKed our last response. The translator
                # produced something Envoy can't parse — surface
                # loudly so the bug is noticed.
                logger.error(
                    "xds_nack type=%s version=%s code=%d msg=%s",
                    self._type_url,
                    req.version_info,
                    req.error_detail.code,
                    req.error_detail.message,
                )


class ClusterDiscoveryServicer(
    cds_pb2_grpc.ClusterDiscoveryServiceServicer,  # type: ignore[misc]
):
    """SOTW CDS: streams DiscoveryResponses containing the current
    `EnvoyConfig.clusters` list whenever the snapshot changes."""

    def __init__(self, cache: XdsSnapshotCache) -> None:
        self._cache = cache

    async def StreamClusters(  # noqa: N802 — gRPC-required PascalCase
        self,
        request_iterator: AsyncIterable[DiscoveryRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[DiscoveryResponse]:
        handler = _StreamHandler(self._cache, _CLUSTER_TYPE_URL, "cds")

        def resources_for(config: EnvoyConfig) -> list[ProtoAny]:
            return [_pack(c, _CLUSTER_TYPE_URL) for c in config.clusters]

        async for response in handler.run(
            request_iterator, resources_for, context,
        ):
            yield response


class RouteDiscoveryServicer(
    rds_pb2_grpc.RouteDiscoveryServiceServicer,  # type: ignore[misc]
):
    """SOTW RDS: streams a single `RouteConfiguration` (with all
    per-connection routes inside one VirtualHost) on every snapshot
    change. Envoy correlates with the bootstrap's `route_config_name`
    (`ROUTE_CONFIGURATION_NAME` constant)."""

    def __init__(self, cache: XdsSnapshotCache) -> None:
        self._cache = cache

    async def StreamRoutes(  # noqa: N802 — gRPC-required PascalCase
        self,
        request_iterator: AsyncIterable[DiscoveryRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[DiscoveryResponse]:
        handler = _StreamHandler(self._cache, _ROUTE_CONFIG_TYPE_URL, "rds")

        def resources_for(config: EnvoyConfig) -> list[ProtoAny]:
            # RDS sends exactly one RouteConfiguration; Envoy
            # references it by name from the bootstrap.
            assert config.route_configuration.name == ROUTE_CONFIGURATION_NAME
            return [_pack(
                config.route_configuration, _ROUTE_CONFIG_TYPE_URL,
            )]

        async for response in handler.run(
            request_iterator, resources_for, context,
        ):
            yield response


def _pack(message: Message, type_url: str) -> ProtoAny:
    packed = ProtoAny()
    packed.Pack(message, type_url_prefix="type.googleapis.com")
    return packed


def build_grpc_server(
    cache: XdsSnapshotCache,
    *,
    listen_addr: str,
) -> grpc.aio.Server:
    """Construct a grpc.aio.Server with both servicers registered.

    Caller is responsible for `await server.start()` /
    `await server.wait_for_termination()`; this just wires the bits
    so tests can construct without binding to a real port (pass
    `listen_addr=''` and call `.add_insecure_port(...)` from the
    test if needed)."""
    server = grpc.aio.server()
    cds_pb2_grpc.add_ClusterDiscoveryServiceServicer_to_server(
        ClusterDiscoveryServicer(cache), server,
    )
    rds_pb2_grpc.add_RouteDiscoveryServiceServicer_to_server(
        RouteDiscoveryServicer(cache), server,
    )
    if listen_addr:
        server.add_insecure_port(listen_addr)
    return server


__all__ = [
    "ClusterDiscoveryServicer",
    "RouteDiscoveryServicer",
    "XdsSnapshotCache",
    "build_grpc_server",
]
