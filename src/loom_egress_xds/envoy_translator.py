"""Snapshot → Envoy CDS+RDS protobuf (#190 PR-C1b).

Pure function over the format-agnostic `Snapshot` shipped in PR-C1a
(#192). Produces:

- One Envoy `Cluster` per ConnectionAllowlist, with the allowlisted
  IPs as static endpoints. Cluster name: `egress-<connection_id>`.
- A single `RouteConfiguration` with one Route per ConnectionAllowlist.
  Each route matches on BOTH the `x-loom-connection-id` header AND
  the `:authority` CONNECT target (`<upstream_host>:443`) — this
  layered match defeats the CDN-IP-collapse problem the spike (#196)
  surfaced: a Cloudflare IP serves thousands of domains via SNI, so
  IP-only enforcement is insufficient.

Why pure (no gRPC/server state): the xds_server holds a
SnapshotCache that maps `Snapshot.version` → these protobuf objects.
Each on_snapshot push from the watcher rebuilds via this function;
the cache then sends DiscoveryResponses to any subscribed Envoy.
"""

from __future__ import annotations

from dataclasses import dataclass

from envoy.config.cluster.v3.cluster_pb2 import Cluster
from envoy.config.core.v3.address_pb2 import Address, SocketAddress
from envoy.config.endpoint.v3.endpoint_components_pb2 import (
    Endpoint,
    LbEndpoint,
    LocalityLbEndpoints,
)
from envoy.config.endpoint.v3.endpoint_pb2 import ClusterLoadAssignment
from envoy.config.route.v3.route_components_pb2 import (
    HeaderMatcher,
    Route,
    RouteAction,
    RouteMatch,
    VirtualHost,
)
from envoy.config.route.v3.route_pb2 import RouteConfiguration
from envoy.type.matcher.v3.string_pb2 import StringMatcher
from google.protobuf.duration_pb2 import Duration

from loom_egress_xds.config_builder import ConnectionAllowlist, Snapshot

# Default upstream port for TLS providers. CONNECT targets land here
# (`<host>:443`); we don't currently allow non-443 upstreams (which
# would simplify nothing — provider APIs are all on HTTPS).
_UPSTREAM_PORT = 443

# Default cluster connect timeout. 5s matches the spike configs and
# is enough for one TCP+TLS handshake from a US-East worker to a US-
# West provider edge.
_CLUSTER_CONNECT_TIMEOUT_SEC = 5

# Top-level RouteConfiguration name. Envoy's RDS request specifies
# the route_config_name; the bootstrap (deploy/envoy/egress-proxy.
# yaml) must reference this exact string.
ROUTE_CONFIGURATION_NAME = "loom_egress_routes"

# Top-level VirtualHost name (only one — all routing happens inside).
_VIRTUAL_HOST_NAME = "loom_egress"

# Header key the gateway-router sends on the CONNECT to identify
# which provider_connection the request is on behalf of. The egress
# proxy uses this for routing; the dispatch is meaningless without it.
CONNECTION_ID_HEADER = "x-loom-connection-id"


@dataclass(frozen=True)
class EnvoyConfig:
    """The two payloads CDS + RDS emit for one snapshot."""

    clusters: tuple[Cluster, ...]
    route_configuration: RouteConfiguration
    # The xds_server uses this as the DiscoveryResponse `version_info`
    # so Envoy can short-circuit no-op pushes (mirrors the watcher's
    # no-op skip but at the gRPC layer too).
    version_info: str


def build_envoy_config(snapshot: Snapshot) -> EnvoyConfig:
    """Build CDS + RDS payloads from a snapshot.

    The `Snapshot.version` is passed straight through as
    `version_info` so consumers (xds_server's snapshot cache) can
    correlate watcher snapshots with delivered DiscoveryResponses.
    """
    clusters = tuple(_build_cluster(e) for e in snapshot.entries)
    route_config = _build_route_configuration(snapshot)
    return EnvoyConfig(
        clusters=clusters,
        route_configuration=route_config,
        version_info=snapshot.version,
    )


def _cluster_name_for(connection_id_str: str) -> str:
    return f"egress-{connection_id_str}"


def _build_cluster(entry: ConnectionAllowlist) -> Cluster:
    """One static cluster per connection. `STATIC` type so Envoy
    doesn't try to DNS-resolve — we already have the IPs from
    `resolved_egress_ips`, and any resolution drift between our
    re-resolver and Envoy would break IP-allowlist enforcement.

    `lb_policy: ROUND_ROBIN` distributes across the allowlisted IPs.
    Healthchecks deliberately omitted — providers won't expose a
    standard health endpoint, and probing them would burn quota.
    Envoy's outlier detection (passive, observation-based) handles
    bad endpoints without active probes.
    """
    connection_id_str = str(entry.connection_id)
    cluster_name = _cluster_name_for(connection_id_str)
    lb_endpoints = [
        LbEndpoint(endpoint=Endpoint(address=Address(
            socket_address=SocketAddress(
                address=ip,
                port_value=_UPSTREAM_PORT,
            ),
        )))
        for ip in entry.ips
    ]
    return Cluster(
        name=cluster_name,
        connect_timeout=Duration(seconds=_CLUSTER_CONNECT_TIMEOUT_SEC),
        type=Cluster.DiscoveryType.STATIC,
        lb_policy=Cluster.LbPolicy.ROUND_ROBIN,
        load_assignment=ClusterLoadAssignment(
            cluster_name=cluster_name,
            endpoints=[LocalityLbEndpoints(lb_endpoints=lb_endpoints)],
        ),
    )


def _build_route_configuration(snapshot: Snapshot) -> RouteConfiguration:
    """Single VirtualHost with one Route per ConnectionAllowlist.

    Each route requires BOTH headers to match:
      - `x-loom-connection-id: <connection_id>` (tenant dispatch)
      - `:authority: <upstream_host>:443` (CONNECT target hostname)

    The pair-match is the spike (#196) finding: header alone isn't
    enough because a malicious tenant could send the right header
    but a wrong CONNECT target; CONNECT target alone isn't enough
    because Cloudflare IPs serve thousands of domains, so the
    cluster's IPs include hostnames the tenant shouldn't reach.
    Both together = the tenant's header AND the expected upstream.
    """
    routes = [_build_route(e) for e in snapshot.entries]
    return RouteConfiguration(
        name=ROUTE_CONFIGURATION_NAME,
        virtual_hosts=[VirtualHost(
            name=_VIRTUAL_HOST_NAME,
            domains=["*"],
            routes=routes,
        )],
    )


def _build_route(entry: ConnectionAllowlist) -> Route:
    connection_id_str = str(entry.connection_id)
    expected_authority = f"{entry.upstream_host}:{_UPSTREAM_PORT}"
    return Route(
        match=RouteMatch(
            # `connect_matcher` (set to empty CONNECT match) tells
            # Envoy this route applies to CONNECT requests. The
            # `headers` matchers below further restrict it.
            connect_matcher=RouteMatch.ConnectMatcher(),
            headers=[
                HeaderMatcher(
                    name=CONNECTION_ID_HEADER,
                    string_match=StringMatcher(exact=connection_id_str),
                ),
                HeaderMatcher(
                    name=":authority",
                    string_match=StringMatcher(exact=expected_authority),
                ),
            ],
        ),
        route=RouteAction(
            cluster=_cluster_name_for(connection_id_str),
            upgrade_configs=[RouteAction.UpgradeConfig(
                upgrade_type="CONNECT",
                connect_config=RouteAction.UpgradeConfig.ConnectConfig(),
            )],
        ),
    )
