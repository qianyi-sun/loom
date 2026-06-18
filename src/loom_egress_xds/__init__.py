"""loom_egress_xds — Envoy egress-proxy control plane (#78 Phase C).

Reads `provider_connections` from Postgres + serves snapshots to an
Envoy egress sidecar that gates outbound provider traffic by
`x-loom-connection-id → resolved_egress_ips` allowlist. Independent
of the sandbox isolation path (Phase B+D): the egress proxy benefits
non-isolated deploys too.

This package is intentionally split:
- `config_builder` is pure (rows -> snapshot dict); easy to unit test.
- `provider_connections_watcher` owns Postgres LISTEN + poll fallback.
- `xds_server` (PR-C1b) wraps the snapshot in Envoy protobuf messages
  and serves them via gRPC.

Why the split: the exact Envoy filter shape (RBAC + HCM + CDS/EDS vs
ext_authz) isn't pinned until the Envoy spike runs. The intermediate
`ConnectionAllowlist` representation is format-agnostic so the spike
can refine the proto layer without disturbing the watcher.
"""
