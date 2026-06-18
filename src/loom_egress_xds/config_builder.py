"""Pure function: provider_connections rows → connection-id allowlist
snapshot (#190).

The snapshot is the contract between the Postgres watcher and the
xDS server (PR-C1b). Format-agnostic intentionally: the xDS server
wraps each entry in Envoy's `Cluster` / `ClusterLoadAssignment` (or
an `RBAC` filter, pending the Envoy spike). Keeping this layer free
of Envoy protobuf imports means the watcher + tests don't need the
`envoy-data-plane` dep, and the Envoy filter shape can change in
PR-C1b without disturbing the data plane.

Filtering rules:
- Soft-deleted rows (`deleted_at IS NOT NULL`) are excluded — the
  egress proxy must NOT honor allowlists for connections the user
  has deleted, even if in-flight trials still hold FKs.
- Rows with empty `resolved_egress_ips` are excluded — there's
  nothing to allow, and an empty allowlist is semantically distinct
  from "any" (which Envoy would otherwise interpret as deny-all,
  which is correct behavior but noisy in the snapshot).
- Rows with `status != 'valid'` are still included — the egress
  proxy enforces IP allowlists regardless of validation status; a
  pending connection's IPs are still its IPs.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


class ProviderConnectionRow(Protocol):
    """Subset of `loom.db.schema.ProviderConnection` columns the
    builder needs. Declared as Protocol so the watcher can pass raw
    `asyncpg.Record` rows AND tests can pass dataclasses, without
    importing the full ORM model into the egress-xds package."""

    id: UUID
    resolved_egress_ips: list[str]
    upstream_host: str
    deleted_at: datetime | None


@dataclass(frozen=True)
class ConnectionAllowlist:
    """One entry in the snapshot. `ips` is the sorted, deduped set
    of egress IPs the connection is allowed to reach. `upstream_host`
    is informational (Envoy filter logs it on deny) but is NOT used
    for filtering — IP match is authoritative."""

    connection_id: UUID
    ips: tuple[str, ...]
    upstream_host: str


@dataclass(frozen=True)
class Snapshot:
    """Aggregate of all connection allowlists at a point in time.
    Snapshot version is the count + hash of the entries; consumers
    can short-circuit `if new.version == current.version: skip`."""

    entries: tuple[ConnectionAllowlist, ...]
    version: str

    def lookup(self, connection_id: UUID) -> ConnectionAllowlist | None:
        for e in self.entries:
            if e.connection_id == connection_id:
                return e
        return None


def build_snapshot(rows: Iterable[ProviderConnectionRow]) -> Snapshot:
    """Build a stable snapshot from a sequence of rows.

    Stable = sorted by `connection_id`, IPs sorted within each entry,
    duplicates removed. Stable ordering is critical: the xDS server
    sends a snapshot version per push, and Envoy thrashes its
    cluster pool if the same logical data shows up under a new
    version string due to dict/set ordering.
    """
    entries: list[ConnectionAllowlist] = []
    for row in rows:
        if row.deleted_at is not None:
            continue
        ips = tuple(sorted(set(row.resolved_egress_ips)))
        if not ips:
            continue
        entries.append(ConnectionAllowlist(
            connection_id=row.id,
            ips=ips,
            upstream_host=row.upstream_host,
        ))
    entries.sort(key=lambda e: e.connection_id)
    version = _compute_version(entries)
    return Snapshot(entries=tuple(entries), version=version)


def _compute_version(entries: list[ConnectionAllowlist]) -> str:
    """Deterministic hash of the snapshot contents. Used by the xDS
    server as the `version_info` field on DiscoveryResponse, which
    Envoy echoes back in the next request — letting us short-circuit
    on no-op pushes.

    Uses sha256(canonical-repr) rather than `hash()` (Python's hash
    randomization would make the same snapshot land under different
    version strings across server restarts, defeating the
    short-circuit).
    """
    import hashlib
    h = hashlib.sha256()
    for e in entries:
        h.update(str(e.connection_id).encode())
        h.update(b"|")
        for ip in e.ips:
            h.update(ip.encode())
            h.update(b",")
        h.update(b"|")
        h.update(e.upstream_host.encode())
        h.update(b";")
    return h.hexdigest()[:16]
