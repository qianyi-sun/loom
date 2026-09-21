"""Read-only shadow reconciler for the staging rollout (#1097 / #1085 phase 4).

Computes a structured desired-vs-live drift report **without writing anything**.
This is the read-only shadow step of the reconciler migration — observe + diff,
never write — proving the reconciler's core comparison logic against reality
before the write path exists.

Secret-safe by construction: a drift record carries resource *identities* and the
JSON *paths* that differ, never field *values* (which can carry secrets such as
env vars or tokens). This is the closed-schema secret-safety from #1085/#1077 —
free text never originates from unaudited resource content.

The comparison is server-side-apply-shaped: a resource is in sync when every
*desired* field is present and equal in the live object (desired is a subset of
live). Live-only fields (controller defaults, status, managed-fields) are not
drift; a curated ignore set drops the always-volatile ones.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

# Controller-owned / volatile paths that are never part of a desired declaration
# and therefore never count as drift.
_DEFAULT_IGNORED_PATHS: frozenset[str] = frozenset(
    {
        "metadata.resourceVersion",
        "metadata.uid",
        "metadata.generation",
        "metadata.creationTimestamp",
        "metadata.managedFields",
        "metadata.selfLink",
        "metadata.annotations.kubectl.kubernetes.io/last-applied-configuration",
        "metadata.annotations.deployment.kubernetes.io/revision",
        "status",
    }
)

# (kind, namespace, name) — an apiVersion-independent resource identity. Cluster-scoped
# resources use an empty namespace.
ResourceKey = tuple[str, str, str]


class DriftStatus(StrEnum):
    """How a single resource's live state relates to its desired declaration."""

    IN_SYNC = "in-sync"
    # Desired object has no live counterpart — applying would create it.
    ABSENT_FROM_LIVE = "absent-from-live"
    # Desired fields differ from live — applying would change it.
    MODIFIED = "modified"
    # Live object is not in the desired set — it would be orphaned/pruned (only
    # meaningful when the desired set is authoritative; off by default).
    ABSENT_FROM_DESIRED = "absent-from-desired"


@dataclass(frozen=True)
class ResourceDrift:
    """Drift for one resource. `changed_paths` are field paths, never values."""

    kind: str
    namespace: str
    name: str
    status: DriftStatus
    changed_paths: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        record: dict[str, object] = {
            "kind": self.kind,
            "namespace": self.namespace,
            "name": self.name,
            "status": self.status.value,
        }
        if self.changed_paths:
            record["changed_paths"] = list(self.changed_paths)
        return record


@dataclass(frozen=True)
class ShadowDriftReport:
    """A read-only desired-vs-live drift report for one environment + target."""

    environment: str
    # An identifier for what is being reconciled to (a git SHA / artifact digest);
    # an identity string, never secret-bearing content.
    target: str
    resources: tuple[ResourceDrift, ...]

    @property
    def in_sync(self) -> bool:
        return all(r.status is DriftStatus.IN_SYNC for r in self.resources)

    def summary(self) -> dict[str, int]:
        counts = {status.value: 0 for status in DriftStatus}
        for resource in self.resources:
            counts[resource.status.value] += 1
        return counts

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "environment": self.environment,
            "target": self.target,
            "in_sync": self.in_sync,
            "summary": self.summary(),
            "resources": [resource.to_dict() for resource in self.resources],
        }


def _resource_key(obj: Mapping[str, object]) -> ResourceKey:
    metadata = obj.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    return (
        str(obj.get("kind", "")),
        str(metadata.get("namespace", "")),
        str(metadata.get("name", "")),
    )


def _changed_paths(
    desired: object,
    live: object,
    prefix: str,
    ignored: frozenset[str],
) -> list[str]:
    """Paths where `desired` is missing from or unequal to `live` (desired ⊆ live).

    Only desired fields are inspected — extra live-only fields are not drift. The
    returned entries are paths, so the result never contains a field value.
    """
    if prefix in ignored:
        return []
    if isinstance(desired, Mapping):
        if not isinstance(live, Mapping):
            return [prefix or "<root>"]
        paths: list[str] = []
        for key, desired_value in desired.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            if child in ignored:
                continue
            if key not in live:
                paths.append(child)
            else:
                paths.extend(_changed_paths(desired_value, live[key], child, ignored))
        return paths
    if isinstance(desired, Sequence) and not isinstance(desired, (str, bytes)):
        if (
            not isinstance(live, Sequence)
            or isinstance(live, (str, bytes))
            or len(desired) != len(live)
        ):
            return [prefix or "<root>"]
        paths = []
        for index, (desired_item, live_item) in enumerate(zip(desired, live, strict=True)):
            paths.extend(_changed_paths(desired_item, live_item, f"{prefix}[{index}]", ignored))
        return paths
    return [] if desired == live else [prefix or "<root>"]


def compute_drift(
    desired: Iterable[Mapping[str, object]],
    live: Iterable[Mapping[str, object]],
    *,
    environment: str,
    target: str,
    ignored_paths: frozenset[str] = _DEFAULT_IGNORED_PATHS,
    prune: bool = False,
) -> ShadowDriftReport:
    """Compute a read-only drift report of `desired` k8s objects against `live`.

    Pure and side-effect-free. `prune=True` additionally reports live objects with
    no desired counterpart (`ABSENT_FROM_DESIRED`); leave it off unless the desired
    set is the authoritative, complete inventory for the environment.
    """
    desired_by_key = {_resource_key(obj): obj for obj in desired}
    live_by_key = {_resource_key(obj): obj for obj in live}

    resources: list[ResourceDrift] = []
    for key in sorted(desired_by_key):
        kind, namespace, name = key
        live_obj = live_by_key.get(key)
        if live_obj is None:
            resources.append(ResourceDrift(kind, namespace, name, DriftStatus.ABSENT_FROM_LIVE))
            continue
        paths = tuple(sorted(_changed_paths(desired_by_key[key], live_obj, "", ignored_paths)))
        status = DriftStatus.MODIFIED if paths else DriftStatus.IN_SYNC
        resources.append(ResourceDrift(kind, namespace, name, status, paths))

    if prune:
        for key in sorted(live_by_key.keys() - desired_by_key.keys()):
            kind, namespace, name = key
            resources.append(ResourceDrift(kind, namespace, name, DriftStatus.ABSENT_FROM_DESIRED))

    return ShadowDriftReport(environment=environment, target=target, resources=tuple(resources))
