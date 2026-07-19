"""Freshness-bound staging object/disk capacity and admission authority."""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom.data_lifecycle_gc import ObservedObject

STAGING_CAPACITY_FRESHNESS = timedelta(minutes=5)
CAPACITY_SOURCE = "exact-object-inventory-v1"


class StagingAdmissionError(RuntimeError):
    """Stable fail-closed rejection for stale or exhausted staging capacity."""


@dataclass(frozen=True, slots=True)
class StagingCapacityEvidence:
    namespace: str
    capacity: StagingCapacity
    policy_sha256: str
    evidence_sha256: str
    observed_at: datetime
    source: str = CAPACITY_SOURCE

    def __post_init__(self) -> None:
        if (
            not self.namespace
            or self.namespace != self.namespace.strip()
            or self.policy_sha256 != staging_capacity_policy_digest()
            or self.evidence_sha256 != self.capacity.evidence_digest
            or self.observed_at.tzinfo is None
            or self.observed_at.utcoffset() is None
            or self.source != CAPACITY_SOURCE
        ):
            raise ValueError("staging capacity evidence identity is invalid")

    def require_fresh_admission(self, *, now: datetime) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("admission time must be timezone-aware")
        age = now - self.observed_at
        if age < timedelta(0) or age > STAGING_CAPACITY_FRESHNESS:
            raise StagingAdmissionError("staging_capacity_evidence_stale")
        if not self.capacity.admission_allowed:
            raise StagingAdmissionError("staging_capacity_high_water")


def collect_staging_capacity(
    *,
    namespace: str,
    objects: Iterable[ObservedObject],
    filesystem_path: Path,
    observed_at: datetime,
) -> StagingCapacityEvidence:
    """Collect exact object totals plus filesystem byte/inode headroom."""
    resolved = filesystem_path.resolve(strict=True)
    stats = os.statvfs(resolved)
    if stats.f_blocks <= 0 or stats.f_files <= 0:
        raise RuntimeError("staging capacity filesystem totals are unavailable")
    observed = tuple(objects)
    identities = [item.identity for item in observed]
    if len(identities) != len(set(identities)):
        raise RuntimeError("staging capacity object identities are duplicated")
    capacity = StagingCapacity(
        object_count=len(observed),
        bytes_used=sum(item.size_bytes for item in observed),
        disk_free_percent=(stats.f_bavail * 100) // stats.f_blocks,
        inode_free_percent=(stats.f_favail * 100) // stats.f_files,
    )
    return StagingCapacityEvidence(
        namespace=namespace,
        capacity=capacity,
        policy_sha256=staging_capacity_policy_digest(),
        evidence_sha256=capacity.evidence_digest,
        observed_at=observed_at,
    )


__all__ = [
    "CAPACITY_SOURCE",
    "STAGING_CAPACITY_FRESHNESS",
    "StagingAdmissionError",
    "StagingCapacityEvidence",
    "collect_staging_capacity",
]
