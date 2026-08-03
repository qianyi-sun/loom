"""Freshness-bound staging object/disk capacity and admission authority."""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
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


@dataclass(frozen=True, slots=True)
class DriveHeadroom:
    """One backing drive's byte + inode headroom, in any self-consistent unit.

    Only the free/total *ratio* per drive matters, so a filesystem source may
    report raw block/inode counts while a MinIO-admin source reports bytes;
    each drive's percentage is computed from its own totals.
    """

    total_bytes: int
    free_bytes: int
    total_inodes: int
    free_inodes: int

    def __post_init__(self) -> None:
        if self.total_bytes <= 0 or self.total_inodes <= 0:
            raise RuntimeError("staging capacity drive totals are unavailable")
        if not 0 <= self.free_bytes <= self.total_bytes:
            raise RuntimeError("staging capacity drive free bytes are invalid")
        if not 0 <= self.free_inodes <= self.total_inodes:
            raise RuntimeError("staging capacity drive free inodes are invalid")

    @property
    def disk_free_percent(self) -> int:
        return (self.free_bytes * 100) // self.total_bytes

    @property
    def inode_free_percent(self) -> int:
        return (self.free_inodes * 100) // self.total_inodes


def _evidence_from_drives(
    *,
    namespace: str,
    objects: Iterable[ObservedObject],
    drives: Sequence[DriveHeadroom],
    observed_at: datetime,
) -> StagingCapacityEvidence:
    """Fold the exact object inventory + per-drive headroom into fail-closed
    evidence, pinned to the most constrained drive."""
    if not drives:
        raise RuntimeError("staging capacity drive headroom is unavailable")
    observed = tuple(objects)
    identities = [item.identity for item in observed]
    if len(identities) != len(set(identities)):
        raise RuntimeError("staging capacity object identities are duplicated")
    capacity = StagingCapacity(
        object_count=len(observed),
        bytes_used=sum(item.size_bytes for item in observed),
        disk_free_percent=min(drive.disk_free_percent for drive in drives),
        inode_free_percent=min(drive.inode_free_percent for drive in drives),
    )
    return StagingCapacityEvidence(
        namespace=namespace,
        capacity=capacity,
        policy_sha256=staging_capacity_policy_digest(),
        evidence_sha256=capacity.evidence_digest,
        observed_at=observed_at,
    )


def collect_staging_capacity(
    *,
    namespace: str,
    objects: Iterable[ObservedObject],
    filesystem_paths: Sequence[Path],
    observed_at: datetime,
) -> StagingCapacityEvidence:
    """Collect exact object totals plus the lowest backing-store headroom from
    locally-mounted drives (single-node / hostPath MinIO).

    Each PVC/host path is stat'd once; distinct ``(st_dev, st_ino)`` identities
    guard against sampling the same mount twice.  Admission fails closed on the
    most constrained member.  For distributed MinIO (RWO drive PVCs that cannot
    be co-mounted) use :func:`collect_staging_capacity_from_drives` fed by the
    MinIO admin API instead.
    """
    if not filesystem_paths:
        raise RuntimeError("staging capacity filesystem paths are unavailable")
    drives: list[DriveHeadroom] = []
    filesystem_identities: set[tuple[int, int]] = set()
    for path in filesystem_paths:
        resolved = path.resolve(strict=True)
        stat = resolved.stat()
        identity = (stat.st_dev, stat.st_ino)
        if identity in filesystem_identities:
            raise RuntimeError("staging capacity filesystem identities are duplicated")
        filesystem_identities.add(identity)
        value = os.statvfs(resolved)
        if value.f_blocks <= 0 or value.f_files <= 0:
            raise RuntimeError("staging capacity filesystem totals are unavailable")
        drives.append(
            DriveHeadroom(
                total_bytes=value.f_blocks,
                free_bytes=value.f_bavail,
                total_inodes=value.f_files,
                free_inodes=value.f_favail,
            )
        )
    return _evidence_from_drives(
        namespace=namespace,
        objects=objects,
        drives=drives,
        observed_at=observed_at,
    )


def collect_staging_capacity_from_drives(
    *,
    namespace: str,
    objects: Iterable[ObservedObject],
    drives: Sequence[DriveHeadroom],
    observed_at: datetime,
) -> StagingCapacityEvidence:
    """Collect exact object totals plus the lowest per-drive headroom from an
    out-of-band source (distributed MinIO admin API).

    Distributed MinIO backs each replica with a ReadWriteOnce PVC already
    attached to the running ``loom-minio-*`` pod, so the maintenance Job cannot
    co-mount the drives (Multi-Attach).  The caller supplies every drive's
    headroom queried over the network instead; admission still fails closed on
    the most constrained drive.
    """
    return _evidence_from_drives(
        namespace=namespace,
        objects=objects,
        drives=drives,
        observed_at=observed_at,
    )


__all__ = [
    "CAPACITY_SOURCE",
    "STAGING_CAPACITY_FRESHNESS",
    "DriveHeadroom",
    "StagingAdmissionError",
    "StagingCapacityEvidence",
    "collect_staging_capacity",
    "collect_staging_capacity_from_drives",
]
