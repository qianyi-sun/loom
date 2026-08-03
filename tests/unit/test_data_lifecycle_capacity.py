from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom.data_lifecycle_capacity import (
    CAPACITY_SOURCE,
    STAGING_CAPACITY_FRESHNESS,
    DriveHeadroom,
    StagingAdmissionError,
    StagingCapacityEvidence,
    collect_staging_capacity,
    collect_staging_capacity_from_drives,
)
from loom.data_lifecycle_capacity_sql import require_staging_capacity_admission
from loom.data_lifecycle_gc import ObservedObject

NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)


def _evidence(capacity: StagingCapacity, *, observed_at: datetime = NOW):
    return StagingCapacityEvidence(
        namespace="loom-staging",
        capacity=capacity,
        policy_sha256=staging_capacity_policy_digest(),
        evidence_sha256=capacity.evidence_digest,
        observed_at=observed_at,
    )


def test_fresh_capacity_allows_admission_and_stale_or_high_water_denies() -> None:
    _evidence(StagingCapacity(10, 100, 80, 90)).require_fresh_admission(now=NOW)

    with pytest.raises(StagingAdmissionError, match="evidence_stale"):
        _evidence(
            StagingCapacity(10, 100, 80, 90),
            observed_at=NOW - STAGING_CAPACITY_FRESHNESS - timedelta(seconds=1),
        ).require_fresh_admission(now=NOW)
    with pytest.raises(StagingAdmissionError, match="high_water"):
        _evidence(StagingCapacity(250_000, 100, 80, 90)).require_fresh_admission(now=NOW)


def test_collector_aggregates_exact_objects_and_statvfs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "loom.data_lifecycle_capacity.os.statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=100,
            f_bavail=45,
            f_files=200,
            f_favail=150,
        ),
    )
    evidence = collect_staging_capacity(
        namespace="loom-staging",
        objects=[
            ObservedObject("artifacts", "a", None, 3),
            ObservedObject("trajectories", "b", "v1", 7),
        ],
        filesystem_paths=[tmp_path],
        observed_at=NOW,
    )

    assert evidence.capacity == StagingCapacity(2, 10, 45, 75)
    assert evidence.source == CAPACITY_SOURCE


def test_collect_from_drives_fails_closed_on_most_constrained_drive() -> None:
    # bytes: min free% = 40/100; inodes: min free% = 30/100 (different drives).
    drives = [
        DriveHeadroom(total_bytes=100, free_bytes=40, total_inodes=100, free_inodes=90),
        DriveHeadroom(total_bytes=100, free_bytes=80, total_inodes=100, free_inodes=30),
    ]
    evidence = collect_staging_capacity_from_drives(
        namespace="loom-staging",
        objects=[
            ObservedObject("artifacts", "a", None, 3),
            ObservedObject("trajectories", "b", "v1", 7),
        ],
        drives=drives,
        observed_at=NOW,
    )
    assert evidence.capacity == StagingCapacity(2, 10, 40, 30)
    assert evidence.source == CAPACITY_SOURCE


def test_collect_from_drives_matches_filesystem_percentages(
    monkeypatch, tmp_path: Path
) -> None:
    # Scale-invariance: bytes-unit drives yield the same integer percentages as
    # the statvfs block-count path, so single-node evidence never shifts.
    monkeypatch.setattr(
        "loom.data_lifecycle_capacity.os.statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=100, f_bavail=45, f_files=200, f_favail=150
        ),
    )
    objects = [ObservedObject("artifacts", "a", None, 3)]
    fs = collect_staging_capacity(
        namespace="loom-staging",
        objects=list(objects),
        filesystem_paths=[tmp_path],
        observed_at=NOW,
    )
    drive = collect_staging_capacity_from_drives(
        namespace="loom-staging",
        objects=list(objects),
        drives=[
            DriveHeadroom(
                total_bytes=100 * 4096,
                free_bytes=45 * 4096,
                total_inodes=200,
                free_inodes=150,
            )
        ],
        observed_at=NOW,
    )
    assert drive.capacity == fs.capacity


def test_collect_from_drives_requires_at_least_one_drive() -> None:
    with pytest.raises(RuntimeError, match="drive headroom is unavailable"):
        collect_staging_capacity_from_drives(
            namespace="loom-staging", objects=[], drives=[], observed_at=NOW
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"total_bytes": 0, "free_bytes": 0, "total_inodes": 10, "free_inodes": 5},
        {"total_bytes": 10, "free_bytes": 20, "total_inodes": 10, "free_inodes": 5},
        {"total_bytes": 10, "free_bytes": 5, "total_inodes": 10, "free_inodes": 20},
    ],
)
def test_drive_headroom_rejects_invalid_totals(kwargs: dict[str, int]) -> None:
    with pytest.raises(RuntimeError):
        DriveHeadroom(**kwargs)


@pytest.mark.asyncio
async def test_runtime_admission_requires_exact_policy_evidence() -> None:
    capacity = StagingCapacity(10, 100, 80, 90)
    session = AsyncMock()
    session.scalar.return_value = SimpleNamespace(
        namespace="loom-staging",
        object_count=capacity.object_count,
        bytes_used=capacity.bytes_used,
        disk_free_percent=capacity.disk_free_percent,
        inode_free_percent=capacity.inode_free_percent,
        policy_sha256=staging_capacity_policy_digest(),
        evidence_sha256=capacity.evidence_digest,
        source=CAPACITY_SOURCE,
        observed_at=NOW,
    )

    await require_staging_capacity_admission(
        session,
        namespace="loom-staging",
        now=NOW,
    )

    session.scalar.return_value = None
    with pytest.raises(StagingAdmissionError, match="evidence_missing"):
        await require_staging_capacity_admission(
            session,
            namespace="loom-staging",
            now=NOW,
        )
