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
    StagingAdmissionError,
    StagingCapacityEvidence,
    collect_staging_capacity,
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
        filesystem_path=tmp_path,
        observed_at=NOW,
    )

    assert evidence.capacity == StagingCapacity(2, 10, 45, 75)
    assert evidence.source == CAPACITY_SOURCE


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
