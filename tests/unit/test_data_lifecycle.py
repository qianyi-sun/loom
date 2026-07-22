from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.data_lifecycle import (
    STAGING_EPHEMERAL_TTL,
    DataClass,
    LifecycleAuthoritySpec,
    OwnerKind,
    StagingCapacity,
)


def test_staging_ephemeral_authority_is_complete_and_bounded() -> None:
    created_at = datetime(2026, 7, 19, tzinfo=UTC)
    authority = LifecycleAuthoritySpec.staging_ephemeral(
        namespace="loom-staging",
        team_id=uuid4(),
        data_class=DataClass.TRIAL,
        owner_kind=OwnerKind.TRIAL,
        owner_id="trial-1",
        created_at=created_at,
    )

    assert authority.environment == "staging"
    assert authority.expires_at == created_at + STAGING_EPHEMERAL_TTL
    assert not authority.eligible_at(created_at + STAGING_EPHEMERAL_TTL - timedelta(seconds=1))
    assert authority.eligible_at(created_at + STAGING_EPHEMERAL_TTL)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"environment": "", "namespace": "loom-staging"},
        {"environment": "staging", "namespace": " loom-staging"},
    ],
)
def test_authority_rejects_ambiguous_scope(kwargs: dict[str, str]) -> None:
    now = datetime(2026, 7, 19, tzinfo=UTC)
    with pytest.raises(ValueError, match="normalized"):
        LifecycleAuthoritySpec(
            team_id=uuid4(),
            data_class=DataClass.ARTIFACT,
            owner_kind=OwnerKind.ARTIFACT,
            owner_id="artifact-1",
            created_at=now,
            expires_at=now + timedelta(days=7),
            **kwargs,
        )


def test_catalog_requires_pinned_authority() -> None:
    now = datetime(2026, 7, 19, tzinfo=UTC)
    with pytest.raises(ValueError, match="must be pinned"):
        LifecycleAuthoritySpec(
            environment="staging",
            namespace="loom-staging",
            team_id=None,
            data_class=DataClass.CATALOG,
            owner_kind=OwnerKind.BENCHMARK,
            owner_id="terminal-bench-2",
            created_at=now,
            expires_at=now + timedelta(days=7),
        )


def test_capacity_requires_gc_before_hard_admission_limit() -> None:
    soft = StagingCapacity(
        object_count=200_000,
        bytes_used=1,
        disk_free_percent=30,
        inode_free_percent=30,
    )
    hard = StagingCapacity(
        object_count=250_000,
        bytes_used=1,
        disk_free_percent=30,
        inode_free_percent=30,
    )

    assert soft.gc_required
    assert soft.admission_allowed
    assert hard.gc_required
    assert not hard.admission_allowed


def test_capacity_fails_admission_on_low_inode_headroom() -> None:
    capacity = StagingCapacity(
        object_count=1,
        bytes_used=1,
        disk_free_percent=50,
        inode_free_percent=19,
    )
    assert capacity.gc_required
    assert not capacity.admission_allowed
