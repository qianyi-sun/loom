from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.preflight_artifact_retention import (
    ARTIFACT_FILE_NAMES,
    MAX_RETIREMENTS_PER_PLAN,
    ArtifactDirectoryIdentity,
    ArtifactFileIdentity,
    OpaqueArtifactEvidence,
    PreflightArtifactInventoryRecord,
    PreflightArtifactProtection,
    PreflightArtifactRetentionPlan,
    build_preflight_artifact_retention_plan,
)

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
DAY_NS = 24 * 60 * 60 * 1_000_000_000


def _directory(index: int, *, newest_ns: int) -> ArtifactDirectoryIdentity:
    return ArtifactDirectoryIdentity(
        device=1,
        inode=1000 + index,
        owner_uid=2001,
        owner_gid=2001,
        mode=0o700,
        link_count=2,
        size_bytes=4096,
        modified_ns=newest_ns,
        changed_ns=newest_ns,
    )


def _file(name: str, index: int, *, newest_ns: int) -> ArtifactFileIdentity:
    return ArtifactFileIdentity(
        name=name,
        device=1,
        inode=2000 + index,
        owner_uid=2001,
        owner_gid=2001,
        mode=0o600,
        link_count=1,
        size_bytes=100 + index,
        modified_ns=newest_ns,
        changed_ns=newest_ns,
        sha256=f"{index + 1:064x}",
    )


def _record(index: int, *, newest_ns: int) -> PreflightArtifactInventoryRecord:
    return PreflightArtifactInventoryRecord(
        bundle_digest=f"{index + 100:064x}",
        directory=_directory(index, newest_ns=newest_ns),
        files=tuple(
            _file(name, index * len(ARTIFACT_FILE_NAMES) + offset, newest_ns=newest_ns)
            for offset, name in enumerate(ARTIFACT_FILE_NAMES)
        ),
    )


def test_retention_plan_round_trips_exact_metadata_and_digest() -> None:
    inventory_ns = int(NOW.timestamp()) * 1_000_000_000
    old = _record(1, newest_ns=inventory_ns - 8 * DAY_NS)
    active = _record(2, newest_ns=inventory_ns - 9 * DAY_NS)

    plan = build_preflight_artifact_retention_plan(
        root=_directory(99, newest_ns=inventory_ns),
        records=(active, old),
        references=(PreflightArtifactProtection(active.bundle_digest, ("active-rollout",)),),
        opaque_evidence=(),
        inventory_at=NOW,
        environment="staging",
        namespace="loom-staging",
    )

    assert plan.candidates == (old,)
    assert plan.protected == (active,)
    assert plan.protections == (
        PreflightArtifactProtection(active.bundle_digest, ("active-rollout",)),
    )
    assert PreflightArtifactRetentionPlan.from_dict(plan.to_dict()) == plan
    assert len(plan.plan_digest) == 64
    assert plan.plan_digest == PreflightArtifactRetentionPlan.from_dict(plan.to_dict()).plan_digest
    assert old.files[0].to_dict() == {
        "changed_ns": old.files[0].changed_ns,
        "device": 1,
        "inode": old.files[0].inode,
        "link_count": 1,
        "mode": 0o600,
        "modified_ns": old.files[0].modified_ns,
        "name": "artifact.json",
        "owner_gid": 2001,
        "owner_uid": 2001,
        "sha256": old.files[0].sha256,
        "size_bytes": old.files[0].size_bytes,
    }


def test_retention_plan_uses_exact_seven_day_boundary_and_oldest_32() -> None:
    inventory_ns = int(NOW.timestamp()) * 1_000_000_000
    boundary = _record(0, newest_ns=inventory_ns - 7 * DAY_NS)
    young = _record(1, newest_ns=inventory_ns - 7 * DAY_NS + 1)
    old = tuple(
        _record(index + 2, newest_ns=inventory_ns - (50 - index) * DAY_NS) for index in range(40)
    )

    plan = build_preflight_artifact_retention_plan(
        root=_directory(99, newest_ns=inventory_ns),
        records=(*old, boundary, young),
        references=(),
        opaque_evidence=(),
        inventory_at=NOW,
        environment="staging",
        namespace="loom-staging",
    )

    assert len(plan.candidates) == MAX_RETIREMENTS_PER_PLAN == 32
    assert (
        plan.candidates
        == tuple(sorted(old, key=lambda item: (item.newest_ns, item.bundle_digest)))[:32]
    )
    assert young in plan.protected
    assert PreflightArtifactProtection(young.bundle_digest, ("grace-period",)) in plan.protections
    assert boundary in plan.protected
    assert boundary not in plan.candidates
    assert (
        PreflightArtifactProtection(boundary.bundle_digest, ("batch-deferred",)) in plan.protections
    )

    remaining = build_preflight_artifact_retention_plan(
        root=plan.root,
        records=(boundary, young),
        references=(),
        opaque_evidence=(),
        inventory_at=NOW,
        environment="staging",
        namespace="loom-staging",
    )
    assert remaining.candidates == (boundary,)


def test_opaque_evidence_blocks_all_candidates() -> None:
    inventory_ns = int(NOW.timestamp()) * 1_000_000_000
    old = _record(1, newest_ns=inventory_ns - 8 * DAY_NS)
    opaque = OpaqueArtifactEvidence(
        name="unexpected",
        kind="file",
        device=1,
        inode=5000,
        owner_uid=2001,
        owner_gid=2001,
        mode=0o600,
        link_count=1,
        size_bytes=5,
        modified_ns=inventory_ns,
        changed_ns=inventory_ns,
        reason="unknown-entry",
        sha256="f" * 64,
    )

    plan = build_preflight_artifact_retention_plan(
        root=_directory(99, newest_ns=inventory_ns),
        records=(old,),
        references=(),
        opaque_evidence=(opaque,),
        inventory_at=NOW,
        environment="staging",
        namespace="loom-staging",
    )

    assert plan.candidates == ()
    assert plan.protected == (old,)
    assert plan.protections == (PreflightArtifactProtection(old.bundle_digest, ("opaque-store",)),)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda record: replace(record, bundle_digest="A" * 64), "bundle"),
        (
            lambda record: replace(
                record,
                directory=replace(record.directory, mode=0o755),
            ),
            "directory",
        ),
        (
            lambda record: replace(
                record,
                files=(replace(record.files[0], link_count=2), *record.files[1:]),
            ),
            "file",
        ),
        (
            lambda record: replace(
                record,
                files=tuple(reversed(record.files)),
            ),
            "file",
        ),
    ],
)
def test_inventory_record_rejects_unsafe_or_noncanonical_identity(
    mutation,  # type: ignore[no-untyped-def]
    message: str,
) -> None:
    inventory_ns = int(NOW.timestamp()) * 1_000_000_000
    with pytest.raises(ValueError, match=message):
        mutation(_record(1, newest_ns=inventory_ns - 8 * DAY_NS))


def test_plan_rejects_duplicate_overlap_bad_reason_and_unbounded_candidates() -> None:
    inventory_ns = int(NOW.timestamp()) * 1_000_000_000
    records = tuple(
        _record(index, newest_ns=inventory_ns - (index + 8) * DAY_NS) for index in range(33)
    )
    base = build_preflight_artifact_retention_plan(
        root=_directory(99, newest_ns=inventory_ns),
        records=records,
        references=(),
        opaque_evidence=(),
        inventory_at=NOW,
        environment="staging",
        namespace="loom-staging",
    )

    with pytest.raises(ValueError, match="candidate"):
        replace(base, candidates=records)
    with pytest.raises(ValueError, match="overlap"):
        replace(base, protected=(base.candidates[0],))
    with pytest.raises(ValueError, match="protection"):
        PreflightArtifactProtection(records[0].bundle_digest, ("unknown-reason",))
    with pytest.raises(ValueError, match="inventory time"):
        replace(base, inventory_at=NOW.replace(tzinfo=None))


def test_plan_rejects_reference_to_missing_bundle() -> None:
    inventory_ns = int(NOW.timestamp()) * 1_000_000_000
    with pytest.raises(ValueError, match="missing"):
        build_preflight_artifact_retention_plan(
            root=_directory(99, newest_ns=inventory_ns),
            records=(_record(1, newest_ns=inventory_ns - 8 * DAY_NS),),
            references=(PreflightArtifactProtection("f" * 64, ("active-rollout",)),),
            opaque_evidence=(),
            inventory_at=NOW + timedelta(microseconds=1),
            environment="staging",
            namespace="loom-staging",
        )
