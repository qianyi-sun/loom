"""Regression tests for durable lifecycle-GC journal reconstruction."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcScope,
    RegisteredObject,
    build_gc_plan,
)
from loom.data_lifecycle_gc_sql import SqlAlchemyGcJournal, _plan_inventory


def test_compact_resume_reconstructs_canonical_object_order() -> None:
    """SQL UUID ordering must not invalidate an identity-ordered GC plan."""
    authority_id = UUID("10000000-0000-0000-0000-000000000000")
    deletion_token = UUID("20000000-0000-0000-0000-000000000000")
    planned_at = datetime(2026, 8, 14, 18, 10, tzinfo=UTC)
    plan = build_gc_plan(
        scope=GcScope(environment="staging", namespace="loom-staging"),
        mutation_epoch=26,
        now=planned_at,
        authorities=(
            AuthorityInventory(
                id=authority_id,
                environment="staging",
                namespace="loom-staging",
                owner_kind="trial",
                owner_id="resume-order-regression",
                pinned=False,
                expires_at=planned_at - timedelta(days=1),
                state="active",
            ),
        ),
        objects=(
            RegisteredObject(
                id=UUID("00000000-0000-0000-0000-000000000001"),
                authority_id=authority_id,
                environment="staging",
                namespace="loom-staging",
                bucket="artifacts",
                object_key="z-last",
                version_id="version-z",
                content_sha256="a" * 64,
                size_bytes=2,
                state="active",
            ),
            RegisteredObject(
                id=UUID("00000000-0000-0000-0000-000000000002"),
                authority_id=authority_id,
                environment="staging",
                namespace="loom-staging",
                bucket="artifacts",
                object_key="a-first",
                version_id="version-a",
                content_sha256="b" * 64,
                size_bytes=1,
                state="active",
            ),
        ),
    )
    rows = tuple(
        (
            item.id,
            deletion_token,
            "marked",
            item.authority_id,
            item.bucket,
            item.object_key,
            item.version_id,
            item.content_sha256,
            item.size_bytes,
        )
        for item in sorted(plan.objects, key=lambda item: item.id)
    )

    reconstructed = SqlAlchemyGcJournal._resume_plan_from_items(
        inventory=_plan_inventory(plan),
        rows=rows,
        authority_rows=((authority_id, deletion_token),),
    )

    assert [item.object_key for item in reconstructed.objects] == ["a-first", "z-last"]
    assert reconstructed.inventory_digest == plan.inventory_digest
