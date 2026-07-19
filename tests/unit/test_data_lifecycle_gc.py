from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcPlan,
    GcScope,
    LifecycleGcExecutionError,
    LifecycleGcPlanError,
    ObservedObject,
    RegisteredObject,
    build_gc_plan,
    execute_gc,
    reconcile_object_inventory,
)

NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)
SCOPE = GcScope(environment="staging", namespace="loom-staging")


def _authority(**overrides: object) -> AuthorityInventory:
    values: dict[str, object] = {
        "id": uuid4(),
        "environment": "staging",
        "namespace": "loom-staging",
        "owner_kind": "trial",
        "owner_id": "trial-1",
        "expires_at": NOW - timedelta(seconds=1),
        "pinned": False,
        "state": "active",
    }
    values.update(overrides)
    return AuthorityInventory(**values)  # type: ignore[arg-type]


def _object(authority_id: UUID, **overrides: object) -> RegisteredObject:
    values: dict[str, object] = {
        "id": uuid4(),
        "authority_id": authority_id,
        "environment": "staging",
        "namespace": "loom-staging",
        "bucket": "loom-staging-artifacts",
        "object_key": "teams/t/trials/1/trajectory.jsonl",
        "version_id": None,
        "content_sha256": "a" * 64,
        "size_bytes": 42,
        "state": "active",
    }
    values.update(overrides)
    return RegisteredObject(**values)  # type: ignore[arg-type]


def _plan() -> GcPlan:
    authority = _authority()
    return build_gc_plan(
        scope=SCOPE,
        mutation_epoch=7,
        now=NOW,
        authorities=[authority],
        objects=[_object(authority.id)],
    )


def test_gc_scope_is_staging_only() -> None:
    with pytest.raises(ValueError, match="staging-only"):
        GcScope(environment="production", namespace="loom-prod")


def test_plan_is_deterministic_and_selects_only_expired_authority() -> None:
    expired = _authority(owner_id="expired")
    live = _authority(owner_id="live", expires_at=NOW + timedelta(days=1))
    pinned = _authority(owner_id="pinned", expires_at=None, pinned=True)
    deleted = _object(expired.id)
    plan = build_gc_plan(
        scope=SCOPE,
        mutation_epoch=3,
        now=NOW,
        authorities=[live, expired, pinned],
        objects=[deleted, _object(live.id), _object(pinned.id)],
    )
    reversed_plan = build_gc_plan(
        scope=SCOPE,
        mutation_epoch=3,
        now=NOW,
        authorities=[pinned, expired, live],
        objects=[_object(pinned.id), _object(live.id), deleted],
    )
    assert plan.authority_ids == (expired.id,)
    assert plan.objects == (deleted,)
    assert plan.inventory_digest == reversed_plan.inventory_digest


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("environment", "production", "crosses GC scope"),
        ("namespace", "other", "crosses GC scope"),
        ("content_sha256", None, "lacks an exact SHA-256"),
        ("content_sha256", "not-a-sha", "invalid SHA-256"),
        ("state", "delete_pending", "is not active"),
        ("size_bytes", -1, "negative size"),
    ],
)
def test_plan_blocks_ambiguous_or_cross_scope_objects(
    field: str,
    value: object,
    match: str,
) -> None:
    authority = _authority()
    plan = build_gc_plan(
        scope=SCOPE,
        mutation_epoch=0,
        now=NOW,
        authorities=[authority],
        objects=[replace(_object(authority.id), **{field: value})],
    )
    with pytest.raises(LifecycleGcPlanError, match=match):
        plan.require_applicable()


def test_versioned_object_does_not_require_content_hash() -> None:
    authority = _authority()
    item = _object(authority.id, version_id="v1", content_sha256=None)
    plan = build_gc_plan(
        scope=SCOPE,
        mutation_epoch=0,
        now=NOW,
        authorities=[authority],
        objects=[item],
    )
    plan.require_applicable()
    assert plan.objects == (item,)


def test_reconciliation_reports_both_orphan_directions() -> None:
    authority = _authority()
    registered = _object(authority.id)
    missing = _object(authority.id, object_key="missing")
    report = reconcile_object_inventory(
        registered=[registered, missing],
        observed=[
            ObservedObject(
                bucket=registered.bucket,
                object_key=registered.object_key,
                version_id=None,
                size_bytes=registered.size_bytes,
            ),
            ObservedObject(
                bucket=registered.bucket,
                object_key="unknown",
                version_id=None,
                size_bytes=1,
            ),
        ],
    )
    assert report.registered_missing == ((registered.bucket, "missing", ""),)
    assert report.observed_unregistered == ((registered.bucket, "unknown", ""),)
    assert not report.clean


class _Journal:
    def __init__(self, *, epoch_after: int = 8) -> None:
        self.run_id = uuid4()
        self.epoch_after = epoch_after
        self.events: list[tuple[str, object]] = []

    def record_dry_run(self, *, plan: GcPlan, requested_by: str) -> UUID:
        self.events.append(("dry_run", (plan.inventory_digest, requested_by)))
        return self.run_id

    def begin_apply(
        self,
        *,
        plan: GcPlan,
        requested_by: str,
        deletion_token: UUID,
    ) -> UUID:
        self.events.append(("begin", (plan.mutation_epoch, requested_by, deletion_token)))
        return self.run_id

    def record_object_deleted(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None:
        self.events.append(("deleted", (run_id, object_id, deletion_token)))

    def record_object_verified(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None:
        self.events.append(("verified", (run_id, object_id, deletion_token)))

    def delete_business_metadata(
        self,
        *,
        run_id: UUID,
        authority_ids: tuple[UUID, ...],
        deletion_token: UUID,
    ) -> None:
        self.events.append(("metadata", (run_id, authority_ids, deletion_token)))

    def complete_apply(
        self,
        *,
        run_id: UUID,
        expected_epoch: int,
        deletion_token: UUID,
    ) -> int:
        self.events.append(("complete", (run_id, expected_epoch, deletion_token)))
        return self.epoch_after

    def fail_apply(self, *, run_id: UUID, reason: str) -> None:
        self.events.append(("failed", (run_id, reason)))


class _Deleter:
    def __init__(self, *, verify: bool = True) -> None:
        self.verify = verify
        self.deleted: list[UUID] = []

    def delete_exact(self, item: RegisteredObject) -> None:
        self.deleted.append(item.id)

    def exact_absent(self, item: RegisteredObject) -> bool:
        return self.verify and item.id in self.deleted


def test_dry_run_never_calls_object_store_or_mutates_epoch() -> None:
    journal = _Journal()
    deleter = _Deleter()
    result = execute_gc(
        plan=_plan(),
        requested_by="qianyi",
        journal=journal,
        object_deleter=deleter,
        dry_run=True,
    )
    assert result.dry_run
    assert result.mutation_epoch_after is None
    assert deleter.deleted == []
    assert [event[0] for event in journal.events] == ["dry_run"]


def test_apply_orders_delete_verify_metadata_and_epoch() -> None:
    plan = _plan()
    journal = _Journal()
    result = execute_gc(
        plan=plan,
        requested_by="qianyi",
        journal=journal,
        object_deleter=_Deleter(),
        dry_run=False,
    )
    assert result.mutation_epoch_after == 8
    assert result.deleted_objects == 1
    assert result.deleted_bytes == 42
    assert [event[0] for event in journal.events] == [
        "begin",
        "deleted",
        "verified",
        "metadata",
        "complete",
    ]


def test_apply_seals_failure_before_business_metadata_removal() -> None:
    journal = _Journal()
    with pytest.raises(LifecycleGcExecutionError, match="still present"):
        execute_gc(
            plan=_plan(),
            requested_by="qianyi",
            journal=journal,
            object_deleter=_Deleter(verify=False),
            dry_run=False,
        )
    assert [event[0] for event in journal.events] == ["begin", "deleted", "failed"]


def test_apply_rejects_non_monotonic_epoch_completion() -> None:
    with pytest.raises(LifecycleGcExecutionError, match="non-monotonic"):
        execute_gc(
            plan=_plan(),
            requested_by="qianyi",
            journal=_Journal(epoch_after=9),
            object_deleter=_Deleter(),
            dry_run=False,
        )
