from __future__ import annotations

from loom.data_lifecycle_dirty_epoch_reconcile import build_dirty_epoch_reconcile_plan
from loom.data_lifecycle_gc import GcScope

SCOPE = GcScope(environment="staging", namespace="loom-staging")
EXECUTION_COUNTS = (
    ("artifacts", 9, 9),
    ("batches", 2, 2),
    ("llm_calls", 11, 11),
    ("trial_events", 15, 15),
    ("trials", 4, 3),
)


def _plan(**overrides: object):
    values: dict[str, object] = {
        "scope": SCOPE,
        "schema_revision": "0075",
        "state_fingerprint": "a" * 64,
        "epoch_count": 0,
        "epoch_event_count": 0,
        "gc_authority_count": 0,
        "gc_item_count": 0,
        "gc_run_count": 0,
        "authority_count": 12,
        "object_count": 9,
        "capacity_count": 1,
        "unsafe_authority_count": 0,
        "unsafe_object_count": 0,
        "execution_counts": EXECUTION_COUNTS,
    }
    values.update(overrides)
    return build_dirty_epoch_reconcile_plan(**values)  # type: ignore[arg-type]


def test_dirty_epoch_reconcile_plan_accepts_only_nonmutated_dirty_state() -> None:
    plan = _plan()

    assert plan.applicable
    assert plan.blockers == ()
    assert plan.classified_execution_count == 40
    assert plan.unclassified_execution_count == 1
    assert len(plan.inventory_digest) == 64


def test_dirty_epoch_reconcile_plan_digest_binds_exact_state_fingerprint() -> None:
    before = _plan(state_fingerprint="a" * 64)
    after = _plan(state_fingerprint="b" * 64)

    assert before.inventory_digest != after.inventory_digest


def test_dirty_epoch_reconcile_plan_rejects_clean_bootstrap_state() -> None:
    plan = _plan(
        authority_count=0,
        object_count=0,
        capacity_count=0,
        execution_counts=tuple((table, 0, 0) for table, _total, _classified in EXECUTION_COUNTS),
    )

    assert not plan.applicable
    assert plan.blockers == ("lifecycle state is clean; use standard bootstrap",)


def test_dirty_epoch_reconcile_plan_rejects_existing_mutation_or_gc_state() -> None:
    plan = _plan(
        epoch_count=1,
        epoch_event_count=1,
        gc_authority_count=1,
        gc_item_count=1,
        gc_run_count=1,
    )

    assert not plan.applicable
    assert plan.blockers == (
        "lifecycle GC state already exists",
        "mutation epoch authority already exists",
        "mutation epoch events already exist",
    )


def test_dirty_epoch_reconcile_plan_rejects_deletion_state() -> None:
    plan = _plan(unsafe_authority_count=1, unsafe_object_count=2)

    assert not plan.applicable
    assert plan.blockers == (
        "lifecycle authority deletion state already exists",
        "lifecycle object deletion state already exists",
    )


def test_dirty_epoch_reconcile_plan_is_pinned_to_reviewed_schema() -> None:
    plan = _plan(schema_revision="0076")

    assert not plan.applicable
    assert plan.blockers == (
        "dirty epoch reconciliation requires exact schema revision 0075",
    )
