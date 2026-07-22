from __future__ import annotations

from loom.data_lifecycle_bootstrap import build_lifecycle_bootstrap_plan
from loom.data_lifecycle_gc import GcScope

SCOPE = GcScope(environment="staging", namespace="loom-staging")


def test_empty_post_migration_database_is_bootstrap_applicable() -> None:
    plan = build_lifecycle_bootstrap_plan(scope=SCOPE, schema_revision="0069")

    plan.require_applicable_or_converged()
    assert plan.applicable
    assert not plan.converged


def test_pre_lifecycle_revision_is_not_bootstrap_applicable() -> None:
    plan = build_lifecycle_bootstrap_plan(scope=SCOPE, schema_revision="0067")

    assert plan.blockers == ("lifecycle bootstrap requires schema revision 0069 or later",)


def test_exact_bootstrap_row_is_idempotently_converged() -> None:
    plan = build_lifecycle_bootstrap_plan(
        scope=SCOPE,
        schema_revision="0069",
        epoch_rows=(("staging", "loom-staging", 0, "bootstrap", None, None),),
    )

    plan.require_applicable_or_converged()
    assert plan.converged
    assert not plan.applicable


def test_registry_or_existing_nonbootstrap_epoch_blocks_initialization() -> None:
    plan = build_lifecycle_bootstrap_plan(
        scope=SCOPE,
        schema_revision="0069",
        epoch_rows=(("staging", "loom-staging", 1, "lifecycle_gc", "req-gc0000000", "a" * 64),),
        authority_count=1,
    )

    assert not plan.applicable
    assert not plan.converged
    assert plan.blockers == (
        "existing mutation epoch authority is not the exact bootstrap row",
        "lifecycle registry or GC journal is not empty",
    )


def test_digest_binds_revision_and_classified_counts() -> None:
    baseline = build_lifecycle_bootstrap_plan(scope=SCOPE, schema_revision="0069")
    changed = build_lifecycle_bootstrap_plan(
        scope=SCOPE,
        schema_revision="0069",
        classified_row_counts=(("trials", 1),),
    )

    assert baseline.inventory_digest != changed.inventory_digest
    assert changed.blockers == ("execution rows already carry lifecycle authority",)


def test_bootstrap_rejects_an_alternate_staging_namespace() -> None:
    plan = build_lifecycle_bootstrap_plan(
        scope=GcScope(environment="staging", namespace="another-staging"),
        schema_revision="0069",
    )

    assert plan.blockers == ("lifecycle bootstrap is fixed to staging/loom-staging",)
