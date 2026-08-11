"""Expand the inert legacy fence to the complete workload mutation inventory.

Revision ID: guard_0008
Revises: guard_0007
Create Date: 2026-08-10

The original fence predated the unified Pipeline ExecutionAttempt worker
protocol.  Existing preparation evidence cannot be reinterpreted as covering
writers it never observed, so this migration refuses to rewrite a non-empty
fence.  The guard is still disabled and disconnected; an operator that
experimentally prepared obsolete evidence must explicitly downgrade past
guard_0005 and prepare a fresh complete inventory.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0008"
down_revision: str | Sequence[str] | None = "guard_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
OLD_INVENTORY_DIGEST = "81b48ba31d00111a532b2317248357f8af05a40b53e4b2b8bf7cd00c3ad59616"
NEW_INVENTORY_DIGEST = "607f561f4af380cb1452995512e2ec4ab32490c29552b7a6618a3f76fea168a4"
OLD_MUTATION_PATH_IDS = (
    "batch-hard-budget-cancel",
    "batch-user-cancel",
    "dead-worker-reclaim",
    "dev-environment-destroy",
    "family-finalize-cascade",
    "legacy-compatibility-writer",
    "neutral-pool-assignment",
    "pre-start-heartbeat",
    "pre-start-retry-requeue",
    "queued-to-claimed",
    "single-trial-cancel",
    "slurm-job-launch-registry-release",
    "stale-running-failure",
    "trial-requirement-and-lifecycle-binding",
    "trial-submission",
    "worker-drain-and-release",
    "worker-heartbeat-status",
    "worker-registration",
    "worker-result-state",
    "worker-token-issuance",
)
NEW_MUTATION_PATH_IDS = (
    "batch-hard-budget-cancel",
    "batch-user-cancel",
    "dead-worker-reclaim",
    "dev-environment-destroy",
    "execution-attempt-heartbeat",
    "execution-attempt-queued-to-claimed",
    "execution-attempt-result-state",
    "execution-attempt-worker-loss",
    "family-finalize-cascade",
    "legacy-compatibility-writer",
    "neutral-pool-assignment",
    "pipeline-attempt-cancellation",
    "pipeline-attempt-retry",
    "pipeline-attempt-submission",
    "pre-start-heartbeat",
    "pre-start-retry-requeue",
    "queued-to-claimed",
    "single-trial-cancel",
    "slurm-job-launch-registry-release",
    "stale-running-failure",
    "trial-requirement-and-lifecycle-binding",
    "trial-submission",
    "worker-drain-and-release",
    "worker-heartbeat-status",
    "worker-registration",
    "worker-result-state",
    "worker-token-issuance",
)


def _quoted_literals(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _assert_fence_is_empty() -> None:
    counts = (
        op.get_bind()
        .execute(
            sa.text(
                f"""
            SELECT
              (SELECT count(*) FROM {SCHEMA}.legacy_compatibility_preparations)
                AS preparations,
              (SELECT count(*) FROM {SCHEMA}.legacy_writer_cursors) AS cursors,
              (SELECT count(*) FROM {SCHEMA}.legacy_compatibility_freezes) AS freezes
            """
            )
        )
        .mappings()
        .one()
    )
    if any(int(counts[key]) != 0 for key in ("preparations", "cursors", "freezes")):
        raise RuntimeError(
            "legacy compatibility evidence uses an obsolete mutation inventory; "
            "downgrade past guard_0005 and prepare fresh complete evidence"
        )


def _rewrite_validation_function(
    name: str,
    *,
    old_digest: str,
    new_digest: str,
    old_paths: tuple[str, ...],
    new_paths: tuple[str, ...],
) -> None:
    signature = f"{SCHEMA}.{name}(uuid,jsonb,bytea,text)"
    definition = (
        op.get_bind()
        .execute(
            sa.text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
            {"signature": signature},
        )
        .scalar_one()
    )
    old_path_sql = _quoted_literals(old_paths)
    new_path_sql = _quoted_literals(new_paths)
    old_floor = f"v_cursor_count < {len(old_paths)}"
    new_floor = f"v_cursor_count < {len(new_paths)}"
    if old_path_sql not in definition or old_floor not in definition:
        raise RuntimeError(f"{signature} does not match the expected prior inventory policy")
    rewritten = definition.replace(old_path_sql, new_path_sql).replace(old_floor, new_floor)
    if old_digest in rewritten:
        rewritten = rewritten.replace(old_digest, new_digest)
    if old_path_sql in rewritten or old_floor in rewritten or old_digest in rewritten:
        raise RuntimeError(f"{signature} retained obsolete inventory validation")
    op.execute(rewritten)


def _replace_constraints(
    *,
    inventory_digest: str,
    mutation_path_ids: tuple[str, ...],
) -> None:
    op.drop_constraint(
        "guard_legacy_freeze_inventory_check",
        "legacy_compatibility_freezes",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_constraint(
        "guard_legacy_cursor_path_check",
        "legacy_writer_cursors",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_constraint(
        "guard_legacy_preparation_inventory_check",
        "legacy_compatibility_preparations",
        schema=SCHEMA,
        type_="check",
    )
    op.create_check_constraint(
        "guard_legacy_preparation_inventory_check",
        "legacy_compatibility_preparations",
        f"mutation_inventory_digest = '{inventory_digest}'",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "guard_legacy_cursor_path_check",
        "legacy_writer_cursors",
        f"mutation_path_id IN ({_quoted_literals(mutation_path_ids)})",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "guard_legacy_freeze_inventory_check",
        "legacy_compatibility_freezes",
        f"mutation_inventory_digest = '{inventory_digest}'",
        schema=SCHEMA,
    )


def _migrate_policy(
    *,
    old_digest: str,
    new_digest: str,
    old_paths: tuple[str, ...],
    new_paths: tuple[str, ...],
) -> None:
    _assert_fence_is_empty()
    for function in (
        "prepare_inert_legacy_compatibility",
        "freeze_inert_legacy_compatibility",
    ):
        _rewrite_validation_function(
            function,
            old_digest=old_digest,
            new_digest=new_digest,
            old_paths=old_paths,
            new_paths=new_paths,
        )
    _replace_constraints(
        inventory_digest=new_digest,
        mutation_path_ids=new_paths,
    )


def upgrade() -> None:
    _migrate_policy(
        old_digest=OLD_INVENTORY_DIGEST,
        new_digest=NEW_INVENTORY_DIGEST,
        old_paths=OLD_MUTATION_PATH_IDS,
        new_paths=NEW_MUTATION_PATH_IDS,
    )


def downgrade() -> None:
    _migrate_policy(
        old_digest=NEW_INVENTORY_DIGEST,
        new_digest=OLD_INVENTORY_DIGEST,
        old_paths=NEW_MUTATION_PATH_IDS,
        new_paths=OLD_MUTATION_PATH_IDS,
    )
