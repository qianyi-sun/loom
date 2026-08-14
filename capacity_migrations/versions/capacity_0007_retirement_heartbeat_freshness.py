"""Require post-inventory heartbeat for database retirement safety.

Revision ID: capacity_0007
Revises: capacity_0006
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "capacity_0007"
down_revision: str | Sequence[str] | None = "capacity_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RETIREMENT_CHECK = (
    "((retirement_safe AND retirement_inventory_digest IS NOT NULL "
    "AND retirement_inventory_digest ~ '^[0-9a-f]{64}$' "
    "AND retirement_inventory_digest = last_inventory_digest "
    "AND inventory_high_water > 0 AND inventory_payload IS NOT NULL "
    "AND jsonb_typeof(inventory_payload) = 'object' "
    "AND last_inventory_at IS NOT NULL "
    "AND last_heartbeat_at > last_inventory_at "
    "AND inventory_payload -> 'schema_version' = '2'::jsonb "
    "AND inventory_payload -> 'inventory_sequence' = to_jsonb(inventory_high_water) "
    "AND inventory_payload ->> 'executor_id' = executor_id "
    "AND inventory_payload ->> 'executor_incarnation' = executor_incarnation::text "
    "AND inventory_payload ->> 'pool_id' = pool_id "
    "AND inventory_payload -> 'pool_generation' = to_jsonb(pool_generation) "
    "AND inventory_confirmation_journal_digest ~ '^[0-9a-f]{64}$' "
    "AND ((inventory_payload -> 'journal_sequence' = to_jsonb(journal_high_water) "
    "AND inventory_payload ->> 'journal_digest' = journal_digest) OR "
    "(inventory_payload -> 'journal_sequence' = to_jsonb(journal_high_water - 2) "
    "AND inventory_confirmation_journal_digest = journal_digest)) "
    "AND inventory_payload -> 'execution' -> 'execution_epoch' = to_jsonb(execution_epoch) "
    "AND inventory_payload -> 'execution' ->> 'execution_manifest_sha256' "
    "= execution_manifest_sha256) IS TRUE) OR "
    "(NOT retirement_safe AND retirement_inventory_digest IS NULL)"
)

_OLD_RETIREMENT_CHECK = _RETIREMENT_CHECK.replace(
    "AND last_heartbeat_at > last_inventory_at ",
    "",
)


def upgrade() -> None:
    op.execute(
        "UPDATE capacity_executable_executor_states SET retirement_safe = false, "
        "retirement_inventory_digest = NULL WHERE retirement_safe"
    )
    op.drop_constraint(
        "capacity_executable_executor_retirement_check",
        "capacity_executable_executor_states",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_executable_executor_retirement_check",
        "capacity_executable_executor_states",
        _RETIREMENT_CHECK,
    )


def downgrade() -> None:
    op.drop_constraint(
        "capacity_executable_executor_retirement_check",
        "capacity_executable_executor_states",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_executable_executor_retirement_check",
        "capacity_executable_executor_states",
        _OLD_RETIREMENT_CHECK,
    )
