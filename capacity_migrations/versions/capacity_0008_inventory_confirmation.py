"""bind retirement to exact inventory journal confirmation

Revision ID: capacity_0008
Revises: capacity_0007
Create Date: 2026-08-13
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0008"
down_revision: str | Sequence[str] | None = "capacity_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DIGEST_CHECK = (
    "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
    "AND ((heartbeat_high_water = 0 AND last_heartbeat_digest IS NULL) "
    "OR (heartbeat_high_water > 0 AND last_heartbeat_digest ~ '^[0-9a-f]{64}$')) "
    "AND ((command_high_water = 0 AND last_command_digest IS NULL) "
    "OR (command_high_water > 0 AND last_command_digest ~ '^[0-9a-f]{64}$')) "
    "AND ((journal_high_water = 0 AND journal_digest = repeat('0', 64)) "
    "OR (journal_high_water > 0 AND journal_digest ~ '^[0-9a-f]{64}$' "
    "AND journal_digest <> repeat('0', 64))) "
    "AND ((inventory_high_water = 0 AND last_inventory_digest IS NULL) "
    "OR (inventory_high_water > 0 AND last_inventory_digest ~ '^[0-9a-f]{64}$'))"
)

_CONFIRMATION_DIGEST_CHECK = (
    _DIGEST_CHECK + " AND (inventory_confirmation_journal_digest IS NULL OR "
    "inventory_confirmation_journal_digest ~ '^[0-9a-f]{64}$')"
)

_OLD_RETIREMENT_CHECK = (
    "(retirement_safe AND retirement_inventory_digest IS NOT NULL "
    "AND retirement_inventory_digest ~ '^[0-9a-f]{64}$' "
    "AND retirement_inventory_digest = last_inventory_digest "
    "AND inventory_high_water > 0 AND inventory_payload IS NOT NULL "
    "AND jsonb_typeof(inventory_payload) = 'object' "
    "AND last_inventory_at IS NOT NULL "
    "AND inventory_payload -> 'schema_version' = '2'::jsonb "
    "AND inventory_payload -> 'inventory_sequence' = to_jsonb(inventory_high_water) "
    "AND inventory_payload ->> 'executor_id' = executor_id "
    "AND inventory_payload ->> 'executor_incarnation' = executor_incarnation::text "
    "AND inventory_payload ->> 'pool_id' = pool_id "
    "AND inventory_payload -> 'pool_generation' = to_jsonb(pool_generation) "
    "AND inventory_payload -> 'journal_sequence' = to_jsonb(journal_high_water) "
    "AND inventory_payload ->> 'journal_digest' = journal_digest "
    "AND inventory_payload -> 'execution' -> 'execution_epoch' = to_jsonb(execution_epoch) "
    "AND inventory_payload -> 'execution' ->> 'execution_manifest_sha256' "
    "= execution_manifest_sha256) OR "
    "(NOT retirement_safe AND retirement_inventory_digest IS NULL)"
)

_CONFIRMATION_RETIREMENT_CHECK = (
    "(retirement_safe AND retirement_inventory_digest IS NOT NULL "
    "AND retirement_inventory_digest ~ '^[0-9a-f]{64}$' "
    "AND retirement_inventory_digest = last_inventory_digest "
    "AND inventory_high_water > 0 AND inventory_payload IS NOT NULL "
    "AND jsonb_typeof(inventory_payload) = 'object' "
    "AND last_inventory_at IS NOT NULL "
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
    "= execution_manifest_sha256) OR "
    "(NOT retirement_safe AND retirement_inventory_digest IS NULL)"
)


def _replace_checks(*, digest_check: str, retirement_check: str) -> None:
    op.drop_constraint(
        "capacity_executable_executor_state_digest_check",
        "capacity_executable_executor_states",
        type_="check",
    )
    op.drop_constraint(
        "capacity_executable_executor_retirement_check",
        "capacity_executable_executor_states",
        type_="check",
    )
    op.create_check_constraint(
        "capacity_executable_executor_state_digest_check",
        "capacity_executable_executor_states",
        digest_check,
    )
    op.create_check_constraint(
        "capacity_executable_executor_retirement_check",
        "capacity_executable_executor_states",
        retirement_check,
    )


def upgrade() -> None:
    op.add_column(
        "capacity_executable_executor_states",
        sa.Column("inventory_confirmation_journal_digest", sa.Text(), nullable=True),
    )
    op.execute(
        "UPDATE capacity_executable_executor_states SET retirement_safe = false, "
        "retirement_inventory_digest = NULL"
    )
    _replace_checks(
        digest_check=_CONFIRMATION_DIGEST_CHECK,
        retirement_check=_CONFIRMATION_RETIREMENT_CHECK,
    )


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN "
        "IF EXISTS (SELECT 1 FROM capacity_executable_executor_states "
        "WHERE inventory_confirmation_journal_digest IS NOT NULL) THEN "
        "RAISE EXCEPTION 'cannot downgrade capacity_0008 with inventory confirmation evidence'; "
        "END IF; END $$"
    )
    _replace_checks(
        digest_check=_DIGEST_CHECK,
        retirement_check=_OLD_RETIREMENT_CHECK,
    )
    op.drop_column(
        "capacity_executable_executor_states",
        "inventory_confirmation_journal_digest",
    )
