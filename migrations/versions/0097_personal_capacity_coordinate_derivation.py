"""Enforce derived personal capacity authority coordinates.

Revision ID: 0097
Revises: 0096
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0097"
down_revision = "0096"
branch_labels = None
depends_on = None


_DERIVED_CHECK = (
    "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
    "OR (candidate_id IS NOT NULL AND capacity_namespace = 'loom-dev-' || name "
    "AND capacity_database = 'loom_dev_' || replace(name, '-', '_'))"
)

_SYNTAX_CHECK = (
    "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
    "OR (candidate_id IS NOT NULL AND capacity_namespace ~ '^[a-z0-9-]{1,63}$' "
    "AND capacity_database ~ '^[a-z0-9_]{1,63}$')"
)


def upgrade() -> None:
    op.drop_constraint("dev_instances_personal_capacity_identity_check", "dev_instances")
    op.execute(
        "UPDATE dev_instances SET capacity_namespace = 'loom-dev-' || name, "
        "capacity_database = 'loom_dev_' || replace(name, '-', '_') "
        "WHERE candidate_id IS NOT NULL"
    )
    op.create_check_constraint(
        "dev_instances_personal_capacity_identity_check",
        "dev_instances",
        _DERIVED_CHECK,
    )


def downgrade() -> None:
    op.drop_constraint("dev_instances_personal_capacity_identity_check", "dev_instances")
    op.create_check_constraint(
        "dev_instances_personal_capacity_identity_check",
        "dev_instances",
        sa.text(_SYNTAX_CHECK),
    )
