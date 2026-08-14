"""Persist immutable local authority coordinates for personal capacity status.

Revision ID: 0090
Revises: 0089
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0090"
down_revision = "0089"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dev_instances", sa.Column("capacity_namespace", sa.Text(), nullable=True))
    op.add_column("dev_instances", sa.Column("capacity_database", sa.Text(), nullable=True))
    # Existing rows are bound once during this forward migration; request-time
    # status reads never recover authority coordinates from a display name.
    op.execute(
        "UPDATE dev_instances SET capacity_namespace = 'loom-dev-' || name, "
        "capacity_database = 'loom_dev_' || replace(name, '-', '_') "
        "WHERE candidate_id IS NOT NULL"
    )
    op.create_check_constraint(
        "dev_instances_personal_capacity_identity_check",
        "dev_instances",
        "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
        "OR (candidate_id IS NOT NULL AND capacity_namespace ~ '^[a-z0-9-]{1,63}$' "
        "AND capacity_database ~ '^[a-z0-9_]{1,63}$')",
    )


def downgrade() -> None:
    op.drop_constraint("dev_instances_personal_capacity_identity_check", "dev_instances")
    op.drop_column("dev_instances", "capacity_database")
    op.drop_column("dev_instances", "capacity_namespace")
