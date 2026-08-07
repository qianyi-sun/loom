"""Persist exactly-once autoscaler pool assignment for neutral trials.

Revision ID: 0077
Revises: 0076
Create Date: 2026-08-07
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0077"
down_revision = "0076"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "trials",
        sa.Column("autoscaler_pool_name", sa.Text(), nullable=True),
    )
    op.add_column(
        "trials",
        sa.Column(
            "autoscaler_pool_assigned_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "trials_autoscaler_pool_assignment_check",
        "trials",
        "(autoscaler_pool_name IS NULL AND autoscaler_pool_assigned_at IS NULL) "
        "OR (length(trim(autoscaler_pool_name)) > 0 "
        "AND autoscaler_pool_assigned_at IS NOT NULL)",
    )
    op.create_index(
        "trials_queued_autoscaler_pool_idx",
        "trials",
        ["autoscaler_pool_name", "submitted_at"],
        postgresql_where=sa.text("state = 'queued'"),
    )


def downgrade() -> None:
    op.drop_index("trials_queued_autoscaler_pool_idx", table_name="trials")
    op.drop_constraint(
        "trials_autoscaler_pool_assignment_check",
        "trials",
        type_="check",
    )
    op.drop_column("trials", "autoscaler_pool_assigned_at")
    op.drop_column("trials", "autoscaler_pool_name")
