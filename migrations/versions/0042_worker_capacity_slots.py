"""workers: resource-pool slot capacity fields

Revision ID: 0042
Revises: 0041
Create Date: 2026-06-26

Persist worker-local slot capacity at registration so Monitor, CLI, and
metrics can report execution slots instead of inferring concurrency from
worker process count.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0042"
down_revision = "0041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workers",
        sa.Column(
            "max_concurrent",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )
    op.add_column(
        "workers",
        sa.Column(
            "pool_name",
            sa.Text(),
            nullable=False,
            server_default="default",
        ),
    )
    op.create_check_constraint(
        "workers_max_concurrent_positive_check",
        "workers",
        "max_concurrent > 0",
    )
    op.create_check_constraint(
        "workers_pool_name_nonempty_check",
        "workers",
        "length(trim(pool_name)) > 0",
    )
    op.create_index(
        "idx_workers_fresh_capacity",
        "workers",
        ["status", "last_seen_at", "pool_name"],
    )


def downgrade() -> None:
    op.drop_index("idx_workers_fresh_capacity", table_name="workers")
    op.drop_constraint(
        "workers_pool_name_nonempty_check",
        "workers",
        type_="check",
    )
    op.drop_constraint(
        "workers_max_concurrent_positive_check",
        "workers",
        type_="check",
    )
    op.drop_column("workers", "pool_name")
    op.drop_column("workers", "max_concurrent")
