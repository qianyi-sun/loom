"""Persist GB10 desired source checkout commit.

The node-agent must compare source checkout provenance against desired state,
not just image/env tags, so public-beta rollouts cannot report convergence
while claimable GB10 workers still run stale source.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0059"
down_revision = "0058"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "gb10_worker_pool_desired_states",
        sa.Column("source_git_commit", sa.Text(), nullable=True),
    )
    op.add_column(
        "gb10_worker_pool_desired_states",
        sa.Column("previous_source_git_commit", sa.Text(), nullable=True),
    )
    op.add_column(
        "gb10_worker_node_statuses",
        sa.Column("desired_source_git_commit", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("gb10_worker_node_statuses", "desired_source_git_commit")
    op.drop_column("gb10_worker_pool_desired_states", "previous_source_git_commit")
    op.drop_column("gb10_worker_pool_desired_states", "source_git_commit")
