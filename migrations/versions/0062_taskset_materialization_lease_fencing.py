"""Persist TaskSet materialization lease and publication fencing state.

Revision ID: 0062
Revises: 0061
Create Date: 2026-07-10

Claimed and running jobs created before lease heartbeats existed retain their
claim timestamp as their initial heartbeat. Queued jobs do not have an owner
and therefore intentionally retain a null heartbeat.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0062"
down_revision = "0061"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "task_set_materialization_jobs",
        sa.Column(
            "lease_epoch",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "task_set_materialization_jobs",
        sa.Column(
            "lease_heartbeat_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "task_set_materialization_jobs",
        sa.Column(
            "published_materialization_generation",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.execute(
        """
        UPDATE task_set_materialization_jobs
           SET lease_heartbeat_at = claimed_at
         WHERE state IN ('claimed', 'running')
           AND claimed_at IS NOT NULL
        """,
    )
    op.create_index(
        "task_set_materialization_jobs_active_heartbeat_idx",
        "task_set_materialization_jobs",
        ["lease_heartbeat_at"],
        postgresql_where=sa.text(
            "state IN ('claimed', 'running') "
            "AND lease_heartbeat_at IS NOT NULL",
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "task_set_materialization_jobs_active_heartbeat_idx",
        table_name="task_set_materialization_jobs",
    )
    op.drop_column(
        "task_set_materialization_jobs",
        "published_materialization_generation",
    )
    op.drop_column("task_set_materialization_jobs", "lease_heartbeat_at")
    op.drop_column("task_set_materialization_jobs", "lease_epoch")
