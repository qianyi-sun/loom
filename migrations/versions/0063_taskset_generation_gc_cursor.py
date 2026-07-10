"""Persist scheduling progress for bounded live TaskSet generation GC.

Revision ID: 0063
Revises: 0062
Create Date: 2026-07-10

This singleton stores only the next bounded reconciliation sequence.  It has
no bucket, object prefix, team, TaskSet, job, or deletion-authority fields.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0063"
down_revision = "0062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_set_generation_gc_cursors",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column(
            "next_sweep",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.CheckConstraint(
            "name = 'live-generation-gc'",
            name="task_set_generation_gc_cursors_name_check",
        ),
        sa.CheckConstraint(
            "next_sweep >= 0",
            name="task_set_generation_gc_cursors_next_sweep_nonneg_check",
        ),
        sa.PrimaryKeyConstraint("name"),
    )


def downgrade() -> None:
    op.drop_table("task_set_generation_gc_cursors")
