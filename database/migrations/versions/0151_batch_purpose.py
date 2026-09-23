"""Add batches.purpose for evaluation vs trajectory_generation.

Revision ID: 0151
Revises: 0150

Purpose distinguishes catalog intent at batch create time:
evaluation = native benchmarks with verification required;
trajectory_generation = TaskSets and/or benchmarks (transition),
verifier optional. Existing rows backfill to trajectory_generation.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0151"
down_revision = "0150"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "batches",
        sa.Column(
            "purpose",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'trajectory_generation'"),
        ),
    )
    op.create_check_constraint(
        "batches_purpose_check",
        "batches",
        "purpose IN ('evaluation', 'trajectory_generation')",
    )
    # Older service replicas omit this field during a rolling upgrade. Keep
    # their historical meaning compatible; the new API requires purpose and
    # rejects missing values independently of this database compatibility default.


def downgrade() -> None:
    op.drop_constraint("batches_purpose_check", "batches", type_="check")
    op.drop_column("batches", "purpose")
