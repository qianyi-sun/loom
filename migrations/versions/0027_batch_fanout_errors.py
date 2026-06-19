"""batches: persist fan-out submit failures (#281)

Revision ID: 0027
Revises: 0026
Create Date: 2026-06-19

Batch fan-out can fail before Control Plane accepts a child Trial, for
example when a task is rejected by team license policy. Store those failures
on the batch so operators can diagnose the blocker and the runner can skip
known non-retryable units on later ticks.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "batches",
        sa.Column(
            "fanout_errors",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )


def downgrade() -> None:
    op.drop_column("batches", "fanout_errors")
