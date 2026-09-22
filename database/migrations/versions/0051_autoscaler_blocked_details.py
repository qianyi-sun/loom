"""Persist autoscaler blocked diagnostics."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0051"
down_revision = "0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "worker_pool_autoscaler_policies",
        sa.Column(
            "last_blocked_details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("worker_pool_autoscaler_policies", "last_blocked_details")
