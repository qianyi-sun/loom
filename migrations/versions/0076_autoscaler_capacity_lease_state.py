"""Persist shared-capacity lease retirement state on autoscaler policies."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0076"
down_revision = "0075"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "worker_pool_autoscaler_policies",
        sa.Column("capacity_lease_state", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("worker_pool_autoscaler_policies", "capacity_lease_state")
