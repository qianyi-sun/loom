"""Add TaskSet quota columns to team_quotas (#242 sub-plan 7).

Adds nullable taskset_max_count (int) and taskset_max_storage_bytes (bigint)
to team_quotas for per-team TaskSet quota overrides.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0057"
down_revision = "0056"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "team_quotas",
        sa.Column("taskset_max_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "team_quotas",
        sa.Column("taskset_max_storage_bytes", sa.BigInteger(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("team_quotas", "taskset_max_storage_bytes")
    op.drop_column("team_quotas", "taskset_max_count")
