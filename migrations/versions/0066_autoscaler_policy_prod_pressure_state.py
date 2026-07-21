"""Add prod-pressure drain intent to worker-pool autoscaler policies.

A single nullable JSONB column holds the prod-pressure drain intent for each
(environment, pool_name). The CP request handler is the sole writer; the
external Slurm actor and the scheduler claim path are readers. NULL means no
active drain, so existing rows migrate untouched. See #892.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0066"
down_revision = "0065"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "worker_pool_autoscaler_policies",
        sa.Column("prod_pressure_state", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("worker_pool_autoscaler_policies", "prod_pressure_state")
