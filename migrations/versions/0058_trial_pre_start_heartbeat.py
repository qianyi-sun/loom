"""Track worker pre-start progress for claimed trials (#442).

Workers can spend many minutes in setup/cache-build work before a trial reaches
started_at. This timestamp lets the crash detector distinguish a live local
pre-start queue from an abandoned claimed row.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0058"
down_revision = "0057"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "trials",
        sa.Column("pre_start_heartbeat_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index(
        "trials_pre_start_heartbeat_idx",
        "trials",
        ["pre_start_heartbeat_at"],
        postgresql_where=sa.text("state = 'claimed' AND started_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("trials_pre_start_heartbeat_idx", table_name="trials")
    op.drop_column("trials", "pre_start_heartbeat_at")
