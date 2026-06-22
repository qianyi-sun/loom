"""public beta team emergency controls (#334)

Revision ID: 0034
Revises: 0033
Create Date: 2026-06-22

Adds operator-owned team disable and submission-pause state. These controls
are incident levers only; quota and rate-limit enforcement are intentionally
out of scope for the public-beta ops slice.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0034"
down_revision = "0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teams",
        sa.Column("disabled_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column("teams", sa.Column("disabled_reason", sa.Text(), nullable=True))
    op.add_column(
        "teams",
        sa.Column(
            "submissions_paused_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "teams",
        sa.Column("submissions_paused_reason", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("teams", "submissions_paused_reason")
    op.drop_column("teams", "submissions_paused_at")
    op.drop_column("teams", "disabled_reason")
    op.drop_column("teams", "disabled_at")
