"""API token metadata and scoped CLI lifecycle (#328)

Revision ID: 0032
Revises: 0031
Create Date: 2026-06-22

Adds user-facing metadata for scoped API tokens. Existing DB-backed team and
worker tokens remain valid because verification is still hash-based; these
columns are nullable so historical/operator-seeded rows do not need synthetic
names or owners.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0032"
down_revision = "0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tokens", sa.Column("name", sa.Text(), nullable=True))
    op.add_column(
        "tokens",
        sa.Column(
            "created_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "tokens",
        sa.Column("created_by_actor", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tokens", "created_by_actor")
    op.drop_column("tokens", "created_by_user_id")
    op.drop_column("tokens", "name")
