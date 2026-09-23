"""Persist the owner of an idle Nebius rollout; idle means no row.

Revision ID: 0152
Revises: 0151
"""
import sqlalchemy as sa
from alembic import op

revision = "0152"
down_revision = "0151"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nebius_rollout_guard",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("owner", sa.Text(), nullable=False),
        sa.Column("candidate_sha", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("id = 1", name="nebius_rollout_guard_singleton_check"),
    )


def downgrade() -> None:
    op.drop_table("nebius_rollout_guard")
