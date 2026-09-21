"""Retain scheduling diagnostics without changing execution authority.

Revision ID: 0153
Revises: 0152
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0153"
down_revision = "0152"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE public.trials, public.task_image_capacity_waits IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.add_column("trials", sa.Column("scheduling_observation", postgresql.JSONB(), nullable=True))
    op.add_column("task_image_capacity_waits", sa.Column("reason", sa.Text(), nullable=True))


def downgrade() -> None:
    op.execute("LOCK TABLE public.trials, public.task_image_capacity_waits IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.drop_column("task_image_capacity_waits", "reason")
    op.drop_column("trials", "scheduling_observation")
