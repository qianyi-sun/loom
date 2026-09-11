"""Retain native task-image Job identity and resource observations per attempt.

Revision ID: 0135
Revises: 0134
"""
from alembic import op

revision = "0135"
down_revision = "0134"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE task_image_materialization_attempts ADD COLUMN native_build jsonb")


def downgrade() -> None:
    op.execute("ALTER TABLE task_image_materialization_attempts DROP COLUMN native_build")
