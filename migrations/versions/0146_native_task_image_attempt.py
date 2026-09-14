"""Retain native task-image Job identity and resource observations per attempt.

Revision ID: 0146
Revises: 0145
"""
from alembic import op

revision = "0146"
down_revision = "0145"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Preserve dev's fail-fast authority-table migrations under live writers.
    op.execute("LOCK TABLE task_image_materialization_attempts IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("ALTER TABLE task_image_materialization_attempts ADD COLUMN native_build jsonb")


def downgrade() -> None:
    op.execute("LOCK TABLE task_image_materialization_attempts IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("ALTER TABLE task_image_materialization_attempts DROP COLUMN native_build")
