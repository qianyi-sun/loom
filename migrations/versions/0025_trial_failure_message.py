"""trial.failure_message: short user-facing diagnostic

Revision ID: 0025
Revises: 0024
Create Date: 2026-06-17

Adds a nullable Text column to the trials table to store a short,
redacted, user-facing diagnostic message set by classify_failure()
when a provider/gateway HTTP error is the root cause.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("trials", sa.Column("failure_message", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("trials", "failure_message")
