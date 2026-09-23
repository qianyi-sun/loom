"""batches: link failed-case reruns (#298)

Revision ID: 0028
Revises: 0027
Create Date: 2026-06-19

Failed-case reruns are ordinary batches with a durable parent pointer and a
JSONB list of exact trial coordinates to re-submit. Keeping reruns as batches
preserves history while allowing the service/API/UI to compute an effective
rollup for the original batch.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "batches",
        sa.Column(
            "rerun_of_batch_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("batches.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "batches",
        sa.Column(
            "rerun_targets",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.create_index(
        "batches_rerun_of_batch_idx",
        "batches",
        ["rerun_of_batch_id"],
    )


def downgrade() -> None:
    op.drop_index("batches_rerun_of_batch_idx", table_name="batches")
    op.drop_column("batches", "rerun_targets")
    op.drop_column("batches", "rerun_of_batch_id")
