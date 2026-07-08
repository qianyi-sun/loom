"""Family runs: batches.family_run_spec + trials.family_key + batch_family_state (#672).

Revision ID: 0061
Revises: 0060
Create Date: 2026-07-08

Introduces per-family progression state so the scheduler can serialise a
family of related trials and evolve a shared scratch space between them.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0061"
down_revision = "0060"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "batches",
        sa.Column(
            "family_run_spec",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "trials",
        sa.Column("family_key", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_trials_family_key",
        "trials",
        ["batch_id", "family_key"],
        postgresql_where=sa.text("family_key IS NOT NULL"),
    )
    op.create_table(
        "batch_family_state",
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("family_key", sa.Text(), nullable=False),
        sa.Column(
            "task_sequence",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "current_index",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("state_uri", sa.Text(), nullable=True),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["batches.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("batch_id", "family_key"),
    )
    op.create_index(
        "idx_batch_family_state_adapting",
        "batch_family_state",
        ["next_attempt_at"],
        postgresql_where=sa.text("state = 'adapting'"),
    )


def downgrade() -> None:
    op.drop_index("idx_batch_family_state_adapting", table_name="batch_family_state")
    op.drop_table("batch_family_state")
    op.drop_index("idx_trials_family_key", table_name="trials")
    op.drop_column("trials", "family_key")
    op.drop_column("batches", "family_run_spec")
