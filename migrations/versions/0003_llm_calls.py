"""llm_calls table

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-07
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "llm_calls",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("trial_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("step_id", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("dialect", sa.Text(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column(
            "provider_extras",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("cost_usd", sa.Numeric(12, 6), nullable=False),
        sa.Column("rate_card_hash", sa.Text(), nullable=False),
        sa.Column(
            "captured_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "llm_calls_step_idx",
        "llm_calls",
        ["trial_id", "step_id", "captured_at"],
    )


def downgrade() -> None:
    op.drop_index("llm_calls_step_idx", table_name="llm_calls")
    op.drop_table("llm_calls")
