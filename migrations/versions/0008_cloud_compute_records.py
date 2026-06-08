"""cloud_compute_records table — generic per-sandbox lifetime + cost

Revision ID: 0008
Revises: 0007
Create Date: 2026-06-08

Per amendment A26.1, this table is multi-provider from day one. Plan 26
inserts rows with cloud_provider='daytona'; Plan 27 (Modal driver) will
insert with cloud_provider='modal'. The /api/v1/usage rollup filters by
cloud_provider to surface per-provider compute-seconds and cost.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "cloud_compute_records",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True), primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("trial_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cloud_provider", sa.Text(), nullable=False),
        sa.Column("sandbox_id", sa.Text(), nullable=False),
        sa.Column("image", sa.Text(), nullable=False),
        sa.Column(
            "started_at", sa.TIMESTAMP(timezone=True), nullable=False,
        ),
        sa.Column(
            "stopped_at", sa.TIMESTAMP(timezone=True), nullable=False,
        ),
        sa.Column(
            "compute_seconds", sa.Numeric(14, 3), nullable=False,
        ),
        sa.Column("cost_usd", sa.Numeric(12, 6), nullable=False),
        sa.Column(
            "captured_at", sa.TIMESTAMP(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "cloud_compute_records_trial_idx",
        "cloud_compute_records", ["trial_id"],
    )
    op.create_index(
        "cloud_compute_records_team_provider_time_idx",
        "cloud_compute_records",
        ["team_id", "cloud_provider", "stopped_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "cloud_compute_records_team_provider_time_idx",
        table_name="cloud_compute_records",
    )
    op.drop_index(
        "cloud_compute_records_trial_idx",
        table_name="cloud_compute_records",
    )
    op.drop_table("cloud_compute_records")
