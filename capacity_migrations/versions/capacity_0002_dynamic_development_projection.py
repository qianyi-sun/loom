"""dynamic development projection

Revision ID: capacity_0002
Revises: capacity_0001
Create Date: 2026-08-11 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "capacity_0002"
down_revision: str | Sequence[str] | None = "capacity_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "capacity_demand_reporters",
        sa.Column("token_sha256", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        "capacity_demand_reporter_token_digest_check",
        "capacity_demand_reporters",
        "token_sha256 IS NULL OR token_sha256 ~ '^[0-9a-f]{64}$'",
    )
    op.create_unique_constraint(
        "capacity_demand_reporter_token_key",
        "capacity_demand_reporters",
        ["token_sha256"],
    )
    op.create_table(
        "capacity_development_projections",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("operation_id", sa.UUID(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("request_digest", sa.Text(), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("subject_id", sa.UUID(), nullable=False),
        sa.Column("subject_incarnation", sa.UUID(), nullable=False),
        sa.Column("configuration_generation", sa.BigInteger(), nullable=False),
        sa.Column("configuration_epoch", sa.BigInteger(), nullable=False),
        sa.Column("result_digest", sa.Text(), nullable=False),
        sa.Column("result_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "request_digest ~ '^[0-9a-f]{64}$' AND result_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_development_projection_digest_check",
        ),
        sa.CheckConstraint(
            "configuration_epoch > 0 AND configuration_generation > 0",
            name="capacity_development_projection_generation_check",
        ),
        sa.ForeignKeyConstraint(
            ["configuration_epoch"],
            ["capacity_configuration_epochs.configuration_epoch"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "operation_id",
            name="capacity_development_projection_operation_key",
        ),
        sa.UniqueConstraint(
            "idempotency_key",
            name="capacity_development_projection_idempotency_key",
        ),
    )


def downgrade() -> None:
    op.drop_table("capacity_development_projections")
    op.drop_constraint(
        "capacity_demand_reporter_token_key",
        "capacity_demand_reporters",
        type_="unique",
    )
    op.drop_constraint(
        "capacity_demand_reporter_token_digest_check",
        "capacity_demand_reporters",
        type_="check",
    )
    op.drop_column("capacity_demand_reporters", "token_sha256")
