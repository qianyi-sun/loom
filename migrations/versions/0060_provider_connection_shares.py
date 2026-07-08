"""Add provider connection sharing and usage attribution.

Revision ID: 0060
Revises: 0059
Create Date: 2026-07-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0060"
down_revision = "0059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "provider_connection_shares",
        sa.Column(
            "provider_connection_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("target_team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_by_actor", sa.Text(), nullable=False),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["provider_connection_id"],
            ["provider_connections.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["target_team_id"],
            ["teams.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "provider_connection_id",
            "target_team_id",
        ),
    )
    op.create_index(
        "provider_connection_shares_target_team_idx",
        "provider_connection_shares",
        ["target_team_id"],
        unique=False,
    )

    op.add_column(
        "batches",
        sa.Column(
            "usage_attributed_user_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.add_column(
        "batches",
        sa.Column("usage_attributed_actor", sa.Text(), nullable=True),
    )
    op.create_foreign_key(
        "batches_usage_attributed_user_id_fkey",
        "batches",
        "users",
        ["usage_attributed_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "batches_usage_attributed_user_id_idx",
        "batches",
        ["usage_attributed_user_id"],
        unique=False,
    )

    op.add_column(
        "trials",
        sa.Column(
            "usage_attributed_user_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.add_column(
        "trials",
        sa.Column("usage_attributed_actor", sa.Text(), nullable=True),
    )
    op.create_foreign_key(
        "trials_usage_attributed_user_id_fkey",
        "trials",
        "users",
        ["usage_attributed_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "trials_usage_attributed_user_id_idx",
        "trials",
        ["usage_attributed_user_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("trials_usage_attributed_user_id_idx", table_name="trials")
    op.drop_constraint(
        "trials_usage_attributed_user_id_fkey",
        "trials",
        type_="foreignkey",
    )
    op.drop_column("trials", "usage_attributed_actor")
    op.drop_column("trials", "usage_attributed_user_id")

    op.drop_index("batches_usage_attributed_user_id_idx", table_name="batches")
    op.drop_constraint(
        "batches_usage_attributed_user_id_fkey",
        "batches",
        type_="foreignkey",
    )
    op.drop_column("batches", "usage_attributed_actor")
    op.drop_column("batches", "usage_attributed_user_id")

    op.drop_index(
        "provider_connection_shares_target_team_idx",
        table_name="provider_connection_shares",
    )
    op.drop_table("provider_connection_shares")
