"""admin_audit_events for admin mutation forensics

Revision ID: 0023
Revises: 0022
Create Date: 2026-06-16

Adds durable audit rows for issue #10 admin mutations. Mutating routes write
safe metadata in the same transaction as the domain change.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "admin_audit_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("target_id", sa.Text(), nullable=False),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.Column("source_ip_hash", sa.Text(), nullable=True),
        sa.Column("user_agent_hash", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("jsonb_build_object()"),
        ),
    )
    op.create_index(
        "admin_audit_events_created_at_id_idx",
        "admin_audit_events",
        [sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "admin_audit_events_target_idx",
        "admin_audit_events",
        ["target_type", "target_id"],
    )


def downgrade() -> None:
    op.drop_index("admin_audit_events_target_idx", table_name="admin_audit_events")
    op.drop_index("admin_audit_events_created_at_id_idx", table_name="admin_audit_events")
    op.drop_table("admin_audit_events")
