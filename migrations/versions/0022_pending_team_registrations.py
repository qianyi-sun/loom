"""pending_team_registrations for admin-approved onboarding

Revision ID: 0022
Revises: 0021
Create Date: 2026-06-16

Adds the default-closed team registration queue from issue #10. Public
registration creates a pending row; an admin approval later creates the Team,
TeamQuota, and first team Token row.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pending_team_registrations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("contact_email", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "requested_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "reviewed_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column("reviewed_by_actor", sa.Text(), nullable=True),
        sa.Column(
            "approved_team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id"),
            nullable=True,
        ),
        sa.Column("source_ip_hash", sa.Text(), nullable=True),
        sa.Column("user_agent_hash", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="pending_team_registrations_status_check",
        ),
    )
    op.create_index(
        "pending_team_registrations_active_name_uidx",
        "pending_team_registrations",
        [sa.text("lower(name)")],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'approved')"),
    )


def downgrade() -> None:
    op.drop_index(
        "pending_team_registrations_active_name_uidx",
        table_name="pending_team_registrations",
    )
    op.drop_table("pending_team_registrations")
