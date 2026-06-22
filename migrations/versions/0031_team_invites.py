"""team invites and membership onboarding (#327)

Revision ID: 0031
Revises: 0030
Create Date: 2026-06-22

Adds hashed invite links for public beta onboarding. Raw invite codes are
returned only on create/resend responses; the database stores SHA-256 hashes
plus short prefixes for audit and operator reference.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0031"
down_revision = "0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "team_invites",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("allowed_domain", sa.Text(), nullable=True),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("code_hash", sa.LargeBinary(), nullable=False),
        sa.Column("code_prefix", sa.Text(), nullable=False),
        sa.Column("max_uses", sa.Integer(), nullable=True),
        sa.Column(
            "accepted_uses",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("created_by_actor", sa.Text(), nullable=False),
        sa.Column(
            "created_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "expires_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "last_sent_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "accepted_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "revoked_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column("revoked_reason", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="team_invites_role_check",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'accepted', 'revoked', 'expired')",
            name="team_invites_status_check",
        ),
        sa.CheckConstraint(
            "max_uses IS NULL OR max_uses > 0",
            name="team_invites_max_uses_positive_check",
        ),
        sa.CheckConstraint(
            "accepted_uses >= 0",
            name="team_invites_accepted_uses_nonnegative_check",
        ),
    )
    op.create_index(
        "team_invites_team_status_idx",
        "team_invites",
        ["team_id", "status"],
    )
    op.create_index(
        "team_invites_code_hash_uidx",
        "team_invites",
        ["code_hash"],
        unique=True,
    )
    op.create_index(
        "team_invites_email_lower_idx",
        "team_invites",
        [sa.text("lower(email)")],
    )


def downgrade() -> None:
    op.drop_index("team_invites_email_lower_idx", table_name="team_invites")
    op.drop_index("team_invites_code_hash_uidx", table_name="team_invites")
    op.drop_index("team_invites_team_status_idx", table_name="team_invites")
    op.drop_table("team_invites")
