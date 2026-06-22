"""user sessions, memberships, and login challenges (#326)

Revision ID: 0030
Revises: 0029
Create Date: 2026-06-22

Adds the durable browser identity foundation for invite-only public Loom.
Existing bearer-token auth remains in `tokens`; these tables model browser
users, team roles, HttpOnly cookie sessions, and one-time login challenges.
Raw session/challenge/CSRF secrets are never stored, only SHA-256 hashes.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0030"
down_revision = "0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column(
            "is_platform_admin",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "last_login_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "users_email_lower_uidx",
        "users",
        [sa.text("lower(email)")],
        unique=True,
    )

    op.create_table(
        "team_memberships",
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="team_memberships_role_check",
        ),
    )
    op.create_index(
        "team_memberships_user_id_idx",
        "team_memberships",
        ["user_id"],
    )

    op.create_table(
        "user_sessions",
        sa.Column("session_hash", sa.LargeBinary(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "current_team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("csrf_hash", sa.LargeBinary(), nullable=False),
        sa.Column(
            "issued_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "expires_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "revoked_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_seen_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "user_sessions_user_id_idx",
        "user_sessions",
        ["user_id"],
    )
    op.create_index(
        "user_sessions_active_idx",
        "user_sessions",
        ["expires_at"],
        postgresql_where=sa.text("revoked_at IS NULL"),
    )

    op.create_table(
        "login_challenges",
        sa.Column("challenge_hash", sa.LargeBinary(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "issued_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "expires_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "consumed_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column("source_ip_hash", sa.Text(), nullable=True),
        sa.Column("user_agent_hash", sa.Text(), nullable=True),
    )
    op.create_index(
        "login_challenges_user_id_idx",
        "login_challenges",
        ["user_id"],
    )
    op.create_index(
        "login_challenges_active_idx",
        "login_challenges",
        ["expires_at"],
        postgresql_where=sa.text("consumed_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("login_challenges_active_idx", table_name="login_challenges")
    op.drop_index("login_challenges_user_id_idx", table_name="login_challenges")
    op.drop_table("login_challenges")

    op.drop_index("user_sessions_active_idx", table_name="user_sessions")
    op.drop_index("user_sessions_user_id_idx", table_name="user_sessions")
    op.drop_table("user_sessions")

    op.drop_index("team_memberships_user_id_idx", table_name="team_memberships")
    op.drop_table("team_memberships")

    op.drop_index("users_email_lower_uidx", table_name="users")
    op.drop_table("users")
