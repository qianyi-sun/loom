"""username password accounts and submitter attribution

Revision ID: 0045
Revises: 0044
Create Date: 2026-06-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0045"
down_revision = "0044"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("users", "email", existing_type=sa.Text(), nullable=True)
    op.add_column("users", sa.Column("username", sa.Text(), nullable=True))
    op.add_column("users", sa.Column("username_normalized", sa.Text(), nullable=True))
    op.add_column("users", sa.Column("password_hash", sa.Text(), nullable=True))
    op.add_column(
        "users",
        sa.Column("password_set_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column(
        "users",
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'pending_setup'"),
            nullable=True,
        ),
    )
    op.add_column(
        "users",
        sa.Column("disabled_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    )

    op.execute(
        """
        WITH candidates AS (
            SELECT
                id,
                COALESCE(
                    NULLIF(
                        lower(
                            regexp_replace(
                                left(
                                    trim(
                                        COALESCE(
                                            NULLIF(display_name, ''),
                                            NULLIF(split_part(COALESCE(email, ''), '@', 1), ''),
                                            'user_' || replace(id::text, '-', '')
                                        )
                                    ),
                                    55
                                ),
                                '[^A-Za-z0-9_.-]+',
                                '_',
                                'g'
                            )
                        ),
                        ''
                    ),
                    'user_' || replace(id::text, '-', '')
                ) AS username_base
            FROM users
        ),
        ranked AS (
            SELECT
                id,
                username_base,
                row_number() OVER (
                    PARTITION BY username_base
                    ORDER BY id
                ) AS rn
            FROM candidates
        ),
        final AS (
            SELECT
                id,
                CASE
                    WHEN rn = 1 THEN username_base
                    ELSE left(username_base, 55) || '_' || rn::text
                END AS username_value
            FROM ranked
        )
        UPDATE users
           SET username = final.username_value,
               username_normalized = lower(final.username_value),
               status = 'active'
          FROM final
         WHERE users.id = final.id
        """,
    )
    op.alter_column("users", "username", existing_type=sa.Text(), nullable=False)
    op.alter_column("users", "username_normalized", existing_type=sa.Text(), nullable=False)
    op.alter_column("users", "status", existing_type=sa.Text(), nullable=False)
    op.create_unique_constraint(
        "users_username_normalized_uidx",
        "users",
        ["username_normalized"],
    )
    op.create_check_constraint(
        "users_status_check",
        "users",
        "status IN ('pending_setup', 'active', 'disabled')",
    )

    op.create_table(
        "user_registration_requests",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("username_normalized", sa.Text(), nullable=False),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.Text(), nullable=False, server_default=sa.text("'member'")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column(
            "requested_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("reviewed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "reviewed_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("reviewed_by_actor", sa.Text(), nullable=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("setup_token_prefix", sa.Text(), nullable=True),
        sa.Column("rejection_reason", sa.Text(), nullable=True),
        sa.Column("source_ip_hash", sa.Text(), nullable=True),
        sa.Column("user_agent_hash", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="user_registration_requests_role_check",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="user_registration_requests_status_check",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "user_registration_requests_active_username_uidx",
        "user_registration_requests",
        ["username_normalized"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'approved')"),
    )
    op.create_index(
        "user_registration_requests_team_status_idx",
        "user_registration_requests",
        ["team_id", "status"],
    )

    op.create_table(
        "password_reset_requests",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("username_normalized", sa.Text(), nullable=False),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column(
            "requested_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("reviewed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "reviewed_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("reviewed_by_actor", sa.Text(), nullable=True),
        sa.Column("reset_token_prefix", sa.Text(), nullable=True),
        sa.Column("rejection_reason", sa.Text(), nullable=True),
        sa.Column("source_ip_hash", sa.Text(), nullable=True),
        sa.Column("user_agent_hash", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="password_reset_requests_status_check",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "password_reset_requests_active_username_uidx",
        "password_reset_requests",
        ["username_normalized"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'approved')"),
    )
    op.create_index(
        "password_reset_requests_user_status_idx",
        "password_reset_requests",
        ["user_id", "status"],
    )

    op.create_table(
        "account_action_tokens",
        sa.Column("token_hash", sa.LargeBinary(), nullable=False),
        sa.Column("token_prefix", sa.Text(), nullable=False),
        sa.Column("purpose", sa.Text(), nullable=False),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "registration_request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("user_registration_requests.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "password_reset_request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("password_reset_requests.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("issued_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("consumed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("revoked_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "purpose IN ('setup_password', 'reset_password')",
            name="account_action_tokens_purpose_check",
        ),
        sa.PrimaryKeyConstraint("token_hash"),
    )
    op.create_index(
        "account_action_tokens_user_purpose_idx",
        "account_action_tokens",
        ["user_id", "purpose"],
    )
    op.create_index(
        "account_action_tokens_prefix_idx",
        "account_action_tokens",
        ["token_prefix"],
    )

    op.add_column(
        "batches",
        sa.Column(
            "submitted_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "trials",
        sa.Column(
            "submitted_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index("batches_submitted_by_user_id_idx", "batches", ["submitted_by_user_id"])
    op.create_index("trials_submitted_by_user_id_idx", "trials", ["submitted_by_user_id"])

    op.execute(
        """
        DO $$
        DECLARE
            admin_team_id uuid;
        BEGIN
            SELECT id
              INTO admin_team_id
              FROM teams
             WHERE lower(name) = 'admin'
             ORDER BY created_at ASC, id ASC
             LIMIT 1;

            IF admin_team_id IS NULL THEN
                INSERT INTO teams (id, name)
                VALUES (gen_random_uuid(), 'admin')
                RETURNING id INTO admin_team_id;
            END IF;

            INSERT INTO team_quotas (team_id)
            VALUES (admin_team_id)
            ON CONFLICT (team_id) DO NOTHING;

            INSERT INTO users (
                id,
                email,
                username,
                username_normalized,
                display_name,
                is_platform_admin,
                created_at,
                status
            )
            VALUES
                (gen_random_uuid(), NULL, 'Qianyi', 'qianyi', 'Qianyi', true, now(), 'pending_setup'),
                (gen_random_uuid(), NULL, 'Hongjian', 'hongjian', 'Hongjian', true, now(), 'pending_setup')
            ON CONFLICT (username_normalized) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    is_platform_admin = true;

            INSERT INTO team_memberships (team_id, user_id, role)
            SELECT admin_team_id, id, 'owner'
              FROM users
             WHERE username_normalized IN ('qianyi', 'hongjian')
            ON CONFLICT (team_id, user_id) DO UPDATE
                SET role = EXCLUDED.role;
        END $$;
        """,
    )


def downgrade() -> None:
    op.drop_index("trials_submitted_by_user_id_idx", table_name="trials")
    op.drop_index("batches_submitted_by_user_id_idx", table_name="batches")
    op.drop_column("trials", "submitted_by_user_id")
    op.drop_column("batches", "submitted_by_user_id")

    op.drop_index("account_action_tokens_prefix_idx", table_name="account_action_tokens")
    op.drop_index("account_action_tokens_user_purpose_idx", table_name="account_action_tokens")
    op.drop_table("account_action_tokens")

    op.drop_index("password_reset_requests_user_status_idx", table_name="password_reset_requests")
    op.drop_index(
        "password_reset_requests_active_username_uidx",
        table_name="password_reset_requests",
    )
    op.drop_table("password_reset_requests")

    op.drop_index(
        "user_registration_requests_team_status_idx",
        table_name="user_registration_requests",
    )
    op.drop_index(
        "user_registration_requests_active_username_uidx",
        table_name="user_registration_requests",
    )
    op.drop_table("user_registration_requests")

    op.drop_constraint("users_status_check", "users", type_="check")
    op.drop_constraint("users_username_normalized_uidx", "users", type_="unique")
    op.drop_column("users", "disabled_at")
    op.drop_column("users", "status")
    op.drop_column("users", "password_set_at")
    op.drop_column("users", "password_hash")
    op.drop_column("users", "username_normalized")
    op.drop_column("users", "username")
    op.alter_column("users", "email", existing_type=sa.Text(), nullable=False)
