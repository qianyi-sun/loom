"""gb10 worker lifecycle desired state and node status

Revision ID: 0043
Revises: 0042
Create Date: 2026-06-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0043"
down_revision = "0042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gb10_worker_pool_desired_states",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("pool_name", sa.Text(), nullable=False),
        sa.Column("image_tag", sa.Text(), nullable=False),
        sa.Column("max_concurrent", sa.Integer(), nullable=False),
        sa.Column("env_config_version", sa.Text(), nullable=False),
        sa.Column(
            "rollout_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "env",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("force", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("previous_image_tag", sa.Text(), nullable=True),
        sa.Column("previous_max_concurrent", sa.Integer(), nullable=True),
        sa.Column("previous_env_config_version", sa.Text(), nullable=True),
        sa.Column(
            "previous_env",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(trim(environment)) > 0",
            name="gb10_worker_pool_desired_states_environment_nonempty_check",
        ),
        sa.CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="gb10_worker_pool_desired_states_pool_name_nonempty_check",
        ),
        sa.CheckConstraint(
            "length(trim(image_tag)) > 0",
            name="gb10_worker_pool_desired_states_image_tag_nonempty_check",
        ),
        sa.CheckConstraint(
            "max_concurrent > 0",
            name="gb10_worker_pool_desired_states_max_concurrent_positive_check",
        ),
        sa.CheckConstraint(
            "length(trim(env_config_version)) > 0",
            name="gb10_worker_pool_desired_states_env_version_nonempty_check",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "environment",
            "pool_name",
            name="gb10_worker_pool_desired_states_environment_pool_uidx",
        ),
    )
    op.create_index(
        "gb10_worker_pool_desired_states_pool_idx",
        "gb10_worker_pool_desired_states",
        ["environment", "pool_name"],
    )

    op.create_table(
        "gb10_worker_node_statuses",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("pool_name", sa.Text(), nullable=False),
        sa.Column("hostname", sa.Text(), nullable=False),
        sa.Column("worker_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("current_image_tag", sa.Text(), nullable=True),
        sa.Column("current_max_concurrent", sa.Integer(), nullable=True),
        sa.Column("current_env_config_version", sa.Text(), nullable=True),
        sa.Column("desired_image_tag", sa.Text(), nullable=True),
        sa.Column("desired_max_concurrent", sa.Integer(), nullable=True),
        sa.Column("desired_env_config_version", sa.Text(), nullable=True),
        sa.Column(
            "apply_state",
            sa.Text(),
            server_default=sa.text("'unknown'"),
            nullable=False,
        ),
        sa.Column("last_apply_result", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("agent_version", sa.Text(), nullable=True),
        sa.Column("compose_project_dir", sa.Text(), nullable=True),
        sa.Column(
            "last_heartbeat_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_apply_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(trim(environment)) > 0",
            name="gb10_worker_node_statuses_environment_nonempty_check",
        ),
        sa.CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="gb10_worker_node_statuses_pool_name_nonempty_check",
        ),
        sa.CheckConstraint(
            "length(trim(hostname)) > 0",
            name="gb10_worker_node_statuses_hostname_nonempty_check",
        ),
        sa.CheckConstraint(
            "current_max_concurrent IS NULL OR current_max_concurrent > 0",
            name="gb10_worker_node_statuses_current_max_positive_check",
        ),
        sa.CheckConstraint(
            "desired_max_concurrent IS NULL OR desired_max_concurrent > 0",
            name="gb10_worker_node_statuses_desired_max_positive_check",
        ),
        sa.CheckConstraint(
            "apply_state IN ('unknown', 'idle', 'applying', 'draining', 'applied', 'blocked', 'failed', 'rolled_back')",
            name="gb10_worker_node_statuses_apply_state_check",
        ),
        sa.ForeignKeyConstraint(
            ["worker_id"],
            ["workers.id"],
            name="gb10_worker_node_statuses_worker_id_fkey",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "environment",
            "pool_name",
            "hostname",
            name="gb10_worker_node_statuses_environment_pool_host_uidx",
        ),
    )
    op.create_index(
        "gb10_worker_node_statuses_pool_state_idx",
        "gb10_worker_node_statuses",
        ["environment", "pool_name", "apply_state"],
    )


def downgrade() -> None:
    op.drop_index(
        "gb10_worker_node_statuses_pool_state_idx",
        table_name="gb10_worker_node_statuses",
    )
    op.drop_table("gb10_worker_node_statuses")
    op.drop_index(
        "gb10_worker_pool_desired_states_pool_idx",
        table_name="gb10_worker_pool_desired_states",
    )
    op.drop_table("gb10_worker_pool_desired_states")
