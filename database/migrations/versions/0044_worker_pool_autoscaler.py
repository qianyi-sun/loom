"""worker pool autoscaler policies and drain state

Revision ID: 0044
Revises: 0043
Create Date: 2026-06-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0044"
down_revision = "0043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workers",
        sa.Column(
            "drain_state",
            sa.Text(),
            server_default=sa.text("'active'"),
            nullable=False,
        ),
    )
    op.add_column(
        "workers",
        sa.Column(
            "drain_requested_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.add_column("workers", sa.Column("drain_reason", sa.Text(), nullable=True))
    op.add_column("workers", sa.Column("drain_owner", sa.Text(), nullable=True))
    op.create_check_constraint(
        "workers_drain_state_check",
        "workers",
        "drain_state IN ('active', 'draining', 'drained')",
    )
    op.create_index("idx_workers_drain_state", "workers", ["drain_state"])

    op.add_column(
        "gb10_worker_pool_desired_states",
        sa.Column("target_slots", sa.Integer(), nullable=True),
    )
    op.add_column(
        "gb10_worker_pool_desired_states",
        sa.Column(
            "host_intents",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )
    op.add_column(
        "gb10_worker_node_statuses",
        sa.Column("desired_intent", sa.Text(), nullable=True),
    )
    op.add_column(
        "gb10_worker_node_statuses",
        sa.Column("current_intent", sa.Text(), nullable=True),
    )
    op.drop_constraint(
        "gb10_worker_node_statuses_apply_state_check",
        "gb10_worker_node_statuses",
        type_="check",
    )
    op.create_check_constraint(
        "gb10_worker_node_statuses_apply_state_check",
        "gb10_worker_node_statuses",
        (
            "apply_state IN ('unknown', 'idle', 'applying', 'draining', "
            "'stopped', 'applied', 'blocked', 'failed', 'rolled_back')"
        ),
    )

    op.create_table(
        "worker_pool_autoscaler_policies",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("pool_name", sa.Text(), nullable=False),
        sa.Column("actuator", sa.Text(), nullable=False),
        sa.Column(
            "enabled",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("min_slots", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_slots", sa.Integer(), nullable=False),
        sa.Column(
            "scale_up_threshold_slots",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column(
            "scale_down_idle_seconds",
            sa.Integer(),
            server_default=sa.text("600"),
            nullable=False,
        ),
        sa.Column(
            "scale_up_cooldown_seconds",
            sa.Integer(),
            server_default=sa.text("60"),
            nullable=False,
        ),
        sa.Column(
            "scale_down_cooldown_seconds",
            sa.Integer(),
            server_default=sa.text("300"),
            nullable=False,
        ),
        sa.Column(
            "drain_timeout_seconds",
            sa.Integer(),
            server_default=sa.text("600"),
            nullable=False,
        ),
        sa.Column(
            "force",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("disabled_reason", sa.Text(), nullable=True),
        sa.Column(
            "actuator_config",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("idle_since_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_decision", sa.Text(), nullable=True),
        sa.Column("last_decision_reason", sa.Text(), nullable=True),
        sa.Column("last_desired_slots", sa.Integer(), nullable=True),
        sa.Column("last_actual_slots", sa.Integer(), nullable=True),
        sa.Column("last_pending_slots", sa.Integer(), nullable=True),
        sa.Column("last_draining_slots", sa.Integer(), nullable=True),
        sa.Column("last_occupied_slots", sa.Integer(), nullable=True),
        sa.Column("last_queued_slots", sa.Integer(), nullable=True),
        sa.Column("last_blocked_reason", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_scale_up_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "last_scale_down_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column("last_decision_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
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
            name="worker_pool_autoscaler_policies_environment_nonempty_check",
        ),
        sa.CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="worker_pool_autoscaler_policies_pool_name_nonempty_check",
        ),
        sa.CheckConstraint(
            "actuator IN ('slurm', 'gb10')",
            name="worker_pool_autoscaler_policies_actuator_check",
        ),
        sa.CheckConstraint(
            "min_slots >= 0",
            name="worker_pool_autoscaler_policies_min_slots_nonnegative_check",
        ),
        sa.CheckConstraint(
            "max_slots >= min_slots",
            name="worker_pool_autoscaler_policies_max_slots_check",
        ),
        sa.CheckConstraint(
            "scale_up_threshold_slots >= 0",
            name="worker_pool_autoscaler_policies_scale_up_threshold_check",
        ),
        sa.CheckConstraint(
            "scale_down_idle_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_down_idle_check",
        ),
        sa.CheckConstraint(
            "scale_up_cooldown_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_up_cooldown_check",
        ),
        sa.CheckConstraint(
            "scale_down_cooldown_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_down_cooldown_check",
        ),
        sa.CheckConstraint(
            "drain_timeout_seconds > 0",
            name="worker_pool_autoscaler_policies_drain_timeout_positive_check",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "environment",
            "pool_name",
            name="worker_pool_autoscaler_policies_environment_pool_uidx",
        ),
    )
    op.create_index(
        "worker_pool_autoscaler_policies_pool_idx",
        "worker_pool_autoscaler_policies",
        ["environment", "pool_name"],
    )


def downgrade() -> None:
    op.drop_index(
        "worker_pool_autoscaler_policies_pool_idx",
        table_name="worker_pool_autoscaler_policies",
    )
    op.drop_table("worker_pool_autoscaler_policies")

    op.drop_constraint(
        "gb10_worker_node_statuses_apply_state_check",
        "gb10_worker_node_statuses",
        type_="check",
    )
    op.create_check_constraint(
        "gb10_worker_node_statuses_apply_state_check",
        "gb10_worker_node_statuses",
        (
            "apply_state IN ('unknown', 'idle', 'applying', 'draining', "
            "'applied', 'blocked', 'failed', 'rolled_back')"
        ),
    )
    op.drop_column("gb10_worker_node_statuses", "current_intent")
    op.drop_column("gb10_worker_node_statuses", "desired_intent")
    op.drop_column("gb10_worker_pool_desired_states", "host_intents")
    op.drop_column("gb10_worker_pool_desired_states", "target_slots")

    op.drop_index("idx_workers_drain_state", table_name="workers")
    op.drop_constraint("workers_drain_state_check", "workers", type_="check")
    op.drop_column("workers", "drain_owner")
    op.drop_column("workers", "drain_reason")
    op.drop_column("workers", "drain_requested_at")
    op.drop_column("workers", "drain_state")
