"""Add the durable shared-fleet development-instance registry.

Revision ID: 0076
Revises: 0075
Create Date: 2026-08-06
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0076"
down_revision = "0075"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dev_instances",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("owner_user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("min_slots", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_slots", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'provisioning'"),
            nullable=False,
        ),
        sa.Column("deployment_generation", sa.BigInteger(), nullable=False),
        sa.Column("candidate_sha", sa.String(length=40), nullable=False),
        sa.Column(
            "operation_epoch",
            sa.BigInteger(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "operation_step",
            sa.String(length=32),
            server_default=sa.text("'claimed'"),
            nullable=False,
        ),
        sa.Column("secret_ref", sa.Text(), nullable=True),
        sa.Column(
            "keep_data",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("failure_reason", sa.String(length=256), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("ready_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("deleted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "name ~ '^[a-z]([-a-z0-9]{0,18}[a-z0-9])?$'",
            name="dev_instances_name_check",
        ),
        sa.CheckConstraint(
            "status IN ('provisioning', 'ready', 'deleting', 'failed', 'deleted')",
            name="dev_instances_status_check",
        ),
        sa.CheckConstraint(
            "min_slots >= 0 AND max_slots >= min_slots AND max_slots <= 8",
            name="dev_instances_slots_check",
        ),
        sa.CheckConstraint(
            "deployment_generation > 0",
            name="dev_instances_deployment_generation_check",
        ),
        sa.CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{40}$'",
            name="dev_instances_candidate_sha_check",
        ),
        sa.CheckConstraint(
            "operation_epoch > 0",
            name="dev_instances_operation_epoch_check",
        ),
        sa.ForeignKeyConstraint(
            ["owner_team_id"],
            ["teams.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_user_id"],
            ["users.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("name"),
    )
    op.create_index(
        "dev_instances_owner_status_idx",
        "dev_instances",
        ["owner_user_id", "status"],
    )
    op.create_index(
        "dev_instances_team_status_idx",
        "dev_instances",
        ["owner_team_id", "status"],
    )


def downgrade() -> None:
    op.drop_index("dev_instances_team_status_idx", table_name="dev_instances")
    op.drop_index("dev_instances_owner_status_idx", table_name="dev_instances")
    op.drop_table("dev_instances")
