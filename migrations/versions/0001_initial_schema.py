"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-06-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "teams",
        sa.Column("id", PgUUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String, nullable=False, unique=True),
        sa.Column("created_at", TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_table(
        "team_quotas",
        sa.Column("team_id", PgUUID(as_uuid=True), sa.ForeignKey("teams.id"), primary_key=True),
        sa.Column("fair_share_weight", sa.Float, nullable=False, server_default="1.0"),
        sa.Column("max_attempts", sa.Integer, nullable=False, server_default="3"),
        sa.Column("in_flight_count", sa.Integer, nullable=False, server_default="0"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("checksum", sa.String, nullable=False),
        sa.Column("config", JSONB, nullable=False),
        sa.Column("source", sa.String, nullable=True),
        sa.Column("registered_at", TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_table(
        "agents",
        sa.Column("name", sa.String, primary_key=True),
        sa.Column("version", sa.String, primary_key=True),
        sa.Column("mode", sa.String, nullable=False),
        sa.Column("spec", JSONB, nullable=False),
    )
    op.create_table(
        "workers",
        sa.Column("id", PgUUID(as_uuid=True), primary_key=True),
        sa.Column("hostname", sa.String, nullable=False),
        sa.Column("version", sa.String, nullable=False),
        sa.Column("capabilities", JSONB, nullable=False),
        sa.Column("registered_at", TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_seen_at", TIMESTAMP(timezone=True), nullable=False),
        sa.Column("status", sa.String, nullable=False),
    )
    op.create_table(
        "trials",
        sa.Column("id", PgUUID(as_uuid=True), primary_key=True),
        sa.Column("team_id", PgUUID(as_uuid=True), sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("task_id", sa.String, sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("config", JSONB, nullable=False),
        sa.Column("requires_caps", JSONB, nullable=False),
        sa.Column("state", sa.String, nullable=False),
        sa.Column("failure_reason", sa.String, nullable=True),
        sa.Column("submit_priority", sa.Integer, nullable=False, server_default="100"),
        sa.Column("submitted_at", TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("claimed_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("started_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("cancellation_requested_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("cancellation_observed_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("worker_id", PgUUID(as_uuid=True), sa.ForeignKey("workers.id"), nullable=True),
        sa.Column("attempt_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("next_attempt_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("result", JSONB, nullable=True),
        sa.Column("trajectory_index", JSONB, nullable=True),
    )
    op.create_index(
        "idx_trials_state_queued",
        "trials", ["state"],
        postgresql_where=sa.text("state = 'queued'"),
    )
    op.create_index(
        "idx_trials_team_inflight",
        "trials", ["team_id"],
        postgresql_where=sa.text("state IN ('claimed','running')"),
    )
    op.create_index(
        "idx_trials_worker",
        "trials", ["worker_id"],
        postgresql_where=sa.text("worker_id IS NOT NULL"),
    )
    op.create_table(
        "tokens",
        sa.Column("token_hash", sa.LargeBinary, primary_key=True),
        sa.Column("type", sa.String, nullable=False),
        sa.Column("scopes", sa.ARRAY(sa.String), nullable=False),
        sa.Column("team_id", PgUUID(as_uuid=True), sa.ForeignKey("teams.id"), nullable=True),
        sa.Column("issued_at", TIMESTAMP(timezone=True), nullable=False),
        sa.Column("expires_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("revoked_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_used_at", TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_table(
        "rate_cards",
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("captured_at", TIMESTAMP(timezone=True), nullable=False),
        sa.Column("table", JSONB, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("rate_cards")
    op.drop_table("tokens")
    op.drop_index("idx_trials_worker", table_name="trials")
    op.drop_index("idx_trials_team_inflight", table_name="trials")
    op.drop_index("idx_trials_state_queued", table_name="trials")
    op.drop_table("trials")
    op.drop_table("workers")
    op.drop_table("agents")
    op.drop_table("tasks")
    op.drop_table("team_quotas")
    op.drop_table("teams")
