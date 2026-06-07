"""campaigns table + trials.campaign_id + trials.idempotency_key

Revision ID: 0007
Revises: 0006
Create Date: 2026-06-07

Plan 19: campaigns are first-class. Each campaign materializes a
task_filter into N trial submissions. `trials.campaign_id` is the
back-link the service runner uses to figure out which trials have
already been emitted for a campaign and to roll up state.
`trials.idempotency_key` (text, UNIQUE WHERE NOT NULL) lets the
runner re-submit safely — the CP's POST /trials does an ON CONFLICT
DO NOTHING on this column so a re-attempted submission returns the
existing trial_id rather than creating a duplicate.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "campaigns",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True), primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "team_id", postgresql.UUID(as_uuid=True), nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("task_filter", postgresql.JSONB, nullable=False),
        sa.Column("trial_config", postgresql.JSONB, nullable=False),
        sa.Column(
            "state", sa.Text(), nullable=False,
            server_default=sa.text("'submitted'"),
        ),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
        sa.Column(
            "finished_at", sa.TIMESTAMP(timezone=True), nullable=True,
        ),
        sa.Column("created_by_token_prefix", sa.Text(), nullable=False),
        sa.Column(
            "expected_trial_count", sa.Integer(),
            nullable=False, server_default="0",
        ),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.CheckConstraint(
            "state IN ('submitted','running','finished','cancelled')",
            name="campaigns_state_chk",
        ),
    )
    op.create_index("campaigns_team_idx", "campaigns", ["team_id"])
    op.create_index("campaigns_state_idx", "campaigns", ["state"])

    op.add_column(
        "trials",
        sa.Column(
            "campaign_id", postgresql.UUID(as_uuid=True), nullable=True,
        ),
    )
    op.create_foreign_key(
        "trials_campaign_fk", "trials", "campaigns",
        ["campaign_id"], ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "trials_campaign_idx", "trials", ["campaign_id"],
        postgresql_where=sa.text("campaign_id IS NOT NULL"),
    )

    op.add_column(
        "trials",
        sa.Column("idempotency_key", sa.Text(), nullable=True),
    )
    # Partial unique index so the same idempotency_key can never insert
    # two trial rows; null idempotency_key (hand-submitted trials) is
    # excluded from the constraint.
    op.create_index(
        "trials_idempotency_key_uidx", "trials", ["idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("trials_idempotency_key_uidx", table_name="trials")
    op.drop_column("trials", "idempotency_key")
    op.drop_index("trials_campaign_idx", table_name="trials")
    op.drop_constraint(
        "trials_campaign_fk", "trials", type_="foreignkey",
    )
    op.drop_column("trials", "campaign_id")
    op.drop_index("campaigns_state_idx", table_name="campaigns")
    op.drop_index("campaigns_team_idx", table_name="campaigns")
    op.drop_table("campaigns")
