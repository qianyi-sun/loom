"""Persist batch budget estimates and diagnostics (#389).

Adds optional per-batch budget policy, pre-run cost estimate metadata, and
runner hard-stop diagnostics.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0056"
down_revision = "0055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "batches",
        sa.Column("budget_usd", sa.Numeric(12, 6), nullable=True),
    )
    op.add_column(
        "batches",
        sa.Column(
            "budget_policy",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'none'"),
        ),
    )
    op.add_column(
        "batches",
        sa.Column(
            "budget_confirmed_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "batches",
        sa.Column("pre_run_estimated_cost_usd", sa.Numeric(12, 6), nullable=True),
    )
    op.add_column(
        "batches",
        sa.Column("pre_run_cost_estimate_source", sa.Text(), nullable=True),
    )
    op.add_column(
        "batches",
        sa.Column("pre_run_cost_estimate_confidence", sa.Text(), nullable=True),
    )
    op.add_column(
        "batches",
        sa.Column(
            "budget_diagnostics",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.create_check_constraint(
        "batches_budget_policy_check",
        "batches",
        "budget_policy IN ('none', 'soft', 'hard')",
    )
    op.create_check_constraint(
        "batches_budget_usd_nonnegative_check",
        "batches",
        "budget_usd IS NULL OR budget_usd >= 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "batches_budget_usd_nonnegative_check",
        "batches",
        type_="check",
    )
    op.drop_constraint(
        "batches_budget_policy_check",
        "batches",
        type_="check",
    )
    op.drop_column("batches", "budget_diagnostics")
    op.drop_column("batches", "pre_run_cost_estimate_confidence")
    op.drop_column("batches", "pre_run_cost_estimate_source")
    op.drop_column("batches", "pre_run_estimated_cost_usd")
    op.drop_column("batches", "budget_confirmed_at")
    op.drop_column("batches", "budget_policy")
    op.drop_column("batches", "budget_usd")
