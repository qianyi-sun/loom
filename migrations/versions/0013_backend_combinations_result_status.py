"""backend column, combinations JSONB, result_status, combination_idx

Revision ID: 0013
Revises: 0012
Create Date: 2026-06-11

Per loom-spa-v3 / Plan 28 PR-3:

- `batches.backend` — surfaces backend selection at the batch level
  (was implicit). Default `docker` preserves single-backend behavior.
- `batches.combinations` — JSONB list of (agent, model, n_per_task,
  label) tuples for multi-(agent, model) batches. Default `[]`
  means single-combination (back-compat: agent + model live on
  trial_config as before).
- `batches.result_status` — outcome lane separate from lifecycle
  `status`. NULL until terminal; then one of `succeeded`,
  `partial_failed`, `all_failed`, `cancelled`. The batch_runner
  computes this when transitioning to a terminal lifecycle state.
- `trials.combination_idx` — which Combination this trial belongs
  to within its parent Batch. 0 for single-combination batches.

All four use server_default so existing rows read sensibly without
a backfill UPDATE.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "batches",
        sa.Column(
            "backend", sa.Text(), nullable=False,
            server_default=sa.text("'docker'"),
        ),
    )
    op.add_column(
        "batches",
        sa.Column(
            "combinations", postgresql.JSONB, nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "batches",
        sa.Column("result_status", sa.Text(), nullable=True),
    )
    op.add_column(
        "trials",
        sa.Column(
            "combination_idx", sa.Integer(), nullable=False,
            server_default=sa.text("0"),
        ),
    )


def downgrade() -> None:
    op.drop_column("trials", "combination_idx")
    op.drop_column("batches", "result_status")
    op.drop_column("batches", "combinations")
    op.drop_column("batches", "backend")
