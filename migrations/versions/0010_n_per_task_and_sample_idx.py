"""n_per_task on campaigns/workflows, sample_idx on trials — n-sampling

Revision ID: 0010
Revises: 0009
Create Date: 2026-06-11

Adds three columns for n-sampling fan-out:

- `campaigns.n_per_task` — how many trials the runner submits per
  matched task. Default 1 preserves current behavior.
- `workflows.n_per_task` — same field on the Workflow recipe; the
  launch route copies it onto the Campaign so the run is reproducible.
- `trials.sample_idx` — which sample within `(campaign_id, task_id)`
  this trial is. The pair forms the basis of the new
  `idempotency_key = {campaign}::{task}::{sample_idx}` format.

All three use `server_default` so existing rows naturally read as 1 /
0 without an explicit backfill UPDATE — see the design checkpoint on
the PR.

The runner's `already_submitted` set previously held bare task_ids;
it now needs `(task_id, sample_idx)` pairs to know which samples
remain pending. Reading `trials.sample_idx` directly avoids parsing
the opaque idempotency_key string at runtime.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "campaigns",
        sa.Column(
            "n_per_task", sa.Integer(), nullable=False,
            server_default=sa.text("1"),
        ),
    )
    op.add_column(
        "workflows",
        sa.Column(
            "n_per_task", sa.Integer(), nullable=False,
            server_default=sa.text("1"),
        ),
    )
    op.add_column(
        "trials",
        sa.Column(
            "sample_idx", sa.Integer(), nullable=False,
            server_default=sa.text("0"),
        ),
    )


def downgrade() -> None:
    op.drop_column("trials", "sample_idx")
    op.drop_column("workflows", "n_per_task")
    op.drop_column("campaigns", "n_per_task")
