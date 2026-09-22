"""llm_calls: attempt column for gateway-level retries (#298 Slice B)

Revision ID: 0029
Revises: 0028
Create Date: 2026-06-22

When the gateway retries a transient upstream failure (#298 Slice A,
PR #322), the only visible record today is an INFO log line. This
migration adds a per-row `attempt` counter so operators can answer
"did this trial's calls hit retries?" from a single SELECT:

    SELECT trial_id, MAX(attempt) FROM llm_calls
    WHERE trial_id = $1 GROUP BY trial_id;

`attempt = 1` (the default) means the upstream call succeeded on
the first try — true for every existing row, so backfill is trivial.
A value > 1 means N-1 retries happened inside the gateway before
returning the successful response.

The column is NOT NULL with a server default so old code paths that
don't pass `attempt` (e.g. unit tests not yet updated) keep inserting
attempt=1 cleanly. No backfill needed.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "llm_calls",
        sa.Column(
            "attempt",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
    )


def downgrade() -> None:
    op.drop_column("llm_calls", "attempt")
