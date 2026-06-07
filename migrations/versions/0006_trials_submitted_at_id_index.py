"""composite index on trials(submitted_at DESC, id DESC) for service-layer paging

Revision ID: 0006
Revises: 0005
Create Date: 2026-06-07

Plan 18's `GET /api/v1/trials` paginates by `(submitted_at, id)` DESC
+ a `WHERE (submitted_at < c.t) OR (submitted_at == c.t AND id < c.id)`
keyset filter. Without an index, every page does a full sort. This
index lets Postgres walk it as a reverse btree scan.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ASC-only btree — Postgres walks it backwards for DESC ORDER BY,
    # so the same index serves both directions.
    op.create_index(
        "idx_trials_submitted_at_id_desc",
        "trials",
        ["submitted_at", "id"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_trials_submitted_at_id_desc", table_name="trials",
    )
