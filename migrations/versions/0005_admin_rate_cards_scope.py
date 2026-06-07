"""grant admin:rate_cards to existing admin:tokens holders

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-07

Plan 17 introduces the `admin:rate_cards` scope (Plan 20 will require
it on `/admin/rate-cards` writes). Existing admin token holders should
keep their current authority surface — this migration adds the new
scope to every token already carrying `admin:tokens`. Idempotent: the
WHERE NOT clause prevents duplicate appends on a second `upgrade`.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE tokens
           SET scopes = array_append(scopes, 'admin:rate_cards')
         WHERE 'admin:tokens' = ANY(scopes)
           AND NOT ('admin:rate_cards' = ANY(scopes))
        """,
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE tokens
           SET scopes = array_remove(scopes, 'admin:rate_cards')
         WHERE 'admin:rate_cards' = ANY(scopes)
        """,
    )
