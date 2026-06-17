"""revoke legacy database-backed admin tokens

Revision ID: 0024
Revises: 0023
Create Date: 2026-06-17

Admin credentials are now file-backed singleton secrets. Preserve historical
token rows for forensic prefix lookup, but revoke active DB admin rows so they
can no longer authenticate.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().execute(
        sa.text(
            "UPDATE tokens "
            "SET revoked_at = now() "
            "WHERE revoked_at IS NULL "
            "AND (type = :token_type OR EXISTS ("
            "  SELECT 1 FROM unnest(scopes) AS scope(value) "
            "  WHERE value LIKE :admin_scope_like"
            "))",
        ),
        {"token_type": "admin", "admin_scope_like": "admin:%"},
    )


def downgrade() -> None:
    # Intentionally not reversible: once DB-admin credentials are revoked,
    # downgrading must not silently reactivate them.
    pass
