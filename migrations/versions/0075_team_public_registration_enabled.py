"""Team-level public registration policy (#775).

Revision ID: 0075
Revises: 0074
Create Date: 2026-07-29

Adds an explicit per-Team opt-in for public account onboarding discovery and
submission. Existing and newly created Teams remain private by default; no
bootstrap rule or name heuristic auto-enables registration.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0075"
down_revision = "0074"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teams",
        sa.Column(
            "public_registration_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("teams", "public_registration_enabled")
