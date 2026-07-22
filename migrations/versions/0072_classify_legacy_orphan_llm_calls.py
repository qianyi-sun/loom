"""Allow exact authorities for legacy LLM calls without a trial.

Revision ID: 0072
Revises: 0071
Create Date: 2026-07-19
"""

from __future__ import annotations

from alembic import op

revision = "0072"
down_revision = "0071"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "data_lifecycle_authorities_owner_kind_check",
        "data_lifecycle_authorities",
        type_="check",
    )
    op.create_check_constraint(
        "data_lifecycle_authorities_owner_kind_check",
        "data_lifecycle_authorities",
        "owner_kind IN ('batch','trial','artifact','benchmark','system','orphan')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "data_lifecycle_authorities_owner_kind_check",
        "data_lifecycle_authorities",
        type_="check",
    )
    op.create_check_constraint(
        "data_lifecycle_authorities_owner_kind_check",
        "data_lifecycle_authorities",
        "owner_kind IN ('batch','trial','artifact','benchmark','system')",
    )
