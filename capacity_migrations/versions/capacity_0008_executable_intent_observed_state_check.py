"""Constrain executable intent observed-state evidence.

Revision ID: capacity_0008
Revises: capacity_0007
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "capacity_0008"
down_revision: str | Sequence[str] | None = "capacity_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OBSERVED_STATE_CHECK = (
    "observed_state IS NULL OR observed_state IN "
    "('pending','active','draining','terminal','unknown')"
)


def upgrade() -> None:
    op.create_check_constraint(
        "capacity_executable_intent_observed_state_check",
        "capacity_executable_intents",
        _OBSERVED_STATE_CHECK,
    )


def downgrade() -> None:
    op.drop_constraint(
        "capacity_executable_intent_observed_state_check",
        "capacity_executable_intents",
        type_="check",
    )
