"""Rename team_quotas.max_attempts to max_attempts_ceiling (#401 PR-1).

Semantic rename only — no data change, no behavior change. The column
previously named `max_attempts` is the *admin's ceiling* on how many
attempts a team's trials may accumulate before the scheduler stops
re-claiming them. It's semantically distinct from
`TrialConfig.retry.max_attempts` (the submitter's requested count),
which stays named `max_attempts` because it IS the requested count.
See #401 for the design.
"""

from __future__ import annotations

from alembic import op

revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "team_quotas",
        "max_attempts",
        new_column_name="max_attempts_ceiling",
    )


def downgrade() -> None:
    op.alter_column(
        "team_quotas",
        "max_attempts_ceiling",
        new_column_name="max_attempts",
    )
