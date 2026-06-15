"""AIME per-year split: drop aime-aimo-validation, rename aime-2025 → aime-25

Revision ID: 0016
Revises: 0015
Create Date: 2026-06-14

Follow-up to the series/tags catalog redesign. The combined
`aime-aimo-validation` adapter is replaced by per-year siblings
(`aime-22`, `aime-23`, `aime-24`) under series=aime, and `aime-2025`
is renamed `aime-25` for slug consistency with the new siblings.

Three branches, like the 0015 rename:
1. No old row exists → no-op.
2. `aime-aimo-validation` exists → drop its tasks + the benchmark row.
   The new per-year adapters will be repopulated by the next
   `loom_benchmark_tool register` cycle.
3. `aime-2025` exists → either rename PK to `aime-25` (if no collision)
   or drop the duplicate (if `aime-25` is already there).

Trials that reference dropped task ids block the migration via the
trials.task_id FK — deliberate, same policy as 0015. Operators with
production trial history must back up + re-key manually.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1. Drop the combined aime-aimo-validation row + its tasks.
    has_combined = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime-aimo-validation'"),
    ).first()
    if has_combined is not None:
        conn.execute(text(
            "DELETE FROM tasks WHERE benchmark_id = 'aime-aimo-validation'",
        ))
        conn.execute(text(
            "DELETE FROM benchmarks WHERE id = 'aime-aimo-validation'",
        ))

    # 2. Rename aime-2025 → aime-25.
    has_old = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime-2025'"),
    ).first()
    if has_old is None:
        return
    has_new = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime-25'"),
    ).first()
    if has_new is None:
        # Tasks for aime-2025 (if any) reference the old PK via FK;
        # drop them so the benchmark rename doesn't violate the FK.
        # The per-year adapter will republish on next register.
        conn.execute(text("DELETE FROM tasks WHERE benchmark_id = 'aime-2025'"))
        conn.execute(text(
            "UPDATE benchmarks SET id = 'aime-25' WHERE id = 'aime-2025'",
        ))
    else:
        conn.execute(text("DELETE FROM tasks WHERE benchmark_id = 'aime-2025'"))
        conn.execute(text("DELETE FROM benchmarks WHERE id = 'aime-2025'"))


def downgrade() -> None:
    conn = op.get_bind()
    has_25 = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime-25'"),
    ).first()
    has_2025 = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime-2025'"),
    ).first()
    if has_25 is not None and has_2025 is None:
        conn.execute(text(
            "UPDATE benchmarks SET id = 'aime-2025' WHERE id = 'aime-25'",
        ))
