"""Rename benchmark `aime` → `aime-aimo-validation` (series/tags follow-up)

Revision ID: 0015
Revises: 0014
Create Date: 2026-06-14

PR-1 (#24) renamed the AIME adapter from `aime` to `aime-aimo-validation`
and reshaped its instance ids from opaque integers (`aime/47`) to
structured triples (`aime-aimo-validation/2024-I/7`). The DB rows
registered by the old adapter were left untouched — this migration
brings them in line.

Two cases handled idempotently:

1. **Only the old `aime` row exists** (most common in deployments that
   ran `loom_benchmark_tool register` before PR-1): drop its tasks (old
   integer ids can't be remapped to structured ids in pure SQL — the
   mapping lives in the adapter's URL parser), then UPDATE the
   benchmark row's PK from `aime` → `aime-aimo-validation`. The new
   adapter run will re-populate tasks with structured ids.

2. **Both rows exist** (deployment that registered the new adapter
   without dropping the old row): drop the old `aime` benchmark and
   its tasks; the new `aime-aimo-validation` row already has the
   correct shape.

Trials that reference old `aime/*` task ids will block the task DELETE
via the `trials.task_id → tasks.id` FK (no ON DELETE cascade). That is
deliberate: silently nuking trial history is worse than failing the
migration loudly so the operator can decide. Downgrade does not
restore deleted rows — destructive.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    has_old = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime'"),
    ).first()
    if has_old is None:
        return

    # Old AIME tasks use the opaque integer-id format (`aime/47`); the
    # new adapter emits structured ids (`aime-aimo-validation/2024-I/7`).
    # There's no SQL-only remap, so drop the old rows and let
    # `loom_benchmark_tool register` repopulate them.
    conn.execute(text("DELETE FROM tasks WHERE benchmark_id = 'aime'"))

    has_new = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime-aimo-validation'"),
    ).first()
    if has_new is None:
        # Promote the old row to the new PK. With no tasks remaining
        # (just deleted above) the FK from `tasks.benchmark_id` poses
        # no constraint issue.
        conn.execute(text(
            "UPDATE benchmarks SET "
            "id = 'aime-aimo-validation', "
            "display_name = 'AIME (AIMO validation 2022–2024)', "
            "series = 'aime' "
            "WHERE id = 'aime'",
        ))
    else:
        # Both rows present — old one is the stale duplicate.
        conn.execute(text("DELETE FROM benchmarks WHERE id = 'aime'"))


def downgrade() -> None:
    # Reverse the PK rename when the row was promoted (case 1). If
    # someone needs the old data they restore from backup; this
    # migration's deletes are not recoverable.
    conn = op.get_bind()
    has_new = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime-aimo-validation'"),
    ).first()
    if has_new is None:
        return
    has_old = conn.execute(
        text("SELECT 1 FROM benchmarks WHERE id = 'aime'"),
    ).first()
    if has_old is not None:
        return
    conn.execute(text(
        "UPDATE benchmarks SET "
        "id = 'aime', display_name = 'AIME', series = NULL "
        "WHERE id = 'aime-aimo-validation'",
    ))
