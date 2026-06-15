"""Reclassify series for sharper SPA grouping.

Revision ID: 0017
Revises: 0016
Create Date: 2026-06-15

PR-2 (#37) gave every benchmark a series so the SPA had no "Other"
bucket, but lumped osworld/webarena/gaia into one `agents` series and
bfcl into `code`. Both turned out misleading on the picker: OSWorld
tests computer-use, WebArena tests web browsing, GAIA tests
general-research; BFCL tests function calling, not code generation.
Splitting them apart so series labels match what the benchmark
actually measures.

Existing DB rows: the seed's backfill path only writes series when
it's NULL (so it doesn't clobber operator overrides on re-seed); this
migration explicitly resyncs the moved adapters. No-op if a row
doesn't exist (operator hasn't seeded yet).

Reversible: downgrade reverts to the (\#37) classification."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


_UP: dict[str, tuple[str, str]] = {
    # (old_series, new_series) per benchmark id. Only the moved rows.
    "osworld":  ("agents", "ui-agent"),
    "webarena": ("agents", "ui-agent"),
    "gaia":     ("agents", "research-agent"),
    "bfcl":     ("code",   "tool-use"),
}


def upgrade() -> None:
    conn = op.get_bind()
    for bench_id, (_old, new) in _UP.items():
        conn.execute(
            text("UPDATE benchmarks SET series = :s WHERE id = :id"),
            {"s": new, "id": bench_id},
        )


def downgrade() -> None:
    conn = op.get_bind()
    for bench_id, (old, _new) in _UP.items():
        conn.execute(
            text("UPDATE benchmarks SET series = :s WHERE id = :id"),
            {"s": old, "id": bench_id},
        )
