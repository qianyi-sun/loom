"""tasks.tags + benchmarks.series for the benchmark-series grouping work

Revision ID: 0014
Revises: 0013
Create Date: 2026-06-14

Per the series/tags design (catalog rework):

- `tasks.tags` — open-ended key→value metadata per task. Adapters
  populate from upstream (year/exam/difficulty/topic/verified/…).
  Default `{}` so old rows stay valid. The SPA reads these for the
  tag-filter card; the backend exposes a discovery endpoint that
  walks distinct values per benchmark.
- `benchmarks.series` — varchar(64) grouping label. Convention:
  `"aime"`, `"swe-bench"`, …; NULL = standalone benchmark, not part
  of a series. The SPA's dropdown groups by series; disjoint
  variants (AIME by year) sit as siblings so group-select unions
  cleanly without duplicates.

GIN index on `tasks.tags` so the tag-filter query path uses an
index rather than seq-scanning the table; benchmark-scoped filter
typically narrows to a few thousand rows first, but for SWE-Bench
(2294 rows) the index still pays off.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column(
            "tags",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index(
        "ix_tasks_tags_gin",
        "tasks",
        ["tags"],
        postgresql_using="gin",
    )
    op.add_column(
        "benchmarks",
        sa.Column("series", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "ix_benchmarks_series",
        "benchmarks",
        ["series"],
    )


def downgrade() -> None:
    op.drop_index("ix_benchmarks_series", table_name="benchmarks")
    op.drop_column("benchmarks", "series")
    op.drop_index("ix_tasks_tags_gin", table_name="tasks")
    op.drop_column("tasks", "tags")
