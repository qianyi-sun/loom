"""bundle store + license tracking

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-07

Per benchmark integrations spec §6-7 + amendments A13.1 (expanded
default license allowlist including CC-BY-4.0) and A13.3 (new
`tokens.last_seen_at` column used by the service layer for member
last-active rendering).

Schema deltas:
- `tasks.license` text          (SPDX-style tag per benchmark adapter)
- `tasks.benchmark_id` text     (nullable FK to benchmarks.id)
- `benchmarks`                  (new table — one row per registered benchmark)
- `team_quotas.license_allowlist text[]`
                                (default ARRAY['MIT', 'Apache-2.0',
                                'BSD-3-Clause', 'CC-BY-4.0'])
- `tokens.last_seen_at`         (timestamptz, nullable)

The benchmarks table primary key is the human-readable id (e.g.
"swe-bench-verified") so Plan 14's CLI can write `ON CONFLICT (id) DO
UPDATE` upserts naturally (amendment A13.2).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. benchmarks table (must exist before tasks.benchmark_id FK).
    op.create_table(
        "benchmarks",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("upstream_kind", sa.Text(), nullable=False),
        sa.Column("upstream_locator", sa.Text(), nullable=False),
        sa.Column("upstream_revision", sa.Text(), nullable=False),
        sa.Column("license_spdx", sa.Text(), nullable=False),
        sa.Column("license_url", sa.Text(), nullable=False),
        sa.Column(
            "splits", postgresql.ARRAY(sa.Text()), nullable=False,
        ),
        sa.Column(
            "imported_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("imported_by", sa.Text(), nullable=True),
    )

    # 2. tasks.license + tasks.benchmark_id.
    op.add_column(
        "tasks",
        sa.Column("license", sa.Text(), nullable=True),
    )
    op.add_column(
        "tasks",
        sa.Column("benchmark_id", sa.Text(), nullable=True),
    )
    op.create_foreign_key(
        "tasks_benchmark_id_fkey",
        "tasks", "benchmarks",
        ["benchmark_id"], ["id"],
    )

    # 3. team_quotas.license_allowlist. Default per A13.1 — added
    #    CC-BY-4.0 so the public-benchmark slate (MBPP, GAIA) passes
    #    the submit-time check without operator action.
    op.add_column(
        "team_quotas",
        sa.Column(
            "license_allowlist",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text(
                "ARRAY['MIT', 'Apache-2.0', 'BSD-3-Clause', 'CC-BY-4.0']::text[]",
            ),
        ),
    )

    # 4. tokens.last_seen_at (A13.3). Service layer + Control Plane +
    #    Gateway debounce-update this on each verify_bearer_token success.
    op.add_column(
        "tokens",
        sa.Column(
            "last_seen_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("tokens", "last_seen_at")
    op.drop_column("team_quotas", "license_allowlist")
    op.drop_constraint("tasks_benchmark_id_fkey", "tasks", type_="foreignkey")
    op.drop_column("tasks", "benchmark_id")
    op.drop_column("tasks", "license")
    op.drop_table("benchmarks")
