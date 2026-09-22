"""Add immutable benchmark profiles and archive legacy Terminal-Bench 2.

Revision ID: 0062_tb21_profile_catalog
Revises: 0061
Create Date: 2026-07-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0062_tb21_profile_catalog"
down_revision = "0061"
branch_labels = None
depends_on = None

LEGACY_BENCHMARK_ID = "terminal-bench-2"
LEGACY_PROFILE_ID = "terminal-bench-2@tb2.0-91e10457"
MIGRATION_REVISION = "0062_tb21_profile_catalog"


def _rewrite_batch_selectors(*, from_id: str, to_id: str) -> None:
    """Replace only exact TB2 selectors, including plural selectors."""
    op.execute(
        sa.text(
            """
            WITH singular_rewritten AS (
                SELECT
                    id,
                    CASE
                        WHEN task_filter ->> 'benchmark_id' = :from_id THEN
                            jsonb_set(
                                task_filter,
                                '{benchmark_id}',
                                to_jsonb(CAST(:to_id AS text)),
                                true
                            )
                        ELSE task_filter
                    END AS task_filter
                FROM batches
                WHERE task_filter ->> 'benchmark_id' = :from_id
                   OR (
                        jsonb_typeof(task_filter -> 'benchmark_ids') = 'array'
                        AND task_filter -> 'benchmark_ids'
                            @> jsonb_build_array(CAST(:from_id AS text))
                   )
            ),
            rewritten AS (
                SELECT
                    id,
                    CASE
                        WHEN jsonb_typeof(task_filter -> 'benchmark_ids') = 'array'
                             AND task_filter -> 'benchmark_ids'
                                 @> jsonb_build_array(CAST(:from_id AS text)) THEN
                            jsonb_set(
                                task_filter,
                                '{benchmark_ids}',
                                (
                                    SELECT jsonb_agg(
                                        CASE
                                            WHEN item = to_jsonb(CAST(:from_id AS text))
                                                THEN to_jsonb(CAST(:to_id AS text))
                                            ELSE item
                                        END
                                        ORDER BY ordinality
                                    )
                                    FROM jsonb_array_elements(
                                        task_filter -> 'benchmark_ids'
                                    ) WITH ORDINALITY AS values(item, ordinality)
                                ),
                                true
                            )
                        ELSE task_filter
                    END AS task_filter
                FROM singular_rewritten
            )
            UPDATE batches AS target
            SET task_filter = rewritten.task_filter
            FROM rewritten
            WHERE target.id = rewritten.id
            """,
        ).bindparams(from_id=from_id, to_id=to_id),
    )


def upgrade() -> None:
    op.add_column(
        "benchmarks",
        sa.Column(
            "execution_state",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'runnable'"),
        ),
    )
    op.add_column(
        "benchmarks",
        sa.Column(
            "profile_provenance",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "tasks",
        sa.Column(
            "source_provenance",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "batches",
        sa.Column(
            "resolved_task_ids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.create_table(
        "benchmark_aliases",
        sa.Column("alias", sa.Text(), primary_key=True),
        sa.Column("benchmark_id", sa.Text(), nullable=False),
        sa.Column(
            "activated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["benchmark_id"],
            ["benchmarks.id"],
            ondelete="RESTRICT",
        ),
    )

    op.execute(
        sa.text(
            """
            INSERT INTO benchmarks (
                id,
                display_name,
                upstream_kind,
                upstream_locator,
                upstream_revision,
                license_spdx,
                license_url,
                splits,
                series,
                execution_state,
                profile_provenance,
                imported_at,
                imported_by
            )
            SELECT
                :profile_id,
                'Terminal-Bench 2.0 (archived, 91e10457)',
                upstream_kind,
                upstream_locator,
                upstream_revision,
                license_spdx,
                license_url,
                splits,
                series,
                'historical',
                jsonb_build_object(
                    'migration_revision', :migration_revision,
                    'legacy_benchmark_id', id,
                    'legacy_display_name', display_name,
                    'legacy_upstream_revision', upstream_revision
                ),
                imported_at,
                imported_by
            FROM benchmarks
            WHERE id = :legacy_id
            ON CONFLICT (id) DO NOTHING
            """,
        ).bindparams(
            profile_id=LEGACY_PROFILE_ID,
            legacy_id=LEGACY_BENCHMARK_ID,
            migration_revision=MIGRATION_REVISION,
        ),
    )
    op.execute(
        sa.text(
            "UPDATE tasks SET benchmark_id = :profile_id "
            "WHERE benchmark_id = :legacy_id",
        ).bindparams(profile_id=LEGACY_PROFILE_ID, legacy_id=LEGACY_BENCHMARK_ID),
    )
    _rewrite_batch_selectors(from_id=LEGACY_BENCHMARK_ID, to_id=LEGACY_PROFILE_ID)
    op.execute(
        sa.text("DELETE FROM benchmarks WHERE id = :legacy_id").bindparams(
            legacy_id=LEGACY_BENCHMARK_ID,
        ),
    )


def downgrade() -> None:
    op.drop_table("benchmark_aliases")
    op.execute(
        sa.text(
            """
            INSERT INTO benchmarks (
                id,
                display_name,
                upstream_kind,
                upstream_locator,
                upstream_revision,
                license_spdx,
                license_url,
                splits,
                series,
                execution_state,
                profile_provenance,
                imported_at,
                imported_by
            )
            SELECT
                :legacy_id,
                COALESCE(
                    profile_provenance ->> 'legacy_display_name',
                    'Terminal-Bench 2'
                ),
                upstream_kind,
                upstream_locator,
                upstream_revision,
                license_spdx,
                license_url,
                splits,
                series,
                'runnable',
                '{}'::jsonb,
                imported_at,
                imported_by
            FROM benchmarks
            WHERE id = :profile_id
            ON CONFLICT (id) DO NOTHING
            """,
        ).bindparams(profile_id=LEGACY_PROFILE_ID, legacy_id=LEGACY_BENCHMARK_ID),
    )
    op.execute(
        sa.text(
            "UPDATE tasks SET benchmark_id = :legacy_id "
            "WHERE benchmark_id = :profile_id",
        ).bindparams(profile_id=LEGACY_PROFILE_ID, legacy_id=LEGACY_BENCHMARK_ID),
    )
    _rewrite_batch_selectors(from_id=LEGACY_PROFILE_ID, to_id=LEGACY_BENCHMARK_ID)
    op.execute(
        sa.text("DELETE FROM benchmarks WHERE id = :profile_id").bindparams(
            profile_id=LEGACY_PROFILE_ID,
        ),
    )
    op.drop_column("batches", "resolved_task_ids")
    op.drop_column("tasks", "source_provenance")
    op.drop_column("benchmarks", "profile_provenance")
    op.drop_column("benchmarks", "execution_state")
