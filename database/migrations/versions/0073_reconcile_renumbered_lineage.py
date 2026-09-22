"""Reconcile the pre-#857 renumbered migration lineage on staging.

Staging's database was built on the pre-#857 lineage where the four
staging-lifecycle migrations were numbered 0066-0069. dev-tip renumbered them to
0069-0072 and *inserted* four migrations before them: ``0062_tb21_profile_catalog``
(benchmark profile catalog + Terminal-Bench-2 archival), ``0066_autoscaler_policy
_prod_pressure_state`` (one nullable JSONB column), ``0067_rename_pool_gb10arm64
_to_gb10`` (the ``gb10-arm64`` -> ``gb10`` pool rename, #883), and ``0068`` (a
graph merge, no content).

A live DB carrying the old lineage therefore already holds the full lifecycle
content (identical bytes to dev-tip's 0069-0072) but is *missing* the three
inserted migrations' content, and its alembic stamp collides with dev-tip's
different-content 0069. The live cutover (see #949) stamps such a DB to ``0072``
out-of-band — accepting the lifecycle content it demonstrably already has — then
runs this migration, which idempotently applies exactly the content of 0062,
0066 and 0067. On a normally-migrated database (fresh ``upgrade head``, which ran
0062/0066/0067 already) every step here is a guarded no-op, so both lineages
converge on an identical schema. See #949.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

revision = "0073"
down_revision = "0072"
branch_labels = None
depends_on = None

# Verbatim from 0062_tb21_profile_catalog.
_LEGACY_BENCHMARK_ID = "terminal-bench-2"
_LEGACY_PROFILE_ID = "terminal-bench-2@tb2.0-91e10457"
_MIGRATION_REVISION = "0062_tb21_profile_catalog"

# Verbatim from 0067_rename_pool_gb10arm64_to_gb10.
_POOL_OLD = "gb10-arm64"
_POOL_NEW = "gb10"

_WORKERS_SQL = """
UPDATE workers
   SET pool_name = '{new}'
 WHERE pool_name = '{old}'
"""

_ENV_POOL_SQL = """
UPDATE {table} p
   SET pool_name = '{new}'
 WHERE p.pool_name = '{old}'
   AND NOT EXISTS (
        SELECT 1 FROM {table} q
         WHERE q.pool_name = '{new}'
           AND q.environment = p.environment
   )
"""

_ENV_POOL_HOST_SQL = """
UPDATE gb10_worker_node_statuses p
   SET pool_name = '{new}'
 WHERE p.pool_name = '{old}'
   AND NOT EXISTS (
        SELECT 1 FROM gb10_worker_node_statuses q
         WHERE q.pool_name = '{new}'
           AND q.environment = p.environment
           AND q.hostname = p.hostname
   )
"""

_SLURM_SQL = """
UPDATE slurm_worker_jobs p
   SET pool_name = '{new}'
 WHERE p.pool_name = '{old}'
   AND NOT EXISTS (
        SELECT 1 FROM slurm_worker_jobs q
         WHERE q.pool_name = '{new}'
           AND q.environment = p.environment
           AND q.nodelist = p.nodelist
           AND coalesce(q.requested_cpus, -1) = coalesce(p.requested_cpus, -1)
           AND coalesce(q.requested_memory_mib, -1)
                 = coalesce(p.requested_memory_mib, -1)
           AND q.requested_concurrency = p.requested_concurrency
           AND q.state IN ('pending', 'running')
           AND p.state IN ('pending', 'running')
   )
"""


def _has_column(bind: sa.engine.Connection, table: str, column: str) -> bool:
    return (
        bind.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t "
                "AND column_name = :c"
            ),
            {"t": table, "c": column},
        ).scalar()
        is not None
    )


def _has_table(bind: sa.engine.Connection, table: str) -> bool:
    return (
        bind.execute(
            sa.text("SELECT to_regclass(:t)"), {"t": f"public.{table}"}
        ).scalar()
        is not None
    )


def _rewrite_batch_selectors(*, from_id: str, to_id: str) -> None:
    """Replace only exact TB2 selectors, including plural selectors. From 0062."""
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


def _reconcile_0062_benchmark_profiles(bind: sa.engine.Connection) -> None:
    """0062 content: profile-catalog columns/table + TB2 archival (idempotent)."""
    if not _has_column(bind, "benchmarks", "execution_state"):
        op.add_column(
            "benchmarks",
            sa.Column(
                "execution_state",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'runnable'"),
            ),
        )
    if not _has_column(bind, "benchmarks", "profile_provenance"):
        op.add_column(
            "benchmarks",
            sa.Column(
                "profile_provenance",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
        )
    if not _has_column(bind, "tasks", "source_provenance"):
        op.add_column(
            "tasks",
            sa.Column(
                "source_provenance",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
        )
    if not _has_column(bind, "batches", "resolved_task_ids"):
        op.add_column(
            "batches",
            sa.Column(
                "resolved_task_ids",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
        )
    if not _has_table(bind, "benchmark_aliases"):
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
    # Terminal-Bench-2 archival. Every statement is naturally idempotent: it
    # keys on the legacy id, which is absent once archived (or was never present
    # on this environment), so a second run — or a fresh DB where 0062 already
    # archived it — is a no-op.
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
            profile_id=_LEGACY_PROFILE_ID,
            legacy_id=_LEGACY_BENCHMARK_ID,
            migration_revision=_MIGRATION_REVISION,
        ),
    )
    op.execute(
        sa.text(
            "UPDATE tasks SET benchmark_id = :profile_id "
            "WHERE benchmark_id = :legacy_id",
        ).bindparams(profile_id=_LEGACY_PROFILE_ID, legacy_id=_LEGACY_BENCHMARK_ID),
    )
    _rewrite_batch_selectors(from_id=_LEGACY_BENCHMARK_ID, to_id=_LEGACY_PROFILE_ID)
    op.execute(
        sa.text("DELETE FROM benchmarks WHERE id = :legacy_id").bindparams(
            legacy_id=_LEGACY_BENCHMARK_ID,
        ),
    )


def _reconcile_0066_prod_pressure(bind: sa.engine.Connection) -> None:
    if not _has_column(bind, "worker_pool_autoscaler_policies", "prod_pressure_state"):
        op.add_column(
            "worker_pool_autoscaler_policies",
            sa.Column("prod_pressure_state", JSONB(), nullable=True),
        )


def _reconcile_0067_pool_rename() -> None:
    """0067 content: gb10-arm64 -> gb10 across every pool_name column. The
    per-table NOT EXISTS guards (verbatim from 0067) make it a no-op once every
    row already reads 'gb10'."""
    op.execute(_WORKERS_SQL.format(old=_POOL_OLD, new=_POOL_NEW))
    for table in (
        "gb10_worker_pool_desired_states",
        "worker_pool_autoscaler_policies",
    ):
        op.execute(_ENV_POOL_SQL.format(table=table, old=_POOL_OLD, new=_POOL_NEW))
    op.execute(_ENV_POOL_HOST_SQL.format(old=_POOL_OLD, new=_POOL_NEW))
    op.execute(_SLURM_SQL.format(old=_POOL_OLD, new=_POOL_NEW))


def upgrade() -> None:
    bind = op.get_bind()
    _reconcile_0062_benchmark_profiles(bind)
    _reconcile_0066_prod_pressure(bind)
    _reconcile_0067_pool_rename()


def downgrade() -> None:
    # One-directional lineage reconciliation. On a normally-migrated database
    # upgrade() was a guarded no-op, so there is nothing to reverse; on a
    # reconciled staging DB the applied content belongs to 0062/0066/0067 and
    # must not be dropped here.
    pass
