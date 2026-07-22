"""Migration 0016: AIME per-year split.

Replaces the combined `aime-aimo-validation` adapter with per-year
siblings (`aime-22`, `aime-23`, `aime-24`) and renames `aime-2025` →
`aime-25` for slug consistency. The migration drops the old rows; the
adapter side repopulates on next `loom_benchmark_tool register`.
"""

from __future__ import annotations

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _cfg(db_url: str) -> Config:
    cfg = Config("migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture
def at_0015(isolated_migration_postgres_url: str) -> Engine:
    cfg = _cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0015")
    engine = create_engine(isolated_migration_postgres_url, future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM tasks WHERE benchmark_id IN "
                "('aime-aimo-validation', 'aime-2025', 'aime-22', 'aime-25')",
            )
        )
        conn.execute(
            text(
                "DELETE FROM benchmarks WHERE id IN "
                "('aime-aimo-validation', 'aime-2025', 'aime-22', 'aime-25')",
            )
        )
    return engine


def _insert_bench(conn, bench_id: str, display: str) -> None:
    conn.execute(
        text(
            "INSERT INTO benchmarks (id, display_name, upstream_kind, "
            "upstream_locator, upstream_revision, license_spdx, license_url, "
            "splits, series) VALUES (:id, :display, 'huggingface', 'x/y', "
            "'main', 'proprietary-MAA', '', ARRAY['train'], 'aime')"
        ),
        {"id": bench_id, "display": display},
    )


def test_0016_no_op_when_neither_old_row_exists(
    at_0015: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    command.upgrade(_cfg(isolated_migration_postgres_url), "0016")
    # No exceptions, no leftover rows.
    with at_0015.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id FROM benchmarks WHERE id LIKE 'aime%'",
            )
        ).fetchall()
    assert rows == []


def test_0016_drops_combined_aime_aimo_validation(
    at_0015: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0015.begin() as conn:
        _insert_bench(conn, "aime-aimo-validation", "AIME combined")
    command.upgrade(_cfg(isolated_migration_postgres_url), "0016")
    with at_0015.begin() as conn:
        gone = conn.execute(
            text(
                "SELECT 1 FROM benchmarks WHERE id = 'aime-aimo-validation'",
            )
        ).first()
    assert gone is None


def test_0016_renames_aime_2025_to_aime_25(
    at_0015: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0015.begin() as conn:
        _insert_bench(conn, "aime-2025", "AIME 2025 (old slug)")
    command.upgrade(_cfg(isolated_migration_postgres_url), "0016")
    with at_0015.begin() as conn:
        new = conn.execute(
            text(
                "SELECT id, display_name FROM benchmarks WHERE id = 'aime-25'",
            )
        ).first()
        old = conn.execute(
            text(
                "SELECT 1 FROM benchmarks WHERE id = 'aime-2025'",
            )
        ).first()
    assert new is not None
    assert new[0] == "aime-25"
    assert old is None


def test_0016_drops_duplicate_when_both_aime_2025_and_aime_25_exist(
    at_0015: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    """Operator who already ran post-0016 register has the new aime-25
    row; the migration must drop the stale aime-2025 rather than
    failing on PK conflict."""
    with at_0015.begin() as conn:
        _insert_bench(conn, "aime-2025", "AIME 2025 (old)")
        _insert_bench(conn, "aime-25", "AIME 2025")
    command.upgrade(_cfg(isolated_migration_postgres_url), "0016")
    with at_0015.begin() as conn:
        ids = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT id FROM benchmarks WHERE id LIKE 'aime%'",
                )
            ).fetchall()
        }
    assert ids == {"aime-25"}


def test_0016_downgrade_restores_aime_2025(
    at_0015: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0015.begin() as conn:
        _insert_bench(conn, "aime-2025", "AIME 2025")
    command.upgrade(_cfg(isolated_migration_postgres_url), "0016")
    command.downgrade(_cfg(isolated_migration_postgres_url), "0015")
    with at_0015.begin() as conn:
        row = conn.execute(
            text(
                "SELECT id FROM benchmarks WHERE id IN ('aime-2025', 'aime-25')",
            )
        ).first()
    assert row is not None
    assert row[0] == "aime-2025"
    # Restore head so subsequent tests run at the latest revision.
    command.upgrade(_cfg(isolated_migration_postgres_url), "head")
