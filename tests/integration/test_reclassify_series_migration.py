"""Migration 0017: reclassify series for sharper SPA grouping.

Pins the UPDATE behavior so a future re-shuffle has a baseline. Also
covers the no-op case (row absent) so the migration doesn't trip on a
fresh DB that hasn't seeded yet."""

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
def at_0016(isolated_migration_postgres_url: str) -> Engine:
    cfg = _cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0016")
    engine = create_engine(isolated_migration_postgres_url, future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM benchmarks WHERE id IN "
                "('osworld', 'webarena', 'gaia', 'bfcl', 'humaneval')",
            )
        )
    return engine


def _insert(conn, bench_id: str, series: str) -> None:
    conn.execute(
        text(
            "INSERT INTO benchmarks (id, display_name, upstream_kind, "
            "upstream_locator, upstream_revision, license_spdx, license_url, "
            "splits, series) VALUES (:id, :id, 'huggingface', 'x/y', 'main', "
            "'MIT', '', ARRAY['test'], :s)"
        ),
        {"id": bench_id, "s": series},
    )


def test_0017_reclassifies_moved_adapters(
    at_0016: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0016.begin() as conn:
        _insert(conn, "osworld", "agents")
        _insert(conn, "webarena", "agents")
        _insert(conn, "gaia", "agents")
        _insert(conn, "bfcl", "code")
        _insert(conn, "humaneval", "code")  # unchanged
    command.upgrade(_cfg(isolated_migration_postgres_url), "0017")
    with at_0016.begin() as conn:
        rows = dict(
            conn.execute(
                text(
                    "SELECT id, series FROM benchmarks "
                    "WHERE id IN ('osworld', 'webarena', 'gaia', 'bfcl', 'humaneval')",
                )
            ).fetchall()
        )
    assert rows["osworld"] == "ui-agent"
    assert rows["webarena"] == "ui-agent"
    assert rows["gaia"] == "research-agent"
    assert rows["bfcl"] == "tool-use"
    # humaneval was 'code', should stay 'code' — not in the moved set.
    assert rows["humaneval"] == "code"


def test_0017_is_no_op_when_rows_absent(
    at_0016: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    """Fresh DB without those benchmark rows shouldn't break the
    migration — UPDATE matches zero rows, succeeds silently."""
    command.upgrade(_cfg(isolated_migration_postgres_url), "0017")
    # No assertions — just verify no exception. Roll forward.
    command.upgrade(_cfg(isolated_migration_postgres_url), "head")


def test_0017_downgrade_restores_prior_series(
    at_0016: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0016.begin() as conn:
        _insert(conn, "osworld", "agents")
        _insert(conn, "bfcl", "code")
    command.upgrade(_cfg(isolated_migration_postgres_url), "0017")
    command.downgrade(_cfg(isolated_migration_postgres_url), "0016")
    with at_0016.begin() as conn:
        rows = dict(
            conn.execute(
                text(
                    "SELECT id, series FROM benchmarks WHERE id IN ('osworld', 'bfcl')",
                )
            ).fetchall()
        )
    assert rows["osworld"] == "agents"
    assert rows["bfcl"] == "code"
    command.upgrade(_cfg(isolated_migration_postgres_url), "head")
