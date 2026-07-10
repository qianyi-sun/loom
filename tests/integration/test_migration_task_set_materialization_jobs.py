"""Migration 0053 — task_set_materialization_jobs (#242 sub-plan 2)."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from testcontainers.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parents[2]


def _alembic(url: str, *args: str) -> None:
    env = os.environ.copy()
    env["LOOM_DB_URL"] = url
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", *args],
        cwd=REPO_ROOT,
        env=env,
        check=True,
    )


@pytest.fixture(scope="module")
def postgres_url_at_0052() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        _alembic(url, "upgrade", "0052")
        yield url


@pytest.fixture(scope="module")
def postgres_url_at_0061() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        _alembic(url, "upgrade", "0061")
        yield url


@pytest.fixture(scope="module")
def postgres_url_at_0062() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        _alembic(url, "upgrade", "0062")
        yield url


def _insert_task_set(conn, *, team_id: str, slug: str) -> str:
    task_set_id = f"ts/{team_id}/{slug}"
    conn.execute(
        text(
            "INSERT INTO teams (id, name) VALUES (:id, :name) "
            "ON CONFLICT DO NOTHING",
        ),
        {"id": team_id, "name": f"t-{team_id}"},
    )
    conn.execute(
        text(
            "INSERT INTO task_sets ("
            "id, owning_team_id, slug, display_name, status, intents, "
            "manifest_blob_uri"
            ") VALUES ("
            ":id, :team, :slug, 'Test', 'materializing', "
            "ARRAY['trajectory_generation']::text[], 's3://bucket/manifest.yaml'"
            ")",
        ),
        {"id": task_set_id, "team": team_id, "slug": slug},
    )
    return task_set_id


def test_upgrade_creates_materialization_jobs_table(
    postgres_url_at_0052: str,
) -> None:
    _alembic(postgres_url_at_0052, "upgrade", "0053")
    engine = create_engine(postgres_url_at_0052)
    with engine.begin() as conn:
        names = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'",
                ),
            )
        }
    engine.dispose()
    assert "task_set_materialization_jobs" in names
    _alembic(postgres_url_at_0052, "downgrade", "0052")


def test_partial_unique_rejects_second_active_job(postgres_url_at_0052: str) -> None:
    _alembic(postgres_url_at_0052, "upgrade", "0053")
    team_id = str(uuid4())
    engine = create_engine(postgres_url_at_0052)
    with engine.begin() as conn:
        task_set_id = _insert_task_set(conn, team_id=team_id, slug="dup-job")
        conn.execute(
            text(
                "INSERT INTO task_set_materialization_jobs "
                "(task_set_id, owning_team_id, state) "
                "VALUES (:ts, :team, 'queued')",
            ),
            {"ts": task_set_id, "team": team_id},
        )
        with pytest.raises(IntegrityError):
            conn.execute(
                text(
                    "INSERT INTO task_set_materialization_jobs "
                    "(task_set_id, owning_team_id, state) "
                    "VALUES (:ts, :team, 'queued')",
                ),
                {"ts": task_set_id, "team": team_id},
            )
    engine.dispose()
    _alembic(postgres_url_at_0052, "downgrade", "0052")


def test_cascade_on_task_set_delete(postgres_url_at_0052: str) -> None:
    _alembic(postgres_url_at_0052, "upgrade", "0053")
    team_id = str(uuid4())
    engine = create_engine(postgres_url_at_0052)
    with engine.begin() as conn:
        task_set_id = _insert_task_set(conn, team_id=team_id, slug="cascade")
        conn.execute(
            text(
                "INSERT INTO task_set_materialization_jobs "
                "(task_set_id, owning_team_id, state) "
                "VALUES (:ts, :team, 'queued')",
            ),
            {"ts": task_set_id, "team": team_id},
        )
        conn.execute(text("DELETE FROM task_sets WHERE id = :id"), {"id": task_set_id})
        remaining = conn.execute(
            text("SELECT count(*) FROM task_set_materialization_jobs"),
        ).scalar_one()
    engine.dispose()
    assert remaining == 0
    _alembic(postgres_url_at_0052, "downgrade", "0052")


def test_upgrade_persists_lease_fencing_state(
    postgres_url_at_0061: str,
) -> None:
    """Revision 0062 fences active legacy materialization jobs by heartbeat."""
    team_id = str(uuid4())
    engine = create_engine(postgres_url_at_0061)
    with engine.begin() as conn:
        claimed_task_set_id = _insert_task_set(
            conn,
            team_id=team_id,
            slug="legacy-claimed",
        )
        running_task_set_id = _insert_task_set(
            conn,
            team_id=team_id,
            slug="legacy-running",
        )
        conn.execute(
            text(
                "INSERT INTO task_set_materialization_jobs "
                "(task_set_id, owning_team_id, state, claimed_at) "
                "VALUES (:ts, :team, 'claimed', "
                "TIMESTAMPTZ '2026-07-10 12:00:00+00')",
            ),
            {"ts": claimed_task_set_id, "team": team_id},
        )
        conn.execute(
            text(
                "INSERT INTO task_set_materialization_jobs "
                "(task_set_id, owning_team_id, state, claimed_at) "
                "VALUES (:ts, :team, 'running', "
                "TIMESTAMPTZ '2026-07-10 12:05:00+00')",
            ),
            {"ts": running_task_set_id, "team": team_id},
        )
    engine.dispose()

    _alembic(postgres_url_at_0061, "upgrade", "0062")

    engine = create_engine(postgres_url_at_0061)
    with engine.begin() as conn:
        columns = {
            row.column_name: row.is_nullable
            for row in conn.execute(
                text(
                    "SELECT column_name, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' "
                    "AND table_name = 'task_set_materialization_jobs' "
                    "AND column_name IN ("
                    "'lease_epoch', 'lease_heartbeat_at', "
                    "'published_materialization_generation'"
                    ")",
                ),
            )
        }
        assert columns == {
            "lease_epoch": "NO",
            "lease_heartbeat_at": "YES",
            "published_materialization_generation": "NO",
        }

        legacy_rows = conn.execute(
            text(
                "SELECT state, lease_heartbeat_at = claimed_at, "
                "lease_epoch, published_materialization_generation "
                "FROM task_set_materialization_jobs "
                "WHERE task_set_id IN (:claimed, :running) "
                "ORDER BY state",
            ),
            {"claimed": claimed_task_set_id, "running": running_task_set_id},
        ).all()
        assert legacy_rows == [
            ("claimed", True, 0, 0),
            ("running", True, 0, 0),
        ]

        default_task_set_id = _insert_task_set(
            conn,
            team_id=team_id,
            slug="defaults",
        )
        default_values = conn.execute(
            text(
                "INSERT INTO task_set_materialization_jobs "
                "(task_set_id, owning_team_id, state) "
                "VALUES (:ts, :team, 'queued') "
                "RETURNING lease_epoch, published_materialization_generation",
            ),
            {"ts": default_task_set_id, "team": team_id},
        ).one()
        assert default_values == (0, 0)

        heartbeat_index_predicate = conn.execute(
            text(
                "SELECT pg_get_expr(indexes.indpred, indexes.indrelid) "
                "FROM pg_index AS indexes "
                "JOIN pg_class AS classes ON classes.oid = indexes.indexrelid "
                "WHERE classes.relname = "
                "'task_set_materialization_jobs_active_heartbeat_idx'",
            ),
        ).scalar_one()
        assert "claimed" in heartbeat_index_predicate
        assert "running" in heartbeat_index_predicate
        assert "lease_heartbeat_at IS NOT NULL" in heartbeat_index_predicate
    engine.dispose()

    _alembic(postgres_url_at_0061, "downgrade", "0061")
    engine = create_engine(postgres_url_at_0061)
    with engine.begin() as conn:
        remaining_columns = set(conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' "
                "AND table_name = 'task_set_materialization_jobs'",
            ),
        ).scalars())
        remaining_indexes = set(conn.execute(
            text(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' "
                "AND tablename = 'task_set_materialization_jobs'",
            ),
        ).scalars())
    engine.dispose()
    assert {"claimed_at", "task_set_id"}.issubset(remaining_columns)
    assert not {
        "lease_epoch",
        "lease_heartbeat_at",
        "published_materialization_generation",
    } & remaining_columns
    assert "task_set_materialization_jobs_active_heartbeat_idx" not in remaining_indexes


def test_upgrade_persists_generation_gc_cursor(postgres_url_at_0062: str) -> None:
    """Revision 0063 adds a scheduling-only durable GC cursor."""
    _alembic(postgres_url_at_0062, "upgrade", "0063")
    engine = create_engine(postgres_url_at_0062)
    with engine.begin() as conn:
        columns = {
            row.column_name: row.is_nullable
            for row in conn.execute(
                text(
                    "SELECT column_name, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' "
                    "AND table_name = 'task_set_generation_gc_cursors'",
                ),
            )
        }
        assert columns == {"name": "NO", "next_sweep": "NO"}

        next_sweep = conn.execute(
            text(
                "INSERT INTO task_set_generation_gc_cursors (name) "
                "VALUES ('live-generation-gc') RETURNING next_sweep",
            ),
        ).scalar_one()
        assert next_sweep == 0

        with pytest.raises(IntegrityError):
            conn.execute(
                text(
                    "INSERT INTO task_set_generation_gc_cursors (name) "
                    "VALUES ('unexpected-authority')",
                ),
            )
    engine.dispose()

    _alembic(postgres_url_at_0062, "downgrade", "0062")
    engine = create_engine(postgres_url_at_0062)
    with engine.begin() as conn:
        table_exists = conn.execute(
            text(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' "
                "AND table_name = 'task_set_generation_gc_cursors'"
                ")",
            ),
        ).scalar_one()
    engine.dispose()
    assert table_exists is False
