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
