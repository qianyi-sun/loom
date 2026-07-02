"""Migration 0054 — tasks.task_set_id (#242 sub-plan 3)."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
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
def postgres_url_at_0053() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        _alembic(url, "upgrade", "0053")
        yield url


def test_upgrade_adds_task_set_id_column(postgres_url_at_0053: str) -> None:
    _alembic(postgres_url_at_0053, "upgrade", "0054")
    engine = create_engine(postgres_url_at_0053)
    with engine.begin() as conn:
        cols = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'tasks' AND column_name = 'task_set_id'",
            ),
        ).scalars().all()
    engine.dispose()
    assert "task_set_id" in cols
    _alembic(postgres_url_at_0053, "downgrade", "0053")


def test_benchmark_or_taskset_check(postgres_url_at_0053: str) -> None:
    _alembic(postgres_url_at_0053, "upgrade", "0054")
    engine = create_engine(postgres_url_at_0053)
    with engine.begin() as conn:
        constraints = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid = 'tasks'::regclass",
                ),
            )
        }
    engine.dispose()
    assert "tasks_benchmark_or_taskset_check" in constraints
    _alembic(postgres_url_at_0053, "downgrade", "0053")
