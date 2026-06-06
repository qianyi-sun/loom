"""Top-level test fixtures live here. Per-package fixtures live in
nested conftest.py files.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """Session-scoped Postgres 16 container with Loom's Alembic schema applied.

    Tests that need a real DB import this fixture; the container is created
    once per pytest session (~1.5s overhead) and shared.
    """
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        repo_root = Path(__file__).resolve().parents[1]
        subprocess.run(
            [sys.executable, "-m", "alembic",
             "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root, check=True,
        )
        yield url
