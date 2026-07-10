"""Migration 0065 reserves the deployment-only TaskSet canary identity."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

_SYSTEM_CANARY_TEAM_ID = "2c9506e1-7d5e-4b49-b532-4b8f0a3f5ea9"
_SYSTEM_CANARY_TEAM_NAME = "loom-system-taskset-fence-canary"


def _cfg(postgres_url: str) -> Config:
    repo_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo_root / "migrations" / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


@pytest.fixture(scope="module")
def postgres_url_at_0064() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        postgres_url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = postgres_url
        command.upgrade(_cfg(postgres_url), "0064")
        yield postgres_url


def test_upgrade_reserves_the_fixed_canary_team_and_quota(
    postgres_url_at_0064: str,
) -> None:
    """The canary identity is independent of a mutable ``admin`` Team."""
    cfg = _cfg(postgres_url_at_0064)
    engine = create_engine(postgres_url_at_0064)
    try:
        command.upgrade(cfg, "head")
        with engine.connect() as conn:
            team = conn.execute(
                text("SELECT id::text, name, disabled_at FROM teams WHERE id = :team_id"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            ).one_or_none()
            quota_team_id = conn.execute(
                text("SELECT team_id::text FROM team_quotas WHERE team_id = :team_id"),
                {"team_id": _SYSTEM_CANARY_TEAM_ID},
            ).scalar_one_or_none()
    finally:
        engine.dispose()

    assert team == (_SYSTEM_CANARY_TEAM_ID, _SYSTEM_CANARY_TEAM_NAME, None)
    assert quota_team_id == _SYSTEM_CANARY_TEAM_ID
