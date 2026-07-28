"""Migration 0075: durable shared-capacity lease retirement state."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def _cfg(url: str) -> Config:
    repo_root = Path(__file__).resolve().parents[2]
    config = Config(str(repo_root / "migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(repo_root / "migrations"))
    config.set_main_option("sqlalchemy.url", url)
    return config


def test_0075_adds_and_reversibly_drops_capacity_lease_state(
    isolated_migration_postgres_url: str,
) -> None:
    config = _cfg(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)

    command.downgrade(config, "0074")
    columns = {
        column["name"] for column in inspect(engine).get_columns("worker_pool_autoscaler_policies")
    }
    assert "capacity_lease_state" not in columns

    command.upgrade(config, "0075")
    columns = {
        column["name"] for column in inspect(engine).get_columns("worker_pool_autoscaler_policies")
    }
    assert "capacity_lease_state" in columns

    command.downgrade(config, "0074")
    columns = {
        column["name"] for column in inspect(engine).get_columns("worker_pool_autoscaler_policies")
    }
    assert "capacity_lease_state" not in columns

    command.upgrade(config, "head")
