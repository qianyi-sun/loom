from __future__ import annotations

import os
from importlib.resources import files

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine

from agentic_data_platform.persistence.database import create_database_engine, normalize_database_url


def alembic_config(*, database_url: str | None = None, connection=None) -> Config:
    config = Config()
    script_location = files("agentic_data_platform.persistence").joinpath("alembic")
    config.set_main_option("script_location", str(script_location))
    config.set_main_option("sqlalchemy.url", normalize_database_url(database_url or "sqlite+pysqlite:///:memory:"))
    if connection is not None:
        config.attributes["connection"] = connection
    return config


def upgrade_database(engine_or_url: Engine | str | None = None, revision: str = "head") -> None:
    if isinstance(engine_or_url, Engine):
        with engine_or_url.begin() as connection:
            command.upgrade(alembic_config(connection=connection), revision)
        return

    database_url = engine_or_url or os.environ.get("DATABASE_URL", "")
    engine = create_database_engine(database_url)
    try:
        with engine.begin() as connection:
            command.upgrade(alembic_config(database_url=database_url, connection=connection), revision)
    finally:
        engine.dispose()


def main() -> None:
    upgrade_database()


if __name__ == "__main__":
    main()
