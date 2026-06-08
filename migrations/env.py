"""Alembic environment — reads DB URL from LOOM_DB_URL env var."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from loom.db import schema  # noqa: F401  (registers models with Base.metadata)
from loom.db.base import Base

config = context.config
if config.config_file_name is not None:
    # disable_existing_loggers=False prevents fileConfig from disabling
    # loggers configured before alembic runs — notably pytest's caplog
    # attachment to project loggers like `loom_worker.trial_runner`.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

db_url = os.environ.get("LOOM_DB_URL")
if not db_url:
    raise RuntimeError("LOOM_DB_URL must be set to run migrations")
config.set_main_option("sqlalchemy.url", db_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
