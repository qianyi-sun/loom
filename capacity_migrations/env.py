"""Alembic environment for only the global capacity management database."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from loom_capacity_manager.models import Base
from loom_capacity_manager.postgres_timeouts import capacity_migration_connect_args

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

db_url = os.environ.get("LOOM_CAPACITY_DB_URL")
target_metadata = Base.metadata


def _required_database_url() -> str:
    if not db_url:
        raise RuntimeError("LOOM_CAPACITY_DB_URL must be set to run capacity migrations")
    config.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))
    return db_url


def run_migrations_offline() -> None:
    context.configure(
        url=_required_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        context.configure(
            connection=supplied_connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()
        return

    _required_database_url()
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=capacity_migration_connect_args(),
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
