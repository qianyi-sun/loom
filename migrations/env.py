"""Alembic environment — reads DB URL from LOOM_DB_URL env var."""

from __future__ import annotations

import os
from logging.config import fileConfig
from typing import Any
from uuid import uuid4

from alembic import context
from sqlalchemy import engine_from_config, pool

from loom.db import schema  # noqa: F401  (registers models with Base.metadata)
from loom.db.base import Base


def _assert_direct_postgres_connection(connectable: Any) -> None:
    """Alembic MUST run direct-to-Postgres (#609).

    Under pgbouncer transaction mode, session-scoped operations
    (SET LOCAL, autocommit-DDL, session-scoped advisory locks) silently
    break because backends are recycled between transactions. Today's
    Loom migrations work on either path, but the invariant is worth
    enforcing at every migration run so future migration authors can't
    accidentally depend on session semantics that only work on one path.

    Probe uses its OWN short-lived connection so Alembic's connection
    is untouched. SET a synthetic application_name, commit, read it
    back. Under session-preserving semantics the value persists; under
    pgbouncer transaction mode the next statement lands on a different
    backend with the default application_name.
    """
    marker = f"alembic-probe-{uuid4()}"
    with connectable.connect() as probe_conn:
        probe_conn.exec_driver_sql(f"SET application_name = '{marker}'")
        probe_conn.commit()
        actual = probe_conn.exec_driver_sql("SHOW application_name").scalar()
    if actual != marker:
        raise RuntimeError(
            f"Alembic connection is not direct-to-Postgres. "
            f"application_name did not persist across commit "
            f"(saw {actual!r}, expected {marker!r}). This means the "
            f"connection routes through pgbouncer transaction mode, "
            f"which silently breaks session-scoped operations that "
            f"Alembic migrations may depend on. "
            f"Fix: point LOOM_DB_URL at loom-postgres:5432 direct, "
            f"not loom-pgbouncer:6432."
        )

target_metadata = Base.metadata

# The block below only executes when Alembic drives this file directly.
# Importing the module in unit tests (e.g. to access _assert_direct_postgres_connection)
# does NOT trigger it, because context.config is unavailable outside an Alembic run.
if hasattr(context, "config"):
    config = context.config
    if config.config_file_name is not None:
        # disable_existing_loggers=False prevents fileConfig from disabling
        # loggers configured before alembic runs — notably pytest's caplog
        # attachment to project loggers like `loom_worker.trial_runner`.
        fileConfig(config.config_file_name, disable_existing_loggers=False)

    configured_url = config.get_main_option("sqlalchemy.url")
    db_url: str | None = configured_url or os.environ.get("LOOM_DB_URL")
    if not db_url:
        raise RuntimeError(
            "sqlalchemy.url or LOOM_DB_URL must be set to run migrations",
        )
    config.set_main_option("sqlalchemy.url", db_url)

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
        _assert_direct_postgres_connection(connectable)
        with connectable.connect() as connection:
            context.configure(connection=connection, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()

    if context.is_offline_mode():
        run_migrations_offline()
    else:
        run_migrations_online()
