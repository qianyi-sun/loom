"""Startup validation for the Alembic-managed database schema."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from loom.startup_retry import (
    DEFAULT_STARTUP_RETRY_CONFIG,
    StartupRetryConfig,
    retry_startup_dependency,
)


class SchemaNotAtHeadError(RuntimeError):
    """Raised when the database Alembic revision is behind repository code."""


def _default_alembic_ini() -> Path:
    repo_root = Path(__file__).resolve().parents[3]
    candidate = repo_root / "migrations" / "alembic.ini"
    if candidate.is_file():
        return candidate
    return Path("migrations/alembic.ini")


def _script_heads(alembic_ini: Path | None = None) -> set[str]:
    ini = alembic_ini or _default_alembic_ini()
    if not ini.is_file():
        raise RuntimeError(f"Alembic config not found at {ini}")
    config = Config(str(ini))
    config.set_main_option("path_separator", "os")
    config.set_main_option("script_location", str(ini.parent))
    return set(ScriptDirectory.from_config(config).get_heads())


async def _database_current_heads(engine: AsyncEngine) -> set[str]:
    async with engine.connect() as conn:
        return await conn.run_sync(_current_heads_sync)


def _current_heads_sync(connection: Connection) -> set[str]:
    context = MigrationContext.configure(connection)
    return set(context.get_current_heads())


def _format_revisions(revisions: set[str]) -> str:
    if not revisions:
        return "<none>"
    return ", ".join(sorted(revisions))


async def assert_schema_at_head(
    engine: AsyncEngine,
    *,
    db_url_env_var: str,
    alembic_ini: Path | None = None,
    retry_config: StartupRetryConfig = DEFAULT_STARTUP_RETRY_CONFIG,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    """Fail fast when DB migrations have not been applied.

    The local `loom service up` wrapper still owns dev auto-migration.
    Long-running service processes only validate; production migration remains
    an explicit operator action.
    """
    current_heads = await retry_startup_dependency(
        lambda: _database_current_heads(engine),
        operation_name=f"database schema startup check ({db_url_env_var})",
        config=retry_config,
        sleep=sleep,
    )
    expected_heads = _script_heads(alembic_ini)
    if current_heads == expected_heads:
        return len(current_heads)

    raise SchemaNotAtHeadError(
        "Database schema is not at Alembic head; refusing to start. "
        f"current revision(s): {_format_revisions(current_heads)}; "
        f"head revision(s): {_format_revisions(expected_heads)}. "
        "Apply pending migrations before starting this service: "
        f'LOOM_DB_URL="${db_url_env_var}" '
        "alembic -c migrations/alembic.ini upgrade head"
    )
