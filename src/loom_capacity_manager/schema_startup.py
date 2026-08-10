"""Fail-closed startup validation for the independent capacity schema."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine


class CapacitySchemaNotAtHeadError(RuntimeError):
    """Raised when a database is absent, wrong, or not at capacity head."""


def _capacity_head() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / "capacity_migrations" / "alembic.ini"
    config = AlembicConfig(str(config_path))
    config.set_main_option("script_location", str(repo_root / "capacity_migrations"))
    head = ScriptDirectory.from_config(config).get_current_head()
    if head is None:  # pragma: no cover - packaging corruption
        raise CapacitySchemaNotAtHeadError("capacity migration tree has no head revision")
    return head


def _remediation(actual: str | None, expected: str) -> CapacitySchemaNotAtHeadError:
    observed = actual if actual is not None else "<missing>"
    return CapacitySchemaNotAtHeadError(
        "capacity management database schema is not at head "
        f"(observed {observed}, expected {expected}); export the owner-only "
        "runtime URL as LOOM_CAPACITY_DB_URL and run "
        "alembic -c capacity_migrations/alembic.ini upgrade head"
    )


async def assert_capacity_schema_at_head(engine: AsyncEngine) -> int:
    """Return numeric schema generation only for the exact capacity head."""

    expected = _capacity_head()
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text("SELECT version_num FROM alembic_version"))
            actual = result.scalar_one_or_none()
    except SQLAlchemyError as exc:
        raise _remediation(None, expected) from exc
    if actual != expected:
        raise _remediation(actual, expected)
    suffix = expected.removeprefix("capacity_")
    if not suffix.isdigit():  # pragma: no cover - migration naming invariant
        raise CapacitySchemaNotAtHeadError(
            f"capacity migration head {expected!r} has no numeric generation"
        )
    return int(suffix)


__all__ = ["CapacitySchemaNotAtHeadError", "assert_capacity_schema_at_head"]
