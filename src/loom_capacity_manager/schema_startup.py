"""Fail-closed startup validation for the independent capacity schema."""

from __future__ import annotations

from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from loom_capacity_manager.migration_resources import (
    resolve_capacity_migration_resources,
)


class CapacitySchemaNotAtHeadError(RuntimeError):
    """Raised when a database is absent, wrong, or not at capacity head."""


def _capacity_head() -> str:
    resources = resolve_capacity_migration_resources()
    config = AlembicConfig(str(resources.config))
    config.set_main_option("script_location", str(resources.scripts))
    head = ScriptDirectory.from_config(config).get_current_head()
    if head is None:  # pragma: no cover - packaging corruption
        raise CapacitySchemaNotAtHeadError("capacity migration tree has no head revision")
    return head


def _remediation(actual: str | None, expected: str) -> CapacitySchemaNotAtHeadError:
    observed = actual if actual is not None else "<missing>"
    return CapacitySchemaNotAtHeadError(
        "capacity management database schema is not at head "
        f"(observed {observed}, expected {expected}); run the installed migration "
        "command with the reviewed bootstrap identity: "
        "python -m loom_capacity_manager.migrate "
        "--db-url-file <owner-only-database-url-file> "
        "--expected-authority-incarnation <reviewed-non-nil-uuid>"
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
