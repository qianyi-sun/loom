"""Fail-closed startup validation for the protected admission schema."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

_VERSION_QUERY = "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"


class CapacityGuardSchemaNotAtHeadError(RuntimeError):
    """The independently owned protected schema is missing or behind."""


def _guard_head() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / "capacity_guard_migrations" / "alembic.ini"
    config = AlembicConfig(str(config_path))
    config.set_main_option("script_location", str(repo_root / "capacity_guard_migrations"))
    head = ScriptDirectory.from_config(config).get_current_head()
    if head is None:  # pragma: no cover - packaging corruption
        raise CapacityGuardSchemaNotAtHeadError(
            "protected capacity migration tree has no head revision"
        )
    return head


def capacity_guard_schema_head() -> tuple[str, int]:
    """Return the packaged protected migration head and its numeric generation."""

    head = _guard_head()
    suffix = head.removeprefix("guard_")
    if not suffix.isdigit():  # pragma: no cover - migration naming invariant
        raise CapacityGuardSchemaNotAtHeadError(
            f"protected capacity migration head {head!r} has no numeric generation"
        )
    return head, int(suffix)


async def _database_guard_revision(engine: AsyncEngine) -> str | None:
    async with engine.connect() as connection:
        return (await connection.execute(text(_VERSION_QUERY))).scalar_one_or_none()


def _remediation(
    actual: str | None,
    expected: str,
) -> CapacityGuardSchemaNotAtHeadError:
    observed = actual if actual is not None else "<missing>"
    return CapacityGuardSchemaNotAtHeadError(
        "protected capacity database schema is not at head "
        f"(observed {observed}, expected {expected}); export the owner-only "
        "runtime URL as LOOM_CAPACITY_GUARD_DB_URL and run "
        "alembic -c capacity_guard_migrations/alembic.ini upgrade head"
    )


async def assert_capacity_guard_schema_at_head(engine: AsyncEngine) -> int:
    """Return the numeric protected-schema generation only at exact head."""

    expected, generation = capacity_guard_schema_head()
    try:
        actual = await _database_guard_revision(engine)
    except SQLAlchemyError as exc:
        raise _remediation(None, expected) from exc
    if actual != expected:
        raise _remediation(actual, expected)
    return generation


__all__ = [
    "CapacityGuardSchemaNotAtHeadError",
    "assert_capacity_guard_schema_at_head",
    "capacity_guard_schema_head",
]
