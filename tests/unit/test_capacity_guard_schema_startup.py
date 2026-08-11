"""Startup checks for the independent protected-capacity revision table."""

from __future__ import annotations

import inspect

import pytest

from loom_capacity_guard import schema_startup


@pytest.mark.asyncio
@pytest.mark.parametrize("actual", [None, "guard_0000"])
async def test_capacity_guard_schema_rejects_missing_or_behind_revision(
    actual: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def database_revision(_engine: object) -> str | None:
        return actual

    monkeypatch.setattr(schema_startup, "_database_guard_revision", database_revision)
    monkeypatch.setattr(schema_startup, "_guard_head", lambda: "guard_0001")

    with pytest.raises(schema_startup.CapacityGuardSchemaNotAtHeadError) as caught:
        await schema_startup.assert_capacity_guard_schema_at_head(object())

    message = str(caught.value)
    assert f"observed {actual or '<missing>'}" in message
    assert "expected guard_0001" in message
    assert "LOOM_CAPACITY_GUARD_DB_URL" in message
    assert "capacity_guard_migrations/alembic.ini" in message
    assert "LOOM_DB_URL" not in message
    assert "LOOM_CP_DB_URL" not in message
    assert "LOOM_CAPACITY_DB_URL" not in message
    assert "migrations/alembic.ini" not in message.replace(
        "capacity_guard_migrations/alembic.ini", ""
    )


@pytest.mark.asyncio
async def test_capacity_guard_schema_accepts_exact_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def database_revision(_engine: object) -> str | None:
        return "guard_0001"

    monkeypatch.setattr(schema_startup, "_database_guard_revision", database_revision)
    monkeypatch.setattr(schema_startup, "_guard_head", lambda: "guard_0001")

    assert await schema_startup.assert_capacity_guard_schema_at_head(object()) == 1


def test_capacity_guard_schema_uses_only_its_qualified_revision_table() -> None:
    source = inspect.getsource(schema_startup)
    assert "loom_capacity_guard.capacity_guard_alembic_version" in source
    assert "FROM alembic_version" not in source
