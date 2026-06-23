from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_assert_schema_at_head_accepts_matching_revisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.db import schema_startup

    async def _current_heads(_engine: object) -> set[str]:
        return {"0036_provider_model_preflight"}

    monkeypatch.setattr(
        schema_startup,
        "_database_current_heads",
        _current_heads,
    )
    monkeypatch.setattr(
        schema_startup,
        "_script_heads",
        lambda _alembic_ini=None: {"0036_provider_model_preflight"},
    )

    checked = await schema_startup.assert_schema_at_head(
        object(),
        db_url_env_var="LOOM_SVC_DB_URL",
    )

    assert checked == 1


@pytest.mark.asyncio
async def test_assert_schema_at_head_rejects_stale_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.db import schema_startup

    async def _current_heads(_engine: object) -> set[str]:
        return {"0035_provider_rate_cards"}

    monkeypatch.setattr(
        schema_startup,
        "_database_current_heads",
        _current_heads,
    )
    monkeypatch.setattr(
        schema_startup,
        "_script_heads",
        lambda _alembic_ini=None: {"0036_provider_model_preflight"},
    )

    with pytest.raises(schema_startup.SchemaNotAtHeadError) as excinfo:
        await schema_startup.assert_schema_at_head(
            object(),
            db_url_env_var="LOOM_SVC_DB_URL",
        )

    message = str(excinfo.value)
    assert "Database schema is not at Alembic head" in message
    assert "current revision(s): 0035_provider_rate_cards" in message
    assert "head revision(s): 0036_provider_model_preflight" in message
    assert "LOOM_SVC_DB_URL" in message
    assert "alembic -c migrations/alembic.ini upgrade head" in message


@pytest.mark.asyncio
async def test_assert_schema_at_head_reports_unversioned_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.db import schema_startup

    async def _current_heads(_engine: object) -> set[str]:
        return set()

    monkeypatch.setattr(
        schema_startup,
        "_database_current_heads",
        _current_heads,
    )
    monkeypatch.setattr(
        schema_startup,
        "_script_heads",
        lambda _alembic_ini=None: {"0036_provider_model_preflight"},
    )

    with pytest.raises(schema_startup.SchemaNotAtHeadError) as excinfo:
        await schema_startup.assert_schema_at_head(
            object(),
            db_url_env_var="LOOM_CP_DB_URL",
        )

    message = str(excinfo.value)
    assert "current revision(s): <none>" in message
    assert "LOOM_CP_DB_URL" in message
