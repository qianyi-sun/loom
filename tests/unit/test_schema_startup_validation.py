from __future__ import annotations

import socket

import pytest
from sqlalchemy.exc import OperationalError


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


def test_startup_retry_treats_invalidated_db_connections_as_retryable() -> None:
    from loom.startup_retry import is_retryable_startup_exception

    exc = OperationalError(
        "select version_num from alembic_version",
        {},
        RuntimeError("connection dropped"),
        connection_invalidated=True,
    )

    assert is_retryable_startup_exception(exc)


@pytest.mark.asyncio
async def test_assert_schema_at_head_retries_transient_dns_before_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.db import schema_startup
    from loom.startup_retry import StartupRetryConfig

    calls = 0

    async def _current_heads(_engine: object) -> set[str]:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OperationalError(
                "select version_num from alembic_version",
                {},
                socket.gaierror(-3, "Temporary failure in name resolution"),
            )
        return {"0036_provider_model_preflight"}

    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

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
        db_url_env_var="LOOM_CP_DB_URL",
        retry_config=StartupRetryConfig(
            max_attempts=4,
            base_backoff_sec=0.1,
            max_backoff_sec=1.0,
            budget_sec=30.0,
            jitter_sec=0.0,
        ),
        sleep=_sleep,
    )

    assert checked == 1
    assert calls == 3
    assert sleeps == [0.1, 0.2]


@pytest.mark.asyncio
async def test_assert_schema_at_head_does_not_retry_schema_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.db import schema_startup
    from loom.startup_retry import StartupRetryConfig

    calls = 0

    async def _current_heads(_engine: object) -> set[str]:
        nonlocal calls
        calls += 1
        return {"0035_provider_rate_cards"}

    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

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

    with pytest.raises(schema_startup.SchemaNotAtHeadError):
        await schema_startup.assert_schema_at_head(
            object(),
            db_url_env_var="LOOM_CP_DB_URL",
            retry_config=StartupRetryConfig(
                max_attempts=4,
                base_backoff_sec=0.1,
                max_backoff_sec=1.0,
                budget_sec=30.0,
                jitter_sec=0.0,
            ),
            sleep=_sleep,
        )

    assert calls == 1
    assert sleeps == []
