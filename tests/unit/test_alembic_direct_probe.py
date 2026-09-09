from __future__ import annotations

import re

import pytest


class _FakeCursor:
    def __init__(self, app_name_persists: bool) -> None:
        self._persists = app_name_persists
        self._app_name = "psql"
        self._read_after_commit = False

    def execute(self, sql: str) -> None:
        m = re.match(r"SET application_name = '([^']+)'", sql)
        if m:
            self._app_name = m.group(1)

    def scalar(self) -> str:
        if self._read_after_commit and not self._persists:
            return "unknown"
        return self._app_name


class _FakeConn:
    """Minimal fake matching SQLAlchemy Connection surface used by the probe."""

    def __init__(self, app_name_persists: bool) -> None:
        self._persists = app_name_persists
        self._cursor = _FakeCursor(app_name_persists)
        self._committed = False

    def exec_driver_sql(self, sql: str) -> _FakeCursor:
        self._cursor.execute(sql)
        # Signal to scalar() that we're reading after commit
        if self._committed and sql.startswith("SHOW"):
            self._cursor._read_after_commit = True
        return self._cursor

    def commit(self) -> None:
        self._committed = True
        # Simulate pgbouncer transaction-mode: backend rotation clears
        # session state.
        if not self._persists:
            self._cursor._app_name = "unknown"


class _FakeConnectable:
    """Fake engine — supports `with connectable.connect() as conn`."""

    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def connect(self) -> _FakeCtx:
        return _FakeCtx(self._conn)


class _FakeCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeConn:
        return self._conn

    def __exit__(self, *_args: object) -> None:
        return None


def test_probe_passes_on_direct_connection() -> None:
    from migrations.env import _assert_direct_postgres_connection

    conn = _FakeConn(app_name_persists=True)
    _assert_direct_postgres_connection(_FakeConnectable(conn))  # no raise


def test_probe_raises_on_pgbouncer_transaction_mode() -> None:
    from migrations.env import _assert_direct_postgres_connection

    conn = _FakeConn(app_name_persists=False)
    with pytest.raises(RuntimeError, match="not direct-to-Postgres"):
        _assert_direct_postgres_connection(_FakeConnectable(conn))


def test_probe_error_message_mentions_fix() -> None:
    """The error message should tell the operator how to fix — point
    LOOM_DB_URL at loom-postgres:5432 direct, not loom-pgbouncer:6432."""
    from migrations.env import _assert_direct_postgres_connection

    conn = _FakeConn(app_name_persists=False)
    with pytest.raises(RuntimeError) as excinfo:
        _assert_direct_postgres_connection(_FakeConnectable(conn))
    msg = str(excinfo.value).lower()
    assert "loom-postgres" in msg or "direct" in msg
    assert "pgbouncer" in msg


@pytest.mark.parametrize("source", ["environment", "alembic_config"])
def test_tls_database_url_reaches_engine_without_interpolation(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    import runpy
    from pathlib import Path

    import sqlalchemy
    from alembic import context
    from alembic.config import Config
    from sqlalchemy.engine import URL

    # Both the credential and verify-full CA path legitimately contain percent
    # escapes. Alembic's INI boundary must preserve the original URL exactly.
    url = URL.create(
        "postgresql+psycopg",
        username="loom_test",
        password="test%password@with/slash",
        host="loom-postgres.loom-nebius-platform.svc",
        database="loom",
        query={"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"},
    ).render_as_string(hide_password=False)
    config = Config()
    if source == "environment":
        monkeypatch.setenv("LOOM_DB_URL", url)
    else:
        config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    monkeypatch.setattr(context, "config", config, raising=False)
    monkeypatch.setattr(context, "is_offline_mode", lambda: False)

    class ReachedEngineError(Exception):
        pass

    def engine_from_config(settings: dict, **kwargs: object) -> None:
        assert settings["sqlalchemy.url"] == url
        restored = sqlalchemy.make_url(settings["sqlalchemy.url"])
        assert restored.password == "test%password@with/slash"
        assert restored.query["sslmode"] == "verify-full"
        assert restored.query["sslrootcert"] == "/var/run/loom-db/ca.crt"
        raise ReachedEngineError

    monkeypatch.setattr(sqlalchemy, "engine_from_config", engine_from_config)
    with pytest.raises(ReachedEngineError):
        runpy.run_path(str(Path(__file__).resolve().parents[2] / "migrations/env.py"))
