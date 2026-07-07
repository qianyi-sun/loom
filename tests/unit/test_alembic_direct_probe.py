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


def test_probe_passes_on_direct_connection() -> None:
    from migrations.env import _assert_direct_postgres_connection

    conn = _FakeConn(app_name_persists=True)
    _assert_direct_postgres_connection(conn)  # no raise


def test_probe_raises_on_pgbouncer_transaction_mode() -> None:
    from migrations.env import _assert_direct_postgres_connection

    conn = _FakeConn(app_name_persists=False)
    with pytest.raises(RuntimeError, match="not direct-to-Postgres"):
        _assert_direct_postgres_connection(conn)


def test_probe_error_message_mentions_fix() -> None:
    """The error message should tell the operator how to fix — point
    LOOM_DB_URL at loom-postgres:5432 direct, not loom-pgbouncer:6432."""
    from migrations.env import _assert_direct_postgres_connection

    conn = _FakeConn(app_name_persists=False)
    with pytest.raises(RuntimeError) as excinfo:
        _assert_direct_postgres_connection(conn)
    msg = str(excinfo.value).lower()
    assert "loom-postgres" in msg or "direct" in msg
    assert "pgbouncer" in msg
