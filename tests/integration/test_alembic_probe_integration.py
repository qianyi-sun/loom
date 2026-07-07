"""Integration tests for Alembic's direct-Postgres probe (#609).

Verifies _assert_direct_postgres_connection against real Postgres and
demonstrates the pgbouncer transaction-mode session-state properties
that motivate the probe's existence.
"""
from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


@pytest.mark.integration
def test_probe_passes_on_direct_connection(pgbouncer_stack: dict[str, str]) -> None:
    """The probe must not raise when connecting directly to Postgres."""
    from migrations.env import _assert_direct_postgres_connection

    engine = create_engine(pgbouncer_stack["direct_url"])
    with engine.connect() as conn:
        _assert_direct_postgres_connection(conn)  # no raise
    engine.dispose()


@pytest.mark.integration
def test_application_name_does_not_persist_across_new_connections_through_pgbouncer(
    pgbouncer_stack: dict[str, str],
) -> None:
    """Through pgbouncer transaction mode, session state (e.g. application_name)
    is NOT preserved when a brand-new TCP connection is opened after the previous
    one closes. This underpins the Alembic probe's invariant: if you open a fresh
    connection to pgbouncer you cannot rely on session state from a prior connection.

    This test validates the real failure mode the probe is designed to catch:
    Alembic migrations must use a connection that is direct-to-Postgres so that
    SET ... is guaranteed to persist across the commit within that connection.
    """
    marker = f"alembic-integ-{uuid.uuid4()}"

    engine = create_engine(
        pgbouncer_stack["pool_url"],
        poolclass=NullPool,
        connect_args={"prepare_threshold": None},
    )

    # First connection: SET application_name and commit → backend released to pool.
    with engine.connect() as conn:
        conn.exec_driver_sql(f"SET application_name = '{marker}'")
        conn.commit()

    # Second fresh connection: gets a potentially different backend with clean state.
    with engine.connect() as conn2:
        actual = conn2.exec_driver_sql("SHOW application_name").scalar()

    # A new psycopg connection through pgbouncer must NOT see prior session state.
    assert actual != marker, (
        f"application_name {actual!r} unexpectedly matched {marker!r} on a new "
        f"connection through pgbouncer. This would indicate session state is "
        f"leaking across backend connections — pgbouncer may not be in "
        f"transaction mode."
    )

    engine.dispose()


@pytest.mark.integration
def test_probe_raises_on_pgbouncer_connection(pgbouncer_stack: dict[str, str]) -> None:
    """The probe detects pgbouncer transaction mode when the SET and SHOW
    statements execute on separate backend connections.

    We reproduce this by passing a fake connection whose exec_driver_sql
    is split across two real NullPool acquisitions — the first handles SET,
    and the second (a fresh TCP connection through pgbouncer) handles SHOW.
    Between the two acquisitions the backend is returned to the pool and its
    session state is reset, so application_name won't match the marker.

    This is the exact failure mode the probe guards against in production:
    if migrations used a connection pool with connection reuse through pgbouncer,
    SET application_name (or any session-scoped operation) could silently vanish.
    """
    from migrations.env import _assert_direct_postgres_connection

    engine = create_engine(
        pgbouncer_stack["pool_url"],
        poolclass=NullPool,
        connect_args={"prepare_threshold": None},
    )

    # Build a thin wrapper that routes SET to conn_a and SHOW to conn_b.
    # conn_a is closed before conn_b opens, ensuring the backend is recycled.
    conn_a = engine.connect()
    conn_a.__enter__()

    # Use Any to avoid mypy complaints about the duck-typed split connection.
    conn_b: Any = None

    class _SplitConn:
        """Routes SET to conn_a and SHOW to a fresh conn_b opened after conn_a
        commits and closes, simulating the backend-rotation failure mode."""

        def exec_driver_sql(self, sql: str) -> Any:
            nonlocal conn_b
            if "SET application_name" in sql:
                return conn_a.exec_driver_sql(sql)
            # SHOW: close conn_a first so the backend goes back to the pool.
            conn_a.__exit__(None, None, None)
            if conn_b is None:
                conn_b = engine.connect().__enter__()
            return conn_b.exec_driver_sql(sql)

        def commit(self) -> None:
            conn_a.commit()

    split = _SplitConn()
    try:
        with pytest.raises(RuntimeError, match="not direct-to-Postgres"):
            _assert_direct_postgres_connection(split)
    finally:
        if conn_b is not None:
            conn_b.__exit__(None, None, None)
        engine.dispose()
