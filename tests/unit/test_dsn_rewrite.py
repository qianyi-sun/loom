from __future__ import annotations

import pytest

from loom_config.bootstrap import _rewrite_dsn_host_port


def test_rewrites_host_and_port_preserving_credentials() -> None:
    src = "postgresql+psycopg://loom:s3cret@loom-postgres:5432/loom"
    got = _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)
    assert got == "postgresql+psycopg://loom:s3cret@loom-pgbouncer:6432/loom"


def test_adds_port_when_input_has_no_port() -> None:
    src = "postgresql+psycopg://loom:pw@loom-postgres/loom"
    got = _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)
    assert got == "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"


def test_preserves_query_parameters() -> None:
    src = "postgresql+psycopg://loom:pw@loom-postgres:5432/loom?sslmode=require&connect_timeout=5"
    got = _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)
    assert got == (
        "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"
        "?sslmode=require&connect_timeout=5"
    )


def test_preserves_scheme_variants() -> None:
    for scheme in ("postgresql", "postgres", "postgresql+psycopg"):
        src = f"{scheme}://loom:pw@loom-postgres:5432/loom"
        got = _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)
        assert got.startswith(f"{scheme}://")


def test_preserves_database_name_with_path_chars() -> None:
    src = "postgresql+psycopg://loom:pw@loom-postgres:5432/loom_staging"
    got = _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)
    assert got.endswith("/loom_staging")


def test_raises_on_missing_userinfo() -> None:
    src = "postgresql://loom-postgres:5432/loom"
    with pytest.raises(ValueError, match="userinfo"):
        _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)


def test_raises_on_missing_password() -> None:
    src = "postgresql://user@loom-postgres:5432/loom"
    with pytest.raises(ValueError, match="userinfo"):
        _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)


def test_raises_on_missing_host() -> None:
    src = "postgresql+psycopg://loom:pw@:5432/loom"
    with pytest.raises(ValueError, match="host"):
        _rewrite_dsn_host_port(src, host="loom-pgbouncer", port=6432)


def test_raises_on_malformed_input() -> None:
    with pytest.raises(ValueError):
        _rewrite_dsn_host_port("not-a-url", host="loom-pgbouncer", port=6432)
