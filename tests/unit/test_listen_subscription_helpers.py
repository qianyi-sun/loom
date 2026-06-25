"""#5 Slice 3e — pure-function helpers around the LISTEN subscription.

The route-level integration (SSE inner loop actually waking on
NOTIFY end-to-end) is covered by
`tests/integration/test_service_trajectory.py`; here we pin the
DSN translation that the route relies on so the psycopg connection
opens against the right URL shape."""

from __future__ import annotations

from loom_service.routes.trajectory import _sqla_url_to_psycopg_dsn


def test_sqla_psycopg_prefix_stripped() -> None:
    """The SQLAlchemy DSN prefix is `postgresql+psycopg://`; psycopg
    itself wants the bare `postgresql://` scheme."""
    out = _sqla_url_to_psycopg_dsn(
        "postgresql+psycopg://loom:loom@localhost:5432/loom",
    )
    assert out == "postgresql://loom:loom@localhost:5432/loom"


def test_sqla_asyncpg_prefix_also_stripped() -> None:
    """Some services configure the async engine with the asyncpg
    driver — same idea, different prefix."""
    out = _sqla_url_to_psycopg_dsn(
        "postgresql+asyncpg://u:p@db/loom",
    )
    assert out == "postgresql://u:p@db/loom"


def test_bare_postgresql_url_passthrough() -> None:
    """Already-bare URLs round-trip unchanged."""
    out = _sqla_url_to_psycopg_dsn("postgresql://u:p@db/loom")
    assert out == "postgresql://u:p@db/loom"


def test_other_scheme_passthrough() -> None:
    """Defensive: an unfamiliar scheme passes through rather than
    getting mangled. psycopg will reject it cleanly downstream."""
    out = _sqla_url_to_psycopg_dsn("sqlite:///x.db")
    assert out == "sqlite:///x.db"
