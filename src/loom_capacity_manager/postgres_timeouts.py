"""Fixed PostgreSQL bounds for capacity schema and authority bootstrap work."""

from __future__ import annotations

POSTGRES_CONNECT_TIMEOUT_SECONDS = 10
POSTGRES_LOCK_TIMEOUT_MILLISECONDS = 30_000
POSTGRES_STATEMENT_TIMEOUT_MILLISECONDS = 300_000


def capacity_migration_connect_args() -> dict[str, int | str]:
    """Return non-overridable libpq connection and transaction wait bounds."""

    return {
        "connect_timeout": POSTGRES_CONNECT_TIMEOUT_SECONDS,
        "options": (
            f"-c lock_timeout={POSTGRES_LOCK_TIMEOUT_MILLISECONDS} "
            f"-c statement_timeout={POSTGRES_STATEMENT_TIMEOUT_MILLISECONDS}"
        ),
    }


__all__ = [
    "POSTGRES_CONNECT_TIMEOUT_SECONDS",
    "POSTGRES_LOCK_TIMEOUT_MILLISECONDS",
    "POSTGRES_STATEMENT_TIMEOUT_MILLISECONDS",
    "capacity_migration_connect_args",
]
