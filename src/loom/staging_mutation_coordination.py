"""Cross-process mutation coordination for protected staging operations."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager

from sqlalchemy import Engine, text

STAGING_MUTATION_ADVISORY_LOCK_KEY = 5_498_691_230_183_247_727
STAGING_MUTATION_TRY_LOCK_SQL = f"SELECT pg_try_advisory_lock({STAGING_MUTATION_ADVISORY_LOCK_KEY})"
STAGING_MUTATION_UNLOCK_SQL = f"SELECT pg_advisory_unlock({STAGING_MUTATION_ADVISORY_LOCK_KEY})"


def hold_staging_mutation_guard(engine: Engine) -> AbstractContextManager[bool]:
    """Try the fixed session lock and hold its connection for the context."""

    return _hold_staging_mutation_guard(engine)


@contextmanager
def _hold_staging_mutation_guard(engine: Engine) -> Iterator[bool]:
    connection = engine.connect()
    acquired = False
    try:
        result = connection.execute(text(STAGING_MUTATION_TRY_LOCK_SQL)).scalar_one()
        if type(result) is not bool:
            raise RuntimeError("staging mutation guard acquisition result is invalid")
        acquired = result
        yield acquired
    finally:
        try:
            if acquired:
                released = connection.execute(text(STAGING_MUTATION_UNLOCK_SQL)).scalar_one()
                if released is not True:
                    raise RuntimeError("staging mutation guard unlock result is invalid")
        finally:
            connection.close()


__all__ = [
    "STAGING_MUTATION_ADVISORY_LOCK_KEY",
    "STAGING_MUTATION_TRY_LOCK_SQL",
    "STAGING_MUTATION_UNLOCK_SQL",
    "hold_staging_mutation_guard",
]
