"""Narrow transaction/JSON-row contract for admitted application handoffs.

Both a real psycopg connection and the installed peer-psql transport implement
this interface. It neither opens a connection nor selects an authority/target.
"""

from contextlib import AbstractContextManager
from typing import Protocol

from psycopg import sql
from psycopg.pq import TransactionStatus


class ApplicationDatabaseRows(Protocol):
    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class ApplicationDatabaseInfo(Protocol):
    @property
    def server_version(self) -> int: ...

    @property
    def transaction_status(self) -> TransactionStatus: ...


class ApplicationDatabaseConnection(Protocol):
    @property
    def info(self) -> ApplicationDatabaseInfo: ...

    def execute(self, query: str | sql.SQL | sql.Composed) -> ApplicationDatabaseRows: ...

    def transaction(self) -> AbstractContextManager[object]: ...


def application_sql(template: str, *values: object) -> sql.Composed:
    """Compose source-code templates with driver-quoted literals, not peer interpolation."""
    return sql.SQL(template).format(*(sql.Literal(value) for value in values))
