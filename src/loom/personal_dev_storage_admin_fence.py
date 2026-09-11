"""Permanent retirement for PostgreSQL cluster DDL and guarded target transactions.

Each guarded backend fences its own effects; no lock covers an external service.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql

from loom.dev_instance import DevInstanceIdentity
from loom.personal_dev_incarnation_storage import validate_personal_dev_storage_identity
from loom_capacity_manager.contracts import canonical_bytes


class PersonalDevStorageRetiredError(RuntimeError):
    """Bound storage is retired or its permanent authority is not authentic."""


def _comment(identity: DevInstanceIdentity, state: str) -> str:
    assert identity.storage_binding is not None
    return json.dumps({
        "schema_version": 1, "kind": "loom-personal-dev-storage-retirement",
        "binding": json.loads(canonical_bytes(identity.storage_binding)), "state": state,
    }, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _maintenance_url(admin_url: str) -> str:
    parsed = urlsplit(admin_url.replace("postgresql+psycopg://", "postgresql://", 1))
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise ValueError("storage administration requires a PostgreSQL URL")
    return urlunsplit((parsed.scheme, parsed.netloc, "/postgres", parsed.query, ""))


async def _read_guard(connection: psycopg.AsyncConnection[Any], name: str) -> tuple[Any, ...] | None:
    result = await connection.execute(
        "SELECT oid, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolinherit, "
        "rolreplication, rolbypassrls, rolpassword IS NULL, rolvaliduntil IS NULL, "
        "NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole = r.oid), "
        "pg_catalog.shobj_description(oid, 'pg_authid'), rolconnlimit "
        "FROM pg_catalog.pg_authid r WHERE rolname = %s", (name,),
    )
    row = await result.fetchone()
    return None if row is None else tuple(row)


async def _validated_guard(
    connection: psycopg.AsyncConnection[Any], name: str, identity: DevInstanceIdentity,
) -> tuple[Any, ...]:
    row = await _read_guard(connection, name)
    if row is None:
        raise PersonalDevStorageRetiredError("storage retirement guard is unavailable")
    memberships = await connection.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE roleid = %s OR member = %s)",
        (row[0], row[0]),
    )
    if row[1:11] != (False, False, False, False, False, False, False, True, True, True) or (
        await memberships.fetchone()
    ) != (False,) or row[12] != -1 or row[11] not in {_comment(identity, "active"), _comment(identity, "retired")}:
        raise PersonalDevStorageRetiredError("storage retirement guard is not authentic")
    return row


async def fence_storage_target_transaction(
    connection: psycopg.AsyncConnection[Any], identity: DevInstanceIdentity,
) -> None:
    """Fence the actual privileged target transaction, never a proxy backend.

    Call before effects or SET ROLE, inside a READ COMMITTED transaction. The
    shared-catalog row lock lasts until that transaction commits or rolls back.
    Restricted migrator sessions instead rely on NOLOGIN plus verified drain.
    """
    if identity.storage_binding is None:
        if identity.storage_incarnation is not None:
            raise PersonalDevStorageRetiredError("storage administration requires its complete binding")
        return
    validate_personal_dev_storage_identity(identity)
    if connection.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
        raise PersonalDevStorageRetiredError("storage target guard requires an active transaction")
    observed = await connection.execute("SELECT current_database(), current_setting('transaction_isolation')")
    if await observed.fetchone() != (identity.database, "read committed"):
        raise PersonalDevStorageRetiredError("storage target transaction database or isolation is invalid")
    assert identity.storage_incarnation is not None
    name = f"ld_fence_{identity.storage_incarnation.hex}"
    async with asyncio.timeout(30):
        await connection.execute("SELECT oid FROM pg_catalog.pg_authid WHERE rolname = %s FOR SHARE", (name,))
    # Lock and read must be separate statements: COMMENT updates pg_shdescription,
    # and a snapshot obtained before a lock wait could otherwise see active.
    row = await _validated_guard(connection, name, identity)
    if row[11] != _comment(identity, "active"):
        raise PersonalDevStorageRetiredError("storage incarnation is permanently retired")


async def _guard(
    connection: psycopg.AsyncConnection[Any], identity: DevInstanceIdentity, *, retire: bool,
    allow_retired: bool = False,
) -> None:
    assert identity.storage_incarnation is not None
    name = f"ld_fence_{identity.storage_incarnation.hex}"
    key = int.from_bytes(hashlib.sha256(
        b"loom-personal-dev-storage-retirement-v1\0" + identity.storage_incarnation.bytes,
    ).digest()[:8], signed=True)
    async with asyncio.timeout(30):
        await connection.execute("SELECT pg_catalog.pg_advisory_lock(%s)", (key,))
    # The session lock deliberately outlives these transactions: CREATE/DROP
    # DATABASE must run in autocommit, on this same protected backend.
    async with connection.transaction():
        await connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        async with asyncio.timeout(30):
            await connection.execute("SELECT oid FROM pg_catalog.pg_authid WHERE rolname = %s FOR UPDATE", (name,))
        row = await _read_guard(connection, name)
        if row is None:
            await connection.execute(sql.SQL(
                "CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
            ).format(sql.Identifier(name)))
            await connection.execute(sql.SQL("COMMENT ON ROLE {} IS {}").format(
                sql.Identifier(name), sql.Literal(_comment(identity, "retired" if retire else "active")),
            ))
        row = await _validated_guard(connection, name, identity)
        if row[11] == _comment(identity, "retired") and not (retire or allow_retired):
            raise PersonalDevStorageRetiredError("storage incarnation is permanently retired")
        if retire and row[11] != _comment(identity, "retired"):
            await connection.execute(sql.SQL("COMMENT ON ROLE {} IS {}").format(
                sql.Identifier(name), sql.Literal(_comment(identity, "retired")),
            ))
    # Committed retirement survives interruption before NOLOGIN or DROP. A
    # failed cleanup can resume, but no later provisioner can reopen the guard.


@asynccontextmanager
async def storage_admin_connection(
    admin_url: str, identity: DevInstanceIdentity, *, action: Literal["provision", "retire", "cleanup", "restrict"],
) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    if identity.storage_incarnation is not None and identity.storage_binding is None:
        raise PersonalDevStorageRetiredError("storage administration requires its complete binding")
    if identity.storage_binding is None:
        connect_url = admin_url.replace("postgresql+psycopg://", "postgresql://", 1)
    else:
        validate_personal_dev_storage_identity(identity)
        connect_url = _maintenance_url(admin_url)
    async with await psycopg.AsyncConnection.connect(connect_url, autocommit=True) as connection:
        if identity.storage_binding is not None:
            database = await connection.execute("SELECT pg_catalog.current_database()")
            if await database.fetchone() != ("postgres",):
                raise PersonalDevStorageRetiredError("storage administration is not on the maintenance database")
            await _guard(connection, identity, retire=action in {"retire", "cleanup"}, allow_retired=action == "restrict")
        yield connection
    # Closing this connection releases its session lock only with its backend;
    # never hand the lock to a caller that executes cluster DDL elsewhere.
