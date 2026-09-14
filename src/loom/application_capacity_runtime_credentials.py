"""Credential phases for saved ordinary capacity roles, never owner membership.

The installed bootstrap admits and journals all role OIDs, seed credentials,
generation lease and the original guard before arming. It independently checks
the exact desired schema/configuration and retires the elevated migrator before
promoting these ordinary runtime credentials to their intended durable lifetime.
These helpers neither grant object privileges nor create/reset/adopt roles.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from psycopg import sql

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_migrator_provision import _transaction
from loom.application_password import matches_application_scram

_EXECUTOR = "loom_cap_staging_executor"
_LOGIN_ROLES = {"loom_cap_staging_agent", "loom_cap_staging_observer", "loom_cap_staging_runtime"}


def arm_application_capacity_runtime_credentials(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    role_oids: Mapping[str, int], passwords: Mapping[str, str], expires_at: datetime,
) -> None:
    """Arm the fixed seed in a new journaled generation; preserve durable logins."""
    _identity(target, coordination_guard, role_oids, passwords)
    if not isinstance(expires_at, datetime) or expires_at.utcoffset() is None:
        raise ValueError("application capacity credential expiry is invalid")
    expiry = expires_at.isoformat()
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        if connection.execute(application_sql(
            "SELECT {}::timestamptz>clock_timestamp() AND {}::timestamptz<=clock_timestamp()+interval '1 hour'",
            expiry, expiry,
        )).fetchone() != (True,):
            raise RuntimeError("application capacity credential expiry is not bounded and live")
        rows = _roles(connection, role_oids)
        _credentials(rows, passwords, require_login=False)
        connection.execute("SET LOCAL password_encryption='scram-sha-256'")
        for _oid, role, login, _password, infinite, exact_expiry in rows:
            if role == _EXECUTOR or (login and (infinite or datetime.fromisoformat(str(exact_expiry)) == expires_at)):
                continue
            if login:
                connection.execute(sql.SQL("ALTER ROLE {} VALID UNTIL {}").format(sql.Identifier(str(role)), sql.Literal(expiry)))
            else:
                connection.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {} VALID UNTIL {}").format(
                    sql.Identifier(str(role)), sql.Literal(passwords[str(role)]), sql.Literal(expiry)))
        _credentials(_roles(connection, role_oids), passwords, require_login=True)


def finalize_application_capacity_runtime_credentials(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    role_oids: Mapping[str, int], passwords: Mapping[str, str],
) -> None:
    """Promote only the same ordinary credentials after exact bootstrap admission."""
    _identity(target, coordination_guard, role_oids, passwords)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        rows = _roles(connection, role_oids)
        _credentials(rows, passwords, require_login=True)
        for _oid, role, _login, _password, infinite, _expiry in rows:
            if role != _EXECUTOR and not infinite:
                connection.execute(sql.SQL("ALTER ROLE {} VALID UNTIL 'infinity'").format(sql.Identifier(str(role))))
        rows = _roles(connection, role_oids)
        _credentials(rows, passwords, require_login=True)
        if any(not row[4] for row in rows if row[1] != _EXECUTOR):
            raise RuntimeError("application capacity runtime credential durability changed")


def _identity(target: ApplicationDatabaseAdmissionTarget, guard: ApplicationDatabaseCoordinationGuard,
              roles: Mapping[str, int], passwords: Mapping[str, str]) -> None:
    if (set(roles) != {*_LOGIN_ROLES, _EXECUTOR} or set(passwords) != _LOGIN_ROLES
            or any(type(oid) is not int or not 0 < oid < 2**32 for oid in roles.values())
            or len({*roles.values(), target.owner_oid, target.successor_oid, guard.role_oid}) != 7
            or any(not isinstance(value, str) or not 32 <= len(value) <= 1024
                   or any(not 0x21 <= ord(c) <= 0x7e for c in value) for value in passwords.values())):
        raise ValueError("application capacity runtime credential identity is invalid")


def _roles(connection: ApplicationDatabaseConnection, roles: Mapping[str, int]) -> list[tuple[object, ...]]:
    rows = connection.execute(application_sql(
        "SELECT oid::bigint,rolname,rolcanlogin,rolpassword,rolvaliduntil='infinity'::timestamptz,rolvaliduntil::text "
        "FROM pg_catalog.pg_authid WHERE rolname=ANY({}::text[]) AND NOT (rolsuper OR rolinherit OR rolcreatedb "
        "OR rolcreaterole OR rolreplication OR rolbypassrls) AND NOT EXISTS "
        "(SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=pg_authid.oid) ORDER BY rolname", sorted(roles),
    )).fetchall()
    if len(rows) != 4 or any(not isinstance(row[1], str) or roles.get(row[1]) != row[0] for row in rows):
        raise RuntimeError("application capacity saved runtime role identity changed")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=ANY({}::oid[]) OR roleid=ANY({}::oid[]))",
        list(roles.values()), list(roles.values()),
    )).fetchone() != (False,):
        raise RuntimeError("application capacity runtime memberships changed")
    return rows


def _credentials(rows: list[tuple[object, ...]], passwords: Mapping[str, str], *, require_login: bool) -> None:
    for _oid, role, login, password, _infinite, expiry in rows:
        if role == _EXECUTOR:
            if login or password is not None:
                raise RuntimeError("application capacity executor credential is not sealed")
        elif login:
            if expiry is None or not matches_application_scram(passwords[str(role)], password):
                raise RuntimeError("application capacity original runtime credential changed")
        elif require_login or password is not None:
            raise RuntimeError("application capacity runtime credential is not armed or sealed")
