"""Arm the journal-admitted permanent guard migrator with a fixed owner lease.

The installed caller admits the completed handoff and guard schema, records both
owner OIDs and this migrator OID, retains the original guard and fixes credentials
and expiry before calling. These helpers never discover/create roles, normalize
unknown grants, renew a lease or establish successor authority themselves.
Resumed generations must retire before another arm, as enforced by their journal.
"""

from __future__ import annotations

from datetime import datetime

from psycopg import sql

from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_guard_migrator_retirement import (
    _identity,
    _memberships,
    _require_roles,
    _sealed,
)
from loom.application_migrator_provision import _transaction
from loom.application_password import matches_application_scram


def arm_application_guard_migrator(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationOwnerSuccessor, password: str, expires_at: datetime,
) -> None:
    """Grant only the two saved owners; reconcile the same committed credential."""
    _identity(identity, target, provisioner_role, coordination_guard)
    assert identity.guard_owner is not None
    if (not isinstance(password, str) or not 1 <= len(password) <= 1024 or "\x00" in password
            or not isinstance(expires_at, datetime) or expires_at.utcoffset() is None):
        raise ValueError("application guard migrator credential or expiry is invalid")
    expiry = expires_at.isoformat()
    with _transaction(connection, target, coordination_guard, provisioner_role):
        if connection.execute(application_sql(
            "SELECT {}::timestamptz>clock_timestamp() AND {}::timestamptz<=clock_timestamp()+interval '1 hour'",
            expiry, expiry,
        )).fetchone() != (True,):
            raise RuntimeError("application guard migrator expiry is not bounded and live")
        _require_roles(connection, target, identity, allow_migrator_login=True)
        memberships = _memberships(connection, target, identity)
        row = connection.execute(application_sql(
            "SELECT rolcanlogin,rolpassword,rolvaliduntil={}::timestamptz FROM pg_catalog.pg_authid WHERE oid={}",
            expiry, identity.role_oid,
        )).fetchone()
        if row is None:
            raise RuntimeError("application guard migrator saved identity disappeared")
        if row[0]:
            if row[2] is not True or not matches_application_scram(password, row[1]):
                raise RuntimeError("application guard migrator armed credential changed")
            if not memberships or connection.execute(application_sql(
                "SELECT has_database_privilege({},{}::oid,'CONNECT')", identity.role_oid, target.database_oid,
            )).fetchone() != (True,):
                raise RuntimeError("application guard migrator armed authority changed")
            return
        _sealed(connection, target, identity)
        if memberships:
            raise RuntimeError("application guard migrator previous memberships have not retired")
        connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={}) "
            "OR EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE database={} OR owner={}) "
            "OR EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
            "AND classid='pg_catalog.pg_database'::regclass AND mode='RowExclusiveLock')",
            identity.role_oid, target.database, identity.role_name,
        )).fetchone() != (False,):
            raise RuntimeError("application guard migrator previous sessions have not retired")
        connection.execute(sql.SQL("GRANT {},{} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
            sql.Identifier(target.successor_role), sql.Identifier(identity.guard_owner.role_name), sql.Identifier(identity.role_name)))
        connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(target.database), sql.Identifier(identity.role_name)))
        connection.execute("SET LOCAL password_encryption='scram-sha-256'")
        connection.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {} VALID UNTIL {}").format(
            sql.Identifier(identity.role_name), sql.Literal(password), sql.Literal(expiry)))
        _require_roles(connection, target, identity, allow_migrator_login=True)
        if not _memberships(connection, target, identity):
            raise RuntimeError("application guard migrator owner grants did not commit")


def seal_application_guard_migrator(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationOwnerSuccessor,
) -> None:
    """Stop new logins for the saved migrator, preserving its permanent role."""
    _identity(identity, target, provisioner_role, coordination_guard)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        _require_roles(connection, target, identity, allow_migrator_login=True)
        _memberships(connection, target, identity)
        connection.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(identity.role_name)))
        _sealed(connection, target, identity)
