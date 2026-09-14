"""Closed-admission cleanup for the exact journaled post-handoff migrator.

The enclosing lifecycle journals closure/reopen intent and the maintenance peer,
retains its original guard, admits SQL/primary inputs, and stops the owned Job.
These phases never capture new authority, signal foreign sessions or rotate the
runtime credential. A lost peer can recover through the maintenance database;
the caller must first establish that the previous peer cannot still commit.
"""

from __future__ import annotations

from psycopg import sql

from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_migrator_provision import _require_role, _transaction, _validate_identity
from loom.application_migrator_retirement import _require_memberships
from loom.application_password import matches_application_scram


def close_application_migrator_admission(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationOwnerSuccessor,
) -> None:
    """Commit closure before retiring any already-authenticated owner sessions."""
    _validate_identity(identity, target, provisioner_role)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        _require_maintenance(connection, target)
        _require_role(connection, target, identity)
        if connection.execute(application_sql(
            "SELECT NOT rolcanlogin AND rolpassword IS NULL FROM pg_catalog.pg_authid WHERE oid={}", identity.role_oid,
        )).fetchone() != (True,):
            raise RuntimeError("application migrator admission requires the sealed role")
        if _allowed(connection, target):
            connection.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
        if _allowed(connection, target):
            raise RuntimeError("application migrator admission did not close")


def reopen_application_migrator_admission(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationOwnerSuccessor, runtime_password: str,
) -> None:
    """Reopen only after the saved role and all its owner sessions have retired."""
    _validate_identity(identity, target, provisioner_role)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        _require_maintenance(connection, target)
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_authid WHERE oid={} OR rolname={})",
            identity.role_oid, identity.role_name,
        )).fetchone() != (False,):
            raise RuntimeError("application migrator saved role has not retired")
        if _require_memberships(connection, target, identity.role_oid):
            raise RuntimeError("application migrator owner memberships have not retired")
        if connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
            "AND classid='pg_catalog.pg_database'::regclass AND mode='RowExclusiveLock')"
        ).fetchone() != (False,):
            raise RuntimeError("application migrator admission has pending database startup")
        connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={})", identity.role_oid,
        )).fetchone() != (False,):
            raise RuntimeError("application migrator owner sessions have not retired")
        row = connection.execute(application_sql(
            "SELECT rolname={} AND rolcanlogin AND NOT (rolinherit OR rolsuper OR rolcreatedb "
            "OR rolcreaterole OR rolreplication OR rolbypassrls) AND (rolvaliduntil IS NULL "
            "OR rolvaliduntil='infinity'::timestamptz) AND NOT EXISTS "
            "(SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=pg_authid.oid),rolpassword "
            "FROM pg_catalog.pg_authid WHERE oid={}", target.owner_role, target.owner_oid,
        )).fetchone()
        if (not isinstance(runtime_password, str) or not 1 <= len(runtime_password) <= 1024
                or row is None or row[0] is not True or not matches_application_scram(runtime_password, row[1])):
            raise RuntimeError("application migrator original runtime credential changed")
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member={} OR roleid={})",
            target.owner_oid, target.owner_oid,
        )).fetchone() != (False,):
            raise RuntimeError("application migrator runtime memberships changed")
        if not _allowed(connection, target):
            connection.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(target.database)))
        if not _allowed(connection, target):
            raise RuntimeError("application migrator admission did not reopen")


def _require_maintenance(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget) -> None:
    if connection.execute(application_sql(
        "SELECT current_database()='postgres' AND current_database()<>{}", target.database,
    )).fetchone() != (True,):
        raise RuntimeError("application migrator admission requires separate maintenance peer")


def _allowed(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget) -> bool:
    row = connection.execute(application_sql(
        "SELECT datallowconn FROM pg_catalog.pg_database WHERE oid={}", target.database_oid,
    )).fetchone()
    if row is None or type(row[0]) is not bool:
        raise RuntimeError("application migrator admission database disappeared")
    return row[0]
