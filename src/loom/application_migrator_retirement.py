"""Retire one saved transient DDL login under closed database admission.

The protected caller must have journaled this role's original name/OID before
arming it, committed NOLOGIN/PASSWORD NULL, and stopped its owned migration Job.
It must serialize administrator, database-admission, role and Job writers for the
whole operation. This does not create that exclusion, close/reopen admission,
remove a Kubernetes Secret, or publish component completion.

Revoking membership alone leaves already SET ROLE sessions privileged. Check
startup locks before fresh session statistics, terminate only exact saved-role
sessions in the target database, then remove its CONNECT grant and role. No
DROP OWNED, foreign-session signalling or ownership transfer is permitted.
"""

from __future__ import annotations

import re

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    _require_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql


class ApplicationMigratorRetirementError(RuntimeError):
    """Saved transient authority or its closed-admission boundary changed."""


def retire_application_migrator(
    connection: ApplicationDatabaseConnection, *,
    target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard,
    provisioner_role: str,
    migrator_role: str,
    migrator_oid: int,
) -> None:
    """Retire the exact sealed login, preserving the original guard and owner.

    Use the original maintenance peer in postgres. A lost commit acknowledgement
    may be reconciled by the saved OID's absence; a recreated same-name role is
    never adopted. Backend signals cannot roll back, so every admission check
    precedes signalling. Any SQL/guard uncertainty leaves admission closed.
    """
    if (
        any(re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None
            for role in (provisioner_role, migrator_role))
        or migrator_role in {target.owner_role, target.successor_role, provisioner_role,
                             "loom_rollout_readonly"}
        or type(migrator_oid) is not int or not 0 < migrator_oid < 2**32
        or migrator_oid in {target.owner_oid, target.successor_oid, coordination_guard.role_oid}
        or connection.info.transaction_status != TransactionStatus.IDLE
        or connection.info.server_version // 10000 not in {16, 17}
    ):
        raise ApplicationMigratorRetirementError("application migrator retirement identity is invalid")
    with connection.transaction():
        connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        connection.execute("SELECT pg_catalog.set_config('lock_timeout','1s',true)")
        connection.execute("SELECT pg_catalog.set_config('statement_timeout','30s',true)")
        if connection.execute(application_sql(
            "SELECT current_database()='postgres' AND current_user=session_user "
            "AND current_user={} AND rolsuper "
            "AND current_setting('transaction_isolation')='read committed' "
            "FROM pg_catalog.pg_roles WHERE rolname=current_user", provisioner_role,
        )).fetchone() != (True,):
            raise ApplicationMigratorRetirementError("application migrator requires protected maintenance peer")
        _require_target(connection, target, coordination_guard)
        rows = connection.execute(application_sql(
            "SELECT oid::bigint,rolname,NOT (rolcanlogin OR rolinherit OR rolsuper "
            "OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls) "
            "AND rolpassword IS NULL AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=pg_authid.oid) FROM pg_catalog.pg_authid "
            "WHERE oid={} OR rolname={}", migrator_oid, migrator_role,
        )).fetchall()
        if rows not in ([], [(migrator_oid, migrator_role, True)]):
            raise ApplicationMigratorRetirementError("application migrator login is not the exact sealed role")
        _require_memberships(connection, target, migrator_oid)
        # Transient sessions must create objects via SET ROLE. Unexpected owned
        # objects or grants are evidence of drift, never an invitation to delete.
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend "
            "WHERE refclassid='pg_catalog.pg_authid'::regclass AND refobjid={} "
            "AND NOT (dbid=0 AND classid='pg_catalog.pg_database'::regclass "
            "AND objid={} AND deptype='a'))", migrator_oid, target.database_oid,
        )).fetchone() != (False,):
            raise ApplicationMigratorRetirementError("application migrator has unexpected object authority")
        _retire_sessions(connection, target=target, coordination_guard=coordination_guard,
            migrator_role=migrator_role, migrator_oid=migrator_oid)
        if rows:
            memberships = _require_memberships(connection, target, migrator_oid)
            if memberships:
                connection.execute(sql.SQL("REVOKE {} FROM {}").format(
                    sql.Identifier(target.successor_role), sql.Identifier(migrator_role),
                ))
            connection.execute(sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                sql.Identifier(target.database), sql.Identifier(migrator_role),
            ))
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(migrator_role)))
        _require_target(connection, target, coordination_guard)


def _require_target(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    guard: ApplicationDatabaseCoordinationGuard,
) -> None:
    if connection.execute(application_sql(
        "SELECT s.system_identifier::text={},d.datname={},d.datdba::bigint={},"
        "NOT d.datallowconn FROM pg_catalog.pg_database d "
        "CROSS JOIN pg_catalog.pg_control_system() s WHERE d.oid={}",
        target.system_identifier, target.database, target.successor_oid, target.database_oid,
    )).fetchone() != (True, True, True, True):
        raise ApplicationMigratorRetirementError("application migrator database identity or closed admission changed")
    if connection.execute(application_sql(
        "SELECT rolname={} AND NOT (rolcanlogin OR rolinherit OR rolsuper OR rolcreatedb "
        "OR rolcreaterole OR rolreplication OR rolbypassrls) AND rolpassword IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=pg_authid.oid) FROM pg_catalog.pg_authid WHERE oid={}",
        target.successor_role, target.successor_oid,
    )).fetchone() != (True,):
        raise ApplicationMigratorRetirementError("application migrator owner is not sealed")
    _require_coordination_guard(connection, target, guard)
    # Roles are cluster-wide. An already-authenticated startup in another
    # database can still carry this login before publishing its session row.
    # Refuse every pending database startup before trusting role-wide statistics.
    if connection.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
        "AND classid='pg_catalog.pg_database'::regclass AND mode='RowExclusiveLock')"
    ).fetchone() != (False,):
        raise ApplicationMigratorRetirementError("application migrator database startup is still pending")


def _require_memberships(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    migrator_oid: int,
) -> list[tuple[object, ...]]:
    memberships = connection.execute(application_sql(
        "SELECT roleid::bigint,member::bigint,admin_option,inherit_option,set_option "
        "FROM pg_catalog.pg_auth_members WHERE roleid IN ({},{}) OR member IN ({},{})",
        migrator_oid, target.successor_oid, migrator_oid, target.successor_oid,
    )).fetchall()
    if memberships not in ([], [(target.successor_oid, migrator_oid, False, False, True)]):
        raise ApplicationMigratorRetirementError("application migrator owner memberships changed")
    return memberships


def _retire_sessions(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, migrator_role: str, migrator_oid: int,
) -> None:
    _require_target(connection, target, coordination_guard)
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts "
        "WHERE database={} OR owner={})", target.database, migrator_role,
    )).fetchone() != (False,):
        raise ApplicationMigratorRetirementError("application migrator has pending prepared work")
    connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity "
        "WHERE usesysid={} AND (datid IS DISTINCT FROM {} "
        "OR backend_type<>'client backend' OR usename IS DISTINCT FROM {}))",
        migrator_oid, target.database_oid, migrator_role,
    )).fetchone() != (False,):
        raise ApplicationMigratorRetirementError("application migrator has foreign or unknown sessions")
    sessions = connection.execute(application_sql(
        "SELECT pid,backend_start::text FROM pg_catalog.pg_stat_activity "
        "WHERE usesysid={} ORDER BY pid", migrator_oid,
    )).fetchall()
    _require_target(connection, target, coordination_guard)
    for pid, started_at in sessions:
        # The saved start time and role OID prevent signalling a reused PID.
        result = connection.execute(application_sql(
            "SELECT pg_catalog.pg_terminate_backend(pid,1000) "
            "FROM pg_catalog.pg_stat_activity WHERE pid={} AND "
            "backend_start={}::timestamptz AND usesysid={} AND datid={}",
            pid, started_at, migrator_oid, target.database_oid,
        )).fetchall()
        if result not in ([], [(True,)]):
            raise ApplicationMigratorRetirementError("application migrator session did not retire")
    connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={})",
        migrator_oid,
    )).fetchone() != (False,):
        raise ApplicationMigratorRetirementError("application migrator sessions remain")
    _require_target(connection, target, coordination_guard)
