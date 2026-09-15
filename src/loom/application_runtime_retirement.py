"""Retire sealed runtime sessions without removing their role or credential.

Internal cutover phase: the caller retains the original target and credential,
validates separated schema authority, stops workloads, closes admission, and
excludes role/DDL/admission writers. This catalog-only maintenance operation does
not establish those exclusions or certify retirement of other fleet writers.
Backend signals cannot roll back; uncertainty always leaves admission closed.
"""

from __future__ import annotations

import re

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    _maintenance_transaction,
    _read_target,
    _require_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_password import matches_application_scram


class ApplicationRuntimeRetirementError(RuntimeError):
    """The saved runtime or its closed admission boundary changed."""


def retire_application_runtime_sessions(
    connection: ApplicationDatabaseConnection, *,
    target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard,
    provisioner_role: str,
    password: str,
) -> None:
    """Signal only exact target-runtime backends under the actual saved guard.

    Replay revalidates all boundaries and observes disappearance. The runtime's
    name/OID, original SCRAM verifier, memberships and grants remain untouched.
    Foreign role sessions, startup locks and prepared work refuse before signals.
    This is not proof that administrator SQL or other fleet processes retired.
    """
    if (
        not isinstance(coordination_guard, ApplicationDatabaseCoordinationGuard)
        or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", provisioner_role) is None
        or provisioner_role in {target.owner_role, target.successor_role}
        or coordination_guard.role_oid in {target.owner_oid, target.successor_oid}
    ):
        raise ApplicationRuntimeRetirementError("application runtime retirement identity is invalid")
    with _maintenance_transaction(connection, database=target.database, provisioner_role=provisioner_role):
        if connection.execute("SELECT current_database()='postgres'").fetchone() != (True,):
            raise ApplicationRuntimeRetirementError("application runtime requires postgres maintenance peer")
        verifier = _require_boundary(connection, target, coordination_guard, password)
        _require_no_foreign_work(connection, target)
        sessions = connection.execute(application_sql(
            "SELECT pid,backend_start::text FROM pg_catalog.pg_stat_activity "
            "WHERE usesysid={} ORDER BY pid", target.owner_oid,
        )).fetchall()
        for pid, started_at in sessions:
            # Recheck immediately before every irreversible signal, including
            # guard loss between two backends. Never adopt a reused backend PID.
            if _require_boundary(connection, target, coordination_guard, password) != verifier:
                raise ApplicationRuntimeRetirementError("application runtime verifier changed")
            _require_no_foreign_work(connection, target)
            result = connection.execute(application_sql(
                "SELECT pg_catalog.pg_terminate_backend(pid,1000) "
                "FROM pg_catalog.pg_stat_activity WHERE pid={} AND "
                "backend_start={}::timestamptz AND usesysid={} AND datid={} "
                "AND usename={} AND backend_type='client backend'",
                pid, started_at, target.owner_oid, target.database_oid, target.owner_role,
            )).fetchall()
            if result not in ([], [(True,)]):
                raise ApplicationRuntimeRetirementError("application runtime session did not retire")
        if _require_boundary(connection, target, coordination_guard, password) != verifier:
            raise ApplicationRuntimeRetirementError("application runtime verifier changed")
        _require_no_foreign_work(connection, target)
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={})",
            target.owner_oid,
        )).fetchone() != (False,):
            raise ApplicationRuntimeRetirementError("application runtime sessions remain")


def _require_boundary(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    guard: ApplicationDatabaseCoordinationGuard, password: str,
) -> str:
    observed, allowed, owner = _read_target(connection, database=target.database,
        owner_role=target.owner_role, successor_role=target.successor_role, runtime_password=password)
    if observed != target or allowed or owner != target.successor_oid:
        raise ApplicationRuntimeRetirementError("application runtime identity or closed admission changed")
    row = connection.execute(application_sql(
        "SELECT rolpassword,rolvaliduntil IS NULL OR rolvaliduntil='infinity'::timestamptz "
        "FROM pg_catalog.pg_authid WHERE oid={}", target.owner_oid,
    )).fetchone()
    if row is None or not isinstance(row[0], str) or row[1] is not True or not matches_application_scram(password, row[0]):
        raise ApplicationRuntimeRetirementError("application runtime credential changed")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole IN ({},{})) "
        "OR EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend WHERE "
        "refclassid='pg_catalog.pg_authid'::regclass AND refobjid IN ({},{}) "
        "AND NOT (dbid={} OR dbid=0 AND classid='pg_catalog.pg_database'::regclass AND objid={}))",
        target.owner_oid, target.successor_oid, target.owner_oid, target.successor_oid,
        target.database_oid, target.database_oid,
    )).fetchone() != (False,):
        raise ApplicationRuntimeRetirementError("application runtime has foreign role authority")
    _require_coordination_guard(connection, target, guard)
    # Startup may hold an authenticated cluster-wide role before publishing its
    # pg_stat_activity row. Observe locks FIRST, then clear the statistics cache.
    if connection.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
        "AND classid='pg_catalog.pg_database'::regclass AND mode='RowExclusiveLock')"
    ).fetchone() != (False,):
        raise ApplicationRuntimeRetirementError("application runtime database startup is still pending")
    return row[0]


def _require_no_foreign_work(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
) -> None:
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE database={} OR owner={})",
        target.database, target.owner_role,
    )).fetchone() != (False,):
        raise ApplicationRuntimeRetirementError("application runtime has pending prepared work")
    connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={} "
        "AND (datid IS DISTINCT FROM {} OR backend_type IS DISTINCT FROM 'client backend' "
        "OR usename IS DISTINCT FROM {} OR backend_start IS NULL))",
        target.owner_oid, target.database_oid, target.owner_role,
    )).fetchone() != (False,):
        raise ApplicationRuntimeRetirementError("application runtime has foreign or unknown sessions")
