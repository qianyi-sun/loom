"""Catalog-only recovery of recorded migrator roles and privileged peer identities.

The enclosing protected lifecycle admits its original SQL/cluster inputs and
serializes privileged writers. These checks never terminate privileged peers,
adopt an unrecorded role, arm a credential, or reopen database admission.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_migrator_provision import _require_role, _transaction, _validate_identity


def observe_application_migration_backend(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str, maintenance: bool,
) -> ApplicationDatabaseHandoffBackend:
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=maintenance):
        if connection.execute(application_sql(
            "SELECT current_database()={}", "postgres" if maintenance else target.database,
        )).fetchone() != (True,):
            raise RuntimeError("application migration peer database changed")
        row = connection.execute(
            "SELECT a.pid,a.backend_start::text,s.system_identifier::text,pg_catalog.pg_postmaster_start_time()::text,a.datid::bigint "
            "FROM pg_catalog.pg_stat_activity a CROSS JOIN pg_catalog.pg_control_system() s WHERE a.pid=pg_backend_pid()"
        ).fetchone()
        if (row is None or len(row) != 5 or any(type(row[i]) is not int for i in (0, 4))
                or any(not isinstance(row[i], str) for i in (1, 2, 3))):
            raise RuntimeError("application migration peer identity is invalid")
        backend = ApplicationDatabaseHandoffBackend(int(str(row[0])), _utc(str(row[1])), str(row[2]),
                                                     _utc(str(row[3])), int(str(row[4])))
        if (backend.system_identifier != target.system_identifier or backend.pid == coordination_guard.backend.pid
                or backend.server_started_at != coordination_guard.backend.server_started_at):
            raise RuntimeError("application migration peer server or guard changed")
        return backend


def require_application_migration_peer_retired(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    backend: ApplicationDatabaseHandoffBackend,
) -> None:
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        if (type(backend) is not ApplicationDatabaseHandoffBackend
                or backend.system_identifier != target.system_identifier
                or backend.server_started_at != coordination_guard.backend.server_started_at
                or backend.pid == coordination_guard.backend.pid):
            raise RuntimeError("application migration previous peer authority changed")
        connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid={} AND backend_start={}::timestamptz)",
            backend.pid, backend.started_at,
        )).fetchone() != (False,):
            raise RuntimeError("application migration previous peer still exists")
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE owner={} OR database={})",
            provisioner_role, target.database,
        )).fetchone() != (False,):
            raise RuntimeError("application migration previous peer has unretired prepared work")


def observe_application_migrator_role(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    migrator_role: str, migrator_oid: int | None,
) -> bool:
    identity = ApplicationOwnerSuccessor(migrator_role, 1 if migrator_oid is None else migrator_oid)
    _validate_identity(identity, target, provisioner_role, check_oid=migrator_oid is not None)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        rows = connection.execute(application_sql(
            "SELECT oid::bigint,rolname FROM pg_catalog.pg_authid WHERE rolname={} OR oid={}",
            migrator_role, 0 if migrator_oid is None else migrator_oid,
        )).fetchall()
        if rows:
            if migrator_oid is None or rows != [(migrator_oid, migrator_role)]:
                raise RuntimeError("application migration found an unrecorded or replaced role")
            _require_role(connection, target, identity)
            return True
        if migrator_oid is not None:
            connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
            if connection.execute(application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={}) "
                "OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member={} OR roleid={}) "
                "OR EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend WHERE refclassid='pg_catalog.pg_authid'::regclass AND refobjid={})",
                migrator_oid, migrator_oid, migrator_oid, migrator_oid,
            )).fetchone() != (False,):
                raise RuntimeError("application migration absent role retains authority")
        return False


def _utc(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(UTC).isoformat()
    except (ValueError, TypeError):
        raise RuntimeError("application migration peer timestamp is invalid") from None
