"""Retire the exact two-owner guard migrator without deleting permanent roles.

The enclosing protected lifecycle journals all role OIDs and its maintenance peer,
retains the original guard, excludes privileged writers, seals credentials and
stops the exact bootstrap Job before closure. Reopening requires session and
membership retirement and the original runtime password. These helpers neither
establish that authority nor create/rearm a role or deliver a credential.
"""

from __future__ import annotations

from psycopg import sql

from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_migrator_admission import (
    _allowed,
    _require_maintenance,
    _require_runtime_credential,
)
from loom.application_migrator_provision import _transaction
from loom.application_migrator_retirement import _require_target, _retire_sessions


def close_application_guard_migrator_admission(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str, identity: ApplicationOwnerSuccessor,
) -> None:
    _identity(identity, target, provisioner_role, coordination_guard)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        _require_maintenance(connection, target)
        _sealed(connection, target, identity)
        _memberships(connection, target, identity)
        if _allowed(connection, target):
            connection.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
        if _allowed(connection, target):
            raise RuntimeError("application guard migrator admission did not close")


def retire_application_guard_migrator(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str, identity: ApplicationOwnerSuccessor,
) -> None:
    _identity(identity, target, provisioner_role, coordination_guard)
    assert identity.guard_owner is not None
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        _require_maintenance(connection, target)
        _require_target(connection, target, coordination_guard)
        _sealed(connection, target, identity)
        memberships = _memberships(connection, target, identity)
        _retire_sessions(connection, target=target, coordination_guard=coordination_guard,
            migrator_role=identity.role_name, migrator_oid=identity.role_oid)
        if memberships:
            connection.execute(sql.SQL("REVOKE {},{} FROM {}").format(sql.Identifier(target.successor_role),
                sql.Identifier(identity.guard_owner.role_name), sql.Identifier(identity.role_name)))
        connection.execute(sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
            sql.Identifier(target.database), sql.Identifier(identity.role_name)))
        connection.execute(sql.SQL("REVOKE CREATE ON DATABASE {} FROM {}").format(
            sql.Identifier(target.database), sql.Identifier(identity.guard_owner.role_name)))
        # Restore the permanent role profile only after its password, sessions
        # and owner memberships are gone. No credential survives this change.
        connection.execute(sql.SQL("ALTER ROLE {} VALID UNTIL 'infinity'").format(sql.Identifier(identity.role_name)))
        _sealed(connection, target, identity)
        if _memberships(connection, target, identity):
            raise RuntimeError("application guard migrator memberships did not retire")
        _require_target(connection, target, coordination_guard)


def reopen_application_guard_migrator_admission(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str, identity: ApplicationOwnerSuccessor,
    runtime_password: str,
) -> None:
    _identity(identity, target, provisioner_role, coordination_guard)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        _require_maintenance(connection, target)
        _require_retired(connection, target, identity)
        _require_runtime_credential(connection, target, runtime_password)
        if not _allowed(connection, target):
            connection.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(target.database)))
        if not _allowed(connection, target):
            raise RuntimeError("application guard migrator admission did not reopen")


def require_application_guard_migrator_retired(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str, identity: ApplicationOwnerSuccessor,
) -> None:
    """Read current retirement of the permanent role under the original authority."""
    _identity(identity, target, provisioner_role, coordination_guard)
    with _transaction(connection, target, coordination_guard, provisioner_role, allow_closed=True):
        _require_retired(connection, target, identity)


def _require_retired(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
                     identity: ApplicationOwnerSuccessor) -> None:
    _sealed(connection, target, identity)
    assert identity.guard_owner is not None
    if connection.execute(application_sql(
        "SELECT has_database_privilege({},{}::oid,'CREATE')", identity.guard_owner.role_oid, target.database_oid,
    )).fetchone() != (False,):
        raise RuntimeError("application guard owner schema creation grant has not retired")
    if _memberships(connection, target, identity):
        raise RuntimeError("application guard migrator memberships have not retired")
    if connection.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
        "AND classid='pg_catalog.pg_database'::regclass AND mode='RowExclusiveLock')"
    ).fetchone() != (False,):
        raise RuntimeError("application guard migrator admission has pending database startup")
    connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usesysid={}) "
        "OR EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend WHERE refclassid='pg_catalog.pg_authid'::regclass AND refobjid={})",
        identity.role_oid, identity.role_oid,
    )).fetchone() != (False,):
        raise RuntimeError("application guard migrator authority has not retired")


def _identity(identity: ApplicationOwnerSuccessor, target: ApplicationDatabaseAdmissionTarget,
              provisioner_role: str, guard: ApplicationDatabaseCoordinationGuard) -> None:
    if type(identity) is not ApplicationOwnerSuccessor or identity.guard_owner is None:
        raise ValueError("application guard migrator identity is invalid")
    owners = identity.guard_owner
    if (len({target.owner_oid, target.successor_oid, guard.role_oid, identity.role_oid, owners.role_oid}) != 5
            or len({target.owner_role, target.successor_role, provisioner_role, "loom_rollout_readonly", identity.role_name, owners.role_name}) != 6):
        raise ValueError("application guard migrator identity overlaps protected roles")


def _sealed(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
            identity: ApplicationOwnerSuccessor) -> None:
    _require_roles(connection, target, identity, allow_migrator_login=False)


def _require_roles(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
                   identity: ApplicationOwnerSuccessor, *, allow_migrator_login: bool) -> None:
    assert identity.guard_owner is not None
    for role, oid, inherit in ((identity.role_name, identity.role_oid, True),
                              (identity.guard_owner.role_name, identity.guard_owner.role_oid, False)):
        if connection.execute(application_sql(
            "SELECT oid={} AND rolinherit={} AND ({} OR NOT rolcanlogin AND rolpassword IS NULL) "
            "AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls) AND NOT EXISTS "
            "(SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=pg_authid.oid) "
            "FROM pg_catalog.pg_authid WHERE rolname={}", oid, inherit, allow_migrator_login and role == identity.role_name, role,
        )).fetchone() != (True,):
            raise RuntimeError("application guard migrator role is not the exact sealed identity")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend WHERE refclassid='pg_catalog.pg_authid'::regclass "
        "AND refobjid={} AND NOT (dbid=0 AND classid='pg_catalog.pg_database'::regclass AND objid={} AND deptype='a')) "
        "OR EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend WHERE refclassid='pg_catalog.pg_authid'::regclass "
        "AND refobjid={} AND NOT (dbid={} OR dbid=0 AND classid='pg_catalog.pg_database'::regclass AND objid={}))",
        identity.role_oid, target.database_oid, identity.guard_owner.role_oid, target.database_oid, target.database_oid,
    )).fetchone() != (False,):
        raise RuntimeError("application guard migrator role has unexpected object authority")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_database d CROSS JOIN LATERAL pg_catalog.aclexplode(d.datacl) a "
        "WHERE a.grantee={} AND (d.oid<>{} OR a.privilege_type<>'CONNECT' OR a.is_grantable))",
        identity.role_oid, target.database_oid,
    )).fetchone() != (False,):
        raise RuntimeError("application guard migrator database grants changed")


def _memberships(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
                 identity: ApplicationOwnerSuccessor) -> list[tuple[object, ...]]:
    assert identity.guard_owner is not None
    roles = [target.owner_oid, target.successor_oid, identity.role_oid, identity.guard_owner.role_oid]
    rows = connection.execute(application_sql(
        "SELECT roleid::bigint,member::bigint,admin_option,inherit_option,set_option FROM pg_catalog.pg_auth_members "
        "WHERE roleid=ANY({}::oid[]) OR member=ANY({}::oid[]) ORDER BY roleid,member", roles, roles,
    )).fetchall()
    expected = sorted([(target.successor_oid, identity.role_oid, False, True, True),
                       (identity.guard_owner.role_oid, identity.role_oid, False, True, True)])
    if rows not in ([], expected):
        raise RuntimeError("application guard migrator owner memberships changed")
    return rows
