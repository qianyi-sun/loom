"""Protected SQL phases for a journaled transient application migration login.

The caller admits the completed ownership handoff, retains the original guard,
serializes privileged writers, and journals a fixed role name, credential and
expiry before these phases. Creation publishes the new OID before SQL commit;
an interrupted creation with a saved but absent OID must retire that generation,
never adopt another role. Only the enclosing pending lifecycle may arm a login.
Job/Secret delivery, database admission closure, session retirement and lifecycle
completion remain caller responsibilities. Sealing alone does not retire sessions.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    _require_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_migrator_retirement import _require_memberships
from loom.application_password import matches_application_scram


def create_application_migrator(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    migrator_role: str, persist_identity: Callable[[ApplicationOwnerSuccessor], None],
) -> ApplicationOwnerSuccessor:
    """Create only an absent sealed role; durably publish its OID before commit."""
    _validate_identity(ApplicationOwnerSuccessor(migrator_role, 1), target, provisioner_role, check_oid=False)
    if not callable(persist_identity):
        raise ValueError("application migrator identity publisher is invalid")
    with _transaction(connection, target, coordination_guard, provisioner_role):
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname={})", migrator_role,
        )).fetchone() != (False,):
            raise RuntimeError("application migrator role already exists")
        _require_memberships(connection, target, 0)
        connection.execute(sql.SQL(
            "CREATE ROLE {} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD NULL"
        ).format(sql.Identifier(migrator_role)))
        row = connection.execute(application_sql(
            "SELECT oid::bigint FROM pg_catalog.pg_roles WHERE rolname={}", migrator_role,
        )).fetchone()
        if row is None or type(row[0]) is not int:
            raise RuntimeError("application migrator created identity is absent")
        identity = ApplicationOwnerSuccessor(migrator_role, row[0])
        persist_identity(identity)
        _require_role(connection, target, identity)
    return identity


def arm_application_migrator(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationOwnerSuccessor, password: str, expires_at: datetime,
) -> None:
    """Arm the saved identity at its fixed expiry, or verify a lost acknowledgement.

    Replaying an expired credential or changing an armed credential refuses. The
    caller's journal must exclude rearming after retirement has started.
    """
    _validate_identity(identity, target, provisioner_role)
    if (not isinstance(password, str) or not 1 <= len(password) <= 1024 or "\x00" in password
            or not isinstance(expires_at, datetime) or expires_at.utcoffset() is None):
        raise ValueError("application migrator credential or expiry is invalid")
    expiry = expires_at.isoformat()
    with _transaction(connection, target, coordination_guard, provisioner_role):
        if connection.execute(application_sql(
            "SELECT {}::timestamptz>clock_timestamp() AND {}::timestamptz<=clock_timestamp()+interval '1 hour'",
            expiry, expiry,
        )).fetchone() != (True,):
            raise RuntimeError("application migrator expiry is not bounded and live")
        _require_role(connection, target, identity)
        row = connection.execute(application_sql(
            "SELECT rolcanlogin,rolpassword,rolvaliduntil={}::timestamptz FROM pg_catalog.pg_authid WHERE oid={}",
            expiry, identity.role_oid,
        )).fetchone()
        if row is None:
            raise RuntimeError("application migrator saved role disappeared")
        if row[0]:
            if row[2] is not True or not matches_application_scram(password, row[1]):
                raise RuntimeError("application migrator armed credential changed")
            if _require_memberships(connection, target, identity.role_oid) != [
                    (target.successor_oid, identity.role_oid, False, False, True)]:
                raise RuntimeError("application migrator armed membership changed")
            if connection.execute(application_sql(
                "SELECT has_database_privilege({},{}::oid,'CONNECT')", identity.role_oid, target.database_oid,
            )).fetchone() != (True,):
                raise RuntimeError("application migrator armed admission changed")
            return
        if row[1] is not None:
            raise RuntimeError("application migrator sealed credential changed")
        connection.execute(sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE").format(
            sql.Identifier(target.successor_role), sql.Identifier(identity.role_name)))
        connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(target.database), sql.Identifier(identity.role_name)))
        connection.execute("SET LOCAL password_encryption='scram-sha-256'")
        connection.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {} VALID UNTIL {}").format(
            sql.Identifier(identity.role_name), sql.Literal(password), sql.Literal(expiry)))
        _require_role(connection, target, identity)


def seal_application_migrator(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationOwnerSuccessor,
) -> None:
    """Stop new logins for the saved role without claiming session retirement."""
    _validate_identity(identity, target, provisioner_role)
    with _transaction(connection, target, coordination_guard, provisioner_role):
        _require_role(connection, target, identity)
        connection.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(identity.role_name)))


def _validate_identity(
    identity: ApplicationOwnerSuccessor, target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str, *, check_oid: bool = True,
) -> None:
    if (type(identity) is not ApplicationOwnerSuccessor or identity.guard_owner is not None
            or identity.role_name in {target.owner_role, target.successor_role, provisioner_role, "loom_rollout_readonly"}
            or (check_oid and identity.role_oid in {target.owner_oid, target.successor_oid})):
        raise ValueError("application migrator identity is invalid")


@contextmanager
def _transaction(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
) -> Iterator[None]:
    if (connection.info.transaction_status != TransactionStatus.IDLE
            or connection.info.server_version // 10000 not in {16, 17}):
        raise RuntimeError("application migrator connection is invalid")
    with connection.transaction():
        connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        connection.execute("SET LOCAL lock_timeout='1s'")
        connection.execute("SET LOCAL statement_timeout='30s'")
        if connection.execute(application_sql(
            "SELECT current_user=session_user AND current_user={} AND rolsuper "
            "AND current_setting('transaction_isolation')='read committed' "
            "AND current_database()={} FROM pg_catalog.pg_roles WHERE rolname=current_user",
            provisioner_role, target.database,
        )).fetchone() != (True,):
            raise RuntimeError("application migrator requires protected application peer")
        _require_authority(connection, target, guard)
        yield
        _require_authority(connection, target, guard)


def _require_authority(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    guard: ApplicationDatabaseCoordinationGuard,
) -> None:
    if connection.execute(application_sql(
        "SELECT d.datname={} AND d.datdba={} AND s.system_identifier::text={} "
        "AND r.rolname={} AND NOT (r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreatedb "
        "OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) AND r.rolpassword IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=r.oid) "
        "FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_control_system() s "
        "JOIN pg_catalog.pg_authid r ON r.oid=d.datdba WHERE d.oid={}",
        target.database, target.successor_oid, target.system_identifier, target.successor_role, target.database_oid,
    )).fetchone() != (True,):
        raise RuntimeError("application migrator database or owner identity changed")
    _require_coordination_guard(connection, target, guard)


def _require_role(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    identity: ApplicationOwnerSuccessor,
) -> None:
    if connection.execute(application_sql(
        "SELECT oid={} AND NOT (rolinherit OR rolsuper OR rolcreatedb OR rolcreaterole "
        "OR rolreplication OR rolbypassrls) AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting "
        "WHERE setrole=pg_authid.oid) FROM pg_catalog.pg_authid WHERE rolname={}",
        identity.role_oid, identity.role_name,
    )).fetchone() != (True,):
        raise RuntimeError("application migrator saved role authority changed")
    _require_memberships(connection, target, identity.role_oid)
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend "
        "WHERE refclassid='pg_catalog.pg_authid'::regclass AND refobjid={} "
        "AND NOT (dbid=0 AND classid='pg_catalog.pg_database'::regclass AND objid={} AND deptype='a'))",
        identity.role_oid, target.database_oid,
    )).fetchone() != (False,):
        raise RuntimeError("application migrator has unexpected object authority")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_database d CROSS JOIN LATERAL "
        "pg_catalog.aclexplode(d.datacl) a WHERE a.grantee={} "
        "AND (d.oid<>{} OR a.privilege_type<>'CONNECT' OR a.is_grantable))", identity.role_oid, target.database_oid,
    )).fetchone() != (False,):
        raise RuntimeError("application migrator database grants changed")
