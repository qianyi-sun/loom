"""Issue only the saved executor login after a completed protected bootstrap.

The installed caller verifies the completed bootstrap and schema profile, serializes
privileged writers, and retains the exact identity and credential before issuance.
These SQL phases never create a role, grant membership or change routine privileges.
An issued credential is accepted only as an exact replay, never replaced.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from psycopg import sql

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_migrator_provision import _transaction
from loom.application_password import matches_application_scram

_EXECUTOR = "loom_cap_staging_executor"


@dataclass(frozen=True, slots=True)
class ApplicationExecutorAdmissionIdentity:
    role_oid: int
    privilege_sha256: str

    def __post_init__(self) -> None:
        if (type(self.role_oid) is not int or not 0 < self.role_oid < 2**32
                or not isinstance(self.privilege_sha256, str) or len(self.privilege_sha256) != 64
                or any(c not in "0123456789abcdef" for c in self.privilege_sha256)
                or self.privilege_sha256 == "0" * 64):
            raise ValueError("executor admission identity is invalid")


def admit_sealed_executor(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
) -> ApplicationExecutorAdmissionIdentity:
    """Capture a sealed original role after the caller admits the bootstrap profile."""
    with _transaction(connection, target, coordination_guard, provisioner_role):
        role = _role(connection, target)
        identity = ApplicationExecutorAdmissionIdentity(role[0], _privilege_digest(connection, role[0]))
        if role[1] or role[2] is not None or _database_acl(connection, target, role[0]):
            raise RuntimeError("executor admission role is not sealed")
        if connection.execute(application_sql(
            "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usesysid={}", role[0],
        )).fetchone() != (0,):
            raise RuntimeError("executor admission sealed role has sessions")
        return identity


def issue_executor_admission(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationExecutorAdmissionIdentity, password: str,
) -> None:
    """Atomically arm the retained role and CONNECT, or verify an exact lost reply."""
    _input(identity, password)
    with _transaction(connection, target, coordination_guard, provisioner_role):
        role = _bound_role(connection, target, identity)
        acl = _database_acl(connection, target, identity.role_oid)
        if role[1]:
            _issued(role, acl, target=target, password=password)
            return
        if role[2] is not None or acl:
            raise RuntimeError("executor admission sealed credential or ACL changed")
        if connection.execute(application_sql(
            "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usesysid={}", identity.role_oid,
        )).fetchone() != (0,):
            raise RuntimeError("executor admission sealed role has sessions")
        connection.execute("SET LOCAL password_encryption='scram-sha-256'")
        connection.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD {} VALID UNTIL 'infinity'").format(
            sql.Identifier(_EXECUTOR), sql.Literal(password)))
        # Explicit owner role makes the ACL grantor part of retained evidence.
        connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(target.successor_role)))
        connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(target.database), sql.Identifier(_EXECUTOR)))
        connection.execute("RESET ROLE")
        _issued(_bound_role(connection, target, identity), _database_acl(connection, target, identity.role_oid),
                target=target, password=password)


def require_issued_executor(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    identity: ApplicationExecutorAdmissionIdentity, password: str,
) -> None:
    """Read back exactly the retained issuance, including routine privileges and ACL."""
    _input(identity, password)
    with _transaction(connection, target, coordination_guard, provisioner_role):
        _issued(_bound_role(connection, target, identity), _database_acl(connection, target, identity.role_oid),
                target=target, password=password)


def _input(identity: ApplicationExecutorAdmissionIdentity, password: str) -> None:
    if (not isinstance(identity, ApplicationExecutorAdmissionIdentity) or not isinstance(password, str)
            or not 32 <= len(password) <= 1024 or any(not 0x21 <= ord(c) <= 0x7e for c in password)):
        raise ValueError("executor admission identity or credential is invalid")


def _role(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget) -> tuple[int, bool, object, bool]:
    if connection.execute(application_sql("SELECT current_database()={}", target.database)).fetchone() != (True,):
        raise RuntimeError("executor admission requires original application database")
    row = connection.execute(application_sql(
        "SELECT oid::bigint,rolcanlogin,rolpassword,rolvaliduntil='infinity'::timestamptz "
        "FROM pg_catalog.pg_authid WHERE rolname={} AND NOT (rolinherit OR rolsuper OR rolcreatedb "
        "OR rolcreaterole OR rolreplication OR rolbypassrls) AND NOT EXISTS "
        "(SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=pg_authid.oid) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=pg_authid.oid OR roleid=pg_authid.oid)",
        _EXECUTOR,
    )).fetchone()
    if (row is None or type(row[0]) is not int or type(row[1]) is not bool
            or row[0] in {target.owner_oid, target.successor_oid}):
        raise RuntimeError("executor admission role identity changed")
    return row[0], row[1], row[2], row[3] is True


def _bound_role(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
                identity: ApplicationExecutorAdmissionIdentity) -> tuple[int, bool, object, bool]:
    row = _role(connection, target)
    if row[0] != identity.role_oid:
        raise RuntimeError("executor admission saved identity changed")
    if _privilege_digest(connection, identity.role_oid) != identity.privilege_sha256:
        raise RuntimeError("executor admission named privileges changed")
    return row


def _database_acl(connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
                  role_oid: int) -> list[tuple[object, ...]]:
    rows = connection.execute(application_sql(
        "SELECT d.oid::bigint,a.grantor::bigint,a.privilege_type,a.is_grantable "
        "FROM pg_catalog.pg_database d CROSS JOIN LATERAL "
        "pg_catalog.aclexplode(COALESCE(d.datacl,pg_catalog.acldefault('d',d.datdba))) a "
        "WHERE a.grantee={} ORDER BY d.oid,a.grantor,a.privilege_type", role_oid,
    )).fetchall()
    if connection.execute(application_sql(
        "SELECT pg_catalog.has_database_privilege({}, {}, 'CREATE') OR "
        "pg_catalog.has_database_privilege({}, {}, 'TEMPORARY')", role_oid, target.database_oid,
        role_oid, target.database_oid,
    )).fetchone() != (False,):
        raise RuntimeError("executor admission database privileges changed")
    return rows


def _issued(role: tuple[int, bool, object, bool], acl: list[tuple[object, ...]], *,
            target: ApplicationDatabaseAdmissionTarget, password: str) -> None:
    if not role[1] or not role[3] or not matches_application_scram(password, role[2]):
        raise RuntimeError("executor admission issued credential changed")
    if acl != [(target.database_oid, target.successor_oid, "CONNECT", False)]:
        raise RuntimeError("executor admission issued CONNECT authority changed")


def _privilege_digest(connection: ApplicationDatabaseConnection, role_oid: int) -> str:
    # Include effective grants and shared ownership dependencies. Exclude only
    # database ACL dependencies: CONNECT is the separately checked transition.
    rows = connection.execute(application_sql(
        "WITH a(kind,object_id,sub_id,acl) AS ("
        "SELECT 'routine',oid::bigint,0,proacl FROM pg_catalog.pg_proc UNION ALL "
        "SELECT 'relation',oid::bigint,0,relacl FROM pg_catalog.pg_class UNION ALL "
        "SELECT 'column',attrelid::bigint,attnum,attacl FROM pg_catalog.pg_attribute UNION ALL "
        "SELECT 'schema',oid::bigint,0,nspacl FROM pg_catalog.pg_namespace UNION ALL "
        "SELECT 'type',oid::bigint,0,typacl FROM pg_catalog.pg_type UNION ALL "
        "SELECT 'default',oid::bigint,0,defaclacl FROM pg_catalog.pg_default_acl), "
        "evidence AS (SELECT a.kind,a.object_id,a.sub_id,x.grantor::bigint," 
        "x.privilege_type,x.is_grantable FROM a CROSS JOIN LATERAL pg_catalog.aclexplode(a.acl) x "
        "WHERE x.grantee={} UNION ALL "
        "SELECT 'dependency-'||classid::text,objid::bigint,objsubid,dbid::bigint,deptype::text,false "
        "FROM pg_catalog.pg_shdepend WHERE refclassid='pg_catalog.pg_authid'::regclass AND refobjid={} "
        "AND NOT (classid='pg_catalog.pg_database'::regclass AND deptype='a')) "
        "SELECT * FROM evidence ORDER BY 1,2,3,4,5,6", role_oid, role_oid,
    )).fetchall()
    if not rows:
        raise RuntimeError("executor admission named privileges are absent")
    return hashlib.sha256(json.dumps(rows, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
