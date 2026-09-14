"""Read the enduring ownership effect of an already-completed handoff.

This is NOT initial handoff admission, a schema reference, migration authorization,
credential retirement or workload recovery. The protected caller must establish
an exact durable handoff terminal, verify the current guard/epoch, admit any
recorded successor operation and its credential/Job lifecycle, and serialize
privileged writers around this observation. A pending handoff must use the strict
original schema/process/guard recovery path instead.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from psycopg.pq import TransactionStatus

from loom.application_database_admission import ApplicationDatabaseAdmissionTarget
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_ownership_transfer import require_application_role_scope
from loom.application_password import matches_application_scram
from loom.application_schema_inventory import require_application_event_trigger_policy


@dataclass(frozen=True, slots=True)
class ApplicationOwnerSuccessor:
    """Exact role identity from an admitted successor journal, never live discovery.

    This constrains membership only. Its enclosing successor must separately
    verify credentials, expiration, Job identity and process retirement.
    """

    role_name: str
    role_oid: int

    def __post_init__(self) -> None:
        if (re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.role_name) is None
                or type(self.role_oid) is not int or not 0 < self.role_oid < 2**32):
            raise ValueError("completed application successor identity is invalid")


def observe_completed_application_authority(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    runtime_password: str, successor: ApplicationOwnerSuccessor | None = None,
) -> str:
    """Check current catalog authority without assuming the original schema remains.

    Later owner migrations may add public objects and advance version rows. They
    must preserve the saved database/role identities and ordinary runtime's lack
    of DDL/owner access. No live SQL definition is executed or adopted. Return only
    a non-secret identity digest after the live checks pass; this digest is not a
    substitute for the caller's completed journal and successor admission.
    """
    if (connection.info.transaction_status != TransactionStatus.IDLE
            or connection.info.server_version // 10000 not in {16, 17}
            or not isinstance(runtime_password, str) or not 1 <= len(runtime_password) <= 1024
            or (successor is not None and (successor.role_name in {target.owner_role, target.successor_role}
                or successor.role_oid in {target.owner_oid, target.successor_oid}))):
        raise RuntimeError("completed application observation context is invalid")
    with connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        connection.execute("SET LOCAL statement_timeout='30s'")
        if connection.execute(
            "SELECT current_user=session_user AND rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user"
        ).fetchone() != (True,):
            raise RuntimeError("completed application observer is not the protected administrator")
        require_application_event_trigger_policy(connection)
        if connection.execute(application_sql(
            "SELECT s.system_identifier::text={} AND d.oid={} AND d.datname={} "
            "AND d.datallowconn AND d.datdba={} AND n.nspowner={} "
            "AND a.oid={} AND b.oid={} "
            "FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_control_system() s "
            "JOIN pg_catalog.pg_roles a ON a.rolname={} JOIN pg_catalog.pg_roles b ON b.rolname={} "
            "JOIN pg_catalog.pg_namespace n ON n.nspname='public' "
            "WHERE d.datname=pg_catalog.current_database()",
            target.system_identifier, target.database_oid, target.database, target.successor_oid,
            target.successor_oid, target.owner_oid, target.successor_oid,
            target.owner_role, target.successor_role,
        )).fetchone() != (True,):
            raise RuntimeError("completed application database identity or ownership changed")
        roles = connection.execute(application_sql(
            "SELECT oid::bigint,NOT rolsuper AND NOT rolinherit AND NOT rolcreatedb "
            "AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls, "
            "rolcanlogin,rolpassword,rolvaliduntil IS NULL OR rolvaliduntil='infinity'::timestamptz "
            "FROM pg_catalog.pg_authid WHERE oid=ANY({}::oid[]) ORDER BY oid",
            [target.owner_oid, target.successor_oid],
        )).fetchall()
        by_oid = {row[0]: row[1:] for row in roles}
        runtime, owner = by_oid.get(target.owner_oid), by_oid.get(target.successor_oid)
        if (runtime is None or owner is None or not runtime[0] or not runtime[1] or not runtime[3]
                or not matches_application_scram(runtime_password, runtime[2])
                or not owner[0] or owner[1] or owner[2] is not None):
            raise RuntimeError("completed application role authority or credential changed")
        _require_successor(connection, target=target, successor=successor)
        require_application_role_scope(connection, roles=[target.owner_role])
        # Query catalog metadata only. Runtime DML grants/defaults are allowed;
        # ownership, CREATE, TRIGGER and callable elevated routines are not.
        if connection.execute(application_sql(
            "SELECT NOT pg_catalog.has_database_privilege({},current_database(),'CREATE') "
            "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n "
            "WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' "
            "AND pg_catalog.has_schema_privilege({},n.oid,'CREATE')) "
            "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='public' AND (c.relowner<>{} OR c.relkind IN ('r','p','v','m','f') "
            "AND pg_catalog.has_table_privilege({},c.oid,'TRIGGER'))) "
            "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='public' AND p.proowner<>{}) "
            "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace "
            "WHERE n.nspname='public' AND t.typowner<>{}) "
            "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND p.prosecdef "
            "AND pg_catalog.has_schema_privilege({},n.oid,'USAGE') "
            "AND pg_catalog.has_function_privilege({},p.oid,'EXECUTE'))",
            target.owner_oid, target.owner_oid, target.successor_oid, target.owner_oid,
            target.successor_oid, target.successor_oid, target.owner_oid, target.owner_oid,
        )).fetchone() != (True,):
            raise RuntimeError("completed application runtime DDL or object ownership changed")
    return hashlib.sha256(json.dumps(asdict(target), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _require_successor(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    successor: ApplicationOwnerSuccessor | None,
) -> None:
    roles = [target.owner_oid, target.successor_oid]
    if successor is not None:
        roles.append(successor.role_oid)
        if connection.execute(application_sql(
            "SELECT oid={} AND NOT rolsuper AND NOT rolinherit AND NOT rolcreatedb "
            "AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls "
            "AND (NOT rolcanlogin AND rolpassword IS NULL OR rolcanlogin AND rolpassword IS NOT NULL "
            "AND rolvaliduntil IS NOT NULL AND rolvaliduntil<>'infinity'::timestamptz) "
            "FROM pg_catalog.pg_authid WHERE rolname={}", successor.role_oid, successor.role_name,
        )).fetchone() != (True,):
            raise RuntimeError("completed application successor role identity or authority changed")
    memberships = connection.execute(application_sql(
        "SELECT member::bigint,roleid::bigint,admin_option,inherit_option,set_option "
        "FROM pg_catalog.pg_auth_members WHERE member=ANY({}::oid[]) OR roleid=ANY({}::oid[]) "
        "ORDER BY member,roleid", roles, roles,
    )).fetchall()
    expected = [] if successor is None else [(successor.role_oid, target.successor_oid, False, False, True)]
    if memberships != expected:
        raise RuntimeError("completed application owner or runtime memberships changed")
