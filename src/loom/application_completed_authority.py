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
from loom.trial_writer_trigger_authority import application_public_definer_references


@dataclass(frozen=True, slots=True)
class ApplicationGuardOwner:
    """Exact guard schema owner from the independently admitted capacity operation."""

    role_name: str
    role_oid: int

    def __post_init__(self) -> None:
        if (re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.role_name) is None
                or type(self.role_oid) is not int or not 0 < self.role_oid < 2**32):
            raise ValueError("completed application guard owner identity is invalid")


@dataclass(frozen=True, slots=True)
class ApplicationOwnerSuccessor:
    """Exact role identity from an admitted successor journal, never live discovery.

    This constrains membership only. Its enclosing successor must separately
    verify credentials, expiration, Job identity and process retirement.
    """

    role_name: str
    role_oid: int
    guard_owner: ApplicationGuardOwner | None = None

    def __post_init__(self) -> None:
        if (re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.role_name) is None
                or type(self.role_oid) is not int or not 0 < self.role_oid < 2**32
                or (self.guard_owner is not None and (type(self.guard_owner) is not ApplicationGuardOwner
                    or self.guard_owner.role_name == self.role_name or self.guard_owner.role_oid == self.role_oid))):
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
        if connection.execute("SELECT current_setting('transaction_isolation')").fetchone() != ("read committed",):
            raise RuntimeError("completed application observation requires READ COMMITTED")
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
        _require_fixed_definers(connection)
    return hashlib.sha256(json.dumps(asdict(target), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _require_successor(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    successor: ApplicationOwnerSuccessor | None,
) -> None:
    roles = [target.owner_oid, target.successor_oid]
    if successor is not None:
        roles.append(successor.role_oid)
        guard = successor.guard_owner
        if guard is not None:
            if (guard.role_name in {target.owner_role, target.successor_role}
                    or guard.role_oid in roles):
                raise RuntimeError("completed application guard owner identity overlaps")
            roles.append(guard.role_oid)
            if connection.execute(application_sql(
                "SELECT r.oid={} AND NOT (r.rolcanlogin OR r.rolinherit OR r.rolsuper "
                "OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) "
                "AND r.rolpassword IS NULL AND n.nspowner=r.oid "
                "FROM pg_catalog.pg_authid r JOIN pg_catalog.pg_namespace n "
                "ON n.nspname='loom_capacity_guard' WHERE r.rolname={}",
                guard.role_oid, guard.role_name,
            )).fetchone() != (True,):
                raise RuntimeError("completed application guard owner authority changed")
            if connection.execute(application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend "
                "WHERE refclassid='pg_catalog.pg_authid'::regclass AND refobjid={} "
                "AND NOT (dbid={} OR dbid=0 AND classid='pg_catalog.pg_database'::regclass AND objid={}))",
                guard.role_oid, target.database_oid, target.database_oid,
            )).fetchone() != (False,):
                raise RuntimeError("completed application guard owner has foreign dependencies")
        if connection.execute(application_sql(
            "SELECT oid={} AND NOT rolsuper AND rolinherit={} AND NOT rolcreatedb "
            "AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls "
            "AND (NOT rolcanlogin AND rolpassword IS NULL OR rolcanlogin AND rolpassword IS NOT NULL "
            "AND rolvaliduntil IS NOT NULL AND rolvaliduntil<>'infinity'::timestamptz) "
            "FROM pg_catalog.pg_authid WHERE rolname={}", successor.role_oid, guard is not None, successor.role_name,
        )).fetchone() != (True,):
            raise RuntimeError("completed application successor role identity or authority changed")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=ANY({}::oid[]))", roles,
    )).fetchone() != (False,):
        raise RuntimeError("completed application role settings changed")
    memberships = connection.execute(application_sql(
        "SELECT member::bigint,roleid::bigint,admin_option,inherit_option,set_option "
        "FROM pg_catalog.pg_auth_members WHERE member=ANY({}::oid[]) OR roleid=ANY({}::oid[]) "
        "ORDER BY member,roleid", roles, roles,
    )).fetchall()
    expected = []
    if successor is not None:
        expected.append((successor.role_oid, target.successor_oid, False, successor.guard_owner is not None, True))
        if successor.guard_owner is not None:
            expected.append((successor.role_oid, successor.guard_owner.role_oid, False, True, True))
        expected.sort()
    if memberships != expected:
        raise RuntimeError("completed application owner or runtime memberships changed")


def _require_fixed_definers(connection: ApplicationDatabaseConnection) -> None:
    # Trigger functions run without checking the DML caller's EXECUTE ACL. A
    # hidden/new definer can therefore give the runtime DDL despite no explicit
    # routine grant. Only the reviewed bridge/helper bodies may retain elevation.
    references = {name: (body, result, ["search_path=pg_catalog"])
                  for name, body, result in application_public_definer_references()}
    # Exact source body from migration0135, independently verified by fresh
    # provisioning. Its retention trigger also fixes row_security=off.
    references["task_image_registry_reject_retired_attempt"] = (
        "9e7666277888bc7a3a457ece3e417938a7d04a0ae70f1440d92c279ed22e5e04",
        "trigger", ["search_path=pg_catalog", "row_security=off"],
    )
    rows = connection.execute(
        "SELECT p.proname,encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),p.prorettype::regtype::text, "
        "p.prokind='f' AND p.pronargs=0 AND NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof "
        "AND p.prosupport=0 AND p.provolatile='v' AND p.proparallel='u' "
        "AND l.lanname='plpgsql',p.proconfig "
        "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
        "JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE n.nspname='public' AND p.prosecdef "
        "ORDER BY p.proname,p.oid"
    ).fetchall()
    names = {row[0] for row in rows}
    required = {"loom_close_protected_runtime_trial_claim", "loom_transform_protected_runtime_trial_requeue"}
    if (len(names) != len(rows) or not required <= names
            or any(not isinstance(name, str) or references.get(name) != (body, result, config)
                   or valid is not True for name, body, result, valid, config in rows)):
        raise RuntimeError("completed application elevated definer authority changed")
