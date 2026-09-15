"""Journal-bound guard-owner prerequisite for application migration 0147.

Apply only guard0033's backward-compatible conflict-target rewrite. The regular
guard migration still advances its own version in order and recognizes this
exact rewrite on replay. No application role receives private schema authority,
no credentials are issued, and no guard version marker is changed here.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping

from psycopg import sql

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_migrator_provision import _transaction

_FUNCTION = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"
_OLD = "ON CONFLICT (trial_id, attempt, execution_role) DO NOTHING"
_NEW = "-- guard_0033: refundable admission compatibility\n          ON CONFLICT DO NOTHING"


def _sha(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def ensure_guard_claim_compatibility(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
    guard_owner: str, retained: Mapping[str, object] | None,
    persist: Callable[[Mapping[str, object]], None],
) -> None:
    """Retain the exact original function/owner before the guarded catalog edit.

    The installed caller owns the original completed handoff, source plan,
    administrator writer exclusion, and active retained migration journal.
    A lost SQL reply accepts only the recorded before/after definition on the
    same function OID and owner. A failed persistence callback rolls back.
    """
    if guard_owner in {target.owner_role, target.successor_role, provisioner_role}:
        raise ValueError("guard compatibility owner overlaps application authority")
    with _transaction(connection, target, coordination_guard, provisioner_role):
        row = connection.execute(application_sql(
            "SELECT p.oid::bigint,p.proowner::bigint,pg_catalog.pg_get_functiondef(p.oid), "
            "p.prosecdef,p.proconfig,l.lanname,r.rolname, "
            "NOT (r.rolcanlogin OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole "
            "OR r.rolreplication OR r.rolbypassrls) AND r.rolpassword IS NULL, "
            "n.nspowner=p.proowner, "
            "(SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version) "
            "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
            "JOIN pg_catalog.pg_authid r ON r.oid=p.proowner "
            "JOIN pg_catalog.pg_language l ON l.oid=p.prolang "
            "WHERE p.oid=pg_catalog.to_regprocedure({})", _FUNCTION,
        )).fetchone()
        if (row is None or len(row) != 10 or type(row[0]) is not int or type(row[1]) is not int
                or not isinstance(row[2], str) or len(row[2].encode()) > 256 * 1024
                or row[3] is not True or row[4] != ["search_path=pg_catalog"]
                or row[5:9] != ("plpgsql", guard_owner, True, True)
                or row[9] not in {"guard_0030", "guard_0031", "guard_0032", "guard_0033", "guard_0034", "guard_0035", "guard_0036"}):
            raise RuntimeError("guard compatibility source authority changed")
        definition = row[2]
        if definition.count(_OLD) == 1 and "guard_0033" not in definition:
            replacement = definition.replace(_OLD, _NEW)
        elif definition.count(_NEW) == 1 and _OLD not in definition:
            replacement = definition
        else:
            raise RuntimeError("guard compatibility source definition changed")
        before = hashlib.sha256(definition.encode()).hexdigest()
        after = hashlib.sha256(replacement.encode()).hexdigest()
        identity: dict[str, object] = {"schema_version": 1, "database_oid": target.database_oid,
            "system_identifier": target.system_identifier, "function_oid": row[0],
            "owner_oid": row[1], "owner_role": guard_owner, "before_sha256": before,
            "after_sha256": after}
        if retained is not None:
            if (set(retained) != set(identity)
                    or any(not _sha(retained[key]) for key in ("before_sha256", "after_sha256"))
                    or any(retained[key] != identity[key] for key in identity if key not in {"before_sha256", "after_sha256"})
                    or before not in {retained["before_sha256"], retained["after_sha256"]}
                    or after != retained["after_sha256"]):
                raise RuntimeError("guard compatibility retained source changed")
        else:
            persist(identity)
        if replacement != definition:
            # Execute as the existing private owner, preserving owner, ACL,
            # signature, SECURITY DEFINER and search_path. Restore peer identity
            # before the enclosing surviving-guard readback and commit.
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(guard_owner)))
            connection.execute(sql.SQL(replacement))
            connection.execute("SET LOCAL ROLE NONE")
        observed = connection.execute(application_sql(
            "SELECT pg_catalog.pg_get_functiondef(oid),proowner::bigint,prosecdef,proconfig "
            "FROM pg_catalog.pg_proc WHERE oid={}", row[0],
        )).fetchone()
        if observed != (replacement, row[1], True, ["search_path=pg_catalog"]):
            raise RuntimeError("guard compatibility rewrite readback changed")
