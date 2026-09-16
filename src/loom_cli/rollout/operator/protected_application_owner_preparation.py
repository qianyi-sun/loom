"""Journal the exact non-login staging owner before committing its creation.

Runs inside the admitted handoff and original retained guard, before admission
capture. The enclosing operation supplies administrator/DDL exclusion. This
neither transfers objects nor publishes a handoff terminal or rollout authority.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
    _read_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.staging_mutation_coordination import rollout_guard_application_name
from loom_cli.rollout.application_migration_contract import (
    APPLICATION_OWNER_ROLE as APPLICATION_OWNER_ROLE,
)

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import _backend, admission_record_digest

if TYPE_CHECKING:
    from .protected_apply_journal import ProtectedApplyJournal
    from .staging_mutation_guard import MutationGuardEvidence

MAX_OWNER_CREATIONS = 8


@dataclass(frozen=True, slots=True)
class ApplicationOwnerCreationIntent:
    ordinal: int
    previous_record_digest: str
    backend: ApplicationDatabaseHandoffBackend
    coordination_guard: ApplicationDatabaseCoordinationGuard

    def __post_init__(self) -> None:
        if (type(self.ordinal) is not int or not 1 <= self.ordinal <= MAX_OWNER_CREATIONS
                or re.fullmatch(r"[0-9a-f]{64}", self.previous_record_digest) is None
                or type(self.backend) is not ApplicationDatabaseHandoffBackend
                or type(self.coordination_guard) is not ApplicationDatabaseCoordinationGuard
                or any(getattr(self.backend, key) != getattr(self.coordination_guard.backend, key)
                       for key in ("system_identifier", "server_started_at", "database_oid"))):
            raise ValueError("application owner creation identity is invalid")

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "owner_role": APPLICATION_OWNER_ROLE, **asdict(self)}

    @property
    def digest(self) -> str:
        return admission_record_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApplicationOwnerCreationIntent:
        if (set(value) != {"schema_version", "owner_role", "ordinal", "previous_record_digest", "backend", "coordination_guard"}
                or type(value["schema_version"]) is not int or value["schema_version"] != 1
                or value["owner_role"] != APPLICATION_OWNER_ROLE or type(value["ordinal"]) is not int
                or not isinstance(value["previous_record_digest"], str)):
            raise ValueError("application owner creation fields are invalid")
        guard = value["coordination_guard"]
        if (not isinstance(guard, dict) or set(guard) != {"backend", "role_oid", "application_name"}
                or type(guard["role_oid"]) is not int or not isinstance(guard["application_name"], str)):
            raise ValueError("application owner guard fields are invalid")
        return cls(value["ordinal"], value["previous_record_digest"], _backend(value["backend"]),
                   ApplicationDatabaseCoordinationGuard(_backend(guard["backend"]), guard["role_oid"], guard["application_name"]))


def _observe(connection: ApplicationDatabaseConnection, guard: MutationGuardEvidence, *, owner_role: str = "loom",
             ) -> tuple[ApplicationDatabaseHandoffBackend, ApplicationDatabaseCoordinationGuard]:
    if owner_role not in {"loom", APPLICATION_OWNER_ROLE}:
        raise ValueError("application owner observer role is invalid")
    with connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        if connection.execute(application_sql(
            "SELECT current_database()='loom' AND current_user=session_user AND current_user='postgres' "
            "AND r.rolsuper AND current_setting('transaction_isolation')='read committed' "
            "AND pg_get_userbyid(d.datdba)={} FROM pg_roles r CROSS JOIN pg_database d "
            "WHERE r.rolname=current_user AND d.datname=current_database()", owner_role,
        )).fetchone() != (True,):
            raise RuntimeError("application owner preparation database authority changed")
        row = connection.execute(
            "SELECT a.pid,a.backend_start::text,s.system_identifier::text,pg_postmaster_start_time()::text,a.datid::bigint "
            "FROM pg_catalog.pg_stat_activity a CROSS JOIN pg_catalog.pg_control_system() s WHERE a.pid=pg_backend_pid()"
        ).fetchone()
        if row is None or len(row) != 5 or any(type(row[i]) is not int for i in (0, 4)):
            raise RuntimeError("application owner preparation peer identity changed")
        backend = ApplicationDatabaseHandoffBackend(int(str(row[0])), str(row[1]), str(row[2]), str(row[3]), int(str(row[4])))
        saved = _read_coordination_guard(connection, target=backend, backend_pid=guard.database_backend_pid,
            application_name=rollout_guard_application_name(request_id=guard.request_id, candidate_sha=guard.candidate_sha,
                candidate_tree=guard.candidate_tree, generation=guard.generation))
        # Canonical timestamps must compare with the existing guard serializer.
        backend = ApplicationDatabaseHandoffBackend(backend.pid, datetime.fromisoformat(backend.started_at).astimezone(UTC).isoformat(),
            backend.system_identifier, datetime.fromisoformat(backend.server_started_at).astimezone(UTC).isoformat(), backend.database_oid)
        return backend, saved


def _role(connection: ApplicationDatabaseConnection) -> int | None:
    row = connection.execute(application_sql(
        "SELECT oid::bigint,NOT (rolcanlogin OR rolinherit OR rolsuper OR rolcreatedb OR rolcreaterole "
        "OR rolreplication OR rolbypassrls) AND rolpassword IS NULL AND rolvaliduntil IS NULL "
        "AND rolconnlimit=-1 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole=r.oid) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=r.oid OR roleid=r.oid) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend WHERE refclassid='pg_catalog.pg_authid'::pg_catalog.regclass AND refobjid=r.oid) "
        "FROM pg_catalog.pg_authid r WHERE rolname={}", APPLICATION_OWNER_ROLE,
    )).fetchone()
    if row is None:
        return None
    if len(row) != 2 or type(row[0]) is not int or row[1] is not True:
        raise RuntimeError("application owner preparation role authority changed")
    return row[0]


def prepare_application_owner(plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
                              connection: ApplicationDatabaseConnection, guard: MutationGuardEvidence) -> int:
    """Create or recover one recorded OID; a rolled-back OID is never adopted."""
    journal.require_application_guard_retained(plan, guard=guard)
    if connection.info.transaction_status != TransactionStatus.IDLE or connection.info.server_version // 10000 not in {16, 17}:
        raise RuntimeError("application owner preparation requires an idle supported peer")
    backend, saved = _observe(connection, guard)
    records = journal.read_application_owner_creations(plan)
    if records and any(record.coordination_guard != saved for record, _ in records):
        raise RuntimeError("application owner preparation original guard changed")
    # A prior peer may still be committing; inspect its retirement before reading
    # role absence. No backend signalling or speculative DROP ROLE is allowed.
    if records and records[-1][0].backend != backend:
        old = records[-1][0].backend
        connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if connection.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid={} "
            "AND backend_start={}::pg_catalog.timestamptz)", old.pid, old.started_at,
        )).fetchone() != (False,):
            raise RuntimeError("application owner preparation previous peer still exists")
    observed = _role(connection)
    if observed is not None:
        if not records or records[-1][1] != observed:
            raise RuntimeError("application owner preparation found an unrecorded role")
        if _observe(connection, guard) != (backend, saved):
            raise RuntimeError("application owner preparation recovered identity changed")
        journal.require_application_guard_retained(plan, guard=guard)
        return observed
    intent = journal.prepare_application_owner_creation(plan, backend=backend, coordination_guard=saved)
    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout='1s'")
        connection.execute("SET LOCAL statement_timeout='30s'")
        connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(APPLICATION_OWNER_ROLE)))
        role_oid = _role(connection)
        if role_oid is None:
            raise RuntimeError("application owner preparation did not create its role")
        journal.record_application_owner_oid(plan, ordinal=intent.ordinal, role_oid=role_oid)
        journal.require_application_guard_retained(plan, guard=guard)
        if _read_coordination_guard(connection, target=backend, backend_pid=saved.backend.pid,
                                     application_name=saved.application_name) != saved:
            raise RuntimeError("application owner preparation coordination guard changed before commit")
    if _role(connection) != role_oid or _observe(connection, guard) != (backend, saved):
        raise RuntimeError("application owner preparation commit identity changed")
    journal.require_application_guard_retained(plan, guard=guard)
    return role_oid
