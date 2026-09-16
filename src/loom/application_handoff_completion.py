"""Recover and finish the database phases of an admitted application handoff.

The installed component must supply its immutable original admission, a journaled
live peer, original supervised guard, original credential and fixed ACL profile.
Administrator/DDL exclusion, CNPG retirement and owned workload containment must
remain in force. This module opens no connections and is not a rollout entrypoint,
a component terminal or authority to retire the CNPG input fence.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass

from psycopg import Error as PsycopgError
from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionError,
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
    _maintenance_transaction,
    _require_coordination_guard,
    _require_handoff_identity,
    reclose_application_database_for_handoff_recovery,
    reopen_application_database_after_handoff,
    require_application_database_drained,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_ownership_transfer import (
    ApplicationOwnershipTransferError,
    transfer_application_ownership,
)
from loom.application_runtime_login import (
    ApplicationRuntimeLoginState,
    observe_application_runtime_login,
    restore_application_runtime_login,
)
from loom.application_schema_reference import ApplicationSchemaAclProfile, ApplicationSchemaRevision


@dataclass(frozen=True, slots=True)
class ApplicationHandoffDatabaseOutcome:
    """Observed database result only; does not prove workload or external authority."""

    target: ApplicationDatabaseAdmissionTarget
    coordination_guard: ApplicationDatabaseCoordinationGuard


_ALIASES = frozenset({
    "application-owner", "guard-owner", "guard-migrator", "guard-agent",
    "guard-executor", "guard-observer", "guard-runtime", "provisioner",
})


def _login_enabled(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner: str,
) -> bool:
    """Observe routing only, without accepting a login, password or schema profile."""
    with connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        if connection.execute("SELECT pg_catalog.current_setting('transaction_isolation')").fetchone() != ("read committed",):
            raise RuntimeError("application handoff completion requires READ COMMITTED")
        if connection.execute(application_sql(
            "SELECT current_user=session_user AND current_user={} AND rolsuper "
            "FROM pg_catalog.pg_roles WHERE rolname=current_user", provisioner,
        )).fetchone() != (True,):
            raise RuntimeError("application handoff completion administrator changed")
        _require_handoff_identity(connection, target, handoff_backend)
        _require_coordination_guard(connection, target, coordination_guard)
        # The saved peer may be the original or a journaled recovery successor,
        # but a live connection cannot impersonate either by supplying its PID.
        if connection.execute(application_sql(
            "SELECT a.pid={} AND a.backend_start={}::pg_catalog.timestamptz "
            "AND a.datid={} FROM pg_catalog.pg_stat_activity a "
            "WHERE a.pid=pg_catalog.pg_backend_pid()",
            handoff_backend.pid, handoff_backend.started_at, target.database_oid,
        )).fetchone() != (True,):
            raise RuntimeError("application handoff completion peer changed")
        row = connection.execute(application_sql(
            "SELECT r.rolcanlogin FROM pg_catalog.pg_authid r JOIN pg_catalog.pg_database d "
            "ON d.oid={} WHERE r.oid={} AND r.rolname={} AND d.datname={} "
            "AND d.datdba IN ({},{})",
            target.database_oid, target.owner_oid, target.owner_role, target.database,
            target.owner_oid, target.successor_oid,
        )).fetchone()
        if row is None or len(row) != 1 or type(row[0]) is not bool:
            raise RuntimeError("application handoff completion saved identity changed")
        return row[0]


def complete_application_handoff_database(
    connection: ApplicationDatabaseConnection,
    *,
    maintenance: ApplicationDatabaseConnection,
    target: ApplicationDatabaseAdmissionTarget,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    coordination_guard: ApplicationDatabaseCoordinationGuard,
    role_bindings: Mapping[str, str],
    password: str,
    schema_acl_profile: ApplicationSchemaAclProfile,
    schema_revision: ApplicationSchemaRevision = "0148/guard_0036",
