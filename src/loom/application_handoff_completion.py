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
    schema_revision: ApplicationSchemaRevision = "0150/guard_0035",
) -> ApplicationHandoffDatabaseOutcome:
    """Finish or reconcile transfer/reopen/login without creating another guard.

    The sealed path first serializes closure with any unacknowledged reopen,
    drains, validates/transfers the exact schema, commits ownership, then reopens
    and restores the original ordinary login. A previously restored login is
    observed read-only; it is never resealed or overwritten to force a replay.
    Every completion requires the saved postmaster/peer/guard and complete
    trusted schema/role/password checks. Interruptions leave the actual database
    state for the same admitted operation to reconcile; no speculative cleanup
    closes a database after runtime login has been restored.
    """
    bindings = dict(role_bindings)
    if (len(bindings) != len(_ALIASES) or set(bindings.values()) != _ALIASES
            or bindings.get(target.owner_role) != "application-owner"
            or target.successor_role in bindings
            or not isinstance(password, str) or not 1 <= len(password) <= 1024
            or any(not 0x21 <= ord(character) <= 0x7E for character in password)
            or schema_acl_profile not in {"application-only", "staging-readonly", "cnpg-staging"}):
        raise ValueError("application handoff completion binding is invalid")
    if any(channel.info.transaction_status != TransactionStatus.IDLE for channel in (connection, maintenance)):
        raise RuntimeError("application handoff completion requires idle connections")
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    def login_enabled() -> bool:
        return _login_enabled(
            connection, target=target, handoff_backend=handoff_backend,
            coordination_guard=coordination_guard, provisioner=provisioner,
        )

    login = login_enabled()
    if not login:
        deadline = time.monotonic() + 30
        while True:
            try:
                reclose_application_database_for_handoff_recovery(
                    maintenance, target=target, provisioner_role=provisioner,
                    handoff_backend=handoff_backend, coordination_guard=coordination_guard,
                    runtime_password=password,
                )
                require_application_database_drained(
                    maintenance, target=target, provisioner_role=provisioner,
                    handoff_backend=handoff_backend, coordination_guard=coordination_guard,
                    runtime_password=password,
                )
                _require_retired_client_work(
                    maintenance, target=target, handoff_backend=handoff_backend,
                    coordination_guard=coordination_guard, provisioner=provisioner,
                )
                with connection.transaction():
                    transfer_application_ownership(
                        connection, owner_role=target.successor_role, role_bindings=bindings,
                        runtime_password=password, admission_target=target,
                        coordination_guard=coordination_guard, schema_acl_profile=schema_acl_profile, schema_revision=schema_revision,
                    )
                break
            except (ApplicationDatabaseAdmissionError, ApplicationOwnershipTransferError, PsycopgError) as exc:
                message = exc.diag.message_primary if isinstance(exc, PsycopgError) else str(exc)
                quiescence = (isinstance(exc, PsycopgError) and exc.sqlstate == "55L01") or message in {
                        "application database sessions are not drained",
                        "application ownership requires reconciled sessions",
                        "application trigger handoff requires quiescent legacy authority"}
                if (not quiescence
                        or time.monotonic() >= deadline
                        or not _quiescence_retry_admitted(maintenance, target=target, handoff_backend=handoff_backend,
                            coordination_guard=coordination_guard, provisioner=provisioner)):
                    raise
                # ALLOW_CONNECTIONS does not exclude autovacuum. A refused SQL
                # transaction has rolled back; retry only under the same original
                # authority, repeating all closure, drainage and schema checks.
                time.sleep(0.1)
        reopen_application_database_after_handoff(
            maintenance, target=target, provisioner_role=provisioner,
            handoff_backend=handoff_backend, coordination_guard=coordination_guard,
            runtime_password=password,
        )
        restore_application_runtime_login(
            connection, owner_role=target.successor_role, role_bindings=bindings,
            password=password, target=target, schema_acl_profile=schema_acl_profile, schema_revision=schema_revision,
            coordination_guard=coordination_guard,
        )
    if observe_application_runtime_login(
        connection, owner_role=target.successor_role, role_bindings=bindings,
        password=password, target=target, schema_acl_profile=schema_acl_profile, schema_revision=schema_revision,
        coordination_guard=coordination_guard,
    ) is not ApplicationRuntimeLoginState.RESTORED:
        raise RuntimeError("application handoff runtime login is not restored")
    if not login_enabled():
        raise RuntimeError("application handoff completion login changed")
    return ApplicationHandoffDatabaseOutcome(target, coordination_guard)


def _quiescence_retry_admitted(
    maintenance: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner: str,
) -> bool:
    """Retain exact clients; autovacuum may still run or already have exited."""
    _require_retired_client_work(maintenance, target=target, handoff_backend=handoff_backend,
        coordination_guard=coordination_guard, provisioner=provisioner)
    with _maintenance_transaction(maintenance, database=target.database, provisioner_role=provisioner):
        maintenance.execute("SET TRANSACTION READ ONLY")
        _require_handoff_identity(maintenance, target, handoff_backend)
        _require_coordination_guard(maintenance, target, coordination_guard)
        maintenance.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        return maintenance.execute(application_sql(
            "SELECT count(*) FILTER (WHERE backend_type='client backend')=2 AND COALESCE(bool_and(("
            "backend_type='autovacuum worker' OR backend_type='client backend' AND ("
            "pid={} AND backend_start={}::pg_catalog.timestamptz AND usename={} OR "
            "pid={} AND backend_start={}::pg_catalog.timestamptz AND usesysid={} AND application_name={}"
            ")) IS TRUE),false) FROM pg_catalog.pg_stat_activity WHERE datid={}",
            handoff_backend.pid, handoff_backend.started_at, provisioner,
            coordination_guard.backend.pid, coordination_guard.backend.started_at,
            coordination_guard.role_oid, coordination_guard.application_name, target.database_oid,
        )).fetchone() == (True,)


def _require_retired_client_work(
    maintenance: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner: str,
) -> None:
    """Observe cluster-wide client retirement before ownership changes.

    Role DDL accepted through postgres or another database outlives application
    admission closure. Under the enclosing manager replacement and external
    writer exclusion, require no such clients or prepared work. Never signal an
    unknown peer or infer retirement from an idle/query-text observation. Native
    background and replication processes remain subject to the separately
    admitted SQL/process profile; this is not proof of arbitrary SQL silence.
    """
    with _maintenance_transaction(maintenance, database=target.database, provisioner_role=provisioner):
        maintenance.execute("SET TRANSACTION READ ONLY")
        _require_handoff_identity(maintenance, target, handoff_backend)
        _require_coordination_guard(maintenance, target, coordination_guard)
        # Startup acquires this cluster-wide shared-object lock before its
        # session statistics become visible. Inspect locks before fresh stats.
        if maintenance.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
            "AND classid='pg_catalog.pg_database'::regclass AND mode='RowExclusiveLock')"
        ).fetchone() != (False,):
            raise RuntimeError("application handoff client work has a pending database startup")
        if maintenance.execute("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts)").fetchone() != (False,):
            raise RuntimeError("application handoff client work has a prepared transaction")
        maintenance.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if maintenance.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE "
            "a.pid<>pg_catalog.pg_backend_pid() AND NOT ("
            "a.pid={} AND a.backend_start={}::pg_catalog.timestamptz AND a.datid={} "
            "AND a.usename={} AND a.backend_type='client backend') AND NOT ("
            "a.pid={} AND a.backend_start={}::pg_catalog.timestamptz AND a.datid={} "
            "AND a.usesysid={} AND a.application_name={} AND a.backend_type='client backend') "
            "AND (a.backend_type IS NULL OR a.backend_type NOT IN ("
            "'autovacuum launcher','autovacuum worker','background writer','checkpointer',"
            "'archiver','walwriter','walsender','walreceiver','logical replication launcher')))",
            handoff_backend.pid, handoff_backend.started_at, target.database_oid, provisioner,
            coordination_guard.backend.pid, coordination_guard.backend.started_at, target.database_oid,
            coordination_guard.role_oid, coordination_guard.application_name,
        )).fetchone() != (False,):
            raise RuntimeError("application handoff client work has surviving or unknown backends")
        _require_coordination_guard(maintenance, target, coordination_guard)


def application_handoff_recovery_login_enabled(
    maintenance: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    coordination_guard: ApplicationDatabaseCoordinationGuard, provisioner_role: str,
) -> bool:
    """Route a journaled lost-peer recovery without claiming schema completion.

    A live previous or unknown privileged peer refuses. A restored route permits
    only opening the fixed replacement peer for full read-only schema/credential
    verification. It never authorizes SQL mutation or treats LOGIN as a terminal.
    Sealed routes still require serialized reclosure and the exact loss-drain
    before any reopening. Other administrator/DDL writers remain excluded by the
    enclosing authority; these observations do not establish that exclusion.
    """
    with _maintenance_transaction(maintenance, database=target.database, provisioner_role=provisioner_role):
        _require_handoff_identity(maintenance, target, handoff_backend)
        _require_coordination_guard(maintenance, target, coordination_guard)
        state = maintenance.execute(application_sql(
            "SELECT a.rolcanlogin,d.datallowconn,d.datdba={} "
            "FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_control_system() s "
            "JOIN pg_catalog.pg_roles a ON a.rolname={} JOIN pg_catalog.pg_roles b ON b.rolname={} "
            "WHERE d.oid={} AND d.datname={} AND a.oid={} AND b.oid={} AND s.system_identifier::text={}",
            target.successor_oid, target.owner_role, target.successor_role,
            target.database_oid, target.database, target.owner_oid, target.successor_oid,
            target.system_identifier,
        )).fetchone()
        if state is None or len(state) != 3 or any(type(value) is not bool for value in state):
            raise RuntimeError("application completion recovery target changed")
        if state[0] and (not state[1] or not state[2]):
            raise RuntimeError("application completion recovery login precedes ownership or admission")
        if maintenance.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
            "AND classid='pg_catalog.pg_database'::regclass AND objid={} AND mode='RowExclusiveLock')",
            target.database_oid,
        )).fetchone() != (False,):
            raise RuntimeError("application completion recovery startup is pending")
        maintenance.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if maintenance.execute(application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE "
            "(pid={} AND backend_start={}::pg_catalog.timestamptz) OR "
            "(datid={} AND usesysid IS DISTINCT FROM {} AND pid<>{})) OR "
            "EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE database={})",
            handoff_backend.pid, handoff_backend.started_at, target.database_oid,
            target.owner_oid, coordination_guard.backend.pid, target.database,
        )).fetchone() != (False,):
            raise RuntimeError("application completion recovery has surviving or unknown peers")
        _require_coordination_guard(maintenance, target, coordination_guard)
        return state[0] is True
