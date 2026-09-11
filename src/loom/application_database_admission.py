"""Internal database admission barrier; no deployment caller or PID signalling.

The protected caller must admit exclusive maintenance scope for this database,
persist the captured target before closing admission, and serialize ALL admission,
role and database DDL writers throughout the operation (including legacy owners).
An explicit original runtime password permits only an independently admitted
same-password refresher; all LOGIN, membership, owner and other DDL remains excluded.
Owned clients must shut down through their protected workload paths. These helpers
neither supply that containment nor replace guard/schema/credential admission.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_password import require_sealed_runtime_password
from loom.staging_mutation_coordination import rollout_guard_application_name


class ApplicationDatabaseAdmissionError(RuntimeError):
    """Admission identity, state or ordered drain observation was not exact."""


@dataclass(frozen=True, slots=True)
class ApplicationDatabaseAdmissionTarget:
    system_identifier: str
    database: str
    database_oid: int
    owner_role: str
    owner_oid: int
    successor_role: str
    successor_oid: int

    def __post_init__(self) -> None:
        if (
            re.fullmatch(r"[0-9]{1,20}", self.system_identifier) is None
            or any(
                re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None
                for name in (self.database, self.owner_role, self.successor_role)
            )
            or any(
                type(oid) is not int or not 0 < oid < 2**32
                for oid in (self.database_oid, self.owner_oid, self.successor_oid)
            )
            or self.owner_role == self.successor_role
            or self.owner_oid == self.successor_oid
        ):
            raise ValueError("application database admission target is invalid")


@dataclass(frozen=True, slots=True)
class ApplicationDatabaseHandoffBackend:
    pid: int
    started_at: str
    system_identifier: str
    server_started_at: str
    database_oid: int

    def __post_init__(self) -> None:
        if (
            type(self.pid) is not int
            or not 0 < self.pid < 2**31
            or re.fullmatch(r"[0-9]{1,20}", self.system_identifier) is None
            or type(self.database_oid) is not int
            or not 0 < self.database_oid < 2**32
        ):
            raise ValueError("application database handoff backend is invalid")
        for value in (self.started_at, self.server_started_at):
            try:
                if len(value) > 128 or datetime.fromisoformat(value).utcoffset() is None:
                    raise ValueError
            except (ValueError, TypeError):
                raise ValueError("application database handoff timestamp is invalid") from None


@dataclass(frozen=True, slots=True)
class ApplicationDatabaseCoordinationGuard:
    """Exact already-admitted rollout guard, not a general allowed-backend list."""

    backend: ApplicationDatabaseHandoffBackend
    role_oid: int
    application_name: str

    def __post_init__(self) -> None:
        if (type(self.role_oid) is not int or not 0 < self.role_oid < 2**32
                or re.fullmatch(r"loom-rollout-guard-[0-9a-f]{40}", self.application_name) is None):
            raise ValueError("application coordination guard identity is invalid")


def _read_coordination_guard(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    backend_pid: int, application_name: str,
) -> ApplicationDatabaseCoordinationGuard:
    connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
    row = connection.execute(application_sql(
        "SELECT a.pid,a.backend_start::text,s.system_identifier::text,"
        "pg_catalog.pg_postmaster_start_time()::text,a.datid::bigint,r.oid::bigint "
        "FROM pg_catalog.pg_stat_activity a CROSS JOIN pg_catalog.pg_control_system() s "
        "JOIN pg_catalog.pg_roles r ON r.oid=a.usesysid "
        "WHERE a.pid={} AND a.datid={} AND a.application_name={} "
        "AND a.backend_type='client backend' AND r.rolname='loom_rollout_readonly' "
        "AND r.rolcanlogin AND NOT (r.rolsuper OR r.rolinherit OR r.rolcreatedb "
        "OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid OR m.roleid=r.oid) "
        "AND EXISTS (SELECT 1 FROM pg_catalog.pg_locks l WHERE l.pid=a.pid "
        "AND l.database=a.datid AND l.locktype='advisory' AND l.classid=1280263818 "
        "AND l.objid=1621151599 AND l.objsubid=1 AND l.mode='ExclusiveLock' AND l.granted)",
        backend_pid, target.database_oid, application_name,
    )).fetchone()
    if (row is None or len(row) != 6
            or any(type(row[i]) is not int for i in (0, 4, 5))
            or any(not isinstance(row[i], str) for i in (1, 2, 3))):
        raise ApplicationDatabaseAdmissionError("application coordination guard identity or lock changed")
    backend = ApplicationDatabaseHandoffBackend(
        pid=int(str(row[0])), started_at=datetime.fromisoformat(str(row[1])).astimezone(UTC).isoformat(),
        system_identifier=str(row[2]),
        server_started_at=datetime.fromisoformat(str(row[3])).astimezone(UTC).isoformat(),
        database_oid=int(str(row[4])),
    )
    _require_handoff_identity(connection, target, backend)
    return ApplicationDatabaseCoordinationGuard(backend, int(str(row[5])), application_name)


def capture_application_coordination_guard(
    connection: ApplicationDatabaseConnection, *, target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str, backend_pid: int, request_id: str, candidate_sha: str,
    candidate_tree: str, generation: str, runtime_password: str | None = None,
) -> ApplicationDatabaseCoordinationGuard:
    """Capture the installed guard before closure for immutable operation recovery.

    PID and request identity must come from the admitted live mutation guard,
    never session-name discovery. The caller independently admits its process,
    effective readonly role authority and continuous supervision; names and lock
    observations do not authenticate a process or prove uninterrupted ownership.
    Its existing catalog-only health loop can continue during application locks.
    """
    if type(backend_pid) is not int or not 0 < backend_pid < 2**31:
        raise ValueError("application coordination guard PID is invalid")
    name = rollout_guard_application_name(request_id=request_id, candidate_sha=candidate_sha,
                                         candidate_tree=candidate_tree, generation=generation)
    with _maintenance_transaction(connection, database=target.database, provisioner_role=provisioner_role):
        if not _checked_state(connection, target, runtime_password=runtime_password):
            raise ApplicationDatabaseAdmissionError("application coordination guard capture requires open admission")
        return _read_coordination_guard(connection, target=target, backend_pid=backend_pid, application_name=name)


def _require_coordination_guard(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    guard: ApplicationDatabaseCoordinationGuard,
) -> None:
    observed = _read_coordination_guard(connection, target=target, backend_pid=guard.backend.pid,
                                       application_name=guard.application_name)
    if observed != guard:
        raise ApplicationDatabaseAdmissionError("application coordination guard saved identity changed")


def coordination_guard_handoff_predicate(guard: ApplicationDatabaseCoordinationGuard) -> sql.Composed:
    """Exact pg_stat_activity alias `a` predicate for the fixed handoff SQL.

    Recheck identity, closed current database and lock inside the definer-transfer
    statement as well as in its Python caller. This is not arbitrary SQL authority.
    """
    return application_sql(
        "a.pid={} AND a.backend_start={}::pg_catalog.timestamptz AND a.datid={} "
        "AND a.usesysid={} AND a.usename='loom_rollout_readonly' "
        "AND a.backend_type='client backend' AND a.application_name={} "
        "AND a.datid=(SELECT oid FROM pg_catalog.pg_database "
        "WHERE datname=pg_catalog.current_database() AND NOT datallowconn) "
        "AND (SELECT system_identifier::text FROM pg_catalog.pg_control_system())={} "
        "AND pg_catalog.pg_postmaster_start_time()={}::pg_catalog.timestamptz "
        "AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.oid=a.usesysid "
        "AND r.rolcanlogin AND NOT (r.rolsuper OR r.rolinherit OR r.rolcreatedb "
        "OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=a.usesysid OR m.roleid=a.usesysid) "
        "AND EXISTS (SELECT 1 FROM pg_catalog.pg_locks l WHERE l.pid=a.pid "
        "AND l.database=a.datid AND l.locktype='advisory' AND l.classid=1280263818 "
        "AND l.objid=1621151599 AND l.objsubid=1 AND l.mode='ExclusiveLock' AND l.granted)",
        guard.backend.pid, guard.backend.started_at, guard.backend.database_oid,
        guard.role_oid, guard.application_name, guard.backend.system_identifier, guard.backend.server_started_at,
    )


@contextmanager
def _maintenance_transaction(
    connection: ApplicationDatabaseConnection, *, database: str, provisioner_role: str
) -> Iterator[None]:
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise ApplicationDatabaseAdmissionError("application admission requires an idle connection")
    if connection.info.server_version // 10000 not in {16, 17}:
        raise ApplicationDatabaseAdmissionError("application admission requires PostgreSQL 16 or 17")
    with connection.transaction():
        connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        if connection.execute(
            "SELECT pg_catalog.current_setting('transaction_isolation')"
        ).fetchone() != ("read committed",):
            raise ApplicationDatabaseAdmissionError("application admission requires READ COMMITTED")
        if connection.execute(
            application_sql(
                "SELECT current_user=session_user AND current_user={} AND rolsuper "
                "AND pg_catalog.current_database() <> {} FROM pg_catalog.pg_roles WHERE rolname=current_user",
                provisioner_role,
                database,
            )
        ).fetchone() != (True,):
            raise ApplicationDatabaseAdmissionError(
                "application admission requires separate maintenance administrator"
            )
        for name, value, limit in (
            ("lock_timeout", "1s", 1000),
            ("statement_timeout", "30s", 30000),
        ):
            connection.execute(
                application_sql(
                    "SELECT pg_catalog.set_config({},{},true) FROM pg_catalog.pg_settings "
                    "WHERE name={} AND (setting::integer=0 OR setting::integer>{})",
                    name,
                    value,
                    name,
                    limit,
                )
            )
        yield


def _read_target(
    connection: ApplicationDatabaseConnection,
    *,
    database: str,
    owner_role: str,
    successor_role: str,
    runtime_password: str | None,
) -> tuple[ApplicationDatabaseAdmissionTarget, bool, int]:
    row = connection.execute(
        application_sql(
            "SELECT s.system_identifier::pg_catalog.text,d.oid::pg_catalog.int8,"
            "a.oid::pg_catalog.int8,b.oid::pg_catalog.int8,d.datallowconn,d.datdba::pg_catalog.int8 "
            "FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_control_system() s "
            "JOIN pg_catalog.pg_authid a ON a.rolname={} JOIN pg_catalog.pg_authid b ON b.rolname={} "
            "WHERE d.datname={} AND NOT d.datistemplate AND a.oid<>b.oid "
            "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_authid r WHERE r.oid IN (a.oid,b.oid) AND "
            "(r.rolcanlogin OR r.rolpassword IS NOT NULL AND NOT ({} AND r.oid=a.oid) "
            "OR r.rolsuper OR r.rolinherit OR r.rolcreatedb "
            "OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)) "
            "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m "
            "WHERE m.member IN (a.oid,b.oid) OR m.roleid IN (a.oid,b.oid))",
            owner_role,
            successor_role,
            database,
            runtime_password is not None,
        )
    ).fetchone()
    if (
        row is None
        or len(row) != 6
        or any(
            type(value) is not (str if index == 0 else bool if index == 4 else int)
            for index, value in enumerate(row)
        )
    ):
        raise ApplicationDatabaseAdmissionError(
            "application admission database or sealed role identity changed"
        )
    require_sealed_runtime_password(connection, role=owner_role, password=runtime_password)
    observed = ApplicationDatabaseAdmissionTarget(
        system_identifier=str(row[0]),
        database=database,
        database_oid=int(str(row[1])),
        owner_role=owner_role,
        owner_oid=int(str(row[2])),
        successor_role=successor_role,
        successor_oid=int(str(row[3])),
    )
    return observed, row[4] is True, int(str(row[5]))


def capture_application_database_admission(
    connection: ApplicationDatabaseConnection,
    *,
    database: str,
    owner_role: str,
    successor_role: str,
    provisioner_role: str,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    runtime_password: str | None = None,
) -> ApplicationDatabaseAdmissionTarget:
    """Capture an open, sealed-role source identity for a caller's durable intent.

    This observation is not operation authority. Never recapture a closed target
    after a crash: recovery must use the exact previously persisted identity.
    Optional runtime_password must come from that protected original-credential
    recovery; it accepts only matching SCRAM on NOLOGIN, never a new writer.
    """
    with _maintenance_transaction(connection, database=database, provisioner_role=provisioner_role):
        target, allowed, owner = _read_target(
            connection, database=database, owner_role=owner_role, successor_role=successor_role,
            runtime_password=runtime_password,
        )
        if owner != target.owner_oid:
            raise ApplicationDatabaseAdmissionError("application admission source owner changed")
        if not allowed:
            raise ApplicationDatabaseAdmissionError(
                "application database admission is already closed"
            )
        _require_handoff_identity(connection, target, handoff_backend)
        connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        if connection.execute(
            application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid={} "
                "AND backend_start={}::pg_catalog.timestamptz AND usename={} "
                "AND datid={} AND backend_type='client backend')",
                handoff_backend.pid,
                handoff_backend.started_at,
                provisioner_role,
                target.database_oid,
            )
        ).fetchone() != (True,):
            raise ApplicationDatabaseAdmissionError("application database handoff backend changed")
        return target


def _require_handoff_identity(
    connection: ApplicationDatabaseConnection,
    target: ApplicationDatabaseAdmissionTarget,
    handoff_backend: ApplicationDatabaseHandoffBackend,
) -> None:
    if (
        handoff_backend.system_identifier != target.system_identifier
        or handoff_backend.database_oid != target.database_oid
        or connection.execute(
            application_sql(
                "SELECT pg_catalog.pg_postmaster_start_time()={}::pg_catalog.timestamptz",
                handoff_backend.server_started_at,
            )
        ).fetchone()
        != (True,)
    ):
        raise ApplicationDatabaseAdmissionError(
            "application database handoff server identity changed"
        )


def _checked_state(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    *, runtime_password: str | None,
) -> bool:
    observed, allowed, owner = _read_target(
        connection,
        database=target.database,
        owner_role=target.owner_role,
        successor_role=target.successor_role,
        runtime_password=runtime_password,
    )
    if observed != target or owner not in {target.owner_oid, target.successor_oid}:
        raise ApplicationDatabaseAdmissionError("application database admission identity changed")
    return allowed


def _set_admission(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    allowed: bool,
    runtime_password: str | None,
) -> None:
    with _maintenance_transaction(
        connection, database=target.database, provisioner_role=provisioner_role
    ):
        if _checked_state(connection, target, runtime_password=runtime_password) == allowed:
            return
        connection.execute(
            sql.SQL(
                "ALTER DATABASE {} ALLOW_CONNECTIONS " + ("true" if allowed else "false")
            ).format(sql.Identifier(target.database))
        )
        if _checked_state(connection, target, runtime_password=runtime_password) != allowed:
            raise ApplicationDatabaseAdmissionError(
                "application database admission did not converge"
            )


def close_application_database_admission(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    runtime_password: str | None = None,
) -> None:
    """Commit closure; existing/startup backends survive and must be drained separately."""
    _set_admission(connection, target=target, provisioner_role=provisioner_role, allowed=False,
                   runtime_password=runtime_password)


def reopen_application_database_admission(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    runtime_password: str | None = None,
) -> None:
    """Restore the captured open state; caller must independently admit operation recovery."""
    _set_admission(connection, target=target, provisioner_role=provisioner_role, allowed=True,
                   runtime_password=runtime_password)


def require_application_database_drained(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    runtime_password: str | None = None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
) -> None:
    """Observe startup locks BEFORE fresh backend statistics under stable closed admission.

    Assumes externally serialized admission/DDL and owned-client shutdown. Never
    signals any backend; outstanding or unknown work causes bounded refusal.
    The exact already-open administrator handoff backend must remain available.
    An optional, previously captured and durably bound installed coordination
    guard must also remain present with its exact lock. No other session is allowed.
    """
    _require_database_drained(
        connection, target=target, provisioner_role=provisioner_role, handoff_backend=handoff_backend,
        runtime_password=runtime_password, coordination_guard=coordination_guard, handoff_present=True,
    )


def require_application_database_recovery_drained(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    runtime_password: str | None = None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
) -> None:
    """Prove the saved handoff has exited, without reopening or replacing it.

    Requires the same externally serialized admission/DDL and owned-workload
    shutdown as normal drain. The saved target and optional guard must come from
    the existing durable operation, not fresh discovery. Every target session
    except that exact surviving guard must be absent; a live old handoff refuses.
    Postmaster restart or guard loss also refuses: this is connection-loss
    recovery, not permission to reacquire a lost operation lock or accept a new
    primary. It neither restores LOGIN nor admits a replacement peer. The caller
    must separately authorize and durably sequence those subsequent actions.
    """
    _require_database_drained(
        connection, target=target, provisioner_role=provisioner_role, handoff_backend=handoff_backend,
        runtime_password=runtime_password, coordination_guard=coordination_guard, handoff_present=False,
    )


def _require_database_drained(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    runtime_password: str | None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None,
    handoff_present: bool,
    reopen_after_drain: bool = False,
    require_successor_owner: bool = False,
) -> None:
    with _maintenance_transaction(
        connection, database=target.database, provisioner_role=provisioner_role
    ):
        if _checked_state(connection, target, runtime_password=runtime_password):
            raise ApplicationDatabaseAdmissionError("application database admission is not closed")
        _require_handoff_identity(connection, target, handoff_backend)
        if coordination_guard is not None:
            if coordination_guard.backend.pid == handoff_backend.pid:
                raise ApplicationDatabaseAdmissionError("application coordination and handoff backends overlap")
            _require_coordination_guard(connection, target, coordination_guard)
        if connection.execute(
            application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
                "AND classid='pg_catalog.pg_database'::pg_catalog.regclass AND objid={} "
                "AND mode='RowExclusiveLock')",
                target.database_oid,
            )
        ).fetchone() != (False,):
            raise ApplicationDatabaseAdmissionError("application database startup is still pending")
        # A startup publishes its backend status before releasing that lock.
        # Reading statistics first could miss BOTH during the transition.
        connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
        allowed_backend = application_sql(
            "(pid={} AND backend_start={}::pg_catalog.timestamptz AND usename={})",
            handoff_backend.pid, handoff_backend.started_at, provisioner_role,
        ) if handoff_present else sql.SQL("false")
        if coordination_guard is not None:
            allowed_backend = sql.SQL("({} OR {})").format(allowed_backend, application_sql(
                "(pid={} AND backend_start={}::pg_catalog.timestamptz AND usesysid={} "
                "AND usename='loom_rollout_readonly' AND application_name={})",
                coordination_guard.backend.pid, coordination_guard.backend.started_at,
                coordination_guard.role_oid, coordination_guard.application_name,
            ))
        expected_count = int(handoff_present) + int(coordination_guard is not None)
        if connection.execute(
            sql.SQL(
                "SELECT count(*)={} AND COALESCE(bool_and({} AND backend_type='client backend'),{}) "
                "FROM pg_catalog.pg_stat_activity WHERE datid={}"
            ).format(
                sql.Literal(expected_count), allowed_backend, sql.Literal(expected_count == 0),
                sql.Literal(target.database_oid),
            )
        ).fetchone() != (True,):
            raise ApplicationDatabaseAdmissionError("application database sessions are not drained")
        if connection.execute(
            application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE database={})",
                target.database,
            )
        ).fetchone() != (False,):
            raise ApplicationDatabaseAdmissionError(
                "application database has prepared transactions"
            )
        if _checked_state(connection, target, runtime_password=runtime_password):
            raise ApplicationDatabaseAdmissionError(
                "application database admission changed during drain"
            )
        if coordination_guard is not None:
            _require_coordination_guard(connection, target, coordination_guard)
        if require_successor_owner and connection.execute(application_sql(
            "SELECT datdba={} FROM pg_catalog.pg_database WHERE oid={}",
            target.successor_oid, target.database_oid,
        )).fetchone() != (True,):
            raise ApplicationDatabaseAdmissionError("application ownership transfer is not committed")
        if reopen_after_drain:
            connection.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(sql.Identifier(target.database)))
            if not _checked_state(connection, target, runtime_password=runtime_password):
                raise ApplicationDatabaseAdmissionError("application recovery admission did not reopen")
            if coordination_guard is not None:
                _require_coordination_guard(connection, target, coordination_guard)


def reopen_application_database_after_handoff(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    coordination_guard: ApplicationDatabaseCoordinationGuard,
    runtime_password: str,
) -> None:
    """Reopen after committed transfer with the original guard and exact live peer.

    The caller must already have verified the complete sealed schema profile.
    Admission remains closed on detected guard loss, startup work, surviving
    clients or uncommitted ownership. Runtime LOGIN is restored separately.
    """
    _require_database_drained(
        connection, target=target, provisioner_role=provisioner_role,
        handoff_backend=handoff_backend, coordination_guard=coordination_guard,
        runtime_password=runtime_password, handoff_present=True,
        reopen_after_drain=True, require_successor_owner=True,
    )


def reopen_application_database_for_handoff_recovery(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    runtime_password: str | None = None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
) -> None:
    """Commit recovery admission only after the old peer and clients have exited.

    Uses the saved operation identity; caller must durably authorize this edge
    before invoking it and retain exclusive admission/role/DDL/workload control.
    This is not runtime LOGIN restoration. Both application roles stay sealed.
    A surviving saved guard is checked before and after ALTER DATABASE in the
    same transaction; detected loss rolls admission back. Already-open admission
    refuses rather than adopting an uncertain prior commit. The caller must
    reconcile that state, admit and record any replacement peer, reclose and
    drain before ownership transfer. Server/guard loss is not recoverable here.
    """
    _require_database_drained(
        connection, target=target, provisioner_role=provisioner_role, handoff_backend=handoff_backend,
        runtime_password=runtime_password, coordination_guard=coordination_guard,
        handoff_present=False, reopen_after_drain=True,
    )


def reclose_application_database_for_handoff_recovery(
    connection: ApplicationDatabaseConnection,
    *,
    target: ApplicationDatabaseAdmissionTarget,
    provisioner_role: str,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    runtime_password: str | None = None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
) -> None:
    """Reconcile uncertain reopening under the saved server and surviving guard.

    The caller must first read back its durable recovery intent. Closure is
    idempotent but does not drain/adopt/signal any surviving connection. Unknown
    peers must exit before a new reopening; a recorded fresh peer must pass the
    normal exact-peer drain before transfer. Server/guard loss refuses mutation.
    """
    with _maintenance_transaction(connection, database=target.database, provisioner_role=provisioner_role):
        _checked_state(connection, target, runtime_password=runtime_password)
        _require_handoff_identity(connection, target, handoff_backend)
        if coordination_guard is not None:
            if coordination_guard.backend.pid == handoff_backend.pid:
                raise ApplicationDatabaseAdmissionError("application coordination and handoff backends overlap")
            _require_coordination_guard(connection, target, coordination_guard)
        # Even committed false may hide a prior backend's uncommitted reopen.
        # Always take the catalog update lock; a pending ALTER must finish first
        # or our bounded lock timeout refuses to certify closure.
        connection.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
        if _checked_state(connection, target, runtime_password=runtime_password):
            raise ApplicationDatabaseAdmissionError("application recovery admission did not close")
        if coordination_guard is not None:
            _require_coordination_guard(connection, target, coordination_guard)
