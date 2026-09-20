"""Internal ownership transaction, not a deployment or credential-retirement API.

The protected caller must admit its installed release and the private guard
definitions, ownership and ACLs, seal login/membership credentials in a prior committed phase,
reconcile sessions, and externally serialize relevant administrator DDL for the
whole transaction. These prerequisites cannot be inferred from a public catalog
digest. No production caller may use this substep until the durable operation,
credential/Secret lifecycle and recovery workflow supply those guarantees.
Session reconciliation requires an admitted startup barrier: a pre-seal login
can publish its backend statistics after an otherwise empty session observation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    _checked_state,
    _require_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_password import require_sealed_runtime_password
from loom.application_runtime_grants import application_runtime_grants_ddl
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    ApplicationSchemaAclProfile,
    ApplicationSchemaRevision,
    application_schema_profile,
    require_application_migration_revisions,
    require_application_schema_reference,
)
from loom.trial_writer_trigger_authority import application_trigger_owner_handoff_ddl


class ApplicationOwnershipTransferError(RuntimeError):
    """The exact sealed ownership transition could not be admitted."""


_ALIASES = {
    "application-owner",
    "guard-owner",
    "guard-migrator",
    "guard-agent",
    "guard-executor",
    "guard-observer",
    "guard-runtime",
    "provisioner",
}


def transfer_application_ownership(
    connection: ApplicationDatabaseConnection,
    *,
    owner_role: str,
    role_bindings: Mapping[str, str],
    runtime_password: str | None = None,
    admission_target: ApplicationDatabaseAdmissionTarget | None = None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
    schema_acl_profile: ApplicationSchemaAclProfile = "application-only",
    schema_revision: ApplicationSchemaRevision = "0151/guard_0035",
) -> None:
    """Transfer an admitted legacy database, or validate an exact sealed replay.

    Requires an active READ COMMITTED transaction and external DDL serialization.
    ``role_bindings`` retains the original legacy application-owner alias on
    retries. Neither source nor destination hashes are caller-selectable.
    Success retains locks until the caller commits; any failure rolls back this
    complete substep to a savepoint. Never commits or alters login credentials.
    runtime_password optionally admits the original SCRAM credential on the
    NOLOGIN runtime only; its protected caller must independently admit any
    overlapping same-password refresher and exclude all other role/DDL writers.
    A durably captured coordination guard may survive only with its matching
    admission target still closed. All other backends then remain excluded.
    schema_acl_profile must come from trusted operation requirements and remain
    fixed across transfer/replay/login; never infer it from observed grants.
    """
    legacy_profile = application_schema_profile(ownership="legacy-owner", acl_profile=schema_acl_profile)
    sealed_profile = application_schema_profile(ownership="sealed-owner", acl_profile=schema_acl_profile)
    bindings = dict(role_bindings)
    if (
        len(bindings) != len(_ALIASES)
        or set(bindings.values()) != _ALIASES
        or owner_role in bindings
        or any(
            re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None for role in (*bindings, owner_role)
        )
        or (admission_target is None) != (coordination_guard is None)
    ):
        raise ApplicationOwnershipTransferError("application ownership identities are invalid")
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise ApplicationOwnershipTransferError(
            "application ownership requires an active transaction"
        )
    identities = {alias: role for role, alias in bindings.items()}
    previous = identities["application-owner"]
    if admission_target is not None and (
        admission_target.owner_role != previous or admission_target.successor_role != owner_role
    ):
        raise ApplicationOwnershipTransferError("application ownership admission roles changed")
    sealed_bindings = {**bindings, previous: "application-runtime", owner_role: "application-owner"}
    with connection.transaction():
        settings = connection.execute(
            "SELECT pg_catalog.current_setting('search_path'), pg_catalog.current_setting('lock_timeout')"
        ).fetchone()
        assert settings is not None
        connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        _preflight(
            connection, previous=previous, owner=owner_role, provisioner=identities["provisioner"],
            runtime_password=runtime_password,
            admission_target=admission_target, coordination_guard=coordination_guard,
        )
        connection.execute(
            "SELECT pg_catalog.set_config('lock_timeout','1s',true) FROM pg_catalog.pg_settings "
            "WHERE name='lock_timeout' AND (setting::integer=0 OR setting::integer>1000)"
        )
        row = connection.execute(
            "SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_catalog.pg_database "
            "WHERE datname=pg_catalog.current_database()"
        ).fetchone()
        if row not in {(previous,), (owner_role,)}:
            raise ApplicationOwnershipTransferError("application database owner changed")
        replay = row == (owner_role,)
        profile = sealed_profile if replay else legacy_profile
        active_bindings = sealed_bindings if replay else bindings
        require_application_schema_reference(
            read_application_schema_inventory(connection, role_bindings=active_bindings),
            profile=profile, revision=schema_revision,
        )
        if not replay and connection.execute(
            application_sql(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend "
                "WHERE refclassid='pg_catalog.pg_authid'::regclass AND refobjid={}::regrole)",
                owner_role,
            )
        ).fetchone() != (False,):
            raise ApplicationOwnershipTransferError("application destination already has authority")
        # Exact shape admission precedes name discovery. ONLY prevents accidental
        # recursive locking; a changed inheritance tree also changes the pin.
        relations = connection.execute(
            "SELECT c.relname,c.relkind FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','c') "
            "ORDER BY c.relname"
        ).fetchall()
        for name, kind in relations:
            if kind in {"r", "p", "m"}:
                connection.execute(
                    sql.SQL("LOCK TABLE ONLY {} IN ACCESS EXCLUSIVE MODE NOWAIT").format(
                        sql.Identifier("public", str(name))
                    )
                )
        require_application_schema_reference(
            read_application_schema_inventory(connection, role_bindings=active_bindings),
            profile=profile, revision=schema_revision,
        )
        # A replaced view/function must be rejected by catalog admission BEFORE
        # any data query. The table lock also excludes concurrent marker writers.
        require_application_migration_revisions(connection, revision=schema_revision)
        if not replay:
            _transfer(
                connection,
                relations=relations,
                previous=previous,
                owner=owner_role,
                guard=identities["guard-owner"],
                coordination_guard=coordination_guard, schema_revision=schema_revision,
            )
        else:
            connection.execute(
                application_trigger_owner_handoff_ddl(
                    previous_owner=previous,
                    application_owner=owner_role,
                    guard_owner=identities["guard-owner"],
                    coordination_guard=coordination_guard, schema_revision=schema_revision,
                )
            )
        connection.execute(
            application_runtime_grants_ddl(
                owner_role=owner_role, runtime_role=previous,
                allow_password_on_nologin_runtime=runtime_password is not None,
            )
        )
        require_sealed_runtime_password(connection, role=previous, password=runtime_password)
        require_application_schema_reference(
            read_application_schema_inventory(connection, role_bindings=sealed_bindings),
            profile=sealed_profile, revision=schema_revision,
        )
        if admission_target is not None and coordination_guard is not None:
            _require_guarded_transfer(connection, admission_target, coordination_guard, runtime_password)
        for setting, value in zip(("search_path", "lock_timeout"), settings, strict=True):
            connection.execute(
                application_sql("SELECT pg_catalog.set_config({},{},true)", setting, value)
            )


def _preflight(
    connection: ApplicationDatabaseConnection,
    *,
    previous: str,
    owner: str,
    provisioner: str,
    runtime_password: str | None,
    admission_target: ApplicationDatabaseAdmissionTarget | None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None,
) -> None:
    if connection.execute(
        "SELECT pg_catalog.current_setting('transaction_isolation')"
    ).fetchone() != ("read committed",):
        raise ApplicationOwnershipTransferError("application ownership requires READ COMMITTED")
    if connection.execute(
        application_sql(
            "SELECT current_user=session_user AND current_user={} AND rolsuper "
            "FROM pg_catalog.pg_roles WHERE rolname=current_user",
            provisioner,
        )
    ).fetchone() != (True,):
        raise ApplicationOwnershipTransferError(
            "application ownership requires protected administrator"
        )
    if connection.execute(
        application_sql(
            "SELECT count(*) FROM pg_catalog.pg_authid WHERE rolname=ANY({}) "
            "AND NOT rolcanlogin AND NOT rolinherit AND NOT rolsuper AND NOT rolcreatedb "
            "AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls "
            "AND (rolpassword IS NULL OR {} AND rolname={})",
            [previous, owner],
            runtime_password is not None,
            previous,
        )
    ).fetchone() != (2,):
        raise ApplicationOwnershipTransferError("application ownership roles are not sealed")
    require_sealed_runtime_password(connection, role=previous, password=runtime_password)
    if connection.execute(
        application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r "
            "ON r.oid=m.member OR r.oid=m.roleid WHERE r.rolname=ANY({}))",
            [previous, owner],
        )
    ).fetchone() != (False,):
        raise ApplicationOwnershipTransferError("application ownership memberships are not sealed")
    if admission_target is not None and coordination_guard is not None:
        _require_guarded_transfer(connection, admission_target, coordination_guard, runtime_password)
    connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
    excluded_session = sql.SQL("NOT r.rolsuper") if coordination_guard is None else application_sql(
        "NOT (a.pid={} AND a.backend_start={}::pg_catalog.timestamptz AND a.usesysid={} "
        "AND a.application_name={} AND a.backend_type='client backend')",
        coordination_guard.backend.pid, coordination_guard.backend.started_at,
        coordination_guard.role_oid, coordination_guard.application_name,
    )
    if connection.execute(
        sql.SQL("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a LEFT JOIN pg_catalog.pg_roles r "
        "ON r.oid=a.usesysid WHERE a.datname=pg_catalog.current_database() AND {} "
        "AND a.pid<>pg_catalog.pg_backend_pid()) OR EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts "
        "WHERE database=pg_catalog.current_database())").format(excluded_session)
    ).fetchone() != (False,):
        raise ApplicationOwnershipTransferError(
            "application ownership requires reconciled sessions"
        )
    require_application_role_scope(connection, roles=[previous, owner])


def _require_guarded_transfer(
    connection: ApplicationDatabaseConnection, target: ApplicationDatabaseAdmissionTarget,
    guard: ApplicationDatabaseCoordinationGuard, runtime_password: str | None,
) -> None:
    if (connection.execute(application_sql("SELECT pg_catalog.current_database()={}", target.database)).fetchone() != (True,)
            or _checked_state(connection, target, runtime_password=runtime_password)):
        raise ApplicationOwnershipTransferError("application ownership requires exact closed admission")
    _require_coordination_guard(connection, target, guard)
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
        "AND classid='pg_catalog.pg_database'::pg_catalog.regclass AND objid={} AND mode='RowExclusiveLock')",
        target.database_oid,
    )).fetchone() != (False,):
        raise ApplicationOwnershipTransferError("application ownership startup is still pending")
    connection.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
    if connection.execute(application_sql(
        "SELECT count(*)=2 AND COALESCE(bool_and(backend_type='client backend' AND "
        "(pid=pg_catalog.pg_backend_pid() OR pid={} AND backend_start={}::pg_catalog.timestamptz "
        "AND usesysid={} AND application_name={})),false) FROM pg_catalog.pg_stat_activity WHERE datid={}",
        guard.backend.pid, guard.backend.started_at, guard.role_oid, guard.application_name, target.database_oid,
    )).fetchone() != (True,):
        raise ApplicationOwnershipTransferError("application ownership requires reconciled sessions")
    if connection.execute(application_sql(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_prepared_xacts WHERE database={})", target.database,
    )).fetchone() != (False,):
        raise ApplicationOwnershipTransferError("application ownership has prepared transactions")


def require_application_role_scope(
    connection: ApplicationDatabaseConnection, *, roles: list[str]
) -> None:
    """Catalog-only role scope shared by pre-transfer login sealing and transfer.

    The caller must validate administrator/role/database identity, use a protected
    search path and READ COMMITTED, and externally serialize administrator DDL.
    This observes dependencies only, not trusted definitions or session retirement.
    """
    if connection.execute(
        application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend s JOIN pg_catalog.pg_roles r "
            "ON s.refclassid='pg_catalog.pg_authid'::regclass AND s.refobjid=r.oid "
            "JOIN pg_catalog.pg_database d ON d.datname=pg_catalog.current_database() "
            "WHERE r.rolname=ANY({}) AND NOT (s.dbid=d.oid OR s.dbid=0 "
            "AND s.classid='pg_catalog.pg_database'::regclass AND s.objid=d.oid))",
            roles,
        )
    ).fetchone() != (False,):
        raise ApplicationOwnershipTransferError(
            "application ownership has foreign role dependencies"
        )
    # Transfer checks public references with its exact shape pin; pre-transfer
    # sealing uses only this catalog scope and cannot certify trusted content.
    # The only admitted private references are USAGE/EXECUTE grants for the
    # fixed definer bridges;
    # no foreign object ownership, ACL or policy/default authority is adopted.
    if connection.execute(
        application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend s "
            "JOIN pg_catalog.pg_roles r ON s.refclassid='pg_catalog.pg_authid'::regclass AND s.refobjid=r.oid "
            "JOIN pg_catalog.pg_database d ON d.datname=pg_catalog.current_database() "
            "WHERE r.rolname=ANY({}) AND s.dbid=d.oid AND NOT COALESCE(("
            "s.classid='pg_catalog.pg_class'::regclass AND EXISTS (SELECT 1 FROM pg_catalog.pg_class c "
            "WHERE c.oid=s.objid AND c.relnamespace='public'::regnamespace) OR "
            "s.classid='pg_catalog.pg_type'::regclass AND EXISTS (SELECT 1 FROM pg_catalog.pg_type t "
            "WHERE t.oid=s.objid AND t.typnamespace='public'::regnamespace) OR "
            "s.classid='pg_catalog.pg_proc'::regclass AND EXISTS (SELECT 1 FROM pg_catalog.pg_proc p "
            "WHERE p.oid=s.objid AND p.pronamespace='public'::regnamespace) OR "
            "s.classid='pg_catalog.pg_namespace'::regclass AND s.objid='public'::regnamespace OR "
            "s.classid='pg_catalog.pg_default_acl'::regclass AND EXISTS (SELECT 1 FROM pg_catalog.pg_default_acl a "
            "WHERE a.oid=s.objid AND (a.defaclnamespace='public'::regnamespace "
            "OR a.defaclnamespace=0 AND a.defaclrole=r.oid)) OR "
            "s.deptype='a' AND (s.classid='pg_catalog.pg_namespace'::regclass "
            "AND s.objid=pg_catalog.to_regnamespace('loom_capacity_guard') OR "
            "s.classid='pg_catalog.pg_proc'::regclass AND s.objid IN ("
            "pg_catalog.to_regprocedure('loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)'),"
            "pg_catalog.to_regprocedure('loom_capacity_guard.transform_protected_runtime_trial_requeue(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)')))),false))",
            roles,
        )
    ).fetchone() != (False,):
        raise ApplicationOwnershipTransferError(
            "application ownership has foreign object authority"
        )
    # Sealing may precede guard installation. Missing objects confer no authority;
    # transfer separately requires the complete trusted guard/application shape.
    if connection.execute(
        application_sql(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname=ANY({}) AND ("
            "COALESCE(pg_catalog.has_schema_privilege(r.oid,pg_catalog.to_regnamespace('loom_capacity_guard'),'CREATE'),false) OR "
            "COALESCE(pg_catalog.has_schema_privilege(r.oid,pg_catalog.to_regnamespace('loom_capacity_guard'),'USAGE WITH GRANT OPTION'),false) OR "
            "COALESCE(pg_catalog.has_function_privilege(r.oid, "
            "pg_catalog.to_regprocedure('loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)'), "
            "'EXECUTE WITH GRANT OPTION'),false) OR COALESCE(pg_catalog.has_function_privilege(r.oid, "
            "pg_catalog.to_regprocedure('loom_capacity_guard.transform_protected_runtime_trial_requeue(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)'), "
            "'EXECUTE WITH GRANT OPTION'),false)))",
            roles,
        )
    ).fetchone() != (False,):
        raise ApplicationOwnershipTransferError(
            "application ownership has residual private authority"
        )


def _transfer(
    connection: ApplicationDatabaseConnection,
    *,
    relations: list[tuple[object, ...]],
    previous: str,
    owner: str,
    guard: str,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None,
    schema_revision: ApplicationSchemaRevision,
) -> None:
    # Table ownership carries its row types, indexes, and owned sequences.
    for name, kind in relations:
        if kind in {"r", "p", "v", "m", "c"}:
            prefix = {
                "r": "TABLE ONLY",
                "p": "TABLE ONLY",
                "v": "VIEW",
                "m": "MATERIALIZED VIEW",
                "c": "TYPE",
            }[str(kind)]
            connection.execute(
                sql.SQL("ALTER " + prefix + " {} OWNER TO {}").format(
                    sql.Identifier("public", str(name)), sql.Identifier(owner)
                )
            )
    connection.execute(
        application_trigger_owner_handoff_ddl(
            previous_owner=previous, application_owner=owner, guard_owner=guard,
            coordination_guard=coordination_guard, schema_revision=schema_revision,
        )
    )
    for name, kind in relations:
        if kind == "S":
            connection.execute(
                sql.SQL("ALTER SEQUENCE {} OWNER TO {}").format(
                    sql.Identifier("public", str(name)), sql.Identifier(owner)
                )
            )
    for name, arguments in connection.execute(
        "SELECT p.proname,pg_catalog.pg_get_function_identity_arguments(p.oid) FROM pg_catalog.pg_proc p "
        "JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' ORDER BY p.proname,p.oid"
    ).fetchall():
        # Signature text comes only from the exact admitted catalog with protected
        # search_path; it is not caller input or an arbitrary discovery snapshot.
        connection.execute(
            sql.SQL("ALTER ROUTINE {}(" + str(arguments) + ") OWNER TO {}").format(
                sql.Identifier("public", str(name)), sql.Identifier(owner)
            )
        )
    for name, kind in connection.execute(
        "SELECT t.typname,t.typtype FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace "
        "WHERE n.nspname='public' AND t.typrelid=0 AND t.typelem=0 ORDER BY t.typname"
    ).fetchall():
        connection.execute(
            sql.SQL("ALTER " + ("DOMAIN" if kind == "d" else "TYPE") + " {} OWNER TO {}").format(
                sql.Identifier("public", str(name)), sql.Identifier(owner)
            )
        )
    database = connection.execute("SELECT pg_catalog.current_database()").fetchone()
    assert database is not None
    connection.execute(
        sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
            sql.Identifier(str(database[0])),
            sql.Identifier(owner),
        )
    )
    connection.execute(sql.SQL("ALTER SCHEMA public OWNER TO {}").format(sql.Identifier(owner)))
    connection.execute(
        sql.SQL("REVOKE USAGE ON SCHEMA loom_capacity_guard FROM {}").format(
            sql.Identifier(previous)
        )
    )
