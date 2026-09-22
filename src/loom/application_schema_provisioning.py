"""Disposable schema-reference provisioning for published application lineage.

Only the pinned-container reference generator and compatibility tests use this
module. Retained SQL role recipes reconstruct historical schema permissions;
there is no fleet installer, Kubernetes client or remote lifecycle authority.
"""

from __future__ import annotations

import asyncio
import re
import secrets
from dataclasses import dataclass
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from sqlalchemy.engine import URL, make_url

from loom.trial_writer_trigger_authority import trial_writer_trigger_retirement_ddl


@dataclass(frozen=True)
class ReferenceIdentity:
    name: str
    database: str
    db_role: str


def derive_identity(name: str) -> ReferenceIdentity:
    if re.fullmatch("(?:reference|transfer)-[a-z0-9-]{1,20}", name) is None:
        raise ValueError("reference database requires an isolated reference/transfer name")
    database = "loom_dev_" + name.replace("-", "_")
    return ReferenceIdentity(name=name, database=database, db_role=database)


def _role_names(identity: ReferenceIdentity) -> tuple[str, str, str, str, str, str]:
    prefix = "loom_cap_" + identity.name.replace("-", "_")
    return (
        prefix + "_owner",
        prefix + "_migrator",
        prefix + "_agent",
        prefix + "_executor",
        prefix + "_observer",
        prefix + "_runtime",
    )


_SAFE_IDENTIFIER = re.compile("^[a-z][a-z0-9_]*$")
_HEX_PASSWORD = re.compile("^[0-9a-f]{16,}$")


class UnsafeIdentifierError(ValueError):
    """A database/role identifier or password failed the safety check."""


def _require_safe_identifier(value: str, kind: str) -> None:
    if not _SAFE_IDENTIFIER.fullmatch(value):
        raise UnsafeIdentifierError(f"unsafe {kind} identifier: {value!r}")


def _require_hex_password(password: str) -> None:
    if not _HEX_PASSWORD.fullmatch(password):
        raise UnsafeIdentifierError("dev-instance role password must be >=16 hex chars")


def render_role_convergence_sql(identity: ReferenceIdentity, password: str) -> str:
    """Render one idempotent role-convergence transaction for the maintenance DB.

    Creates (if absent) the instance's LOGIN role and sets its password +
    least-privilege attributes. Run this on the maintenance database *before*
    ``CREATE DATABASE ... OWNER <role>``. Pure: no I/O.
    """
    _require_safe_identifier(identity.db_role, "role")
    _require_hex_password(password)
    role = identity.db_role
    return (
        f"""\nBEGIN;\nDO $loom$\nBEGIN\n  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{role}') THEN\n    CREATE ROLE "{role}";\n  END IF;\nEND\n$loom$;\nALTER ROLE "{role}" WITH LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE\n  NOREPLICATION NOBYPASSRLS PASSWORD '{password}';\nCOMMIT;\nSELECT 'dev-instance-role-converged-v1';\n""".strip()
        + "\n"
    )


def render_create_database_sql(identity: ReferenceIdentity) -> str:
    """Render the ``CREATE DATABASE`` statement (owner = the instance role).

    Emitted separately because ``CREATE DATABASE`` cannot run inside a
    transaction; the executor issues it in autocommit only when the database
    is absent (Postgres has no ``CREATE DATABASE IF NOT EXISTS``). Pure.
    """
    _require_safe_identifier(identity.database, "database")
    _require_safe_identifier(identity.db_role, "role")
    return f'CREATE DATABASE "{identity.database}" OWNER "{identity.db_role}";'


class DevInstanceRuntimeError(RuntimeError):
    """Bounded runtime failure safe to persist and surface."""


def instance_database_url(admin_url: str, identity: ReferenceIdentity, password: str) -> str:
    """Derive the role-scoped instance DSN without altering endpoint/TLS query."""
    parsed = urlsplit(admin_url)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        raise ValueError("shared fixture database URL must use PostgreSQL")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("shared fixture database URL must include a host")
    host = f"[{hostname}]" if ":" in hostname else hostname
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    netloc = f"{quote(identity.db_role, safe='')}:{quote(password, safe='')}@{host}"
    scheme = "postgresql+psycopg" if parsed.scheme == "postgresql+psycopg" else "postgresql"
    return urlunsplit((scheme, netloc, f"/{identity.database}", parsed.query, ""))


def fixture_database_url(admin_url: str, database: str) -> str:
    """Retarget a protected fixture-admin URL to one validated database name."""
    if not database or any(
        char not in "abcdefghijklmnopqrstuvwxyz0123456789_" for char in database
    ):
        raise ValueError("fixture database name is invalid")
    parsed = urlsplit(admin_url)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        raise ValueError("shared fixture database URL must use PostgreSQL")
    return urlunsplit((parsed.scheme, parsed.netloc, f"/{database}", parsed.query, ""))


@dataclass(slots=True)
class PsycopgSharedFixtureSqlExecutor:
    admin_url: str

    @property
    def _connect_url(self) -> str:
        return self.admin_url.replace("postgresql+psycopg://", "postgresql://", 1)

    async def apply_role_and_database(
        self, identity: ReferenceIdentity, *, role_sql: str, create_database_sql: str
    ) -> None:
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url, autocommit=True
            ) as connection:
                await connection.execute(role_sql)
                exists = await connection.execute(
                    "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (identity.database,)
                )
                if await exists.fetchone() is None:
                    await connection.execute(create_database_sql)
                await connection.execute(
                    sql.SQL("REVOKE CONNECT, TEMPORARY ON DATABASE {} FROM PUBLIC").format(
                        sql.Identifier(identity.database)
                    )
                )
                await connection.execute(
                    sql.SQL("GRANT CONNECT, TEMPORARY ON DATABASE {} TO {}").format(
                        sql.Identifier(identity.database), sql.Identifier(identity.db_role)
                    )
                )
        except Exception:
            raise DevInstanceRuntimeError("shared fixture database provisioning failed") from None

    async def drop_database_and_role(self, identity: ReferenceIdentity) -> None:
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url, autocommit=True
            ) as connection:
                await connection.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                    (identity.database,),
                )
                await connection.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(identity.database))
                )
                await connection.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(identity.db_role))
                )
        except Exception:
            raise DevInstanceRuntimeError("shared fixture database cleanup failed") from None


class PersonalDevCapacityInstallationError(RuntimeError):
    """Trusted local capacity installation could not be converged exactly."""


@dataclass(frozen=True, slots=True)
class ApplicationOwnerBinding:
    """Operator-owned identity for guard provisioning after application handoff.

    This is a selection constraint, not proof of complete ownership retirement.
    It must come from the protected lifecycle operation, never candidate input
    or a role discovered in the live catalog. No production installer selects
    this mode until ownership, credentials and recovery are composed.
    """

    database: str
    runtime_role: str
    owner_role: str

    def __post_init__(self) -> None:
        if self.owner_role == self.runtime_role or any(
            re.fullmatch("[a-z][a-z0-9_]{0,62}", value) is None
            for value in (self.database, self.runtime_role, self.owner_role)
        ):
            raise ValueError("application owner binding is invalid")


def _retarget_database_url(admin_url: str, *, database: str, username: str, password: str) -> str:
    parsed = make_url(fixture_database_url(admin_url, database))
    drivername = "postgresql+psycopg" if parsed.drivername == "postgresql" else parsed.drivername
    return URL.create(
        drivername=drivername,
        username=username,
        password=password,
        host=parsed.host,
        port=parsed.port,
        database=parsed.database,
        query=parsed.query,
    ).render_as_string(hide_password=False)


@dataclass(frozen=True, slots=True)
class CapacityDatabaseCredentials:
    reporter_incarnation: UUID
    reporter_token: str
    migrator_password: str
    agent_password: str
    observer_password: str
    runtime_password: str


def _new_credentials(
    *, reporter_incarnation: UUID | None = None, runtime_password: str | None = None
) -> CapacityDatabaseCredentials:
    return CapacityDatabaseCredentials(
        reporter_incarnation=reporter_incarnation or uuid4(),
        reporter_token=secrets.token_urlsafe(48),
        migrator_password=secrets.token_urlsafe(48),
        agent_password=secrets.token_urlsafe(48),
        observer_password=secrets.token_urlsafe(48),
        runtime_password=runtime_password or secrets.token_urlsafe(48),
    )


class ReferenceDatabase:
    """Reconstruct published role grants in a disposable reference database."""

    def __init__(
        self,
        admin_url: str,
        *,
        migration_timeout_seconds: float = 180.0,
        application_owner_binding: ApplicationOwnerBinding | None = None,
    ) -> None:
        self._admin_url = admin_url
        self._migration_timeout_seconds = migration_timeout_seconds
        self._transient_role_admin = False  # Frozen baseline recipe consults this fixed mode.
        self._application_owner_binding = application_owner_binding

    def _application_owner(self, identity: ReferenceIdentity) -> str:
        binding = self._application_owner_binding
        if binding is None:
            return identity.db_role
        if (
            binding.database != identity.database
            or binding.runtime_role != identity.db_role
            or binding.owner_role in _role_names(identity)
        ):
            raise PersonalDevCapacityInstallationError("application owner binding does not match")
        return binding.owner_role

    async def _verify_application_owner_binding(
        self, connection: psycopg.AsyncConnection[tuple[object, ...]], identity: ReferenceIdentity
    ) -> None:
        if self._application_owner_binding is None:
            return
        application_owner = self._application_owner(identity)
        await self._verify_application_owner_scope(connection, identity)
        observed = await connection.execute(
            "SELECT current_database(), pg_catalog.pg_get_userbyid(d.datdba), pg_catalog.pg_get_userbyid(c.relowner), NOT r.rolcanlogin AND NOT r.rolinherit AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls FROM pg_catalog.pg_database AS d JOIN pg_catalog.pg_roles AS r ON r.rolname = %s JOIN pg_catalog.pg_class AS c ON c.oid = 'public.trials'::regclass WHERE d.datname = current_database()",
            (application_owner,),
        )
        if await observed.fetchone() != (
            identity.database,
            application_owner,
            application_owner,
            True,
        ):
            raise PersonalDevCapacityInstallationError("application owner binding is not exact")
        memberships = await connection.execute(
            "SELECT member.rolname, granted.rolname, m.admin_option, m.inherit_option, m.set_option FROM pg_catalog.pg_auth_members AS m JOIN pg_catalog.pg_roles AS member ON member.oid = m.member JOIN pg_catalog.pg_roles AS granted ON granted.oid = m.roleid WHERE member.rolname = %s OR granted.rolname = %s",
            (application_owner, application_owner),
        )
        expected = (
            [(_role_names(identity)[1], application_owner, False, True, True)]
            if self._transient_role_admin
            else []
        )
        if await memberships.fetchall() != expected:
            raise PersonalDevCapacityInstallationError(
                "application owner binding memberships changed"
            )

    async def _verify_application_owner_scope(
        self, connection: psycopg.AsyncConnection[tuple[object, ...]], identity: ReferenceIdentity
    ) -> None:
        """Never adopt a role with authority or dependencies outside this database."""
        observed = await connection.execute(
            "SELECT NOT r.rolcanlogin AND NOT r.rolinherit AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend AS s WHERE s.refclassid = 'pg_catalog.pg_authid'::regclass AND s.refobjid = r.oid AND NOT (s.dbid = COALESCE(d.oid, 0) AND s.dbid <> 0 OR s.dbid = 0 AND s.classid = 'pg_catalog.pg_database'::regclass AND s.objid = COALESCE(d.oid, 0))) FROM pg_catalog.pg_roles AS r LEFT JOIN pg_catalog.pg_database AS d ON d.datname = %s WHERE r.rolname = %s",
            (identity.database, self._application_owner(identity)),
        )
        if await observed.fetchone() != (True,):
            raise PersonalDevCapacityInstallationError("application owner binding scope changed")

    @property
    def _connect_url(self) -> str:
        return self._admin_url.replace("postgresql+psycopg://", "postgresql://", 1)

    async def _converge_roles(
        self, identity: ReferenceIdentity, credentials: CapacityDatabaseCredentials
    ) -> tuple[str, str, str, str, str, str, str, str]:
        owner, migrator, agent, executor, observer, runtime = _role_names(identity)
        application_owner = self._application_owner(identity)
        migrator_url = _retarget_database_url(
            self._admin_url,
            database=identity.database,
            username=migrator,
            password=credentials.migrator_password,
        )
        agent_url = _retarget_database_url(
            self._admin_url,
            database=identity.database,
            username=agent,
            password=credentials.agent_password,
        )
        if self._application_owner_binding is not None:
            try:
                async with await psycopg.AsyncConnection.connect(
                    fixture_database_url(self._connect_url, identity.database)
                ) as connection:
                    await self._verify_application_owner_binding(connection, identity)
            except PersonalDevCapacityInstallationError:
                raise
            except Exception:
                raise PersonalDevCapacityInstallationError(
                    "application owner binding verification failed"
                ) from None
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url, autocommit=True
            ) as connection:
                protected_roles = sql.SQL(", ").join(
                    sql.Identifier(role)
                    for role in (owner, migrator, agent, executor, observer, runtime)
                )
                for role in (owner, migrator, agent, executor, observer, runtime):
                    await connection.execute(
                        sql.SQL(
                            "DO $loom$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {}) THEN CREATE ROLE {}; END IF; END $loom$"
                        ).format(sql.Literal(role), sql.Identifier(role))
                    )
                    await connection.execute(
                        sql.SQL("ALTER ROLE {} RESET ALL").format(sql.Identifier(role))
                    )
                restricted_nologin = "NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
                restricted_login = "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD {}"
                for role in (owner, executor):
                    await connection.execute(
                        sql.SQL("ALTER ROLE {} " + restricted_nologin).format(sql.Identifier(role))
                    )
                await connection.execute(
                    sql.SQL("ALTER ROLE {} " + restricted_login).format(
                        sql.Identifier(runtime), sql.Literal(credentials.runtime_password)
                    )
                )
                credential_roles: tuple[tuple[str, str, str], ...] = (
                    (migrator, credentials.migrator_password, "INHERIT"),
                    (agent, credentials.agent_password, "NOINHERIT"),
                    (observer, credentials.observer_password, "NOINHERIT"),
                )
                for role, password, inherit in credential_roles:
                    credential_attributes = f"LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE {inherit} NOREPLICATION NOBYPASSRLS PASSWORD {{}}"
                    await connection.execute(
                        sql.SQL("ALTER ROLE {} " + credential_attributes).format(
                            sql.Identifier(role), sql.Literal(password)
                        )
                    )
                await connection.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(owner), sql.Identifier(migrator)
                    )
                )
                await connection.execute(
                    sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                        sql.Identifier(identity.database), protected_roles
                    )
                )
                await connection.execute(
                    sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}, {}, {}").format(
                        sql.Identifier(identity.database),
                        sql.Identifier(migrator),
                        sql.Identifier(agent),
                        sql.Identifier(observer),
                        sql.Identifier(runtime),
                    )
                )
                await connection.execute(
                    sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                        sql.Identifier(identity.database), sql.Identifier(owner)
                    )
                )
                memberships = await connection.execute(
                    "SELECT member.rolname AS member, granted.rolname AS granted FROM pg_auth_members m JOIN pg_roles member ON member.oid = m.member JOIN pg_roles granted ON granted.oid = m.roleid WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s) ORDER BY member.rolname, granted.rolname",
                    (
                        [owner, migrator, agent, executor, observer, runtime],
                        [owner, migrator, agent, executor, observer, runtime],
                    ),
                )
                observed = {(row[0], row[1]) for row in await memberships.fetchall()}
                expected_memberships = {(migrator, owner)}
                if observed != expected_memberships:
                    for member, granted in sorted(observed - expected_memberships):
                        await connection.execute(
                            sql.SQL("REVOKE {} FROM {}").format(
                                sql.Identifier(granted), sql.Identifier(member)
                            )
                        )
                    raise PersonalDevCapacityInstallationError(
                        "protected capacity roles have unexpected memberships"
                    )
            database_admin_url = fixture_database_url(self._admin_url, identity.database)
            async with await psycopg.AsyncConnection.connect(
                database_admin_url.replace("postgresql+psycopg://", "postgresql://", 1)
            ) as connection:
                async with connection.transaction():
                    await self._verify_application_owner_binding(connection, identity)
                    protected_roles = sql.SQL(", ").join(
                        sql.Identifier(role)
                        for role in (owner, migrator, agent, executor, observer, runtime)
                    )
                    for object_kind in (
                        "SCHEMA public",
                        "ALL TABLES IN SCHEMA public",
                        "ALL SEQUENCES IN SCHEMA public",
                        "ALL FUNCTIONS IN SCHEMA public",
                    ):
                        await connection.execute(
                            sql.SQL("REVOKE ALL PRIVILEGES ON {} FROM {}").format(
                                sql.SQL(object_kind), protected_roles
                            )
                        )
                    application_role_result = await connection.execute(
                        "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
                        (identity.db_role,),
                    )
                    application_role_row = await application_role_result.fetchone()
                    if application_role_row is None:
                        raise PersonalDevCapacityInstallationError(
                            "application database role lookup failed"
                        )
                    application_role_exists = bool(application_role_row[0])
                    schemas_result = await connection.execute(
                        "SELECT namespace.nspname, EXISTS (SELECT 1 FROM aclexplode(COALESCE(namespace.nspacl, acldefault('n', namespace.nspowner))) AS privilege WHERE privilege.grantee = 0 AND privilege.privilege_type = 'USAGE') AS public_usage FROM pg_namespace AS namespace WHERE nspname <> 'information_schema' AND nspname NOT LIKE 'pg\\_%' ESCAPE '\\' ORDER BY nspname"
                    )
                    for schema_name, public_usage in await schemas_result.fetchall():
                        for object_kind in (
                            "SCHEMA {}",
                            "ALL TABLES IN SCHEMA {}",
                            "ALL SEQUENCES IN SCHEMA {}",
                            "ALL FUNCTIONS IN SCHEMA {}",
                        ):
                            await connection.execute(
                                sql.SQL(
                                    "REVOKE ALL PRIVILEGES ON " + object_kind + " FROM {}"
                                ).format(sql.Identifier(schema_name), sql.Identifier(executor))
                            )
                            await connection.execute(
                                sql.SQL(
                                    "REVOKE ALL PRIVILEGES ON " + object_kind + " FROM {}"
                                ).format(sql.Identifier(schema_name), sql.Identifier(observer))
                            )
                            await connection.execute(
                                sql.SQL(
                                    "REVOKE ALL PRIVILEGES ON " + object_kind + " FROM {}"
                                ).format(sql.Identifier(schema_name), sql.Identifier(runtime))
                            )
                        if public_usage:
                            await connection.execute(
                                sql.SQL("REVOKE USAGE ON SCHEMA {} FROM PUBLIC").format(
                                    sql.Identifier(schema_name)
                                )
                            )
                            if application_role_exists and schema_name != "loom_capacity_guard":
                                await connection.execute(
                                    sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                                        sql.Identifier(schema_name),
                                        sql.Identifier(identity.db_role),
                                    )
                                )
                    await connection.execute(
                        sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL("GRANT REFERENCES (id) ON TABLE public.trials TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL("GRANT TRIGGER ON TABLE public.trials TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    helper_authority = await connection.execute(
                        "SELECT current_user, pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE oid = 'public.trials'::regclass"
                    )
                    helper_roles = await helper_authority.fetchone()
                    expected_helper_owners = {application_owner}
                    if self._application_owner_binding is None and helper_roles is not None:
                        expected_helper_owners.add(helper_roles[0])
                    if helper_roles is None or helper_roles[1] not in expected_helper_owners:
                        raise PersonalDevCapacityInstallationError(
                            "trial retirement application owner is unexpected"
                        )
                    provisioner_role, helper_owner = helper_roles
                    if helper_owner != provisioner_role:
                        await connection.execute(
                            sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(helper_owner))
                        )
                    await connection.execute(trial_writer_trigger_retirement_ddl(guard_owner=owner))
                    if helper_owner != provisioner_role:
                        await connection.execute(
                            sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(provisioner_role))
                        )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, team_id, task_id, config, state, requires_caps, submit_priority, batch_id, idempotency_key, sample_idx, combination_idx, provider_connection_id, provider_model_id, submitted_by_user_id, usage_attributed_user_id, usage_attributed_actor, family_key, lifecycle_authority_id, submitted_at, started_at, cancellation_requested_at, cancellation_observed_at, finished_at, next_attempt_at, autoscaler_pool_name, worker_id, attempt_count, execution_route_json) ON TABLE public.trials TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (lifecycle_authority_id, state, requires_caps, worker_id, claimed_at, pre_start_heartbeat_at, failure_reason, failure_message, attempt_count, next_attempt_at, cancellation_requested_at, cancellation_observed_at, finished_at) ON TABLE public.trials TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (id, team_id, task_id, config, requires_caps, state, submit_priority, batch_id, idempotency_key, sample_idx, combination_idx, provider_connection_id, provider_model_id, submitted_by_user_id, usage_attributed_user_id, usage_attributed_actor, family_key) ON TABLE public.trials TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id) ON TABLE public.data_lifecycle_authorities TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (environment, namespace, team_id, data_class, owner_kind, owner_id, created_at, expires_at, pinned, state) ON TABLE public.data_lifecycle_authorities TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT REFERENCES (id) ON TABLE public.data_lifecycle_authorities TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, checksum, config, source, source_provenance) ON TABLE public.tasks TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, materialization_key, task_id, task_checksum, cpu_arch, task_config, task_source, task_source_provenance, state, registry_images, ready_publication_operation_id) ON TABLE public.task_image_materializations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (trial_id, materialization_id) ON TABLE public.trial_task_image_materializations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (state) ON TABLE public.task_image_materializations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, trial_id, combination_idx, mix_mode, k1, k2, teacher_episodes, beta, seed, prng_version, student_model_snapshot, teacher_model_snapshot, provider_connection_id, pricing_snapshot, capability_snapshot, inherited_from_plan_id, created_at) ON TABLE public.model_switch_plans TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    # Published guard_0023 routines and the guard migration preflight
                    # require these worker/job grants in disposable references. They
                    # reconstruct historical permissions, not hosted runtime writers.
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, hostname, version, capabilities, supported_work_kinds, capability_snapshot_digest, capability_snapshot_json, slurm_gpu_allocation_evidence_json, slurm_gpu_allocation_evidence_digest, auth_token_hash, max_concurrent, pool_name, input_cache_capacity_bytes, input_cache_reserved_bytes, input_cache_ready_bytes, status, drain_state) ON TABLE public.workers TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (id, hostname, version, capabilities, supported_work_kinds, capability_snapshot_digest, capability_snapshot_json, slurm_gpu_allocation_evidence_json, slurm_gpu_allocation_evidence_digest, auth_token_hash, max_concurrent, pool_name, input_cache_capacity_bytes, input_cache_reserved_bytes, input_cache_ready_bytes, registered_at, last_seen_at, status) ON TABLE public.workers TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL("GRANT UPDATE (status) ON TABLE public.workers TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, slurm_cluster_id, environment, pool_name, nodelist, requested_cpus, requested_memory_mib, requested_pids, requested_gpu_tres, requested_gpus, requested_concurrency, sandbox_identity, candidate_sha, compose_project, job_id, slurm_state, state, worker_id) ON TABLE public.slurm_worker_jobs TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (id, slurm_cluster_id, environment, pool_name, nodelist, requested_cpus, requested_memory_mib, requested_gpu_tres, requested_gpus, requested_concurrency, sandbox_identity, candidate_sha, compose_project, job_id, slurm_state, state, submitted_at, started_at, last_reconciled_at) ON TABLE public.slurm_worker_jobs TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (worker_id) ON TABLE public.slurm_worker_jobs TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    claim_select_columns = {
                        "execution_attempts": ("worker_id", "state"),
                        "worker_pool_autoscaler_policies": (
                            "id",
                            "pool_name",
                            "actuator",
                            "actuator_config",
                            "enabled",
                            "prod_pressure_state",
                            "updated_at",
                        ),
                        "pipeline_acceptance_preflight_prerequisites": ("worker_id", "fence_state"),
                        "team_quotas": (
                            "team_id",
                            "in_flight_count",
                            "fair_share_weight",
                            "max_attempts_ceiling",
                        ),
                        "batch_family_state": (
                            "batch_id",
                            "family_key",
                            "state",
                            "task_sequence",
                            "current_index",
                            "state_uri",
                        ),
                        "batches": ("id", "family_run_spec"),
                        "execution_admission_policies": (
                            "scope_kind",
                            "scope_key",
                            "max_concurrent",
                            "active_count",
                            "enabled",
                        ),
                        "execution_admission_reservations": (
                            "id",
                            "trial_id",
                            "attempt",
                            "execution_role",
                            "team_id",
                            "batch_id",
                            "environment",
                            "region",
                            "execution_class_id",
                            "pool_id",
                            "owner_kind",
                            "state",
                        ),
                    }
                    for table, columns in claim_select_columns.items():
                        await connection.execute(
                            sql.SQL("GRANT SELECT ({}) ON TABLE public.{} TO {}").format(
                                sql.SQL(", ").join(map(sql.Identifier, columns)),
                                sql.Identifier(table),
                                sql.Identifier(owner),
                            )
                        )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (state, updated_at) ON TABLE public.batch_family_state TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (in_flight_count) ON TABLE public.team_quotas TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL("GRANT UPDATE (id) ON TABLE public.batches TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (id) ON TABLE public.model_switch_plans TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (active_count, counter_updated_at) ON TABLE public.execution_admission_policies TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT INSERT (trial_id, attempt, execution_role, team_id, batch_id, environment, region, execution_class_id, pool_id, owner_kind, owner_id, acquired_at) ON TABLE public.execution_admission_reservations TO {}"
                        ).format(sql.Identifier(owner))
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT UPDATE (state, released_at, release_reason) ON TABLE public.execution_admission_reservations TO {}"
                        ).format(sql.Identifier(owner))
                    )
        except asyncio.CancelledError:
            await self._seal_migrator(identity, owner=owner, migrator=migrator)
            raise
        except PersonalDevCapacityInstallationError:
            await self._seal_migrator(identity, owner=owner, migrator=migrator)
            raise
        except Exception:
            await self._seal_migrator(identity, owner=owner, migrator=migrator)
            raise PersonalDevCapacityInstallationError(
                "protected capacity database role convergence failed"
            ) from None
        return (owner, migrator, agent, executor, observer, runtime, migrator_url, agent_url)

    async def _seal_migrator(
        self, identity: ReferenceIdentity, *, owner: str, migrator: str
    ) -> None:
        """Remove transient schema authority and credentials between runs."""
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url, autocommit=True
            ) as connection:
                roles_result = await connection.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", ([owner, migrator],)
                )
                existing = {row[0] for row in await roles_result.fetchall()}
                if migrator in existing:
                    await connection.execute(
                        sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                            sql.Identifier(identity.database), sql.Identifier(migrator)
                        )
                    )
                if owner in existing:
                    await connection.execute(
                        sql.SQL("REVOKE CREATE ON DATABASE {} FROM {}").format(
                            sql.Identifier(identity.database), sql.Identifier(owner)
                        )
                    )
                if migrator in existing:
                    if owner in existing:
                        await connection.execute(
                            sql.SQL("REVOKE {} FROM {}").format(
                                sql.Identifier(owner), sql.Identifier(migrator)
                            )
                        )
                    await connection.execute(
                        sql.SQL("ALTER ROLE {} NOLOGIN NOCREATEROLE PASSWORD NULL").format(
                            sql.Identifier(migrator)
                        )
                    )
        except Exception:
            raise PersonalDevCapacityInstallationError(
                "protected capacity migration authority could not be sealed"
            ) from None

    async def destroy(self, identity: ReferenceIdentity) -> None:
        if derive_identity(identity.name) != identity:
            raise ValueError("unexpected reference database identity")
        async with await psycopg.AsyncConnection.connect(
            self._connect_url, autocommit=True
        ) as connection:
            await connection.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(identity.database)
                )
            )
            roles = [*_role_names(identity), identity.db_role]
            if self._application_owner_binding is not None:
                roles.append(self._application_owner_binding.owner_role)
            for role in roles:
                await connection.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
                )
