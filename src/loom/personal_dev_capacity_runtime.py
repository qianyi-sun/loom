"""Trusted local capacity authority and agent installer for personal development."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import secrets
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import psycopg
import yaml  # type: ignore[import-untyped]
from psycopg import sql
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.dev_instance import DevInstanceIdentity, derive_identity
from loom.dev_instance_runtime import KubectlClient, fixture_database_url
from loom.personal_dev_capacity import (
    PersonalDevCapacityInstallation,
    PersonalDevCapacityInstaller,
)
from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    canonical_manager_origin,
    read_owner_only_bytes,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    ReporterConfigurationV1,
)
from loom_capacity_agent.store import CapacityAgentStore, CapacityAgentStoreError
from loom_capacity_guard.contracts import GuardFenceV1, canonical_bytes, canonical_digest
from loom_capacity_guard.schema_startup import capacity_guard_schema_head
from loom_capacity_guard.store import CapacityGuardStore, GuardNotInitializedError

_SECRET_NAME = "loom-capacity-agent"
_CREDENTIALS_SECRET_NAME = "loom-capacity-agent-credentials"
_DEPLOYMENT_NAME = "loom-capacity-agent"
_ROLE_REPLACEMENTS = {"-": "_"}
_IMMUTABLE_IMAGE_RE = re.compile(
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*@sha256:[0-9a-f]{64}"
)
_K8S_DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?")
_K8S_LABEL_NAME_RE = re.compile(r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?")


class PersonalDevCapacityInstallationError(RuntimeError):
    """Trusted local capacity installation could not be converged exactly."""


def _sha256_json(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _role_names(identity: DevInstanceIdentity) -> tuple[str, str, str]:
    slug = identity.name
    for old, new in _ROLE_REPLACEMENTS.items():
        slug = slug.replace(old, new)
    return (
        f"loom_cap_{slug}_owner",
        f"loom_cap_{slug}_migrator",
        f"loom_cap_{slug}_agent",
    )


def _retarget_database_url(
    admin_url: str,
    *,
    database: str,
    username: str,
    password: str,
) -> str:
    parsed = make_url(fixture_database_url(admin_url, database))
    return parsed.set(username=username, password=password).render_as_string(
        hide_password=False
    )


def _validate_kubernetes_label_key(value: str) -> None:
    parts = value.split("/")
    if len(parts) > 2 or not parts[-1] or _K8S_LABEL_NAME_RE.fullmatch(parts[-1]) is None:
        raise ValueError("capacity Kubernetes label key is invalid")
    if len(parts) == 2 and (
        len(parts[0]) > 253
        or any(_K8S_DNS_LABEL_RE.fullmatch(item) is None for item in parts[0].split("."))
    ):
        raise ValueError("capacity Kubernetes label key is invalid")


@dataclass(frozen=True, slots=True)
class PersonalDevCapacityRuntimeConfig:
    manager_origin: str
    tls_files: DemandReporterTLSFiles
    trusted_agent_image: str
    pool_capabilities: tuple[AgentPoolCapabilityV1, ...]
    poll_interval_seconds: float = 5.0
    max_attempts: int = 10_000
    manager_namespace: str = "loom-dev"
    manager_pod_label_key: str = "app.kubernetes.io/name"
    manager_pod_label: str = "loom-capacity-manager"
    manager_port: int = 8443
    database_namespace: str = "loom-dev"
    database_pod_label_key: str = "app"
    database_pod_label: str = "loom-dev-postgres"
    database_port: int = 5432
    dns_namespace: str = "kube-system"
    dns_pod_label_key: str = "k8s-app"
    dns_pod_label: str = "kube-dns"
    dns_port: int = 53

    def __post_init__(self) -> None:
        canonical_manager_origin(self.manager_origin)
        if _IMMUTABLE_IMAGE_RE.fullmatch(
            self.trusted_agent_image
        ) is None or self.trusted_agent_image.endswith("@sha256:" + "0" * 64):
            raise ValueError("trusted capacity agent image must be immutable")
        if not self.pool_capabilities:
            raise ValueError("trusted capacity pool capabilities must be explicit")
        if not 0 < self.poll_interval_seconds <= 300:
            raise ValueError("capacity agent poll interval must be between 0 and 300 seconds")
        if not 1 <= self.max_attempts <= 10_000:
            raise ValueError("capacity agent capture bound must be between 1 and 10000")
        if any(
            not 1 <= value <= 65535
            for value in (self.manager_port, self.database_port, self.dns_port)
        ):
            raise ValueError("capacity dependency port is invalid")
        if any(
            _K8S_DNS_LABEL_RE.fullmatch(value) is None
            for value in (
                self.manager_namespace,
                self.database_namespace,
                self.dns_namespace,
            )
        ) or any(
            _K8S_LABEL_NAME_RE.fullmatch(value) is None
            for value in (
                self.manager_pod_label,
                self.database_pod_label,
                self.dns_pod_label,
            )
        ):
            raise ValueError("capacity dependency Kubernetes identity is invalid")
        _validate_kubernetes_label_key(self.manager_pod_label_key)
        _validate_kubernetes_label_key(self.database_pod_label_key)
        _validate_kubernetes_label_key(self.dns_pod_label_key)


@dataclass(frozen=True, slots=True)
class _CapacityCredentials:
    reporter_incarnation: UUID
    reporter_token: str
    migrator_password: str
    agent_password: str


@dataclass(frozen=True, slots=True)
class CapacityDatabaseInstallation:
    protected_admission_sha256: str
    agent_database_url: str


class PersonalDevCapacityDatabase(Protocol):
    async def converge(
        self,
        *,
        identity: DevInstanceIdentity,
        claim: PersonalDevReconciliationClaim,
        credentials: _CapacityCredentials,
        configuration: ReporterConfigurationV1,
    ) -> CapacityDatabaseInstallation: ...

    async def seal(self, identity: DevInstanceIdentity) -> None: ...

    async def destroy(self, identity: DevInstanceIdentity) -> None: ...


class PsycopgPersonalDevCapacityDatabase:
    """Provision least-privilege roles, migrate the guard, and bind the agent."""

    def __init__(self, admin_url: str, *, migration_timeout_seconds: float = 180.0) -> None:
        self._admin_url = admin_url
        self._migration_timeout_seconds = migration_timeout_seconds

    @property
    def _connect_url(self) -> str:
        return self._admin_url.replace("postgresql+psycopg://", "postgresql://", 1)

    async def _converge_roles(
        self,
        identity: DevInstanceIdentity,
        credentials: _CapacityCredentials,
    ) -> tuple[str, str, str, str, str]:
        owner, migrator, agent = _role_names(identity)
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
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url,
                autocommit=True,
            ) as connection:
                protected_roles = sql.SQL(", ").join(
                    sql.Identifier(role) for role in (owner, migrator, agent)
                )
                for role in (owner, migrator, agent):
                    await connection.execute(
                        sql.SQL(
                            "DO $loom$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles "
                            "WHERE rolname = {}) THEN CREATE ROLE {}; END IF; END $loom$"
                        ).format(sql.Literal(role), sql.Identifier(role))
                    )
                    await connection.execute(
                        sql.SQL("ALTER ROLE {} RESET ALL").format(sql.Identifier(role))
                    )
                await connection.execute(
                    sql.SQL(
                        "ALTER ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOINHERIT NOREPLICATION NOBYPASSRLS"
                    ).format(sql.Identifier(owner))
                )
                for role, password, inherit in (
                    (migrator, credentials.migrator_password, "INHERIT"),
                    (agent, credentials.agent_password, "NOINHERIT"),
                ):
                    await connection.execute(
                        sql.SQL(
                            "ALTER ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE {} "
                            "NOREPLICATION NOBYPASSRLS PASSWORD {}"
                        ).format(
                            sql.Identifier(role),
                            sql.SQL(inherit),
                            sql.Literal(password),
                        )
                    )
                await connection.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(owner),
                        sql.Identifier(migrator),
                    )
                )
                await connection.execute(
                    sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                        sql.Identifier(identity.database),
                        protected_roles,
                    )
                )
                await connection.execute(
                    sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}").format(
                        sql.Identifier(identity.database),
                        sql.Identifier(migrator),
                        sql.Identifier(agent),
                    )
                )
                await connection.execute(
                    sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                        sql.Identifier(identity.database),
                        sql.Identifier(owner),
                    )
                )
                memberships = await connection.execute(
                    "SELECT member.rolname AS member, granted.rolname AS granted "
                    "FROM pg_auth_members m JOIN pg_roles member ON member.oid = m.member "
                    "JOIN pg_roles granted ON granted.oid = m.roleid "
                    "WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s) "
                    "ORDER BY member.rolname, granted.rolname",
                    ([owner, migrator, agent], [owner, migrator, agent]),
                )
                observed = {(row[0], row[1]) for row in await memberships.fetchall()}
                if observed != {(migrator, owner)}:
                    for member, granted in sorted(observed - {(migrator, owner)}):
                        await connection.execute(
                            sql.SQL("REVOKE {} FROM {}").format(
                                sql.Identifier(granted),
                                sql.Identifier(member),
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
                    protected_roles = sql.SQL(", ").join(
                        sql.Identifier(role) for role in (owner, migrator, agent)
                    )
                    for object_kind in (
                        "SCHEMA public",
                        "ALL TABLES IN SCHEMA public",
                        "ALL SEQUENCES IN SCHEMA public",
                        "ALL FUNCTIONS IN SCHEMA public",
                    ):
                        await connection.execute(
                            sql.SQL("REVOKE ALL PRIVILEGES ON {} FROM {}").format(
                                sql.SQL(object_kind),
                                protected_roles,
                            )
                        )
                    await connection.execute(
                        sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL("GRANT REFERENCES ON TABLE public.trials TO {}").format(
                            sql.Identifier(owner)
                        )
                    )
                    await connection.execute(
                        sql.SQL(
                            "GRANT SELECT (id, state, requires_caps, cancellation_requested_at, "
                            "next_attempt_at, autoscaler_pool_name, worker_id, attempt_count, "
                            "submit_priority, submitted_at) ON TABLE public.trials TO {}"
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
        return owner, migrator, agent, migrator_url, agent_url

    async def _seal_migrator(
        self,
        identity: DevInstanceIdentity,
        *,
        owner: str,
        migrator: str,
    ) -> None:
        """Remove transient schema authority and credentials between runs."""

        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url,
                autocommit=True,
            ) as connection:
                await connection.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(owner),
                        sql.Identifier(migrator),
                    )
                )
                await connection.execute(
                    sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(
                        sql.Identifier(migrator)
                    )
                )
                await connection.execute(
                    sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                        sql.Identifier(identity.database),
                        sql.Identifier(migrator),
                    )
                )
                await connection.execute(
                    sql.SQL("REVOKE CREATE ON DATABASE {} FROM {}").format(
                        sql.Identifier(identity.database),
                        sql.Identifier(owner),
                    )
                )
        except Exception:
            raise PersonalDevCapacityInstallationError(
                "protected capacity migration authority could not be sealed"
            ) from None

    async def seal(self, identity: DevInstanceIdentity) -> None:
        """Disable every protected login before retained data can outlive its pod."""

        owner, migrator, agent = _role_names(identity)
        protected = (owner, migrator, agent, identity.db_role)
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url,
                autocommit=True,
            ) as connection:
                roles_result = await connection.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                    (list(protected),),
                )
                existing = {row[0] for row in await roles_result.fetchall()}
                await connection.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity "
                    "WHERE usename = ANY(%s) AND pid <> pg_backend_pid()",
                    (list(protected),),
                )
                if owner in existing and migrator in existing:
                    await connection.execute(
                        sql.SQL("REVOKE {} FROM {}").format(
                            sql.Identifier(owner),
                            sql.Identifier(migrator),
                        )
                    )
                for role in protected:
                    if role in existing:
                        await connection.execute(
                            sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(
                                sql.Identifier(role)
                            )
                        )
                database_exists = (
                    await connection.execute(
                        "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                        (identity.database,),
                    )
                )
                database_row = await database_exists.fetchone()
                if database_row is None:
                    raise PersonalDevCapacityInstallationError(
                        "protected capacity database lookup returned no row"
                    )
                if database_row[0]:
                    for role in protected:
                        if role in existing:
                            await connection.execute(
                                sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                                    sql.Identifier(identity.database),
                                    sql.Identifier(role),
                                )
                            )
        except Exception:
            raise PersonalDevCapacityInstallationError(
                "protected capacity authority could not be sealed for destroy"
            ) from None

    async def destroy(self, identity: DevInstanceIdentity) -> None:
        """Drop the isolated database and every role after namespace termination."""

        await self.seal(identity)
        owner, migrator, agent = _role_names(identity)
        roles = (agent, migrator, owner, identity.db_role)
        try:
            async with await psycopg.AsyncConnection.connect(
                self._connect_url,
                autocommit=True,
            ) as connection:
                await connection.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (identity.database,),
                )
                await connection.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        sql.Identifier(identity.database)
                    )
                )
                existing_result = await connection.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                    (list(roles),),
                )
                existing = {row[0] for row in await existing_result.fetchall()}
                for role in roles:
                    if role in existing:
                        await connection.execute(
                            sql.SQL("DROP ROLE {}").format(sql.Identifier(role))
                        )
        except Exception:
            raise PersonalDevCapacityInstallationError(
                "personal-dev protected database cleanup failed"
            ) from None

    async def _migrate(self, *, migrator_url: str, owner: str, agent: str) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        config_path = repo_root / "capacity_guard_migrations" / "alembic.ini"
        if not config_path.is_file():
            raise PersonalDevCapacityInstallationError(
                "protected capacity migrations are absent from the trusted image"
            )
        environment = os.environ.copy()
        environment.update(
            {
                "LOOM_CAPACITY_GUARD_DB_URL": migrator_url,
                "LOOM_CAPACITY_GUARD_OWNER_ROLE": owner,
                "LOOM_CAPACITY_GUARD_AGENT_ROLE": agent,
            }
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(config_path),
            "upgrade",
            "head",
            cwd=repo_root,
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(process.wait(), timeout=self._migration_timeout_seconds)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise PersonalDevCapacityInstallationError(
                "protected capacity migration timed out"
            ) from None
        except asyncio.CancelledError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        if process.returncode != 0:
            raise PersonalDevCapacityInstallationError(
                "protected capacity migration failed"
            )

    async def converge(
        self,
        *,
        identity: DevInstanceIdentity,
        claim: PersonalDevReconciliationClaim,
        credentials: _CapacityCredentials,
        configuration: ReporterConfigurationV1,
    ) -> CapacityDatabaseInstallation:
        owner, migrator, agent, migrator_url, agent_url = await self._converge_roles(
            identity,
            credentials,
        )
        try:
            await self._migrate(migrator_url=migrator_url, owner=owner, agent=agent)
            fence = GuardFenceV1(
                environment_id=configuration.environment_id,
                subject_id=configuration.subject_id,
                subject_incarnation=configuration.subject_incarnation,
                authority_incarnation=configuration.authority_incarnation,
                reporter_incarnation=configuration.reporter_incarnation,
                candidate_digest=configuration.candidate_digest,
                deployment_generation=configuration.deployment_generation,
                configuration_generation=configuration.configuration_generation,
            )
            registration = AgentRegistrationV1.model_validate(
                {
                    field: getattr(configuration, field)
                    for field in AgentRegistrationV1.model_fields
                }
            )
            engine = create_async_engine(migrator_url, isolation_level="SERIALIZABLE")
            quoted_owner = engine.sync_engine.dialect.identifier_preparer.quote(owner)
            try:
                factory = async_sessionmaker(engine, expire_on_commit=False)
                async with factory() as session, session.begin():
                    await session.execute(text(f"SET LOCAL ROLE {quoted_owner}"))
                    guard = CapacityGuardStore(session, expected_owner_role=owner)
                    agent_store = CapacityAgentStore(
                        session,
                        expected_owner_role=owner,
                        expected_agent_role=agent,
                    )
                    try:
                        current = await guard.read_guard_fence()
                    except GuardNotInitializedError:
                        await guard.initialize_disabled_authority(fence)
                        await agent_store.register_agent(registration)
                    else:
                        if current != fence:
                            await guard.reconfigure_disabled_authority(
                                fence,
                                expected_configuration_generation=(
                                    current.configuration_generation
                                ),
                            )
                        try:
                            await agent_store.reconfigure_agent(
                                registration,
                                expected_configuration_generation=(
                                    current.configuration_generation
                                ),
                            )
                        except CapacityAgentStoreError as exc:
                            if "insert was not observable" not in str(exc):
                                raise
                            await agent_store.register_agent(registration)
            finally:
                await engine.dispose()
        except PersonalDevCapacityInstallationError:
            raise
        except Exception:
            raise PersonalDevCapacityInstallationError(
                "protected capacity registration convergence failed"
            ) from None
        finally:
            await self._seal_migrator(identity, owner=owner, migrator=migrator)
        evidence = {
            "authority_incarnation": str(fence.authority_incarnation),
            "candidate_digest": fence.candidate_digest,
            "deployment_generation": fence.deployment_generation,
            "environment_id": fence.environment_id,
            "guard_schema_generation": capacity_guard_schema_head()[1],
            "reporter_incarnation": str(fence.reporter_incarnation),
            "roles": {"agent": agent, "owner": owner},
            "schema_version": 1,
            "subject_id": str(fence.subject_id),
            "subject_incarnation": str(fence.subject_incarnation),
        }
        return CapacityDatabaseInstallation(
            protected_admission_sha256=_sha256_json(evidence),
            agent_database_url=agent_url,
        )


def _new_credentials(*, reporter_incarnation: UUID | None = None) -> _CapacityCredentials:
    return _CapacityCredentials(
        reporter_incarnation=reporter_incarnation or uuid4(),
        reporter_token=secrets.token_urlsafe(48),
        migrator_password=secrets.token_urlsafe(48),
        agent_password=secrets.token_urlsafe(48),
    )


def _decode_secret_text(data: dict[str, bytes], key: str) -> str:
    try:
        value = data[key].decode("utf-8")
    except (KeyError, UnicodeDecodeError):
        raise PersonalDevCapacityInstallationError(
            "protected capacity credential set is incomplete"
        ) from None
    if not value or value != value.strip() or any(item in value for item in ("\r", "\n", "\x00")):
        raise PersonalDevCapacityInstallationError(
            "protected capacity credential set is invalid"
        )
    return value


def _agent_password_from_secret(data: dict[str, bytes]) -> str:
    try:
        parsed = make_url(_decode_secret_text(data, "database-url"))
    except (ArgumentError, ValueError):
        raise PersonalDevCapacityInstallationError(
            "protected capacity agent database credential is invalid"
        ) from None
    if not parsed.password:
        raise PersonalDevCapacityInstallationError(
            "protected capacity agent database credential is invalid"
        )
    return _opaque_credential(parsed.password, label="agent database")


def _opaque_credential(value: str, *, label: str) -> str:
    try:
        payload = value.encode("ascii")
    except UnicodeEncodeError:
        raise PersonalDevCapacityInstallationError(
            f"protected capacity {label} credential is invalid"
        ) from None
    if not 32 <= len(payload) <= 1024 or any(not 0x21 <= byte <= 0x7E for byte in payload):
        raise PersonalDevCapacityInstallationError(
            f"protected capacity {label} credential is invalid"
        )
    return value


class KubectlPersonalDevCapacityInstaller(PersonalDevCapacityInstaller):
    """Converge the secret, protected database, and candidate-independent pod."""

    def __init__(
        self,
        *,
        kubectl: KubectlClient,
        database: PersonalDevCapacityDatabase,
        config: PersonalDevCapacityRuntimeConfig,
    ) -> None:
        self._kubectl = kubectl
        self._database = database
        self._config = config

    async def _credentials(
        self,
        claim: PersonalDevReconciliationClaim,
        identity: DevInstanceIdentity,
    ) -> _CapacityCredentials:
        existing = await self._kubectl.read_secret_optional(
            identity.namespace,
            _CREDENTIALS_SECRET_NAME,
        )
        persisted_seed = existing is not None
        if existing is None:
            existing = await self._kubectl.read_secret_optional(
                identity.namespace,
                _SECRET_NAME,
            )
        if existing is None:
            return _new_credentials()
        try:
            subject_incarnation = UUID(_decode_secret_text(existing, "subject-incarnation"))
            operation_id = UUID(_decode_secret_text(existing, "operation-id"))
            reporter_incarnation = UUID(_decode_secret_text(existing, "reporter-incarnation"))
        except ValueError:
            raise PersonalDevCapacityInstallationError(
                "protected capacity credential identity is invalid"
            ) from None
        if subject_incarnation != claim.operation.subject_incarnation:
            raise PersonalDevCapacityInstallationError(
                "protected capacity credential belongs to another subject incarnation"
            )
        base = _CapacityCredentials(
            reporter_incarnation=reporter_incarnation,
            reporter_token=_opaque_credential(
                _decode_secret_text(existing, "reporter-token"),
                label="reporter",
            ),
            migrator_password=secrets.token_urlsafe(48),
            agent_password=(
                _opaque_credential(
                    _decode_secret_text(existing, "agent-password"),
                    label="agent database",
                )
                if persisted_seed
                else _agent_password_from_secret(existing)
            ),
        )
        if operation_id == claim.operation.id or claim.operation.kind == "capacity":
            return base
        return _CapacityCredentials(
            reporter_incarnation=uuid4(),
            reporter_token=secrets.token_urlsafe(48),
            migrator_password=base.migrator_password,
            agent_password=base.agent_password,
        )

    def _credential_seed_manifest(
        self,
        claim: PersonalDevReconciliationClaim,
        identity: DevInstanceIdentity,
        credentials: _CapacityCredentials,
    ) -> dict[str, object]:
        labels = {
            "app.kubernetes.io/managed-by": "loom-personal-dev-lifecycle",
            "app.kubernetes.io/name": _CREDENTIALS_SECRET_NAME,
            "loom.dev/instance": identity.name,
            "loom.dev/trust-domain": "capacity-credential-seed",
        }
        data = {
            "agent-password": credentials.agent_password.encode("ascii"),
            "operation-id": str(claim.operation.id).encode("ascii"),
            "reporter-incarnation": str(credentials.reporter_incarnation).encode("ascii"),
            "reporter-token": credentials.reporter_token.encode("ascii"),
            "subject-incarnation": str(claim.operation.subject_incarnation).encode("ascii"),
        }
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": _CREDENTIALS_SECRET_NAME,
                "namespace": identity.namespace,
                "labels": labels,
            },
            "type": "Opaque",
            "data": {
                key: base64.b64encode(value).decode("ascii") for key, value in data.items()
            },
        }

    async def _persist_credentials(
        self,
        claim: PersonalDevReconciliationClaim,
        identity: DevInstanceIdentity,
        credentials: _CapacityCredentials,
    ) -> None:
        """Durably bind retry credentials before any protected database mutation."""

        manifest = self._credential_seed_manifest(claim, identity, credentials)
        await self._kubectl.apply(
            yaml.safe_dump_all((manifest,), sort_keys=False, explicit_start=True)
        )
        observed = await self._kubectl.read_secret_optional(
            identity.namespace,
            _CREDENTIALS_SECRET_NAME,
        )
        expected = {
            "agent-password": credentials.agent_password.encode("ascii"),
            "operation-id": str(claim.operation.id).encode("ascii"),
            "reporter-incarnation": str(credentials.reporter_incarnation).encode("ascii"),
            "reporter-token": credentials.reporter_token.encode("ascii"),
            "subject-incarnation": str(claim.operation.subject_incarnation).encode("ascii"),
        }
        if observed != expected:
            raise PersonalDevCapacityInstallationError(
                "protected capacity credential seed was not installed exactly"
            )

    async def _assert_installed_credentials(
        self,
        claim: PersonalDevReconciliationClaim,
        installation: PersonalDevCapacityInstallation,
        identity: DevInstanceIdentity,
    ) -> None:
        seed = await self._kubectl.read_secret_optional(
            identity.namespace,
            _CREDENTIALS_SECRET_NAME,
        )
        runtime = await self._kubectl.read_secret_optional(identity.namespace, _SECRET_NAME)
        if seed is None or runtime is None:
            raise PersonalDevCapacityInstallationError(
                "protected capacity credential installation is unavailable"
            )
        try:
            seed_subject_incarnation = UUID(_decode_secret_text(seed, "subject-incarnation"))
            seed_operation_id = UUID(_decode_secret_text(seed, "operation-id"))
            seed_reporter_incarnation = UUID(_decode_secret_text(seed, "reporter-incarnation"))
            runtime_subject_incarnation = UUID(
                _decode_secret_text(runtime, "subject-incarnation")
            )
            runtime_operation_id = UUID(_decode_secret_text(runtime, "operation-id"))
            runtime_reporter_incarnation = UUID(
                _decode_secret_text(runtime, "reporter-incarnation")
            )
            seed_token = _opaque_credential(
                _decode_secret_text(seed, "reporter-token"),
                label="reporter",
            )
            runtime_token = _opaque_credential(
                _decode_secret_text(runtime, "reporter-token"),
                label="reporter",
            )
            reporter_token_sha256 = hashlib.sha256(
                runtime_token.encode("ascii")
            ).hexdigest()
            seed_agent_password = _opaque_credential(
                _decode_secret_text(seed, "agent-password"),
                label="agent database",
            )
            runtime_agent_password = _agent_password_from_secret(runtime)
        except (UnicodeEncodeError, ValueError):
            raise PersonalDevCapacityInstallationError(
                "protected capacity credential installation is invalid"
            ) from None
        if (
            seed_subject_incarnation != claim.operation.subject_incarnation
            or runtime_subject_incarnation != claim.operation.subject_incarnation
            or seed_operation_id != claim.operation.id
            or runtime_operation_id != claim.operation.id
            or seed_reporter_incarnation != installation.reporter_incarnation
            or runtime_reporter_incarnation != installation.reporter_incarnation
            or seed_token != runtime_token
            or seed_agent_password != runtime_agent_password
            or reporter_token_sha256 != installation.reporter_token_sha256
        ):
            raise PersonalDevCapacityInstallationError(
                "protected capacity credential installation was superseded"
            )

    def _configuration(
        self,
        claim: PersonalDevReconciliationClaim,
        credentials: _CapacityCredentials,
    ) -> ReporterConfigurationV1:
        operation = claim.operation
        authority_incarnation = uuid5(
            NAMESPACE_URL,
            f"loom:{operation.subject_incarnation}:capacity-authority",
        )
        agent_incarnation = uuid5(
            NAMESPACE_URL,
            f"loom:{operation.subject_incarnation}:capacity-agent",
        )
        return ReporterConfigurationV1(
            environment_id=f"dev-{operation.environment_name}",
            subject_id=operation.subject_id,
            subject_incarnation=operation.subject_incarnation,
            authority_incarnation=authority_incarnation,
            agent_incarnation=agent_incarnation,
            reporter_incarnation=credentials.reporter_incarnation,
            candidate_digest=operation.candidate_sha,
            deployment_generation=operation.deployment_generation,
            configuration_generation=operation.operation_epoch,
            pool_capabilities=self._config.pool_capabilities,
        )

    def _manifests(
        self,
        *,
        claim: PersonalDevReconciliationClaim,
        identity: DevInstanceIdentity,
        credentials: _CapacityCredentials,
        configuration: ReporterConfigurationV1,
        database: CapacityDatabaseInstallation,
        tls: dict[str, bytes],
    ) -> tuple[tuple[dict[str, object], ...], str]:
        labels = {
            "app.kubernetes.io/managed-by": "loom-personal-dev-lifecycle",
            "app.kubernetes.io/name": _DEPLOYMENT_NAME,
            "loom.dev/instance": identity.name,
            "loom.dev/trust-domain": "capacity-agent",
        }
        configuration_bytes = canonical_bytes(configuration)
        secret_data = {
            "ca.pem": tls["ca.pem"],
            "certificate.pem": tls["certificate.pem"],
            "database-url": database.agent_database_url.encode("utf-8"),
            "operation-id": str(claim.operation.id).encode("ascii"),
            "private-key.pem": tls["private-key.pem"],
            "reporter-configuration.json": configuration_bytes,
            "reporter-incarnation": str(credentials.reporter_incarnation).encode("ascii"),
            "reporter-token": credentials.reporter_token.encode("ascii"),
            "subject-incarnation": str(claim.operation.subject_incarnation).encode("ascii"),
        }
        secret: dict[str, object] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": _SECRET_NAME,
                "namespace": identity.namespace,
                "labels": labels,
            },
            "type": "Opaque",
            "data": {
                key: base64.b64encode(value).decode("ascii")
                for key, value in secret_data.items()
            },
        }
        command = [
            "python",
            "-m",
            "loom_capacity_agent.runtime",
            "--configuration-file",
            "/run/loom-capacity/files/reporter-configuration.json",
            "--database-url-file",
            "/run/loom-capacity/files/database-url",
            "--manager-origin",
            self._config.manager_origin,
            "--bearer-token-file",
            "/run/loom-capacity/files/reporter-token",
            "--ca-file",
            "/run/loom-capacity/files/ca.pem",
            "--certificate-file",
            "/run/loom-capacity/files/certificate.pem",
            "--private-key-file",
            "/run/loom-capacity/files/private-key.pem",
            "--poll-interval-seconds",
            str(self._config.poll_interval_seconds),
            "--max-attempts",
            str(self._config.max_attempts),
        ]
        restricted = {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "readOnlyRootFilesystem": True,
            "runAsNonRoot": True,
            "runAsUser": 65532,
            "seccompProfile": {"type": "RuntimeDefault"},
        }
        deployment: dict[str, object] = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": _DEPLOYMENT_NAME,
                "namespace": identity.namespace,
                "labels": labels,
            },
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app.kubernetes.io/name": _DEPLOYMENT_NAME}},
                "template": {
                    "metadata": {
                        "labels": labels,
                        "annotations": {
                            "loom.dev/capacity-installation-input-sha256": _sha256_json(
                                {
                                    "configuration": canonical_digest(configuration),
                                    "database": database.protected_admission_sha256,
                                    "reporter_token": hashlib.sha256(
                                        credentials.reporter_token.encode("ascii")
                                    ).hexdigest(),
                                    "tls": {
                                        key: hashlib.sha256(value).hexdigest()
                                        for key, value in sorted(tls.items())
                                    },
                                }
                            )
                        },
                    },
                    "spec": {
                        "automountServiceAccountToken": False,
                        "enableServiceLinks": False,
                        "securityContext": {
                            "fsGroup": 65532,
                            "fsGroupChangePolicy": "OnRootMismatch",
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "initContainers": [
                            {
                                "name": "credential-init",
                                "image": self._config.trusted_agent_image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "python",
                                    "-m",
                                    "loom_capacity_agent.secret_init",
                                    "--source",
                                    "/var/run/loom-capacity-projected",
                                    "--destination",
                                    "/run/loom-capacity/files",
                                ],
                                "securityContext": restricted,
                                "volumeMounts": [
                                    {
                                        "name": "projected",
                                        "mountPath": "/var/run/loom-capacity-projected",
                                        "readOnly": True,
                                    },
                                    {"name": "runtime", "mountPath": "/run/loom-capacity"},
                                ],
                            }
                        ],
                        "containers": [
                            {
                                "name": "capacity-agent",
                                "image": self._config.trusted_agent_image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": command,
                                "ports": [{"name": "health", "containerPort": 8081}],
                                "readinessProbe": {
                                    "httpGet": {"path": "/ready", "port": "health"},
                                    "periodSeconds": 5,
                                    "timeoutSeconds": 2,
                                    "failureThreshold": 3,
                                },
                                "resources": {
                                    "requests": {"cpu": "25m", "memory": "64Mi"},
                                    "limits": {"cpu": "500m", "memory": "256Mi"},
                                },
                                "securityContext": restricted,
                                "volumeMounts": [
                                    {
                                        "name": "runtime",
                                        "mountPath": "/run/loom-capacity",
                                        "readOnly": True,
                                    }
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "projected",
                                "secret": {"secretName": _SECRET_NAME, "defaultMode": 288},
                            },
                            {"name": "runtime", "emptyDir": {"medium": "Memory"}},
                        ],
                    },
                },
            },
        }
        network_policy: dict[str, object] = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": "capacity-agent-egress",
                "namespace": identity.namespace,
                "labels": labels,
            },
            "spec": {
                "podSelector": {
                    "matchLabels": {"app.kubernetes.io/name": _DEPLOYMENT_NAME}
                },
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": (
                                            self._config.manager_namespace
                                        )
                                    }
                                },
                                "podSelector": {
                                    "matchLabels": {
                                        self._config.manager_pod_label_key: (
                                            self._config.manager_pod_label
                                        )
                                    }
                                },
                            }
                        ],
                        "ports": [
                            {"protocol": "TCP", "port": self._config.manager_port}
                        ],
                    },
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": (
                                            self._config.database_namespace
                                        )
                                    }
                                },
                                "podSelector": {
                                    "matchLabels": {
                                        self._config.database_pod_label_key: (
                                            self._config.database_pod_label
                                        )
                                    }
                                },
                            }
                        ],
                        "ports": [
                            {"protocol": "TCP", "port": self._config.database_port}
                        ],
                    },
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": (
                                            self._config.dns_namespace
                                        )
                                    }
                                },
                                "podSelector": {
                                    "matchLabels": {
                                        self._config.dns_pod_label_key: (
                                            self._config.dns_pod_label
                                        )
                                    }
                                },
                            }
                        ],
                        "ports": [
                            {"protocol": "UDP", "port": self._config.dns_port},
                            {"protocol": "TCP", "port": self._config.dns_port},
                        ],
                    },
                ],
            },
        }
        documents = (secret, deployment, network_policy)
        stable_configuration = {
            "agent_incarnation": str(configuration.agent_incarnation),
            "authority_incarnation": str(configuration.authority_incarnation),
            "candidate_digest": configuration.candidate_digest,
            "deployment_generation": configuration.deployment_generation,
            "environment_id": configuration.environment_id,
            "pool_capabilities": [
                item.model_dump(mode="json", exclude_none=False)
                for item in configuration.pool_capabilities
            ],
            "reporter_incarnation": str(configuration.reporter_incarnation),
            "subject_id": str(configuration.subject_id),
            "subject_incarnation": str(configuration.subject_incarnation),
        }
        stable_deployment = json.loads(json.dumps(deployment))
        stable_annotations = stable_deployment["spec"]["template"]["metadata"][
            "annotations"
        ]
        del stable_annotations["loom.dev/capacity-installation-input-sha256"]
        if not stable_annotations:
            del stable_deployment["spec"]["template"]["metadata"]["annotations"]
        evidence = {
            "agent_image": self._config.trusted_agent_image,
            "agent_database_url_sha256": hashlib.sha256(
                database.agent_database_url.encode("utf-8")
            ).hexdigest(),
            "database_evidence_sha256": database.protected_admission_sha256,
            "database_network": {
                "namespace": self._config.database_namespace,
                "pod_label_key": self._config.database_pod_label_key,
                "pod_label": self._config.database_pod_label,
                "port": self._config.database_port,
            },
            "dns_network": {
                "namespace": self._config.dns_namespace,
                "pod_label_key": self._config.dns_pod_label_key,
                "pod_label": self._config.dns_pod_label,
                "port": self._config.dns_port,
            },
            "deployment_contract": stable_deployment,
            "manager": {
                "namespace": self._config.manager_namespace,
                "origin": self._config.manager_origin,
                "pod_label_key": self._config.manager_pod_label_key,
                "pod_label": self._config.manager_pod_label,
                "port": self._config.manager_port,
            },
            "network_policy": network_policy,
            "poll_interval_seconds": self._config.poll_interval_seconds,
            "reporter_token_sha256": hashlib.sha256(
                credentials.reporter_token.encode("ascii")
            ).hexdigest(),
            "schema_version": 2,
            "stable_configuration": stable_configuration,
            "tls_sha256": {
                key: hashlib.sha256(tls[key]).hexdigest()
                for key in ("ca.pem", "certificate.pem", "private-key.pem")
            },
        }
        return documents, _sha256_json(evidence)

    async def converge(
        self,
        claim: PersonalDevReconciliationClaim,
    ) -> PersonalDevCapacityInstallation:
        identity = derive_identity(claim.operation.environment_name)
        credentials = await self._credentials(claim, identity)
        await self._persist_credentials(claim, identity, credentials)
        configuration = self._configuration(claim, credentials)
        database = await self._database.converge(
            identity=identity,
            claim=claim,
            credentials=credentials,
            configuration=configuration,
        )
        if _agent_password_from_secret(
            {"database-url": database.agent_database_url.encode("utf-8")}
        ) != credentials.agent_password:
            raise PersonalDevCapacityInstallationError(
                "protected capacity database returned a mismatched agent credential"
            )
        tls = {
            "ca.pem": read_owner_only_bytes(self._config.tls_files.ca_file),
            "certificate.pem": read_owner_only_bytes(
                self._config.tls_files.certificate_file
            ),
            "private-key.pem": read_owner_only_bytes(
                self._config.tls_files.private_key_file
            ),
        }
        documents, installation_sha256 = self._manifests(
            claim=claim,
            identity=identity,
            credentials=credentials,
            configuration=configuration,
            database=database,
            tls=tls,
        )
        await self._kubectl.apply(
            yaml.safe_dump_all(documents, sort_keys=False, explicit_start=True)
        )
        return PersonalDevCapacityInstallation(
            reporter_incarnation=credentials.reporter_incarnation,
            reporter_token=credentials.reporter_token,
            protected_admission_sha256=database.protected_admission_sha256,
            capacity_agent_installation_sha256=installation_sha256,
            supported_pool_ids=tuple(
                sorted({capability.pool_id for capability in configuration.pool_capabilities})
            ),
            supported_architectures=tuple(
                sorted(
                    {
                        capability.cpu_architecture
                        for capability in configuration.pool_capabilities
                    }
                )
            ),
        )

    async def verify_publishing(
        self,
        claim: PersonalDevReconciliationClaim,
        installation: PersonalDevCapacityInstallation,
    ) -> None:
        """Wait for an exact installed agent to publish after manager registration."""

        if not isinstance(installation, PersonalDevCapacityInstallation):
            raise TypeError("personal-dev capacity installation is invalid")
        identity = derive_identity(claim.operation.environment_name)
        await self._assert_installed_credentials(claim, installation, identity)
        await self._kubectl.wait_deployment(identity.namespace, _DEPLOYMENT_NAME)
        await self._assert_installed_credentials(claim, installation, identity)

    async def seal(self, claim: PersonalDevReconciliationClaim) -> None:
        if claim.operation.kind != "destroy":
            raise ValueError("capacity sealing requires a destroy operation")
        await self._database.seal(derive_identity(claim.operation.environment_name))

    async def destroy(self, claim: PersonalDevReconciliationClaim) -> None:
        if claim.operation.kind != "destroy":
            raise ValueError("capacity cleanup requires a destroy operation")
        await self._database.destroy(derive_identity(claim.operation.environment_name))


def parse_pool_capabilities(raw: str) -> tuple[AgentPoolCapabilityV1, ...]:
    try:
        payload = json.loads(raw)
        if not isinstance(payload, list):
            raise TypeError
        values = tuple(AgentPoolCapabilityV1.model_validate(item) for item in payload)
        if {item.pool_id for item in values} != {"oldlab", "gb10"} or {
            item.cpu_architecture for item in values
        } != {"x86_64", "arm64"}:
            raise ValueError
        # Exercise canonical duplicate and bound checks once at startup.
        marker = uuid4()
        ReporterConfigurationV1(
            environment_id="dev-validation",
            subject_id=uuid4(),
            subject_incarnation=uuid4(),
            authority_incarnation=uuid4(),
            agent_incarnation=uuid4(),
            reporter_incarnation=marker,
            candidate_digest="0" * 64,
            deployment_generation=1,
            configuration_generation=1,
            pool_capabilities=values,
        )
    except (TypeError, ValueError):
        raise ValueError("personal-dev capacity pool capabilities JSON is invalid") from None
    return values


__all__ = [
    "CapacityDatabaseInstallation",
    "KubectlPersonalDevCapacityInstaller",
    "PersonalDevCapacityDatabase",
    "PersonalDevCapacityInstallationError",
    "PersonalDevCapacityRuntimeConfig",
    "PsycopgPersonalDevCapacityDatabase",
    "parse_pool_capabilities",
]
