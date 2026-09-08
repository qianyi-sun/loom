"""Management-only membership readback; receipt digests are not authentication.

The caller supplies an authenticated manager checkpoint, while the installer pins
the independently prepared execution authority. Personal source gets neither the
management database connection nor the Kubernetes credentials used here.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.dev_instance import DevInstanceIdentity, derive_identity
from loom.dev_instance_runtime import fixture_database_url
from loom.personal_dev_capacity import PersonalDevCapacityInstallation
from loom.personal_dev_capacity_identity import capacity_role_names
from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom.personal_dev_membership_checkpoint import (
    PersonalDevMembershipEnvelopeV1,
    PersonalDevMembershipObservationV1,
)
from loom_capacity_agent.contracts import AgentRegistrationV1, ReporterConfigurationV1
from loom_capacity_agent.store import CapacityAgentStore, read_agent_reporter_high_water
from loom_capacity_guard.contracts import GuardFenceV1, canonical_digest
from loom_capacity_guard.schema_startup import capacity_guard_schema_head
from loom_capacity_guard.store import CapacityGuardStore
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutionAuthorityV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import PersonalMembershipCheckpointV1

if TYPE_CHECKING:
    from loom.personal_dev_capacity_runtime import (
        CapacityDatabaseInstallation,
        KubectlPersonalDevCapacityInstaller,
    )


class PersonalDevMembershipDatabase(Protocol):
    async def observe_membership(
        self,
        *,
        identity: DevInstanceIdentity,
        configuration: ReporterConfigurationV1,
        agent_database_url: str,
        retirement_from_generation: int | None = None,
    ) -> dict[str, Any]: ...


def validate_membership_observation_context(
    claim: PersonalDevReconciliationClaim,
    checkpoint: PersonalMembershipCheckpointV1,
    execution_pin: ExecutionAuthorityV2 | None,
    observed_at: datetime,
) -> None:
    """Reject re-attestation and stale claim bundles before protected side effects."""
    if execution_pin is None or checkpoint.execution != execution_pin:
        raise ValueError("membership execution differs from trusted prepared execution")
    operation, attempt = claim.operation, claim.attempt
    if (
        observed_at.tzinfo is None
        or operation.capacity_mode != "membership-v1"
        or operation.capacity_membership_envelope is not None
        or operation.attempt_id != attempt.id
        or operation.id != attempt.operation_id
        or operation.operation_epoch != attempt.operation_epoch
        or operation.subject_id != attempt.subject_id
        or operation.subject_incarnation != attempt.subject_incarnation
        or attempt.lease_epoch < 1
        or not attempt.claimed_by
        or attempt.state not in ("running", "activating")
        or attempt.lease_expires_at is None
        or attempt.lease_expires_at <= observed_at
        or not operation.local_activation_sha256
        or claim.candidate.id != operation.candidate_id
        or claim.candidate.candidate_sha != operation.candidate_sha
        or claim.candidate.status != "ready"
        or not claim.candidate.publication_sha256
    ):
        raise ValueError("membership observation requires a fresh exact leased operation")


def _registration(configuration: ReporterConfigurationV1) -> AgentRegistrationV1:
    return AgentRegistrationV1.model_validate(
        {name: getattr(configuration, name) for name in AgentRegistrationV1.model_fields}
    )


async def read_protected_membership(
    session: AsyncSession,
    *,
    owner: str,
    agent: str,
    configuration: ReporterConfigurationV1,
    retirement_from_generation: int | None = None,
) -> dict[str, Any]:
    """Measure protected history, optionally advancing only a retirement binding.

    This primitive runs inside the trusted installer's SERIALIZABLE owner
    transaction. It never initializes, migrates, truncates, seals, or resets
    sequences. A legacy installation needs its separate authenticated freeze
    transition; an absent legacy inventory truthfully measures zero.
    """
    guard = CapacityGuardStore(session, expected_owner_role=owner)
    store = CapacityAgentStore(session, expected_owner_role=owner, expected_agent_role=agent)
    fence = await guard.read_guard_fence()
    await store._assert_agent_role_binding()
    actual = await store._read_registration(lock=True)
    await store._verify_registration_audit(actual)
    expected = _registration(configuration)
    expected_fence = GuardFenceV1.model_validate(
        {name: getattr(configuration, name) for name in GuardFenceV1.model_fields}
    )
    if retirement_from_generation is not None:
        prior = expected.model_copy(update={"configuration_generation": retirement_from_generation})
        prior_fence = expected_fence.model_copy(
            update={"configuration_generation": retirement_from_generation}
        )
        if actual not in (prior, expected) or fence not in (prior_fence, expected_fence):
            raise ValueError("protected retirement binding differs from retained installation")
        if fence != expected_fence:
            await guard.reconfigure_disabled_authority(
                expected_fence, expected_configuration_generation=retirement_from_generation
            )
        if actual != expected:
            await store.reconfigure_agent(
                expected, expected_configuration_generation=retirement_from_generation
            )
        fence = await guard.read_guard_fence()
        actual = await store._read_registration(lock=True)
        await store._verify_registration_audit(actual)
    if actual != expected or fence != expected_fence:
        raise ValueError("protected membership registration differs from installed publication")
    revision = (
        await session.execute(
            text("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version")
        )
    ).scalar_one()
    if revision != capacity_guard_schema_head()[0]:
        raise ValueError("protected membership schema is not at the trusted head")
    legacy = (
        (
            await session.execute(
                text(
                    "SELECT count(*) AS cursor_count, coalesce(max(high_water), 0) AS high_water "
                    "FROM loom_capacity_guard.legacy_writer_cursors"
                )
            )
        )
        .mappings()
        .one()
    )
    legacy_preparations = (
        await session.execute(
            text("SELECT count(*) FROM loom_capacity_guard.legacy_compatibility_preparations")
        )
    ).scalar_one()
    if legacy["cursor_count"] or legacy_preparations:
        raise ValueError("personal membership requires separate authenticated legacy transition")
    sequence = (
        await session.execute(
            text(
                "SELECT high_water FROM loom_capacity_guard.agent_reporter_state "
                "WHERE agent_incarnation = :agent"
            ),
            {"agent": configuration.agent_incarnation},
        )
    ).scalar_one()
    return {
        "guard_sha256": canonical_digest(fence),
        "registration_sha256": canonical_digest(actual),
        "legacy_writer_high_water": legacy["high_water"],
        "reporter_high_water": sequence,
        "schema_head": revision,
    }


async def observe_database(
    admin_url: str,
    *,
    identity: DevInstanceIdentity,
    configuration: ReporterConfigurationV1,
    agent_database_url: str,
    retirement_from_generation: int | None = None,
) -> dict[str, Any]:
    owner, _migrator, agent, _executor, observer, runtime = capacity_role_names(identity)
    trusted_url = make_url(fixture_database_url(admin_url, identity.database))
    installed_url = make_url(agent_database_url)
    if trusted_url.drivername in ("postgres", "postgresql"):
        trusted_url = trusted_url.set(drivername="postgresql+psycopg")
    if installed_url.drivername in ("postgres", "postgresql"):
        installed_url = installed_url.set(drivername="postgresql+psycopg")
    if (
        not installed_url.password
        or installed_url.set(username=trusted_url.username, password=trusted_url.password)
        != trusted_url
        or installed_url.username != agent
    ):
        raise ValueError(
            "installed membership database endpoint differs from trusted management database"
        )
    engine = create_async_engine(trusted_url, isolation_level="SERIALIZABLE")
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session, session.begin():
            untrusted_roles = sorted({identity.db_role, agent, observer, runtime})
            roles = (
                (
                    await session.execute(
                        text(
                            "SELECT rolname FROM pg_roles WHERE rolname = ANY(:roles) "
                            "AND NOT rolsuper AND NOT rolcreaterole AND NOT rolreplication "
                            "AND NOT rolbypassrls AND NOT pg_has_role(rolname, :owner, 'MEMBER') "
                            "AND NOT has_schema_privilege(rolname, 'loom_capacity_guard', 'CREATE')"
                        ),
                        {"roles": untrusted_roles, "owner": owner},
                    )
                )
                .scalars()
                .all()
            )
            if sorted(roles) != untrusted_roles:
                raise ValueError("protected membership database role separation is invalid")
            unsafe_tables = (
                await session.execute(
                    text(
                        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "CROSS JOIN pg_roles r WHERE n.nspname = 'loom_capacity_guard' "
                        "AND c.relkind IN ('r', 'p') AND r.rolname = ANY(:roles) "
                        "AND (has_table_privilege(r.oid, c.oid, 'INSERT,UPDATE,DELETE,TRUNCATE') "
                        "OR has_any_column_privilege(r.oid, c.oid, 'INSERT,UPDATE'))"
                    ),
                    {"roles": untrusted_roles},
                )
            ).scalar_one()
            if unsafe_tables:
                raise ValueError("protected membership tables expose untrusted mutation authority")
            quoted = engine.sync_engine.dialect.identifier_preparer.quote(owner)
            await session.execute(text(f"SET LOCAL ROLE {quoted}"))
            snapshot = await read_protected_membership(
                session,
                owner=owner,
                agent=agent,
                configuration=configuration,
                retirement_from_generation=retirement_from_generation,
            )
    finally:
        await engine.dispose()
    # Authenticate the actual installed agent login against the same trusted
    # database. The agent-only function verifies session_user, role separation,
    # and the guard/registration join; an owner read alone cannot prove this.
    agent_engine = create_async_engine(installed_url)
    try:
        async with async_sessionmaker(agent_engine)() as session:
            sequence = await read_agent_reporter_high_water(
                session, registration=_registration(configuration)
            )
            if sequence < snapshot["reporter_high_water"]:
                raise ValueError("installed membership agent sequence regressed")
    finally:
        await agent_engine.dispose()
    return snapshot


_KUBERNETES_DEFAULTS: dict[str, object] = {
    "dnsPolicy": "ClusterFirst",
    "restartPolicy": "Always",
    "schedulerName": "default-scheduler",
    "terminationGracePeriodSeconds": 30,
    "terminationMessagePath": "/dev/termination-log",
    "terminationMessagePolicy": "File",
    "progressDeadlineSeconds": 600,
    "revisionHistoryLimit": 10,
    "protocol": "TCP",
    "scheme": "HTTP",
    "successThreshold": 1,
    "initialDelaySeconds": 0,
    "resources": {},
    "privileged": False,
}


def _contains_contract(actual: Any, expected: Any, *, field: str = "") -> bool:
    """Permit known API defaults, never extra executable workload configuration."""
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        for key in actual.keys() - expected.keys():
            if field == "metadata" or (not field and key == "status"):
                continue
            if key not in _KUBERNETES_DEFAULTS or actual[key] != _KUBERNETES_DEFAULTS[key]:
                return False
        return all(
            key in actual and _contains_contract(actual[key], value, field=key)
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(
                _contains_contract(left, right, field=field)
                for left, right in zip(actual, expected, strict=True)
            )
        )
    return bool(actual == expected)


class PersonalDevMembershipObserver:
    """Private adapter around the existing trusted installer, not a source API."""

    def __init__(self, installer: KubectlPersonalDevCapacityInstaller) -> None:
        self.installer = installer

    async def _installed(
        self,
        claim: PersonalDevReconciliationClaim,
        installation: PersonalDevCapacityInstallation,
        *,
        retirement: bool = False,
        require_current: bool = False,
    ) -> tuple[
        DevInstanceIdentity,
        ReporterConfigurationV1,
        CapacityDatabaseInstallation,
        tuple[dict[str, object], ...],
    ]:
        from loom.personal_dev_capacity_identity import PROTECTED_WORKER_RUNTIME_SECRET_NAME
        from loom.personal_dev_capacity_runtime import (
            _CREDENTIALS_SECRET_NAME,
            _SECRET_NAME,
            CapacityDatabaseCredentials,
            CapacityDatabaseInstallation,
            _decode_secret_text,
            protected_capacity_database_admission_digest,
        )

        runtime = self.installer
        identity = derive_identity(claim.operation.environment_name)
        seed = await runtime._kubectl.read_secret_optional(
            identity.namespace, _CREDENTIALS_SECRET_NAME
        )
        secret = await runtime._kubectl.read_secret_optional(identity.namespace, _SECRET_NAME)
        worker = await runtime._kubectl.read_secret_optional(
            identity.namespace, PROTECTED_WORKER_RUNTIME_SECRET_NAME
        )
        if seed is None or secret is None or worker is None:
            raise ValueError("protected membership installation is unavailable")
        credential_claim = claim
        if retirement:
            # Retirement retains the original installed operation's credential tags.
            credential_claim = replace(
                claim,
                operation=replace(
                    claim.operation, id=UUID(_decode_secret_text(seed, "operation-id"))
                ),
            )
        await runtime._assert_installed_credentials(credential_claim, installation, identity)
        credentials = CapacityDatabaseCredentials(
            reporter_incarnation=installation.reporter_incarnation,
            reporter_token=_decode_secret_text(seed, "reporter-token"),
            migrator_password="",  # Never reopen or use the sealed migration login.
            agent_password=_decode_secret_text(seed, "agent-password"),
            observer_password=_decode_secret_text(seed, "observer-password"),
            runtime_password=_decode_secret_text(seed, "runtime-password"),
        )
        configuration = ReporterConfigurationV1.model_validate_json(
            secret["reporter-configuration.json"]
        )
        expected = runtime._configuration(claim, credentials).model_copy(
            update={
                "protected_admission_sha256": installation.protected_admission_sha256,
            }
        )
        if retirement:
            prior = expected.model_copy(
                update={
                    "configuration_generation": claim.operation.expected_operation_epoch,
                }
            )
            if configuration not in (prior, expected):
                raise ValueError("retirement agent configuration differs from retained publication")
        elif configuration != expected:
            raise ValueError("installed membership agent publication differs from operation")
        if require_current and configuration != expected:
            raise ValueError("installed membership agent generation has not advanced")
        database = CapacityDatabaseInstallation(
            protected_admission_sha256=installation.protected_admission_sha256,
            agent_database_url=_decode_secret_text(secret, "database-url"),
            runtime_database_url=_decode_secret_text(worker, "database-url"),
        )
        if (
            protected_capacity_database_admission_digest(
                identity=identity,
                configuration=expected,
                runtime_database_url=database.runtime_database_url,
            )
            != installation.protected_admission_sha256
        ):
            raise ValueError("protected admission differs from trusted installed database")
        tls = {name: secret[name] for name in ("ca.pem", "certificate.pem", "private-key.pem")}
        # Current trusted management TLS must match; secret content cannot pick a
        # different manager endpoint or credential principal.
        from loom_capacity_agent.client import read_owner_only_bytes

        for name, path in (
            ("ca.pem", runtime._config.tls_files.ca_file),
            ("certificate.pem", runtime._config.tls_files.certificate_file),
            ("private-key.pem", runtime._config.tls_files.private_key_file),
        ):
            if tls[name] != read_owner_only_bytes(path):
                raise ValueError("installed membership management TLS differs from trusted files")
        documents, digest = runtime._manifests(
            claim=credential_claim,
            identity=identity,
            credentials=credentials,
            configuration=configuration,
            database=database,
            tls=tls,
        )
        if digest != installation.capacity_agent_installation_sha256:
            raise ValueError("membership installation digest differs from trusted manifest")
        if installation.supported_pool_ids != tuple(
            sorted({item.pool_id for item in expected.pool_capabilities})
        ) or installation.supported_architectures != tuple(
            sorted({item.cpu_architecture for item in expected.pool_capabilities})
        ):
            raise ValueError("membership installation capabilities differ from trusted manifest")
        alternatives = [documents]
        if retirement:
            for generation in (
                claim.operation.expected_operation_epoch,
                claim.operation.operation_epoch,
            ):
                alternative, changed_digest = runtime._manifests(
                    claim=credential_claim,
                    identity=identity,
                    credentials=credentials,
                    configuration=expected.model_copy(
                        update={"configuration_generation": generation}
                    ),
                    database=database,
                    tls=tls,
                )
                if changed_digest != digest:
                    raise ValueError("retirement changed stable installation evidence")
                alternatives.append(alternative)
            documents = alternatives[-1]
        if require_current:
            alternatives = [documents]
        for index, document in enumerate(documents[1:], start=1):
            metadata = document["metadata"]
            assert isinstance(metadata, dict)
            actual = await runtime._kubectl.read_resource_json(
                namespace=identity.namespace,
                kind=str(document["kind"]),
                name=str(metadata["name"]),
            )
            if not any(
                _contains_contract(actual, alternative[index]) for alternative in alternatives
            ):
                raise ValueError("installed membership workload differs from trusted manifest")
        return identity, expected, database, documents

    def _retained_installation(
        self, claim: PersonalDevReconciliationClaim, token: str
    ) -> PersonalDevCapacityInstallation:
        operation = claim.operation
        if (
            operation.capacity_reporter_incarnation is None
            or operation.protected_admission_sha256 is None
            or operation.capacity_agent_installation_sha256 is None
            or operation.capacity_supported_pool_ids is None
            or operation.capacity_supported_architectures is None
            or hashlib.sha256(token.encode("ascii")).hexdigest()
            != operation.capacity_reporter_token_sha256
        ):
            raise ValueError("retained membership installation is incomplete")
        return PersonalDevCapacityInstallation(
            reporter_incarnation=operation.capacity_reporter_incarnation,
            reporter_token=token,
            protected_admission_sha256=operation.protected_admission_sha256,
            capacity_agent_installation_sha256=operation.capacity_agent_installation_sha256,
            supported_pool_ids=operation.capacity_supported_pool_ids,
            supported_architectures=operation.capacity_supported_architectures,
        )

    async def observe(
        self,
        claim: PersonalDevReconciliationClaim,
        installation: PersonalDevCapacityInstallation,
        checkpoint: PersonalMembershipCheckpointV1,
        *,
        observed_at: datetime,
        retirement: bool = False,
    ) -> PersonalDevMembershipObservationV1:
        validate_membership_observation_context(
            claim, checkpoint, self.installer._membership_execution, observed_at
        )
        identity, configuration, database, documents = await self._installed(
            claim, installation, retirement=retirement
        )
        snapshot = await cast(
            PersonalDevMembershipDatabase, self.installer._database
        ).observe_membership(
            identity=identity,
            configuration=configuration,
            agent_database_url=database.agent_database_url,
            retirement_from_generation=claim.operation.expected_operation_epoch
            if retirement
            else None,
        )
        if retirement:
            await self.installer._kubectl.apply(
                yaml.safe_dump_all(documents, sort_keys=False, explicit_start=True)
            )
        await self._installed(claim, installation, retirement=retirement, require_current=True)
        operation = claim.operation
        assert operation.local_activation_sha256 is not None
        binding = {
            "operation_id": str(operation.id),
            "operation_epoch": operation.operation_epoch,
            "attempt_id": str(claim.attempt.id),
            "observation_lease_epoch": claim.attempt.lease_epoch,
            "observed_at": observed_at.isoformat(),
            "execution": checkpoint.execution.model_dump(mode="json"),
            "local_activation_sha256": operation.local_activation_sha256,
            "capacity_agent_installation_sha256": installation.capacity_agent_installation_sha256,
            "protected": snapshot,
        }
        acknowledgement = SubjectExecutionAcknowledgementV2(
            subject_id=configuration.subject_id,
            subject_incarnation=configuration.subject_incarnation,
            configuration_generation=configuration.configuration_generation,
            deployment_generation=configuration.deployment_generation,
            candidate=CandidateBindingV2(
                algorithm="source-sha256",
                identity=configuration.candidate_identity,
                publication_sha256=configuration.candidate_publication_sha256,
            ),
            reporter_incarnation=configuration.reporter_incarnation,
            protected_admission_sha256=installation.protected_admission_sha256,
            legacy_writer_high_water=snapshot["legacy_writer_high_water"],
            acknowledgement_sha256=hashlib.sha256(
                json.dumps(binding, sort_keys=True, separators=(",", ":")).encode("ascii")
            ).hexdigest(),
        )
        return PersonalDevMembershipObservationV1(
            operation_id=operation.id,
            operation_epoch=operation.operation_epoch,
            attempt_id=claim.attempt.id,
            observation_lease_epoch=claim.attempt.lease_epoch,
            observed_at=observed_at,
            execution=checkpoint.execution,
            local_activation_sha256=operation.local_activation_sha256,
            capacity_agent_installation_sha256=installation.capacity_agent_installation_sha256,
            acknowledgement=acknowledgement,
        )

    async def retirement(
        self,
        claim: PersonalDevReconciliationClaim,
        checkpoint: PersonalMembershipCheckpointV1,
        *,
        observed_at: datetime,
    ) -> PersonalDevMembershipObservationV1:
        from loom.personal_dev_capacity_runtime import _SECRET_NAME, _decode_secret_text

        if claim.operation.kind != "destroy":
            raise ValueError("membership retirement requires destroy")
        validate_membership_observation_context(
            claim, checkpoint, self.installer._membership_execution, observed_at
        )
        secret = await self.installer._kubectl.read_secret_optional(
            derive_identity(claim.operation.environment_name).namespace, _SECRET_NAME
        )
        if secret is None:
            raise ValueError("retained membership agent is unavailable")
        installation = self._retained_installation(
            claim, _decode_secret_text(secret, "reporter-token")
        )
        return await self.observe(
            claim, installation, checkpoint, observed_at=observed_at, retirement=True
        )

    async def verify(
        self, claim: PersonalDevReconciliationClaim, envelope: PersonalDevMembershipEnvelopeV1
    ) -> None:
        from loom.personal_dev_capacity_runtime import _SECRET_NAME, _decode_secret_text

        operation = claim.operation
        projection = envelope.request.projection
        if (
            envelope.observation.operation_id != operation.id
            or envelope.observation.operation_epoch != operation.operation_epoch
            or projection.subject_id != operation.subject_id
            or projection.subject_incarnation != operation.subject_incarnation
            or projection.candidate_sha256 != operation.candidate_sha
            or projection.candidate_publication_sha256 != claim.candidate.publication_sha256
            or projection.configuration_generation != operation.operation_epoch
            or projection.deployment_generation != operation.deployment_generation
            or projection.local_activation_sha256 != operation.local_activation_sha256
            or projection.operation_kind != operation.kind
        ):
            raise ValueError("membership verification differs from persisted operation")
        identity = derive_identity(operation.environment_name)
        secret = await self.installer._kubectl.read_secret_optional(
            identity.namespace, _SECRET_NAME
        )
        if secret is None:
            raise ValueError("membership agent is unavailable")
        installation = PersonalDevCapacityInstallation(
            reporter_incarnation=projection.demand_reporter_incarnation,
            reporter_token=_decode_secret_text(secret, "reporter-token"),
            protected_admission_sha256=projection.protected_admission_sha256,
            capacity_agent_installation_sha256=projection.capacity_agent_installation_sha256,
            supported_pool_ids=cast(
                tuple[Literal["oldlab", "gb10"], ...], projection.supported_pool_ids
            ),
            supported_architectures=projection.supported_architectures,
        )
        if installation.reporter_token_sha256 != projection.demand_reporter_token_sha256:
            raise ValueError("membership reporter credential differs from persisted request")
        for wait in (True, False):
            _, configuration, database, _ = await self._installed(
                claim, installation, retirement=operation.kind == "destroy", require_current=True
            )
            await cast(PersonalDevMembershipDatabase, self.installer._database).observe_membership(
                identity=identity,
                configuration=configuration,
                agent_database_url=database.agent_database_url,
            )
            if wait:
                await self.installer._kubectl.wait_deployment(
                    identity.namespace, "loom-capacity-agent"
                )
