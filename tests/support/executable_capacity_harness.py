"""Executable-v2 integration harness spanning manager, agents, and Slurm."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID, uuid5

import httpx
from alembic import command
from alembic.config import Config as AlembicConfig
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import create_engine, insert, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from tests.support.fake_slurm import FakeSlurm

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_capacity_agent.admission import (
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    PhysicalJobBindingV2,
    PublishableExecutableProtectedReleaseV2,
)
from loom_capacity_agent.claim_guard import (
    ExecutableClaimProposalV2,
    InertAttemptTransitionV1,
)
from loom_capacity_agent.client import DemandPublishError, DemandReporterClient
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    ReporterConfigurationV1,
)
from loom_capacity_agent.executable_admission import ExecutableAdmissionStore
from loom_capacity_agent.executable_release_reporter import (
    ExecutableProtectedReleaseReporterRuntime,
)
from loom_capacity_agent.lifecycle_store import CapacityAttemptLifecycleStore
from loom_capacity_agent.store import CapacityAgentStore, read_next_executable_protected_release
from loom_capacity_executor.bootstrap_handoff import (
    BootstrapHandoffStore,
)
from loom_capacity_executor.client import (
    ExecutableCapacityExecutorClient,
)
from loom_capacity_executor.config import ImmutablePoolManifest, PoolExecutorConfig
from loom_capacity_executor.executable import ExecutablePoolExecutor, ExecutorTickResult
from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_executor.keys import ExecutorOwnershipKey
from loom_capacity_executor.launch_renderer import (
    OperatorLaunchProfileV2,
    OperatorResourceDomainV2,
    canonical_launch_policy_digest,
)
from loom_capacity_executor.runtime import (
    ActivationRuntimeArtifactV2,
    AdmissionBindingEntryV2,
    RoutedExecutableAdmissionClient,
    build_executable_runtime,
    canonical_admission_directory_digest,
    canonical_approved_profiles_digest,
    write_admission_binding_directory,
)
from loom_capacity_executor.slurm_contracts import (
    SlurmExecutableIdentityV2,
    SlurmFileIdentityV2,
    SlurmLaunchRequestV2,
)
from loom_capacity_executor.trusted_launcher import (
    WORKER_CREDENTIAL_ENV,
    TrustedLauncherConfigV2,
    run_trusted_launcher_process,
)
from loom_capacity_guard.contracts import (
    GuardFenceV1,
    ProtectedAttemptV1,
    SealedRequirementsV1,
)
from loom_capacity_guard.contracts import (
    canonical_digest as canonical_guard_digest,
)
from loom_capacity_guard.store import CapacityGuardStore
from loom_capacity_manager.api import create_app
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.config import CapacityManagerSettings
from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    DemandBucketV1,
    DemandSnapshotV1,
    DevelopmentSubjectTemplateV1,
    DynamicDevelopmentSubjectProjectionV1,
    FleetManifestV1,
    NodeEnvelopeV1,
    PoolManifestV1,
    PoolObservationV1,
    ProfileReferenceV1,
    ResourceDomainV1,
    ResourceVectorV1,
    StaticCandidateProvenanceV1,
    SubjectConfigurationV1,
    TierPolicyV1,
    WorkerShapeV1,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableProtectedReleaseV2,
    ExecutionActivationV2,
    ExecutionAuthorityV2,
    ExecutionContextV2,
    ExecutionDrainV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    ExecutionRetirementExecutorCheckpointV2,
    ExecutionRetirementV2,
    LegacyWriterFenceV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    SignedExecutableOwnershipProofV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityDevelopmentProjection,
    CapacityExecutableExecutorState,
    CapacityExecutableIntent,
)
from loom_capacity_manager.ownership import OwnershipKeyring, public_key_fingerprint
from loom_capacity_manager.store import CapacityManagementStore, ExecutionConflictError

_AUTHORITY_ID = UUID("0f8ad3cd-6cb9-5cb5-8270-356dfe5f98ad")
_NAMESPACE = UUID("1a2d45de-825b-5b74-a0a7-16afbc193f8d")
_FIXED_TIME = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
_TRUSTED_RELEASE = hashlib.sha256(b"task-13-trusted-launcher-release").hexdigest()
_OPERATOR_TOKEN = "task-13-capacity-operator"
_POOL_TOKENS = {
    "gb10": "task-13-gb10-pool-reporter",
    "oldlab": "task-13-oldlab-pool-reporter",
}
_EXECUTOR_TOKENS = {
    "gb10": "task-13-gb10-executor",
    "oldlab": "task-13-oldlab-executor",
}
_POOL_ORDER = ("gb10", "oldlab")


def _uuid(label: str) -> UUID:
    return uuid5(_NAMESPACE, label)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _token_sha256(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _write_private(path: Path, value: str) -> Path:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


def _write_private_bytes(path: Path, value: bytes) -> Path:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        os.write(descriptor, value)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


def _database_value(database: Mapping[str, object], key: str) -> str:
    value = database[key]
    if not isinstance(value, str):
        raise TypeError(f"protected database {key} is not a string")
    return value


@dataclass(frozen=True, slots=True)
class CanonicalDigests:
    allocation: str
    inventory: str
    release: str
    evidence: str


@dataclass(frozen=True, slots=True)
class ManagerIntentEvidence:
    intent_id: UUID
    subject_id: UUID
    pool_id: str
    state: str
    observed_state: str | None
    concurrency_slots: int


@dataclass(frozen=True, slots=True)
class ExecutorIdentity:
    registration: ExecutableExecutorRegistrationV2
    client: ExecutableCapacityExecutorClient
    heartbeat_sequence: int


@dataclass(slots=True)
class _Attempt:
    protected_attempt_id: UUID
    execution_generation: int
    requirements_digest: str
    claimed: bool = False
    terminal: bool = False


@dataclass(slots=True)
class _Worker:
    binding: ExecutableIntentBindingV2
    job_id: str
    registration: ExecutableWorkerRegistrationV2
    credential: str


@dataclass(frozen=True, slots=True)
class _TrustedProcessEntry:
    process_argv: tuple[str, ...]
    submitted_launcher_argv: tuple[str, ...]
    worker_credential: str | None


@dataclass(slots=True)
class _Claim:
    owner: OwnerHandle
    attempt: _Attempt
    worker: _Worker
    operation_id: UUID
    claim_high_water: int
    terminal: bool = False


@dataclass(frozen=True, slots=True)
class ProtectedDrainEvidence:
    intent_id: UUID
    event_kind: Literal["released", "withdrawn", "prepared-revoked"]
    executor_status: str
    manager_state: str
    terminal_kind: str | None


@dataclass(frozen=True, slots=True)
class ProtectedReleaseReplayEvidence:
    intent_id: UUID
    event_kind: Literal["released", "withdrawn", "prepared-revoked"]
    release_digest: str
    publish_attempts: int
    idempotency_keys: tuple[UUID, ...]
    manager_replayed_flags: tuple[bool, ...]
    guard_publication_count: int


class _RecordingExecutableReleasePublisher:
    def __init__(
        self,
        inner: DemandReporterClient,
        *,
        lose_response_after_manager_ack: bool,
    ) -> None:
        self._inner = inner
        self._lose_response_after_manager_ack = lose_response_after_manager_ack
        self.calls: list[tuple[PublishableExecutableProtectedReleaseV2, UUID]] = []
        self.replayed: list[bool] = []

    async def publish_executable_protected_release(
        self,
        publication: PublishableExecutableProtectedReleaseV2,
        *,
        idempotency_key: UUID,
    ) -> Any:
        self.calls.append((publication, idempotency_key))
        receipt = await self._inner.publish_executable_protected_release(
            publication,
            idempotency_key=idempotency_key,
        )
        self.replayed.append(receipt.replayed)
        if self._lose_response_after_manager_ack:
            self._lose_response_after_manager_ack = False
            raise RuntimeError("simulated response loss after manager acknowledgement")
        return receipt


class _SwitchableASGITransport(httpx.AsyncBaseTransport):
    """Keep long-lived clients while swapping the in-process manager epoch."""

    def __init__(self) -> None:
        self._delegate: httpx.ASGITransport | None = None
        self.online = True

    def bind(self, app: Any) -> None:
        self._delegate = httpx.ASGITransport(app=app)
        self.online = True

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if not self.online or self._delegate is None:
            raise httpx.ConnectError("capacity manager is offline", request=request)
        return await self._delegate.handle_async_request(request)

    async def aclose(self) -> None:
        if self._delegate is not None:
            await self._delegate.aclose()


class _ProtectedAdmissionRouter:
    """Open a fresh protected executor-role transaction for every operation."""

    def __init__(self, harness: ExecutableCapacityHarness) -> None:
        self._harness = harness
        self._registrations: dict[UUID, ExecutableWorkerRegistrationV2] = {}

    def _owner(self, binding: ExecutableIntentBindingV2) -> OwnerHandle:
        owner = self._harness._owners_by_subject.get(binding.subject_id)
        if owner is None:
            raise RuntimeError("executable intent has no protected owner database")
        return owner

    @asynccontextmanager
    async def _store(
        self,
        binding: ExecutableIntentBindingV2,
    ) -> AsyncIterator[ExecutableAdmissionStore]:
        owner = self._owner(binding)
        engine = create_async_engine(
            make_url(_database_value(owner.database, "executor_url")),
            isolation_level="SERIALIZABLE",
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session, session.begin():
                yield ExecutableAdmissionStore(session, registration=owner.registration)
        finally:
            await engine.dispose()

    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> Any:
        async with self._store(request.binding) as store:
            return await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)

    def bootstrap_handoff_route_sha256(self, binding: ExecutableIntentBindingV2) -> str:
        owner = self._owner(binding)
        payload = {
            "subject_id": str(binding.subject_id),
            "subject_incarnation": str(binding.subject_incarnation),
            "configuration_generation": binding.execution.configuration_epoch,
            "deployment_generation": binding.deployment_generation,
            "candidate_generation": binding.candidate_generation,
            "protected_admission_sha256": owner.projection.protected_admission_sha256,
            "database_url_sha256": hashlib.sha256(
                _database_value(owner.database, "executor_url").encode("utf-8")
            ).hexdigest(),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> Any:
        async with self._store(request.binding) as store:
            return await store.bind_slurm_job(request)

    async def observe_intent(self, binding: ExecutableIntentBindingV2) -> Any:
        async with self._store(binding) as store:
            return await store.observe_intent(binding)

    async def begin_drain(self, request: Any) -> Any:
        async with self._store(request.binding) as store:
            return await store.begin_drain(request)

    async def withdraw_unregistered_worker(self, request: Any) -> Any:
        async with self._store(request.binding) as store:
            return await store.withdraw_unregistered_worker(request)

    async def revoke_prepared_bootstrap(
        self,
        request: ExecutablePreparedBootstrapRevocationV2,
    ) -> Any:
        async with self._store(request.binding) as store:
            return await store.revoke_prepared_bootstrap(request)

    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str,
    ) -> Any:
        async with self._store(request.binding) as store:
            receipt = await store.register_worker(
                request,
                bootstrap_capability=bootstrap_capability,
            )
        self._harness._record_worker_registration(request)
        self._registrations[request.binding.intent_id] = request
        return receipt

    def last_registration(self, intent_id: UUID) -> ExecutableWorkerRegistrationV2:
        try:
            return self._registrations[intent_id]
        except KeyError as exc:
            raise RuntimeError("protected worker registration was not recorded") from exc

    async def admit_claim(self, binding: ExecutableIntentBindingV2, request: Any) -> Any:
        async with self._store(binding) as store:
            return await store.admit_claim(request)

    async def acknowledge_release(
        self,
        binding: ExecutableIntentBindingV2,
        request: ExecutableReleaseRequestV2,
        *,
        current_worker_credential: str,
    ) -> Any:
        async with self._store(binding) as store:
            return await store.acknowledge_release(
                request,
                current_worker_credential=current_worker_credential,
            )


class _HarnessAdmissionClient:
    """Per-subject local test DB client reached only after routed resolution."""

    def __init__(
        self,
        harness: ExecutableCapacityHarness,
        database_url: bytes,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
    ) -> None:
        self._harness = harness
        self.subject_id = subject_id
        self.subject_incarnation = subject_incarnation
        try:
            resolved_url = database_url.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError("resolved admission URL is not UTF-8") from exc
        owner = harness._owners_by_subject.get(subject_id)
        if owner is None or owner.subject_incarnation != subject_incarnation:
            raise RuntimeError("resolved admission subject is not installed in harness")
        if resolved_url != _database_value(owner.database, "executor_url"):
            raise RuntimeError("resolved admission URL differs from subject binding")
        self._owner = owner
        self._engine = create_async_engine(
            make_url(resolved_url),
            isolation_level="SERIALIZABLE",
        )
        self._factory = async_sessionmaker(self._engine, expire_on_commit=False)

    async def aclose(self) -> None:
        await self._engine.dispose()

    @asynccontextmanager
    async def _store(self) -> AsyncIterator[ExecutableAdmissionStore]:
        async with self._factory() as session, session.begin():
            yield ExecutableAdmissionStore(session, registration=self._owner.registration)

    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> Any:
        async with self._store() as store:
            return await store.prepare_worker(request, bootstrap_sha256=bootstrap_sha256)

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> Any:
        async with self._store() as store:
            return await store.bind_slurm_job(request)

    async def observe_intent(self, binding: ExecutableIntentBindingV2) -> Any:
        async with self._store() as store:
            return await store.observe_intent(binding)

    async def begin_drain(self, request: Any) -> Any:
        async with self._store() as store:
            return await store.begin_drain(request)

    async def withdraw_unregistered_worker(self, request: Any) -> Any:
        async with self._store() as store:
            return await store.withdraw_unregistered_worker(request)

    async def revoke_prepared_bootstrap(
        self,
        request: ExecutablePreparedBootstrapRevocationV2,
    ) -> Any:
        async with self._store() as store:
            return await store.revoke_prepared_bootstrap(request)

    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str,
    ) -> Any:
        async with self._store() as store:
            receipt = await store.register_worker(
                request,
                bootstrap_capability=bootstrap_capability,
            )
        self._harness._record_worker_registration(request)
        return receipt

    async def acknowledge_release(
        self,
        request: ExecutableReleaseRequestV2,
        *,
        current_worker_credential: str,
    ) -> Any:
        async with self._store() as store:
            return await store.acknowledge_release(
                request,
                current_worker_credential=current_worker_credential,
            )

    async def admit_claim(self, proposal: ExecutableClaimProposalV2) -> Any:
        async with self._store() as store:
            return await store.admit_claim(proposal)


@dataclass(slots=True)
class OwnerHandle:
    harness: ExecutableCapacityHarness
    name: str
    database: dict[str, object]
    subject_id: UUID
    subject_incarnation: UUID
    owner_id: UUID
    candidate_sha256: str
    candidate_publication_sha256: str
    candidate_generation: int
    deployment_generation: int
    configuration_generation: int
    reporter_incarnation: UUID
    reporter_token: str
    registration: AgentRegistrationV1
    projection: DynamicDevelopmentSubjectProjectionV1
    demand_sequence: int = 0
    demand_kind: Literal["x86", "arm", "neutral", "zero"] = "zero"
    demand_count: int = 0
    active: bool = True
    attempts: list[_Attempt] = field(default_factory=list)

    @property
    def candidate(self) -> CandidateBindingV2:
        return CandidateBindingV2(
            algorithm="source-sha256",
            identity=self.candidate_sha256,
            publication_sha256=self.candidate_publication_sha256,
        )

    async def publish_x86_demand(self, slots: int) -> None:
        await self.harness._publish_owner_demand(self, "x86", slots)

    async def publish_arm_demand(self, slots: int) -> None:
        await self.harness._publish_owner_demand(self, "arm", slots)

    async def publish_neutral_demand(self, slots: int) -> None:
        await self.harness._publish_owner_demand(self, "neutral", slots)

    async def publish_zero_demand(self) -> None:
        await self.harness._publish_owner_demand(self, "zero", 0)


class PoolHarness:
    """Test-facing view of one real executor and one subprocess controller."""

    def __init__(
        self,
        harness: ExecutableCapacityHarness,
        pool_id: str,
        fake: FakeSlurm,
        profile: OperatorLaunchProfileV2,
    ) -> None:
        self.harness = harness
        self.pool_id = pool_id
        self.fake = fake
        self.profile = profile
        self.registration: ExecutableExecutorRegistrationV2 | None = None
        self.ownership_key: ExecutorOwnershipKey | None = None
        self.client: ExecutableCapacityExecutorClient | None = None
        self.executor: ExecutablePoolExecutor | None = None
        self.journal: ExecutorJournal | None = None
        self.journal_path: Path | None = None
        self.runtime_config: PoolExecutorConfig | None = None
        self.runtime_artifact: ActivationRuntimeArtifactV2 | None = None
        self.handoff_directory = harness.root / "bootstrap-handoff" / pool_id
        self.handoff_directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.handoff_directory.chmod(0o700)
        self.handoff_store = BootstrapHandoffStore(self.handoff_directory)
        self.heartbeat_sequence = 0
        self.last_inventory: ExecutableExecutorInventoryV2 | None = None

    async def install(
        self,
        registration: ExecutableExecutorRegistrationV2,
        ownership_key: ExecutorOwnershipKey,
        journal_path: Path,
    ) -> None:
        if self.journal is not None:
            self.journal.close()
        self.registration = registration
        self.ownership_key = ownership_key
        self.journal_path = journal_path
        self.heartbeat_sequence = 0
        await self._rebuild_executor()

    async def _rebuild_executor(self) -> None:
        if self.registration is None or self.ownership_key is None or self.journal_path is None:
            raise RuntimeError("pool runtime is incomplete")
        state_directory = self.journal_path.parent
        state_directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        state_directory.chmod(0o700)
        self.client = ExecutableCapacityExecutorClient(
            self.registration,
            manager_origin="https://capacity.test",
            bearer_token=_EXECUTOR_TOKENS[self.pool_id],
            http_client=self.harness.http,
        )
        current_context = await self.client.current_execution_context()
        profiles = self.harness.approved_profiles(self.pool_id)
        approved_profiles_sha256 = canonical_approved_profiles_digest(profiles)
        authority = self.fake.backend().authority
        key_path = _write_private_bytes(
            self.harness.root
            / "executor-keys"
            / f"epoch-{self.harness._epoch_number}"
            / f"{self.pool_id}.raw",
            self.ownership_key.private_key.private_bytes_raw(),
        )
        bearer_path = _write_private(
            self.harness.root
            / "executor-tokens"
            / f"epoch-{self.harness._epoch_number}"
            / f"{self.pool_id}.token",
            _EXECUTOR_TOKENS[self.pool_id],
        )
        executables = authority.executables
        manifest = ImmutablePoolManifest(
            pool_id=cast(Any, self.pool_id),
            pool_generation=self.registration.pool_generation,
            controller_authority_sha256=self.registration.controller_authority_sha256,
            approved_profiles_sha256=approved_profiles_sha256,
            executor_id=self.registration.executor_id,
            executor_incarnation=self.registration.executor_incarnation,
            local_authority_sha256=self.registration.local_authority_sha256,
            signing_key_id=self.registration.signing_key_id,
            signing_key_sha256=self.registration.signing_key_sha256,
            ownership_key_file=key_path,
            manager_origin="https://capacity.test",
            bearer_token_file=bearer_path,
            tls_ca_file=self.harness._ca_path,
            tls_certificate_file=self.harness._cert_path,
            tls_private_key_file=self.harness._key_path,
            state_directory=state_directory,
            journal_file=self.journal_path,
            local_uid=os.geteuid(),
            slurm_cluster=authority.cluster,
            controller_host=authority.controller_host,
            partition=authority.partition,
            association=authority.account,
            submitter=authority.submitter,
            qos=authority.qos,
            profile_id=self.profile.profile_id,
            profile_generation=self.profile.profile_generation,
            profile_digest=self.profile.profile_digest,
            slurm_executables=tuple(
                sorted(
                    (
                        (name, Path(getattr(executables, name).path))
                        for name in ("sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue")
                    ),
                    key=lambda item: item[0],
                )
            ),
            executor_image=self.profile.image_digest,
            service_user=self.profile.submitter,
        )
        self.runtime_config = PoolExecutorConfig(
            pool_id=cast(Any, self.pool_id),
            pool_generation=self.registration.pool_generation,
            executor_id=self.registration.executor_id,
            executor_incarnation=self.registration.executor_incarnation,
            controller_authority_sha256=self.registration.controller_authority_sha256,
            approved_profiles_sha256=approved_profiles_sha256,
            local_authority_sha256=self.registration.local_authority_sha256,
            signing_key_id=self.registration.signing_key_id,
            signing_key_sha256=self.registration.signing_key_sha256,
            ownership_key_file=key_path,
            ownership_key=self.ownership_key,
            manager_origin="https://capacity.test",
            local_uid=os.geteuid(),
            bearer_token_file=bearer_path,
            tls_ca_file=self.harness._ca_path,
            tls_certificate_file=self.harness._cert_path,
            tls_private_key_file=self.harness._key_path,
            state_directory=state_directory,
            journal_file=self.journal_path,
            slurm_cluster=authority.cluster,
            controller_host=authority.controller_host,
            partition=authority.partition,
            association=authority.account,
            submitter=authority.submitter,
            qos=authority.qos,
            profile_id=self.profile.profile_id,
            profile_generation=self.profile.profile_generation,
            profile_digest=self.profile.profile_digest,
            executor_image=self.profile.image_digest,
            service_user=self.profile.submitter,
            manifest=manifest,
            expected_manifest_sha256=manifest.sha256(),
            execution=self.registration.execution,
        )
        admission_directory = self.harness.admission_directory(self.pool_id)
        admission_directory_sha256 = canonical_admission_directory_digest(admission_directory)
        self.runtime_artifact = ActivationRuntimeArtifactV2(
            execution=self.registration.execution,
            pool_id=cast(Any, self.pool_id),
            pool_generation=self.registration.pool_generation,
            executor_id=self.registration.executor_id,
            executor_incarnation=self.registration.executor_incarnation,
            controller_authority_sha256=self.registration.controller_authority_sha256,
            approved_profiles_sha256=approved_profiles_sha256,
            local_authority_sha256=self.registration.local_authority_sha256,
            signing_key_id=self.registration.signing_key_id,
            signing_key_sha256=self.registration.signing_key_sha256,
            immutable_manifest_sha256=manifest.sha256(),
            admission_directory=str(admission_directory),
            admission_directory_sha256=admission_directory_sha256,
            handoff_directory=str(self.handoff_directory),
            journal_file=str(self.journal_path),
            state_directory=str(state_directory),
            slurm_authority=authority,
            profiles=profiles,
        )
        self.executor = build_executable_runtime(
            self.runtime_config,
            self.runtime_artifact,
            manager_client=self.client,
            current_context=current_context,
            admission_client_factory=self.harness.routed_admission_factory,
            slurm_backend_factory=lambda _authority: self.fake.backend(),
        )
        self.executor._now = lambda: _FIXED_TIME
        self.journal = self.executor.journal

    async def restart(self) -> None:
        if self.journal is None or self.journal_path is None:
            raise RuntimeError("pool executor is not installed")
        self.journal.close()
        await self._rebuild_executor()

    async def tick(self) -> ExecutorTickResult:
        if self.executor is None:
            raise RuntimeError("pool executor is not installed")
        return await self.executor.tick()

    async def recover(self) -> ExecutorTickResult:
        if self.executor is None:
            raise RuntimeError("pool executor is not installed")
        return await self.executor.recover()

    async def heartbeat(self) -> None:
        if self.journal is None or self.registration is None or self.client is None:
            raise RuntimeError("pool executor is not installed")
        heartbeat = await ExecutableHeartbeatLoop(
            self.registration,
            self.journal,
            self.client,
        ).heartbeat()
        self.heartbeat_sequence = heartbeat.heartbeat_sequence

    def owner_slots(self, subject_id: UUID) -> int:
        bindings = self.harness._pool_job_bindings(self.pool_id)
        return sum(
            binding.resources.slots
            for job_id, binding in bindings.items()
            if binding.subject_id == subject_id
            and any(job["job_id"] == job_id for job in self.fake.live_jobs())
        )

    def inventory_records(self) -> tuple[dict[str, Any], ...]:
        return self.fake.live_jobs()

    def fail_command(self, command: str) -> None:
        self.fake.set_fault(command, "failure")

    def clear_command_fault(self, command: str) -> None:
        self.fake.clear_fault(command)

    def fail_submission_after_mutation(self) -> None:
        self.fake.set_fault("sbatch", "after_mutation_failure")

    def add_foreign_job(self, job_id: str) -> str:
        return self.fake.add_foreign_job(job_id)

    def job_snapshot(self, job_id: str) -> dict[str, Any]:
        return self.fake.job_snapshot(job_id)

    def replace_job(self, job_id: str, **changes: object) -> None:
        self.fake.replace_job(job_id, **changes)

    def restore_job(self, job_id: str, snapshot: dict[str, Any]) -> None:
        self.fake.restore_job(job_id, snapshot)

    def loom_job_snapshot(self) -> tuple[dict[str, Any], ...]:
        return self.fake.live_jobs()

    def unscoped_query_job_ids(self) -> tuple[str, ...]:
        return self.fake.unscoped_squeue_job_ids()

    async def scoped_inventory_job_ids(self) -> tuple[str, ...]:
        return tuple(item.job_id for item in await self.fake.backend().inventory())

    def latest_loom_job_id(self) -> str:
        jobs = self.fake.live_jobs()
        if not jobs:
            raise RuntimeError("pool has no Loom job")
        return max((str(job["job_id"]) for job in jobs), key=int)

    @property
    def sbatch_calls(self) -> tuple[Any, ...]:
        return self.fake.sbatch_calls

    def cancelled_job_ids(self) -> tuple[str, ...]:
        return tuple(call.argv[-1] for call in self.fake.scancel_calls)

    def runtime_entry_components(self) -> dict[str, object]:
        if self.executor is None or self.runtime_artifact is None:
            raise RuntimeError("pool runtime is not installed")
        config_path = Path(self.profile.trusted_launcher_config.path)
        return {
            "runtime_artifact": type(self.runtime_artifact).__name__,
            "admission_client": type(self.executor.admission).__name__,
            "profile_count": len(self.executor.profiles),
            "trusted_launcher_config_verified": (
                config_path.is_file()
                and hashlib.sha256(config_path.read_bytes()).hexdigest()
                == self.profile.trusted_launcher_config.sha256
            ),
        }

    def close(self) -> None:
        if self.journal is not None:
            self.journal.close()


class ExecutableCapacityHarness:
    """One isolated, deterministic executable-capacity deployment."""

    def __init__(
        self,
        root: Path,
        capacity_url: str,
        protected_databases: dict[str, dict[str, object]],
        cleanup: Any,
    ) -> None:
        self.root = root
        self.capacity_url = capacity_url
        self.protected_databases = protected_databases
        self._cleanup = cleanup
        self._transport = _SwitchableASGITransport()
        self.http = httpx.AsyncClient(transport=self._transport)
        self._app: Any = None
        self._lifespan: Any = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None
        self.management_store = CapacityManagementStore()
        self.execution_store = CapacityExecutionStore()
        self._owners: dict[str, OwnerHandle] = {}
        self._owners_by_subject: dict[UUID, OwnerHandle] = {}
        self._workers: dict[UUID, _Worker] = {}
        self._worker_registrations: dict[UUID, ExecutableWorkerRegistrationV2] = {}
        self._trusted_process_entries: dict[UUID, _TrustedProcessEntry] = {}
        self._intent_launch_ranks: dict[UUID, int] = {}
        self._claims: list[_Claim] = []
        self._admission = _ProtectedAdmissionRouter(self)
        self._execution: ExecutionAuthorityV2 | None = None
        self._drained: ExecutionContextV2 | None = None
        self._configuration_epoch = 0
        self._fleet_proposal_digest = ""
        self._fleet_reference: ConfigurationGenerationRefV1 | None = None
        self._subject_references: dict[UUID, ConfigurationGenerationRefV1] = {}
        self._epoch_number = 0
        self._pool_sequences = {pool_id: 0 for pool_id in _POOL_ORDER}
        self._executor_material: dict[
            str,
            tuple[PreparedExecutorBindingV2, ExecutorOwnershipKey],
        ] = {}
        self._static_tokens = {index: f"task-13-static-{index}-reporter" for index in (1, 2)}
        self._admission_root = root / "admission-bindings"
        self._db_url_root = root / "admission-db-urls"
        self._trusted_launcher_root = root / "trusted-launcher"
        for directory in (
            self._admission_root,
            self._db_url_root,
            self._trusted_launcher_root,
        ):
            directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            directory.chmod(0o700)

        self.fakes = {
            "gb10": FakeSlurm(
                root / "slurm-gb10",
                cluster="gb10-controller",
                controller="ctl.gb10.internal",
                partition="gb10-loom",
                account="gb10-association",
                submitter="loom",
                qos="gb10-qos",
                next_job_id=2001,
            ),
            "oldlab": FakeSlurm(
                root / "slurm-oldlab",
                cluster="oldlab-controller",
                controller="ctl.oldlab.internal",
                partition="oldlab-loom",
                account="oldlab-association",
                submitter="loom",
                qos="oldlab-qos",
                next_job_id=1001,
            ),
        }
        self.fleet, self.profile_variants = self._build_fleet()
        self._base_development_profiles = cast(
            DevelopmentSubjectTemplateV1,
            self.fleet.development_subject_template,
        ).profiles
        self.profiles = {pool_id: self.profile_variants[pool_id][1] for pool_id in _POOL_ORDER}
        self.static_subjects = self._build_static_subjects()
        self.static_subject_ids = tuple(item.subject_id for item in self.static_subjects)
        self.pools = {
            pool_id: PoolHarness(self, pool_id, self.fakes[pool_id], self.profiles[pool_id])
            for pool_id in _POOL_ORDER
        }

        self._principals_path = root / "principals.json"
        self._database_path = _write_private(root / "capacity-url", capacity_url)
        self._cert_path = _write_private(root / "server.crt", "test")
        self._key_path = _write_private(root / "server.key", "test")
        self._ca_path = _write_private(root / "client-ca.crt", "test")
        self._ownership_path = root / "ownership-public-keys.json"

    @classmethod
    async def create(
        cls,
        root: Path,
        postgres_url: str,
        guard_template: dict[str, object],
        *,
        database_suffix: str | None = None,
    ) -> ExecutableCapacityHarness:
        capacity_url, protected, cleanup = cls._provision_databases(
            postgres_url,
            guard_template,
            database_suffix=database_suffix,
        )
        harness = cls(root, capacity_url, protected, cleanup)
        try:
            await harness._initialize()
        except BaseException:
            await harness.aclose()
            raise
        return harness

    @staticmethod
    def _provision_databases(
        postgres_url: str,
        template: dict[str, object],
        *,
        database_suffix: str | None = None,
    ) -> tuple[str, dict[str, dict[str, object]], Any]:
        source = make_url(postgres_url)
        cluster_admin = source.set(database="postgres")
        engine = create_engine(cluster_admin, isolation_level="AUTOCOMMIT")
        preparer = engine.dialect.identifier_preparer
        if database_suffix is not None:
            if re.fullmatch(r"[a-z0-9_]{1,48}", database_suffix) is None:
                raise ValueError("test database suffix must be a safe postgres identifier segment")
            suffix = database_suffix
        else:
            suffix = f"{os.getpid()}_{_uuid(str(datetime.now(UTC).timestamp())).hex[:8]}"
        capacity_name = f"loom_capacity_bridge_{suffix}"
        guard_names = {
            "alice": f"loom_guard_bridge_alice_{suffix}",
            "bob": f"loom_guard_bridge_bob_{suffix}",
        }
        created = [capacity_name, *guard_names.values()]
        try:
            with engine.connect() as connection:
                if database_suffix is not None:
                    for database_name in created:
                        connection.execute(
                            text(
                                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                            ),
                            {"database_name": database_name},
                        )
                        connection.exec_driver_sql(
                            f"DROP DATABASE IF EXISTS {preparer.quote(database_name)}"
                        )
                connection.exec_driver_sql(
                    f"CREATE DATABASE {preparer.quote(capacity_name)} TEMPLATE template0"
                )
                template_name = _database_value(template, "database_name")
                for name in guard_names.values():
                    connection.exec_driver_sql(
                        f"CREATE DATABASE {preparer.quote(name)} "
                        f"TEMPLATE {preparer.quote(template_name)}"
                    )
                    connection.exec_driver_sql(
                        f"GRANT CREATE ON DATABASE {preparer.quote(name)} "
                        f"TO {preparer.quote(_database_value(template, 'owner_role'))}"
                    )

            capacity_url = source.set(database=capacity_name).render_as_string(hide_password=False)
            repo_root = Path(__file__).resolve().parents[2]
            config = AlembicConfig(str(repo_root / "capacity_migrations" / "alembic.ini"))
            config.set_main_option("script_location", str(repo_root / "capacity_migrations"))
            previous = os.environ.get("LOOM_CAPACITY_DB_URL")
            os.environ["LOOM_CAPACITY_DB_URL"] = capacity_url
            try:
                command.upgrade(config, "head")
            finally:
                if previous is None:
                    os.environ.pop("LOOM_CAPACITY_DB_URL", None)
                else:
                    os.environ["LOOM_CAPACITY_DB_URL"] = previous

            protected: dict[str, dict[str, object]] = {}
            for owner, database_name in guard_names.items():
                protected[owner] = {
                    **template,
                    "database_name": database_name,
                    "admin_url": make_url(_database_value(template, "admin_url"))
                    .set(database=database_name)
                    .render_as_string(hide_password=False),
                    "migrator_url": make_url(_database_value(template, "migrator_url"))
                    .set(database=database_name)
                    .render_as_string(hide_password=False),
                    "agent_url": make_url(_database_value(template, "agent_url"))
                    .set(database=database_name)
                    .render_as_string(hide_password=False),
                    "executor_url": make_url(_database_value(template, "executor_url"))
                    .set(database=database_name)
                    .render_as_string(hide_password=False),
                }

            def cleanup() -> None:
                try:
                    with engine.connect() as connection:
                        for database_name in created:
                            connection.execute(
                                text(
                                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                                ),
                                {"database_name": database_name},
                            )
                            connection.exec_driver_sql(
                                f"DROP DATABASE IF EXISTS {preparer.quote(database_name)}"
                            )
                finally:
                    engine.dispose()

            return capacity_url, protected, cleanup
        except BaseException:
            try:
                with engine.connect() as connection:
                    for database_name in created:
                        connection.execute(
                            text(
                                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                            ),
                            {"database_name": database_name},
                        )
                        connection.exec_driver_sql(
                            f"DROP DATABASE IF EXISTS {preparer.quote(database_name)}"
                        )
            finally:
                engine.dispose()
            raise

    def _build_fleet(
        self,
    ) -> tuple[
        FleetManifestV1,
        dict[str, dict[int, OperatorLaunchProfileV2]],
    ]:
        pools: dict[str, PoolManifestV1] = {}
        references: dict[str, ProfileReferenceV1] = {}
        profile_variants: dict[str, dict[int, OperatorLaunchProfileV2]] = {}
        architectures = {"gb10": "arm64", "oldlab": "x86_64"}
        for pool_id in _POOL_ORDER:
            domain_id = f"{pool_id}-{architectures[pool_id]}"
            node_id = f"{pool_id}-node"
            pool = PoolManifestV1(
                pool_id=pool_id,
                pool_generation=1,
                pool_digest="0" * 64,
                controller=f"{pool_id}-controller",
                partition=f"{pool_id}-loom",
                association=f"{pool_id}-association",
                protocol_generation=1,
                protocol_digest=_sha(f"{pool_id}:protocol"),
                pool_reporter_incarnation=_uuid(f"pool-reporter:{pool_id}"),
                resource_domains=(
                    ResourceDomainV1(
                        domain_id=domain_id,
                        architecture=cast(Any, architectures[pool_id]),
                        partition=f"{pool_id}-loom",
                        nodes=(
                            NodeEnvelopeV1(
                                node_id=node_id,
                                allocatable=ResourceVectorV1(
                                    slots=2,
                                    cpu_millicores=2_000,
                                    memory_bytes=2 * 1024 * 1024 * 1024,
                                ),
                                features=("cpu",),
                            ),
                        ),
                    ),
                ),
                max_slots=2,
                max_pending_slots=2,
                max_pending_jobs=2,
                submission_rate_per_minute=4,
                health="eligible",
            )
            pool = pool.model_copy(
                update={"pool_digest": canonical_digest_excluding(pool, "pool_digest")}
            )
            pools[pool_id] = pool
            shapes = tuple(
                WorkerShapeV1(
                    shape_id={1: "one-slot", 2: "two-slot"}[slots],
                    concurrency_slots=slots,
                    total_resources=ResourceVectorV1(
                        slots=slots,
                        cpu_millicores=slots * 1_000,
                        memory_bytes=slots * 1024 * 1024 * 1024,
                    ),
                    node_resources=(
                        ResourceVectorV1(
                            slots=slots,
                            cpu_millicores=slots * 1_000,
                            memory_bytes=slots * 1024 * 1024 * 1024,
                        ),
                    ),
                    compatible_domain_ids=(domain_id,),
                    capabilities=("cpu",),
                    warm_approved=True,
                )
                for slots in (1, 2)
            )
            reference = ProfileReferenceV1(
                pool_id=pool_id,
                pool_generation=1,
                pool_digest=pool.pool_digest,
                profile_generation=1,
                profile_digest="0" * 64,
                protocol_generation=1,
                protocol_digest=pool.protocol_digest,
                eligible_resource_domains=(domain_id,),
                worker_shapes=shapes,
            )
            reference = reference.model_copy(
                update={
                    "profile_digest": canonical_digest_excluding(
                        reference,
                        "profile_digest",
                    )
                }
            )
            references[pool_id] = reference
            variants: dict[int, OperatorLaunchProfileV2] = {}
            for shape in shapes:
                profile = OperatorLaunchProfileV2(
                    pool_id=pool_id,
                    pool_generation=1,
                    profile_id=shape.shape_id,
                    profile_generation=1,
                    profile_digest=reference.profile_digest,
                    shape_id=shape.shape_id,
                    concurrency_slots=shape.concurrency_slots,
                    controller_authority_sha256="0" * 64,
                    slurm_cluster=pool.controller,
                    controller_host=cast(str, self.fakes[pool_id]._state["controller"]),
                    partition=pool.partition,
                    association=pool.association,
                    submitter="loom",
                    qos=cast(str, self.fakes[pool_id]._state["qos"]),
                    job_name_prefix=f"loom-{pool_id}",
                    resource_domains=(
                        OperatorResourceDomainV2(
                            domain_id=domain_id,
                            node_ids=(node_id,),
                            features=("cpu",),
                        ),
                    ),
                    cpus=shape.concurrency_slots,
                    resources=shape.total_resources,
                    time_limit_seconds=3_600,
                    launcher=SlurmExecutableIdentityV2(
                        path=str(self.fakes[pool_id].launcher),
                        sha256=self.fakes[pool_id].launcher_sha256,
                        owner_uid=self.fakes[pool_id].launcher.stat().st_uid,
                    ),
                    trusted_launcher_config=SlurmFileIdentityV2(
                        path=str(self.root / "trusted-launcher" / f"{pool_id}.json"),
                        sha256=_sha(f"{pool_id}:trusted-launcher-config"),
                        owner_uid=os.geteuid(),
                    ),
                    trusted_launcher_release_sha256=_TRUSTED_RELEASE,
                    image_digest=(
                        f"registry.example/loom/{pool_id}@sha256:{_sha(f'{pool_id}:image')}"
                    ),
                )
                variants[shape.concurrency_slots] = profile.model_copy(
                    update={"controller_authority_sha256": canonical_launch_policy_digest(profile)}
                )
            profile_variants[pool_id] = variants

        owner_template = AccountPolicyV1(
            account_id="personal-development-owner",
            kind="owner_template",
            max_slots=2,
            max_pending_slots=2,
            max_pending_jobs=2,
            submission_rate_per_minute=4,
            max_live_subjects=1,
        )
        service = AccountPolicyV1(
            account_id="shared-development",
            kind="service",
            max_slots=4,
            max_pending_slots=4,
            max_pending_jobs=4,
            submission_rate_per_minute=4,
            max_live_subjects=2,
        )
        template = DevelopmentSubjectTemplateV1(
            owner_account_template_id=owner_template.account_id,
            max_slots_per_subject=2,
            max_pending_slots_per_subject=2,
            max_pending_jobs_per_subject=2,
            profiles=tuple(references[pool_id] for pool_id in _POOL_ORDER),
        )
        fleet = FleetManifestV1(
            authority_incarnation=_AUTHORITY_ID,
            fleet_generation=1,
            fleet_digest="0" * 64,
            executable_new_capacity_ceiling=0,
            tiers=(
                TierPolicyV1(
                    tier_id="production",
                    priority=0,
                    max_slots=0,
                    max_pending_slots=0,
                    max_pending_jobs=0,
                ),
                TierPolicyV1(
                    tier_id="staging",
                    priority=1,
                    max_slots=0,
                    max_pending_slots=0,
                    max_pending_jobs=0,
                ),
                TierPolicyV1(
                    tier_id="development",
                    priority=2,
                    max_slots=4,
                    max_pending_slots=4,
                    max_pending_jobs=4,
                ),
            ),
            account_policies=(owner_template, service),
            pools=tuple(pools[pool_id] for pool_id in _POOL_ORDER),
            development_subject_template=template,
            global_max_pending_slots=4,
            global_max_pending_jobs=4,
            global_submission_rate_per_minute=4,
        )
        fleet = fleet.model_copy(
            update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")}
        )
        return fleet, profile_variants

    def _build_static_subjects(self) -> tuple[SubjectConfigurationV1, ...]:
        profiles = cast(
            DevelopmentSubjectTemplateV1, self.fleet.development_subject_template
        ).profiles
        return tuple(
            SubjectConfigurationV1(
                subject_id=_uuid(f"static:{index}:subject"),
                subject_incarnation=_uuid(f"static:{index}:incarnation"),
                display_name=f"static-{index}",
                account_id="shared-development",
                tier_id="development",
                min_slots=0,
                max_slots=2,
                max_pending_slots=2,
                max_pending_jobs=2,
                submission_rate_per_minute=2,
                lifecycle_state="active",
                candidate_generation=1,
                deployment_generation=1,
                configuration_generation=1,
                demand_reporter_incarnation=_uuid(f"static:{index}:reporter"),
                profiles=profiles,
            )
            for index in (1, 2)
        )

    async def _initialize(self) -> None:
        engine = create_async_engine(self.capacity_url, isolation_level="SERIALIZABLE")
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session, session.begin():
                await session.execute(
                    update(CapacityAuthorityState)
                    .where(CapacityAuthorityState.singleton_id == 1)
                    .values(
                        authority_incarnation=_AUTHORITY_ID,
                        writer_epoch=0,
                        recovery_state="shadow",
                        increase_freeze=True,
                        increase_freeze_reason="initial_shadow_freeze",
                        executable_new_capacity_ceiling=0,
                        execution_epoch=0,
                        execution_state="shadow",
                        execution_manifest_sha256=None,
                        global_pending_slot_ceiling=0,
                        global_pending_job_ceiling=0,
                        global_submission_rate_ceiling=0,
                    )
                )
        finally:
            await engine.dispose()
        await self._restart_manager(policy=None)
        await self._configure_initial_fleet()

    def _principal(
        self,
        principal_id: str,
        token: str,
        scopes: list[str],
        **bindings: object,
    ) -> dict[str, object]:
        return {
            "principal_id": principal_id,
            "token_sha256": _token_sha256(token),
            "scopes": scopes,
            "subject_id": None,
            "subject_incarnation": None,
            "demand_reporter_incarnation": None,
            "pool_id": None,
            "pool_reporter_incarnation": None,
            "executor_id": None,
            "executor_incarnation": None,
            "executor_pool_generation": None,
            **bindings,
        }

    def _principal_document(self) -> dict[str, object]:
        principals = [
            self._principal(
                "capacity-operator",
                _OPERATOR_TOKEN,
                [
                    "capacity:configure:fleet",
                    "capacity:configure:subject",
                    "capacity:configure:activate",
                    "capacity:project:development",
                    "capacity:reconcile",
                    "capacity:read",
                    "capacity:grant:manage",
                ],
            )
        ]
        for index, subject in enumerate(self.static_subjects, start=1):
            principals.append(
                self._principal(
                    f"static-{index}-reporter",
                    self._static_tokens[index],
                    ["capacity:report:demand"],
                    subject_id=str(subject.subject_id),
                    subject_incarnation=str(subject.subject_incarnation),
                    demand_reporter_incarnation=str(subject.demand_reporter_incarnation),
                )
            )
        for pool_id in _POOL_ORDER:
            pool = next(item for item in self.fleet.pools if item.pool_id == pool_id)
            principals.append(
                self._principal(
                    f"{pool_id}-pool-reporter",
                    _POOL_TOKENS[pool_id],
                    ["capacity:report:pool"],
                    pool_id=pool_id,
                    pool_reporter_incarnation=str(pool.pool_reporter_incarnation),
                )
            )
        for owner in self._owners.values():
            if not owner.active:
                continue
            principals.append(
                self._principal(
                    f"{owner.name}-demand-reporter",
                    owner.reporter_token,
                    ["capacity:report:demand"],
                    subject_id=str(owner.subject_id),
                    subject_incarnation=str(owner.subject_incarnation),
                    demand_reporter_incarnation=str(owner.reporter_incarnation),
                )
            )
        for pool_id, material in self._executor_material.items():
            binding, _key = material
            principals.append(
                self._principal(
                    f"{pool_id}-executor",
                    _EXECUTOR_TOKENS[pool_id],
                    ["capacity:execute:pool"],
                    pool_id=pool_id,
                    executor_id=binding.executor_id,
                    executor_incarnation=str(binding.executor_incarnation),
                    executor_pool_generation=binding.pool_generation,
                )
            )
        return {"schema_version": 1, "principals": principals}

    def _write_ownership_keys(self) -> None:
        keys = []
        for _pool_id, (_binding, ownership) in sorted(self._executor_material.items()):
            keys.append(
                {
                    "signing_key_id": ownership.signing_key_id,
                    "public_key_base64": base64.b64encode(
                        ownership.private_key.public_key().public_bytes(
                            encoding=serialization.Encoding.Raw,
                            format=serialization.PublicFormat.Raw,
                        )
                    ).decode("ascii"),
                }
            )
        _write_private(
            self._ownership_path,
            json.dumps({"schema_version": 1, "keys": keys}),
        )

    async def _restart_manager(
        self,
        *,
        policy: ExecutionPreparationPolicyV2 | None,
    ) -> None:
        if self._lifespan is not None:
            await self._lifespan.__aexit__(None, None, None)
            self._lifespan = None
        _write_private(self._principals_path, json.dumps(self._principal_document()))
        self._write_ownership_keys()
        settings = CapacityManagerSettings(
            principals_file=self._principals_path,
            db_url_file=self._database_path,
            expected_authority_incarnation=_AUTHORITY_ID,
            tls_cert_file=self._cert_path,
            tls_key_file=self._key_path,
            tls_client_ca_file=self._ca_path,
            ownership_public_keys_file=self._ownership_path,
            freshness_seconds=3_600,
            allocation_timeout_seconds=5,
        )
        self.management_store = CapacityManagementStore(
            freshness_seconds=3_600,
            execution_policy=policy,
        )
        keyring = OwnershipKeyring(
            {
                ownership.signing_key_id: ownership.private_key.public_key()
                for _binding, ownership in self._executor_material.values()
            }
        )
        self.execution_store = CapacityExecutionStore(
            inventory_freshness_seconds=3_600,
            ownership_keyring=keyring,
        )
        app = create_app(
            settings,
            verifier=CapacityPrincipalVerifier.from_file(self._principals_path),
            management_store=self.management_store,
            execution_store=self.execution_store,
        )
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        if not app.state.ready:
            await lifespan.__aexit__(None, None, None)
            raise RuntimeError("capacity manager failed to initialize")
        self._app = app
        self._lifespan = lifespan
        self._session_factory = app.state.session_factory
        self._transport.bind(app)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        token: str = _OPERATOR_TOKEN,
        value: Any | None = None,
        idempotency_key: UUID | None = None,
    ) -> httpx.Response:
        headers = {"Authorization": f"Bearer {token}"}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = str(idempotency_key)
        content = None
        if value is not None:
            headers["Content-Type"] = "application/json"
            content = (
                canonical_executable_bytes(value)
                if hasattr(value, "executable") and value.executable is True
                else json.dumps(
                    value.model_dump(mode="json", exclude_none=False),
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("ascii")
            )
        return await self.http.request(
            method,
            f"https://capacity.test{path}",
            headers=headers,
            content=content,
        )

    async def _configure_initial_fleet(self) -> None:
        fleet_response = await self._request(
            "PUT",
            "/v1/config-proposals/fleet",
            value=self.fleet,
            idempotency_key=_uuid("initial:fleet"),
        )
        fleet_response.raise_for_status()
        subject_responses = []
        for index, subject in enumerate(self.static_subjects, start=1):
            response = await self._request(
                "PUT",
                f"/v1/config-proposals/subjects/{subject.subject_id}",
                value=subject,
                idempotency_key=_uuid(f"initial:subject:{index}"),
            )
            response.raise_for_status()
            subject_responses.append(response.json())
        fleet_proposal = fleet_response.json()
        self._fleet_proposal_digest = str(fleet_proposal["digest"])
        self._fleet_reference = ConfigurationGenerationRefV1(
            scope="fleet",
            generation=fleet_proposal["generation"],
            digest=fleet_proposal["digest"],
        )
        self._subject_references = {
            UUID(item["subject_id"]): ConfigurationGenerationRefV1(
                scope="subject",
                generation=item["generation"],
                digest=item["digest"],
                subject_id=UUID(item["subject_id"]),
                subject_incarnation=UUID(item["subject_incarnation"]),
            )
            for item in subject_responses
        }
        activation = ConfigurationActivationV1(
            expected_configuration_epoch=0,
            fleet=self._fleet_reference,
            subjects=tuple(self._subject_references.values()),
            static_candidate_provenance=self._static_candidate_provenance(),
        )
        response = await self._request(
            "POST",
            "/v1/config-activations",
            value=activation,
            idempotency_key=_uuid("initial:activation"),
        )
        response.raise_for_status()
        self._configuration_epoch = int(response.json()["configuration_epoch"])
        for index, subject in enumerate(self.static_subjects, start=1):
            report = DemandSnapshotV1(
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
                configuration_generation=1,
                deployment_generation=1,
                reporter_incarnation=subject.demand_reporter_incarnation,
                sequence=1,
                source_observed_at=_FIXED_TIME,
                pending_unassigned=(),
                current_assignments=(),
                fixed_claims=(),
            )
            accepted = await self._request(
                "PUT",
                f"/v1/reports/demand/{subject.subject_id}",
                token=self._static_tokens[index],
                value=report,
            )
            accepted.raise_for_status()
        for pool_id in _POOL_ORDER:
            await self._publish_pool_observation(pool_id)

    async def _rotate_development_profiles(
        self,
        deployment_generation: int,
    ) -> None:
        if self._fleet_reference is None:
            raise RuntimeError("active fleet reference is unavailable")
        template = cast(
            DevelopmentSubjectTemplateV1,
            self.fleet.development_subject_template,
        )
        profiles = []
        for profile in self._base_development_profiles:
            shapes = tuple(
                shape.model_copy(
                    update={"shape_id": (f"{shape.shape_id}-deployment-{deployment_generation}")}
                )
                for shape in profile.worker_shapes
            )
            executable = profile.model_copy(
                update={
                    "profile_generation": deployment_generation,
                    "profile_digest": "0" * 64,
                    "worker_shapes": shapes,
                }
            )
            profiles.append(
                executable.model_copy(
                    update={
                        "profile_digest": canonical_digest_excluding(
                            executable,
                            "profile_digest",
                        )
                    }
                )
            )
        fleet = self.fleet.model_copy(
            update={
                "fleet_generation": self.fleet.fleet_generation + 1,
                "fleet_digest": "0" * 64,
                "development_subject_template": template.model_copy(
                    update={"profiles": tuple(profiles)}
                ),
            }
        )
        fleet = fleet.model_copy(
            update={
                "fleet_digest": canonical_digest_excluding(
                    fleet,
                    "fleet_digest",
                )
            }
        )
        proposed = await self._request(
            "PUT",
            "/v1/config-proposals/fleet",
            value=fleet,
            idempotency_key=_uuid(f"fleet:deployment-profile:{deployment_generation}"),
        )
        proposed.raise_for_status()
        proposal = proposed.json()
        reference = ConfigurationGenerationRefV1(
            scope="fleet",
            generation=proposal["generation"],
            digest=proposal["digest"],
        )
        activation = ConfigurationActivationV1(
            expected_configuration_epoch=self._configuration_epoch,
            fleet=reference,
            subjects=tuple(
                self._subject_references[subject_id]
                for subject_id in sorted(
                    self._subject_references,
                    key=lambda value: value.hex,
                )
            ),
            static_candidate_provenance=self._static_candidate_provenance(),
        )
        activated = await self._request(
            "POST",
            "/v1/config-activations",
            value=activation,
            idempotency_key=_uuid(f"fleet:deployment-profile-activation:{deployment_generation}"),
        )
        activated.raise_for_status()
        self.fleet = fleet
        self._fleet_proposal_digest = str(proposal["digest"])
        self._fleet_reference = reference
        self._configuration_epoch = int(activated.json()["configuration_epoch"])

    async def _enable_projected_owner(
        self,
        subject: SubjectConfigurationV1,
    ) -> SubjectConfigurationV1:
        if self._fleet_reference is None:
            raise RuntimeError("active fleet reference is unavailable")
        executable = subject.model_copy(
            update={
                "configuration_generation": subject.configuration_generation + 1,
                "submission_rate_per_minute": 4,
            }
        )
        proposed = await self._request(
            "PUT",
            f"/v1/config-proposals/subjects/{subject.subject_id}",
            value=executable,
            idempotency_key=_uuid(
                f"owner:{subject.subject_id}:execution-policy:{executable.configuration_generation}"
            ),
        )
        proposed.raise_for_status()
        proposal = proposed.json()
        reference = ConfigurationGenerationRefV1(
            scope="subject",
            generation=proposal["generation"],
            digest=proposal["digest"],
            subject_id=subject.subject_id,
            subject_incarnation=subject.subject_incarnation,
        )
        self._subject_references[subject.subject_id] = reference
        activation = ConfigurationActivationV1(
            expected_configuration_epoch=self._configuration_epoch,
            fleet=self._fleet_reference,
            subjects=tuple(
                self._subject_references[subject_id]
                for subject_id in sorted(
                    self._subject_references,
                    key=lambda value: value.hex,
                )
            ),
            static_candidate_provenance=self._static_candidate_provenance(),
        )
        activated = await self._request(
            "POST",
            "/v1/config-activations",
            value=activation,
            idempotency_key=_uuid(
                f"owner:{subject.subject_id}:execution-activation:"
                f"{executable.configuration_generation}"
            ),
        )
        activated.raise_for_status()
        self._configuration_epoch = int(activated.json()["configuration_epoch"])
        return executable

    def _static_candidate_provenance(self) -> tuple[StaticCandidateProvenanceV1, ...]:
        return tuple(
            StaticCandidateProvenanceV1(
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
                candidate_generation=subject.candidate_generation,
                algorithm="source-sha256",
                identity=_sha(f"static:{index}:candidate"),
                publication_sha256=_sha(f"static:{index}:publication"),
            )
            for index, subject in enumerate(self.static_subjects, start=1)
        )

    async def add_owner(self, name: str, candidate_sha256: str) -> OwnerHandle:
        if name in self._owners:
            return self._owners[name]
        database = self.protected_databases[name]
        subject_id = _uuid(f"owner:{name}:subject")
        subject_incarnation = _uuid(f"owner:{name}:incarnation")
        owner_id = _uuid(f"owner:{name}:account")
        publication = _sha(f"owner:{name}:publication:1:{candidate_sha256}")
        reporter = _uuid(f"owner:{name}:reporter:1")
        token = f"task-13-{name}-reporter-1"
        projection = DynamicDevelopmentSubjectProjectionV1(
            expected_configuration_epoch=self._configuration_epoch,
            operation_kind="create",
            operation_id=_uuid(f"owner:{name}:operation:1"),
            operation_epoch=1,
            environment_name=name,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            owner_id=owner_id,
            min_slots=0,
            max_slots=2,
            candidate_generation=1,
            candidate_sha256=candidate_sha256,
            candidate_publication_sha256=publication,
            deployment_generation=1,
            configuration_generation=1,
            demand_reporter_incarnation=reporter,
            demand_reporter_token_sha256=_token_sha256(token),
            local_activation_sha256=_sha(f"owner:{name}:activation:1"),
            protected_admission_sha256=_sha(f"owner:{name}:protected:1"),
            capacity_agent_installation_sha256=_sha(f"owner:{name}:agent:1"),
            supported_pool_ids=("gb10", "oldlab"),
            supported_architectures=("arm64", "x86_64"),
            protocol_versions={
                "capacity-agent": "v1",
                "claim-guard": "v1",
                "control-plane-worker": "v1",
            },
        )
        response = await self._request(
            "PUT",
            f"/v1/development-projections/{subject_id}",
            value=projection,
            idempotency_key=_uuid(f"owner:{name}:projection-key:1"),
        )
        response.raise_for_status()
        projected = response.json()
        self._configuration_epoch = int(projected["configuration_epoch"])
        projected_subject = SubjectConfigurationV1.model_validate_json(
            json.dumps(projected["subject"])
        )
        self._subject_references[subject_id] = ConfigurationGenerationRefV1(
            scope="subject",
            generation=projected_subject.configuration_generation,
            digest=canonical_digest(projected_subject),
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
        )
        executable_subject = await self._enable_projected_owner(projected_subject)
        registration = AgentRegistrationV1(
            environment_id=f"dev-{name}",
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            authority_incarnation=_AUTHORITY_ID,
            agent_incarnation=_uuid(f"owner:{name}:agent-incarnation:1"),
            reporter_incarnation=reporter,
            candidate_digest=publication,
            candidate_identity_algorithm="source-sha256",
            candidate_identity=candidate_sha256,
            candidate_publication_sha256=publication,
            deployment_generation=1,
            configuration_generation=executable_subject.configuration_generation,
        )
        owner = OwnerHandle(
            harness=self,
            name=name,
            database=database,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            owner_id=owner_id,
            candidate_sha256=candidate_sha256,
            candidate_publication_sha256=publication,
            candidate_generation=1,
            deployment_generation=1,
            configuration_generation=executable_subject.configuration_generation,
            reporter_incarnation=reporter,
            reporter_token=token,
            registration=registration,
            projection=projection,
        )
        await self._initialize_protected_owner(owner)
        self._owners[name] = owner
        self._owners_by_subject[subject_id] = owner
        return owner

    @asynccontextmanager
    async def _owner_stores(
        self,
        owner: OwnerHandle,
    ) -> AsyncIterator[tuple[CapacityAgentStore, CapacityGuardStore, AsyncSession]]:
        engine = create_async_engine(
            make_url(_database_value(owner.database, "migrator_url")),
            isolation_level="SERIALIZABLE",
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        owner_role = _database_value(owner.database, "owner_role")
        quoted = engine.sync_engine.dialect.identifier_preparer.quote(owner_role)
        try:
            async with factory() as session, session.begin():
                await session.execute(text(f"SET LOCAL ROLE {quoted}"))
                yield (
                    CapacityAgentStore(
                        session,
                        expected_owner_role=owner_role,
                        expected_agent_role=_database_value(owner.database, "agent_role"),
                    ),
                    CapacityGuardStore(session, expected_owner_role=owner_role),
                    session,
                )
        finally:
            await engine.dispose()

    async def _initialize_protected_owner(self, owner: OwnerHandle) -> None:
        fence = GuardFenceV1(
            environment_id=f"dev-{owner.name}",
            subject_id=owner.subject_id,
            subject_incarnation=owner.subject_incarnation,
            authority_incarnation=_AUTHORITY_ID,
            reporter_incarnation=owner.reporter_incarnation,
            deployment_generation=owner.deployment_generation,
            configuration_generation=owner.configuration_generation,
            candidate_digest=owner.candidate_publication_sha256,
        )
        async with self._owner_stores(owner) as (agent_store, guard_store, _session):
            await guard_store.initialize_disabled_authority(fence)
            await agent_store.register_agent(owner.registration)

    async def _publish_owner_demand(
        self,
        owner: OwnerHandle,
        kind: Literal["x86", "arm", "neutral", "zero"],
        slots: int,
    ) -> None:
        if slots < 0 or slots > 2:
            raise ValueError("fixture owner demand must be between zero and two slots")
        if slots:
            await self._ensure_attempts(owner, kind, slots)
        owner.demand_sequence += 1
        owner.demand_kind = kind
        owner.demand_count = slots
        pending: tuple[DemandBucketV1, ...] = ()
        if slots:
            eligible = (
                ("oldlab",) if kind == "x86" else ("gb10",) if kind == "arm" else ("gb10", "oldlab")
            )
            pending = (
                DemandBucketV1(
                    bucket_id=f"{kind}-work",
                    requested_slots=slots,
                    local_priority=0,
                    oldest_submitted_at=_FIXED_TIME,
                    eligible_pool_ids=eligible,
                    required_capabilities=("cpu",),
                    attempt_ids=tuple(
                        str(attempt.protected_attempt_id) for attempt in owner.attempts[:slots]
                    ),
                ),
            )
        snapshot = DemandSnapshotV1(
            subject_id=owner.subject_id,
            subject_incarnation=owner.subject_incarnation,
            configuration_generation=owner.configuration_generation,
            deployment_generation=owner.deployment_generation,
            reporter_incarnation=owner.reporter_incarnation,
            sequence=owner.demand_sequence,
            source_observed_at=_FIXED_TIME,
            pending_unassigned=pending,
            current_assignments=(),
            fixed_claims=(),
        )
        response = await self._request(
            "PUT",
            f"/v1/reports/demand/{owner.subject_id}",
            token=owner.reporter_token,
            value=snapshot,
        )
        response.raise_for_status()

    async def _ensure_attempts(
        self,
        owner: OwnerHandle,
        kind: Literal["x86", "arm", "neutral", "zero"],
        count: int,
    ) -> None:
        architecture = "x86_64" if kind == "x86" else "arm64" if kind == "arm" else "any"
        required_pool = "oldlab" if kind == "x86" else "gb10" if kind == "arm" else None
        requirements = SealedRequirementsV1(
            os="linux",
            cpu_arch=cast(Any, architecture),
            gpu_vendor="none",
            network_policies=("public",),
            required_pool=cast(Any, required_pool),
        )
        while len(owner.attempts) < count:
            index = len(owner.attempts) + 1
            trial_id = _uuid(
                f"owner:{owner.name}:deployment:{owner.deployment_generation}:trial:{index}"
            )
            attempt_id = _uuid(
                f"owner:{owner.name}:deployment:{owner.deployment_generation}:attempt:{index}"
            )
            self._seed_trial(owner, trial_id, architecture, index)
            attempt = ProtectedAttemptV1(
                trial_id=trial_id,
                protected_attempt_id=attempt_id,
                execution_generation=owner.deployment_generation,
                requirements_digest=canonical_guard_digest(requirements),
            )
            async with self._owner_stores(owner) as (_agent, guard, _session):
                await guard.register_trial_attempt(attempt, requirements)
            owner.attempts.append(
                _Attempt(
                    protected_attempt_id=attempt_id,
                    execution_generation=owner.deployment_generation,
                    requirements_digest=attempt.requirements_digest,
                )
            )

    def _seed_trial(
        self,
        owner: OwnerHandle,
        trial_id: UUID,
        architecture: str,
        index: int,
    ) -> None:
        engine = create_engine(_database_value(owner.database, "admin_url"))
        team_id = _uuid(f"owner:{owner.name}:team:{owner.deployment_generation}:{index}")
        task_id = f"task-13-{owner.name}-{owner.deployment_generation}-{index}"
        try:
            with engine.begin() as connection:
                connection.execute(
                    insert(Team).values(
                        id=team_id,
                        name=(f"team-{owner.name}-{owner.deployment_generation}-{index}"),
                    )
                )
                connection.execute(insert(TeamQuota).values(team_id=team_id))
                connection.execute(
                    insert(Task).values(
                        id=task_id,
                        checksum=_sha(task_id),
                        config={"schema_version": "1"},
                    )
                )
                connection.execute(
                    insert(Trial).values(
                        id=trial_id,
                        team_id=team_id,
                        task_id=task_id,
                        config={},
                        requires_caps={
                            "os": "linux",
                            "cpu_arch": architecture,
                            "gpu_vendor": "none",
                            "network_policies": ["public"],
                        },
                        state="queued",
                        submit_priority=100,
                    )
                )
        finally:
            engine.dispose()

    async def _publish_pool_observation(self, pool_id: str) -> None:
        pool = next(item for item in self.fleet.pools if item.pool_id == pool_id)
        self._pool_sequences[pool_id] += 1
        observation = PoolObservationV1(
            pool_id=pool_id,
            pool_generation=pool.pool_generation,
            reporter_incarnation=pool.pool_reporter_incarnation,
            sequence=self._pool_sequences[pool_id],
            source_observed_at=_FIXED_TIME,
            health="eligible",
            commitments=(),
        )
        response = await self._request(
            "PUT",
            f"/v1/reports/pools/{pool_id}",
            token=_POOL_TOKENS[pool_id],
            value=observation,
        )
        response.raise_for_status()

    def admission_directory(self, pool_id: str) -> Path:
        return self._admission_root / f"epoch-{self._epoch_number}" / pool_id

    def approved_profiles(self, pool_id: str) -> tuple[OperatorLaunchProfileV2, ...]:
        variants = self.profile_variants[pool_id]
        return tuple(variants[slots] for slots in sorted(variants))

    def _record_worker_registration(self, request: ExecutableWorkerRegistrationV2) -> None:
        self._worker_registrations[request.binding.intent_id] = request

    def _worker_registration(self, intent_id: UUID) -> ExecutableWorkerRegistrationV2:
        try:
            return self._worker_registrations[intent_id]
        except KeyError as exc:
            raise RuntimeError("protected worker registration was not recorded") from exc

    def _routed_admission_client(
        self,
        database_url: bytes,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
    ) -> _HarnessAdmissionClient:
        return _HarnessAdmissionClient(
            self,
            database_url,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
        )

    def routed_admission_factory(
        self,
        directory: Path,
        *,
        expected_directory_sha256: str,
    ) -> RoutedExecutableAdmissionClient:
        return RoutedExecutableAdmissionClient(
            directory,
            expected_directory_sha256=expected_directory_sha256,
            client_factory=self._routed_admission_client,
        )

    def _admission_entries(self, pool_id: str) -> tuple[AdmissionBindingEntryV2, ...]:
        entries = []
        url_directory = self._db_url_root / f"epoch-{self._epoch_number}" / pool_id
        url_directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        url_directory.chmod(0o700)
        for owner in sorted(self._owners.values(), key=lambda item: item.subject_id.hex):
            if not owner.active:
                continue
            database_url = _database_value(owner.database, "executor_url").encode("utf-8")
            url_file = _write_private_bytes(
                url_directory / f"{owner.subject_id.hex}-{owner.subject_incarnation.hex}.url",
                database_url,
            )
            entries.append(
                AdmissionBindingEntryV2(
                    subject_id=owner.subject_id,
                    subject_incarnation=owner.subject_incarnation,
                    configuration_generation=self._configuration_epoch,
                    deployment_generation=owner.deployment_generation,
                    candidate_generation=owner.candidate_generation,
                    protected_admission_sha256=owner.projection.protected_admission_sha256,
                    database_url_file=str(url_file),
                    database_url_sha256=hashlib.sha256(database_url).hexdigest(),
                    environment_name=f"loom-dev-{owner.name}",
                )
            )
        return tuple(entries)

    def _publish_epoch_runtime_files(self) -> None:
        for pool_id in _POOL_ORDER:
            admission_directory = self.admission_directory(pool_id)
            admission_directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            admission_directory.chmod(0o700)
            write_admission_binding_directory(
                admission_directory,
                self._admission_entries(pool_id),
            )
            admission_digest = canonical_admission_directory_digest(admission_directory)
            candidate_path = Path("/usr/bin/true")
            candidate_metadata = candidate_path.stat()
            config = TrustedLauncherConfigV2(
                handoff_directory=str(self.pools[pool_id].handoff_directory),
                admission_directory=str(admission_directory),
                admission_directory_sha256=admission_digest,
                candidate_executable={
                    "path": str(candidate_path),
                    "sha256": hashlib.sha256(candidate_path.read_bytes()).hexdigest(),
                    "owner_uid": candidate_metadata.st_uid,
                    "mode": candidate_metadata.st_mode & 0o777,
                },
                candidate_image_digest=self.profiles[pool_id].image_digest,
                candidate_argv=("/usr/bin/true",),
            )
            payload = canonical_executable_bytes(config)
            config_path = _write_private_bytes(
                self._trusted_launcher_root / f"epoch-{self._epoch_number}" / f"{pool_id}.json",
                payload,
            )
            config_identity = SlurmFileIdentityV2(
                path=str(config_path),
                sha256=hashlib.sha256(payload).hexdigest(),
                owner_uid=os.geteuid(),
            )
            self.profile_variants[pool_id] = {
                slots: (
                    updated := profile.model_copy(
                        update={
                            "trusted_launcher_config": config_identity,
                            "controller_authority_sha256": "0" * 64,
                        }
                    )
                ).model_copy(
                    update={"controller_authority_sha256": canonical_launch_policy_digest(updated)}
                )
                for slots, profile in self.profile_variants[pool_id].items()
            }

    def _select_execution_profiles(self) -> None:
        for pool_id in _POOL_ORDER:
            template = cast(
                DevelopmentSubjectTemplateV1,
                self.fleet.development_subject_template,
            )
            reference = next(item for item in template.profiles if item.pool_id == pool_id)
            refreshed_variants: dict[int, OperatorLaunchProfileV2] = {}
            for shape in reference.worker_shapes:
                profile = self.profile_variants[pool_id][shape.concurrency_slots]
                refreshed = profile.model_copy(
                    update={
                        "profile_id": shape.shape_id,
                        "profile_generation": reference.profile_generation,
                        "profile_digest": reference.profile_digest,
                        "shape_id": shape.shape_id,
                        "controller_authority_sha256": "0" * 64,
                    }
                )
                refreshed_variants[shape.concurrency_slots] = refreshed.model_copy(
                    update={
                        "controller_authority_sha256": canonical_launch_policy_digest(refreshed)
                    }
                )
            self.profile_variants[pool_id] = refreshed_variants
            demanding = tuple(
                owner
                for owner in self._owners.values()
                if owner.active
                and owner.demand_count > 0
                and (
                    owner.demand_kind == "neutral"
                    or (owner.demand_kind == "x86" and pool_id == "oldlab")
                    or (owner.demand_kind == "arm" and pool_id == "gb10")
                )
            )
            slots = max((owner.demand_count for owner in demanding), default=1)
            self.profiles[pool_id] = self.profile_variants[pool_id][slots]
            self.pools[pool_id].profile = self.profiles[pool_id]

    def _new_executor_material(self) -> None:
        self._executor_material = {}
        for pool_id in _POOL_ORDER:
            private = Ed25519PrivateKey.from_private_bytes(
                bytes.fromhex(_sha(f"{pool_id}:ownership:{self._epoch_number}"))
            )
            key = ExecutorOwnershipKey(
                signing_key_id=f"{pool_id}-key-{self._epoch_number}",
                private_key=private,
                public_key_sha256=public_key_fingerprint(private.public_key()),
            )
            binding = PreparedExecutorBindingV2(
                pool_id=cast(Any, pool_id),
                pool_generation=1,
                executor_id=f"{pool_id}-executor-{self._epoch_number}",
                executor_incarnation=_uuid(f"{pool_id}:executor-incarnation:{self._epoch_number}"),
                signing_key_sha256=key.public_key_sha256,
                local_authority_sha256=_sha(f"{pool_id}:local-authority:{self._epoch_number}"),
                controller_authority_sha256=self.profiles[pool_id].controller_authority_sha256,
            )
            self._executor_material[pool_id] = (binding, key)

    def _subject_acknowledgements(self) -> tuple[SubjectExecutionAcknowledgementV2, ...]:
        acknowledgements = []
        for index, subject in enumerate(self.static_subjects, start=1):
            identity = _sha(f"static:{index}:candidate")
            publication = _sha(f"static:{index}:publication")
            acknowledgements.append(
                SubjectExecutionAcknowledgementV2(
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    configuration_generation=subject.configuration_generation,
                    deployment_generation=subject.deployment_generation,
                    candidate=CandidateBindingV2(
                        algorithm="source-sha256",
                        identity=identity,
                        publication_sha256=publication,
                    ),
                    reporter_incarnation=subject.demand_reporter_incarnation,
                    protected_admission_sha256=_sha(f"static:{index}:protected"),
                    legacy_writer_high_water=0,
                    acknowledgement_sha256=_sha(f"static:{index}:acknowledgement"),
                )
            )
        for owner in self._owners.values():
            if not owner.active:
                continue
            acknowledgements.append(
                SubjectExecutionAcknowledgementV2(
                    subject_id=owner.subject_id,
                    subject_incarnation=owner.subject_incarnation,
                    configuration_generation=owner.configuration_generation,
                    deployment_generation=owner.deployment_generation,
                    candidate=owner.candidate,
                    reporter_incarnation=owner.reporter_incarnation,
                    protected_admission_sha256=owner.projection.protected_admission_sha256,
                    legacy_writer_high_water=0,
                    acknowledgement_sha256=_sha(
                        f"owner:{owner.name}:ack:{owner.configuration_generation}"
                    ),
                )
            )
        return tuple(acknowledgements)

    def _legacy_fences(self) -> tuple[LegacyWriterFenceV2, ...]:
        return (
            LegacyWriterFenceV2(
                writer_id="global-dev-supervisor",
                writer_kind="allocation",
                scope_kind="global",
                scope_id="development",
                high_water=0,
                freeze_evidence_sha256=_sha("legacy-writer-freeze"),
                state="frozen",
            ),
        )

    def _execution_policy(self) -> ExecutionPreparationPolicyV2:
        return ExecutionPreparationPolicyV2(
            trusted_fleet_release_sha256=_TRUSTED_RELEASE,
            executable_new_capacity_ceiling=4,
            executable_new_capacity_rate_per_minute=4,
            executors=tuple(self._executor_material[pool_id][0] for pool_id in _POOL_ORDER),
            subject_acknowledgements=self._subject_acknowledgements(),
            rollback_evidence_sha256=_sha(f"rollback:{self._epoch_number}"),
            controller_authorities=tuple(
                PoolControllerAuthorityV2(
                    pool_id=cast(Any, pool_id),
                    controller_authority_sha256=self.profiles[pool_id].controller_authority_sha256,
                )
                for pool_id in _POOL_ORDER
            ),
            legacy_writer_fences=self._legacy_fences(),
        )

    async def _activate_epoch(self) -> None:
        for pool in self.pools.values():
            pool.close()
        self._epoch_number += 1
        self._publish_epoch_runtime_files()
        self._select_execution_profiles()
        self._new_executor_material()
        policy = self._execution_policy()
        await self._restart_manager(policy=policy)
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        writer = self._app.state.writer
        request = ExecutionPreparationV2(
            authority_incarnation=_AUTHORITY_ID,
            expected_writer_epoch=writer.writer_epoch,
            configuration_epoch=self._configuration_epoch,
            fleet_generation=self.fleet.fleet_generation,
            fleet_digest=self._fleet_proposal_digest,
            trusted_fleet_release_sha256=_TRUSTED_RELEASE,
            requested_ceiling=4,
            requested_rate_per_minute=4,
            executors=tuple(self._executor_material[pool_id][0] for pool_id in _POOL_ORDER),
            subject_acknowledgements=policy.subject_acknowledgements,
            legacy_writer_fences=policy.legacy_writer_fences,
            rollback_evidence_sha256=policy.rollback_evidence_sha256,
        )
        async with self._session_factory() as session:
            prepared = await self.management_store.prepare_execution_epoch(
                session,
                request,
                actor="activation-operator",
                idempotency_key=_uuid(f"epoch:{self._epoch_number}:prepare"),
            )
            for pool_id in _POOL_ORDER:
                binding, key = self._executor_material[pool_id]
                await self.management_store.register_execution_executor(
                    session,
                    ExecutableExecutorRegistrationV2(
                        execution=prepared,
                        executor_id=binding.executor_id,
                        executor_incarnation=binding.executor_incarnation,
                        pool_id=binding.pool_id,
                        pool_generation=binding.pool_generation,
                        signing_key_id=key.signing_key_id,
                        signing_key_sha256=key.public_key_sha256,
                        local_authority_sha256=binding.local_authority_sha256,
                        controller_authority_sha256=binding.controller_authority_sha256,
                    ),
                    actor="executor-installer",
                    idempotency_key=_uuid(f"epoch:{self._epoch_number}:register:{pool_id}"),
                )
            active = await self.management_store.activate_execution_epoch(
                session,
                ExecutionActivationV2(
                    authority_incarnation=_AUTHORITY_ID,
                    expected_writer_epoch=prepared.writer_epoch,
                    execution_epoch=prepared.execution_epoch,
                    execution_manifest_sha256=prepared.execution_manifest_sha256,
                    executable_new_capacity_ceiling=4,
                    executable_new_capacity_rate_per_minute=4,
                ),
                actor="activation-operator",
                idempotency_key=_uuid(f"epoch:{self._epoch_number}:activate"),
            )
        self._execution = active
        self._drained = None
        self._workers = {
            intent_id: worker
            for intent_id, worker in self._workers.items()
            if worker.binding.execution.execution_epoch == active.execution_epoch
        }
        self._claims = []
        for pool_id in _POOL_ORDER:
            binding, key = self._executor_material[pool_id]
            registration = ExecutableExecutorRegistrationV2(
                execution=active,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                signing_key_id=key.signing_key_id,
                signing_key_sha256=key.public_key_sha256,
                local_authority_sha256=binding.local_authority_sha256,
                controller_authority_sha256=binding.controller_authority_sha256,
            )
            pool = self.pools[pool_id]
            await pool.install(
                registration,
                key,
                self.root / "journals" / f"epoch-{self._epoch_number}" / f"{pool_id}.journal",
            )
            await pool.heartbeat()
            result = await pool.tick()
            if result.status != "inventory-published":
                raise RuntimeError("initial executor inventory was not published")
            if pool.journal is None:
                raise RuntimeError("pool journal is unavailable")
            latest = pool.journal.latest("inventory", str(registration.executor_incarnation))
            if latest is not None:
                payload = latest.durable_payload()
                if payload is not None:
                    pool.last_inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
            await self._publish_pool_observation(pool_id)

    async def reconcile(self) -> None:
        response = await self._request("POST", "/v1/shadow-reconciliations")
        response.raise_for_status()
        if response.json()["status"] != "committed":
            raise RuntimeError(f"capacity reconciliation failed: {response.text}")

    async def converge(self) -> None:
        if self._execution is None or self.execution_state == "shadow":
            await self._activate_epoch()
        await self.reconcile()
        ranks = await self._latest_launch_ranks()
        for rank in ranks:
            pool_id = str(rank["pool_id"])
            await self.drive_pool(pool_id)
        for pool_id in _POOL_ORDER:
            pool = self.pools[pool_id]
            if self._execution is None or pool.journal is None:
                raise RuntimeError("active pool runtime is unavailable")
            await pool.heartbeat()
            result = await pool.tick()
            if result.status != "inventory-published":
                raise RuntimeError(f"pool {pool_id} retained executable work")
            latest = pool.journal.latest(
                "inventory",
                str(cast(ExecutableExecutorRegistrationV2, pool.registration).executor_incarnation),
            )
            if latest is not None:
                payload = latest.durable_payload()
                if payload is not None:
                    pool.last_inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
            await self._publish_pool_observation(pool_id)

    async def executable_executor_status(self) -> dict[str, Any]:
        """Read the manager's bounded executable executor projection."""

        response = await self._request("GET", "/v2/status/executors")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("executable executor status is invalid")
        return payload

    async def executable_subject_status(self, subject_id: UUID) -> dict[str, Any]:
        """Read the manager's bounded executable subject projection."""

        response = await self._request("GET", f"/v2/status/subjects/{subject_id}")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("executable subject status is invalid")
        return payload

    async def _latest_launch_ranks(self) -> tuple[dict[str, Any], ...]:
        if self._session_factory is None or self._execution is None:
            return ()
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(CapacityAllocationEpoch)
                    .where(
                        CapacityAllocationEpoch.execution_epoch == self._execution.execution_epoch,
                        CapacityAllocationEpoch.status == "executable",
                    )
                    .order_by(CapacityAllocationEpoch.allocation_epoch.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
        if row is None:
            return ()
        return tuple(dict(item) for item in row.complete_payload["hypothetical_launch_rank"])

    def pool_runtime_entry_components(self, pool_id: str) -> dict[str, object]:
        return self.pools[pool_id].runtime_entry_components()

    def trusted_launcher_process_entry(
        self,
        pool_id: str,
        intent_id: UUID,
    ) -> dict[str, bool]:
        entry = self._trusted_process_entries[intent_id]
        worker = self._workers[intent_id]
        if worker.binding.pool_id != pool_id:
            raise RuntimeError("trusted process evidence differs from pool binding")
        return {
            "process_argv_matches_submitted_slurm_argv": (
                entry.process_argv == entry.submitted_launcher_argv
            ),
            "candidate_exec_received_worker_credential": (
                entry.worker_credential == worker.credential
            ),
        }

    async def prepare_unused_intent(self, pool_id: str) -> ExecutableIntentBindingV2:
        pool = self.pools[pool_id]
        binding: ExecutableIntentBindingV2 | None = None
        for _ in range(4):
            accepted = await pool.tick()
            if accepted.status == "accepted" and pool.client is not None:
                checkpoint = await pool.client.executable_checkpoint()
                work = await pool.client.next_executable_work(checkpoint.command_sequence)
                if not isinstance(work, ExecutableIntentBindingV2):
                    raise RuntimeError("accepted reservation did not expose an intent binding")
                binding = work
                break
            if accepted.status not in {"inventory-published", "idle"}:
                raise RuntimeError(f"next reservation was not accepted: {accepted}")
        if binding is None:
            raise RuntimeError("pool did not expose an accepted reservation")
        prepared = await self.pools[pool_id].tick()
        if prepared.status != "bootstrap-registered" or prepared.operation_id != binding.intent_id:
            raise RuntimeError(f"intent was not protected-prepared: {prepared}")
        return binding

    async def submit_unregistered_intent(
        self,
        pool_id: str,
    ) -> tuple[ExecutableIntentBindingV2, str]:
        binding = await self.prepare_unused_intent(pool_id)
        submitted = await self.pools[pool_id].tick()
        if submitted.status != "submitted" or submitted.operation_id != binding.intent_id:
            raise RuntimeError(f"intent was not submitted without worker registration: {submitted}")
        await self.pools[pool_id].heartbeat()
        inventory = await self.pools[pool_id].tick()
        if inventory.status != "inventory-published":
            raise RuntimeError(f"submitted intent inventory was not published: {inventory}")
        return binding, submitted.detail

    def bootstrap_handoff_path(
        self,
        pool_id: str,
        binding: ExecutableIntentBindingV2,
    ) -> Path:
        pool = self.pools[pool_id]
        return pool.handoff_directory / pool.handoff_store.reference_for(binding)

    def worker_registered(self, intent_id: UUID) -> bool:
        return intent_id in self._worker_registrations

    def trusted_launcher_process_count(self) -> int:
        return len(self._trusted_process_entries)

    async def _manager_intent_row(self, intent_id: UUID) -> CapacityExecutableIntent:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(CapacityExecutableIntent).where(
                        CapacityExecutableIntent.intent_id == intent_id
                    )
                )
            ).scalar_one()
        return row

    async def _pending_protected_release(
        self,
        owner: OwnerHandle,
    ) -> PublishableExecutableProtectedReleaseV2:
        engine = create_async_engine(
            make_url(_database_value(owner.database, "agent_url")),
            isolation_level="SERIALIZABLE",
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session, session.begin():
                publication = await read_next_executable_protected_release(
                    session,
                    registration=owner.registration,
                )
        finally:
            await engine.dispose()
        if publication is None:
            raise RuntimeError("protected release outbox is empty")
        return publication

    async def _guard_publication_count(self, owner: OwnerHandle) -> int:
        async with self._owner_stores(owner) as (_agent, _guard, session):
            return int(
                (
                    await session.execute(
                        text(
                            "SELECT count(*) FROM "
                            "loom_capacity_guard.executable_release_publication_events "
                            "WHERE agent_incarnation = :agent_incarnation"
                        ),
                        {"agent_incarnation": owner.registration.agent_incarnation},
                    )
                ).scalar_one()
            )

    async def drain_prepared_unused(
        self,
        pool_id: str,
        binding: ExecutableIntentBindingV2,
    ) -> ProtectedDrainEvidence:
        result = await self.pools[pool_id].tick()
        if result.status != "draining" or result.operation_id != binding.intent_id:
            raise RuntimeError(f"prepared intent did not drain through revocation: {result}")
        owner = self._owners_by_subject[binding.subject_id]
        publication = await self._pending_protected_release(owner)
        row = await self._manager_intent_row(binding.intent_id)
        return ProtectedDrainEvidence(
            intent_id=binding.intent_id,
            event_kind=publication.event_kind,
            executor_status=result.status,
            manager_state=row.state,
            terminal_kind=row.terminal_kind,
        )

    async def drain_unregistered_withdrawn(
        self,
        pool_id: str,
        binding: ExecutableIntentBindingV2,
        job_id: str,
    ) -> ProtectedDrainEvidence:
        result = await self.pools[pool_id].tick()
        if result.status != "pending-cancelled" or result.operation_id != binding.intent_id:
            raise RuntimeError(f"unregistered intent did not cancel pending job: {result}")
        self.pools[pool_id].fake.terminalize_job(job_id)
        await self.pools[pool_id].heartbeat()
        terminal_inventory = await self.pools[pool_id].tick()
        if terminal_inventory.status != "inventory-published":
            raise RuntimeError(
                f"terminal accounting inventory was not published: {terminal_inventory}"
            )
        terminal_close = await self.pools[pool_id].tick()
        if terminal_close.status != "draining" or terminal_close.operation_id != binding.intent_id:
            raise RuntimeError(f"terminal intent did not close centrally: {terminal_close}")
        owner = self._owners_by_subject[binding.subject_id]
        publication = await self._pending_protected_release(owner)
        row = await self._manager_intent_row(binding.intent_id)
        return ProtectedDrainEvidence(
            intent_id=binding.intent_id,
            event_kind=publication.event_kind,
            executor_status=result.status,
            manager_state=row.state,
            terminal_kind=row.terminal_kind,
        )

    def _reporter_configuration(self, owner: OwnerHandle) -> ReporterConfigurationV1:
        capabilities = {
            "oldlab": ("oldlab-x86-none", "x86_64"),
            "gb10": ("gb10-arm-none", "arm64"),
        }
        return ReporterConfigurationV1(
            **owner.registration.model_dump(mode="python"),
            pool_capabilities=tuple(
                AgentPoolCapabilityV1(
                    capability_id=capabilities[pool_id][0],
                    pool_id=cast(Any, pool_id),
                    operating_system="linux",
                    cpu_architecture=cast(Any, capabilities[pool_id][1]),
                    gpu_vendor="none",
                    network_policies=("public",),
                )
                for pool_id in _POOL_ORDER
            ),
        )

    async def _run_release_reporter_once(
        self,
        owner: OwnerHandle,
        publisher: _RecordingExecutableReleasePublisher,
    ) -> None:
        engine = create_async_engine(
            make_url(_database_value(owner.database, "agent_url")),
            isolation_level="SERIALIZABLE",
        )
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            runtime = ExecutableProtectedReleaseReporterRuntime(
                configuration=self._reporter_configuration(owner),
                session_factory=factory,
                publisher=publisher,
            )
            await runtime.initialize()
            await runtime.run_once()
        finally:
            await engine.dispose()

    async def publish_next_protected_release_with_replay(
        self,
        owner: OwnerHandle,
        *,
        manager_outage_before_first_publish: bool = False,
        lose_response_after_manager_ack: bool = False,
    ) -> ProtectedReleaseReplayEvidence:
        inner = DemandReporterClient(
            self._reporter_configuration(owner),
            manager_origin="https://capacity.test",
            bearer_token=owner.reporter_token,
            http_client=self.http,
        )
        publisher = _RecordingExecutableReleasePublisher(
            inner,
            lose_response_after_manager_ack=lose_response_after_manager_ack,
        )
        if manager_outage_before_first_publish:
            self._transport.online = False
            try:
                await self._run_release_reporter_once(owner, publisher)
            except DemandPublishError as exc:
                if "transport failed" not in str(exc):
                    raise
            finally:
                self._transport.online = True
        if lose_response_after_manager_ack:
            try:
                await self._run_release_reporter_once(owner, publisher)
            except RuntimeError as exc:
                if "response loss" not in str(exc):
                    raise
        await self._run_release_reporter_once(owner, publisher)
        if not publisher.calls:
            raise RuntimeError("protected release reporter did not publish")
        first_publication = publisher.calls[0][0]
        return ProtectedReleaseReplayEvidence(
            intent_id=first_publication.release.binding.intent_id,
            event_kind=first_publication.event_kind,
            release_digest=first_publication.publication_digest,
            publish_attempts=len(publisher.calls),
            idempotency_keys=tuple(item[1] for item in publisher.calls),
            manager_replayed_flags=tuple(publisher.replayed),
            guard_publication_count=await self._guard_publication_count(owner),
        )

    async def release_retired_intent(
        self,
        pool_id: str,
        binding: ExecutableIntentBindingV2,
    ) -> ExecutorTickResult:
        pool = self.pools[pool_id]
        result = await pool.tick()
        if result.status != "released":
            raise RuntimeError(f"intent did not release centrally: {result}")
        if await self.manager_commitments() == ():
            for pool in self.pools.values():
                await pool.heartbeat()
                final_inventory = await pool.tick()
                if final_inventory.status != "inventory-published":
                    raise RuntimeError(
                        f"pool {pool.pool_id} final inventory was not published: {final_inventory}"
                    )
                await pool.heartbeat()
                if pool.journal is None or pool.registration is None:
                    raise RuntimeError("pool executor runtime is unavailable")
                latest = pool.journal.latest(
                    "inventory",
                    str(pool.registration.executor_incarnation),
                )
                if latest is not None and (payload := latest.durable_payload()) is not None:
                    pool.last_inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
            await self._retire_exact()
        return result

    async def retirement_is_blocked(self) -> bool:
        try:
            await self.retire()
        except ExecutionConflictError:
            return True
        return False

    async def drive_pool(self, pool_id: str) -> ExecutorTickResult:
        pool = self.pools[pool_id]
        for _ in range(8):
            result = await pool.tick()
            if result.status == "submitted":
                if result.operation_id is None:
                    raise RuntimeError("submitted operation omitted its intent")
                await self._trusted_register(pool_id, result.operation_id, result.detail)
                pool.fake.set_job_state(result.detail, "RUNNING")
                return result
            if result.status not in {
                "accepted",
                "bootstrap-registered",
                "permit-consumed",
            }:
                return result
        raise RuntimeError(f"pool {pool_id} did not reach scheduler submission")

    async def _binding(self, intent_id: UUID) -> ExecutableIntentBindingV2:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        async with self._session_factory() as session:
            payload, launch_rank = (
                await session.execute(
                    select(
                        CapacityExecutableIntent.binding_payload,
                        CapacityExecutableIntent.launch_rank,
                    ).where(CapacityExecutableIntent.intent_id == intent_id)
                )
            ).one()
        self._intent_launch_ranks[intent_id] = launch_rank
        return ExecutableIntentBindingV2.model_validate_json(json.dumps(payload))

    def _physical_binding(self, pool_id: str, intent_id: UUID) -> PhysicalJobBindingV2:
        pool = self.pools[pool_id]
        if pool.journal is None:
            raise RuntimeError("pool journal is unavailable")
        retained = pool.journal.latest("intent", str(intent_id))
        if retained is None or retained.event_kind != "physical-bind-confirmed":
            raise RuntimeError("physical binding was not journaled before wrapper exchange")
        payload = retained.durable_payload()
        if payload is None:
            raise RuntimeError("physical binding payload is absent")
        return PhysicalJobBindingV2.model_validate_json(payload)

    def _launch_request(self, pool_id: str, intent_id: UUID) -> SlurmLaunchRequestV2:
        pool = self.pools[pool_id]
        if pool.journal is None:
            raise RuntimeError("pool journal is unavailable")
        retained = pool.journal.latest("job", str(intent_id))
        if retained is None:
            raise RuntimeError("submitted launch request was not journaled")
        payload = retained.durable_payload()
        if payload is None:
            raise RuntimeError("submitted launch request payload is absent")
        value = json.loads(payload.decode("ascii"))
        return SlurmLaunchRequestV2.model_validate_json(json.dumps(value["request"]))

    async def _trusted_register(self, pool_id: str, intent_id: UUID, job_id: str) -> _Worker:
        binding = await self._binding(intent_id)
        physical = self._physical_binding(pool_id, intent_id)
        if physical.binding != binding or physical.slurm_job_id != job_id:
            raise RuntimeError("physical binding differs from submitted operation")
        request = self._launch_request(pool_id, intent_id)
        process_argv = request.trusted_launcher_argv()
        submitted_job = self.pools[pool_id].fake.job_snapshot(job_id)
        batch_script = str(submitted_job.get("batch_script", ""))
        submitted_argv = process_argv if repr(process_argv) in batch_script else ()
        captured: dict[str, str | None] = {"credential": None}

        class CandidateExecBoundaryError(Exception):
            pass

        def capture_execvpe(
            _file: str,
            _argv: tuple[str, ...],
            environment: Mapping[str, str],
        ) -> None:
            captured["credential"] = environment.get(WORKER_CREDENTIAL_ENV)
            raise CandidateExecBoundaryError

        try:
            await run_trusted_launcher_process(
                process_argv,
                environment={"SLURM_JOB_ID": job_id},
                now=lambda: _FIXED_TIME,
                admission_factory=self.routed_admission_factory,
                execvpe=cast(Any, capture_execvpe),
            )
        except CandidateExecBoundaryError:
            pass
        credential = captured["credential"]
        if not isinstance(credential, str) or not credential:
            raise RuntimeError("trusted launcher did not expose a worker credential")
        registration = self._worker_registration(binding.intent_id)
        self._trusted_process_entries[intent_id] = _TrustedProcessEntry(
            process_argv=process_argv,
            submitted_launcher_argv=submitted_argv,
            worker_credential=credential,
        )
        worker = _Worker(binding, job_id, registration, credential)
        self._workers[intent_id] = worker
        return worker

    async def restart_executor(self, pool_id: str) -> None:
        await self.pools[pool_id].restart()

    async def recover_pool(self, pool_id: str) -> ExecutorTickResult:
        result = await self.pools[pool_id].recover()
        if result.status == "adopted":
            if result.operation_id is None:
                raise RuntimeError("adopted operation omitted its intent")
            await self._trusted_register(pool_id, result.operation_id, result.detail)
            self.pools[pool_id].fake.set_job_state(result.detail, "RUNNING")
        return result

    def _pool_job_bindings(self, pool_id: str) -> dict[str, ExecutableIntentBindingV2]:
        pool = self.pools[pool_id]
        if pool.journal is None:
            return {}
        result: dict[str, ExecutableIntentBindingV2] = {}
        for record in pool.journal.latest_records("job"):
            payload = record.durable_payload()
            if payload is None:
                continue
            value = json.loads(payload.decode("ascii"))
            proof_payload = value.get("ownership_proof")
            request_payload = value.get("request")
            if not isinstance(proof_payload, dict) or not isinstance(request_payload, dict):
                continue
            proof = SignedExecutableOwnershipProofV2.model_validate_json(json.dumps(proof_payload))
            result[str(request_payload["operation_id"])] = proof.metadata.binding
            token = str(request_payload["ownership_token"])
            for job in pool.fake.live_jobs():
                if job["ownership_token"] == token:
                    result[str(job["job_id"])] = proof.metadata.binding
        return result

    def owner_slots(self, subject_id: UUID) -> int:
        return sum(pool.owner_slots(subject_id) for pool in self.pools.values())

    async def manager_intent_evidence(
        self,
        pool_id: str,
        job_id: str,
    ) -> ManagerIntentEvidence:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        binding = self._pool_job_bindings(pool_id).get(job_id)
        if binding is None:
            raise RuntimeError("scheduler job has no durable executable binding")
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(CapacityExecutableIntent).where(
                        CapacityExecutableIntent.intent_id == binding.intent_id
                    )
                )
            ).scalar_one()
        stored = ExecutableIntentBindingV2.model_validate_json(json.dumps(row.binding_payload))
        return ManagerIntentEvidence(
            intent_id=row.intent_id,
            subject_id=row.subject_id,
            pool_id=row.pool_id,
            state=row.state,
            observed_state=row.observed_state,
            concurrency_slots=stored.concurrency_slots,
        )

    def cross_owner_bindings(self) -> list[tuple[str, str]]:
        crossed = []
        for pool_id in self.pools:
            for _job_id, binding in self._pool_job_bindings(pool_id).items():
                owner = self._owners_by_subject.get(binding.subject_id)
                if owner is None:
                    continue
                if owner.demand_kind == "x86" and pool_id != "oldlab":
                    crossed.append((owner.name, pool_id))
                if owner.demand_kind == "arm" and pool_id != "gb10":
                    crossed.append((owner.name, pool_id))
        return sorted(set(crossed))

    def progressive_owner_order(self) -> list[str]:
        ordered = sorted(
            self._workers.values(),
            key=lambda worker: self._intent_launch_ranks[worker.binding.intent_id],
        )
        return [self._owners_by_subject[item.binding.subject_id].name for item in ordered]

    async def canonical_digests(self) -> CanonicalDigests:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        async with self._session_factory() as session:
            intent_rows = (
                (
                    await session.execute(
                        select(CapacityExecutableIntent).order_by(
                            CapacityExecutableIntent.execution_epoch,
                            CapacityExecutableIntent.launch_rank,
                        )
                    )
                )
                .scalars()
                .all()
            )
            executor_rows = (
                (
                    await session.execute(
                        select(CapacityExecutableExecutorState).order_by(
                            CapacityExecutableExecutorState.execution_epoch,
                            CapacityExecutableExecutorState.pool_id,
                        )
                    )
                )
                .scalars()
                .all()
            )
        if not executor_rows:
            raise RuntimeError("manager has no durable executor state")
        execution_epoch = max(row.execution_epoch for row in executor_rows)
        intents = [row for row in intent_rows if row.execution_epoch == execution_epoch]
        executors = [row for row in executor_rows if row.execution_epoch == execution_epoch]

        bindings = []
        manager_evidence = []
        for row in intents:
            binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(row.binding_payload))
            owner = self._owners_by_subject[binding.subject_id]
            bindings.append(
                {
                    "rank": row.launch_rank,
                    "owner": owner.name,
                    "pool": binding.pool_id,
                    "shape": binding.shape_id,
                    "resources": binding.resources.model_dump(mode="json"),
                    "nodes": list(binding.node_ids),
                }
            )
            manager_evidence.append(
                {
                    "rank": row.launch_rank,
                    "owner": owner.name,
                    "pool": row.pool_id,
                    "state": row.state,
                    "observed_state": row.observed_state,
                    "bootstrap_registered": row.bootstrap_registration_epoch is not None,
                    "protected_registered": row.protected_registration_epoch is not None,
                    "protected_released": row.protected_release_sha256 is not None,
                    "terminal_kind": row.terminal_kind,
                    "terminal_identity": row.terminal_identity,
                    "terminal_evidence": row.terminal_evidence_sha256,
                }
            )

        inventory_evidence = []
        journal_evidence = []
        for state in executors:
            pool = self.pools[state.pool_id]
            if state.inventory_payload is None or pool.journal is None:
                raise RuntimeError("manager or journal inventory evidence is absent")
            inventory = ExecutableExecutorInventoryV2.model_validate_json(
                json.dumps(state.inventory_payload)
            )
            journal_record = pool.journal.latest(
                "inventory",
                str(state.executor_incarnation),
            )
            if journal_record is None or journal_record.durable_payload() is None:
                raise RuntimeError("journal inventory evidence is absent")
            manager_bytes = canonical_executable_bytes(inventory)
            if (
                journal_record.durable_payload() != manager_bytes
                or journal_record.payload_digest != canonical_executable_digest(inventory)
                or state.last_inventory_digest != canonical_executable_digest(inventory)
            ):
                raise RuntimeError("manager and journal inventory evidence diverged")
            inventory_evidence.append(
                {
                    "pool": state.pool_id,
                    "records": [
                        {
                            "physical_identity": record.physical_identity,
                            "state": record.state,
                            "resources": record.resources.model_dump(mode="json"),
                            "node_ids": list(record.node_ids),
                            "owned": record.ownership_proof is not None,
                            "terminal": record.terminal_evidence_sha256 is not None,
                        }
                        for record in inventory.records
                    ],
                }
            )
            journal_evidence.extend(
                {
                    "pool": state.pool_id,
                    "kind": record.object_kind,
                    "object": record.object_id,
                    "event": record.event_kind,
                }
                for kind in ("intent", "job")
                for record in pool.journal.latest_records(kind)
            )

        protected = [
            {
                "owner": self._owners_by_subject[item.binding.subject_id].name,
                "pool": item.binding.pool_id,
                "job": item.slurm_job_id,
                "bootstrap_epoch": item.bootstrap_registration_epoch,
                "protected_epoch": item.protected_registration_epoch,
            }
            for item in await self.protected_worker_registrations()
            if item.binding.execution.execution_epoch == execution_epoch
        ]
        evidence = {
            "manager": manager_evidence,
            "journal": sorted(
                journal_evidence,
                key=lambda item: (item["pool"], item["kind"], item["object"]),
            ),
            "protected": sorted(
                protected,
                key=lambda item: (item["pool"], item["owner"], item["job"]),
            ),
        }
        release_digests = []
        for row in intents:
            if row.protected_release_payload is None:
                continue
            release = ExecutableProtectedReleaseV2.model_validate_json(
                json.dumps(row.protected_release_payload)
            )
            release_digest = canonical_executable_digest(release)
            if row.protected_release_digest != release_digest:
                raise RuntimeError("manager protected release digest diverged")
            binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(row.binding_payload))
            release_digests.append(
                {
                    "rank": row.launch_rank,
                    "owner": self._owners_by_subject[binding.subject_id].name,
                    "pool": row.pool_id,
                    "digest": release_digest,
                }
            )
        release_digest = (
            release_digests[0]["digest"]
            if len(release_digests) == 1
            else _sha(
                json.dumps(
                    release_digests,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
        )
        return CanonicalDigests(
            allocation=_sha(json.dumps(bindings, sort_keys=True, separators=(",", ":"))),
            inventory=_sha(json.dumps(inventory_evidence, sort_keys=True, separators=(",", ":"))),
            release=release_digest,
            evidence=_sha(json.dumps(evidence, sort_keys=True, separators=(",", ":"))),
        )

    async def inventory_pipeline_is_exact(self, pool_id: str) -> bool:
        if self._session_factory is None or self._execution is None:
            raise RuntimeError("active manager runtime is unavailable")
        pool = self.pools[pool_id]
        if pool.journal is None or pool.registration is None:
            raise RuntimeError("pool executor runtime is unavailable")
        latest = pool.journal.latest(
            "inventory",
            str(pool.registration.executor_incarnation),
        )
        if latest is None or (durable := latest.durable_payload()) is None:
            return False
        async with self._session_factory() as session:
            state = (
                await session.execute(
                    select(CapacityExecutableExecutorState).where(
                        CapacityExecutableExecutorState.execution_epoch
                        == self._execution.execution_epoch,
                        CapacityExecutableExecutorState.pool_id == pool_id,
                    )
                )
            ).scalar_one()
        if state.inventory_payload is None:
            return False
        stored = json.dumps(
            state.inventory_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        return (
            durable == stored
            and latest.payload_digest == hashlib.sha256(stored).hexdigest()
            and state.last_inventory_digest == hashlib.sha256(stored).hexdigest()
        )

    async def claim_all(self) -> None:
        workers_by_owner: dict[UUID, list[_Worker]] = {}
        for worker in self._workers.values():
            workers_by_owner.setdefault(worker.binding.subject_id, []).append(worker)
        for subject_id, workers in workers_by_owner.items():
            owner = self._owners_by_subject[subject_id]
            available = [attempt for attempt in owner.attempts if not attempt.claimed]
            attempt_index = 0
            for worker in workers:
                for claim_high_water in range(worker.binding.concurrency_slots):
                    if attempt_index == len(available):
                        break
                    attempt = available[attempt_index]
                    attempt_index += 1
                    operation_id = _uuid(
                        f"claim:{worker.binding.intent_id}:{attempt.protected_attempt_id}"
                    )
                    proposal = ExecutableClaimProposalV2(
                        operation_id=operation_id,
                        protected_attempt_id=attempt.protected_attempt_id,
                        execution_generation=attempt.execution_generation,
                        requirements_digest=attempt.requirements_digest,
                        worker_id=worker.registration.worker_id,
                        worker_incarnation=worker.registration.worker_incarnation,
                        expected_claim_high_water=claim_high_water,
                    )
                    receipt = await self._admission.admit_claim(worker.binding, proposal)
                    if receipt is None:
                        raise RuntimeError("protected claim was rejected")
                    attempt.claimed = True
                    self._claims.append(
                        _Claim(owner, attempt, worker, operation_id, receipt.claim_high_water)
                    )
            if attempt_index != len(available):
                raise RuntimeError("owner attempts exceed protected worker capacity")

    async def complete_all_claims(self) -> None:
        for claim in self._claims:
            if claim.terminal:
                continue
            transition = InertAttemptTransitionV1(
                **claim.owner.registration.model_dump(mode="python"),
                transition_id=_uuid(f"claim-terminal:{claim.operation_id}"),
                protected_attempt_id=claim.attempt.protected_attempt_id,
                execution_generation=claim.attempt.execution_generation,
                requirements_digest=claim.attempt.requirements_digest,
                expected_transition_sequence=0,
                operation="cancel",
                expected_state="pending-unassigned",
                target_state="cancelled-terminal",
                transition_reason="task-13-complete",
            )
            engine = create_async_engine(
                make_url(_database_value(claim.owner.database, "agent_url"))
            )
            factory = async_sessionmaker(engine, expire_on_commit=False)
            try:
                async with factory() as session, session.begin():
                    await CapacityAttemptLifecycleStore(
                        session,
                        registration=claim.owner.registration,
                    ).apply_transition(transition)
            finally:
                await engine.dispose()
            claim.terminal = True
            claim.attempt.terminal = True

    async def begin_drain(self) -> None:
        if self._execution is None:
            raise RuntimeError("there is no active execution epoch")
        if self._drained is not None:
            return
        for owner in self._owners.values():
            if owner.active:
                await owner.publish_zero_demand()
        await self.reconcile()
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        active = self._execution
        async with self._session_factory() as session:
            self._drained = await self.management_store.begin_execution_drain(
                session,
                ExecutionDrainV2(
                    authority_incarnation=active.authority_incarnation,
                    expected_writer_epoch=active.writer_epoch,
                    execution_epoch=active.execution_epoch,
                    execution_manifest_sha256=active.execution_manifest_sha256,
                    expected_executable_new_capacity_ceiling=(
                        active.executable_new_capacity_ceiling
                    ),
                    expected_executable_new_capacity_rate_per_minute=(
                        active.executable_new_capacity_rate_per_minute
                    ),
                ),
                actor="activation-operator",
                idempotency_key=_uuid(f"epoch:{self._epoch_number}:drain"),
            )

    async def finish_drain_and_retire(self) -> None:
        if self._drained is None:
            await self.begin_drain()
        if self._session_factory is None or self._execution is None:
            raise RuntimeError("draining execution runtime is unavailable")
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(
                        CapacityExecutableIntent.intent_id,
                        CapacityExecutableIntent.pool_id,
                        CapacityExecutableIntent.launch_rank,
                    )
                    .where(
                        CapacityExecutableIntent.execution_epoch == self._execution.execution_epoch,
                        CapacityExecutableIntent.state != "released",
                    )
                    .order_by(CapacityExecutableIntent.launch_rank)
                )
            ).all()
        completed_claims = False
        for intent_id, pool_id, _rank in rows:
            pool = self.pools[pool_id]
            first = await pool.tick()
            if first.status not in {"draining", "pending-cancelled"}:
                raise RuntimeError(f"intent did not enter protected drain: {first}")
            if not completed_claims:
                await self.complete_all_claims()
                completed_claims = True
            worker = self._workers[intent_id]
            if any(job["job_id"] == worker.job_id for job in pool.fake.live_jobs()):
                pool.fake.terminalize_job(worker.job_id)
            if first.status == "draining":
                closed = await pool.tick()
                if closed.status != "draining":
                    raise RuntimeError(f"intent did not close centrally: {closed}")
            await pool.heartbeat()
            terminal_inventory = await pool.tick()
            if terminal_inventory.status != "inventory-published":
                raise RuntimeError(
                    f"intent terminal inventory was not published: {terminal_inventory}"
                )
            terminal_closed = await pool.tick()
            if terminal_closed.status != "draining":
                raise RuntimeError(f"terminal intent did not close centrally: {terminal_closed}")
            observation = await self._admission.observe_intent(worker.binding)
            release_request = ExecutableReleaseRequestV2(
                operation_id=_uuid(f"protected-release:{intent_id}"),
                binding=worker.binding,
                reporter_incarnation=self._owners_by_subject[
                    worker.binding.subject_id
                ].reporter_incarnation,
                bootstrap_registration_epoch=observation.bootstrap_registration_epoch,
                expected_claim_high_water=observation.claim_high_water,
                protected_registration_epoch=observation.protected_registration_epoch,
                release_epoch=observation.protected_registration_epoch + 1,
            )
            protected = await self._admission.acknowledge_release(
                worker.binding,
                release_request,
                current_worker_credential=worker.credential,
            )
            manager_release = ExecutableProtectedReleaseV2(
                binding=worker.binding,
                reporter_incarnation=protected.reporter_incarnation,
                bootstrap_registration_epoch=protected.bootstrap_registration_epoch,
                protected_registration_epoch=protected.protected_registration_epoch,
                bootstrap_revoked=True,
                protected_release_sha256=protected.protected_release_sha256,
            )
            owner = self._owners_by_subject[worker.binding.subject_id]
            response = await self._request(
                "PUT",
                f"/v2/reports/protected-releases/{owner.subject_id}/"
                f"{worker.binding.shape_instance_id}",
                token=owner.reporter_token,
                value=manager_release,
                idempotency_key=_uuid(f"manager-release:{intent_id}"),
            )
            response.raise_for_status()
            released = await pool.tick()
            if released.status != "released":
                raise RuntimeError(f"intent did not release: {released}")
        for pool_id in _POOL_ORDER:
            pool = self.pools[pool_id]
            await pool.heartbeat()
            final_inventory = await pool.tick()
            if final_inventory.status != "inventory-published":
                raise RuntimeError(
                    f"pool {pool_id} final inventory was not published: {final_inventory}"
                )
            await pool.heartbeat()
            if pool.journal is None or pool.registration is None:
                raise RuntimeError("pool executor runtime is unavailable")
            latest = pool.journal.latest(
                "inventory",
                str(pool.registration.executor_incarnation),
            )
            if latest is None or (payload := latest.durable_payload()) is None:
                raise RuntimeError("final executor inventory is absent from journal")
            pool.last_inventory = ExecutableExecutorInventoryV2.model_validate_json(payload)
        await self._retire_exact()

    async def _retirement_request(self) -> ExecutionRetirementV2:
        if self._session_factory is None or self._drained is None:
            raise RuntimeError("draining execution runtime is unavailable")
        async with self._session_factory() as session:
            states = (
                (
                    await session.execute(
                        select(CapacityExecutableExecutorState)
                        .where(
                            CapacityExecutableExecutorState.execution_epoch
                            == self._drained.execution_epoch
                        )
                        .order_by(CapacityExecutableExecutorState.pool_id)
                    )
                )
                .scalars()
                .all()
            )
        checkpoints = tuple(
            ExecutionRetirementExecutorCheckpointV2(
                executor_id=state.executor_id,
                executor_incarnation=state.executor_incarnation,
                pool_id=cast(Any, state.pool_id),
                pool_generation=state.pool_generation,
                heartbeat_sequence=state.heartbeat_high_water,
                command_sequence=state.command_high_water,
                journal_sequence=state.journal_high_water,
                journal_digest=state.journal_digest,
                inventory_sequence=state.inventory_high_water,
                inventory_digest=cast(str, state.last_inventory_digest),
            )
            for state in states
        )
        return ExecutionRetirementV2(
            authority_incarnation=self._drained.authority_incarnation,
            expected_writer_epoch=self._drained.writer_epoch,
            execution_epoch=self._drained.execution_epoch,
            execution_manifest_sha256=self._drained.execution_manifest_sha256,
            executor_checkpoints=checkpoints,
        )

    async def retire(self) -> None:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        request = await self._retirement_request()
        async with self._session_factory() as session:
            await self.management_store.retire_execution_epoch(
                session,
                request,
                actor="activation-operator",
                idempotency_key=_uuid(f"epoch:{self._epoch_number}:early-retire"),
            )

    async def _retire_exact(self) -> None:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        request = await self._retirement_request()
        async with self._session_factory() as session:
            await self.management_store.retire_execution_epoch(
                session,
                request,
                actor="activation-operator",
                idempotency_key=_uuid(f"epoch:{self._epoch_number}:retire"),
            )
        self._execution = None
        self._drained = None

    async def scale_to_zero(self) -> None:
        await self.begin_drain()
        await self.finish_drain_and_retire()

    async def prepare_with_wrong_candidate(self, owner: OwnerHandle) -> None:
        worker = next(
            worker
            for worker in self._workers.values()
            if worker.binding.subject_id == owner.subject_id
        )
        wrong = worker.binding.model_copy(
            update={
                "candidate": worker.binding.candidate.model_copy(
                    update={"publication_sha256": "0" * 64}
                )
            }
        )
        await self._admission.prepare_worker(
            ExecutableBootstrapRegistrationV2(
                binding=wrong,
                command_sequence=1,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256="1" * 64,
            ),
            bootstrap_sha256="1" * 64,
        )

    async def prepare_binding_with_candidate(
        self,
        owner: OwnerHandle,
        binding: ExecutableIntentBindingV2,
        candidate: CandidateBindingV2,
    ) -> None:
        changed = binding.model_copy(update={"candidate": candidate})
        await self._admission.prepare_worker(
            ExecutableBootstrapRegistrationV2(
                binding=changed,
                command_sequence=1,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256="2" * 64,
            ),
            bootstrap_sha256="2" * 64,
        )

    async def stop_manager(self) -> None:
        self._transport.online = False

    def _redeployment_projection(
        self,
        owner: OwnerHandle,
        candidate_sha256: str,
        generation: int,
    ) -> tuple[DynamicDevelopmentSubjectProjectionV1, UUID, str, str]:
        reporter = _uuid(f"owner:{owner.name}:reporter:{generation}")
        token = f"task-13-{owner.name}-reporter-{generation}"
        publication = _sha(f"owner:{owner.name}:publication:{generation}:{candidate_sha256}")
        projection = owner.projection.model_copy(
            update={
                "expected_configuration_epoch": self._configuration_epoch,
                "operation_kind": "update",
                "operation_id": _uuid(f"owner:{owner.name}:operation:{generation}"),
                "operation_epoch": generation,
                "candidate_generation": generation,
                "candidate_sha256": candidate_sha256,
                "candidate_publication_sha256": publication,
                "deployment_generation": generation,
                "configuration_generation": generation,
                "demand_reporter_incarnation": reporter,
                "demand_reporter_token_sha256": _token_sha256(token),
                "local_activation_sha256": _sha(f"owner:{owner.name}:activation:{generation}"),
                "protected_admission_sha256": _sha(f"owner:{owner.name}:protected:{generation}"),
                "capacity_agent_installation_sha256": _sha(
                    f"owner:{owner.name}:agent:{generation}"
                ),
            }
        )
        return projection, reporter, token, publication

    async def redeploy_owner(self, owner: OwnerHandle, candidate_sha256: str) -> None:
        generation = owner.configuration_generation + 1
        if self.execution_state != "shadow":
            projection, _reporter, _token, _publication = self._redeployment_projection(
                owner,
                candidate_sha256,
                generation,
            )
            response = await self._request(
                "PUT",
                f"/v1/development-projections/{owner.subject_id}",
                value=projection,
                idempotency_key=_uuid(f"owner:{owner.name}:projection-key:{generation}"),
            )
            response.raise_for_status()
            raise RuntimeError("active owner redeployment was unexpectedly accepted")
        await self._rotate_development_profiles(generation)
        projection, reporter, token, publication = self._redeployment_projection(
            owner,
            candidate_sha256,
            generation,
        )
        response = await self._request(
            "PUT",
            f"/v1/development-projections/{owner.subject_id}",
            value=projection,
            idempotency_key=_uuid(f"owner:{owner.name}:projection-key:{generation}"),
        )
        response.raise_for_status()
        projected = response.json()
        self._configuration_epoch = int(projected["configuration_epoch"])
        projected_subject = SubjectConfigurationV1.model_validate_json(
            json.dumps(projected["subject"])
        )
        self._subject_references[owner.subject_id] = ConfigurationGenerationRefV1(
            scope="subject",
            generation=projected_subject.configuration_generation,
            digest=canonical_digest(projected_subject),
            subject_id=owner.subject_id,
            subject_incarnation=owner.subject_incarnation,
        )
        executable_subject = await self._enable_projected_owner(projected_subject)
        old_configuration = owner.configuration_generation
        owner.candidate_sha256 = candidate_sha256
        owner.candidate_publication_sha256 = publication
        owner.candidate_generation = projected_subject.candidate_generation
        owner.deployment_generation = projected_subject.deployment_generation
        owner.configuration_generation = executable_subject.configuration_generation
        owner.reporter_incarnation = reporter
        owner.reporter_token = token
        owner.projection = projection
        owner.demand_sequence = 0
        owner.demand_kind = "zero"
        owner.demand_count = 0
        owner.attempts = []
        owner.registration = owner.registration.model_copy(
            update={
                "reporter_incarnation": reporter,
                "candidate_digest": publication,
                "candidate_identity_algorithm": "source-sha256",
                "candidate_identity": candidate_sha256,
                "candidate_publication_sha256": publication,
                "deployment_generation": projected_subject.deployment_generation,
                "configuration_generation": executable_subject.configuration_generation,
            }
        )
        fence = GuardFenceV1(
            environment_id=f"dev-{owner.name}",
            subject_id=owner.subject_id,
            subject_incarnation=owner.subject_incarnation,
            authority_incarnation=_AUTHORITY_ID,
            reporter_incarnation=reporter,
            deployment_generation=projected_subject.deployment_generation,
            configuration_generation=executable_subject.configuration_generation,
            candidate_digest=publication,
        )
        async with self._owner_stores(owner) as (agent_store, guard_store, _session):
            await guard_store.reconfigure_disabled_authority(
                fence,
                expected_configuration_generation=old_configuration,
            )
            await agent_store.reconfigure_agent(
                owner.registration,
                expected_configuration_generation=old_configuration,
            )

    async def development_projection_count(self) -> int:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        async with self._session_factory() as session:
            return len(
                (await session.execute(select(CapacityDevelopmentProjection.id))).scalars().all()
            )

    async def delete_owner(self, owner: OwnerHandle) -> None:
        generation = owner.configuration_generation + 1
        projection = owner.projection.model_copy(
            update={
                "expected_configuration_epoch": self._configuration_epoch,
                "operation_kind": "destroy",
                "operation_id": _uuid(f"owner:{owner.name}:destroy:{generation}"),
                "operation_epoch": generation,
                "configuration_generation": generation,
            }
        )
        response = await self._request(
            "PUT",
            f"/v1/development-projections/{owner.subject_id}",
            value=projection,
            idempotency_key=_uuid(f"owner:{owner.name}:destroy-key:{generation}"),
        )
        response.raise_for_status()
        self._configuration_epoch = int(response.json()["configuration_epoch"])
        self._subject_references.pop(owner.subject_id, None)
        owner.active = False

    async def reconfigure_stale_agent(
        self,
        owner: OwnerHandle,
        registration: AgentRegistrationV1,
        *,
        expected_configuration_generation: int | None = None,
    ) -> None:
        async with self._owner_stores(owner) as (agent_store, _guard, _session):
            await agent_store.reconfigure_agent(
                registration,
                expected_configuration_generation=(
                    owner.configuration_generation
                    if expected_configuration_generation is None
                    else expected_configuration_generation
                ),
            )

    async def protected_agent_registration(
        self,
        owner: OwnerHandle,
    ) -> AgentRegistrationV1:
        async with self._owner_stores(owner) as (_agent, _guard, session):
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT agent_incarnation, schema_version, environment_id, "
                            "subject_id, subject_incarnation, authority_incarnation, "
                            "reporter_incarnation, authority_mode, allocation_epoch, "
                            "candidate_digest, candidate_identity_algorithm, "
                            "candidate_identity, candidate_publication_sha256, "
                            "deployment_generation, configuration_generation "
                            "FROM loom_capacity_guard.agent_registrations "
                            "WHERE singleton_id = 1"
                        )
                    )
                )
                .mappings()
                .one()
            )
        return AgentRegistrationV1.model_validate({**dict(row), "reporter_high_water": 0})

    async def activate_next_epoch(self) -> None:
        await self._activate_epoch()

    async def accept_next_binding(self, pool_id: str) -> ExecutableIntentBindingV2:
        pool = self.pools[pool_id]
        accepted = await pool.tick()
        if accepted.status != "accepted" or pool.client is None:
            raise RuntimeError(f"next reservation was not accepted: {accepted}")
        checkpoint = await pool.client.executable_checkpoint()
        work = await pool.client.next_executable_work(checkpoint.command_sequence)
        if not isinstance(work, ExecutableIntentBindingV2):
            raise RuntimeError("accepted reservation did not expose an intent binding")
        return work

    def executor_identity(self, pool_id: str) -> ExecutorIdentity:
        pool = self.pools[pool_id]
        if pool.registration is None or pool.client is None:
            raise RuntimeError("pool executor is unavailable")
        return ExecutorIdentity(pool.registration, pool.client, pool.heartbeat_sequence)

    async def heartbeat_stale_executor(self, identity: ExecutorIdentity) -> None:
        registration = identity.registration
        await identity.client.heartbeat_executable_executor(
            ExecutableExecutorHeartbeatV2(
                execution=registration.execution,
                executor_id=registration.executor_id,
                executor_incarnation=registration.executor_incarnation,
                pool_id=registration.pool_id,
                pool_generation=registration.pool_generation,
                heartbeat_sequence=identity.heartbeat_sequence + 1,
                journal_sequence=0,
                journal_digest="0" * 64,
                journal_checkpoint_sequence=0,
                journal_checkpoint_digest="0" * 64,
            )
        )

    def distinct_controller_authorities(self) -> bool:
        oldlab = self.profiles["oldlab"]
        gb10 = self.profiles["gb10"]
        fields = (
            "slurm_cluster",
            "controller_host",
            "partition",
            "association",
            "qos",
            "controller_authority_sha256",
        )
        return all(getattr(oldlab, field) != getattr(gb10, field) for field in fields) and (
            oldlab.submitter == gb10.submitter == "loom"
        )

    def total_loom_jobs(self) -> int:
        return sum(len(pool.fake.live_jobs()) for pool in self.pools.values())

    async def manager_commitments(self) -> tuple[ManagerIntentEvidence, ...]:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        async with self._session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(CapacityExecutableIntent)
                        .where(CapacityExecutableIntent.state != "released")
                        .order_by(
                            CapacityExecutableIntent.execution_epoch,
                            CapacityExecutableIntent.launch_rank,
                        )
                    )
                )
                .scalars()
                .all()
            )
        return tuple(
            ManagerIntentEvidence(
                intent_id=row.intent_id,
                subject_id=row.subject_id,
                pool_id=row.pool_id,
                state=row.state,
                observed_state=row.observed_state,
                concurrency_slots=ExecutableIntentBindingV2.model_validate_json(
                    json.dumps(row.binding_payload)
                ).concurrency_slots,
            )
            for row in rows
        )

    async def charged_slots(self) -> int:
        if self._session_factory is None:
            raise RuntimeError("manager session factory is unavailable")
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(CapacityExecutableIntent.binding_payload).where(
                        CapacityExecutableIntent.state != "released"
                    )
                )
            ).scalars()
        return sum(
            ExecutableIntentBindingV2.model_validate_json(json.dumps(payload)).concurrency_slots
            for payload in rows
        )

    async def protected_worker_registrations(
        self,
    ) -> tuple[ExecutableWorkerRegistrationV2, ...]:
        registrations: list[ExecutableWorkerRegistrationV2] = []
        for owner in self._owners.values():
            async with self._owner_stores(owner) as (_agent, _guard, session):
                payloads = (
                    (
                        await session.execute(
                            text(
                                "SELECT request_payload FROM "
                                "loom_capacity_guard.executable_admission_events "
                                "WHERE event_kind = 'worker-registered' "
                                "ORDER BY event_id"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            registrations.extend(
                ExecutableWorkerRegistrationV2.model_validate_json(json.dumps(payload))
                for payload in payloads
            )
        return tuple(sorted(registrations, key=lambda item: item.binding.intent_id.hex))

    async def protected_live_claim_count(self) -> int:
        count = 0
        for owner in self._owners.values():
            async with self._owner_stores(owner) as (_agent, _guard, session):
                count += int(
                    (
                        await session.execute(
                            text(
                                "SELECT count(*) FROM "
                                "loom_capacity_guard.executable_claim_leases AS lease "
                                "LEFT JOIN "
                                "loom_capacity_guard.executable_claim_terminal_events AS terminal "
                                "ON terminal.admitted_operation_id = lease.operation_id "
                                "WHERE terminal.admitted_operation_id IS NULL"
                            )
                        )
                    ).scalar_one()
                )
        return count

    async def active_subject_ids(self) -> tuple[UUID, ...]:
        response = await self._request("GET", "/v1/status/subjects")
        response.raise_for_status()
        items = response.json()["items"]
        return tuple(
            UUID(item["subject_id"]) for item in items if item["lifecycle_state"] == "active"
        )

    @property
    def execution_state(self) -> str:
        if self._drained is not None:
            return "drain-only"
        if self._execution is not None:
            return "active"
        return "shadow"

    @property
    def oldlab(self) -> PoolHarness:
        return self.pools["oldlab"]

    @property
    def gb10(self) -> PoolHarness:
        return self.pools["gb10"]

    async def aclose(self) -> None:
        for pool in self.pools.values():
            pool.close()
        await self.http.aclose()
        if self._lifespan is not None:
            await self._lifespan.__aexit__(None, None, None)
            self._lifespan = None
        self._cleanup()


__all__ = [
    "CanonicalDigests",
    "ExecutableCapacityHarness",
    "ExecutorIdentity",
    "ManagerIntentEvidence",
    "OwnerHandle",
    "PoolHarness",
]
