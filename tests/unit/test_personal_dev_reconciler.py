from __future__ import annotations

import base64
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
import yaml

from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS, PersonalDevCandidateRecord
from loom.personal_dev_capacity import (
    PersonalDevCapacityInstallation,
    PersonalDevCapacityProjectionResult,
)
from loom.personal_dev_capacity_runtime import (
    CapacityDatabaseInstallation,
    KubectlPersonalDevCapacityInstaller,
    PersonalDevCapacityInstallationError,
    PersonalDevCapacityRuntimeConfig,
)
from loom.personal_dev_environment import (
    PersonalDevAccessBinding,
    PersonalDevEnvironmentRecord,
    PersonalDevLifecycleAttemptRecord,
    PersonalDevLifecycleOperationRecord,
    PersonalDevReconciliationClaim,
)
from loom.personal_dev_reconciler import (
    PersonalDevEnvironmentReconciler,
    PersonalDevReadinessObservation,
    personal_dev_readiness_sha256,
)
from loom.personal_dev_runtime import PersonalDevPreparationRuntime, PersonalDevRuntimeConfig
from loom_capacity_agent.client import DemandReporterTLSFiles
from loom_capacity_agent.contracts import AgentPoolCapabilityV1

_NOW = datetime(2026, 8, 11, tzinfo=UTC)
_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000010")
_ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000011")
_CANDIDATE_ID = UUID("00000000-0000-0000-0000-000000000012")
_SUBJECT_ID = UUID("00000000-0000-0000-0000-000000000013")
_INCARNATION = UUID("00000000-0000-0000-0000-000000000014")
_OWNER = UUID("00000000-0000-0000-0000-000000000015")
_TEAM = UUID("00000000-0000-0000-0000-000000000016")
_RUNTIME_PASSWORD = "r" * 48
_RUNTIME_DATABASE_URL = (
    "postgresql+psycopg://loom_cap_alice_runtime:" + _RUNTIME_PASSWORD + "@db/loom_dev_alice"
)


def _images() -> dict[str, str]:
    return {
        component: f"registry.example/loom-{component}@sha256:{index:064x}"
        for index, component in enumerate(PERSONAL_DEV_COMPONENTS, start=1)
    }


def _candidate(*, status: str = "ready") -> PersonalDevCandidateRecord:
    images = _images()
    publication = {
        "images": {
            component: {"index": reference, "platforms": {}}
            for component, reference in images.items()
        },
        "protocol_versions": {
            "capacity-agent": "v1",
            "claim-guard": "v1",
            "control-plane-worker": "v1",
            "database-migrations": "expand-compatible-v1",
            "personal-dev-activation": "v1",
        },
    }
    return PersonalDevCandidateRecord(
        id=_CANDIDATE_ID,
        owner_user_id=_OWNER,
        owner_team_id=_TEAM,
        candidate_sha="a" * 64,
        source_sha256="b" * 64,
        archive_sha256="c" * 64,
        build_contract_sha256="d" * 64,
        source_commit="e" * 40,
        dirty=True,
        manifest_json={},
        object_bucket="artifacts",
        object_key="personal-dev/source.tar",
        source_generation_id=_CANDIDATE_ID,
        archive_size_bytes=10240,
        status=status,  # type: ignore[arg-type]
        publication_json=publication if status == "ready" else None,
        publication_sha256="f" * 64 if status == "ready" else None,
        image_manifest_digest="sha256:" + "1" * 64 if status == "ready" else None,
        failure_reason="builder_failed" if status == "failed" else None,
        created_at=_NOW,
        updated_at=_NOW,
        ready_at=_NOW if status == "ready" else None,
    )


def _claim(
    *,
    state: str = "running",
    checkpoint: str = "candidate_build",
    status: str = "ready",
    expected_capacity_epoch: int | None = None,
) -> PersonalDevReconciliationClaim:
    operation = PersonalDevLifecycleOperationRecord(
        id=_OPERATION_ID,
        idempotency_key=UUID("00000000-0000-0000-0000-000000000020"),
        environment_name="alice",
        subject_id=_SUBJECT_ID,
        subject_incarnation=_INCARNATION,
        owner_user_id=_OWNER,
        owner_team_id=_TEAM,
        operation_epoch=1,
        expected_operation_epoch=0,
        kind="create",
        state=state,  # type: ignore[arg-type]
        attempt_id=_ATTEMPT_ID,
        attempt_sequence=0,
        request_sha256="2" * 64,
        candidate_id=_CANDIDATE_ID,
        candidate_sha="a" * 64,
        min_slots=0,
        max_slots=2,
        deployment_generation=1,
        checkpoint=checkpoint,
        readiness_evidence_sha256="3" * 64 if state == "activating" else None,
        activation_acknowledgement_sha256="4" * 64 if state == "activating" else None,
        local_activation_sha256="5" * 64 if state == "activating" else None,
        capacity_expected_configuration_epoch=expected_capacity_epoch,
        capacity_projection_request_sha256=None,
        capacity_reporter_incarnation=(
            UUID("00000000-0000-0000-0000-000000000031")
            if expected_capacity_epoch is not None
            else None
        ),
        capacity_reporter_token_sha256=(
            "9620a6302cf6bd606b15d74072424d9c70135ba56a686618dbba4dfa2d554476"
            if expected_capacity_epoch is not None
            else None
        ),
        protected_admission_sha256=("8" * 64 if expected_capacity_epoch is not None else None),
        capacity_agent_installation_sha256=(
            "9" * 64 if expected_capacity_epoch is not None else None
        ),
        created_at=_NOW,
        updated_at=_NOW,
        started_at=_NOW,
    )
    return PersonalDevReconciliationClaim(
        environment=PersonalDevEnvironmentRecord(
            name="alice",
            subject_id=_SUBJECT_ID,
            subject_incarnation=_INCARNATION,
            owner_user_id=_OWNER,
            owner_team_id=_TEAM,
            min_slots=0,
            max_slots=2,
            status="activating" if state == "activating" else "provisioning",
            deployment_generation=1,
            candidate_id=_CANDIDATE_ID,
            candidate_sha="a" * 64,
            operation_epoch=1,
            operation_id=_OPERATION_ID,
            operation_step=checkpoint,
            created_at=_NOW,
            updated_at=_NOW,
        ),
        operation=operation,
        attempt=PersonalDevLifecycleAttemptRecord(
            id=_ATTEMPT_ID,
            operation_id=_OPERATION_ID,
            subject_id=_SUBJECT_ID,
            subject_incarnation=_INCARNATION,
            operation_epoch=1,
            attempt_sequence=0,
            state=state,  # type: ignore[arg-type]
            checkpoint=checkpoint,
            access_binding=PersonalDevAccessBinding(
                auth_kind="bearer",
                credential_hash=b"h" * 32,
            ),
            lease_epoch=7,
            claimed_by="reconciler-a",
            lease_expires_at=_NOW + timedelta(minutes=1),
            created_at=_NOW,
            updated_at=_NOW,
            started_at=_NOW,
        ),
        candidate=_candidate(status=status),
    )


def _destroy_claim(
    checkpoint: str,
    *,
    keep_data: bool = False,
    expected_capacity_epoch: int | None = None,
) -> PersonalDevReconciliationClaim:
    base = _claim()
    operation = replace(
        base.operation,
        operation_epoch=2,
        expected_operation_epoch=1,
        kind="destroy",
        state="running",
        checkpoint=checkpoint,
        keep_data=keep_data,
        local_activation_sha256="5" * 64,
        capacity_expected_configuration_epoch=expected_capacity_epoch,
        capacity_reporter_incarnation=UUID("00000000-0000-0000-0000-000000000031"),
        capacity_reporter_token_sha256=(
            "9620a6302cf6bd606b15d74072424d9c70135ba56a686618dbba4dfa2d554476"
        ),
        protected_admission_sha256="8" * 64,
        capacity_agent_installation_sha256="9" * 64,
        capacity_supported_pool_ids=("gb10", "oldlab"),
        capacity_supported_architectures=("arm64", "x86_64"),
    )
    return replace(
        base,
        environment=replace(
            base.environment,
            status="deleting",
            operation_epoch=2,
            operation_step=checkpoint,
            keep_data=keep_data,
            capacity_configuration_epoch=7,
            capacity_configuration_sha256="7" * 64,
            capacity_reporter_incarnation=operation.capacity_reporter_incarnation,
            capacity_reporter_token_sha256=operation.capacity_reporter_token_sha256,
            local_activation_sha256=operation.local_activation_sha256,
            protected_admission_sha256=operation.protected_admission_sha256,
            capacity_agent_installation_sha256=(operation.capacity_agent_installation_sha256),
            capacity_supported_pool_ids=operation.capacity_supported_pool_ids,
            capacity_supported_architectures=(operation.capacity_supported_architectures),
        ),
        operation=operation,
        attempt=replace(
            base.attempt,
            operation_epoch=2,
            checkpoint=checkpoint,
            state="running",
        ),
    )


def _observation() -> PersonalDevReadinessObservation:
    images = _images()
    return PersonalDevReadinessObservation(
        deployed_images={
            component: images[component]
            for component in ("control-plane", "llm-gateway", "service", "web")
        },
        resource_evidence_sha256="9" * 64,
    )


def test_readiness_digest_binds_exact_attempt_generation_publication_and_images() -> None:
    claim = _claim()
    evidence = personal_dev_readiness_sha256(claim, _observation())
    assert len(evidence) == 64
    assert evidence == personal_dev_readiness_sha256(claim, _observation())

    changed = PersonalDevReadinessObservation(
        deployed_images={**_observation().deployed_images, "web": _images()["service"]},
        resource_evidence_sha256="9" * 64,
    )
    with pytest.raises(ValueError, match="deployed image"):
        personal_dev_readiness_sha256(claim, changed)


class _Authority:
    def __init__(self, claim: PersonalDevReconciliationClaim) -> None:
        self.claim = claim
        self.begun: list[str] = []
        self.failed: list[str] = []
        self.completed = 0
        self.prepared_capacity: list[dict[str, object]] = []
        self.refreshed_capacity: list[int] = []
        self.projected_capacity: list[PersonalDevCapacityProjectionResult] = []
        self.destroy_checkpoints: list[tuple[str, str]] = []

    async def claim_next_reconciliation(self, **_kwargs):
        return self.claim

    async def begin_activation(self, *, readiness_evidence_sha256, **_kwargs):
        self.begun.append(readiness_evidence_sha256)

    async def fail_pre_activation(self, *, failure_reason, **_kwargs):
        self.failed.append(failure_reason)

    async def complete_activation(self, **_kwargs):
        self.completed += 1

    async def prepare_capacity_projection(self, **kwargs):
        self.prepared_capacity.append(kwargs)

    async def refresh_capacity_projection_epoch(self, *, expected_configuration_epoch, **_kwargs):
        self.refreshed_capacity.append(expected_configuration_epoch)

    async def record_capacity_projection(self, *, result, **_kwargs):
        self.projected_capacity.append(result)

    async def advance_destroy_checkpoint(self, *, expected_checkpoint, checkpoint, **_kwargs):
        self.destroy_checkpoints.append((expected_checkpoint, checkpoint))


def _installation() -> PersonalDevCapacityInstallation:
    return PersonalDevCapacityInstallation(
        reporter_incarnation=UUID("00000000-0000-0000-0000-000000000031"),
        reporter_token="reporter-token",
        protected_admission_sha256="8" * 64,
        capacity_agent_installation_sha256="9" * 64,
        supported_pool_ids=("gb10", "oldlab"),
        supported_architectures=("arm64", "x86_64"),
    )


class _Installer:
    def __init__(self) -> None:
        self.calls = 0
        self.publishing_checks = 0
        self.sealed = 0
        self.destroyed = 0

    async def converge(self, _claim):
        self.calls += 1
        return _installation()

    async def verify_publishing(self, _claim, installation):
        assert installation == _installation()
        self.publishing_checks += 1

    async def seal(self, _claim):
        self.sealed += 1

    async def destroy(self, _claim):
        self.destroyed += 1


class _Projector:
    def __init__(self, *, conflict: bool = False) -> None:
        self.conflict = conflict
        self.requests = []

    async def current_manager_checkpoint(self):
        from loom.personal_dev_capacity import PersonalDevCapacityManagerCheckpoint

        return PersonalDevCapacityManagerCheckpoint(
            configuration_epoch=11,
            execution_state="active",
            execution_epoch=7,
            executable_new_capacity_ceiling=2,
        )

    async def project(self, request, *, idempotency_key):
        from loom.personal_dev_capacity import PersonalDevCapacityProjectionConflictError

        self.requests.append((request, idempotency_key))
        if self.conflict:
            raise PersonalDevCapacityProjectionConflictError("stale")
        return PersonalDevCapacityProjectionResult(
            configuration_epoch=request.expected_configuration_epoch + 1,
            configuration_digest="a" * 64,
            subject_id=request.subject_id,
            subject_incarnation=request.subject_incarnation,
            configuration_generation=request.configuration_generation,
            deployment_generation=request.deployment_generation,
            reporter_incarnation=request.demand_reporter_incarnation,
            replayed=False,
        )


class _Executor:
    def __init__(self) -> None:
        self.prepared = 0
        self.bootstrapped = 0
        self.cleanup: list[str] = []

    async def prepare(self, claim, *, access):
        assert claim.attempt.access_binding.credential_hash == b"h" * 32
        assert access == "owner-access-before"
        self.prepared += 1
        return _observation()

    async def bootstrap_access(self, claim, *, access):
        assert claim.attempt.access_binding.credential_hash == b"h" * 32
        assert access == "owner-access-current"
        self.bootstrapped += 1

    async def delete_namespace(self, _claim):
        self.cleanup.append("namespace")

    async def delete_buckets(self, _claim):
        self.cleanup.append("buckets")

    async def delete_tenant(self, _claim):
        self.cleanup.append("tenant")

    async def delete_credentials(self, _claim):
        self.cleanup.append("credentials")


async def test_reconciler_prepares_ready_candidate_but_waits_for_trusted_ack() -> None:
    authority = _Authority(_claim())
    executor = _Executor()
    snapshots = iter(("owner-access-before", "owner-access-current"))

    reconciled = await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        capacity_installer=_Installer(),
        capacity_projector=_Projector(),
        access_loader=lambda _claim: _async_value(next(snapshots)),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert reconciled is True
    assert executor.prepared == 1
    assert executor.bootstrapped == 1
    assert len(authority.begun) == 1
    assert authority.completed == 0


async def test_reconciler_fails_when_bound_credential_is_revoked_during_preparation() -> None:
    authority = _Authority(_claim())
    executor = _Executor()
    calls = 0

    async def load_access(_claim):
        nonlocal calls
        calls += 1
        if calls == 1:
            return "owner-access-before"
        raise RuntimeError("credential revoked")

    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        capacity_installer=_Installer(),
        capacity_projector=_Projector(),
        access_loader=load_access,
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert executor.prepared == 1
    assert executor.bootstrapped == 0
    assert authority.begun == []
    assert authority.failed == ["provisioning_failed"]


async def test_reconciler_terminalizes_failed_build_without_external_effects() -> None:
    authority = _Authority(_claim(status="failed"))
    executor = _Executor()

    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        capacity_installer=_Installer(),
        capacity_projector=_Projector(),
        access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert executor.prepared == 0
    assert authority.failed == ["candidate_build_failed"]


async def test_reconciler_prepares_capacity_before_finalizing_acknowledged_activation() -> None:
    authority = _Authority(_claim(state="activating", checkpoint="activation_acknowledged"))
    executor = _Executor()
    installer = _Installer()
    projector = _Projector()

    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        capacity_installer=installer,
        capacity_projector=projector,
        access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert authority.completed == 0
    assert installer.calls == 1
    assert projector.requests == []
    assert len(authority.prepared_capacity) == 1
    assert authority.prepared_capacity[0]["expected_configuration_epoch"] == 11
    assert executor.prepared == 0


async def test_reconciler_projects_exact_prepared_capacity_then_waits_to_finalize() -> None:
    authority = _Authority(
        _claim(
            state="activating",
            checkpoint="capacity_projection_pending",
            expected_capacity_epoch=11,
        )
    )
    projector = _Projector()

    installer = _Installer()
    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=_Executor(),  # type: ignore[arg-type]
        capacity_installer=installer,
        capacity_projector=projector,
        access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert len(projector.requests) == 1
    request, idempotency_key = projector.requests[0]
    assert request.expected_configuration_epoch == 11
    assert request.local_activation_sha256 == "5" * 64
    assert idempotency_key == _claim().operation.idempotency_key
    assert installer.publishing_checks == 1
    assert len(authority.projected_capacity) == 1
    assert authority.completed == 0


async def test_reconciler_refreshes_global_epoch_after_projection_contention() -> None:
    authority = _Authority(
        _claim(
            state="activating",
            checkpoint="capacity_projection_pending",
            expected_capacity_epoch=10,
        )
    )

    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=_Executor(),  # type: ignore[arg-type]
        capacity_installer=_Installer(),
        capacity_projector=_Projector(conflict=True),
        access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert authority.refreshed_capacity == [11]
    assert authority.projected_capacity == []
    assert authority.completed == 0


async def test_reconciler_finalizes_only_a_capacity_projected_activation() -> None:
    authority = _Authority(_claim(state="activating", checkpoint="capacity_projected"))

    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=_Executor(),  # type: ignore[arg-type]
        capacity_installer=_Installer(),
        capacity_projector=_Projector(),
        access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert authority.completed == 1


async def test_destroy_reconciler_prepares_and_projects_retirement_without_reinstalling() -> None:
    requested_authority = _Authority(_destroy_claim("capacity_retirement_requested"))
    installer = _Installer()
    projector = _Projector()
    await PersonalDevEnvironmentReconciler(
        authority=requested_authority,  # type: ignore[arg-type]
        executor=_Executor(),  # type: ignore[arg-type]
        capacity_installer=installer,
        capacity_projector=projector,
        access_loader=lambda _claim: _async_value("unused"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert installer.calls == 0
    assert len(requested_authority.prepared_capacity) == 1
    assert requested_authority.prepared_capacity[0]["expected_configuration_epoch"] == 11

    pending_authority = _Authority(
        _destroy_claim("capacity_projection_pending", expected_capacity_epoch=11)
    )
    await PersonalDevEnvironmentReconciler(
        authority=pending_authority,  # type: ignore[arg-type]
        executor=_Executor(),  # type: ignore[arg-type]
        capacity_installer=installer,
        capacity_projector=projector,
        access_loader=lambda _claim: _async_value("unused"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    request, _key = projector.requests[-1]
    assert request.operation_kind == "destroy"
    assert request.demand_reporter_token_sha256 == (
        pending_authority.claim.operation.capacity_reporter_token_sha256
    )
    assert installer.publishing_checks == 0
    assert len(pending_authority.projected_capacity) == 1


@pytest.mark.parametrize(
    ("checkpoint", "keep_data", "expected_installer", "expected_executor", "next_checkpoint"),
    [
        ("capacity_retired", False, "seal", None, "local_authority_sealed"),
        ("local_authority_sealed", False, None, "namespace", "namespace_deleted"),
        ("namespace_deleted", False, "destroy", None, "database_deleted"),
        ("database_deleted", False, None, "buckets", "buckets_deleted"),
        ("buckets_deleted", False, None, "tenant", "tenant_deleted"),
        ("tenant_deleted", False, None, "credentials", "complete"),
        ("namespace_deleted", True, None, "tenant", "tenant_deleted"),
        ("tenant_deleted", True, None, "credentials", "complete"),
    ],
)
async def test_destroy_reconciler_advances_one_idempotent_cleanup_checkpoint(
    checkpoint: str,
    keep_data: bool,
    expected_installer: str | None,
    expected_executor: str | None,
    next_checkpoint: str,
) -> None:
    authority = _Authority(_destroy_claim(checkpoint, keep_data=keep_data))
    installer = _Installer()
    executor = _Executor()
    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        capacity_installer=installer,
        capacity_projector=_Projector(),
        access_loader=lambda _claim: _async_value("unused"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert installer.sealed == int(expected_installer == "seal")
    assert installer.destroyed == int(expected_installer == "destroy")
    assert executor.cleanup == ([] if expected_executor is None else [expected_executor])
    assert authority.destroy_checkpoints == [(checkpoint, next_checkpoint)]


async def _async_value(value):
    return value


async def test_live_preparation_runtime_converges_fixtures_then_exact_generation() -> None:
    events: list[str] = []
    owner_access = type("OwnerAccess", (), {"user_id": _OWNER, "team_id": _TEAM})()

    class _Sql:
        async def apply_role_and_database(self, _identity, **_kwargs):
            events.append("database")

    class _Buckets:
        async def ensure_buckets(self, _identity, _buckets):
            events.append("buckets")

    class _Vault:
        password = None

        async def database_password(self, _identity):
            assert events and events[-1] == "namespace-authority"
            return self.password

        async def store(self, _identity, password):
            assert password == "a" * 20
            self.password = password
            events.append("secrets")
            return "k8s-secret://loom-dev-alice/loom-secrets"

    class _Tenant:
        async def converge(self, _identity):
            events.append("tenant")

    class _Cluster:
        async def bootstrap(self, _identity, config):
            assert config.candidate_sha == "a" * 64
            assert config.lifecycle_binding is not None
            assert config.lifecycle_binding.attempt_id == _ATTEMPT_ID
            events.append("namespace-authority")

        async def prepare(self, _identity, config):
            assert config.candidate_sha == "a" * 64
            assert config.lifecycle_binding is not None
            assert config.lifecycle_binding.attempt_id == _ATTEMPT_ID
            events.append("generation")
            return _observation()

    class _Access:
        async def bootstrap(self, _identity, *, password, access):
            assert password == "a" * 20
            assert access is owner_access
            events.append("access")

    runtime = PersonalDevPreparationRuntime(
        config=PersonalDevRuntimeConfig(minio_endpoint="https://minio.example"),
        sql=_Sql(),  # type: ignore[arg-type]
        buckets=_Buckets(),  # type: ignore[arg-type]
        vault=_Vault(),  # type: ignore[arg-type]
        object_store_tenant=_Tenant(),  # type: ignore[arg-type]
        cluster=_Cluster(),  # type: ignore[arg-type]
        access=_Access(),  # type: ignore[arg-type]
        password_factory=lambda: "a" * 20,
    )

    observation = await runtime.prepare(
        _claim(),
        access=owner_access,  # type: ignore[arg-type]
    )

    assert observation == _observation()
    assert events == [
        "namespace-authority",
        "database",
        "buckets",
        "secrets",
        "tenant",
        "generation",
    ]

    await runtime.bootstrap_access(
        _claim(),
        access=owner_access,  # type: ignore[arg-type]
    )
    assert events[-2:] == ["namespace-authority", "access"]


@pytest.mark.asyncio
async def test_capacity_installer_configuration_uses_candidate_publication_digest(
    tmp_path: Path,
) -> None:
    """Real ready candidates may have different source and publication digests."""

    class _Kubectl:
        def __init__(self) -> None:
            self.secrets: dict[str, dict[str, bytes]] = {
                "loom-protected-worker-runtime": {
                    "database-url": _RUNTIME_DATABASE_URL.encode("ascii")
                }
            }

        async def read_secret_optional(self, namespace, name):
            assert namespace == "loom-dev-alice"
            return self.secrets.get(name)

        async def apply(self, manifest):
            for document in yaml.safe_load_all(manifest):
                if document and document["kind"] == "Secret":
                    self.secrets[document["metadata"]["name"]] = {  # type: ignore[index]
                        key: base64.b64decode(value)
                        for key, value in document["data"].items()  # type: ignore[index,union-attr]
                    }

    class _Database:
        def __init__(self) -> None:
            self.configuration = None
            self.runtime_password = None

        async def converge(self, *, configuration, credentials, **_kwargs):
            self.configuration = configuration
            self.runtime_password = credentials.runtime_password
            return CapacityDatabaseInstallation(
                protected_admission_sha256="7" * 64,
                agent_database_url=(
                    "postgresql+psycopg://agent:"
                    + credentials.agent_password
                    + "@loom-dev-postgres/loom_dev_alice"
                ),
                runtime_database_url=_RUNTIME_DATABASE_URL,
            )

    def credential(name: str, payload: str) -> Path:
        path = tmp_path / name
        path.write_text(payload)
        path.chmod(0o600)
        return path

    database = _Database()
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=_Kubectl(),  # type: ignore[arg-type]
        database=database,
        config=PersonalDevCapacityRuntimeConfig(
            manager_origin="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443",
            tls_files=DemandReporterTLSFiles(
                ca_file=credential("ca.pem", "ca"),
                certificate_file=credential("certificate.pem", "certificate"),
                private_key_file=credential("private-key.pem", "private-key"),
            ),
            trusted_agent_image="registry.example/loom-service@sha256:" + "1" * 64,
            pool_capabilities=(
                AgentPoolCapabilityV1(
                    capability_id="oldlab-x86-none",
                    pool_id="oldlab",
                    operating_system="linux",
                    cpu_architecture="x86_64",
                    gpu_vendor="none",
                    network_policies=("public",),
                ),
            ),
        ),
    )

    await installer.converge(_claim())

    assert database.configuration is not None
    assert database.configuration.candidate_digest == "a" * 64
    assert database.configuration.candidate_publication_sha256 == "f" * 64
    assert database.runtime_password == _RUNTIME_PASSWORD


@pytest.mark.asyncio
async def test_capacity_installer_requires_runtime_secret_before_database_mutation(
    tmp_path: Path,
) -> None:
    class _Kubectl:
        async def read_secret_optional(self, namespace, name):
            assert namespace == "loom-dev-alice"
            assert name == "loom-protected-worker-runtime"
            return None

        async def apply(self, _manifest):
            raise AssertionError("credentials must fail before persistence")

    class _Database:
        called = False

        async def converge(self, **_kwargs):
            self.called = True
            raise AssertionError("credentials must fail before database mutation")

    def credential(name: str) -> Path:
        path = tmp_path / name
        path.write_text(name)
        path.chmod(0o600)
        return path

    database = _Database()
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=_Kubectl(),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        config=PersonalDevCapacityRuntimeConfig(
            manager_origin="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443",
            tls_files=DemandReporterTLSFiles(
                ca_file=credential("missing-ca.pem"),
                certificate_file=credential("missing-certificate.pem"),
                private_key_file=credential("missing-private-key.pem"),
            ),
            trusted_agent_image="registry.example/loom-service@sha256:" + "1" * 64,
            pool_capabilities=(
                AgentPoolCapabilityV1(
                    capability_id="oldlab-x86-none",
                    pool_id="oldlab",
                    operating_system="linux",
                    cpu_architecture="x86_64",
                    gpu_vendor="none",
                    network_policies=("public",),
                ),
            ),
        ),
    )

    with pytest.raises(
        PersonalDevCapacityInstallationError,
        match="runtime credential is unavailable",
    ):
        await installer.converge(_claim())

    assert database.called is False


@pytest.mark.asyncio
async def test_capacity_installer_rejects_seed_runtime_secret_mismatch_before_mutation(
    tmp_path: Path,
) -> None:
    class _Kubectl:
        def __init__(self) -> None:
            self.secrets = {
                "loom-protected-worker-runtime": {
                    "database-url": _RUNTIME_DATABASE_URL.encode("ascii")
                },
                "loom-capacity-agent-credentials": {
                    "agent-password": b"a" * 48,
                    "observer-password": b"o" * 48,
                    "operation-id": str(_OPERATION_ID).encode("ascii"),
                    "reporter-incarnation": str(
                        UUID("00000000-0000-0000-0000-000000000099")
                    ).encode("ascii"),
                    "reporter-token": b"t" * 48,
                    "runtime-password": b"s" * 48,
                    "subject-incarnation": str(_INCARNATION).encode("ascii"),
                },
            }

        async def read_secret_optional(self, namespace, name):
            assert namespace == "loom-dev-alice"
            return self.secrets.get(name)

        async def apply(self, _manifest):
            raise AssertionError("mismatch must fail before persistence")

    class _Database:
        called = False

        async def converge(self, **_kwargs):
            self.called = True
            raise AssertionError("mismatch must fail before database mutation")

    def credential(name: str) -> Path:
        path = tmp_path / name
        path.write_text(name)
        path.chmod(0o600)
        return path

    database = _Database()
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=_Kubectl(),  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        config=PersonalDevCapacityRuntimeConfig(
            manager_origin="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443",
            tls_files=DemandReporterTLSFiles(
                ca_file=credential("mismatch-ca.pem"),
                certificate_file=credential("mismatch-certificate.pem"),
                private_key_file=credential("mismatch-private-key.pem"),
            ),
            trusted_agent_image="registry.example/loom-service@sha256:" + "1" * 64,
            pool_capabilities=(
                AgentPoolCapabilityV1(
                    capability_id="oldlab-x86-none",
                    pool_id="oldlab",
                    operating_system="linux",
                    cpu_architecture="x86_64",
                    gpu_vendor="none",
                    network_policies=("public",),
                ),
            ),
        ),
    )

    with pytest.raises(
        PersonalDevCapacityInstallationError,
        match="runtime credential was superseded",
    ):
        await installer.converge(_claim())

    assert database.called is False


@pytest.mark.asyncio
async def test_capacity_installer_is_idempotent_and_rotates_only_for_replacement(
    tmp_path: Path,
) -> None:
    class _Kubectl:
        def __init__(self) -> None:
            self.secrets: dict[str, dict[str, bytes]] = {
                "loom-protected-worker-runtime": {
                    "database-url": _RUNTIME_DATABASE_URL.encode("ascii")
                }
            }
            self.documents: list[dict[str, object]] = []
            self.applied: list[list[dict[str, object]]] = []
            self.waited: list[tuple[str, str]] = []

        async def read_secret_optional(self, namespace, name):
            assert namespace == "loom-dev-alice"
            assert name in {
                "loom-capacity-agent",
                "loom-capacity-agent-credentials",
                "loom-protected-worker-runtime",
            }
            return self.secrets.get(name)

        async def apply(self, manifest):
            self.documents = [item for item in yaml.safe_load_all(manifest) if item]
            self.applied.append(self.documents)
            for document in self.documents:
                if document["kind"] != "Secret":
                    continue
                self.secrets[document["metadata"]["name"]] = {  # type: ignore[index]
                    key: base64.b64decode(value)
                    for key, value in document["data"].items()  # type: ignore[index,union-attr]
                }

        async def wait_deployment(self, namespace, name):
            self.waited.append((namespace, name))

    class _Database:
        def __init__(self) -> None:
            self.configurations = []

        async def converge(self, *, configuration, credentials, **_kwargs):
            self.configurations.append(configuration)
            return CapacityDatabaseInstallation(
                protected_admission_sha256=(
                    ("7" if configuration.candidate_digest == "a" * 64 else "8") * 64
                ),
                agent_database_url=(
                    "postgresql+psycopg://agent:"
                    + credentials.agent_password
                    + "@loom-dev-postgres/loom_dev_alice"
                ),
                runtime_database_url=_RUNTIME_DATABASE_URL,
            )

    def credential(name: str, payload: str) -> Path:
        path = tmp_path / name
        path.write_text(payload)
        path.chmod(0o600)
        return path

    kubectl = _Kubectl()
    database = _Database()
    runtime_config = PersonalDevCapacityRuntimeConfig(
        manager_origin="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443",
        tls_files=DemandReporterTLSFiles(
            ca_file=credential("ca.pem", "ca"),
            certificate_file=credential("certificate.pem", "certificate"),
            private_key_file=credential("private-key.pem", "private-key"),
        ),
        trusted_agent_image="registry.example/loom-service@sha256:" + "1" * 64,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
            AgentPoolCapabilityV1(
                capability_id="gb10-arm-none",
                pool_id="gb10",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=kubectl,  # type: ignore[arg-type]
        database=database,
        config=runtime_config,
    )
    create = _claim()
    first = await installer.converge(create)
    replay = await installer.converge(create)
    assert replay == first
    assert database.configurations[-1].reporter_incarnation == first.reporter_incarnation
    seed = kubectl.secrets["loom-capacity-agent-credentials"]
    runtime_secret = kubectl.secrets["loom-capacity-agent"]
    assert seed["reporter-token"] == runtime_secret["reporter-token"]
    assert seed["operation-id"] == runtime_secret["operation-id"]
    assert seed["agent-password"] in runtime_secret["database-url"]
    assert seed["runtime-password"] == _RUNTIME_PASSWORD.encode("ascii")
    assert kubectl.waited == []
    await installer.verify_publishing(create, first)
    assert kubectl.waited == [("loom-dev-alice", "loom-capacity-agent")]
    changed_runtime = KubectlPersonalDevCapacityInstaller(
        kubectl=kubectl,  # type: ignore[arg-type]
        database=database,
        config=replace(runtime_config, max_attempts=9_999),
    )
    changed_installation = await changed_runtime.converge(create)
    assert (
        changed_installation.capacity_agent_installation_sha256
        != first.capacity_agent_installation_sha256
    )

    capacity_operation = replace(
        create.operation,
        id=UUID("00000000-0000-0000-0000-000000000041"),
        idempotency_key=UUID("00000000-0000-0000-0000-000000000042"),
        operation_epoch=2,
        expected_operation_epoch=1,
        kind="capacity",
        checkpoint="capacity_projection_requested",
    )
    capacity = replace(create, operation=capacity_operation)
    capacity_result = await installer.converge(capacity)
    assert capacity_result.reporter_incarnation == first.reporter_incarnation
    assert capacity_result.protected_admission_sha256 == first.protected_admission_sha256
    assert (
        capacity_result.capacity_agent_installation_sha256
        == first.capacity_agent_installation_sha256
    )
    assert database.configurations[-1].configuration_generation == 2

    replacement_operation = replace(
        create.operation,
        id=UUID("00000000-0000-0000-0000-000000000051"),
        idempotency_key=UUID("00000000-0000-0000-0000-000000000052"),
        operation_epoch=3,
        expected_operation_epoch=2,
        kind="update",
        candidate_sha="b" * 64,
        deployment_generation=2,
    )
    replacement = replace(create, operation=replacement_operation)
    replacement_result = await installer.converge(replacement)
    assert replacement_result.reporter_incarnation != first.reporter_incarnation
    assert replacement_result.protected_admission_sha256 != first.protected_admission_sha256
    assert database.configurations[-1].candidate_digest == "b" * 64
    assert database.configurations[-1].deployment_generation == 2

    deployment = kubectl.documents[1]
    assert deployment["kind"] == "Deployment"
    assert "registry.example/loom-service@sha256:" in str(deployment)
    assert "registry.example/loom-service@sha256:" + "1" * 64 in str(deployment)
    network_policy = kubectl.documents[2]
    assert network_policy["kind"] == "NetworkPolicy"
    egress = network_policy["spec"]["egress"]  # type: ignore[index]
    assert egress == [
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "loom-dev"}
                    },
                    "podSelector": {
                        "matchLabels": {"app.kubernetes.io/name": "loom-capacity-manager"}
                    },
                }
            ],
            "ports": [{"protocol": "TCP", "port": 8443}],
        },
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "loom-dev"}
                    },
                    "podSelector": {"matchLabels": {"app": "loom-dev-postgres"}},
                }
            ],
            "ports": [{"protocol": "TCP", "port": 5432}],
        },
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                    },
                    "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                }
            ],
            "ports": [
                {"protocol": "UDP", "port": 53},
                {"protocol": "TCP", "port": 53},
            ],
        },
    ]
    assert kubectl.waited == [("loom-dev-alice", "loom-capacity-agent")]


@pytest.mark.asyncio
async def test_capacity_installer_persists_credentials_before_database_mutation(
    tmp_path: Path,
) -> None:
    class _Kubectl:
        def __init__(self) -> None:
            self.secrets: dict[str, dict[str, bytes]] = {
                "loom-protected-worker-runtime": {
                    "database-url": _RUNTIME_DATABASE_URL.encode("ascii")
                }
            }
            self.mutate_seed = False

        async def read_secret_optional(self, namespace, name):
            assert namespace == "loom-dev-alice"
            return self.secrets.get(name)

        async def apply(self, manifest):
            for document in yaml.safe_load_all(manifest):
                if document and document["kind"] == "Secret":
                    name = document["metadata"]["name"]
                    self.secrets[name] = {
                        key: base64.b64decode(value) for key, value in document["data"].items()
                    }
                    if self.mutate_seed and name == "loom-capacity-agent-credentials":
                        self.secrets[name]["unexpected"] = b"mutation"

    class _FailingDatabase:
        def __init__(self) -> None:
            self.reporters = []

        async def converge(self, *, configuration, **_kwargs):
            self.reporters.append(configuration.reporter_incarnation)
            raise RuntimeError("database failed after protected mutation")

    def credential(name: str, payload: str) -> Path:
        path = tmp_path / name
        path.write_text(payload)
        path.chmod(0o600)
        return path

    kubectl = _Kubectl()
    database = _FailingDatabase()
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=kubectl,  # type: ignore[arg-type]
        database=database,  # type: ignore[arg-type]
        config=PersonalDevCapacityRuntimeConfig(
            manager_origin="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443",
            tls_files=DemandReporterTLSFiles(
                ca_file=credential("seed-ca.pem", "ca"),
                certificate_file=credential("seed-certificate.pem", "certificate"),
                private_key_file=credential("seed-private-key.pem", "private-key"),
            ),
            trusted_agent_image="registry.example/loom-service@sha256:" + "1" * 64,
            pool_capabilities=(
                AgentPoolCapabilityV1(
                    capability_id="oldlab-x86-none",
                    pool_id="oldlab",
                    operating_system="linux",
                    cpu_architecture="x86_64",
                    gpu_vendor="none",
                    network_policies=("public",),
                ),
                AgentPoolCapabilityV1(
                    capability_id="gb10-arm-none",
                    pool_id="gb10",
                    operating_system="linux",
                    cpu_architecture="arm64",
                    gpu_vendor="none",
                    network_policies=("public",),
                ),
            ),
        ),
    )

    with pytest.raises(RuntimeError, match="database failed"):
        await installer.converge(_claim())
    first_seed = dict(kubectl.secrets["loom-capacity-agent-credentials"])

    legacy_seed = dict(first_seed)
    observer_password = legacy_seed.pop("observer-password")
    kubectl.secrets["loom-capacity-agent-credentials"] = legacy_seed

    with pytest.raises(RuntimeError, match="database failed"):
        await installer.converge(_claim())

    upgraded_seed = kubectl.secrets["loom-capacity-agent-credentials"]
    assert upgraded_seed["observer-password"]
    assert upgraded_seed["observer-password"] != observer_password
    for key in ("agent-password", "reporter-incarnation", "reporter-token"):
        assert upgraded_seed[key] == legacy_seed[key]
    assert len(set(database.reporters)) == 1

    with pytest.raises(RuntimeError, match="database failed"):
        await installer.converge(_claim())
    assert kubectl.secrets["loom-capacity-agent-credentials"] == upgraded_seed
    assert len(set(database.reporters)) == 1
    assert "loom-capacity-agent" not in kubectl.secrets

    kubectl.mutate_seed = True
    with pytest.raises(
        PersonalDevCapacityInstallationError,
        match="seed was not installed exactly",
    ):
        await installer.converge(_claim())
    assert len(database.reporters) == 3
