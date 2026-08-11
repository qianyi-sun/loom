from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS, PersonalDevCandidateRecord
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

_NOW = datetime(2026, 8, 11, tzinfo=UTC)
_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000010")
_ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000011")
_CANDIDATE_ID = UUID("00000000-0000-0000-0000-000000000012")
_SUBJECT_ID = UUID("00000000-0000-0000-0000-000000000013")
_INCARNATION = UUID("00000000-0000-0000-0000-000000000014")
_OWNER = UUID("00000000-0000-0000-0000-000000000015")
_TEAM = UUID("00000000-0000-0000-0000-000000000016")


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
    *, state: str = "running", checkpoint: str = "candidate_build", status: str = "ready"
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

    async def claim_next_reconciliation(self, **_kwargs):
        return self.claim

    async def begin_activation(self, *, readiness_evidence_sha256, **_kwargs):
        self.begun.append(readiness_evidence_sha256)

    async def fail_pre_activation(self, *, failure_reason, **_kwargs):
        self.failed.append(failure_reason)

    async def complete_activation(self, **_kwargs):
        self.completed += 1


class _Executor:
    def __init__(self) -> None:
        self.prepared = 0
        self.bootstrapped = 0

    async def prepare(self, claim, *, access):
        assert claim.attempt.access_binding.credential_hash == b"h" * 32
        assert access == "owner-access-before"
        self.prepared += 1
        return _observation()

    async def bootstrap_access(self, claim, *, access):
        assert claim.attempt.access_binding.credential_hash == b"h" * 32
        assert access == "owner-access-current"
        self.bootstrapped += 1


async def test_reconciler_prepares_ready_candidate_but_waits_for_trusted_ack() -> None:
    authority = _Authority(_claim())
    executor = _Executor()
    snapshots = iter(("owner-access-before", "owner-access-current"))

    reconciled = await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
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
        access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert executor.prepared == 0
    assert authority.failed == ["candidate_build_failed"]


async def test_reconciler_finalizes_only_an_acknowledged_activation_claim() -> None:
    authority = _Authority(_claim(state="activating", checkpoint="activation_acknowledged"))
    executor = _Executor()

    await PersonalDevEnvironmentReconciler(
        authority=authority,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a",
        lease_seconds=60,
    ).reconcile_once(now=_NOW)

    assert authority.completed == 1
    assert executor.prepared == 0


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
    assert events == ["database", "buckets", "secrets", "tenant", "generation"]

    await runtime.bootstrap_access(
        _claim(),
        access=owner_access,  # type: ignore[arg-type]
    )
    assert events[-1] == "access"
