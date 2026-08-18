"""Authenticated and bounded capacity-manager HTTP surface tests."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import ssl
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, event, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom_capacity_manager.allocator import ShadowAllocatorError, allocate_shadow
from loom_capacity_manager.api import (
    RequestBodyLimitMiddleware,
    _health_payload,
    _writer_matches_authority,
    create_app,
)
from loom_capacity_manager.auth import CapacityPrincipal, CapacityPrincipalVerifier
from loom_capacity_manager.config import CapacityManagerSettings, build_uvicorn_kwargs
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    ResourceVectorV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableAdmissionAcknowledgementV2,
    ExecutableAdmissionAllowanceV2,
    ExecutableAdmissionPlanClosureAcknowledgementV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableAdmissionShapeV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableReservationAcceptanceV2,
    ExecutableSubmissionRecoveryV2,
    ExecutionActivationV2,
    ExecutionAuthorityV2,
    ExecutionContextV2,
    ExecutionDrainV2,
    ExecutionFenceV2,
    ExecutionPreparationAbortV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    ExecutionRetirementExecutorCheckpointV2,
    ExecutionRetirementV2,
    ProtectedAdmissionAssignmentV2,
    canonical_executable_bytes,
    canonical_executable_digest,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.execution_store import (
    CapacityExecutionStore,
    ProposedExecutableBootstrap,
    RegisteredExecutableAdmissionPlan,
    RegisteredExecutableAdmissionPlanClosure,
    RegisteredExecutableBootstrap,
)
from loom_capacity_manager.models import (
    Base,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuditEvent,
    CapacityAuthorityState,
)
from loom_capacity_manager.ownership import public_key_fingerprint
from loom_capacity_manager.preparation_readiness import (
    PreparedExecutionReadinessV2,
    canonical_prepared_readiness_digest,
    load_prepared_execution_readiness,
)
from loom_capacity_manager.reconciler import (
    ReconciliationFailurePersistenceError,
    reconcile_shadow_once,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ExecutionPreparationDisabledError,
    StaleWriterError,
    WriterFence,
)
from tests.capacity_execution_fixtures import (
    execution_policy,
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import (
    AUTHORITY_ID,
    DEMAND_REPORTER_ID,
    DEVELOPMENT_REPORTER_INCARNATION,
    DEVELOPMENT_SUBJECT_ID,
    DEVELOPMENT_SUBJECT_INCARNATION,
    POOL_REPORTER_GB10_ID,
    POOL_REPORTER_OLDLAB_ID,
    SUBJECT_ID,
    SUBJECT_INCARNATION,
    demand_snapshot,
    development_projection,
    fleet_manifest,
    fleet_with_development_template,
    pool_observation,
    shape,
    subject_configuration,
)

OPERATOR_TOKEN = "operator-api-secret"
DEMAND_TOKEN = "demand-api-secret"
GB10_TOKEN = "gb10-api-secret"
OLDLAB_TOKEN = "oldlab-api-secret"
DYNAMIC_DEMAND_TOKEN = "dynamic-demand-api-secret"
OLDLAB_EXECUTOR_TOKEN = "oldlab-executor-secret"
OLDLAB_V2_EXECUTOR_TOKEN = "oldlab-v2-executor-secret"
EXECUTION_PREPARE_TOKEN = "execution-prepare-secret"
EXECUTION_ABORT_TOKEN = "execution-abort-secret"
EXECUTION_ACTIVATE_TOKEN = "execution-activate-secret"
EXECUTION_DRAIN_TOKEN = "execution-drain-secret"
EXECUTION_RETIRE_TOKEN = "execution-retire-secret"
EXECUTION_READ_TOKEN = "execution-read-secret"
GB10_V2_EXECUTOR_TOKEN = "gb10-v2-executor-secret"
OLDLAB_EXECUTOR_INCARNATION = UUID("00000000-0000-4000-8000-000000000601")
OLDLAB_OWNERSHIP_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(b"\x03" * 32)
V2_TRANCHE_ID = UUID(int=41)
V2_INTENT_ID = UUID(int=42)
V2_PERMIT_ID = UUID(int=43)
V2_ACCEPTED_INTENT_ID = UUID(int=44)


def _v2_intent_binding() -> ExecutableIntentBindingV2:
    return ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            authority_incarnation=AUTHORITY_ID,
            writer_epoch=1,
            configuration_epoch=1,
            execution_epoch=1,
            execution_manifest_sha256="1" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            trusted_fleet_release_sha256="2" * 64,
            allocation_epoch=1,
        ),
        tranche_id=UUID(int=801),
        intent_id=UUID(int=802),
        shape_instance_id="shape-oldlab-1",
        subject_id=SUBJECT_ID,
        subject_incarnation=SUBJECT_INCARNATION,
        account_id="owner-1",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="3" * 64,
            publication_sha256="4" * 64,
        ),
        candidate_generation=1,
        deployment_generation=1,
        pool_id="oldlab",
        pool_generation=1,
        executor_id="oldlab-executor",
        executor_incarnation=OLDLAB_EXECUTOR_INCARNATION,
        shape_id="one-slot",
        profile_id="oldlab-profile",
        profile_generation=1,
        profile_digest="5" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=1_000,
            memory_bytes=1_073_741_824,
        ),
        node_ids=("oldlab1",),
    )


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _principal(
    principal_id: str,
    token: str,
    scopes: list[str],
    *,
    subject_id: UUID | None = None,
    subject_incarnation: UUID | None = None,
    demand_reporter_incarnation: UUID | None = None,
    pool_id: str | None = None,
    pool_reporter_incarnation: UUID | None = None,
    executor_id: str | None = None,
    executor_incarnation: UUID | None = None,
    executor_pool_generation: int | None = None,
) -> dict[str, object]:
    return {
        "principal_id": principal_id,
        "token_sha256": _hash(token),
        "scopes": scopes,
        "subject_id": None if subject_id is None else str(subject_id),
        "subject_incarnation": (None if subject_incarnation is None else str(subject_incarnation)),
        "demand_reporter_incarnation": (
            None if demand_reporter_incarnation is None else str(demand_reporter_incarnation)
        ),
        "pool_id": pool_id,
        "pool_reporter_incarnation": (
            None if pool_reporter_incarnation is None else str(pool_reporter_incarnation)
        ),
        "executor_id": executor_id,
        "executor_incarnation": (
            None if executor_incarnation is None else str(executor_incarnation)
        ),
        "executor_pool_generation": executor_pool_generation,
    }


def _execution_fence() -> ExecutionFenceV2:
    return ExecutionFenceV2(
        authority_incarnation=AUTHORITY_ID,
        writer_epoch=1,
        configuration_epoch=1,
        execution_epoch=2,
        execution_manifest_sha256="c" * 64,
        execution_state="active",
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        trusted_fleet_release_sha256="d" * 64,
        allocation_epoch=3,
    )


def _intent_binding_v2(
    *,
    pool_id: str = "oldlab",
    pool_generation: int = 1,
    executor_id: str = "oldlab-executor",
    executor_incarnation: UUID = OLDLAB_EXECUTOR_INCARNATION,
) -> ExecutableIntentBindingV2:
    return ExecutableIntentBindingV2(
        execution=_execution_fence(),
        tranche_id=V2_TRANCHE_ID,
        intent_id=V2_INTENT_ID,
        shape_instance_id="shape-1",
        subject_id=SUBJECT_ID,
        subject_incarnation=SUBJECT_INCARNATION,
        account_id="owner-1",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="1" * 64,
            publication_sha256="2" * 64,
        ),
        candidate_generation=1,
        deployment_generation=1,
        pool_id=pool_id,
        pool_generation=pool_generation,
        executor_id=executor_id,
        executor_incarnation=executor_incarnation,
        shape_id="shape-a",
        profile_id="profile-a",
        profile_generation=1,
        profile_digest="3" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=1000,
            memory_bytes=1_073_741_824,
        ),
        node_ids=("node-1",),
    )


def _v2_acceptance_payload(
    *,
    pool_id: str = "oldlab",
    pool_generation: int = 1,
    executor_id: str = "oldlab-executor",
    executor_incarnation: UUID = OLDLAB_EXECUTOR_INCARNATION,
    tranche_id: UUID = V2_TRANCHE_ID,
) -> dict[str, object]:
    return ExecutableReservationAcceptanceV2(
        execution=_execution_fence(),
        tranche_id=tranche_id,
        proposal_digest="4" * 64,
        pool_id=pool_id,
        pool_generation=pool_generation,
        executor_id=executor_id,
        executor_incarnation=executor_incarnation,
        command_sequence=5,
    ).model_dump(mode="json")


def _v2_submission_recovery_payload(
    *,
    binding: ExecutableIntentBindingV2 | None = None,
    permit_id: UUID = V2_PERMIT_ID,
) -> dict[str, object]:
    resolved_binding = _intent_binding_v2() if binding is None else binding
    return ExecutableSubmissionRecoveryV2(
        binding=resolved_binding,
        permit_id=permit_id,
        permit_digest="5" * 64,
        command_sequence=6,
        inventory_sequence=2,
        inventory_digest="6" * 64,
        controller_query_completed_at=datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
        submit_process_absent=True,
        scheduler_submission_absent=True,
        controller_evidence_sha256="7" * 64,
    ).model_dump(mode="json")


class RecordingExecutionStore:
    def __init__(self) -> None:
        self.accept_calls = 0
        self.recovery_calls = 0
        self.last_acceptance: ExecutableReservationAcceptanceV2 | None = None
        self.last_recovery: ExecutableSubmissionRecoveryV2 | None = None

    async def accept_reservation(self, _session, acceptance: ExecutableReservationAcceptanceV2):  # type: ignore[no-untyped-def]
        self.accept_calls += 1
        self.last_acceptance = acceptance
        return {
            "tranche_id": str(acceptance.tranche_id),
            "intent_ids": [str(V2_ACCEPTED_INTENT_ID)],
            "receipt_digest": "8" * 64,
            "replayed": False,
            "executable": True,
        }

    async def recover_unsubmitted_permit(
        self,
        _session,  # type: ignore[no-untyped-def]
        recovery: ExecutableSubmissionRecoveryV2,
    ) -> dict[str, object]:
        self.recovery_calls += 1
        self.last_recovery = recovery
        return {
            "intent_id": str(recovery.binding.intent_id),
            "receipt_digest": "9" * 64,
            "replayed": False,
            "executable": True,
        }


class StaticPrincipalVerifier:
    def __init__(self, principal: CapacityPrincipal) -> None:
        self._principal = principal

    def verify_bearer(self, _header: str | None) -> CapacityPrincipal:
        return self._principal


def _owner_file(path: Path, value: str) -> Path:
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


async def _reset_capacity_database(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        authority_transition_trigger_disabled = False
        await session.execute(
            text(
                "ALTER TABLE capacity_authority_state DISABLE TRIGGER "
                "capacity_authority_execution_transition_guard"
            )
        )
        authority_transition_trigger_disabled = True
        try:
            await session.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(
                    authority_incarnation=AUTHORITY_ID,
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
            for table in reversed(Base.metadata.sorted_tables):
                if table.name == CapacityAuthorityState.__tablename__:
                    continue
                user_triggers_disabled = False
                await session.execute(text(f"ALTER TABLE {table.name} DISABLE TRIGGER USER"))
                user_triggers_disabled = True
                try:
                    await session.execute(delete(table))
                finally:
                    if user_triggers_disabled:
                        await session.execute(text(f"ALTER TABLE {table.name} ENABLE TRIGGER USER"))
        finally:
            if authority_transition_trigger_disabled:
                await session.execute(
                    text(
                        "ALTER TABLE capacity_authority_state ENABLE TRIGGER "
                        "capacity_authority_execution_transition_guard"
                    )
                )


class BlockingAllocator:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()

    def __call__(self, value):  # type: ignore[no-untyped-def]
        self.started.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("test allocator release timed out")
        return allocate_shadow(value)


class ActiveTimeoutAllocator:
    async def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        await asyncio.sleep(5)


class ActiveInvalidAllocator:
    def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        raise ShadowAllocatorError("synthetic invalid allocation")


class ActiveUnexpectedAllocator:
    def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        raise RuntimeError("synthetic unexpected allocation failure")


class ActiveCommitFailureStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self._input_loads = 0

    async def load_allocation_input(self, session, writer):  # type: ignore[no-untyped-def]
        self._input_loads += 1
        if self._input_loads == 2:
            raise RuntimeError("synthetic executable transaction failure")
        return await super().load_allocation_input(session, writer)


class AuthorityResolutionFailureStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self.reconcile_failure_records = 0

    async def execution_authority(self, session):  # type: ignore[no-untyped-def]
        del session
        raise RuntimeError("synthetic authority resolution failure")

    async def record_reconcile_failure(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.reconcile_failure_records += 1
        return await super().record_reconcile_failure(*args, **kwargs)


class CommitAndRecorderFailureStore(ActiveCommitFailureStore):
    async def record_reconcile_failure(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        del args, kwargs
        raise RuntimeError("synthetic recorder failure")


class FailureInputDriftStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self._input_loads = 0
        self.expected_digest: str | None = None
        self.observed_digest: str | None = None

    async def load_allocation_input(self, session, writer):  # type: ignore[no-untyped-def]
        value = await super().load_allocation_input(session, writer)
        self._input_loads += 1
        if self._input_loads == 1:
            self.expected_digest = canonical_digest(value)
            return value
        drifted = value.model_copy(
            update={"existing_pending_slots": value.existing_pending_slots + 1}
        )
        self.observed_digest = canonical_digest(drifted)
        return drifted


class ExpiredFreshnessFenceStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())

    def allocation_input_valid_until(self, value):  # type: ignore[no-untyped-def]
        del value
        return datetime(2000, 1, 1, tzinfo=UTC)


class CrossingFreshnessFenceStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self.validity_checks = 0

    def allocation_input_valid_until(self, value):  # type: ignore[no-untyped-def]
        del value
        self.validity_checks += 1
        year = 2999 if self.validity_checks == 1 else 2000
        return datetime(year, 1, 1, tzinfo=UTC)


class HostileConstraintSearchPathStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self._input_loads = 0

    async def load_allocation_input(self, session, writer):  # type: ignore[no-untyped-def]
        value = await super().load_allocation_input(session, writer)
        self._input_loads += 1
        if self._input_loads == 2:
            await session.execute(
                text("SET LOCAL search_path TO capacity_constraint_decoy, public")
            )
        return value


class ActivateBeforeShadowCommitStore(CapacityManagementStore):
    def __init__(self, activation: ExecutionActivationV2) -> None:
        super().__init__(execution_policy=execution_policy())
        self._activation = activation

    async def commit_shadow_epoch(self, session, writer, epoch):  # type: ignore[no-untyped-def]
        await self.activate_execution_epoch(
            session,
            self._activation,
            actor="activation-operator",
            idempotency_key=UUID(int=882),
        )
        return await super().commit_shadow_epoch(session, writer, epoch)


async def _ingest_fresh_execution_inputs(
    session: AsyncSession,
    store: CapacityManagementStore,
) -> None:
    await store.ingest_demand_snapshot(
        session,
        demand_snapshot(sequence=1, pending_attempt_ids=("attempt-pending",)),
        actor="development",
    )
    for pool_id in ("gb10", "oldlab"):
        await store.ingest_pool_observation(
            session,
            pool_observation(sequence=1, pool_id=pool_id),
            actor=f"{pool_id}-reporter",
        )


@pytest.fixture
def operator_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {OPERATOR_TOKEN}"}


@pytest.fixture
def reporter_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {DEMAND_TOKEN}"}


@pytest.fixture
async def api_context(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator]]:
    await _reset_capacity_database(capacity_session_factory)
    registry_path = _owner_file(
        tmp_path / "principals.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    _principal(
                        "fleet-operator",
                        OPERATOR_TOKEN,
                        [
                            "capacity:configure:fleet",
                            "capacity:configure:subject",
                            "capacity:configure:activate",
                            "capacity:project:development",
                            "capacity:reconcile",
                            "capacity:read",
                            "capacity:grant:manage",
                        ],
                    ),
                    _principal(
                        "dev-reporter",
                        DEMAND_TOKEN,
                        ["capacity:report:demand"],
                        subject_id=SUBJECT_ID,
                        subject_incarnation=SUBJECT_INCARNATION,
                        demand_reporter_incarnation=subject_configuration().demand_reporter_incarnation,
                    ),
                    _principal(
                        "gb10-reporter",
                        GB10_TOKEN,
                        ["capacity:report:pool"],
                        pool_id="gb10",
                        pool_reporter_incarnation=POOL_REPORTER_GB10_ID,
                    ),
                    _principal(
                        "oldlab-reporter",
                        OLDLAB_TOKEN,
                        ["capacity:report:pool"],
                        pool_id="oldlab",
                        pool_reporter_incarnation=POOL_REPORTER_OLDLAB_ID,
                    ),
                    _principal(
                        "oldlab-executor",
                        OLDLAB_EXECUTOR_TOKEN,
                        ["capacity:execute:pool"],
                        pool_id="oldlab",
                        executor_id="oldlab-executor",
                        executor_incarnation=OLDLAB_EXECUTOR_INCARNATION,
                        executor_pool_generation=1,
                    ),
                ],
            }
        ),
    )
    db_url_path = _owner_file(tmp_path / "database-url", capacity_postgres_url)
    dummy_cert = _owner_file(tmp_path / "server.crt", "test")
    dummy_key = _owner_file(tmp_path / "server.key", "test")
    dummy_ca = _owner_file(tmp_path / "client-ca.crt", "test")
    ownership_keys = _owner_file(
        tmp_path / "ownership-public-keys.json",
        json.dumps(
            {
                "schema_version": 1,
                "keys": [
                    {
                        "signing_key_id": "oldlab-key-1",
                        "public_key_base64": base64.b64encode(
                            OLDLAB_OWNERSHIP_PRIVATE_KEY.public_key().public_bytes(
                                encoding=serialization.Encoding.Raw,
                                format=serialization.PublicFormat.Raw,
                            )
                        ).decode("ascii"),
                    }
                ],
            }
        ),
    )
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=db_url_path,
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=dummy_cert,
        tls_key_file=dummy_key,
        tls_client_ca_file=dummy_ca,
        ownership_public_keys_file=ownership_keys,
        allocation_timeout_seconds=5,
    )
    allocator = BlockingAllocator()
    app = create_app(
        settings,
        verifier=CapacityPrincipalVerifier.from_file(registry_path),
        allocator=allocator,
    )
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        fleet = fleet_with_development_template()
        subject = subject_configuration(fleet)
        fleet_response = client.put(
            "/v1/config-proposals/fleet",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=fleet.model_dump(mode="json"),
        )
        assert fleet_response.status_code == 200, fleet_response.text
        subject_response = client.put(
            f"/v1/config-proposals/subjects/{SUBJECT_ID}",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=subject.model_dump(mode="json"),
        )
        assert subject_response.status_code == 200, subject_response.text
        fleet_proposal = fleet_response.json()
        subject_proposal = subject_response.json()
        activation = {
            "schema_version": 1,
            "expected_configuration_epoch": 0,
            "fleet": {
                "schema_version": 1,
                "scope": "fleet",
                "generation": fleet_proposal["generation"],
                "digest": fleet_proposal["digest"],
                "subject_id": None,
                "subject_incarnation": None,
            },
            "subjects": [
                {
                    "schema_version": 1,
                    "scope": "subject",
                    "generation": subject_proposal["generation"],
                    "digest": subject_proposal["digest"],
                    "subject_id": str(SUBJECT_ID),
                    "subject_incarnation": str(SUBJECT_INCARNATION),
                }
            ],
            "static_candidate_provenance": [
                {
                    "schema_version": 1,
                    "subject_id": str(SUBJECT_ID),
                    "subject_incarnation": str(SUBJECT_INCARNATION),
                    "candidate_generation": subject.candidate_generation,
                    "algorithm": "source-sha256",
                    "identity": "1" * 64,
                    "publication_sha256": "2" * 64,
                }
            ],
        }
        activation_response = client.post(
            "/v1/config-activations",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=activation,
        )
        assert activation_response.status_code == 200, activation_response.text
        assert (
            client.put(
                f"/v1/reports/demand/{SUBJECT_ID}",
                headers={"Authorization": f"Bearer {DEMAND_TOKEN}"},
                json=demand_snapshot(sequence=1).model_dump(mode="json"),
            ).status_code
            == 200
        )
        assert (
            client.put(
                "/v1/reports/pools/gb10",
                headers={"Authorization": f"Bearer {GB10_TOKEN}"},
                json=pool_observation(sequence=1, pool_id="gb10").model_dump(mode="json"),
            ).status_code
            == 200
        )
        assert (
            client.put(
                "/v1/reports/pools/oldlab",
                headers={"Authorization": f"Bearer {OLDLAB_TOKEN}"},
                json=pool_observation(sequence=1, pool_id="oldlab").model_dump(mode="json"),
            ).status_code
            == 200
        )
        yield client, app, settings, allocator
    await _reset_capacity_database(capacity_session_factory)


@pytest.fixture
async def api_context_v2_executor_generation(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator]]:
    await _reset_capacity_database(capacity_session_factory)
    registry_path = _owner_file(
        tmp_path / "principals-v2.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    _principal(
                        "fleet-operator",
                        OPERATOR_TOKEN,
                        [
                            "capacity:configure:fleet",
                            "capacity:configure:subject",
                            "capacity:configure:activate",
                            "capacity:project:development",
                            "capacity:reconcile",
                            "capacity:read",
                            "capacity:grant:manage",
                        ],
                    ),
                    _principal(
                        "dev-reporter",
                        DEMAND_TOKEN,
                        ["capacity:report:demand"],
                        subject_id=SUBJECT_ID,
                        subject_incarnation=SUBJECT_INCARNATION,
                        demand_reporter_incarnation=subject_configuration().demand_reporter_incarnation,
                    ),
                    _principal(
                        "gb10-reporter",
                        GB10_TOKEN,
                        ["capacity:report:pool"],
                        pool_id="gb10",
                        pool_reporter_incarnation=POOL_REPORTER_GB10_ID,
                    ),
                    _principal(
                        "oldlab-reporter",
                        OLDLAB_TOKEN,
                        ["capacity:report:pool"],
                        pool_id="oldlab",
                        pool_reporter_incarnation=POOL_REPORTER_OLDLAB_ID,
                    ),
                    _principal(
                        "oldlab-executor-v2",
                        OLDLAB_V2_EXECUTOR_TOKEN,
                        ["capacity:execute:pool"],
                        pool_id="oldlab",
                        executor_id="oldlab-executor",
                        executor_incarnation=OLDLAB_EXECUTOR_INCARNATION,
                        executor_pool_generation=1,
                    ),
                ],
            }
        ),
    )
    db_url_path = _owner_file(tmp_path / "database-url-v2", capacity_postgres_url)
    dummy_cert = _owner_file(tmp_path / "server-v2.crt", "test")
    dummy_key = _owner_file(tmp_path / "server-v2.key", "test")
    dummy_ca = _owner_file(tmp_path / "client-ca-v2.crt", "test")
    ownership_keys = _owner_file(
        tmp_path / "ownership-public-keys-v2.json",
        json.dumps(
            {
                "schema_version": 1,
                "keys": [
                    {
                        "signing_key_id": "oldlab-key-1",
                        "public_key_base64": base64.b64encode(
                            OLDLAB_OWNERSHIP_PRIVATE_KEY.public_key().public_bytes(
                                encoding=serialization.Encoding.Raw,
                                format=serialization.PublicFormat.Raw,
                            )
                        ).decode("ascii"),
                    }
                ],
            }
        ),
    )
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=db_url_path,
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=dummy_cert,
        tls_key_file=dummy_key,
        tls_client_ca_file=dummy_ca,
        ownership_public_keys_file=ownership_keys,
        allocation_timeout_seconds=5,
    )
    allocator = BlockingAllocator()
    app = create_app(
        settings,
        verifier=CapacityPrincipalVerifier.from_file(registry_path),
        allocator=allocator,
    )
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        fleet = fleet_with_development_template()
        subject = subject_configuration(fleet)
        fleet_response = client.put(
            "/v1/config-proposals/fleet",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=fleet.model_dump(mode="json"),
        )
        assert fleet_response.status_code == 200, fleet_response.text
        subject_response = client.put(
            f"/v1/config-proposals/subjects/{SUBJECT_ID}",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=subject.model_dump(mode="json"),
        )
        assert subject_response.status_code == 200, subject_response.text
        fleet_proposal = fleet_response.json()
        subject_proposal = subject_response.json()
        activation = {
            "schema_version": 1,
            "expected_configuration_epoch": 0,
            "fleet": {
                "schema_version": 1,
                "scope": "fleet",
                "generation": fleet_proposal["generation"],
                "digest": fleet_proposal["digest"],
                "subject_id": None,
                "subject_incarnation": None,
            },
            "subjects": [
                {
                    "schema_version": 1,
                    "scope": "subject",
                    "generation": subject_proposal["generation"],
                    "digest": subject_proposal["digest"],
                    "subject_id": str(SUBJECT_ID),
                    "subject_incarnation": str(SUBJECT_INCARNATION),
                }
            ],
            "static_candidate_provenance": [
                {
                    "schema_version": 1,
                    "subject_id": str(SUBJECT_ID),
                    "subject_incarnation": str(SUBJECT_INCARNATION),
                    "candidate_generation": subject.candidate_generation,
                    "algorithm": "source-sha256",
                    "identity": "1" * 64,
                    "publication_sha256": "2" * 64,
                }
            ],
        }
        activation_response = client.post(
            "/v1/config-activations",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=activation,
        )
        assert activation_response.status_code == 200, activation_response.text
        assert (
            client.put(
                f"/v1/reports/demand/{SUBJECT_ID}",
                headers={"Authorization": f"Bearer {DEMAND_TOKEN}"},
                json=demand_snapshot(sequence=1).model_dump(mode="json"),
            ).status_code
            == 200
        )
        assert (
            client.put(
                "/v1/reports/pools/gb10",
                headers={"Authorization": f"Bearer {GB10_TOKEN}"},
                json=pool_observation(sequence=1, pool_id="gb10").model_dump(mode="json"),
            ).status_code
            == 200
        )
        assert (
            client.put(
                "/v1/reports/pools/oldlab",
                headers={"Authorization": f"Bearer {OLDLAB_TOKEN}"},
                json=pool_observation(sequence=1, pool_id="oldlab").model_dump(mode="json"),
            ).status_code
            == 200
        )
        yield client, app, settings, allocator
    await _reset_capacity_database(capacity_session_factory)


@pytest.fixture
async def execution_preparation_api_context(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[
    tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ]
]:
    """Build the real pinned-policy API around one committed shadow fixture."""

    await _reset_capacity_database(capacity_session_factory)
    policy = execution_policy()
    async with capacity_session_factory() as session:
        fixture = await setup_execution(session, execution_policy=policy)
        await session.commit()

    registry_path = _owner_file(
        tmp_path / "execution-principals.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    _principal(
                        "execution-preparer",
                        EXECUTION_PREPARE_TOKEN,
                        ["capacity:execution:prepare", "capacity:reconcile"],
                    ),
                    _principal(
                        "execution-aborter",
                        EXECUTION_ABORT_TOKEN,
                        ["capacity:execution:abort"],
                    ),
                    _principal(
                        "execution-activator",
                        EXECUTION_ACTIVATE_TOKEN,
                        ["capacity:execution:activate"],
                    ),
                    _principal(
                        "execution-drainer",
                        EXECUTION_DRAIN_TOKEN,
                        ["capacity:execution:drain"],
                    ),
                    _principal(
                        "execution-retirer",
                        EXECUTION_RETIRE_TOKEN,
                        ["capacity:execution:retire"],
                    ),
                    _principal(
                        "execution-reader",
                        EXECUTION_READ_TOKEN,
                        ["capacity:read"],
                    ),
                    *(
                        _principal(
                            f"{binding.pool_id}-v2-executor",
                            (
                                GB10_V2_EXECUTOR_TOKEN
                                if binding.pool_id == "gb10"
                                else OLDLAB_V2_EXECUTOR_TOKEN
                            ),
                            ["capacity:execute:pool"],
                            pool_id=binding.pool_id,
                            executor_id=binding.executor_id,
                            executor_incarnation=binding.executor_incarnation,
                            executor_pool_generation=binding.pool_generation,
                        )
                        for binding in policy.executors
                    ),
                ],
            }
        ),
    )
    policy_payload = canonical_executable_bytes(policy)
    policy_path = _owner_file(
        tmp_path / "execution-policy.json",
        policy_payload.decode("ascii"),
    )
    policy_sha256 = hashlib.sha256(policy_payload).hexdigest()
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=_owner_file(tmp_path / "execution-database-url", capacity_postgres_url),
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=_owner_file(tmp_path / "execution-server.crt", "test"),
        tls_key_file=_owner_file(tmp_path / "execution-server.key", "test"),
        tls_client_ca_file=_owner_file(tmp_path / "execution-client-ca.crt", "test"),
        execution_policy_file=policy_path,
        execution_policy_sha256=policy_sha256,
        freshness_seconds=120,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        request = fixture.request.model_copy(
            update={"expected_writer_epoch": app.state.writer.writer_epoch}
        )
        yield client, app, settings, request, policy
    await _reset_capacity_database(capacity_session_factory)


def _execution_registration(
    prepared: ExecutionContextV2,
    policy: ExecutionPreparationPolicyV2,
    *,
    pool_id: str,
) -> ExecutableExecutorRegistrationV2:
    binding = next(item for item in policy.executors if item.pool_id == pool_id)
    return ExecutableExecutorRegistrationV2(
        execution=prepared,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        signing_key_id=f"{binding.pool_id}-key",
        signing_key_sha256=binding.signing_key_sha256,
        local_authority_sha256=binding.local_authority_sha256,
        controller_authority_sha256=binding.controller_authority_sha256,
    )


def _v2_executor_headers(pool_id: str) -> dict[str, str]:
    token = GB10_V2_EXECUTOR_TOKEN if pool_id == "gb10" else OLDLAB_V2_EXECUTOR_TOKEN
    return {"Authorization": f"Bearer {token}"}


def _prepared_readiness_response(
    response,  # type: ignore[no-untyped-def]
) -> tuple[PreparedExecutionReadinessV2, str]:
    assert response.status_code == 200, response.text
    payload = response.json()
    readiness_sha256 = payload.pop("readiness_sha256")
    readiness = PreparedExecutionReadinessV2.model_validate_json(json.dumps(payload))
    assert readiness_sha256 == canonical_prepared_readiness_digest(readiness)
    return readiness, readiness_sha256


def _prepare_ready_execution(
    client: TestClient,
    request: ExecutionPreparationV2,
    policy: ExecutionPreparationPolicyV2,
    *,
    key_base: int,
) -> tuple[ExecutionContextV2, PreparedExecutionReadinessV2, str]:
    prepared_response = client.post(
        "/v2/execution-preparations",
        headers={
            "Authorization": f"Bearer {EXECUTION_PREPARE_TOKEN}",
            "Idempotency-Key": str(UUID(int=key_base)),
        },
        json=request.model_dump(mode="json"),
    )
    assert prepared_response.status_code == 200, prepared_response.text
    prepared = ExecutionContextV2.model_validate_json(prepared_response.content)
    for index, pool_id in enumerate(("gb10", "oldlab"), start=1):
        registration = _execution_registration(prepared, policy, pool_id=pool_id)
        headers = _v2_executor_headers(pool_id)
        registered = client.put(
            f"/v2/executors/{pool_id}/registration",
            headers=headers | {"Idempotency-Key": str(UUID(int=key_base + index))},
            json=registration.model_dump(mode="json"),
        )
        assert registered.status_code == 200, registered.text
        heartbeat = ExecutableExecutorHeartbeatV2(
            execution=prepared,
            executor_id=registration.executor_id,
            executor_incarnation=registration.executor_incarnation,
            pool_id=pool_id,
            pool_generation=registration.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        )
        first_heartbeat = client.put(
            f"/v2/executors/{pool_id}/heartbeat",
            headers=headers,
            json=heartbeat.model_dump(mode="json"),
        )
        assert first_heartbeat.status_code == 200, first_heartbeat.text
        inventory = ExecutableExecutorInventoryV2(
            execution=prepared,
            executor_id=registration.executor_id,
            executor_incarnation=registration.executor_incarnation,
            pool_id=pool_id,
            pool_generation=registration.pool_generation,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(),
        )
        inventoried = client.put(
            f"/v2/executors/{pool_id}/inventory",
            headers=headers,
            json=inventory.model_dump(mode="json"),
        )
        assert inventoried.status_code == 200, inventoried.text
        confirmation_sequence, confirmation_digest = (
            canonical_inventory_confirmation_journal_head(inventory)
        )
        confirmed = client.put(
            f"/v2/executors/{pool_id}/heartbeat",
            headers=headers,
            json=heartbeat.model_copy(
                update={
                    "heartbeat_sequence": 2,
                    "journal_sequence": confirmation_sequence,
                    "journal_digest": confirmation_digest,
                }
            ).model_dump(mode="json"),
        )
        assert confirmed.status_code == 200, confirmed.text
    readiness, readiness_sha256 = _prepared_readiness_response(
        client.get(
            "/v2/status/execution-preparation",
            headers={"Authorization": f"Bearer {EXECUTION_READ_TOKEN}"},
        )
    )
    assert readiness.ready is True
    assert readiness.execution == prepared
    return prepared, readiness, readiness_sha256


async def _store_activation_request(
    session: AsyncSession,
    fixture,  # type: ignore[no-untyped-def]
    prepared: ExecutionContextV2,
) -> ExecutionActivationV2:
    execution_store = CapacityExecutionStore()
    for binding in fixture.request.executors:
        common = {
            "execution": prepared,
            "executor_id": binding.executor_id,
            "executor_incarnation": binding.executor_incarnation,
            "pool_id": binding.pool_id,
            "pool_generation": binding.pool_generation,
        }
        heartbeat = ExecutableExecutorHeartbeatV2(
            **common,
            heartbeat_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        )
        await execution_store.heartbeat_executor(session, heartbeat)
        inventory = ExecutableExecutorInventoryV2(
            **common,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(),
        )
        await execution_store.ingest_executor_inventory(session, inventory)
        confirmation_sequence, confirmation_digest = (
            canonical_inventory_confirmation_journal_head(inventory)
        )
        await execution_store.heartbeat_executor(
            session,
            heartbeat.model_copy(
                update={
                    "heartbeat_sequence": 2,
                    "journal_sequence": confirmation_sequence,
                    "journal_digest": confirmation_digest,
                }
            ),
        )
    policy = fixture.store.execution_policy
    assert policy is not None
    readiness = await load_prepared_execution_readiness(
        session,
        execution_policy=policy,
        execution_policy_sha256=canonical_executable_digest(policy),
        freshness_seconds=120,
    )
    assert readiness.ready is True
    return ExecutionActivationV2(
        authority_incarnation=prepared.authority_incarnation,
        expected_writer_epoch=prepared.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        prepared_readiness_sha256=canonical_prepared_readiness_digest(readiness),
        executable_new_capacity_ceiling=fixture.request.requested_ceiling,
        executable_new_capacity_rate_per_minute=fixture.request.requested_rate_per_minute,
    )


@contextmanager
def hold_reconciliation_open(
    client: TestClient,
    allocator: BlockingAllocator,
    operator_headers: dict[str, str],
) -> Iterator[None]:
    responses: list[object] = []

    def trigger() -> None:
        responses.append(client.post("/v1/shadow-reconciliations", headers=operator_headers))

    thread = Thread(target=trigger, daemon=True)
    thread.start()
    assert allocator.started.wait(timeout=2)
    try:
        yield
    finally:
        allocator.release.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert responses and responses[0].status_code == 200  # type: ignore[union-attr]


def _assert_exact_approved_routes(app: FastAPI) -> None:
    routes = {(route.path, tuple(sorted(route.methods or ()))) for route in app.routes}
    assert routes == {
        ("/healthz", ("GET",)),
        ("/v1/config-proposals/fleet", ("PUT",)),
        ("/v1/config-proposals/subjects/{subject_id}", ("PUT",)),
        ("/v1/config-activations", ("POST",)),
        ("/v1/development-projections/{subject_id}", ("PUT",)),
        ("/v1/reports/demand/{subject_id}", ("PUT",)),
        (
            "/v1/reports/protected-releases/{subject_id}/{shape_instance_id}",
            ("PUT",),
        ),
        ("/v1/reports/pools/{pool_id}", ("PUT",)),
        ("/v1/executors/{pool_id}/registration", ("PUT",)),
        ("/v1/executors/{pool_id}/heartbeat", ("PUT",)),
        ("/v1/executors/{pool_id}/checkpoint", ("GET",)),
        ("/v1/executors/{pool_id}/inventory", ("PUT",)),
        ("/v1/grants/reservations/{tranche_id}", ("PUT",)),
        (
            "/v1/executors/{pool_id}/reservations/{tranche_id}/accept",
            ("POST",),
        ),
        ("/v1/executors/{pool_id}/intents/{intent_id}/bootstrap", ("POST",)),
        ("/v1/grants/launch-permits/{permit_id}", ("PUT",)),
        ("/v1/executors/{pool_id}/permits/{permit_id}/consume", ("POST",)),
        ("/v1/executors/{pool_id}/intents/{intent_id}/close", ("POST",)),
        (
            "/v1/executors/{pool_id}/reservations/{tranche_id}/release",
            ("POST",),
        ),
        ("/v2/execution-preparations", ("POST",)),
        ("/v2/executors/{pool_id}/registration", ("PUT",)),
        ("/v2/execution-preparations/{execution_epoch}/abort", ("POST",)),
        ("/v2/execution-preparations/{execution_epoch}/activate", ("POST",)),
        ("/v2/execution-epochs/{execution_epoch}/drain", ("POST",)),
        ("/v2/execution-epochs/{execution_epoch}/retire", ("POST",)),
        ("/v2/executors/{pool_id}/heartbeat", ("PUT",)),
        ("/v2/executors/{pool_id}/checkpoint", ("GET",)),
        ("/v2/executors/{pool_id}/context", ("GET",)),
        ("/v2/executors/{pool_id}/work", ("GET",)),
        ("/v2/executors/{pool_id}/inventory", ("PUT",)),
        (
            "/v2/executors/{pool_id}/reservations/{tranche_id}/accept",
            ("POST",),
        ),
        (
            "/v2/executors/{pool_id}/intents/{intent_id}/bootstrap-proposals",
            ("POST",),
        ),
        ("/v2/subjects/{subject_id}/bootstrap-work", ("GET",)),
        ("/v2/subjects/{subject_id}/admission-work", ("GET",)),
        (
            "/v2/subjects/{subject_id}/intents/{intent_id}/bootstrap-acknowledgements",
            ("PUT",),
        ),
        (
            "/v2/subjects/{subject_id}/admission-acknowledgements/{proposal_id}",
            ("PUT",),
        ),
        (
            "/v2/subjects/{subject_id}/admission-closures/{closure_id}/acknowledgements",
            ("PUT",),
        ),
        ("/v2/executors/{pool_id}/permits/{permit_id}/consume", ("POST",)),
        ("/v2/executors/{pool_id}/permits/{permit_id}/recover", ("POST",)),
        ("/v2/executors/{pool_id}/intents/{intent_id}/close", ("POST",)),
        (
            "/v2/executors/{pool_id}/reservations/{tranche_id}/release",
            ("POST",),
        ),
        (
            "/v2/reports/protected-releases/{subject_id}/{shape_instance_id}",
            ("PUT",),
        ),
        ("/v1/shadow-reconciliations", ("POST",)),
        ("/v1/status", ("GET",)),
        ("/v1/status/subjects", ("GET",)),
        ("/v1/status/pools", ("GET",)),
        ("/v1/status/executors", ("GET",)),
        ("/v1/status/reservations", ("GET",)),
        ("/v2/status/execution-preparation", ("GET",)),
        ("/v2/status/executors", ("GET",)),
        ("/v2/status/subjects/{subject_id}", ("GET",)),
        ("/v1/shadow-epochs/{allocation_epoch}", ("GET",)),
        ("/v1/shadow-epochs/{allocation_epoch}/allocations", ("GET",)),
        ("/v1/audit-events", ("GET",)),
        ("/metrics", ("GET",)),
    }


def test_shadow_api_exposes_exactly_the_approved_routes(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, app, _settings, _allocator = api_context
    _assert_exact_approved_routes(app)


def test_execution_preparation_and_registration_routes_are_exact_and_replayable(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
) -> None:
    """Protected routes reject identity drift and preserve exact retries."""

    client, _app, _settings, request, policy = execution_preparation_api_context
    prepare_path = "/v2/execution-preparations"
    prepare_headers = {"Authorization": f"Bearer {EXECUTION_PREPARE_TOKEN}"}
    preparation_key = UUID(int=15001)

    assert client.post(prepare_path, json=request.model_dump(mode="json")).status_code == 401
    assert (
        client.post(
            prepare_path,
            headers={"Authorization": "Bearer invalid"},
            json=request.model_dump(mode="json"),
        ).status_code
        == 401
    )
    assert (
        client.post(
            prepare_path,
            headers={"Authorization": f"Bearer {EXECUTION_READ_TOKEN}"},
            json=request.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.post(
            prepare_path,
            headers=prepare_headers,
            json=request.model_dump(mode="json"),
        ).status_code
        == 422
    )
    assert (
        client.post(
            prepare_path,
            headers=prepare_headers | {"Idempotency-Key": "not-a-uuid"},
            json=request.model_dump(mode="json"),
        ).status_code
        == 422
    )
    invalid_contract = client.post(
        prepare_path,
        headers=prepare_headers | {"Idempotency-Key": str(UUID(int=14999))},
        json={},
    )
    assert invalid_contract.status_code == 422
    assert invalid_contract.json() == {"detail": "invalid capacity contract"}
    assert (
        client.post(
            prepare_path,
            headers=prepare_headers | {"Idempotency-Key": str(UUID(int=15000))},
            content=b"x" * (MAX_CONTRACT_BYTES + 1),
        ).status_code
        == 413
    )

    prepared_response = client.post(
        prepare_path,
        headers=prepare_headers | {"Idempotency-Key": str(preparation_key)},
        json=request.model_dump(mode="json"),
    )
    assert prepared_response.status_code == 200, prepared_response.text
    prepared = ExecutionContextV2.model_validate_json(prepared_response.content)
    assert prepared.execution_state == "prepared"
    assert prepared.executable_new_capacity_ceiling == 0
    assert prepared.executable_new_capacity_rate_per_minute == 0
    replay = client.post(
        prepare_path,
        headers=prepare_headers | {"Idempotency-Key": str(preparation_key)},
        json=request.model_dump(mode="json"),
    )
    assert replay.status_code == 200
    assert replay.json() == prepared_response.json()
    conflict = client.post(
        prepare_path,
        headers=prepare_headers | {"Idempotency-Key": str(preparation_key)},
        json=request.model_copy(update={"rollback_evidence_sha256": "f" * 64}).model_dump(
            mode="json"
        ),
    )
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "capacity state conflict"}

    registration = _execution_registration(prepared, policy, pool_id="gb10")
    registration_path = "/v2/executors/gb10/registration"
    executor_headers = _v2_executor_headers("gb10")
    registration_key = UUID(int=15002)
    assert (
        client.put(
            registration_path,
            headers=executor_headers,
            json=registration.model_dump(mode="json"),
        ).status_code
        == 422
    )
    assert (
        client.put(
            registration_path,
            headers=prepare_headers | {"Idempotency-Key": str(UUID(int=15003))},
            json=registration.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.put(
            "/v2/executors/oldlab/registration",
            headers=executor_headers | {"Idempotency-Key": str(UUID(int=15004))},
            json=registration.model_dump(mode="json"),
        ).status_code
        == 403
    )
    for index, change in enumerate(
        (
            {"pool_id": "oldlab"},
            {"executor_id": "other-executor"},
            {"executor_incarnation": UUID(int=15005)},
            {"pool_generation": 2},
        ),
        start=15006,
    ):
        rejected = client.put(
            registration_path,
            headers=executor_headers | {"Idempotency-Key": str(UUID(int=index))},
            json=registration.model_copy(update=change).model_dump(mode="json"),
        )
        assert rejected.status_code == 403
        assert rejected.json() == {"detail": "forbidden"}

    registered = client.put(
        registration_path,
        headers=executor_headers | {"Idempotency-Key": str(registration_key)},
        json=registration.model_dump(mode="json"),
    )
    assert registered.status_code == 200, registered.text
    assert registered.json() == prepared.model_dump(mode="json")
    registration_replay = client.put(
        registration_path,
        headers=executor_headers | {"Idempotency-Key": str(registration_key)},
        json=registration.model_dump(mode="json"),
    )
    assert registration_replay.status_code == 200
    assert registration_replay.json() == registered.json()
    reused = client.put(
        registration_path,
        headers=executor_headers | {"Idempotency-Key": str(registration_key)},
        json=registration.model_copy(update={"signing_key_id": "changed-key"}).model_dump(
            mode="json"
        ),
    )
    assert reused.status_code == 409
    assert reused.json() == {"detail": "capacity state conflict"}


def test_execution_preparation_abort_is_exact_and_replayable(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
) -> None:
    """Abort is separately scoped, exactly fenced, replayable, and zero-only."""

    client, app, settings, request, _policy = execution_preparation_api_context
    prepare_headers = {
        "Authorization": f"Bearer {EXECUTION_PREPARE_TOKEN}",
        "Idempotency-Key": str(UUID(int=15100)),
    }
    prepared_response = client.post(
        "/v2/execution-preparations",
        headers=prepare_headers,
        json=request.model_dump(mode="json"),
    )
    assert prepared_response.status_code == 200, prepared_response.text
    prepared = ExecutionContextV2.model_validate_json(prepared_response.content)
    abort = ExecutionPreparationAbortV2(
        authority_incarnation=prepared.authority_incarnation,
        expected_writer_epoch=prepared.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
    )
    abort_path = f"/v2/execution-preparations/{prepared.execution_epoch}/abort"
    abort_headers = {"Authorization": f"Bearer {EXECUTION_ABORT_TOKEN}"}
    abort_key = UUID(int=15101)

    assert (
        client.post(
            abort_path,
            headers={"Authorization": f"Bearer {EXECUTION_PREPARE_TOKEN}"},
            json=abort.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.post(
            abort_path,
            headers=abort_headers,
            json=abort.model_dump(mode="json"),
        ).status_code
        == 422
    )
    crossed = client.post(
        f"/v2/execution-preparations/{prepared.execution_epoch + 1}/abort",
        headers=abort_headers | {"Idempotency-Key": str(UUID(int=15102))},
        json=abort.model_dump(mode="json"),
    )
    assert crossed.status_code == 403
    assert crossed.json() == {"detail": "forbidden"}

    retired = client.post(
        abort_path,
        headers=abort_headers | {"Idempotency-Key": str(abort_key)},
        json=abort.model_dump(mode="json"),
    )
    assert retired.status_code == 200, retired.text
    assert retired.json()["execution_epoch"] == prepared.execution_epoch
    assert retired.json()["execution_manifest_sha256"] == prepared.execution_manifest_sha256
    assert retired.json()["replayed"] is False
    assert app.state.writer == WriterFence(
        authority_incarnation=prepared.authority_incarnation,
        writer_epoch=prepared.writer_epoch + 1,
    )
    assert client.get("/healthz").json() == {
        "status": "ready",
        "executable_new_capacity_ceiling": 0,
    }
    replay = client.post(
        abort_path,
        headers=abort_headers | {"Idempotency-Key": str(abort_key)},
        json=abort.model_dump(mode="json"),
    )
    assert replay.status_code == 200
    assert replay.json() == retired.json() | {"replayed": True}
    changed = abort.model_copy(update={"execution_manifest_sha256": "f" * 64})
    conflict = client.post(
        abort_path,
        headers=abort_headers | {"Idempotency-Key": str(abort_key)},
        json=changed.model_dump(mode="json"),
    )
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "capacity state conflict"}

    for forbidden_path in (
        "/v2/execution-activations",
        "/v2/execution-drains",
        "/v2/execution-retirements",
        "/v2/execution-transitions",
    ):
        response = client.post(
            forbidden_path,
            headers=abort_headers | {"Idempotency-Key": str(UUID(int=15103))},
            json={},
        )
        assert response.status_code == 404

    replacement_app = create_app(settings)
    with TestClient(replacement_app) as replacement_client:
        replacement_writer = replacement_app.state.writer
        assert replacement_writer.writer_epoch == prepared.writer_epoch + 2
        replacement_replay = replacement_client.post(
            abort_path,
            headers=abort_headers | {"Idempotency-Key": str(abort_key)},
            json=abort.model_dump(mode="json"),
        )
        assert replacement_replay.status_code == 200
        assert replacement_replay.json() == retired.json() | {"replayed": True}
        assert replacement_app.state.writer == replacement_writer
        assert replacement_client.get("/healthz").status_code == 200


def test_execution_activation_route_is_least_scope_atomic_and_exactly_replayable(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
) -> None:
    """Only the scoped operator can activate the exact readiness digest once."""

    client, _app, _settings, request, policy = execution_preparation_api_context
    prepared, _readiness, readiness_sha256 = _prepare_ready_execution(
        client,
        request,
        policy,
        key_base=15120,
    )
    activation = ExecutionActivationV2(
        authority_incarnation=prepared.authority_incarnation,
        expected_writer_epoch=prepared.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        prepared_readiness_sha256=readiness_sha256,
        executable_new_capacity_ceiling=request.requested_ceiling,
        executable_new_capacity_rate_per_minute=request.requested_rate_per_minute,
    )
    path = f"/v2/execution-preparations/{prepared.execution_epoch}/activate"
    headers = {"Authorization": f"Bearer {EXECUTION_ACTIVATE_TOKEN}"}
    activation_key = UUID(int=15123)

    assert client.post(path, json=activation.model_dump(mode="json")).status_code == 401
    assert (
        client.post(
            path,
            headers={"Authorization": f"Bearer {EXECUTION_READ_TOKEN}"},
            json=activation.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.post(path, headers=headers, json=activation.model_dump(mode="json")).status_code
        == 422
    )
    malformed = client.post(
        path,
        headers=headers | {"Idempotency-Key": str(UUID(int=15124))},
        json={},
    )
    assert malformed.status_code == 422
    assert malformed.json() == {"detail": "invalid capacity contract"}
    crossed = client.post(
        f"/v2/execution-preparations/{prepared.execution_epoch + 1}/activate",
        headers=headers | {"Idempotency-Key": str(UUID(int=15125))},
        json=activation.model_dump(mode="json"),
    )
    assert crossed.status_code == 403
    assert crossed.json() == {"detail": "forbidden"}

    stale_digest = client.post(
        path,
        headers=headers | {"Idempotency-Key": str(UUID(int=15127))},
        json=activation.model_copy(
            update={"prepared_readiness_sha256": "f" * 64}
        ).model_dump(mode="json"),
    )
    assert stale_digest.status_code == 409
    assert stale_digest.json() == {"detail": "capacity state conflict"}
    premature_drain = ExecutionDrainV2(
        authority_incarnation=prepared.authority_incarnation,
        expected_writer_epoch=prepared.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        expected_executable_new_capacity_ceiling=request.requested_ceiling,
        expected_executable_new_capacity_rate_per_minute=request.requested_rate_per_minute,
    )
    rejected_drain = client.post(
        f"/v2/execution-epochs/{prepared.execution_epoch}/drain",
        headers={
            "Authorization": f"Bearer {EXECUTION_DRAIN_TOKEN}",
            "Idempotency-Key": str(UUID(int=15128)),
        },
        json=premature_drain.model_dump(mode="json"),
    )
    assert rejected_drain.status_code == 409
    assert rejected_drain.json() == {"detail": "capacity state conflict"}

    activated = client.post(
        path,
        headers=headers | {"Idempotency-Key": str(activation_key)},
        json=activation.model_dump(mode="json"),
    )
    assert activated.status_code == 200, activated.text
    active = ExecutionAuthorityV2.model_validate_json(activated.content)
    assert active.execution_state == "active"
    assert active.executable_new_capacity_ceiling == request.requested_ceiling
    replay = client.post(
        path,
        headers=headers | {"Idempotency-Key": str(activation_key)},
        json=activation.model_dump(mode="json"),
    )
    assert replay.status_code == 200
    assert replay.json() == activated.json()
    changed = client.post(
        path,
        headers=headers | {"Idempotency-Key": str(activation_key)},
        json=activation.model_copy(
            update={"prepared_readiness_sha256": "f" * 64}
        ).model_dump(mode="json"),
    )
    assert changed.status_code == 409
    assert changed.json() == {"detail": "capacity state conflict"}

    abort = ExecutionPreparationAbortV2(
        authority_incarnation=prepared.authority_incarnation,
        expected_writer_epoch=prepared.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
    )
    opposite = client.post(
        f"/v2/execution-preparations/{prepared.execution_epoch}/abort",
        headers={
            "Authorization": f"Bearer {EXECUTION_ABORT_TOKEN}",
            "Idempotency-Key": str(UUID(int=15126)),
        },
        json=abort.model_dump(mode="json"),
    )
    assert opposite.status_code == 409
    assert opposite.json() == {"detail": "capacity state conflict"}


def test_execution_drain_and_retirement_routes_require_exact_final_evidence(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
) -> None:
    """Drain is immediate; retirement waits for both exact final pool receipts."""

    client, app, _settings, request, policy = execution_preparation_api_context
    prepared, readiness, readiness_sha256 = _prepare_ready_execution(
        client,
        request,
        policy,
        key_base=15140,
    )
    activation = ExecutionActivationV2(
        authority_incarnation=prepared.authority_incarnation,
        expected_writer_epoch=prepared.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        prepared_readiness_sha256=readiness_sha256,
        executable_new_capacity_ceiling=request.requested_ceiling,
        executable_new_capacity_rate_per_minute=request.requested_rate_per_minute,
    )
    activated = client.post(
        f"/v2/execution-preparations/{prepared.execution_epoch}/activate",
        headers={
            "Authorization": f"Bearer {EXECUTION_ACTIVATE_TOKEN}",
            "Idempotency-Key": str(UUID(int=15143)),
        },
        json=activation.model_dump(mode="json"),
    )
    assert activated.status_code == 200, activated.text
    active = ExecutionAuthorityV2.model_validate_json(activated.content)
    drain = ExecutionDrainV2(
        authority_incarnation=active.authority_incarnation,
        expected_writer_epoch=active.writer_epoch,
        execution_epoch=active.execution_epoch,
        execution_manifest_sha256=active.execution_manifest_sha256,
        expected_executable_new_capacity_ceiling=(active.executable_new_capacity_ceiling),
        expected_executable_new_capacity_rate_per_minute=(
            active.executable_new_capacity_rate_per_minute
        ),
    )
    drain_path = f"/v2/execution-epochs/{active.execution_epoch}/drain"
    drain_headers = {"Authorization": f"Bearer {EXECUTION_DRAIN_TOKEN}"}
    drain_key = UUID(int=15144)

    assert client.post(drain_path, json=drain.model_dump(mode="json")).status_code == 401
    assert (
        client.post(
            drain_path,
            headers={"Authorization": f"Bearer {EXECUTION_ACTIVATE_TOKEN}"},
            json=drain.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.post(drain_path, headers=drain_headers, json=drain.model_dump(mode="json"))
        .status_code
        == 422
    )
    malformed_drain = client.post(
        drain_path,
        headers=drain_headers | {"Idempotency-Key": str(UUID(int=15145))},
        json={},
    )
    assert malformed_drain.status_code == 422
    assert malformed_drain.json() == {"detail": "invalid capacity contract"}
    crossed_drain = client.post(
        f"/v2/execution-epochs/{active.execution_epoch + 1}/drain",
        headers=drain_headers | {"Idempotency-Key": str(UUID(int=15146))},
        json=drain.model_dump(mode="json"),
    )
    assert crossed_drain.status_code == 403
    assert crossed_drain.json() == {"detail": "forbidden"}

    drained_response = client.post(
        drain_path,
        headers=drain_headers | {"Idempotency-Key": str(drain_key)},
        json=drain.model_dump(mode="json"),
    )
    assert drained_response.status_code == 200, drained_response.text
    drained = ExecutionAuthorityV2.model_validate_json(drained_response.content)
    assert drained.execution_state == "drain-only"
    assert drained.executable_new_capacity_ceiling == 0
    assert drained.executable_new_capacity_rate_per_minute == 0
    drain_replay = client.post(
        drain_path,
        headers=drain_headers | {"Idempotency-Key": str(drain_key)},
        json=drain.model_dump(mode="json"),
    )
    assert drain_replay.status_code == 200
    assert drain_replay.json() == drained_response.json()
    changed_drain = client.post(
        drain_path,
        headers=drain_headers | {"Idempotency-Key": str(drain_key)},
        json=drain.model_copy(
            update={
                "expected_executable_new_capacity_ceiling": (
                    drain.expected_executable_new_capacity_ceiling + 1
                )
            }
        ).model_dump(mode="json"),
    )
    assert changed_drain.status_code == 409
    assert changed_drain.json() == {"detail": "capacity state conflict"}

    unsafe_checkpoints = tuple(
        ExecutionRetirementExecutorCheckpointV2(
            executor_id=item.expected_executor_id,
            executor_incarnation=item.expected_executor_incarnation,
            pool_id=item.pool_id,
            pool_generation=item.expected_pool_generation,
            heartbeat_sequence=item.heartbeat_sequence + 1,
            command_sequence=0,
            journal_sequence=item.journal_sequence,
            journal_digest=item.journal_digest,
            inventory_sequence=item.inventory_sequence,
            inventory_digest=item.inventory_digest or "0" * 64,
        )
        for item in readiness.executors
    )
    unsafe_retirement = ExecutionRetirementV2(
        authority_incarnation=drained.authority_incarnation,
        expected_writer_epoch=drained.writer_epoch,
        execution_epoch=drained.execution_epoch,
        execution_manifest_sha256=drained.execution_manifest_sha256,
        executor_checkpoints=unsafe_checkpoints,
    )
    retire_path = f"/v2/execution-epochs/{drained.execution_epoch}/retire"
    retire_headers = {"Authorization": f"Bearer {EXECUTION_RETIRE_TOKEN}"}
    too_early = client.post(
        retire_path,
        headers=retire_headers | {"Idempotency-Key": str(UUID(int=15147))},
        json=unsafe_retirement.model_dump(mode="json"),
    )
    assert too_early.status_code == 409
    assert too_early.json() == {"detail": "capacity state conflict"}

    readiness_by_pool = {item.pool_id: item for item in readiness.executors}
    for pool_id in ("gb10", "oldlab"):
        item = readiness_by_pool[pool_id]
        headers = _v2_executor_headers(pool_id)
        final_inventory = ExecutableExecutorInventoryV2(
            execution=active,
            executor_id=item.expected_executor_id,
            executor_incarnation=item.expected_executor_incarnation,
            pool_id=pool_id,
            pool_generation=item.expected_pool_generation,
            inventory_sequence=item.inventory_sequence + 1,
            journal_sequence=item.journal_sequence,
            journal_digest=item.journal_digest,
            journal_checkpoint_sequence=item.journal_sequence,
            journal_checkpoint_digest=item.journal_digest,
            records=(),
        )
        inventoried = client.put(
            f"/v2/executors/{pool_id}/inventory",
            headers=headers,
            json=final_inventory.model_dump(mode="json"),
        )
        assert inventoried.status_code == 200, inventoried.text
        confirmation_sequence, confirmation_digest = (
            canonical_inventory_confirmation_journal_head(final_inventory)
        )
        final_heartbeat = ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=item.expected_executor_id,
            executor_incarnation=item.expected_executor_incarnation,
            pool_id=pool_id,
            pool_generation=item.expected_pool_generation,
            heartbeat_sequence=item.heartbeat_sequence + 1,
            journal_sequence=confirmation_sequence,
            journal_digest=confirmation_digest,
            journal_checkpoint_sequence=item.journal_sequence,
            journal_checkpoint_digest=item.journal_digest,
        )
        heartbeated = client.put(
            f"/v2/executors/{pool_id}/heartbeat",
            headers=headers,
            json=final_heartbeat.model_dump(mode="json"),
        )
        assert heartbeated.status_code == 200, heartbeated.text

    executor_status = client.get(
        "/v2/status/executors",
        headers={"Authorization": f"Bearer {EXECUTION_READ_TOKEN}"},
    )
    assert executor_status.status_code == 200, executor_status.text
    items = executor_status.json()["items"]
    assert [item["pool_id"] for item in items] == ["gb10", "oldlab"]
    assert all(item["retirement_safe"] is True for item in items)
    final_checkpoints = tuple(
            ExecutionRetirementExecutorCheckpointV2(
                executor_id=item["executor_id"],
                executor_incarnation=UUID(item["executor_incarnation"]),
            pool_id=item["pool_id"],
            pool_generation=item["pool_generation"],
            heartbeat_sequence=item["heartbeat_sequence"],
            command_sequence=item["command_sequence"],
            journal_sequence=item["journal_sequence"],
            journal_digest=item["journal_digest"],
            inventory_sequence=item["inventory_sequence"],
            inventory_digest=item["inventory_digest"],
        )
        for item in items
    )
    retirement = unsafe_retirement.model_copy(
        update={"executor_checkpoints": final_checkpoints}
    )
    assert client.post(retire_path, json=retirement.model_dump(mode="json")).status_code == 401
    assert (
        client.post(
            retire_path,
            headers={"Authorization": f"Bearer {EXECUTION_DRAIN_TOKEN}"},
            json=retirement.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.post(
            retire_path,
            headers=retire_headers,
            json=retirement.model_dump(mode="json"),
        ).status_code
        == 422
    )
    malformed_retirement = client.post(
        retire_path,
        headers=retire_headers | {"Idempotency-Key": str(UUID(int=15148))},
        json={},
    )
    assert malformed_retirement.status_code == 422
    assert malformed_retirement.json() == {"detail": "invalid capacity contract"}
    crossed_retirement = client.post(
        f"/v2/execution-epochs/{drained.execution_epoch + 1}/retire",
        headers=retire_headers | {"Idempotency-Key": str(UUID(int=15149))},
        json=retirement.model_dump(mode="json"),
    )
    assert crossed_retirement.status_code == 403
    assert crossed_retirement.json() == {"detail": "forbidden"}

    retirement_key = UUID(int=15150)
    retired = client.post(
        retire_path,
        headers=retire_headers | {"Idempotency-Key": str(retirement_key)},
        json=retirement.model_dump(mode="json"),
    )
    assert retired.status_code == 200, retired.text
    assert retired.json()["execution_epoch"] == drained.execution_epoch
    assert retired.json()["replayed"] is False
    assert app.state.writer == WriterFence(
        authority_incarnation=drained.authority_incarnation,
        writer_epoch=drained.writer_epoch,
    )
    retirement_replay = client.post(
        retire_path,
        headers=retire_headers | {"Idempotency-Key": str(retirement_key)},
        json=retirement.model_dump(mode="json"),
    )
    assert retirement_replay.status_code == 200
    assert retirement_replay.json() == retired.json() | {"replayed": True}
    changed_retirement = client.post(
        retire_path,
        headers=retire_headers | {"Idempotency-Key": str(retirement_key)},
        json=retirement.model_copy(
            update={
                "executor_checkpoints": (
                    final_checkpoints[0].model_copy(
                        update={
                            "heartbeat_sequence": (
                                final_checkpoints[0].heartbeat_sequence + 1
                            )
                        }
                    ),
                    final_checkpoints[1],
                )
            }
        ).model_dump(mode="json"),
    )
    assert changed_retirement.status_code == 409
    assert changed_retirement.json() == {"detail": "capacity state conflict"}


def test_execution_preparation_status_requires_complete_fresh_two_pool_evidence(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
) -> None:
    """Readiness exposes bounded progress and becomes true only after confirmation."""

    client, _app, settings, request, policy = execution_preparation_api_context
    status_path = "/v2/status/execution-preparation"
    read_headers = {"Authorization": f"Bearer {EXECUTION_READ_TOKEN}"}
    assert client.get(status_path).status_code == 401
    assert client.get(status_path, headers=_v2_executor_headers("gb10")).status_code == 403

    shadow_response = client.get(status_path, headers=read_headers)
    shadow, shadow_digest = _prepared_readiness_response(shadow_response)
    assert shadow.ready is False
    assert len(shadow_digest) == 64
    assert shadow.policy_sha256 == settings.execution_policy_sha256
    assert shadow.blockers == ("manager-shadow",)

    prepared_response = client.post(
        "/v2/execution-preparations",
        headers={
            "Authorization": f"Bearer {EXECUTION_PREPARE_TOKEN}",
            "Idempotency-Key": str(UUID(int=15200)),
        },
        json=request.model_dump(mode="json"),
    )
    assert prepared_response.status_code == 200, prepared_response.text
    prepared = ExecutionContextV2.model_validate_json(prepared_response.content)
    incomplete, _incomplete_digest = _prepared_readiness_response(
        client.get(status_path, headers=read_headers)
    )
    assert incomplete.ready is False
    assert incomplete.blockers == ("executor-registration-missing",)

    registrations = {
        pool_id: _execution_registration(prepared, policy, pool_id=pool_id)
        for pool_id in ("gb10", "oldlab")
    }
    for index, (pool_id, registration) in enumerate(registrations.items(), start=15201):
        response = client.put(
            f"/v2/executors/{pool_id}/registration",
            headers=_v2_executor_headers(pool_id) | {"Idempotency-Key": str(UUID(int=index))},
            json=registration.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
    registered, _registered_digest = _prepared_readiness_response(
        client.get(status_path, headers=read_headers)
    )
    assert registered.ready is False
    assert registered.blockers == ("executor-inventory-missing", "executor-lease-expired")

    for pool_id, registration in registrations.items():
        headers = _v2_executor_headers(pool_id)
        heartbeat = ExecutableExecutorHeartbeatV2(
            execution=prepared,
            executor_id=registration.executor_id,
            executor_incarnation=registration.executor_incarnation,
            pool_id=pool_id,
            pool_generation=registration.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        )
        response = client.put(
            f"/v2/executors/{pool_id}/heartbeat",
            headers=headers,
            json=heartbeat.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        inventory = ExecutableExecutorInventoryV2(
            execution=prepared,
            executor_id=registration.executor_id,
            executor_incarnation=registration.executor_incarnation,
            pool_id=pool_id,
            pool_generation=registration.pool_generation,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(),
        )
        response = client.put(
            f"/v2/executors/{pool_id}/inventory",
            headers=headers,
            json=inventory.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        confirmation_sequence, confirmation_digest = canonical_inventory_confirmation_journal_head(
            inventory
        )
        confirmed = heartbeat.model_copy(
            update={
                "heartbeat_sequence": 2,
                "journal_sequence": confirmation_sequence,
                "journal_digest": confirmation_digest,
            }
        )
        response = client.put(
            f"/v2/executors/{pool_id}/heartbeat",
            headers=headers,
            json=confirmed.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text

    ready_response = client.get(status_path, headers=read_headers)
    ready, ready_digest = _prepared_readiness_response(ready_response)
    assert ready.ready is True
    assert ready_digest != "0" * 64
    assert ready.blockers == ()
    assert ready.execution == prepared
    assert tuple(item.pool_id for item in ready.executors) == ("gb10", "oldlab")
    assert ready.executable is False


def test_execution_preparation_status_reports_disabled_policy(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    """The default shadow deployment remains healthy but preparation-disabled."""

    client, _app, settings, _allocator = api_context
    response = client.get(
        "/v2/status/execution-preparation",
        headers=operator_headers,
    )

    assert response.status_code == 200
    readiness, readiness_digest = _prepared_readiness_response(response)
    assert settings.execution_policy_file is None
    assert readiness_digest == canonical_prepared_readiness_digest(readiness)
    assert readiness.ready is False
    assert readiness.policy_mode == "disabled"
    assert readiness.policy_sha256 is None
    assert readiness.blockers == ("execution-policy-disabled", "manager-shadow")


def test_execution_preparation_status_rejects_unverified_injected_policy_digest(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
) -> None:
    """Settings cannot lend raw-file provenance to another injected policy."""

    _client, _app, settings, _request, policy = execution_preparation_api_context
    injected_store = CapacityManagementStore(
        execution_policy=policy.model_copy(update={"rollback_evidence_sha256": "f" * 64})
    )
    verifier = StaticPrincipalVerifier(
        CapacityPrincipal(
            principal_id="injected-reader",
            scopes=frozenset({"capacity:read"}),
            subject_id=None,
            subject_incarnation=None,
            demand_reporter_incarnation=None,
            pool_id=None,
            pool_reporter_incarnation=None,
            executor_id=None,
            executor_incarnation=None,
            executor_pool_generation=None,
        )
    )
    app = create_app(
        settings,
        verifier=verifier,
        management_store=injected_store,
    )
    with TestClient(app) as client:
        response = client.get(
            "/v2/status/execution-preparation",
            headers={"Authorization": "Bearer injected"},
        )

    assert response.status_code == 503
    assert response.json() == {"detail": "execution preparation status unavailable"}


def test_execution_transition_route_rejects_an_injected_bound_principal(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
) -> None:
    """Route authorization remains safe if a verifier bypasses registry validation."""

    _client, _app, settings, request, _policy = execution_preparation_api_context
    verifier = StaticPrincipalVerifier(
        CapacityPrincipal(
            principal_id="injected-subject-preparer",
            scopes=frozenset({"capacity:execution:prepare"}),
            subject_id=SUBJECT_ID,
            subject_incarnation=SUBJECT_INCARNATION,
            demand_reporter_incarnation=DEMAND_REPORTER_ID,
            pool_id=None,
            pool_reporter_incarnation=None,
            executor_id=None,
            executor_incarnation=None,
            executor_pool_generation=None,
        )
    )
    app = create_app(settings, verifier=verifier)
    with TestClient(app) as client:
        response = client.post(
            "/v2/execution-preparations",
            headers={
                "Authorization": "Bearer injected",
                "Idempotency-Key": str(UUID(int=15300)),
            },
            json=request.model_copy(
                update={"expected_writer_epoch": app.state.writer.writer_epoch}
            ).model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


@pytest.mark.parametrize(
    ("scope", "route_kind"),
    (
        ("capacity:execution:activate", "activate"),
        ("capacity:execution:drain", "drain"),
        ("capacity:execution:retire", "retire"),
    ),
)
def test_active_execution_transition_routes_reject_injected_bound_principals(
    execution_preparation_api_context: tuple[
        TestClient,
        FastAPI,
        CapacityManagerSettings,
        ExecutionPreparationV2,
        ExecutionPreparationPolicyV2,
    ],
    scope: str,
    route_kind: str,
) -> None:
    """Even a scoped verifier result cannot attach a transition to a subject."""

    _client, _app, settings, request, policy = execution_preparation_api_context
    verifier = StaticPrincipalVerifier(
        CapacityPrincipal(
            principal_id=f"injected-subject-{route_kind}",
            scopes=frozenset({scope}),  # type: ignore[arg-type]
            subject_id=SUBJECT_ID,
            subject_incarnation=SUBJECT_INCARNATION,
            demand_reporter_incarnation=DEMAND_REPORTER_ID,
            pool_id=None,
            pool_reporter_incarnation=None,
            executor_id=None,
            executor_incarnation=None,
            executor_pool_generation=None,
        )
    )
    app = create_app(settings, verifier=verifier)
    with TestClient(app) as client:
        writer_epoch = app.state.writer.writer_epoch
        common = {
            "authority_incarnation": request.authority_incarnation,
            "expected_writer_epoch": writer_epoch,
            "execution_epoch": 1,
            "execution_manifest_sha256": "1" * 64,
        }
        if route_kind == "activate":
            value = ExecutionActivationV2(
                **common,
                prepared_readiness_sha256="2" * 64,
                executable_new_capacity_ceiling=1,
                executable_new_capacity_rate_per_minute=1,
            )
            path = "/v2/execution-preparations/1/activate"
        elif route_kind == "drain":
            value = ExecutionDrainV2(
                **common,
                expected_executable_new_capacity_ceiling=1,
                expected_executable_new_capacity_rate_per_minute=1,
            )
            path = "/v2/execution-epochs/1/drain"
        else:
            checkpoints = tuple(
                ExecutionRetirementExecutorCheckpointV2(
                    executor_id=binding.executor_id,
                    executor_incarnation=binding.executor_incarnation,
                    pool_id=binding.pool_id,
                    pool_generation=binding.pool_generation,
                    heartbeat_sequence=1,
                    command_sequence=0,
                    journal_sequence=0,
                    journal_digest="0" * 64,
                    inventory_sequence=1,
                    inventory_digest="2" * 64,
                )
                for binding in policy.executors
            )
            value = ExecutionRetirementV2(
                **common,
                executor_checkpoints=checkpoints,
            )
            path = "/v2/execution-epochs/1/retire"
        response = client.post(
            path,
            headers={
                "Authorization": "Bearer injected",
                "Idempotency-Key": str(UUID(int=15301)),
            },
            json=value.model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_executable_status_is_read_only_and_shadow_is_never_worker_available(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context

    executors = client.get("/v2/status/executors", headers=operator_headers)
    subject = client.get(f"/v2/status/subjects/{SUBJECT_ID}", headers=operator_headers)

    assert executors.status_code == 200
    assert executors.json() == {
        "schema_version": 2,
        "execution_epoch": 0,
        "execution_state": "shadow",
        "executable_new_capacity_ceiling": 0,
        "items": [],
        "blockers": ["manager-shadow"],
    }
    assert subject.status_code == 200
    assert subject.json() == {
        "schema_version": 2,
        "subject_id": str(SUBJECT_ID),
        "subject_incarnation": str(SUBJECT_INCARNATION),
        "deployment_generation": 1,
        "configuration_epoch": 1,
        "execution_epoch": 0,
        "execution_state": "shadow",
        "executable_new_capacity_ceiling": 0,
        "capacity_prepared": True,
        "capacity_status": "shadow",
        "worker_available": False,
        "active_capacity_intents": [],
        "active_capacity_intent_count": 0,
        "active_capacity_slots": 0,
        "quarantined_intent_count": 0,
        "intent_state_counts": {},
        "blockers": ["manager-shadow"],
    }


@pytest.fixture
def isolated_capacity_api_url(postgres_url: str) -> Iterator[str]:
    source_url = make_url(postgres_url)
    database_name = f"loom_capacity_api_{uuid4().hex}"
    admin_engine = create_engine(
        source_url.set(database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    quoted_database = admin_engine.dialect.identifier_preparer.quote(database_name)
    database_url = source_url.set(database=database_name).render_as_string(hide_password=False)
    migration_engine = create_engine(database_url)
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f"CREATE DATABASE {quoted_database} TEMPLATE template0")
        root = Path(__file__).resolve().parents[2]
        config = AlembicConfig(str(root / "capacity_migrations" / "alembic.ini"))
        config.set_main_option("script_location", str(root / "capacity_migrations"))
        with migration_engine.begin() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield database_url
    finally:
        migration_engine.dispose()
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")
        admin_engine.dispose()


async def test_injected_management_store_is_exact_and_default_remains_disabled(
    tmp_path: Path,
    isolated_capacity_api_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup must use the exact owner-policy store without exposing mutation routes."""

    registry_path = _owner_file(
        tmp_path / "injected-principals.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    _principal(
                        "fleet-operator",
                        OPERATOR_TOKEN,
                        ["capacity:read", "capacity:reconcile"],
                    )
                ],
            }
        ),
    )
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=_owner_file(tmp_path / "injected-database-url", isolated_capacity_api_url),
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=_owner_file(tmp_path / "injected-server.crt", "test"),
        tls_key_file=_owner_file(tmp_path / "injected-server.key", "test"),
        tls_client_ca_file=_owner_file(tmp_path / "injected-client-ca.crt", "test"),
    )
    engine = create_async_engine(isolated_capacity_api_url, isolation_level="SERIALIZABLE")
    try:
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as capacity_session:
            policy = execution_policy()
            fixture = await setup_execution(capacity_session, execution_policy=policy)
            await capacity_session.commit()
            injected_store = fixture.store

            default_app = create_app(
                settings,
                verifier=CapacityPrincipalVerifier.from_file(registry_path),
            )
            with TestClient(default_app) as client:
                assert client.get("/healthz").status_code == 200
                assert default_app.state.store is not injected_store
                with pytest.raises(ExecutionPreparationDisabledError):
                    await default_app.state.store.prepare_execution_epoch(
                        capacity_session,
                        fixture.request.model_copy(
                            update={"expected_writer_epoch": default_app.state.writer.writer_epoch}
                        ),
                        actor="activation-operator",
                        idempotency_key=UUID(int=12303),
                    )

            register_calls = 0
            register_writer = injected_store.register_writer

            async def tracked_register_writer(*args, **kwargs):  # type: ignore[no-untyped-def]
                nonlocal register_calls
                register_calls += 1
                return await register_writer(*args, **kwargs)

            monkeypatch.setattr(injected_store, "register_writer", tracked_register_writer)
            app = create_app(
                settings,
                verifier=CapacityPrincipalVerifier.from_file(registry_path),
                management_store=injected_store,
            )
            with TestClient(app) as client:
                assert client.get("/healthz").status_code == 200
                assert app.state.store is injected_store
                assert register_calls == 1
                assert await injected_store.execution_authority(capacity_session) is None
                prepared = await injected_store.prepare_execution_epoch(
                    capacity_session,
                    fixture.request.model_copy(
                        update={"expected_writer_epoch": app.state.writer.writer_epoch}
                    ),
                    actor="activation-operator",
                    idempotency_key=UUID(int=12301),
                )
                await register_execution_executors(capacity_session, fixture, prepared)
                activation_request = await _store_activation_request(
                    capacity_session,
                    fixture,
                    prepared,
                )
                active = await injected_store.activate_execution_epoch(
                    capacity_session,
                    activation_request,
                    actor="activation-operator",
                    idempotency_key=UUID(int=12302),
                )
                await capacity_session.commit()

                assert active.execution_state == "active"
                assert active.executable_new_capacity_ceiling == 1
                _assert_exact_approved_routes(app)
    finally:
        await engine.dispose()


def test_v2_executor_work_route_is_exactly_pool_bound(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    client, _app, _settings, _allocator = api_context
    headers = {"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"}

    own_pool = client.get("/v2/executors/oldlab/work", headers=headers)
    crossed_pool = client.get("/v2/executors/gb10/work", headers=headers)
    own_context = client.get("/v2/executors/oldlab/context", headers=headers)
    crossed_context = client.get("/v2/executors/gb10/context", headers=headers)

    assert own_pool.status_code == 200
    assert own_pool.json() is None
    assert crossed_pool.status_code == 403
    assert own_context.status_code == 409
    assert crossed_context.status_code == 403


async def test_v2_executor_context_returns_public_execution_context_exactly(
    tmp_path: Path,
    isolated_capacity_api_url: str,
) -> None:
    registry_path = _owner_file(
        tmp_path / "context-principals.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    _principal(
                        "fleet-operator",
                        OPERATOR_TOKEN,
                        ["capacity:reconcile"],
                    ),
                    _principal(
                        "oldlab-executor-v2",
                        OLDLAB_V2_EXECUTOR_TOKEN,
                        ["capacity:execute:pool"],
                        pool_id="oldlab",
                        executor_id="oldlab-executor",
                        executor_incarnation=OLDLAB_EXECUTOR_INCARNATION,
                        executor_pool_generation=1,
                    ),
                ],
            }
        ),
    )
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=_owner_file(tmp_path / "context-database-url", isolated_capacity_api_url),
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=_owner_file(tmp_path / "context-server.crt", "test"),
        tls_key_file=_owner_file(tmp_path / "context-server.key", "test"),
        tls_client_ca_file=_owner_file(tmp_path / "context-client-ca.crt", "test"),
    )
    engine = create_async_engine(isolated_capacity_api_url, isolation_level="SERIALIZABLE")
    try:
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as capacity_session:
            fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
            await capacity_session.commit()
            injected_store = fixture.store
            app = create_app(
                settings,
                verifier=CapacityPrincipalVerifier.from_file(registry_path),
                management_store=injected_store,
            )
            with TestClient(app) as client:
                assert client.get("/healthz").status_code == 200
                prepared = await injected_store.prepare_execution_epoch(
                    capacity_session,
                    fixture.request.model_copy(
                        update={"expected_writer_epoch": app.state.writer.writer_epoch}
                    ),
                    actor="activation-operator",
                    idempotency_key=UUID(int=12321),
                )
                await register_execution_executors(capacity_session, fixture, prepared)
                activation_request = await _store_activation_request(
                    capacity_session,
                    fixture,
                    prepared,
                )
                active = await injected_store.activate_execution_epoch(
                    capacity_session,
                    activation_request,
                    actor="activation-operator",
                    idempotency_key=UUID(int=12322),
                )
                await capacity_session.commit()

                response = client.get(
                    "/v2/executors/oldlab/context",
                    headers={"Authorization": f"Bearer {OLDLAB_V2_EXECUTOR_TOKEN}"},
                )

            assert response.status_code == 200
            active_json = active.model_dump(mode="json")
            expected = {
                "schema_version": 2,
                "authority_incarnation": active_json["authority_incarnation"],
                "writer_epoch": active_json["writer_epoch"],
                "configuration_epoch": active_json["configuration_epoch"],
                "execution_epoch": active_json["execution_epoch"],
                "execution_manifest_sha256": active_json["execution_manifest_sha256"],
                "execution_state": active_json["execution_state"],
                "executable_new_capacity_ceiling": active_json["executable_new_capacity_ceiling"],
                "executable_new_capacity_rate_per_minute": active_json[
                    "executable_new_capacity_rate_per_minute"
                ],
                "trusted_fleet_release_sha256": active_json["trusted_fleet_release_sha256"],
            }
            assert response.json() == expected
            assert "executable" not in response.json()
            assert (
                ExecutionContextV2.model_validate_json(response.content).model_dump(mode="json")
                == expected
            )
    finally:
        await engine.dispose()


def test_v2_work_rejects_legacy_executor_without_positive_pool_generation(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, _app, settings, _allocator = api_context
    verifier = StaticPrincipalVerifier(
        CapacityPrincipal(
            principal_id="legacy-oldlab-executor",
            scopes=frozenset({"capacity:execute:pool"}),
            subject_id=None,
            subject_incarnation=None,
            demand_reporter_incarnation=None,
            pool_id="oldlab",
            pool_reporter_incarnation=None,
            executor_id="oldlab-executor",
            executor_incarnation=OLDLAB_EXECUTOR_INCARNATION,
            executor_pool_generation=None,
        )
    )
    app = create_app(settings, verifier=verifier)
    with TestClient(app) as client:
        response = client.get(
            "/v2/executors/oldlab/work",
            headers={"Authorization": "Bearer legacy-oldlab-executor"},
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_v2_acceptance_rejects_cross_pool_rbac_and_pool_binding_mismatch(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, _app, settings, _allocator = api_context
    store = RecordingExecutionStore()
    app = create_app(
        settings,
        verifier=CapacityPrincipalVerifier.from_file(settings.principals_file),
        execution_store=store,
    )
    tranche_id = UUID(int=41)
    path = f"/v2/executors/oldlab/reservations/{tranche_id}/accept"
    headers = {"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"}
    with TestClient(app) as client:
        accepted = client.post(
            path,
            headers=headers,
            json=_v2_acceptance_payload(tranche_id=tranche_id),
        )
        crossed_pool = client.post(
            f"/v2/executors/gb10/reservations/{tranche_id}/accept",
            headers=headers,
            json=_v2_acceptance_payload(tranche_id=tranche_id),
        )
        mismatched_pool = client.post(
            path,
            headers=headers,
            json=_v2_acceptance_payload(pool_id="gb10", tranche_id=tranche_id),
        )
        mismatched_executor = client.post(
            path,
            headers=headers,
            json=_v2_acceptance_payload(
                executor_id="other-executor",
                tranche_id=tranche_id,
            ),
        )
        mismatched_generation = client.post(
            path,
            headers=headers,
            json=_v2_acceptance_payload(pool_generation=2, tranche_id=tranche_id),
        )

    assert accepted.status_code == 200
    assert accepted.json() == {
        "tranche_id": str(tranche_id),
        "intent_ids": [str(UUID(int=44))],
        "receipt_digest": "8" * 64,
        "replayed": False,
        "executable": True,
    }
    assert store.accept_calls == 1
    assert store.last_acceptance is not None
    assert store.last_acceptance.pool_id == "oldlab"
    assert crossed_pool.status_code == 403
    assert mismatched_pool.status_code == 403
    assert mismatched_executor.status_code == 403
    assert mismatched_generation.status_code == 403


def test_submission_recovery_response_is_explicitly_quarantined(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, _app, settings, _allocator = api_context
    store = RecordingExecutionStore()
    app = create_app(
        settings,
        verifier=CapacityPrincipalVerifier.from_file(settings.principals_file),
        execution_store=store,
    )
    permit_id = UUID(int=43)
    with TestClient(app) as client:
        response = client.post(
            f"/v2/executors/oldlab/permits/{permit_id}/recover",
            headers={"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"},
            json=_v2_submission_recovery_payload(permit_id=permit_id),
        )

    assert response.status_code == 200
    assert response.json() == {
        "intent_id": str(UUID(int=42)),
        "receipt_digest": "9" * 64,
        "replayed": False,
        "state": "quarantined",
        "executable": True,
    }
    assert store.recovery_calls == 1
    assert store.last_recovery is not None
    assert store.last_recovery.binding.pool_generation == 1


def test_submission_recovery_rejects_store_returned_unused_state(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    class ForbiddenRecoveryStateStore(RecordingExecutionStore):
        async def recover_unsubmitted_permit(
            self,
            _session,  # type: ignore[no-untyped-def]
            recovery: ExecutableSubmissionRecoveryV2,
        ) -> dict[str, object]:
            self.recovery_calls += 1
            self.last_recovery = recovery
            return {
                "intent_id": str(recovery.binding.intent_id),
                "receipt_digest": "9" * 64,
                "replayed": False,
                "state": "unused",
                "executable": True,
            }

    _client, _app, settings, _allocator = api_context
    store = ForbiddenRecoveryStateStore()
    app = create_app(
        settings,
        verifier=CapacityPrincipalVerifier.from_file(settings.principals_file),
        execution_store=store,
    )
    permit_id = UUID(int=43)
    with TestClient(app) as client:
        response = client.post(
            f"/v2/executors/oldlab/permits/{permit_id}/recover",
            headers={"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"},
            json=_v2_submission_recovery_payload(permit_id=permit_id),
        )

    assert response.status_code == 409
    assert response.json() == {"detail": "capacity state conflict"}
    assert store.recovery_calls == 1
    assert store.last_recovery is not None


def test_v2_submission_recovery_rejects_path_or_executor_drift(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, _app, settings, _allocator = api_context
    store = RecordingExecutionStore()
    app = create_app(
        settings,
        verifier=CapacityPrincipalVerifier.from_file(settings.principals_file),
        execution_store=store,
    )
    permit_id = UUID(int=43)
    payload = _v2_submission_recovery_payload(permit_id=permit_id)
    with TestClient(app) as client:
        wrong_path = client.post(
            f"/v2/executors/oldlab/permits/{UUID(int=99)}/recover",
            headers={"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"},
            json=payload,
        )
        wrong_executor = client.post(
            f"/v2/executors/oldlab/permits/{permit_id}/recover",
            headers={"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"},
            json=payload | {"binding": payload["binding"] | {"executor_id": "other-executor"}},
        )
        wrong_generation = client.post(
            f"/v2/executors/oldlab/permits/{permit_id}/recover",
            headers={"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"},
            json=payload | {"binding": payload["binding"] | {"pool_generation": 2}},
        )

    assert wrong_path.status_code == 403
    assert wrong_executor.status_code == 403
    assert wrong_generation.status_code == 403
    assert store.recovery_calls == 0


def test_v2_bootstrap_routes_separate_executor_proposal_from_subject_acknowledgement(
    api_context_v2_executor_generation: tuple[
        TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator
    ],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, app, _settings, _allocator = api_context_v2_executor_generation
    binding = _v2_intent_binding()
    proposal = ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="6" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    acknowledgement = ExecutableBootstrapAcknowledgementV2(
        binding=binding,
        proposal_epoch=proposal.proposal_epoch,
        proposal_digest="7" * 64,
        reporter_incarnation=subject_configuration().demand_reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        protected_admission_sha256="3" * 64,
    )
    calls: list[tuple[str, object]] = []

    async def propose(_session, value):  # type: ignore[no-untyped-def]
        calls.append(("proposal", value))
        return ProposedExecutableBootstrap(
            intent_id=value.binding.intent_id,
            proposal_epoch=value.proposal_epoch,
            receipt_digest="9" * 64,
            replayed=False,
        )

    async def next_subject(
        _session,  # type: ignore[no-untyped-def]
        *,
        subject_id,
        subject_incarnation,
        reporter_incarnation,
    ):
        calls.append(
            (
                "subject-work",
                (subject_id, subject_incarnation, reporter_incarnation),
            )
        )
        return proposal

    async def acknowledge(
        _session,  # type: ignore[no-untyped-def]
        value,
        *,
        actor,
        idempotency_key,
    ):
        calls.append(("acknowledgement", (value, actor, idempotency_key)))
        return RegisteredExecutableBootstrap(
            intent_id=value.binding.intent_id,
            bootstrap_registration_epoch=value.bootstrap_registration_epoch,
            receipt_digest="a" * 64,
            replayed=False,
        )

    monkeypatch.setattr(app.state.execution_store, "propose_bootstrap", propose)
    monkeypatch.setattr(app.state.execution_store, "next_subject_bootstrap", next_subject)
    monkeypatch.setattr(app.state.execution_store, "acknowledge_bootstrap", acknowledge)
    executor_headers = {"Authorization": f"Bearer {OLDLAB_V2_EXECUTOR_TOKEN}"}
    reporter_headers = {"Authorization": f"Bearer {DEMAND_TOKEN}"}

    proposed = client.post(
        f"/v2/executors/oldlab/intents/{binding.intent_id}/bootstrap-proposals",
        headers=executor_headers,
        json=proposal.model_dump(mode="json"),
    )
    assert proposed.status_code == 200, proposed.text
    assert proposed.json()["proposal_epoch"] == 1
    assert (
        client.post(
            f"/v2/executors/gb10/intents/{binding.intent_id}/bootstrap-proposals",
            headers=executor_headers,
            json=proposal.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.post(
            f"/v2/executors/oldlab/intents/{binding.intent_id}/bootstrap",
            headers=executor_headers,
            json=proposal.model_dump(mode="json"),
        ).status_code
        == 404
    )

    work = client.get(
        f"/v2/subjects/{SUBJECT_ID}/bootstrap-work",
        headers=reporter_headers,
    )
    assert work.status_code == 200, work.text
    assert ExecutableBootstrapProposalV2.model_validate_json(work.content) == proposal
    assert (
        client.get(
            f"/v2/subjects/{UUID(int=999)}/bootstrap-work",
            headers=reporter_headers,
        ).status_code
        == 403
    )
    assert (
        client.get(
            f"/v2/subjects/{SUBJECT_ID}/bootstrap-work",
            headers=executor_headers,
        ).status_code
        == 403
    )

    idempotency_key = uuid4()
    acknowledged = client.put(
        f"/v2/subjects/{SUBJECT_ID}/intents/{binding.intent_id}/bootstrap-acknowledgements",
        headers=reporter_headers | {"Idempotency-Key": str(idempotency_key)},
        json=acknowledgement.model_dump(mode="json"),
    )
    assert acknowledged.status_code == 200, acknowledged.text
    assert acknowledged.json()["bootstrap_registration_epoch"] == 1
    assert (
        client.put(
            f"/v2/subjects/{UUID(int=999)}/intents/{binding.intent_id}/bootstrap-acknowledgements",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=acknowledgement.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert [kind for kind, _value in calls] == [
        "proposal",
        "subject-work",
        "acknowledgement",
    ]
    _value, actor, received_key = calls[-1][1]  # type: ignore[misc]
    assert actor == "dev-reporter"
    assert received_key == idempotency_key


def test_v2_admission_routes_require_exact_subject_reporter_and_proposal(
    api_context_v2_executor_generation: tuple[
        TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator
    ],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Loosening any route binding must expose another subject's protected plan."""

    client, app, _settings, _allocator = api_context_v2_executor_generation
    binding = _v2_intent_binding()
    worker_shape = shape()
    allowance = ExecutableAdmissionAllowanceV2(
        allowance_id=UUID(int=811),
        protected_attempt_id=UUID(int=812),
        shape_instance_id=binding.shape_instance_id,
        shape_slot_index=0,
        submission_intent_id=binding.intent_id,
    )
    proposal = ExecutableAdmissionPlanProposalV2(
        proposal_id=UUID(int=813),
        plan_id=UUID(int=814),
        admission_incarnation=UUID(int=815),
        reporter_incarnation=subject_configuration().demand_reporter_incarnation,
        protected_admission_sha256="3" * 64,
        manager_input_digest="6" * 64,
        manager_allocation_digest="7" * 64,
        lease_not_after=datetime.now(UTC) + timedelta(minutes=1),
        shapes=(
            ExecutableAdmissionShapeV2(
                binding=binding,
                protocol_generation=1,
                protocol_digest="8" * 64,
                worker_shape=worker_shape,
                worker_shape_digest=canonical_digest(worker_shape),
                bootstrap_registration_epoch=1,
            ),
        ),
        allowances=(allowance,),
    )
    assignment = ProtectedAdmissionAssignmentV2(
        transition_id=UUID(int=816),
        allowance_id=allowance.allowance_id,
        protected_attempt_id=allowance.protected_attempt_id,
        execution_generation=1,
        requirements_digest="9" * 64,
        shape_instance_id=allowance.shape_instance_id,
        shape_slot_index=allowance.shape_slot_index,
        submission_intent_id=allowance.submission_intent_id,
        lifecycle_sequence=1,
    )
    acknowledgement = ExecutableAdmissionAcknowledgementV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        proposal_id=proposal.proposal_id,
        plan_id=proposal.plan_id,
        admission_incarnation=proposal.admission_incarnation,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        pool_id="oldlab",
        reporter_incarnation=proposal.reporter_incarnation,
        protected_admission_sha256=proposal.protected_admission_sha256,
        proposal_digest=canonical_executable_digest(proposal),
        prepared_plan_digest="a" * 64,
        assignment_count=1,
        assignments=(assignment,),
    )
    closure_acknowledgement = ExecutableAdmissionPlanClosureAcknowledgementV2(
        closure_id=UUID(int=817),
        proposal_id=proposal.proposal_id,
        proposal_digest=canonical_executable_digest(proposal),
        plan_id=proposal.plan_id,
        admission_incarnation=proposal.admission_incarnation,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=proposal.reporter_incarnation,
        protected_admission_sha256=proposal.protected_admission_sha256,
        close_reason="expired",
        disposition_kind="never-converged",
        disposition_digest="c" * 64,
    )
    calls: list[tuple[str, object]] = []

    async def next_admission(
        _session,  # type: ignore[no-untyped-def]
        *,
        subject_id,
        subject_incarnation,
        reporter_incarnation,
    ):
        calls.append(
            (
                "admission-work",
                (subject_id, subject_incarnation, reporter_incarnation),
            )
        )
        return proposal

    async def acknowledge_admission(
        _session,  # type: ignore[no-untyped-def]
        value,
        *,
        actor,
        idempotency_key,
    ):
        calls.append(("admission-acknowledgement", (value, actor, idempotency_key)))
        return RegisteredExecutableAdmissionPlan(
            proposal_id=value.proposal_id,
            prepared_plan_digest=value.prepared_plan_digest,
            receipt_digest="b" * 64,
            replayed=False,
        )

    async def acknowledge_admission_closure(
        _session,  # type: ignore[no-untyped-def]
        value,
        *,
        actor,
        idempotency_key,
    ):
        calls.append(("admission-closure", (value, actor, idempotency_key)))
        return RegisteredExecutableAdmissionPlanClosure(
            closure_id=value.closure_id,
            disposition_kind=value.disposition_kind,
            disposition_digest=value.disposition_digest,
            receipt_digest=canonical_executable_digest(value),
            replayed=False,
        )

    monkeypatch.setattr(
        app.state.execution_store,
        "next_subject_admission_plan",
        next_admission,
    )
    monkeypatch.setattr(
        app.state.execution_store,
        "acknowledge_admission_plan",
        acknowledge_admission,
    )
    monkeypatch.setattr(
        app.state.execution_store,
        "acknowledge_admission_plan_closure",
        acknowledge_admission_closure,
    )
    reporter_headers = {"Authorization": f"Bearer {DEMAND_TOKEN}"}
    executor_headers = {"Authorization": f"Bearer {OLDLAB_V2_EXECUTOR_TOKEN}"}

    work = client.get(
        f"/v2/subjects/{SUBJECT_ID}/admission-work",
        headers=reporter_headers,
    )
    assert work.status_code == 200, work.text
    assert work.content == canonical_executable_bytes(proposal)
    assert ExecutableAdmissionPlanProposalV2.model_validate_json(work.content) == proposal
    assert (
        client.get(
            f"/v2/subjects/{UUID(int=999)}/admission-work",
            headers=reporter_headers,
        ).status_code
        == 403
    )
    assert (
        client.get(
            f"/v2/subjects/{SUBJECT_ID}/admission-work",
            headers=executor_headers,
        ).status_code
        == 403
    )

    idempotency_key = uuid4()
    acknowledged = client.put(
        f"/v2/subjects/{SUBJECT_ID}/admission-acknowledgements/{proposal.proposal_id}",
        headers=reporter_headers | {"Idempotency-Key": str(idempotency_key)},
        json=acknowledgement.model_dump(mode="json"),
    )
    assert acknowledged.status_code == 200, acknowledged.text
    assert acknowledged.json()["prepared_plan_digest"] == "a" * 64
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-acknowledgements/{UUID(int=999)}",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=acknowledgement.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.put(
            f"/v2/subjects/{UUID(int=999)}/admission-acknowledgements/"
            f"{proposal.proposal_id}",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=acknowledgement.model_dump(mode="json"),
        ).status_code
        == 403
    )
    wrong_subject = acknowledgement.model_copy(update={"subject_id": UUID(int=999)})
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-acknowledgements/"
            f"{proposal.proposal_id}",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=wrong_subject.model_dump(mode="json"),
        ).status_code
        == 403
    )
    wrong_subject_incarnation = acknowledgement.model_copy(
        update={"subject_incarnation": UUID(int=999)}
    )
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-acknowledgements/"
            f"{proposal.proposal_id}",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=wrong_subject_incarnation.model_dump(mode="json"),
        ).status_code
        == 403
    )
    wrong_reporter = acknowledgement.model_copy(
        update={"reporter_incarnation": UUID(int=999)}
    )
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-acknowledgements/{proposal.proposal_id}",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=wrong_reporter.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-acknowledgements/"
            f"{proposal.proposal_id}",
            headers=executor_headers | {"Idempotency-Key": str(uuid4())},
            json=acknowledgement.model_dump(mode="json"),
        ).status_code
        == 403
    )
    closure_key = uuid4()
    closed = client.put(
        f"/v2/subjects/{SUBJECT_ID}/admission-closures/"
        f"{closure_acknowledgement.closure_id}/acknowledgements",
        headers=reporter_headers | {"Idempotency-Key": str(closure_key)},
        json=closure_acknowledgement.model_dump(mode="json"),
    )
    assert closed.status_code == 200, closed.text
    assert closed.json()["closure_id"] == str(closure_acknowledgement.closure_id)
    assert closed.json()["executable"] is False
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-closures/{UUID(int=999)}/"
            "acknowledgements",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=closure_acknowledgement.model_dump(mode="json"),
        ).status_code
        == 403
    )
    wrong_closure_reporter = closure_acknowledgement.model_copy(
        update={"reporter_incarnation": UUID(int=999)}
    )
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-closures/"
            f"{closure_acknowledgement.closure_id}/acknowledgements",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=wrong_closure_reporter.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert (
        client.put(
            f"/v2/subjects/{SUBJECT_ID}/admission-closures/"
            f"{closure_acknowledgement.closure_id}/acknowledgements",
            headers=executor_headers | {"Idempotency-Key": str(uuid4())},
            json=closure_acknowledgement.model_dump(mode="json"),
        ).status_code
        == 403
    )
    assert [kind for kind, _value in calls] == [
        "admission-work",
        "admission-acknowledgement",
        "admission-closure",
    ]


def test_lifecycle_can_project_and_authenticate_a_personal_demand_reporter(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    projection = development_projection().model_copy(
        update={"demand_reporter_token_sha256": _hash(DYNAMIC_DEMAND_TOKEN)}
    )
    response = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=projection.model_dump(mode="json"),
    )
    assert response.status_code == 200, response.text
    assert response.json()["subject"]["display_name"] == "dev-alice"
    assert response.json()["subject"]["min_slots"] == 0

    report = demand_snapshot(
        subject_id=DEVELOPMENT_SUBJECT_ID,
        subject_incarnation=DEVELOPMENT_SUBJECT_INCARNATION,
        reporter_incarnation=DEVELOPMENT_REPORTER_INCARNATION,
    )
    accepted = client.put(
        f"/v1/reports/demand/{DEVELOPMENT_SUBJECT_ID}",
        headers={"Authorization": f"Bearer {DYNAMIC_DEMAND_TOKEN}"},
        json=report.model_dump(mode="json"),
    )
    assert accepted.status_code == 200, accepted.text

    rejected = client.put(
        f"/v1/reports/demand/{DEVELOPMENT_SUBJECT_ID}",
        headers={"Authorization": "Bearer wrong-dynamic-token"},
        json=report.model_copy(update={"sequence": 2}).model_dump(mode="json"),
    )
    assert rejected.status_code == 401


def test_lifecycle_can_retire_personal_subject_and_fence_its_reporter(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    create = development_projection().model_copy(
        update={"demand_reporter_token_sha256": _hash(DYNAMIC_DEMAND_TOKEN)}
    )
    created = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=create.model_dump(mode="json"),
    )
    assert created.status_code == 200, created.text

    retirement = create.model_copy(
        update={
            "expected_configuration_epoch": 2,
            "operation_kind": "destroy",
            "operation_id": uuid4(),
            "operation_epoch": 2,
            "configuration_generation": 2,
        }
    )
    forged = retirement.model_copy(
        update={
            "operation_id": uuid4(),
            "candidate_publication_sha256": "0" * 64,
        }
    )
    rejected_forgery = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=forged.model_dump(mode="json"),
    )
    assert rejected_forgery.status_code == 409

    retirement_key = uuid4()
    retired = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(retirement_key)},
        json=retirement.model_dump(mode="json"),
    )
    assert retired.status_code == 200, retired.text
    assert retired.json()["subject"]["lifecycle_state"] == "disabled"
    assert retired.json()["subject"]["min_slots"] == 0
    assert retired.json()["subject"]["max_slots"] == 0

    replay = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(retirement_key)},
        json=retirement.model_dump(mode="json"),
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["configuration_epoch"] == retired.json()["configuration_epoch"]
    assert replay.json()["replayed"] is True

    report = demand_snapshot(
        subject_id=DEVELOPMENT_SUBJECT_ID,
        subject_incarnation=DEVELOPMENT_SUBJECT_INCARNATION,
        reporter_incarnation=DEVELOPMENT_REPORTER_INCARNATION,
    ).model_copy(update={"configuration_generation": 2})
    rejected = client.put(
        f"/v1/reports/demand/{DEVELOPMENT_SUBJECT_ID}",
        headers={"Authorization": f"Bearer {DYNAMIC_DEMAND_TOKEN}"},
        json=report.model_dump(mode="json"),
    )
    assert rejected.status_code == 401

    subjects = client.get("/v1/status/subjects", headers=operator_headers)
    assert subjects.status_code == 200
    assert all(
        item["subject_id"] != str(DEVELOPMENT_SUBJECT_ID) for item in subjects.json()["items"]
    )


@pytest.mark.parametrize("operation_kind", ["update", "capacity"])
def test_lifecycle_can_restore_an_unregistered_personal_subject_from_full_evidence(
    operation_kind: str,
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    projection = development_projection().model_copy(
        update={
            "operation_kind": operation_kind,
            "demand_reporter_token_sha256": _hash(DYNAMIC_DEMAND_TOKEN),
        }
    )
    response = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=projection.model_dump(mode="json"),
    )
    assert response.status_code == 200, response.text
    assert response.json()["subject"]["configuration_generation"] == (
        projection.configuration_generation
    )


async def test_streaming_body_limit_rejects_oversized_body_without_content_length() -> None:
    downstream_called = False
    sent: list[dict[str, object]] = []
    incoming = iter(
        [
            {
                "type": "http.request",
                "body": b"x" * MAX_CONTRACT_BYTES,
                "more_body": True,
            },
            {"type": "http.request", "body": b"x", "more_body": False},
        ]
    )

    async def downstream(_scope, _receive, _send):  # type: ignore[no-untyped-def]
        nonlocal downstream_called
        downstream_called = True

    async def receive() -> dict[str, object]:
        return next(incoming)

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    middleware = RequestBodyLimitMiddleware(downstream, maximum_bytes=MAX_CONTRACT_BYTES)
    await middleware({"type": "http", "headers": []}, receive, send)

    assert not downstream_called
    assert sent[0]["status"] == 413
    assert sent[1]["body"] == b'{"detail":"request too large"}'


def test_reporter_cannot_publish_config_or_impersonate_subject(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    reporter_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    assert (
        client.put(
            "/v1/config-proposals/fleet",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=fleet_manifest().model_dump(mode="json"),
        ).status_code
        == 403
    )
    other = uuid4()
    impersonated = demand_snapshot(sequence=2).model_copy(update={"subject_id": other})
    response = client.put(
        f"/v1/reports/demand/{other}",
        headers=reporter_headers,
        json=impersonated.model_dump(mode="json"),
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}

    protected_release = {
        "schema_version": 1,
        "authority_incarnation": str(AUTHORITY_ID),
        "writer_epoch": 1,
        "configuration_epoch": 1,
        "allocation_epoch": 1,
        "tranche_id": str(uuid4()),
        "shape_instance_id": "shape-0001",
        "intent_id": str(uuid4()),
        "subject_id": str(other),
        "subject_incarnation": str(SUBJECT_INCARNATION),
        "reporter_incarnation": str(DEMAND_REPORTER_ID),
        "deployment_generation": 1,
        "pool_id": "gb10",
        "pool_generation": 1,
        "bootstrap_registration_epoch": 0,
        "protected_registration_epoch": 1,
        "bootstrap_revoked": True,
        "protected_release_sha256": "a" * 64,
        "executable": False,
    }
    response = client.put(
        f"/v1/reports/protected-releases/{other}/shape-0001",
        headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
        json=protected_release,
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_pool_executor_registration_heartbeat_inventory_and_cross_pool_rbac(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, app, _settings, _allocator = api_context
    registration = {
        "schema_version": 1,
        "executor_id": "oldlab-executor",
        "executor_incarnation": str(OLDLAB_EXECUTOR_INCARNATION),
        "pool_id": "oldlab",
        "pool_generation": 1,
        "signing_key_id": "oldlab-key-1",
        "signing_key_sha256": public_key_fingerprint(OLDLAB_OWNERSHIP_PRIVATE_KEY.public_key()),
        "local_authority_sha256": "b" * 64,
        "executable": False,
    }
    registered = client.put(
        "/v1/executors/oldlab/registration",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=registration,
    )
    assert registered.status_code == 200, registered.text
    heartbeat = {
        "schema_version": 1,
        "authority_incarnation": str(AUTHORITY_ID),
        "writer_epoch": app.state.writer.writer_epoch,
        "executor_id": "oldlab-executor",
        "executor_incarnation": str(OLDLAB_EXECUTOR_INCARNATION),
        "pool_id": "oldlab",
        "pool_generation": 1,
        "heartbeat_sequence": 1,
        "journal_sequence": 0,
        "journal_digest": "0" * 64,
        "executable": False,
    }
    executor_headers = {"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"}

    checkpoint = client.get(
        "/v1/executors/oldlab/checkpoint",
        headers=executor_headers,
    )
    assert checkpoint.status_code == 200, checkpoint.text
    assert checkpoint.json()["journal_sequence"] == 0
    assert checkpoint.json()["journal_digest"] == "0" * 64
    assert checkpoint.json()["executable"] is False
    assert (
        client.get(
            "/v1/executors/gb10/checkpoint",
            headers=executor_headers,
        ).status_code
        == 403
    )

    response = client.put(
        "/v1/executors/oldlab/heartbeat",
        headers=executor_headers,
        json=heartbeat,
    )
    assert response.status_code == 200, response.text
    assert response.json()["heartbeat_sequence"] == 1
    inventory = {
        "schema_version": 1,
        "authority_incarnation": str(AUTHORITY_ID),
        "writer_epoch": app.state.writer.writer_epoch,
        "executor_id": "oldlab-executor",
        "executor_incarnation": str(OLDLAB_EXECUTOR_INCARNATION),
        "pool_id": "oldlab",
        "pool_generation": 1,
        "inventory_sequence": 1,
        "journal_sequence": 0,
        "journal_digest": "0" * 64,
        "journal_checkpoint_sequence": 0,
        "journal_checkpoint_digest": "0" * 64,
        "complete": True,
        "records": [],
        "executable": False,
    }
    inventory_response = client.put(
        "/v1/executors/oldlab/inventory",
        headers=executor_headers,
        json=inventory,
    )
    assert inventory_response.status_code == 200, inventory_response.text
    assert inventory_response.json()["inventory_sequence"] == 1
    assert (
        client.put(
            "/v1/executors/gb10/heartbeat",
            headers=executor_headers,
            json=heartbeat | {"pool_id": "gb10"},
        ).status_code
        == 403
    )


def test_real_server_requires_mutual_tls(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, _app, settings, _allocator = api_context
    options = build_uvicorn_kwargs(settings)
    assert options["ssl_cert_reqs"] == ssl.CERT_REQUIRED
    assert options["ssl_ca_certs"] == str(settings.tls_client_ca_file)


def test_concurrent_reconciliation_trigger_is_rejected(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, allocator = api_context
    with hold_reconciliation_open(client, allocator, operator_headers):
        response = client.post(
            "/v1/shadow-reconciliations",
            headers=operator_headers,
        )
    assert response.status_code == 409
    assert response.json() == {"detail": "shadow reconciliation already running"}


async def test_allocation_reconcile_keeps_shadow_execution_bindings_null(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _app, _settings, allocator = api_context
    allocator.release.set()

    response = client.post("/v1/shadow-reconciliations", headers=operator_headers)

    assert response.status_code == 200, response.text
    async with capacity_session_factory() as session:
        epoch = (await session.execute(select(CapacityAllocationEpoch))).scalar_one()
        allocations = (await session.execute(select(CapacityAllocation))).scalars().all()
    assert epoch.status == "shadow"
    assert epoch.executable is False
    assert epoch.execution_epoch is None
    assert epoch.execution_manifest_sha256 is None
    assert allocations
    assert all(allocation.mode == "shadow" for allocation in allocations)
    assert all(allocation.executable is False for allocation in allocations)
    assert all(allocation.execution_epoch is None for allocation in allocations)
    assert all(allocation.execution_manifest_sha256 is None for allocation in allocations)
    async with capacity_session_factory() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(update(CapacityAllocation).values(mode="executable"))


async def test_allocation_shadow_commit_preserves_prepared_execution_freeze(
    capacity_session: AsyncSession,
) -> None:
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=879),
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )

    assert result.status == "committed"
    epoch = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert epoch.status == "shadow"
    assert epoch.execution_epoch is None
    assert epoch.execution_manifest_sha256 is None
    assert authority.execution_state == "prepared"
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "execution_epoch_prepared"


async def test_allocation_activation_during_shadow_commit_retries_as_executable(
    capacity_session: AsyncSession,
) -> None:
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await _ingest_fresh_execution_inputs(capacity_session, fixture.store)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=883),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = await _store_activation_request(
        capacity_session,
        fixture,
        prepared,
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    store = ActivateBeforeShadowCommitStore(activation)
    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=store,
    )

    assert result.status == "committed"
    assert result.attempt_count == 2
    epoch = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    assert epoch.status == "executable"
    assert epoch.execution_epoch == prepared.execution_epoch
    assert epoch.execution_manifest_sha256 == prepared.execution_manifest_sha256


async def test_allocation_reconcile_commits_fresh_plan_under_active_execution_fence(
    capacity_session: AsyncSession,
) -> None:
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await _ingest_fresh_execution_inputs(capacity_session, fixture.store)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=880),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = await _store_activation_request(capacity_session, fixture, prepared)
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=881),
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )

    assert result.status == "committed"
    epoch = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    allocations = (await capacity_session.execute(select(CapacityAllocation))).scalars().all()
    current_input = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    assert epoch.status == "executable"
    assert epoch.executable is True
    assert epoch.execution_epoch == active.execution_epoch
    assert epoch.execution_manifest_sha256 == active.execution_manifest_sha256
    assert epoch.input_valid_until == fixture.store.allocation_input_valid_until(current_input)
    assert epoch.sealed is True
    assert epoch.allocation_count == len(allocations)
    assert epoch.complete_payload["execution"]["allocation_epoch"] == epoch.allocation_epoch
    assert epoch.complete_payload["executable_new_capacity_ceiling"] == 1
    assert allocations
    assert all(allocation.mode == "executable" for allocation in allocations)
    assert all(allocation.executable is True for allocation in allocations)
    assert all(allocation.execution_epoch == active.execution_epoch for allocation in allocations)
    assert all(
        allocation.execution_manifest_sha256 == active.execution_manifest_sha256
        for allocation in allocations
    )
    with pytest.raises(DBAPIError):
        async with capacity_session.begin_nested():
            await capacity_session.execute(update(CapacityAllocation).values(mode="shadow"))

    shadow_parent = CapacityAllocationEpoch(
        writer_epoch=fixture.writer.writer_epoch,
        configuration_epoch=active.configuration_epoch,
        input_digest="9" * 64,
        status="shadow",
        failure_reason=None,
        complete_payload={"schema_version": 1},
        executable=False,
        execution_epoch=None,
        execution_manifest_sha256=None,
        committed_at=None,
    )
    capacity_session.add(shadow_parent)
    await capacity_session.flush()
    shadow_child = CapacityAllocation(
        allocation_epoch=shadow_parent.allocation_epoch,
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        deployment_generation=1,
        pool_id="gb10",
        desired_shapes=[],
        desired_resources={},
        commitments=[],
        drains=[],
        allowances=[],
        witness={},
        mode="shadow",
        executable=False,
        execution_epoch=None,
        execution_manifest_sha256=None,
    )
    capacity_session.add(shadow_child)
    await capacity_session.flush()
    with pytest.raises(DBAPIError, match="allocation epoch mode binding is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocationEpoch)
                .where(CapacityAllocationEpoch.allocation_epoch == shadow_parent.allocation_epoch)
                .values(
                    status="executable",
                    executable=True,
                    execution_epoch=active.execution_epoch,
                    execution_manifest_sha256=active.execution_manifest_sha256,
                )
            )
            await capacity_session.execute(
                update(CapacityAllocation)
                .where(CapacityAllocation.id == shadow_child.id)
                .values(
                    mode="executable",
                    executable=True,
                    execution_epoch=active.execution_epoch,
                    execution_manifest_sha256=active.execution_manifest_sha256,
                )
            )

    capacity_session.add(
        CapacityAllocation(
            allocation_epoch=shadow_parent.allocation_epoch,
            subject_id=uuid4(),
            subject_incarnation=uuid4(),
            deployment_generation=1,
            pool_id="oldlab",
            desired_shapes=[],
            desired_resources={},
            commitments=[],
            drains=[],
            allowances=[],
            witness={},
            mode="executable",
            executable=True,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
        )
    )
    with pytest.raises(DBAPIError, match="allocation binding must match its parent epoch"):
        async with capacity_session.begin_nested():
            await capacity_session.flush()


async def test_executable_allocation_evidence_is_immutable_and_not_reparentable(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )
    assert result.status == "committed"
    parent = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    child = (await capacity_session.execute(select(CapacityAllocation))).scalars().first()
    assert child is not None
    second_parent = CapacityAllocationEpoch(
        writer_epoch=fixture.writer.writer_epoch,
        configuration_epoch=active.configuration_epoch,
        input_digest="8" * 64,
        status="executable",
        failure_reason=None,
        complete_payload={"schema_version": 2, "allocations": []},
        executable=True,
        execution_epoch=active.execution_epoch,
        execution_manifest_sha256=active.execution_manifest_sha256,
        input_valid_until=datetime.now(UTC),
        sealed=True,
        allocation_count=0,
        committed_at=parent.committed_at,
    )
    capacity_session.add(second_parent)
    await capacity_session.flush()

    with pytest.raises(DBAPIError, match="executable allocation epoch is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocationEpoch)
                .where(CapacityAllocationEpoch.allocation_epoch == parent.allocation_epoch)
                .values(complete_payload={"tampered": True})
            )
    with pytest.raises(DBAPIError, match="executable allocation is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocation)
                .where(CapacityAllocation.id == child.id)
                .values(desired_shapes=[{"tampered": True}])
            )
    with pytest.raises(DBAPIError, match="executable allocation is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocation)
                .where(CapacityAllocation.id == child.id)
                .values(allocation_epoch=second_parent.allocation_epoch)
            )
    with pytest.raises(DBAPIError, match="executable allocation is append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                delete(CapacityAllocation).where(CapacityAllocation.id == child.id)
            )
    with pytest.raises(DBAPIError, match="executable allocation epoch is append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                delete(CapacityAllocationEpoch).where(
                    CapacityAllocationEpoch.allocation_epoch == second_parent.allocation_epoch
                )
            )
    with pytest.raises(DBAPIError, match="executable allocation epoch is sealed"):
        async with capacity_session.begin_nested():
            capacity_session.add(
                CapacityAllocation(
                    allocation_epoch=parent.allocation_epoch,
                    subject_id=uuid4(),
                    subject_incarnation=uuid4(),
                    deployment_generation=1,
                    pool_id="oldlab",
                    desired_shapes=[],
                    desired_resources={},
                    commitments=[],
                    drains=[],
                    allowances=[],
                    witness={},
                    mode="executable",
                    executable=True,
                    execution_epoch=parent.execution_epoch,
                    execution_manifest_sha256=parent.execution_manifest_sha256,
                )
            )
            await capacity_session.flush()


async def _active_execution_fixture(capacity_session: AsyncSession):  # type: ignore[no-untyped-def]
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await _ingest_fresh_execution_inputs(capacity_session, fixture.store)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=uuid4(),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = await _store_activation_request(capacity_session, fixture, prepared)
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=uuid4(),
    )
    return fixture, active


@pytest.mark.parametrize(
    "allocator, timeout, store_factory, expected_event",
    [
        (
            ActiveTimeoutAllocator(),
            0.01,
            None,
            "shadow_allocation_timeout",
        ),
        (
            ActiveInvalidAllocator(),
            1.0,
            None,
            "shadow_allocation_invalid",
        ),
        (
            ActiveUnexpectedAllocator(),
            1.0,
            None,
            "shadow_allocation_failure",
        ),
        (
            allocate_shadow,
            1.0,
            ActiveCommitFailureStore,
            "shadow_allocation_failure",
        ),
    ],
)
async def test_allocation_active_failure_records_false_null_evidence_and_freezes(
    capacity_session: AsyncSession,
    allocator,  # type: ignore[no-untyped-def]
    timeout: float,
    store_factory,  # type: ignore[no-untyped-def]
    expected_event: str,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    store = fixture.store if store_factory is None else store_factory()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        allocator=allocator,
        allocation_timeout_seconds=timeout,
        store=store,
    )

    assert result.status == "failed"
    failed = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    audit = (
        (
            await capacity_session.execute(
                select(CapacityAuditEvent).order_by(CapacityAuditEvent.id.desc())
            )
        )
        .scalars()
        .first()
    )
    assert failed.status == "failed"
    assert failed.executable is False
    assert failed.execution_epoch is None
    assert failed.execution_manifest_sha256 is None
    assert authority.execution_epoch == active.execution_epoch
    assert authority.execution_manifest_sha256 == active.execution_manifest_sha256
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == expected_event
    assert audit is not None and audit.event_kind == expected_event


async def test_allocation_authority_resolution_failure_uses_locked_durable_authority(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    store = AuthorityResolutionFailureStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=store,
    )

    assert result.status == "failed"
    assert store.reconcile_failure_records == 1
    failed = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    audit = (
        (
            await capacity_session.execute(
                select(CapacityAuditEvent).order_by(CapacityAuditEvent.id.desc())
            )
        )
        .scalars()
        .first()
    )
    assert failed.status == "failed"
    assert failed.writer_epoch == fixture.writer.writer_epoch
    assert failed.configuration_epoch == active.configuration_epoch
    assert failed.executable is False
    assert failed.execution_epoch is None
    assert failed.execution_manifest_sha256 is None
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_invalid"
    assert audit is not None
    assert audit.object_binding == {
        "allocation_epoch": failed.allocation_epoch,
        "configuration_epoch": active.configuration_epoch,
        "execution_epoch": active.execution_epoch,
        "execution_manifest_sha256": active.execution_manifest_sha256,
        "writer_epoch": fixture.writer.writer_epoch,
    }


async def test_allocation_commit_and_failure_recorder_errors_propagate_hard_failure(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    with pytest.raises(
        ReconciliationFailurePersistenceError,
        match="failed to persist reconciliation failure",
    ) as caught:
        await reconcile_shadow_once(
            session_factory,
            fixture.writer,
            store=CommitAndRecorderFailureStore(),
        )

    assert caught.value.__cause__ is not None
    assert "synthetic recorder failure" in str(caught.value.__cause__)


async def test_allocation_input_drift_during_failure_record_still_freezes(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    store = FailureInputDriftStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        allocator=ActiveInvalidAllocator(),
        store=store,
    )

    assert result.status == "failed"
    assert store.expected_digest is not None
    assert store.observed_digest is not None
    assert store.expected_digest != store.observed_digest
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    audit = (
        (
            await capacity_session.execute(
                select(CapacityAuditEvent).order_by(CapacityAuditEvent.id.desc())
            )
        )
        .scalars()
        .first()
    )
    assert authority.execution_epoch == active.execution_epoch
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_invalid"
    assert audit is not None
    assert audit.detail["input_digest"] == store.expected_digest
    assert audit.detail["observed_input_digest"] == store.observed_digest
    assert audit.detail["input_drifted"] is True


async def test_allocation_commit_rejects_an_expired_freshness_fence(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    store = ExpiredFreshnessFenceStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=store,
    )

    assert result.status == "input-contention"
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_input_contention"


async def test_allocation_commit_rechecks_freshness_after_persistence(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    store = CrossingFreshnessFenceStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=store,
    )

    assert store.validity_checks == 2
    assert result.status == "input-contention"
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_input_contention"


async def test_allocation_commit_schema_qualifies_seal_guard_under_hostile_search_path(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    await capacity_session.execute(text("CREATE SCHEMA capacity_constraint_decoy"))
    await capacity_session.execute(
        text(
            "CREATE TABLE capacity_constraint_decoy.guard_decoy ("
            "value integer CONSTRAINT capacity_executable_allocation_seal_guard "
            "UNIQUE DEFERRABLE INITIALLY DEFERRED)"
        )
    )
    await capacity_session.execute(
        text(
            """
            CREATE OR REPLACE FUNCTION public.capacity_executable_allocation_seal_guard()
            RETURNS trigger
            LANGUAGE plpgsql
            SET search_path = pg_catalog
            AS $function$
            BEGIN
              IF NEW.status = 'executable' THEN
                RAISE EXCEPTION 'synthetic executable seal failure'
                  USING ERRCODE = '23514';
              END IF;
              RETURN NULL;
            END;
            $function$
            """
        )
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=HostileConstraintSearchPathStore(),
    )

    assert result.status == "failed"
    failed_epoch = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    assert failed_epoch.status == "failed"
    assert failed_epoch.executable is False
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_failure"


async def test_allocation_commit_samples_final_time_after_seal_validation(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    statements: list[str] = []

    def capture_statement(
        _connection,  # type: ignore[no-untyped-def]
        _cursor,  # type: ignore[no-untyped-def]
        statement: str,
        _parameters,  # type: ignore[no-untyped-def]
        _context,  # type: ignore[no-untyped-def]
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    engine = capacity_session.bind
    assert engine is not None
    event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        result = await reconcile_shadow_once(
            session_factory,
            fixture.writer,
            max_attempts=1,
            store=fixture.store,
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)

    assert result.status == "committed"
    clock_samples = [
        index for index, statement in enumerate(statements) if "clock_timestamp" in statement
    ]
    assert clock_samples
    seal_validation = statements.index(
        "SET CONSTRAINTS public.capacity_executable_allocation_seal_guard IMMEDIATE"
    )
    assert seal_validation < clock_samples[-1]


async def test_allocation_active_failure_rejects_a_stale_authority_fence(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    successor = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )

    with pytest.raises(StaleWriterError):
        await fixture.store.record_reconcile_failure(
            capacity_session,
            fixture.writer,
            event_kind="shadow_allocation_failure",
            reason="synthetic stale active failure",
            expected_input_digest=None,
        )

    assert successor.writer_epoch > fixture.writer.writer_epoch
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None


async def test_allocation_active_failure_freeze_blocks_later_promotion(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    await fixture.store.record_reconcile_failure(
        capacity_session,
        fixture.writer,
        event_kind="shadow_allocation_failure",
        reason="synthetic active failure",
        expected_input_digest=None,
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=fixture.store,
    )

    assert result.status == "failed"
    statuses = (await capacity_session.execute(select(CapacityAllocationEpoch.status))).scalars()
    assert set(statuses) == {"failed"}


async def test_shadow_read_routes_hide_executable_epochs(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _app, _settings, _allocator = api_context
    async with capacity_session_factory() as session:
        fixture, _active = await _active_execution_fixture(session)
        await session.commit()
    result = await reconcile_shadow_once(
        capacity_session_factory,
        fixture.writer,
        store=fixture.store,
    )
    assert result.status == "committed"
    assert result.allocation_epoch is not None

    epoch_response = client.get(
        f"/v1/shadow-epochs/{result.allocation_epoch}",
        headers=operator_headers,
    )
    allocations_response = client.get(
        f"/v1/shadow-epochs/{result.allocation_epoch}/allocations",
        headers=operator_headers,
    )

    assert epoch_response.status_code == 404
    assert epoch_response.json() == {"detail": "shadow epoch not found"}
    assert allocations_response.status_code == 404
    assert allocations_response.json() == {"detail": "shadow epoch not found"}


def test_body_limit_and_status_pagination_are_bounded(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    oversized = client.put(
        "/v1/config-proposals/fleet",
        headers=operator_headers
        | {
            "Idempotency-Key": str(uuid4()),
            "Content-Type": "application/json",
        },
        content=b"x" * (MAX_CONTRACT_BYTES + 1),
    )
    assert oversized.status_code == 413
    assert client.get("/v1/status/subjects?limit=501", headers=operator_headers).status_code == 422
    status_response = client.get("/v1/status", headers=operator_headers)
    assert status_response.status_code == 200
    status_body = status_response.json()
    assert status_body["observer_principal_id"] == "fleet-operator"
    assert status_body["execution_state"] == "shadow"
    assert status_body["execution_epoch"] == 0
    assert status_body["execution_manifest_sha256"] is None
    assert status_body["executable_new_capacity_ceiling"] == 0
    assert (
        client.post(
            "/v1/execution-activations",
            headers=operator_headers | {"Idempotency-Key": str(uuid4())},
            json={},
        ).status_code
        == 404
    )


def test_metrics_have_no_subject_or_environment_labels(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    response = client.get("/metrics", headers=operator_headers)
    assert response.status_code == 200
    body = response.text
    assert "loom_capacity_manager_ready" in body
    assert "subject_id=" not in body
    assert "environment=" not in body
    assert str(SUBJECT_ID) not in body


async def test_health_fails_closed_after_another_process_takes_writer_fence(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _app, _settings, _allocator = api_context
    async with capacity_session_factory() as session:
        await CapacityManagementStore().register_writer(
            session,
            AUTHORITY_ID,
            expected_epoch=1,
        )
        await session.commit()

    response = client.get("/healthz")
    assert response.status_code == 503
    assert response.json() == {
        "status": "not-ready",
        "executable_new_capacity_ceiling": 0,
    }


def test_active_execution_ceiling_does_not_fence_current_writer_health() -> None:
    writer = WriterFence(authority_incarnation=AUTHORITY_ID, writer_epoch=3)
    authority = CapacityAuthorityState(
        authority_incarnation=AUTHORITY_ID,
        writer_epoch=3,
        executable_new_capacity_ceiling=1,
    )

    assert _writer_matches_authority(writer, authority)
    assert json.loads(_health_payload(ready=True, executable_new_capacity_ceiling=1)) == {
        "status": "ready",
        "executable_new_capacity_ceiling": 1,
    }
