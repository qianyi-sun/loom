from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, HTTPException
from pydantic import SecretStr
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request

from loom.auth import AuthContext, verify_step_jwt
from loom.db.schema import (
    AdminAuditEvent,
    Artifact,
    Batch,
    ExecutionAdmissionPolicy,
    ExecutionAdmissionReservation,
    ExecutionBudgetPolicy,
    ExecutionCapacityObservation,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionCostReservationDebit,
    ExecutionNodeCostAllocation,
    ExecutionNodeCostRecord,
    ExecutionPriceSnapshot,
    ExecutionProvisioningAuthorization,
    ExecutionTargetPriceBinding,
    ServiceExecutionCommand,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    ServiceExecutionLeaseHistory,
    ServiceExecutionTarget,
    Task,
    Team,
    TeamQuota,
    Trial,
)
from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    CapacityEvidenceKind,
    ExecutionAdapterKind,
    ExecutionRouteCandidateV1,
    ExecutionRoutingDecisionV1,
    ExecutionRoutingReason,
    ExecutionTargetV1,
    ImageMaterialization,
    IsolationLevel,
    NetworkAccess,
    VerifierTopology,
    WorkloadRequirementsV1,
)
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProcessPhaseV1,
)
from loom.pipeline.artifact_commit import ArtifactCommitService, PartReceiptV1
from loom.pipeline.keys import canonical_digest, canonical_document, digest_bytes
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.execution_capacity import (
    create_execution_capacity_observation,
    fetch_execution_capacity_status,
    reserve_execution_provisioning,
    upsert_execution_capacity_policy,
)
from loom_control_plane.execution_finance import (
    create_execution_price_snapshot,
    record_execution_node_cost,
    settle_execution_cost_reservation,
    upsert_execution_budget_policy,
    upsert_target_price_binding,
)
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    ServiceExecutionFenceError,
    acknowledge_execution_command,
    claim_execution_commands,
    enqueue_execution_transition,
    execution_lease_projection,
    persist_execution_catalog,
    record_execution_event,
    refresh_execution_target_health,
    request_trial_execution_cancellation,
    reserve_trial_execution,
    set_execution_target_health,
    verify_trial_execution_fence,
)
from loom_control_plane.service_execution_output import (
    ServiceExecutionBrokerError,
    ServiceExecutionOutputFileV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionOutputRouteService,
    ServiceExecutionPeerV1,
    authorize_service_execution_peer,
    mint_service_execution_peer_token,
)
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from loom_execution_actuator.contracts import (
    ExecutionTerminationSummaryV1,
    KubernetesApiError,
    KubernetesJobInventory,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_llm_gateway.execution_attempt_dispatch import authorize_trial_execution_dispatch
from tests.support.execution_image_admission import (
    IMAGE_ADMISSION_KEYRING,
    signed_image_admission_bundle,
)


@pytest.fixture(autouse=True)
async def _cleanup_service_execution_test_rows(postgres_url: str):  # type: ignore[no-untyped-def]
    """Keep the session Postgres fixture compatible with legacy broad cleanup fixtures."""

    yield
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(Artifact).where(Artifact.control_producer_kind == "service_execution")
            )
            await session.execute(
                delete(AdminAuditEvent).where(
                    AdminAuditEvent.action == "service_execution.step_token.minted"
                )
            )
            owned_trials = select(Trial.id).join(Team).where(Team.name.like("service-execution-%"))
            owned_cost_reservations = select(ExecutionCostReservation.id).where(
                ExecutionCostReservation.trial_id.in_(owned_trials)
            )
            await session.execute(
                delete(ExecutionProvisioningAuthorization).where(
                    ExecutionProvisioningAuthorization.lease_id.in_(
                        select(ServiceExecutionLease.id).where(
                            ServiceExecutionLease.trial_id.in_(owned_trials)
                        )
                    )
                )
            )
            await session.execute(
                delete(ExecutionNodeCostAllocation).where(
                    ExecutionNodeCostAllocation.cost_reservation_id.in_(owned_cost_reservations)
                )
            )
            await session.execute(
                delete(ExecutionNodeCostRecord).where(
                    ExecutionNodeCostRecord.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ExecutionCostReservationDebit).where(
                    ExecutionCostReservationDebit.reservation_id.in_(owned_cost_reservations)
                )
            )
            await session.execute(
                delete(ExecutionCostReservation).where(
                    ExecutionCostReservation.id.in_(owned_cost_reservations)
                )
            )
            await session.execute(
                delete(ServiceExecutionLease).where(
                    ServiceExecutionLease.trial_id.in_(owned_trials)
                )
            )
            await session.execute(delete(Trial).where(Trial.id.in_(owned_trials)))
            await session.execute(
                delete(Batch).where(
                    Batch.team_id.in_(select(Team.id).where(Team.name.like("service-execution-%")))
                )
            )
            await session.execute(delete(Task).where(Task.id.like("service-execution/%")))
            await session.execute(
                delete(ExecutionTargetPriceBinding).where(
                    ExecutionTargetPriceBinding.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ExecutionBudgetPolicy).where(
                    (ExecutionBudgetPolicy.scope_key == "nebius-cpu")
                    | (ExecutionBudgetPolicy.scope_key.like("nebius-staging-%"))
                )
            )
            await session.execute(
                delete(ExecutionCapacityObservation).where(
                    ExecutionCapacityObservation.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ExecutionCapacityPolicy).where(
                    ExecutionCapacityPolicy.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ServiceExecutionTarget).where(
                    ServiceExecutionTarget.id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ExecutionPriceSnapshot).where(
                    ExecutionPriceSnapshot.source == "service-execution-test"
                )
            )
            await session.execute(
                delete(TeamQuota).where(
                    TeamQuota.team_id.in_(
                        select(Team.id).where(Team.name.like("service-execution-%"))
                    )
                )
            )
            await session.execute(delete(Team).where(Team.name.like("service-execution-%")))
    finally:
        await engine.dispose()


class _FakeKubernetesJobApi:
    def __init__(self, *, ambiguous_create: bool = False) -> None:
        self.jobs: dict[str, KubernetesJobObservation] = {}
        self.watch_events: list[KubernetesJobObservation] = []
        self.create_count = 0
        self.delete_count = 0
        self.ambiguous_create = ambiguous_create

    async def get_job(self, *, namespace: str, job_name: str) -> KubernetesJobObservation | None:
        observation = self.jobs.get(job_name)
        assert observation is None or observation.namespace == namespace
        return observation

    async def create_job(
        self, *, namespace: str, manifest: dict[str, object]
    ) -> KubernetesJobObservation:
        metadata = manifest["metadata"]
        assert isinstance(metadata, dict)
        labels = metadata["labels"]
        annotations = metadata["annotations"]
        assert isinstance(labels, dict) and isinstance(annotations, dict)
        job_name = str(metadata["name"])
        self.create_count += 1
        observation = KubernetesJobObservation(
            namespace=namespace,
            job_name=job_name,
            lease_id=str(labels["loom.openai.com/lease-id"]),
            resource_generation=int(str(labels["loom.openai.com/generation"])),
            target_id=str(annotations["loom.openai.com/target-id"]),
            execution_unit_key=str(annotations["loom.openai.com/execution-unit-key"]),
            normalized_state=NormalizedJobState.PENDING,
            job_uid=f"job-uid-{job_name}",
            resource_version="1",
        )
        self.jobs[job_name] = observation
        if self.ambiguous_create:
            self.ambiguous_create = False
            raise KubernetesApiError("response lost", ambiguous=True)
        return observation

    async def delete_job(
        self,
        *,
        namespace: str,
        job_name: str,
        expected_uid: str,
        grace_period_seconds: int,
    ) -> None:
        assert grace_period_seconds >= 0
        current = self.jobs.get(job_name)
        if current is None:
            return
        assert current.namespace == namespace
        assert current.job_uid == expected_uid
        self.delete_count += 1
        del self.jobs[job_name]

    async def list_jobs(self, *, namespace: str, label_selector: str) -> KubernetesJobInventory:
        assert label_selector == "app.kubernetes.io/managed-by=loom-execution-actuator"
        return KubernetesJobInventory(
            tuple(item for item in self.jobs.values() if item.namespace == namespace)
        )

    async def watch_jobs(
        self,
        *,
        namespace: str,
        label_selector: str,
        resource_version: str | None,
        timeout_seconds: int,
    ) -> tuple[KubernetesJobObservation, ...]:
        del label_selector, resource_version, timeout_seconds
        events = tuple(item for item in self.watch_events if item.namespace == namespace)
        self.watch_events.clear()
        return events


def _target(suffix: str) -> ExecutionTargetV1:
    return ExecutionTargetV1(
        target_id=f"nebius-staging-{suffix}",
        logical_pool_id="nebius-cpu",
        execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
        cluster_scope_id="nebius-eu-north1-shared",
        environment="staging",
        provider="nebius",
        region="eu-north1",
        failure_domain=f"eu-north1-{suffix}",
        data_residency="eu",
        namespace_name=f"loom-nebius-{suffix}",
        health_role="primary",
        health_check_id=f"nebius-staging-health-{suffix}",
        health_check_interval_seconds=10,
        health_stale_after_seconds=60,
    )


def _requirements(
    *,
    verifier_topology: VerifierTopology = VerifierTopology.IN_ATTEMPT,
    data_residency: str | None = None,
) -> WorkloadRequirementsV1:
    return WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        data_residency=data_residency,
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=1000,
        memory_mib=1024,
        ephemeral_storage_mib=2048,
        isolation_level=IsolationLevel.SHARED_KERNEL,
        network_access=NetworkAccess.GATEWAY_ONLY,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref="registry.example/loom/task@sha256:" + "a" * 64,
        sidecar_count=0,
        verifier_topology=verifier_topology,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )


def _runtime_contract(
    *,
    execution_role: str = "attempt",
    verifier_execution: str = "in_attempt",
    now: datetime | None = None,
) -> ExecutionRuntimePlanV1:
    resources = ContainerResourcesV1(
        cpu_millis=1000,
        memory_mib=1024,
        ephemeral_storage_mib=2048,
    )
    task_image_ref = "registry.example/loom/task@sha256:" + "a" * 64
    runtime_image_ref = "registry.example/loom/runtime@sha256:" + "b" * 64
    return ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_role=execution_role,
        execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
        composition="init_payload",
        task_image_ref=task_image_ref,
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256="sha256:" + "c" * 64,
        image_admission=signed_image_admission_bundle((task_image_ref, runtime_image_ref), now=now),
        task_resources=resources,
        workspace_mib=1024,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent" if execution_role == "attempt" else "verifier",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
        verifier_execution=verifier_execution,
        verifier=(
            ProcessPhaseV1(
                role="verifier",
                argv=("/bin/true",),
                working_directory="/workspace",
                timeout_seconds=60,
            )
            if verifier_execution == "in_attempt"
            else None
        ),
    )


def _runtime_result_payload(
    lease: ServiceExecutionLease,
    *,
    started_at: datetime,
) -> dict[str, object]:
    assert lease.runtime_contract_json is not None
    assert lease.runtime_contract_sha256 is not None
    plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
    phases: list[dict[str, object]] = []
    for ordinal, role in enumerate(("agent", "verifier"), start=1):
        phases.append(
            {
                "role": role,
                "ordinal": ordinal,
                "started_at": started_at.isoformat(),
                "finished_at": (started_at + timedelta(seconds=ordinal)).isoformat(),
                "exit_code": 0,
                "signal": None,
                "timed_out": False,
                "stdout": {
                    "path": f"{ordinal:02d}-{role}.stdout",
                    "sha256": "sha256:" + "4" * 64,
                    "bytes_seen": 10,
                    "bytes_saved": 10,
                    "truncated": False,
                },
                "stderr": {
                    "path": f"{ordinal:02d}-{role}.stderr",
                    "sha256": "sha256:" + "5" * 64,
                    "bytes_seen": 0,
                    "bytes_saved": 0,
                    "truncated": False,
                },
            }
        )
    return {
        "schema_version": "loom.execution-runtime-result.v1",
        "runtime_contract_sha256": lease.runtime_contract_sha256,
        "candidate_sha": plan.candidate_sha,
        "task_revision_sha256": plan.task_revision_sha256,
        "command_identity_sha256": plan.command_identity_sha256,
        "execution_role": plan.execution_role,
        "container_roles": ["execution", "agent", "verifier"],
        "task_image_ref": plan.task_image_ref,
        "runtime_image_ref": plan.runtime_image_ref,
        "runtime_binary_sha256": plan.runtime_binary_sha256,
        "execution_class_id": plan.execution_class_id,
        "status": "succeeded",
        "started_at": started_at.isoformat(),
        "finished_at": (started_at + timedelta(seconds=3)).isoformat(),
        "phases": phases,
        "partial_evidence": False,
    }


async def _seed_ready_trial(
    session: AsyncSession,
    *,
    now: datetime,
) -> tuple[UUID, ExecutionTargetV1]:
    suffix = uuid4().hex[:12]
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"service-execution/{suffix}"
    target = _target(suffix)
    session.add_all(
        (
            Team(id=team_id, name=f"service-execution-{suffix}"),
            Task(
                id=task_id,
                checksum="b" * 64,
                config={"schema_version": "1", "task": {"id": task_id}},
            ),
            Trial(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={"agent": {"name": "test"}},
                requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                state="queued",
                attempt_count=0,
            ),
        )
    )
    await persist_execution_catalog(
        session,
        execution_class=NEBIUS_CPU_EXECUTION_CLASS_V1,
        targets=(target,),
    )
    await set_execution_target_health(
        session,
        target_id=target.target_id,
        desired_state="active",
        observed_state="ready",
        health_status="healthy",
        observed_at=now,
    )
    price, _ = await create_execution_price_snapshot(
        session,
        provider="nebius",
        region=target.region,
        sku="test-node",
        source="service-execution-test",
        source_version=target.target_id,
        source_uri="https://example.test/nebius-price",
        effective_at=now - timedelta(days=1),
        observed_at=now,
        base_microusd_per_hour=3_600_000,
        vcpu_microusd_per_hour=0,
        memory_gib_microusd_per_hour=0,
        ephemeral_storage_gib_microusd_per_hour=0,
    )
    await upsert_target_price_binding(
        session,
        target_id=target.target_id,
        price_snapshot_id=price.id,
        enabled=True,
        reason="service execution test",
        now=now,
    )
    for scope_kind, scope_key in (
        ("pool", target.logical_pool_id),
        ("target", target.target_id),
    ):
        await upsert_execution_budget_policy(
            session,
            scope_kind=scope_kind,  # type: ignore[arg-type]
            scope_key=scope_key,
            daily_limit_microusd=100_000_000,
            monthly_limit_microusd=1_000_000_000,
            per_attempt_limit_microusd=10_000_000,
            max_estimate_duration_seconds=7_200,
            emergency_stop=False,
            enabled=True,
            reason="service execution test",
            now=now,
        )
    await upsert_execution_capacity_policy(
        session,
        target_id=target.target_id,
        enabled=True,
        max_nodes=20,
        max_vcpu_millis=1_280_000,
        max_memory_mib=5_242_880,
        max_storage_mib=20_971_520,
        node_cpu_millis=64_000,
        node_memory_mib=262_144,
        node_storage_mib=1_048_576,
        max_pending_jobs=100,
        max_unschedulable_jobs=10,
        max_image_pull_backoff_jobs=10,
        max_create_per_minute=100,
        observation_max_age_seconds=900,
        reason="service execution test",
        now=now,
    )
    await create_execution_capacity_observation(
        session,
        target_id=target.target_id,
        source="service-execution-test",
        source_version=target.target_id,
        observed_at=now,
        provider_capacity_state="available",
        provider_capacity_reason=None,
        autoscaler_state="ready",
        autoscaler_reason=None,
        provider_quota_nodes=20,
        provider_quota_vcpu_millis=1_280_000,
        provider_quota_memory_mib=5_242_880,
        provider_quota_storage_mib=20_971_520,
        provider_used_nodes=1,
        provider_used_vcpu_millis=64_000,
        provider_used_memory_mib=262_144,
        provider_used_storage_mib=1_048_576,
        active_nodes=1,
        provisioned_vcpu_millis=64_000,
        provisioned_memory_mib=262_144,
        provisioned_storage_mib=1_048_576,
        allocatable_cpu_millis=64_000,
        allocatable_memory_mib=262_144,
        allocatable_storage_mib=1_048_576,
        requested_cpu_millis=0,
        requested_memory_mib=0,
        requested_storage_mib=0,
        pending_jobs=0,
        unschedulable_jobs=0,
        image_pull_backoff_jobs=0,
        pending_reasons={},
    )
    return trial_id, target


async def _reserve(
    session: AsyncSession,
    *,
    trial_id: UUID,
    target: ExecutionTargetV1,
    now: datetime,
    request_id: UUID | None = None,
    requirements: WorkloadRequirementsV1 | None = None,
    runtime_contract: ExecutionRuntimePlanV1 | None = None,
    parent_lease_id: UUID | None = None,
) -> ServiceExecutionLease:
    return await reserve_trial_execution(
        session,
        request_id=request_id or uuid4(),
        trial_id=trial_id,
        execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
        target_id=target.target_id,
        requirements=requirements or _requirements(),
        runtime_contract=runtime_contract or _runtime_contract(now=now),
        image_admission_keyring=IMAGE_ADMISSION_KEYRING,
        parent_lease_id=parent_lease_id,
        deadline_at=now + timedelta(hours=1),
        now=now,
    )


async def test_actuator_refreshes_target_health_without_reenabling_operator_state(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            _trial_id, target = await _seed_ready_trial(session, now=now)
            await set_execution_target_health(
                session,
                target_id=target.target_id,
                desired_state="draining",
                observed_state="ready",
                health_status="healthy",
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=_FakeKubernetesJobApi(),
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
            ),
            controller_id="target-health-refresh-test",
        )
        refreshed_at = now + timedelta(seconds=2)
        assert await actuator.reconcile_full_once(now=refreshed_at) == 0

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionTarget, target.target_id)
            assert persisted is not None
            assert persisted.desired_state == "draining"
            assert persisted.observed_state == "ready"
            assert persisted.health_status == "healthy"
            assert persisted.health_observed_at == refreshed_at
            with pytest.raises(
                ServiceExecutionConflict,
                match="health observation regressed",
            ):
                await refresh_execution_target_health(
                    session,
                    target_id=target.target_id,
                    observed_at=refreshed_at - timedelta(seconds=1),
                )
    finally:
        await engine.dispose()


async def test_reservation_persists_trial_lease_command_and_history_atomically(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    request_id = uuid4()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                request_id=request_id,
            )
            await session.commit()
            lease_id = lease.id

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            persisted = await session.get(ServiceExecutionLease, lease_id)
            commands = (
                (
                    await session.execute(
                        select(ServiceExecutionCommand).where(
                            ServiceExecutionCommand.lease_id == lease_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            history = (
                await session.execute(
                    select(ServiceExecutionLeaseHistory).where(
                        ServiceExecutionLeaseHistory.lease_id == lease_id
                    )
                )
            ).scalar_one()
            cost_reservation = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease_id
                    )
                )
            ).scalar_one()
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("claimed", 1)
            route = ExecutionRoutingDecisionV1.model_validate(trial.execution_route_json)
            assert route.reason == ExecutionRoutingReason.ADMIN_TARGET_BINDING
            assert route.selected_pool_id == "nebius-cpu"
            assert persisted is not None
            assert (persisted.attempt, persisted.generation) == (1, 1)
            assert persisted.routing_generation == route.generation
            assert persisted.selected_pool_id == route.selected_pool_id
            assert persisted.routing_decision_sha256 == trial.execution_route_sha256
            assert persisted.last_event_ordinal == 0
            assert len(commands) == 1
            assert (commands[0].command_type, commands[0].state) == ("create", "pending")
            assert commands[0].payload_json["selected_pool_id"] == "nebius-cpu"
            assert commands[0].payload_json["cost_reservation_id"] == str(cost_reservation.id)
            assert commands[0].payload_json["price_snapshot_id"] == str(
                cost_reservation.price_snapshot_id
            )
            assert commands[0].payload_json["estimated_cost_microusd"] == 3_600_000
            assert cost_reservation.estimated_cost_microusd == 3_600_000
            assert cost_reservation.requested_cpu_millis == 1_050
            assert cost_reservation.requested_memory_mib == 1_088
            assert cost_reservation.requested_ephemeral_storage_mib == 4_180
            assert history.snapshot_json["selected_pool_id"] == "nebius-cpu"
            projection = execution_lease_projection(persisted)
            assert projection["selected_pool_id"] == "nebius-cpu"
            assert projection["runtime_identity"]["image_admission_sha256"].startswith("sha256:")
            assert {
                item["image_ref"] for item in projection["runtime_identity"]["image_admissions"]
            } == {
                "registry.example/loom/task@sha256:" + "a" * 64,
                "registry.example/loom/runtime@sha256:" + "b" * 64,
            }

            replay = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                request_id=request_id,
            )
            assert replay.id == lease_id

        async with sessions() as session:
            with pytest.raises(DBAPIError, match="route can change only while queued"):
                await session.execute(
                    update(Trial)
                    .where(Trial.id == trial_id)
                    .values(execution_route_pool_name="gb10")
                )
                await session.commit()
            await session.rollback()

        async with sessions() as session:
            with pytest.raises(DBAPIError, match="immutable identity changed"):
                await session.execute(
                    update(ServiceExecutionLease)
                    .where(ServiceExecutionLease.id == lease_id)
                    .values(selected_pool_id="gb10")
                )
                await session.commit()
            await session.rollback()
    finally:
        await engine.dispose()


async def test_normal_scheduler_reserves_converted_task_without_budget_policy(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            session.add(TeamQuota(team_id=trial.team_id))
            task = await session.get(Task, trial.task_id)
            assert task is not None
            plan = _runtime_contract(now=now)
            task.checksum = plan.task_revision_sha256.removeprefix("sha256:")
            task.config = {
                "schema_version": "1",
                "task": {"id": task.id, "name": "Service execution scheduler"},
                "environment": {
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "docker_image": plan.task_image_ref,
                    "cpus": 1,
                    "memory_mb": 1024,
                    "storage_mb": 2048,
                    "tmpfs": ["/tmp"],
                    "baseline_network_policy": {"kind": "gateway-only"},
                    "network_policies_supported": ["gateway-only"],
                },
                "agent": {"name": "service-smoke"},
                "verifier": {"name": "script"},
                "service_execution": {
                    "schema_version": "loom.task-service-execution.v1",
                    "logical_pool_id": "nebius-cpu",
                    "runtime_template": plan.model_dump(
                        mode="json", exclude={"task_revision_sha256"}
                    ),
                },
            }
            trial.requires_caps = {
                "os": "linux",
                "cpu_arch": "x86_64",
                "gpu_vendor": "none",
                "network_policies": ["gateway-only"],
                "backend": "docker",
                "worker_pool": "nebius-cpu",
            }
            await session.execute(
                delete(ExecutionBudgetPolicy).where(
                    ExecutionBudgetPolicy.scope_key.in_((target.logical_pool_id, target.target_id))
                )
            )
            await session.commit()

        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=now,
            )
            await session.commit()
            assert lease is not None
            assert lease.trial_id == trial_id
            assert lease.target_id == target.target_id
            assert lease.selected_pool_id == "nebius-cpu"
            assert lease.routing_reason == ExecutionRoutingReason.PREEXISTING_ASSIGNMENT
            assert lease.desired_state == "create"

        async with sessions() as session:
            persisted_trial = await session.get(Trial, trial_id)
            assert persisted_trial is not None
            assert persisted_trial.state == "claimed"
            assert persisted_trial.worker_id is None
            assert (
                await session.scalar(
                    select(func.count(ExecutionCostReservation.id)).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count(ExecutionCostReservationDebit.reservation_id)).where(
                        ExecutionCostReservationDebit.reservation_id
                        == select(ExecutionCostReservation.id)
                        .where(ExecutionCostReservation.lease_id == lease.id)
                        .scalar_subquery()
                    )
                )
                == 0
            )
    finally:
        await engine.dispose()


async def test_nebius_budget_reservation_is_race_safe_across_service_leases(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    try:
        async with sessions() as session:
            first_trial_id, target = await _seed_ready_trial(session, now=now)
            first = await session.get(Trial, first_trial_id)
            assert first is not None
            second_trial_id = uuid4()
            session.add(
                Trial(
                    id=second_trial_id,
                    team_id=first.team_id,
                    task_id=first.task_id,
                    config={"agent": {"name": "test"}},
                    requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                    state="queued",
                    attempt_count=0,
                )
            )
            for scope_kind, scope_key in (
                ("pool", target.logical_pool_id),
                ("target", target.target_id),
            ):
                await upsert_execution_budget_policy(
                    session,
                    scope_kind=scope_kind,  # type: ignore[arg-type]
                    scope_key=scope_key,
                    daily_limit_microusd=3_600_000,
                    monthly_limit_microusd=3_600_000,
                    per_attempt_limit_microusd=3_600_000,
                    max_estimate_duration_seconds=3_600,
                    emergency_stop=False,
                    enabled=True,
                    reason="one paid attempt",
                    now=now,
                )
            await session.commit()

        async def reserve(trial_id: UUID) -> ServiceExecutionLease | str:
            async with sessions() as session:
                try:
                    lease = await _reserve(
                        session,
                        trial_id=trial_id,
                        target=target,
                        now=now,
                    )
                    await session.commit()
                    return lease
                except ServiceExecutionConflict as exc:
                    await session.rollback()
                    return str(exc)

        results = await asyncio.gather(reserve(first_trial_id), reserve(second_trial_id))
        leases = [result for result in results if isinstance(result, ServiceExecutionLease)]
        blockers = [result for result in results if isinstance(result, str)]
        assert len(leases) == 1
        assert blockers == ["execution_budget_pool_daily_limit_exceeded"]

        async with sessions() as session:
            policies = (
                (
                    await session.execute(
                        select(ExecutionBudgetPolicy).where(
                            ExecutionBudgetPolicy.scope_key.in_(
                                (target.logical_pool_id, target.target_id)
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert {row.daily_reserved_microusd for row in policies} == {3_600_000}
            assert await session.scalar(select(func.count(ExecutionCostReservation.id))) == 1
    finally:
        await engine.dispose()


async def test_nebius_paid_execution_fails_closed_without_price_or_during_emergency_stop(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            binding = await session.get(ExecutionTargetPriceBinding, target.target_id)
            assert binding is not None
            binding.enabled = False
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                ServiceExecutionConflict,
                match="execution_cost_price_binding_unavailable",
            ):
                await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.rollback()

        async with sessions() as session:
            binding = await session.get(ExecutionTargetPriceBinding, target.target_id)
            assert binding is not None
            binding.enabled = True
            pool_policy = (
                await session.execute(
                    select(ExecutionBudgetPolicy).where(
                        ExecutionBudgetPolicy.scope_kind == "pool",
                        ExecutionBudgetPolicy.scope_key == target.logical_pool_id,
                    )
                )
            ).scalar_one()
            pool_policy.emergency_stop = True
            pool_policy.reason = "provider emergency stop test"
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                ServiceExecutionConflict,
                match="execution_budget_pool_emergency_stop",
            ):
                await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.rollback()
            assert await session.scalar(select(func.count(ExecutionCostReservation.id))) == 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    (
        ("pending", "execution_capacity_pending_limit_exceeded"),
        ("quota", "execution_capacity_provider_quota_nodes_exceeded"),
        ("physical", "execution_capacity_physical_capacity_unavailable"),
        ("stale", "execution_capacity_observation_stale"),
    ),
)
async def test_actuator_defers_create_with_distinct_capacity_blocker(
    postgres_url: str,
    case: str,
    expected_reason: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await upsert_execution_capacity_policy(
                session,
                target_id=target.target_id,
                enabled=True,
                max_nodes=20,
                max_vcpu_millis=1_280_000,
                max_memory_mib=5_242_880,
                max_storage_mib=20_971_520,
                node_cpu_millis=64_000,
                node_memory_mib=262_144,
                node_storage_mib=1_048_576,
                max_pending_jobs=1 if case == "pending" else 100,
                max_unschedulable_jobs=10,
                max_image_pull_backoff_jobs=10,
                max_create_per_minute=100,
                observation_max_age_seconds=10 if case == "stale" else 900,
                reason=f"{case} blocker test",
                now=now + timedelta(seconds=1),
            )
            quota_nodes = 1 if case == "quota" else 20
            quota_vcpu = 64_000 if case == "quota" else 1_280_000
            quota_memory = 262_144 if case == "quota" else 5_242_880
            quota_storage = 1_048_576 if case == "quota" else 20_971_520
            cluster_requested = 64_000 if case in {"quota", "physical"} else 0
            await create_execution_capacity_observation(
                session,
                target_id=target.target_id,
                source="service-execution-test",
                source_version=f"{target.target_id}-{case}-blocker",
                observed_at=now + timedelta(seconds=1),
                provider_capacity_state=("insufficient" if case == "physical" else "available"),
                provider_capacity_reason=(
                    "provider reports no physical placement" if case == "physical" else None
                ),
                autoscaler_state="ready",
                autoscaler_reason=None,
                provider_quota_nodes=quota_nodes,
                provider_quota_vcpu_millis=quota_vcpu,
                provider_quota_memory_mib=quota_memory,
                provider_quota_storage_mib=quota_storage,
                provider_used_nodes=1,
                provider_used_vcpu_millis=64_000,
                provider_used_memory_mib=262_144,
                provider_used_storage_mib=1_048_576,
                active_nodes=1,
                provisioned_vcpu_millis=64_000,
                provisioned_memory_mib=262_144,
                provisioned_storage_mib=1_048_576,
                allocatable_cpu_millis=64_000,
                allocatable_memory_mib=262_144,
                allocatable_storage_mib=1_048_576,
                requested_cpu_millis=cluster_requested,
                requested_memory_mib=(262_144 if cluster_requested else 0),
                requested_storage_mib=(1_048_576 if cluster_requested else 0),
                pending_jobs=1 if case == "pending" else 0,
                unschedulable_jobs=0,
                image_pull_backoff_jobs=0,
                pending_reasons={"autoscaler_delay": 1} if case == "pending" else {},
            )
            await session.commit()

        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
                runtime_class_name="loom-sandbox",
            ),
            controller_id="capacity-blocker-test",
            command_lease_seconds=5,
        )
        actuator_time = now + timedelta(seconds=20 if case == "stale" else 2)
        assert await actuator.run_commands_once(now=actuator_time) == 1
        assert kubernetes.create_count == 0

        async with sessions() as session:
            command = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "create",
                    )
                )
            ).scalar_one()
            assert command.state == "pending"
            assert command.last_error_code == expected_reason
            assert (
                await session.scalar(
                    select(func.count(ExecutionProvisioningAuthorization.id)).where(
                        ExecutionProvisioningAuthorization.lease_id == lease.id
                    )
                )
                == 0
            )
            status = await fetch_execution_capacity_status(
                session,
                pool_id=target.logical_pool_id,
                now=actuator_time,
            )
            targets = status["targets"]
            assert isinstance(targets, list)
            target_status = targets[0]
            assert isinstance(target_status, dict)
            blockers = target_status["blockers"]
            assert isinstance(blockers, list)
            assert expected_reason in blockers
    finally:
        await engine.dispose()


async def test_provisioning_pending_limit_is_race_safe_across_actuators(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            first_trial_id, target = await _seed_ready_trial(session, now=now)
            first_trial = await session.get(Trial, first_trial_id)
            assert first_trial is not None
            second_trial_id = uuid4()
            session.add(
                Trial(
                    id=second_trial_id,
                    team_id=first_trial.team_id,
                    task_id=first_trial.task_id,
                    config={"agent": {"name": "test"}},
                    requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                    state="queued",
                    attempt_count=0,
                )
            )
            first_lease = await _reserve(
                session,
                trial_id=first_trial_id,
                target=target,
                now=now,
            )
            second_lease = await _reserve(
                session,
                trial_id=second_trial_id,
                target=target,
                now=now,
            )
            await upsert_execution_capacity_policy(
                session,
                target_id=target.target_id,
                enabled=True,
                max_nodes=20,
                max_vcpu_millis=1_280_000,
                max_memory_mib=5_242_880,
                max_storage_mib=20_971_520,
                node_cpu_millis=64_000,
                node_memory_mib=262_144,
                node_storage_mib=1_048_576,
                max_pending_jobs=1,
                max_unschedulable_jobs=10,
                max_image_pull_backoff_jobs=10,
                max_create_per_minute=100,
                observation_max_age_seconds=900,
                reason="one pending create at a time",
                now=now + timedelta(seconds=1),
            )
            await create_execution_capacity_observation(
                session,
                target_id=target.target_id,
                source="service-execution-test",
                source_version=f"{target.target_id}-race",
                observed_at=now + timedelta(seconds=1),
                provider_capacity_state="available",
                provider_capacity_reason=None,
                autoscaler_state="ready",
                autoscaler_reason=None,
                provider_quota_nodes=20,
                provider_quota_vcpu_millis=1_280_000,
                provider_quota_memory_mib=5_242_880,
                provider_quota_storage_mib=20_971_520,
                provider_used_nodes=1,
                provider_used_vcpu_millis=64_000,
                provider_used_memory_mib=262_144,
                provider_used_storage_mib=1_048_576,
                active_nodes=1,
                provisioned_vcpu_millis=64_000,
                provisioned_memory_mib=262_144,
                provisioned_storage_mib=1_048_576,
                allocatable_cpu_millis=64_000,
                allocatable_memory_mib=262_144,
                allocatable_storage_mib=1_048_576,
                requested_cpu_millis=0,
                requested_memory_mib=0,
                requested_storage_mib=0,
                pending_jobs=0,
                unschedulable_jobs=0,
                image_pull_backoff_jobs=0,
                pending_reasons={},
            )
            await session.commit()

        runtime = ExecutionTargetRuntime(
            target_id=target.target_id,
            namespace=target.namespace_name,
            runtime_class_name="loom-sandbox",
        )
        actuators = (
            ExecutionActuator(
                sessions=sessions,
                kubernetes=kubernetes,
                target=runtime,
                controller_id="capacity-race-a",
                command_limit=1,
                command_lease_seconds=5,
            ),
            ExecutionActuator(
                sessions=sessions,
                kubernetes=kubernetes,
                target=runtime,
                controller_id="capacity-race-b",
                command_limit=1,
                command_lease_seconds=5,
            ),
        )
        assert sorted(
            await asyncio.gather(
                *(item.run_commands_once(now=now + timedelta(seconds=2)) for item in actuators)
            )
        ) == [1, 1]
        assert kubernetes.create_count == 1

        async with sessions() as session:
            authorizations = (
                (
                    await session.execute(
                        select(ExecutionProvisioningAuthorization).where(
                            ExecutionProvisioningAuthorization.lease_id.in_(
                                (first_lease.id, second_lease.id)
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(authorizations) == 1
            commands = (
                (
                    await session.execute(
                        select(ServiceExecutionCommand).where(
                            ServiceExecutionCommand.lease_id.in_((first_lease.id, second_lease.id)),
                            ServiceExecutionCommand.command_type == "create",
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert {row.state for row in commands} == {"acknowledged", "pending"}
            blocked = next(row for row in commands if row.state == "pending")
            assert blocked.last_error_code == "execution_capacity_pending_limit_exceeded"
    finally:
        await engine.dispose()


async def test_provider_node_bill_allocates_requested_share_and_exposes_overhead(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    pod_started = now + timedelta(seconds=10)
    pod_stopped = now + timedelta(seconds=110)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-finance",
                    "pod_uid": "pod-finance",
                    "resource_version": "1",
                    "node_name": "node-finance",
                    "scheduled_at": (pod_started - timedelta(seconds=1)).isoformat(),
                    "started_at": pod_started.isoformat(),
                },
                observed_at=pod_started,
            )
            await session.commit()

        async with sessions() as session:
            reservation = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
            ).scalar_one()
            with pytest.raises(
                ValueError,
                match="without a persisted termination timestamp",
            ):
                await record_execution_node_cost(
                    session,
                    target_id=target.target_id,
                    price_snapshot_id=reservation.price_snapshot_id,
                    provider_record_id=f"premature-bill-{uuid4()}",
                    node_name="node-finance",
                    interval_started_at=pod_started,
                    interval_stopped_at=pod_started + timedelta(seconds=50),
                    node_cpu_millis=64_000,
                    node_memory_mib=262_144,
                    node_ephemeral_storage_mib=1_048_576,
                    provider_billed_microusd=1_000_000,
                    billing_source="nebius-invoice-export",
                    billing_source_version="invoice-2026-08",
                    observed_at=pod_started + timedelta(seconds=51),
                )
            await session.rollback()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "failed",
                    "job_uid": "job-finance",
                    "pod_uid": "pod-finance",
                    "resource_version": "2",
                    "node_name": "node-finance",
                    "scheduled_at": (pod_started - timedelta(seconds=1)).isoformat(),
                    "started_at": pod_started.isoformat(),
                    "terminated_at": pod_stopped.isoformat(),
                    "message": "test terminal",
                },
                observed_at=pod_stopped,
            )
            await session.commit()

        async with sessions() as session:
            reservation = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
            ).scalar_one()
            assert reservation.state == "awaiting_settlement"
            node_cost, created = await record_execution_node_cost(
                session,
                target_id=target.target_id,
                price_snapshot_id=reservation.price_snapshot_id,
                provider_record_id=f"bill-{uuid4()}",
                node_name="node-finance",
                interval_started_at=pod_started,
                interval_stopped_at=pod_stopped,
                node_cpu_millis=reservation.requested_cpu_millis * 4,
                node_memory_mib=reservation.requested_memory_mib * 4,
                node_ephemeral_storage_mib=(reservation.requested_ephemeral_storage_mib * 4),
                provider_billed_microusd=4_000_000,
                billing_source="nebius-invoice-export",
                billing_source_version="invoice-2026-08",
                observed_at=pod_stopped + timedelta(seconds=1),
            )
            assert created is True
            assert node_cost.allocated_microusd == 1_000_000
            assert node_cost.idle_system_fragmentation_microusd == 3_000_000
            settled = await settle_execution_cost_reservation(
                session,
                reservation_id=reservation.id,
                billing_complete_through=pod_stopped,
                now=pod_stopped + timedelta(seconds=2),
            )
            await session.commit()
            assert settled.state == "settled"
            assert settled.actual_allocated_microusd == 1_000_000

        async with sessions() as session:
            policies = (
                (
                    await session.execute(
                        select(ExecutionBudgetPolicy).where(
                            ExecutionBudgetPolicy.scope_key.in_(
                                (target.logical_pool_id, target.target_id)
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert {row.daily_reserved_microusd for row in policies} == {0}
            assert {row.daily_settled_microusd for row in policies} == {4_000_000}
            allocation = (
                await session.execute(
                    select(ExecutionNodeCostAllocation).where(
                        ExecutionNodeCostAllocation.cost_reservation_id == reservation.id
                    )
                )
            ).scalar_one()
            assert allocation.dominant_resource_fraction_ppb == 250_000_000
            assert allocation.overlap_seconds == 100
            assert allocation.allocated_microusd == 1_000_000
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "scope_kind",
    ["global", "environment", "region", "team", "batch", "execution_class", "pool"],
)
async def test_service_execution_enforces_each_scope_and_releases_on_terminal_event(
    postgres_url: str,
    scope_kind: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    policy_id = uuid4()
    batch_id = uuid4()
    try:
        async with sessions() as session:
            first_trial_id, target = await _seed_ready_trial(session, now=now)
            first_trial = await session.get(Trial, first_trial_id)
            assert first_trial is not None
            second_trial_id = uuid4()
            session.add(
                Batch(
                    id=batch_id,
                    team_id=first_trial.team_id,
                    name=f"service admission {scope_kind}",
                    task_filter={},
                    trial_config={},
                    state="submitted",
                    created_by_token_prefix="test",
                    expected_trial_count=2,
                )
            )
            first_trial.batch_id = batch_id
            session.add(
                Trial(
                    id=second_trial_id,
                    team_id=first_trial.team_id,
                    task_id=first_trial.task_id,
                    config={"agent": {"name": "test"}},
                    requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                    state="queued",
                    attempt_count=0,
                    batch_id=batch_id,
                )
            )
            scope_keys = {
                "global": "*",
                "environment": target.environment,
                "region": target.region,
                "team": str(first_trial.team_id),
                "batch": str(batch_id),
                "execution_class": NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                "pool": target.logical_pool_id,
            }
            session.add(
                ExecutionAdmissionPolicy(
                    id=policy_id,
                    scope_kind=scope_kind,
                    scope_key=scope_keys[scope_kind],
                    max_concurrent=1,
                    enabled=True,
                    reason="service pool ceiling test",
                )
            )
            first = await _reserve(
                session,
                trial_id=first_trial_id,
                target=target,
                now=now,
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                ServiceExecutionConflict,
                match=f"execution_admission_{scope_kind}_ceiling_reached",
            ):
                await _reserve(
                    session,
                    trial_id=second_trial_id,
                    target=target,
                    now=now + timedelta(seconds=1),
                )
            await session.rollback()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=first.id,
                generation=1,
                ordinal=1,
                event_kind="failed",
                payload={"failure_reason": "test_terminal"},
                observed_at=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            released = (
                await session.execute(
                    select(ExecutionAdmissionReservation).where(
                        ExecutionAdmissionReservation.owner_id == first.id
                    )
                )
            ).scalar_one()
            assert released.state == "released"
            released_cost = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == first.id
                    )
                )
            ).scalar_one()
            assert released_cost.state == "released"
            assert released_cost.release_reason == "lease_terminal_before_pod_start"
            second = await _reserve(
                session,
                trial_id=second_trial_id,
                target=target,
                now=now + timedelta(seconds=3),
            )
            assert second.id != first.id
            await session.commit()
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(ExecutionAdmissionPolicy).where(ExecutionAdmissionPolicy.id == policy_id)
            )
        await engine.dispose()


async def test_kubernetes_reservation_rejects_a_legacy_worker_route(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    requirements = _requirements()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            decision = ExecutionRoutingDecisionV1(
                generation=1,
                requirements_sha256=canonical_digest(requirements.model_dump(mode="json")),
                selected_pool_id="gb10",
                selected_adapter_kind=ExecutionAdapterKind.LEGACY_WORKER_CLAIM,
                reason=ExecutionRoutingReason.PREEXISTING_ASSIGNMENT,
                decided_at=now,
                candidates=(
                    ExecutionRouteCandidateV1(
                        logical_pool_id="gb10",
                        adapter_kind=ExecutionAdapterKind.LEGACY_WORKER_CLAIM,
                        enabled=True,
                        healthy=True,
                        draining=False,
                        configured_slots=1,
                        active_slots=1,
                        occupied_slots=0,
                        pending_slots=0,
                        assigned_queued_slots=1,
                        available_slots=0,
                        capacity_evidence_kind=CapacityEvidenceKind.PREEXISTING_ASSIGNMENT,
                    ),
                ),
            )
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.execution_route_generation = decision.generation
            trial.execution_route_pool_name = decision.selected_pool_id
            trial.execution_route_json = decision.model_dump(mode="json")
            trial.execution_route_sha256 = canonical_digest(trial.execution_route_json)

            with pytest.raises(
                ServiceExecutionConflict,
                match="different execution authority",
            ):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now,
                    requirements=requirements,
                )
    finally:
        await engine.dispose()


async def test_kubernetes_reservation_rejects_target_residency_drift(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            with pytest.raises(ServiceExecutionConflict, match="violates data residency"):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now,
                    requirements=_requirements(data_residency="ca"),
                )
    finally:
        await engine.dispose()


async def test_separate_verifier_is_a_parent_bound_execution_lease(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            parent = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
                runtime_contract=_runtime_contract(verifier_execution="separate_execution"),
            )
            await session.commit()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=parent.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "succeeded"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            verifier = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now + timedelta(seconds=2),
                requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
                runtime_contract=_runtime_contract(
                    execution_role="verifier",
                    verifier_execution="skipped",
                ),
                parent_lease_id=parent.id,
            )
            await session.commit()

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            rows = (
                (
                    await session.execute(
                        select(ServiceExecutionLease)
                        .where(ServiceExecutionLease.trial_id == trial_id)
                        .order_by(ServiceExecutionLease.execution_role)
                    )
                )
                .scalars()
                .all()
            )
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("claimed", 1)
            assert [item.execution_role for item in rows] == ["attempt", "verifier"]
            assert verifier.parent_lease_id == parent.id
            assert verifier.attempt == parent.attempt
            assert verifier.job_name.endswith("-v")
            assert rows[0].job_name.endswith("-a")

        async with sessions() as session:
            mismatched = _runtime_contract(
                execution_role="verifier",
                verifier_execution="skipped",
            )
            mismatched = ExecutionRuntimePlanV1.model_validate(
                {**mismatched.canonical_payload(), "candidate_sha": "9" * 40}
            )
            with pytest.raises(ServiceExecutionConflict, match="parent lease is not eligible"):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now + timedelta(seconds=3),
                    requirements=_requirements(
                        verifier_topology=VerifierTopology.SEPARATE_EXECUTION
                    ),
                    runtime_contract=mismatched,
                    parent_lease_id=parent.id,
                )
    finally:
        await engine.dispose()


async def test_runtime_result_identity_is_fenced_and_durably_reported(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="finalize",
                now=now,
            )
            await session.commit()

        payload = _runtime_result_payload(lease, started_at=now)
        async with sessions() as session:
            event, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="result_reported",
                payload=payload,
                observed_at=now + timedelta(seconds=3),
            )
            await session.commit()
            assert duplicate is False
            assert event.payload_sha256 == canonical_digest(payload)

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="identity"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=2,
                    event_kind="result_reported",
                    payload={**payload, "candidate_sha": "9" * 40},
                    observed_at=now + timedelta(seconds=4),
                )
    finally:
        await engine.dispose()


async def test_deferred_outbox_constraint_rolls_back_trial_reservation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            await session.commit()

        async with sessions() as session:
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.execute(
                delete(ServiceExecutionCommand).where(ServiceExecutionCommand.lease_id == lease.id)
            )
            with pytest.raises(DBAPIError, match=r"lacks durable .* command"):
                await session.commit()
            await session.rollback()

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            lease_count = await session.scalar(
                select(func.count(ServiceExecutionLease.id)).where(
                    ServiceExecutionLease.trial_id == trial_id
                )
            )
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("queued", 0)
            assert lease_count == 0
    finally:
        await engine.dispose()


async def test_command_redelivery_and_acknowledgement_are_replay_safe(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            first = await claim_execution_commands(
                session, consumer_id="actuator-a", limit=1, lease_seconds=5, now=now
            )
            await session.commit()
            assert len(first) == 1
            assert first[0].delivery_count == 1

        async with sessions() as session:
            second = await claim_execution_commands(
                session,
                consumer_id="actuator-b",
                limit=1,
                lease_seconds=5,
                now=now + timedelta(seconds=6),
            )
            assert len(second) == 1
            assert second[0].id == first[0].id
            assert second[0].delivery_count == 2
            command = await acknowledge_execution_command(
                session,
                command_id=second[0].id,
                consumer_id="actuator-b",
                acknowledgement={"provider_action": "created", "lease_id": str(lease.id)},
                now=now + timedelta(seconds=7),
            )
            await session.commit()
            assert command.state == "acknowledged"

        async with sessions() as session:
            replay = await acknowledge_execution_command(
                session,
                command_id=second[0].id,
                consumer_id="actuator-b",
                acknowledgement={"provider_action": "created", "lease_id": str(lease.id)},
            )
            assert replay.id == second[0].id
            with pytest.raises(ServiceExecutionConflict, match="replay changed"):
                await acknowledge_execution_command(
                    session,
                    command_id=second[0].id,
                    consumer_id="actuator-b",
                    acknowledgement={"provider_action": "different"},
                )
    finally:
        await engine.dispose()


async def test_events_are_replay_safe_and_lower_ordinals_do_not_regress_projection(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now + timedelta(seconds=1),
            )
            started, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="started",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=2),
            )
            assert not duplicate
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="created",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            event_count = await session.scalar(
                select(func.count(ServiceExecutionEvent.id)).where(
                    ServiceExecutionEvent.lease_id == lease.id
                )
            )
            assert persisted is not None
            assert (persisted.observed_state, persisted.last_event_ordinal) == ("running", 2)
            assert event_count == 2
            replay, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="started",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=2),
            )
            assert duplicate
            assert replay.id == started.id
            with pytest.raises(ServiceExecutionConflict, match="replay changed"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=2,
                    event_kind="started",
                    payload={"runtime": "changed"},
                    observed_at=now + timedelta(seconds=2),
                )
    finally:
        await engine.dispose()


async def test_revocation_fences_old_generation_and_database_rejects_generation_skip(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            cancel = await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()
            assert cancel.generation == 2

        async with sessions() as session:
            for surface in (
                "gateway",
                "artifact",
                "trajectory",
                "usage",
                "heartbeat",
                "result",
            ):
                with pytest.raises(ServiceExecutionFenceError):
                    await verify_trial_execution_fence(
                        session,
                        trial_id=trial_id,
                        lease_id=lease.id,
                        generation=1,
                        surface=surface,
                    )
            with pytest.raises(ServiceExecutionFenceError):
                await verify_trial_execution_fence(
                    session,
                    trial_id=trial_id,
                    lease_id=lease.id,
                    generation=2,
                    surface="heartbeat",
                )
            terminal = await verify_trial_execution_fence(
                session,
                trial_id=trial_id,
                lease_id=lease.id,
                generation=2,
                surface="cancelled",
                allow_terminal_event=True,
            )
            assert terminal is not None

            with pytest.raises(DBAPIError, match="advance monotonically by one"):
                await session.execute(
                    update(ServiceExecutionLease)
                    .where(ServiceExecutionLease.id == lease.id)
                    .values(generation=4)
                )
            await session.rollback()

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.generation == 2
            assert persisted.revoked_at is not None
            assert persisted.cleanup_requested_at == now + timedelta(seconds=1)
            assert persisted.cleanup_deadline_at == now + timedelta(minutes=5, seconds=1)
    finally:
        await engine.dispose()


async def test_trial_cancellation_persists_service_execution_revocation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            command = await request_trial_execution_cancellation(
                session,
                trial_id=trial_id,
                now=now + timedelta(seconds=1),
            )
            await session.commit()
            assert command is not None
            assert command.command_type == "cancel"
            assert command.generation == 2

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.desired_state == "cancel"
            assert persisted.generation == 2
            assert persisted.revoked_at == now + timedelta(seconds=1)
            assert persisted.cleanup_state == "pending"
            assert (
                await request_trial_execution_cancellation(
                    session,
                    trial_id=trial_id,
                    now=now + timedelta(seconds=2),
                )
                is None
            )
            await session.commit()
    finally:
        await engine.dispose()


async def test_retry_creates_a_new_attempt_and_finalization_is_idempotent(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            first = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=first.id,
                expected_generation=1,
                desired_state="retry",
                now=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="cleanup is not complete"):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now + timedelta(seconds=2),
                )
            await session.rollback()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=first.id,
                generation=2,
                ordinal=1,
                event_kind="deleted",
                payload={"resource_release": "complete"},
                observed_at=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            second = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now + timedelta(seconds=3),
            )
            await session.commit()
            assert (second.attempt, second.generation) == (2, 1)
            assert second.id != first.id

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=second.id,
                expected_generation=1,
                desired_state="start",
                now=now + timedelta(seconds=3),
            )
            await enqueue_execution_transition(
                session,
                lease_id=second.id,
                expected_generation=1,
                desired_state="finalize",
                now=now + timedelta(seconds=4),
            )
            await session.commit()

        final_payload = {"trial_state": "succeeded", "result": {"reward": 1.0}}
        async with sessions() as session:
            event, duplicate = await record_execution_event(
                session,
                lease_id=second.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload=final_payload,
                observed_at=now + timedelta(seconds=5),
            )
            assert not duplicate
            await session.commit()

        async with sessions() as session:
            replay, duplicate = await record_execution_event(
                session,
                lease_id=second.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload=final_payload,
                observed_at=now + timedelta(seconds=5),
            )
            assert duplicate
            assert replay.id == event.id
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            assert trial.state == "succeeded"
            assert trial.result == {"reward": 1.0}
            assert trial.finished_at == now + timedelta(seconds=5)
    finally:
        await engine.dispose()


async def test_gateway_dispatch_is_rejected_immediately_after_generation_revocation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            ctx = AuthContext(
                token_hash=b"",
                type="step_session",
                scopes=["llm:call"],
                team_id=lease.team_id,
                expires_at=now + timedelta(minutes=10),
                trial_id=trial_id,
                step_id="agent",
                provider_connection_id=None,
                provider_connection_id_bound=True,
                step_jwt_id=uuid4(),
                service_execution_lease_id=lease.id,
                service_execution_generation=1,
                service_execution_role="attempt",
                service_execution_runtime_contract_sha256=lease.runtime_contract_sha256,
                service_execution_candidate_sha="1" * 40,
                service_execution_task_revision_sha256="sha256:" + "2" * 64,
                service_execution_command_identity_sha256="sha256:" + "3" * 64,
            )
            # The broker can mint the Pod's step token as soon as the Actuator
            # has observed its identity, while the lease is still `creating`.
            # Dispatch must accept that same live authority during cold start.
            lease.observed_state = "creating"
            await session.flush()
            await authorize_trial_execution_dispatch(session, ctx)
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="created",
                payload={"provider_action": "created"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            await authorize_trial_execution_dispatch(session, ctx)
            for changed in (
                replace(ctx, provider_connection_id_bound=False),
                replace(ctx, service_execution_candidate_sha="9" * 40),
                replace(
                    ctx,
                    service_execution_runtime_contract_sha256="sha256:" + "9" * 64,
                ),
                replace(ctx, service_execution_command_identity_sha256="sha256:" + "9" * 64),
            ):
                with pytest.raises(HTTPException, match="service execution dispatch forbidden"):
                    await authorize_trial_execution_dispatch(session, changed)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                HTTPException,
                match="service execution dispatch forbidden",
            ) as exc_info:
                await authorize_trial_execution_dispatch(session, ctx)
            assert exc_info.value.status_code == 403
    finally:
        await engine.dispose()


async def test_service_step_token_freezes_identity_and_persists_audit(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane.routes import step_tokens

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    signing_key = "s" * 64
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        auth = AuthContext(
            token_hash=b"w" * 32,
            type="worker",
            scopes=["worker:report"],
            team_id=None,
            expires_at=None,
        )

        async def verified_auth(*args, **kwargs):  # type: ignore[no-untyped-def]
            del args, kwargs
            return auth

        monkeypatch.setattr(step_tokens, "verify_bearer_token", verified_auth)
        app = FastAPI()
        app.state.session_factory = sessions
        app.state.settings = SimpleNamespace(step_jwt_signing_key=SecretStr(signing_key))
        request = Request({"type": "http", "app": app, "headers": []})
        response = await step_tokens.issue_step_token(
            request,
            step_tokens._IssueStepTokenRequest(
                team_id=lease.team_id,
                trial_id=trial_id,
                step_id="agent",
                ttl_sec=600,
            ),
            authorization="Bearer ignored",
            execution_lease_id=lease.id,
            execution_generation=lease.generation,
        )

        ctx = verify_step_jwt(response["token"], signing_key=signing_key)
        assert ctx.step_jwt_id is not None
        assert ctx.provider_connection_id_bound is True
        assert ctx.service_execution_lease_id == lease.id
        assert ctx.service_execution_generation == lease.generation
        assert ctx.service_execution_role == "attempt"
        assert ctx.service_execution_runtime_contract_sha256 == lease.runtime_contract_sha256
        async with sessions() as session:
            audit = (
                await session.execute(
                    select(AdminAuditEvent).where(
                        AdminAuditEvent.action == "service_execution.step_token.minted",
                        AdminAuditEvent.target_id == str(lease.id),
                    )
                )
            ).scalar_one()
            assert audit.event_metadata["step_jwt_id"] == str(ctx.step_jwt_id)
            assert audit.event_metadata["generation"] == lease.generation
    finally:
        await engine.dispose()


async def test_observed_pod_broker_commits_semantic_runtime_output(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    signing_key = "b" * 64
    pod_ip = "10.24.7.19"
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-uid-1",
                    "pod_uid": "pod-uid-1",
                    "pod_ip": pod_ip,
                    "resource_version": "7",
                },
                observed_at=now,
            )
            await session.commit()

        identity = ServiceExecutionPeerV1(
            lease_id=lease.id,
            generation=1,
            execution_role="attempt",
        )
        async with sessions() as session:
            authorized = await authorize_service_execution_peer(
                session,
                peer_ip=pod_ip,
                identity=identity,
                purpose="token",
            )
            token, _expires_at, _token_id = await mint_service_execution_peer_token(
                session,
                lease=authorized,
                ttl_seconds=480,
                signing_key=signing_key,
                now=now,
            )
            await session.commit()
            session.expunge(authorized)
        token_context = verify_step_jwt(token, signing_key=signing_key)
        assert token_context.service_execution_lease_id == lease.id
        assert token_context.service_execution_generation == 1

        result_document = _runtime_result_payload(authorized, started_at=now)
        result_document.update(status="runtime_error", partial_evidence=True, phases=[])
        result_payload = canonical_document(result_document)
        store = FakeObjectStore()
        repository = SqlArtifactCommitRepository(
            session_factory=sessions,
            store=store,
            bucket="artifacts",
        )
        route = ServiceExecutionOutputRouteService(
            service=ArtifactCommitService(
                store=store,
                bucket="artifacts",
                repository=repository,
            ),
            session_factory=sessions,
        )
        prepare = ServiceExecutionOutputPrepareV1(
            schema_version="loom.service-execution-output-prepare.v1",
            request_id=uuid4(),
            **identity.model_dump(),
            files=(
                ServiceExecutionOutputFileV1(
                    relative_path="result.json",
                    media_type="application/json",
                    size_bytes=len(result_payload),
                    sha256=digest_bytes(result_payload),
                ),
            ),
        )
        grant = await route.prepare(lease=authorized, request=prepare)
        upload_session_id = UUID(grant["upload_session_id"])
        upload_token = str(grant["upload_token"])

        async def body():  # type: ignore[no-untyped-def]
            yield result_payload

        receipt = PartReceiptV1.model_validate(
            await route.put_part(
                lease=authorized,
                session_id=upload_session_id,
                file_index=0,
                part_number=1,
                content_length=len(result_payload),
                content_sha256=digest_bytes(result_payload),
                upload_token=upload_token,
                body=body(),
            )
        )
        await route.complete_file(
            lease=authorized,
            session_id=upload_session_id,
            file_index=0,
            ordered_parts=(receipt,),
            upload_token=upload_token,
        )
        committed = await route.commit(
            lease=authorized,
            session_id=upload_session_id,
            upload_token=upload_token,
        )
        assert committed["state"] == "committed"
        assert committed["manifest_sha256"].startswith("sha256:")
        assert (
            await route.commit(
                lease=authorized,
                session_id=upload_session_id,
                upload_token=upload_token,
            )
            == committed
        )

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            artifact = (
                await session.execute(
                    select(Artifact).where(
                        Artifact.control_producer_kind == "service_execution",
                        Artifact.control_producer_id == lease.id,
                    )
                )
            ).scalar_one()
            assert persisted is not None
            assert persisted.output_commit_state == "committed"
            assert persisted.output_upload_session_id == upload_session_id
            assert persisted.desired_state == "finalize"
            assert artifact.content_hash == digest_bytes(result_payload)
            assert (
                await session.scalar(
                    select(func.count(ServiceExecutionEvent.id)).where(
                        ServiceExecutionEvent.lease_id == lease.id,
                        ServiceExecutionEvent.event_kind == "result_reported",
                    )
                )
                == 1
            )

        terminal_observation = KubernetesJobObservation(
            namespace=target.namespace_name,
            job_name=lease.job_name,
            lease_id=str(lease.id),
            resource_generation=lease.resource_generation,
            target_id=target.target_id,
            execution_unit_key=str(lease.execution_unit_key),
            normalized_state=NormalizedJobState.SUCCEEDED,
            job_uid="job-uid-1",
            pod_uid="pod-uid-1",
            pod_ip=pod_ip,
            resource_version="8",
            terminated_at=now + timedelta(seconds=3),
            termination_summary=ExecutionTerminationSummaryV1(
                schema_version="loom.execution-termination-summary.v1",
                runtime_contract_sha256=authorized.runtime_contract_sha256,
                command_identity_sha256=result_document["command_identity_sha256"],
                execution_role="attempt",
                status="runtime_error",
                partial_evidence=True,
                phase_count=0,
                finished_at=now + timedelta(seconds=3),
                result_path="result.json",
                output_committed=True,
                output_upload_session_id=upload_session_id,
                output_manifest_sha256=committed["manifest_sha256"],
                output_marker_sha256=committed["committed_marker_sha256"],
            ),
        )
        kubernetes = _FakeKubernetesJobApi()
        kubernetes.jobs[lease.job_name] = terminal_observation
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
            ),
            controller_id="output-finalization-test",
        )
        assert await actuator.run_commands_once(now=now + timedelta(seconds=4)) == 3

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert persisted is not None
            assert trial is not None
            assert persisted.desired_state == "delete_pending"
            assert persisted.observed_state == "finalized"
            assert trial.state == "failed"
            assert trial.failure_reason == "runtime_error"
            assert (
                await session.scalar(
                    select(func.count(ServiceExecutionCommand.id)).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "delete",
                    )
                )
                == 1
            )
        assert await actuator.run_commands_once(now=now + timedelta(seconds=5)) == 1
        assert kubernetes.delete_count == 1
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=6)) == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.desired_state == "deleted"
            assert persisted.cleanup_state == "complete"

        async with sessions() as session:
            with pytest.raises(ServiceExecutionBrokerError, match="not_observed"):
                await authorize_service_execution_peer(
                    session,
                    peer_ip="10.24.7.20",
                    identity=identity,
                    purpose="output",
                )
    finally:
        await engine.dispose()


async def test_revocation_fences_tokens_but_bounds_output_flush(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    pod_ip = "10.24.8.31"
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-uid-flush",
                    "pod_uid": "pod-uid-flush",
                    "pod_ip": pod_ip,
                },
                observed_at=now,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()

        identity = ServiceExecutionPeerV1(
            lease_id=lease.id,
            generation=1,
            execution_role="attempt",
        )
        async with sessions() as session:
            with pytest.raises(ServiceExecutionBrokerError, match="generation_fenced"):
                await authorize_service_execution_peer(
                    session,
                    peer_ip=pod_ip,
                    identity=identity,
                    purpose="token",
                    now=now + timedelta(seconds=2),
                )
            output_lease = await authorize_service_execution_peer(
                session,
                peer_ip=pod_ip,
                identity=identity,
                purpose="output",
                now=now + timedelta(minutes=4),
            )
            assert output_lease.resource_generation == 1
            assert output_lease.generation == 2
            with pytest.raises(ServiceExecutionBrokerError, match="window_closed"):
                await authorize_service_execution_peer(
                    session,
                    peer_ip=pod_ip,
                    identity=identity,
                    purpose="output",
                    now=now + timedelta(minutes=5, seconds=1),
                )
    finally:
        await engine.dispose()


async def test_invalid_state_edges_event_bounds_and_payload_bounds_fail_closed(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="create -> finalize"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state="finalize",
                )
            with pytest.raises(ServiceExecutionConflict, match="invalid for desired state"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=1,
                    event_kind="started",
                    payload={},
                    observed_at=now,
                )
            with pytest.raises(ServiceExecutionConflict, match="between 1 and 10000"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=10_001,
                    event_kind="created",
                    payload={},
                    observed_at=now,
                )

        async with sessions() as session:
            with pytest.raises(DBAPIError, match="payload_bound"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state="start",
                    payload={"oversized": "x" * 70_000},
                )
            await session.rollback()
    finally:
        await engine.dispose()


async def test_operator_projection_is_team_isolated(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane.routes import service_executions

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        app = FastAPI()
        app.state.session_factory = sessions
        request = Request({"type": "http", "app": app, "headers": []})

        async def wrong_team_auth(request: Request, authorization: str | None) -> AuthContext:
            del request, authorization
            return AuthContext(
                token_hash=b"x",
                type="team",
                scopes=["read:own"],
                team_id=uuid4(),
                expires_at=None,
            )

        monkeypatch.setattr(service_executions, "_auth", wrong_team_auth)
        with pytest.raises(HTTPException) as hidden:
            await service_executions.get_trial_execution(trial_id, request, None)
        assert (hidden.value.status_code, hidden.value.detail) == (404, "trial not found")

        async def owning_team_auth(request: Request, authorization: str | None) -> AuthContext:
            del request, authorization
            return AuthContext(
                token_hash=b"x",
                type="team",
                scopes=["read:own"],
                team_id=lease.team_id,
                expires_at=None,
            )

        monkeypatch.setattr(service_executions, "_auth", owning_team_auth)
        projection = await service_executions.get_trial_execution(trial_id, request, None)
        assert projection["trial_id"] == str(trial_id)
        assert projection["execution"]["lease_id"] == str(lease.id)
        assert len(projection["commands"]) == 1
        assert projection["events"] == []
        assert projection["history"]
    finally:
        await engine.dispose()


@pytest.mark.parametrize("ambiguous_create", [False, True], ids=["normal", "lost-response"])
async def test_actuator_create_cancel_restart_and_missing_reconcile_converge(
    postgres_url: str,
    ambiguous_create: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi(ambiguous_create=ambiguous_create)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        runtime = ExecutionTargetRuntime(
            target_id=target.target_id,
            namespace=target.namespace_name,
            runtime_class_name="loom-sandbox",
        )
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=runtime,
            controller_id="actuator-a",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        assert kubernetes.create_count == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            create_command = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "create",
                    )
                )
            ).scalar_one()
            capacity_authorization = (
                await session.execute(
                    select(ExecutionProvisioningAuthorization).where(
                        ExecutionProvisioningAuthorization.lease_id == lease.id
                    )
                )
            ).scalar_one()
            assert persisted is not None
            assert persisted.job_uid == f"job-uid-{lease.job_name}"
            assert persisted.observed_state == "creating"
            assert persisted.last_reconciled_at == now
            assert create_command.state == "acknowledged"
            assert capacity_authorization.state == "pending"
            assert capacity_authorization.decision_reason == "existing_allocatable"
            capacity_policy = await session.get(ExecutionCapacityPolicy, target.target_id)
            assert capacity_policy is not None
            capacity_policy.enabled = False
            await session.commit()

        async with sessions() as session:
            replayed_authorization = await reserve_execution_provisioning(
                session,
                lease_id=lease.id,
                now=now + timedelta(milliseconds=1),
            )
            assert replayed_authorization is not None
            assert replayed_authorization.id == capacity_authorization.id
            await session.rollback()

        restarted = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=runtime,
            controller_id="actuator-b",
            command_lease_seconds=5,
        )
        assert await restarted.run_commands_once(now=now + timedelta(seconds=1)) == 0
        assert kubernetes.create_count == 1

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=2),
            )
            await session.commit()

        assert await restarted.run_commands_once(now=now + timedelta(seconds=2)) == 1
        assert kubernetes.delete_count == 1
        assert kubernetes.jobs == {}
        assert await restarted.reconcile_full_once(now=now + timedelta(seconds=3)) == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.desired_state == "deleted"
            assert persisted.observed_state == "deleted"
            assert persisted.cleanup_state == "complete"
            assert persisted.output_commit_state == "unavailable"
            assert persisted.output_unavailable_reason == "operator_cancelled"
            assert persisted.deleted_at == now + timedelta(seconds=3)
            capacity_authorization = (
                await session.execute(
                    select(ExecutionProvisioningAuthorization).where(
                        ExecutionProvisioningAuthorization.lease_id == lease.id
                    )
                )
            ).scalar_one()
            assert capacity_authorization.state == "released"
            assert capacity_authorization.released_at is not None
    finally:
        await engine.dispose()


async def test_actuator_refuses_uid_reuse_and_never_deletes_foreign_scope(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        runtime = ExecutionTargetRuntime(
            target_id=target.target_id,
            namespace=target.namespace_name,
            runtime_class_name="loom-sandbox",
        )
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=runtime,
            controller_id="actuator-a",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        original = kubernetes.jobs[lease.job_name]
        kubernetes.watch_events.append(
            original.model_copy(update={"lease_id": str(uuid4()), "resource_version": "2"})
        )
        assert await actuator.watch_once(timeout_seconds=1) == 0
        assert actuator._watch_resource_version == "2"
        kubernetes.jobs[lease.job_name] = original.model_copy(update={"job_uid": "reused-uid"})

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()
        assert await actuator.run_commands_once(now=now + timedelta(minutes=5, seconds=1)) == 1
        assert kubernetes.delete_count == 0
        async with sessions() as session:
            cancel = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "cancel",
                    )
                )
            ).scalar_one()
            assert cancel.state == "dead_letter"
            assert cancel.last_error_code == "contract_error"

        foreign = KubernetesJobObservation(
            namespace=target.namespace_name,
            job_name="foreign-managed-job",
            lease_id=str(uuid4()),
            resource_generation=1,
            target_id=target.target_id,
            execution_unit_key=str(uuid4()),
            normalized_state=NormalizedJobState.RUNNING,
            job_uid="foreign-uid",
            resource_version="2",
        )
        kubernetes.jobs[foreign.job_name] = foreign
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=2)) >= 1
        assert kubernetes.jobs[foreign.job_name] == foreign
        assert kubernetes.delete_count == 0
    finally:
        await engine.dispose()


async def test_actuator_records_unavailable_before_accepting_an_already_absent_job(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
                runtime_class_name="loom-sandbox",
            ),
            controller_id="actuator-a",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        kubernetes.jobs.clear()
        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()

        assert await actuator.run_commands_once(now=now + timedelta(seconds=1)) == 1
        async with sessions() as session:
            closed = await session.get(ServiceExecutionLease, lease.id)
            assert closed is not None
            assert closed.observed_state == "deleted"
            assert closed.output_commit_state == "unavailable"
            assert closed.output_unavailable_reason == "operator_cancelled"
    finally:
        await engine.dispose()
