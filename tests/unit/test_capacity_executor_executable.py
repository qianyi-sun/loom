from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from loom_capacity_agent.admission import (
    BoundExecutableWorkerV2,
    DrainedExecutableWorkerV2,
    PhysicalJobBindingV2,
    PreparedExecutableAdmissionV2,
)
from loom_capacity_executor.client import ExecutorRejectedError, ExecutorTransportError
from loom_capacity_executor.executable import (
    ExecutablePoolExecutor,
    ProtectedIntentObservationV2,
)
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_executor.launch_renderer import (
    OperatorGenericTresMappingV2,
    OperatorLaunchProfileV2,
    TrustedLaunchContextV2,
    canonical_launch_policy_digest,
)
from loom_capacity_executor.slurm_contracts import (
    SlurmAccountingHighWaterV2,
    SlurmCancelRequestV2,
    SlurmJobObservationV2,
    SlurmSubmissionV2,
    SlurmTerminalEvidenceV2,
    SlurmTresValueV2,
)
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableReleasedShapeV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutionContextV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import ReservationShapeV1
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture

_NOW = datetime(2026, 8, 13, 16, 0, tzinfo=UTC)


class SimulatedCrash(RuntimeError):  # noqa: N818 - matches the task's crash harness term
    pass


@dataclass
class FakeManager:
    registration: ExecutableExecutorRegistrationV2
    work: object | None
    command_sequence: int = 0
    journal_sequence: int = 0
    journal_digest: str = "0" * 64
    inventory_sequence: int = 0
    failure: Exception | None = None

    def __post_init__(self) -> None:
        self.inventories: list[ExecutableExecutorInventoryV2] = []
        self.releases: list[ExecutablePartialReleaseV2] = []
        self.central_requests: list[StrictV2Model] = []
        self.inventory_failure: Exception | None = None
        self.reject_work_fetch = False

    async def executable_checkpoint(self) -> SimpleNamespace:
        return SimpleNamespace(
            execution_epoch=self.registration.execution.execution_epoch,
            execution_manifest_sha256=self.registration.execution.execution_manifest_sha256,
            executor_id=self.registration.executor_id,
            executor_incarnation=self.registration.executor_incarnation,
            pool_id=self.registration.pool_id,
            pool_generation=self.registration.pool_generation,
            command_sequence=self.command_sequence,
            journal_sequence=self.journal_sequence,
            journal_digest=self.journal_digest,
            inventory_sequence=self.inventory_sequence,
            lease_expires_at=_NOW + timedelta(minutes=2),
            executable=True,
        )

    async def next_executable_work(self, command_sequence: int) -> object | None:
        assert command_sequence == self.command_sequence
        if self.reject_work_fetch:
            raise AssertionError("durable central request must replay before work fetch")
        return self.work

    def _transition(self, command_sequence: int, payload: dict[str, object]) -> SimpleNamespace:
        if self.failure is not None:
            raise self.failure
        assert command_sequence == self.command_sequence + 1
        self.command_sequence = command_sequence
        return SimpleNamespace(
            **payload,
            receipt_digest=_receipt_digest(payload),
            replayed=False,
        )

    async def accept_executable_reservation(
        self, value: ExecutableReservationAcceptanceV2
    ) -> SimpleNamespace:
        self.central_requests.append(value)
        return self._transition(
            value.command_sequence,
            {"tranche_id": value.tranche_id, "intent_ids": (UUID(int=101),), "executable": True},
        )

    async def register_executable_bootstrap(
        self, value: ExecutableBootstrapRegistrationV2
    ) -> SimpleNamespace:
        self.central_requests.append(value)
        result = self._transition(
            value.command_sequence,
            {
                "intent_id": value.binding.intent_id,
                "bootstrap_registration_epoch": value.bootstrap_registration_epoch,
                "executable": True,
            },
        )
        self.work = None
        return result

    async def consume_executable_permit(
        self, value: ExecutablePermitConsumptionV2
    ) -> SimpleNamespace:
        self.central_requests.append(value)
        result = self._transition(
            value.command_sequence,
            {
                "permit_id": value.permit_id,
                "intent_id": value.binding.intent_id,
                "executable": True,
            },
        )
        self.work = None
        return result

    async def close_executable_intent(self, value: ExecutableIntentCloseV2) -> SimpleNamespace:
        self.central_requests.append(value)
        result = self._transition(
            value.command_sequence,
            {"intent_id": value.binding.intent_id, "executable": True},
        )
        self.work = None
        return result

    async def release_executable_shapes(self, value: ExecutablePartialReleaseV2) -> SimpleNamespace:
        self.central_requests.append(value)
        released = tuple(item.binding.shape_instance_id for item in value.releases)
        result = self._transition(
            value.command_sequence,
            {"tranche_id": value.tranche_id, "released_shape_ids": released, "executable": True},
        )
        self.releases.append(value)
        self.work = None
        return result

    async def ingest_executable_inventory(
        self, value: ExecutableExecutorInventoryV2
    ) -> SimpleNamespace:
        self.inventories.append(value)
        if self.inventory_failure is not None:
            raise self.inventory_failure
        self.inventory_sequence = value.inventory_sequence
        self.journal_sequence = value.journal_sequence
        self.journal_digest = value.journal_digest
        return SimpleNamespace(
            inventory_sequence=value.inventory_sequence,
            inventory_digest=canonical_executable_digest(value),
            replayed=False,
            executable=True,
        )


class FakeAdmission:
    def __init__(self) -> None:
        self.prepared: dict[UUID, PreparedExecutableAdmissionV2] = {}
        self.prepare_requests: list[ExecutableBootstrapRegistrationV2] = []
        self.crash_after_prepare = False
        self.bound: dict[UUID, BoundExecutableWorkerV2] = {}
        self.bind_requests: list[PhysicalJobBindingV2] = []
        self.bind_failure: Exception | None = None
        self.bind_commits_before_failure = False
        self.withdraw_requests: list[Any] = []
        self.drain_requests: list[Any] = []
        self.crash_after_drain = False
        self.observations: dict[UUID, ProtectedIntentObservationV2] = {}

    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> PreparedExecutableAdmissionV2:
        self.prepare_requests.append(request)
        digest = canonical_executable_digest(request)
        receipt = PreparedExecutableAdmissionV2(
            subject_id=request.binding.subject_id,
            subject_incarnation=request.binding.subject_incarnation,
            intent_id=request.binding.intent_id,
            bootstrap_registration_epoch=request.bootstrap_registration_epoch,
            bootstrap_sha256=bootstrap_sha256,
            request_digest=digest,
            admission_digest=digest,
            protected_high_water=1,
        )
        self.prepared[request.binding.intent_id] = receipt
        if self.crash_after_prepare:
            raise SimulatedCrash("process stopped after protected preparation committed")
        return receipt

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> BoundExecutableWorkerV2:
        self.bind_requests.append(request)
        if self.bind_failure is not None and not self.bind_commits_before_failure:
            raise self.bind_failure
        digest = canonical_executable_digest(request)
        receipt = BoundExecutableWorkerV2(
            subject_id=request.binding.subject_id,
            subject_incarnation=request.binding.subject_incarnation,
            intent_id=request.binding.intent_id,
            bootstrap_registration_epoch=request.bootstrap_registration_epoch,
            slurm_job_id=request.slurm_job_id,
            ownership_evidence_sha256=request.ownership_evidence_sha256,
            request_digest=digest,
            binding_digest=digest,
            protected_high_water=2,
        )
        self.bound[request.binding.intent_id] = receipt
        if self.bind_failure is not None:
            raise self.bind_failure
        return receipt

    async def observe_intent(
        self, binding: ExecutableIntentBindingV2
    ) -> ProtectedIntentObservationV2:
        return self.observations.get(
            binding.intent_id,
            ProtectedIntentObservationV2(binding=binding),
        )

    async def begin_drain(self, request: Any) -> DrainedExecutableWorkerV2:
        self.drain_requests.append(request)
        current = self.observations[request.binding.intent_id]
        receipt = DrainedExecutableWorkerV2(
            subject_id=request.binding.subject_id,
            subject_incarnation=request.binding.subject_incarnation,
            intent_id=request.binding.intent_id,
            worker_id=request.worker_id,
            worker_incarnation=request.worker_incarnation,
            claim_high_water=request.expected_claim_high_water,
            live_claim_count=0,
            drain_epoch=request.drain_epoch,
            request_digest=canonical_executable_digest(request),
            drain_digest=canonical_executable_digest(request),
            protected_high_water=request.drain_epoch,
        )
        self.observations[request.binding.intent_id] = current.model_copy(update={"drain": receipt})
        if self.crash_after_drain:
            raise SimulatedCrash("process stopped after protected drain committed")
        return receipt

    async def withdraw_unregistered_worker(self, request: Any) -> SimpleNamespace:
        self.withdraw_requests.append(request)
        self.observations.setdefault(
            request.binding.intent_id,
            ProtectedIntentObservationV2(
                binding=request.binding,
                bootstrap_registration_epoch=request.bootstrap_registration_epoch,
                claim_high_water=request.expected_claim_high_water,
            ),
        )
        return SimpleNamespace(
            subject_id=request.binding.subject_id,
            subject_incarnation=request.binding.subject_incarnation,
            intent_id=request.binding.intent_id,
            bootstrap_registration_epoch=request.bootstrap_registration_epoch,
            protected_registration_epoch=request.protected_registration_epoch,
            slurm_job_id=request.slurm_job_id,
            ownership_evidence_sha256=request.ownership_evidence_sha256,
            claim_high_water=request.expected_claim_high_water,
            live_claim_count=0,
            bootstrap_revoked=True,
            request_digest=canonical_executable_digest(request),
            withdrawal_digest=canonical_executable_digest(request),
            protected_high_water=request.protected_registration_epoch,
            withdrawal_state="withdrawn",
            executable=True,
        )


class FakeSlurm:
    def __init__(self) -> None:
        self.jobs: list[SlurmJobObservationV2] = []
        self.submit_count = 0
        self.crash_after_submit = False
        self.admission: FakeAdmission | None = None
        self.terminal_jobs: tuple[SlurmTerminalEvidenceV2, ...] = ()
        self.cancel_requests: list[SlurmCancelRequestV2] = []
        self.cancel_failure: Exception | None = None
        self.cancel_commits_before_failure = False

    async def submit(self, request: Any) -> SlurmSubmissionV2:
        self.submit_count += 1
        self.jobs.append(
            SlurmJobObservationV2(
                cluster=request.cluster,
                job_id=str(100 + self.submit_count),
                state="PENDING",
                submitter=request.submitter,
                account=request.account,
                partition=request.partition,
                cpus=request.cpus,
                memory_bytes=request.memory_bytes,
                gpus=request.gpus,
                generic_tres=request.generic_tres,
                nodes=request.nodes,
                pending_reason="Resources",
                ownership_token=request.ownership_token,
            )
        )
        if self.crash_after_submit:
            raise SimulatedCrash("process stopped after sbatch accepted the job")
        return SlurmSubmissionV2(cluster=request.cluster, job_id=str(100 + self.submit_count))

    async def inventory(self) -> tuple[SlurmJobObservationV2, ...]:
        return tuple(self.jobs)

    async def accounting_high_water(self, *, since: datetime) -> SlurmAccountingHighWaterV2:
        return SlurmAccountingHighWaterV2(
            cluster="oldlab",
            account="loom-executor",
            submitter="loom-oldlab",
            since=since,
            observed_through=_NOW,
            terminal_jobs=self.terminal_jobs,
        )

    async def cancel_pending(self, request: SlurmCancelRequestV2) -> SlurmJobObservationV2:
        self.cancel_requests.append(request)
        assert self.admission is not None
        intent = next(iter(self.admission.observations))
        assert (
            self.admission.observations[intent].drain is not None
            or len(self.admission.withdraw_requests) >= 1
        )
        job = next(item for item in self.jobs if item.job_id == request.job_id)
        assert job.state == "PENDING"
        if self.cancel_failure is not None and not self.cancel_commits_before_failure:
            raise self.cancel_failure
        self.jobs.remove(job)
        if self.cancel_failure is not None:
            raise self.cancel_failure
        return job


def _receipt_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def executor_fixture(
    tmp_path: Path,
    *,
    work: object | None,
    profiles: tuple[OperatorLaunchProfileV2, ...] | None = None,
) -> tuple[
    ExecutablePoolExecutor,
    ExecutorJournal,
    FakeManager,
    FakeAdmission,
    FakeSlurm,
    TrustedLaunchContextV2,
]:
    launch = launch_context_fixture()
    approved_profiles = profiles or (launch.profile,)
    primary_profile = approved_profiles[0]
    context = ExecutionContextV2.model_validate(
        launch.binding.execution.model_dump(exclude={"allocation_epoch", "executable"})
    )
    registration = ExecutableExecutorRegistrationV2(
        execution=context,
        executor_id=launch.binding.executor_id,
        executor_incarnation=launch.binding.executor_incarnation,
        pool_id=launch.binding.pool_id,
        pool_generation=launch.binding.pool_generation,
        signing_key_id=launch.ownership_key.signing_key_id,
        signing_key_sha256=launch.ownership_key.public_key_sha256,
        local_authority_sha256="a" * 64,
        controller_authority_sha256=launch.controller_authority.controller_authority_sha256,
    )
    manager = FakeManager(registration, work)
    admission = FakeAdmission()
    slurm = FakeSlurm()
    slurm.admission = admission
    journal = ExecutorJournal(tmp_path / "executor.journal")
    journal.__enter__()
    executor = ExecutablePoolExecutor(
        registration,
        journal,
        manager,
        admission,
        slurm,
        profile=primary_profile,
        controller_authority=PoolControllerAuthorityV2(
            pool_id="oldlab",
            controller_authority_sha256=primary_profile.controller_authority_sha256,
        ),
        ownership_key=launch.ownership_key,
        profiles=approved_profiles,
        now=lambda: _NOW,
        bootstrap_digest=lambda _binding: "b" * 64,
    )
    return executor, journal, manager, admission, slurm, launch


def _alternate_generic_profile(base: OperatorLaunchProfileV2) -> OperatorLaunchProfileV2:
    draft = base.model_copy(
        update={
            "profile_id": "oldlab-asic",
            "profile_generation": base.profile_generation + 1,
            "profile_digest": "9" * 64,
            "shape_id": "oldlab-asic-one-slot",
            "resources": ResourceVectorV1(
                slots=1,
                cpu_millicores=base.cpus * 1_000,
                memory_bytes=base.resources.memory_bytes,
                gpu_count=0,
                generic={"asic": 3},
            ),
            "generic_tres": (
                OperatorGenericTresMappingV2(resource_name="asic", tres_name="gres/asic"),
            ),
            "controller_authority_sha256": "0" * 64,
        }
    )
    return OperatorLaunchProfileV2.model_validate(
        draft.model_copy(
            update={"controller_authority_sha256": canonical_launch_policy_digest(draft)}
        ).model_dump(mode="python")
    )


def _binding_for_profile(
    binding: ExecutableIntentBindingV2,
    profile: OperatorLaunchProfileV2,
    *,
    intent_id: UUID,
) -> ExecutableIntentBindingV2:
    return binding.model_copy(
        update={
            "intent_id": intent_id,
            "shape_instance_id": f"shape-{profile.shape_id}-{intent_id.int}",
            "profile_id": profile.profile_id,
            "profile_generation": profile.profile_generation,
            "profile_digest": profile.profile_digest,
            "shape_id": profile.shape_id,
            "concurrency_slots": profile.concurrency_slots,
            "resources": profile.resources,
        }
    )


def permit_fixture(binding: ExecutableIntentBindingV2) -> ExecutableLaunchPermitV2:
    return ExecutableLaunchPermitV2(
        permit_id=UUID(int=200),
        binding=binding,
        permit_epoch=1,
        launch_rank=1,
        expires_at=_NOW + timedelta(minutes=1),
    )


def proposal_fixture(binding: ExecutableIntentBindingV2) -> ExecutableReservationProposalV2:
    return ExecutableReservationProposalV2(
        tranche_id=binding.tranche_id,
        execution=binding.execution,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        account_id=binding.account_id,
        tier_id=binding.tier_id,
        candidate=binding.candidate,
        candidate_generation=binding.candidate_generation,
        deployment_generation=binding.deployment_generation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        shapes=(
            ReservationShapeV1(
                shape_instance_id=binding.shape_instance_id,
                intent_id=binding.intent_id,
                shape_id=binding.shape_id,
                profile_id=binding.profile_id,
                profile_generation=binding.profile_generation,
                profile_digest=binding.profile_digest,
                concurrency_slots=binding.concurrency_slots,
                resources=binding.resources,
                node_ids=binding.node_ids,
            ),
        ),
    )


@pytest.mark.parametrize("fresh_work", ("proposal", "permit"))
async def test_drain_only_tick_rejects_new_capacity_work_without_side_effects(
    tmp_path: Path,
    fresh_work: str,
) -> None:
    launch = launch_context_fixture()
    work = (
        proposal_fixture(launch.binding)
        if fresh_work == "proposal"
        else permit_fixture(launch.binding)
    )
    executor, journal, manager, _admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=work,
    )

    with pytest.raises(Exception, match="drain-only"):
        await executor.tick_drain_only()

    assert manager.central_requests == []
    assert slurm.submit_count == 0
    journal.close()


async def test_drain_only_tick_allows_close_work(tmp_path: Path) -> None:
    launch = launch_context_fixture()
    close = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=1)
    executor, journal, manager, _admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=close,
    )

    result = await executor.tick_drain_only()

    assert result.status == "draining"
    assert manager.central_requests == [close]
    assert slurm.submit_count == 0
    journal.close()


def _durable_central_request(
    event: str,
    binding: ExecutableIntentBindingV2,
) -> tuple[StrictV2Model, str, str, str]:
    if event == "reservation-accept":
        return (
            ExecutableReservationAcceptanceV2(
                execution=binding.execution,
                tranche_id=binding.tranche_id,
                proposal_digest="d" * 64,
                pool_generation=binding.pool_generation,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                command_sequence=1,
            ),
            "tranche",
            str(binding.tranche_id),
            "accepted",
        )
    if event == "bootstrap-register":
        return (
            ExecutableBootstrapRegistrationV2(
                binding=binding,
                command_sequence=1,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256="b" * 64,
            ),
            "intent",
            str(binding.intent_id),
            "bootstrap-registered",
        )
    if event == "permit-consume":
        return (
            ExecutablePermitConsumptionV2(
                permit_id=UUID(int=200),
                permit_digest="e" * 64,
                binding=binding,
                command_sequence=1,
            ),
            "intent",
            str(binding.intent_id),
            "permit-consumed",
        )
    if event == "intent-close":
        return (
            ExecutableIntentCloseV2(binding=binding, command_sequence=1),
            "intent",
            str(binding.intent_id),
            "draining",
        )
    if event == "reservation-release":
        return (
            ExecutablePartialReleaseV2(
                execution=binding.execution,
                tranche_id=binding.tranche_id,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                command_sequence=1,
                releases=(
                    ExecutableReleasedShapeV2(
                        binding=binding,
                        inventory_sequence=1,
                        terminal_kind="slurm-job",
                        terminal_identity="101",
                        terminal_evidence_sha256="f" * 64,
                        protected_registration_epoch=2,
                        bootstrap_revoked=True,
                        protected_release_sha256="1" * 64,
                    ),
                ),
            ),
            "tranche",
            str(binding.tranche_id),
            "released",
        )
    raise AssertionError(f"unexpected central event: {event}")


@pytest.mark.parametrize(
    "event",
    (
        "reservation-accept",
        "bootstrap-register",
        "permit-consume",
        "intent-close",
        "reservation-release",
    ),
)
async def test_tick_replays_each_durable_central_request_before_fetching_work(
    tmp_path: Path,
    event: str,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, _admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=None,
    )
    request, object_kind, object_id, expected_status = _durable_central_request(
        event,
        launch.binding,
    )
    payload = canonical_executable_bytes(request)
    journal.append(
        f"{event}-requested",
        canonical_executable_digest(request),
        object_kind=object_kind,
        object_id=object_id,
        payload=payload,
    )
    manager.reject_work_fetch = True

    result = await executor.tick()

    assert result.status == expected_status
    assert manager.central_requests == [request]
    assert slurm.submit_count == 0
    confirmed = journal.latest(object_kind, object_id)
    assert confirmed is not None
    assert confirmed.event_kind == f"{event}-confirmed"
    assert confirmed.durable_payload() == payload
    journal.close()


# Production break caught: a crash after protected preparation could leave no
# durable request and cause a different registration epoch on the next tick.
async def test_protected_bootstrap_is_journaled_and_replayed_exactly_after_crash(
    tmp_path: Path,
) -> None:
    binding = launch_context_fixture().binding
    executor, journal, _manager, admission, _slurm, _launch = executor_fixture(
        tmp_path,
        work=binding,
    )
    admission.crash_after_prepare = True

    with pytest.raises(SimulatedCrash):
        await executor.tick()

    requested = journal.latest("bootstrap", str(binding.intent_id))
    assert requested is not None
    assert requested.event_kind == "protected-bootstrap-requested"

    admission.crash_after_prepare = False
    result = await executor.tick()

    assert result.status == "bootstrap-registered"
    assert admission.prepare_requests[1] == admission.prepare_requests[0]
    journal.close()


# Production break caught: an inventory timeout could be retried with a newer
# journal head, equivocating at the manager instead of replaying exact bytes.
async def test_inventory_publish_is_journaled_and_replayed_exactly_after_timeout(
    tmp_path: Path,
) -> None:
    executor, journal, manager, _admission, _slurm, _launch = executor_fixture(
        tmp_path,
        work=None,
    )
    manager.inventory_failure = ExecutorTransportError("transport failed")

    with pytest.raises(ExecutorTransportError):
        await executor.tick()

    requested = journal.latest("inventory", str(manager.registration.executor_incarnation))
    assert requested is not None
    assert requested.event_kind == "inventory-publish-requested"

    manager.inventory_failure = None
    result = await executor.tick()

    assert result.status == "inventory-published"
    assert manager.inventories[1] == manager.inventories[0]
    journal.close()


# Production break caught: a job that left the live queue was never projected
# from accounting as terminal inventory, so the manager could never issue release.
async def test_idle_inventory_publishes_exact_owned_terminal_accounting_evidence(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, _admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    job = slurm.jobs.pop()
    terminal = SlurmTerminalEvidenceV2(
        cluster=job.cluster,
        job_id=job.job_id,
        state="COMPLETED",
        submitter=job.submitter,
        account=job.account,
        partition=job.partition,
        submitted_at=_NOW,
        started_at=_NOW,
        ended_at=_NOW + timedelta(minutes=1),
        elapsed_seconds=60,
        exit_code="0:0",
        cpus=job.cpus,
        memory_bytes=job.memory_bytes,
        gpus=job.gpus,
        generic_tres=job.generic_tres,
        nodes=launch.binding.node_ids,
        ownership_token=job.ownership_token,
    )
    slurm.terminal_jobs = (terminal,)
    manager.work = None

    result = await executor.tick()

    assert result.status == "inventory-published"
    record = manager.inventories[-1].records[0]
    assert record.physical_identity == job.job_id
    assert record.state == "terminal"
    assert (
        record.terminal_evidence_sha256
        == hashlib.sha256(
            json.dumps(
                terminal.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
    )
    assert record.ownership_proof is not None
    journal.close()


# Production break caught: a live job whose resources changed lost its durable
# ownership proof, so the manager could not attribute and quarantine the intent.
async def test_resource_mismatch_inventory_retains_owned_binding_for_quarantine(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, _admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    submitted = slurm.jobs[0]
    unexpected_tres = SlurmTresValueV2(name="gres/network", value=1)
    observed_tres = (
        *(
            item.model_copy(update={"value": item.value + 1}) if item.name == "gres/fpga" else item
            for item in submitted.generic_tres
        ),
        unexpected_tres,
    )
    assert observed_tres != submitted.generic_tres
    assert all(isinstance(item, SlurmTresValueV2) for item in observed_tres)
    slurm.jobs[0] = submitted.model_copy(
        update={"cpus": submitted.cpus + 1, "generic_tres": observed_tres}
    )

    result = await executor.tick()

    assert result.status == "inventory-published"
    record = manager.inventories[-1].records[0]
    assert record.ownership_proof is not None
    assert record.ownership_proof.metadata.binding == launch.binding
    assert record.resources.cpu_millicores == (submitted.cpus + 1) * 1_000
    unexpected_resource = (
        f"slurm_unmapped_{hashlib.sha256(unexpected_tres.name.encode('ascii')).hexdigest()[:40]}"
    )
    assert record.resources.generic == {
        "fpga": 2,
        "gpu_a100": 2,
        unexpected_resource: 1,
    }
    journal.close()


# Production break caught: live owned-job inventory must decode Slurm generic
# TRES with the profile bound in that job's ownership proof, not the executor's
# arbitrary primary profile.
async def test_live_inventory_maps_generic_tres_with_each_owned_job_profile(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    primary = launch.profile
    secondary = _alternate_generic_profile(primary)
    binding = _binding_for_profile(
        launch.binding,
        secondary,
        intent_id=UUID("00000000-0000-0000-0000-000000000302"),
    )
    executor, journal, manager, _admission, _slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(binding),
        profiles=(primary, secondary),
    )

    await executor.tick()
    manager.work = None
    result = await executor.tick()

    assert result.status == "inventory-published"
    record = manager.inventories[-1].records[0]
    assert record.ownership_proof is not None
    assert record.ownership_proof.metadata.binding == binding
    assert record.resources.generic == {"asic": 3}
    assert record.resources == binding.resources
    journal.close()


# Production break caught: terminal owned-job inventory must retain the exact
# profile-bound resources for a non-primary profile so retirement evidence is
# not decoded through the wrong resource vocabulary.
async def test_terminal_inventory_keeps_generic_resources_from_owned_job_profile(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    primary = launch.profile
    secondary = _alternate_generic_profile(primary)
    binding = _binding_for_profile(
        launch.binding,
        secondary,
        intent_id=UUID("00000000-0000-0000-0000-000000000303"),
    )
    executor, journal, manager, _admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(binding),
        profiles=(primary, secondary),
    )

    await executor.tick()
    job = slurm.jobs.pop()
    slurm.terminal_jobs = (
        SlurmTerminalEvidenceV2(
            cluster=job.cluster,
            job_id=job.job_id,
            state="COMPLETED",
            submitter=job.submitter,
            account=job.account,
            partition=job.partition,
            submitted_at=_NOW,
            started_at=_NOW,
            ended_at=_NOW + timedelta(minutes=1),
            elapsed_seconds=60,
            exit_code="0:0",
            cpus=job.cpus,
            memory_bytes=job.memory_bytes,
            gpus=job.gpus,
            generic_tres=job.generic_tres,
            nodes=binding.node_ids,
            ownership_token=job.ownership_token,
        ),
    )
    manager.work = None
    result = await executor.tick()

    assert result.status == "inventory-published"
    record = manager.inventories[-1].records[0]
    assert record.state == "terminal"
    assert record.ownership_proof is not None
    assert record.ownership_proof.metadata.binding == binding
    assert record.resources == binding.resources
    journal.close()


# Production break caught: timeout/5xx could be recorded as rejected and permit a
# later scheduler mutation without knowing whether the central transition committed.
@pytest.mark.parametrize(
    "failure",
    (ExecutorTransportError("status 503"), ExecutorTransportError("transport failed")),
)
async def test_unresolved_central_failure_stays_requested(
    tmp_path: Path, failure: Exception
) -> None:
    executor, journal, manager, _admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch_context_fixture().binding)
    )
    manager.failure = failure
    with pytest.raises(ExecutorTransportError):
        await executor.tick()

    pending = journal.pending_requests()
    assert len(pending) == 1
    assert pending[0].event_kind == "permit-consume-requested"
    assert slurm.jobs == []
    assert launch.binding.intent_id == permit_fixture(launch.binding).binding.intent_id
    journal.close()


# Production break caught: a verified 4xx could be retried forever as an ambiguous
# transition rather than durably ending the exact operation as rejected.
async def test_verified_central_rejection_is_journaled(tmp_path: Path) -> None:
    executor, journal, manager, _admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch_context_fixture().binding)
    )
    manager.failure = ExecutorRejectedError("status 409")
    with pytest.raises(ExecutorRejectedError):
        await executor.tick()

    latest = journal.latest("intent", str(launch.binding.intent_id))
    assert latest is not None
    assert latest.event_kind == "permit-consume-rejected"
    assert journal.pending_requests() == ()
    assert slurm.jobs == []
    journal.close()


# Production break caught: the trusted renderer could sign or submit a fresh UUID
# rather than the manager-authored stable intent operation identity.
async def test_launch_uses_exact_signed_operation_identity(tmp_path: Path) -> None:
    executor, journal, _manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch_context_fixture().binding)
    )
    result = await executor.tick()

    assert result.status == "submitted"
    assert slurm.jobs[0].ownership_token != ""
    submitted = journal.latest("intent", str(launch.binding.intent_id))
    assert submitted is not None
    assert submitted.event_kind == "physical-bind-confirmed"
    assert admission.bound[launch.binding.intent_id].slurm_job_id == "101"
    assert result.operation_id == launch.binding.intent_id
    journal.close()


# Production break caught: protected bootstrap and physical binding cannot reuse
# one durable operation identity, and the protected request must equal the bytes
# retained for recovery.
async def test_physical_bind_uses_distinct_deterministic_journaled_operation(
    tmp_path: Path,
) -> None:
    executor, journal, _manager, admission, _slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch_context_fixture().binding),
    )

    await executor.tick()

    request = admission.bind_requests[0]
    assert request.operation_id == UUID("23661071-f8d3-57ba-8707-581ddecc9fee")
    assert request.operation_id != launch.binding.intent_id
    retained = journal.latest("intent", str(launch.binding.intent_id))
    assert retained is not None
    assert retained.event_kind == "physical-bind-confirmed"
    assert retained.durable_payload() == canonical_executable_bytes(request)
    journal.close()


# Production break caught: pending cancellation could race ahead of the protected
# claim fence. The fake scheduler refuses cancellation until real drain state exists.
async def test_protected_drain_precedes_conditional_pending_cancel(tmp_path: Path) -> None:
    launch = launch_context_fixture()
    permit = permit_fixture(launch.binding)
    executor, journal, _manager, admission, slurm, _launch = executor_fixture(tmp_path, work=permit)
    await executor.tick()
    _manager.work = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    worker_id = UUID(int=301)
    worker_incarnation = UUID(int=302)
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=worker_id,
        worker_incarnation=worker_incarnation,
        protected_registration_epoch=2,
        claim_high_water=0,
    )

    result = await executor.tick()

    assert result.status == "pending-cancelled"
    assert admission.observations[launch.binding.intent_id].drain is not None
    assert slurm.jobs == []
    journal.close()


# Production break caught: ordinary reclamation could use a broad worker/job signal
# after drain instead of waiting for an active worker to terminate naturally.
async def test_ordinary_reclamation_never_signals_active_worker(tmp_path: Path) -> None:
    launch = launch_context_fixture()
    permit = permit_fixture(launch.binding)
    executor, journal, _manager, admission, slurm, _launch = executor_fixture(tmp_path, work=permit)
    await executor.tick()
    _manager.work = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=UUID(int=311),
        worker_incarnation=UUID(int=312),
        protected_registration_epoch=2,
        claim_high_water=0,
    )
    pending = slurm.jobs[0]
    active = pending.model_copy(
        update={"state": "RUNNING", "nodes": launch.binding.node_ids, "pending_reason": None}
    )
    slurm.jobs[0] = active

    result = await executor.tick()

    assert result.status == "draining"
    assert slurm.jobs == [active]
    assert admission.observations[launch.binding.intent_id].drain is not None
    journal.close()


# Production break caught: an exact pending physical job with no registered
# worker stayed quarantined instead of first revoking bootstrap, fencing late
# registration, and then conditionally cancelling the owned pending job.
async def test_close_withdraws_bound_unregistered_pending_job_before_cancel(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    pending = slurm.jobs[0]
    prior_requests = tuple(manager.central_requests)
    close = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    manager.work = close

    result = await executor.tick()

    assert result.status == "pending-cancelled"
    assert result.operation_id == launch.binding.intent_id
    assert manager.command_sequence == 2
    assert manager.work is None
    assert tuple(manager.central_requests) == (*prior_requests, close)
    assert slurm.jobs == []
    assert len(admission.withdraw_requests) == 1
    withdrawal = admission.withdraw_requests[0]
    assert withdrawal.binding == launch.binding
    assert withdrawal.slurm_job_id == pending.job_id
    assert (
        withdrawal.ownership_evidence_sha256 == admission.bind_requests[0].ownership_evidence_sha256
    )
    assert withdrawal.expected_claim_high_water == 0
    assert withdrawal.bootstrap_registration_epoch == 1
    assert withdrawal.protected_registration_epoch == 2
    retained = journal.latest("intent", str(launch.binding.intent_id))
    assert retained is not None and retained.event_kind == "intent-close-confirmed"
    journal.close()


# Production break caught: after a protected drain, cancellation was selected from
# any exact-looking pending observation instead of the protected physical binding.
async def test_close_never_cancels_unbound_duplicate_pending_job_after_drain(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    bound = slurm.jobs[0]
    duplicate = bound.model_copy(update={"job_id": "202"})
    slurm.jobs[:] = [duplicate]
    manager.work = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=UUID(int=501),
        worker_incarnation=UUID(int=502),
        protected_registration_epoch=2,
        claim_high_water=0,
    )

    result = await executor.tick()

    assert result.status == "quarantined"
    assert admission.observations[launch.binding.intent_id].drain is not None
    assert slurm.cancel_requests == []
    assert slurm.jobs == [duplicate]
    assert manager.work is not None
    journal.close()


# Production break caught: pending observations with missing or changed requested
# nodes were treated as cancellable because PENDING node sets were ignored.
async def test_close_never_cancels_bound_pending_job_with_node_mismatch(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    pending = slurm.jobs[0]
    changed = pending.model_copy(update={"nodes": ("oldlab-6",)})
    slurm.jobs[0] = changed
    manager.work = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)

    result = await executor.tick()

    assert result.status == "quarantined"
    assert admission.withdraw_requests == []
    assert slurm.cancel_requests == []
    assert slurm.jobs == [changed]
    assert manager.work is not None
    journal.close()


# Production break caught: a manager-authored release could be applied locally
# after only one of the protected or physical terminal fences was observed.
async def test_release_requires_protected_and_physical_terminal_evidence(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, _slurm, _launch = executor_fixture(tmp_path, work=None)
    release = ExecutablePartialReleaseV2.model_construct(
        execution=launch.binding.execution,
        tranche_id=launch.binding.tranche_id,
        executor_id=launch.binding.executor_id,
        executor_incarnation=launch.binding.executor_incarnation,
        command_sequence=1,
        releases=(),
        executable=True,
    )
    manager.work = release

    result = await executor.tick()

    assert result.status == "quarantined"
    assert manager.releases == []
    assert admission.observations == {}
    journal.close()


def test_fixture_registration_is_exact() -> None:
    launch = launch_context_fixture()
    binding = PreparedExecutorBindingV2(
        pool_id="oldlab",
        pool_generation=launch.binding.pool_generation,
        executor_id=launch.binding.executor_id,
        executor_incarnation=launch.binding.executor_incarnation,
        signing_key_sha256=launch.ownership_key.public_key_sha256,
        local_authority_sha256="a" * 64,
        controller_authority_sha256=launch.profile.controller_authority_sha256,
    )
    assert binding.executor_incarnation == launch.binding.executor_incarnation
