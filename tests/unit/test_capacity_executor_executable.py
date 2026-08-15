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

import loom_capacity_executor.executable as executable_module
from loom_capacity_agent.admission import (
    BoundExecutableWorkerV2,
    DrainedExecutableWorkerV2,
    ExecutableReleaseReceiptV2,
    PhysicalJobBindingV2,
    PreparedExecutableAdmissionV2,
    RevokedExecutableBootstrapV2,
    WithdrawnExecutableWorkerV2,
)
from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffStore
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
from loom_capacity_executor.runtime import RuntimeAssemblyError
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
        self.prepared_revocation_requests: list[Any] = []
        self.crash_after_prepared_revocation = False
        self.drain_requests: list[Any] = []
        self.crash_after_drain = False
        self.observations: dict[UUID, ProtectedIntentObservationV2] = {}

    def bootstrap_handoff_route_sha256(self, _binding: object) -> str:
        return "1" * 64

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
        self.observations[request.binding.intent_id] = ProtectedIntentObservationV2(
            binding=request.binding,
            bootstrap_registration_epoch=request.bootstrap_registration_epoch,
        )
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
        current = self.observations.setdefault(
            request.binding.intent_id,
            ProtectedIntentObservationV2(
                binding=request.binding,
                bootstrap_registration_epoch=request.bootstrap_registration_epoch,
                claim_high_water=request.expected_claim_high_water,
            ),
        )
        receipt = WithdrawnExecutableWorkerV2(
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
        )
        self.observations[request.binding.intent_id] = current.model_copy(
            update={"withdrawal": receipt}
        )
        return receipt

    async def revoke_prepared_bootstrap(self, request: Any) -> SimpleNamespace:
        self.prepared_revocation_requests.append(request)
        receipt = RevokedExecutableBootstrapV2(
            binding=request.binding,
            reporter_incarnation=UUID(int=9),
            bootstrap_registration_epoch=request.bootstrap_registration_epoch,
            protected_registration_epoch=request.protected_registration_epoch,
            claim_high_water=0,
            live_claim_count=0,
            bootstrap_revoked=True,
            request_digest=canonical_executable_digest(request),
            protected_release_sha256=canonical_executable_digest(request),
            protected_high_water=request.protected_registration_epoch,
        )
        if self.crash_after_prepared_revocation:
            raise SimulatedCrash("prepared revocation committed before response loss")
        self.observations[request.binding.intent_id] = self.observations[
            request.binding.intent_id
        ].model_copy(update={"prepared_revocation": receipt})
        return receipt


class FakeSlurm:
    def __init__(self) -> None:
        self.jobs: list[SlurmJobObservationV2] = []
        self.submit_count = 0
        self.crash_before_submit_commit = False
        self.crash_after_submit = False
        self.admission: FakeAdmission | None = None
        self.terminal_jobs: tuple[SlurmTerminalEvidenceV2, ...] = ()
        self.cancel_requests: list[SlurmCancelRequestV2] = []
        self.cancel_failure: Exception | None = None
        self.cancel_commits_before_failure = False

    async def submit(self, request: Any) -> SlurmSubmissionV2:
        if self.crash_before_submit_commit:
            raise SimulatedCrash("process stopped before sbatch outcome was known")
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


def _terminal_from_job(job: SlurmJobObservationV2) -> SlurmTerminalEvidenceV2:
    return SlurmTerminalEvidenceV2(
        cluster=job.cluster,
        job_id=job.job_id,
        state="COMPLETED",
        submitter=job.submitter,
        account=job.account,
        partition=job.partition,
        submitted_at=_NOW,
        started_at=_NOW,
        ended_at=_NOW,
        elapsed_seconds=0,
        exit_code="0:0",
        cpus=job.cpus,
        memory_bytes=job.memory_bytes,
        gpus=job.gpus,
        generic_tres=job.generic_tres,
        nodes=job.nodes,
        ownership_token=job.ownership_token,
    )


def _job_from_launch(
    launch: TrustedLaunchContextV2,
    *,
    rendered_request: Any,
    job_id: str = "101",
) -> SlurmJobObservationV2:
    return SlurmJobObservationV2(
        cluster=rendered_request.cluster,
        job_id=job_id,
        state="RUNNING",
        submitter=rendered_request.submitter,
        account=rendered_request.account,
        partition=rendered_request.partition,
        cpus=rendered_request.cpus,
        memory_bytes=rendered_request.memory_bytes,
        gpus=rendered_request.gpus,
        generic_tres=rendered_request.generic_tres,
        nodes=launch.binding.node_ids,
        ownership_token=rendered_request.ownership_token,
    )


def _hide_latest_launch_record(journal: ExecutorJournal, intent_id: UUID) -> None:
    retained = journal.latest("job", str(intent_id))
    assert retained is not None
    payload = retained.durable_payload()
    assert payload is not None
    journal.append(
        "slurm-submit-terminal-observed",
        retained.payload_digest,
        object_kind="job",
        object_id=str(intent_id),
        payload=payload,
    )


def _protected_release_receipt(
    binding: ExecutableIntentBindingV2,
    *,
    digest: str,
    protected_registration_epoch: int = 2,
) -> ExecutableReleaseReceiptV2:
    return ExecutableReleaseReceiptV2(
        binding=binding,
        reporter_incarnation=UUID(int=9),
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        protected_registration_epoch=protected_registration_epoch,
        release_epoch=1,
        request_digest=digest,
        protected_release_sha256=digest,
        protected_high_water=protected_registration_epoch,
    )


def _withdrawal_receipt(
    binding: ExecutableIntentBindingV2,
    *,
    digest: str,
    protected_registration_epoch: int = 2,
    slurm_job_id: str = "101",
) -> WithdrawnExecutableWorkerV2:
    return WithdrawnExecutableWorkerV2(
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        intent_id=binding.intent_id,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=protected_registration_epoch,
        slurm_job_id=slurm_job_id,
        ownership_evidence_sha256="e" * 64,
        request_digest=digest,
        withdrawal_digest=digest,
        protected_high_water=protected_registration_epoch,
    )


def _prepared_revocation_receipt(
    binding: ExecutableIntentBindingV2,
    *,
    digest: str,
    protected_registration_epoch: int = 2,
) -> RevokedExecutableBootstrapV2:
    return RevokedExecutableBootstrapV2(
        binding=binding,
        reporter_incarnation=UUID(int=9),
        bootstrap_registration_epoch=1,
        protected_registration_epoch=protected_registration_epoch,
        claim_high_water=0,
        live_claim_count=0,
        request_digest=digest,
        protected_release_sha256=digest,
        protected_high_water=protected_registration_epoch,
    )


def _release_work(
    binding: ExecutableIntentBindingV2,
    *,
    command_sequence: int = 1,
    inventory_sequence: int = 1,
    terminal_kind: str = "slurm-job",
    terminal_identity: str = "101",
    terminal_evidence_sha256: str,
    protected_release_sha256: str,
    protected_registration_epoch: int = 2,
) -> ExecutablePartialReleaseV2:
    return ExecutablePartialReleaseV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=command_sequence,
        releases=(
            ExecutableReleasedShapeV2(
                binding=binding,
                inventory_sequence=inventory_sequence,
                terminal_kind=terminal_kind,  # type: ignore[arg-type]
                terminal_identity=terminal_identity,
                terminal_evidence_sha256=terminal_evidence_sha256,
                protected_registration_epoch=protected_registration_epoch,
                bootstrap_revoked=True,
                protected_release_sha256=protected_release_sha256,
            ),
        ),
    )


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


# Production break caught: render/acceptance must consume the same public
# runtime profile resolver used by daemon assembly. Reintroducing a private
# duplicate matcher would let executor work selection drift from runtime policy.
def test_render_launch_uses_public_runtime_profile_resolver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launch = launch_context_fixture()
    executor, journal, _manager, _admission, _slurm, _launch = executor_fixture(
        tmp_path,
        work=None,
    )
    calls: list[tuple[UUID, int, str]] = []

    def resolver(
        binding: ExecutableIntentBindingV2,
        profiles: tuple[OperatorLaunchProfileV2, ...],
        *,
        controller_authority_sha256: str,
    ) -> OperatorLaunchProfileV2:
        calls.append((binding.intent_id, len(profiles), controller_authority_sha256))
        raise RuntimeAssemblyError("resolver sentinel")

    monkeypatch.setattr(
        executable_module,
        "resolve_runtime_profile",
        resolver,
        raising=False,
    )

    with pytest.raises(RuntimeAssemblyError, match="resolver sentinel"):
        executor.render_launch(launch.binding)

    assert calls == [
        (
            launch.binding.intent_id,
            1,
            launch.controller_authority.controller_authority_sha256,
        )
    ]
    journal.close()


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
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=close,
    )

    async def _unexpected_observe(_binding: ExecutableIntentBindingV2) -> object:
        raise AssertionError("accepted close must not require protected observation")

    admission.observe_intent = _unexpected_observe  # type: ignore[method-assign]
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
                pool_id=binding.pool_id,
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
    if isinstance(request, ExecutablePartialReleaseV2):
        terminal = _terminal_from_job(
            SlurmJobObservationV2(
                cluster="oldlab",
                job_id="101",
                state="RUNNING",
                submitter="loom-oldlab",
                account="loom-executor",
                partition="batch",
                cpus=launch.profile.cpus,
                memory_bytes=launch.binding.resources.memory_bytes,
                gpus=launch.binding.resources.gpu_count,
                generic_tres=(),
                nodes=launch.binding.node_ids,
                ownership_token="a" * 43,
            )
        )
        terminal_digest = executable_module.ExecutablePoolExecutor._slurm_evidence_digest(terminal)
        release_item = request.releases[0].model_copy(
            update={"terminal_evidence_sha256": terminal_digest}
        )
        request = request.model_copy(update={"releases": (release_item,)})
        _admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
            binding=launch.binding,
            bootstrap_registration_epoch=1,
            worker_id=UUID(int=801),
            worker_incarnation=UUID(int=802),
            protected_registration_epoch=2,
            claim_high_water=0,
            release=_protected_release_receipt(launch.binding, digest="1" * 64),
        )
        slurm.terminal_jobs = (terminal,)
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


async def test_close_publishes_terminal_inventory_before_central_close_after_drain(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    permit = permit_fixture(launch.binding)
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit,
    )
    await executor.tick()
    prior_requests = tuple(manager.central_requests)
    close = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    manager.work = close
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=UUID(int=321),
        worker_incarnation=UUID(int=322),
        protected_registration_epoch=2,
        claim_high_water=0,
    )
    active = slurm.jobs[0].model_copy(
        update={"state": "RUNNING", "nodes": launch.binding.node_ids, "pending_reason": None}
    )
    slurm.jobs[0] = active

    first = await executor.tick()

    assert first.status == "draining"
    assert tuple(manager.central_requests) == prior_requests
    slurm.jobs.clear()
    terminal = _terminal_from_job(active)
    slurm.terminal_jobs = (terminal,)

    second = await executor.tick()

    assert second.status == "inventory-published"
    assert tuple(manager.central_requests) == prior_requests
    inventory = manager.inventories[-1]
    record = next(
        record
        for record in inventory.records
        if record.ownership_proof is not None
        and record.ownership_proof.metadata.binding.intent_id == launch.binding.intent_id
    )
    assert record.state == "terminal"
    assert (
        record.terminal_evidence_sha256
        == executable_module.ExecutablePoolExecutor._slurm_evidence_digest(terminal)
    )

    third = await executor.tick()

    assert third.status == "draining"
    assert manager.work is None
    assert tuple(manager.central_requests) == (*prior_requests, close)
    retained = journal.latest("intent", str(launch.binding.intent_id))
    assert retained is not None and retained.event_kind == "intent-close-confirmed"
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
    assert manager.command_sequence == 1
    assert manager.work == close
    assert tuple(manager.central_requests) == prior_requests
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
    assert retained is not None and retained.event_kind == "protected-withdraw-confirmed"
    slurm.terminal_jobs = (_terminal_from_job(pending),)

    inventory = await executor.tick()

    assert inventory.status == "inventory-published"
    assert manager.work == close

    closed = await executor.tick()

    assert closed.status == "draining"
    assert manager.command_sequence == 2
    assert manager.work is None
    assert tuple(manager.central_requests) == (*prior_requests, close)
    retained = journal.latest("intent", str(launch.binding.intent_id))
    assert retained is not None and retained.event_kind == "intent-close-confirmed"
    journal.close()


async def test_close_revokes_prepared_bootstrap_without_a_physical_job(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    prepared = await executor.tick()
    assert prepared.status == "bootstrap-registered"
    close = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    manager.work = close

    result = await executor.tick()

    assert result.status == "draining"
    assert manager.work is None
    assert slurm.jobs == []
    assert len(admission.prepared_revocation_requests) == 1
    revocation = admission.prepared_revocation_requests[0]
    assert revocation.binding == launch.binding
    assert revocation.bootstrap_registration_epoch == 1
    assert revocation.protected_registration_epoch == 2
    assert manager.central_requests[-1] == close
    requested = journal.latest("prepared-revocation", str(launch.binding.intent_id))
    central = journal.latest("intent", str(launch.binding.intent_id))
    assert requested is not None
    assert requested.event_kind == "protected-prepared-revocation-confirmed"
    assert central is not None
    assert central.event_kind == "intent-close-confirmed"
    assert requested.sequence < central.sequence
    assert slurm.submit_count == 0
    journal.close()


# Production break caught: a public close for a launch-ready/permitted but
# unconsumed intent has no scheduler envelope and no handoff ownership sidecar;
# that is the safe boundary where prepared revocation may remove the clear
# handoff before central close.
async def test_close_revokes_unconsumed_handoff_before_central_close(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    handoff_directory = tmp_path / "handoff"
    handoff_directory.mkdir(mode=0o700)
    handoff_store = BootstrapHandoffStore(handoff_directory)
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    executor._bootstrap_handoff_store = handoff_store
    executor._bootstrap_digest = None
    await executor.tick()
    reference = handoff_store.reference_for(launch.binding)
    handoff_path = handoff_directory / reference
    assert handoff_path.exists()
    assert not handoff_path.with_suffix(".ownership").exists()
    assert journal.latest("job", str(launch.binding.intent_id)) is None
    close = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    manager.work = close

    result = await executor.tick()

    assert result.status == "draining"
    assert not handoff_path.exists()
    assert not handoff_path.with_suffix(".ownership").exists()
    assert slurm.submit_count == 0
    assert len(admission.prepared_revocation_requests) == 1
    assert manager.central_requests[-1] == close
    deleted = journal.latest("prepared-revocation", str(launch.binding.intent_id))
    assert deleted is not None
    assert deleted.event_kind == "prepared-handoff-deleted"
    central = journal.latest("intent", str(launch.binding.intent_id))
    assert central is not None
    assert central.event_kind == "intent-close-confirmed"
    assert deleted.sequence < central.sequence
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


# Production break caught: a live-worker protected release could be ignored even
# when the exact protected receipt and exact Slurm terminal evidence both exist.
async def test_protected_terminal_release_digest_allows_live_worker_release(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    job = slurm.jobs.pop()
    terminal = _terminal_from_job(job)
    terminal_digest = executable_module.ExecutablePoolExecutor._slurm_evidence_digest(terminal)
    protected_digest = "1" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=UUID(int=701),
        worker_incarnation=UUID(int=702),
        protected_registration_epoch=2,
        claim_high_water=0,
        release=_protected_release_receipt(launch.binding, digest=protected_digest),
    )
    slurm.terminal_jobs = (terminal,)
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        terminal_evidence_sha256=terminal_digest,
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "released"
    assert manager.releases == [manager.work] or manager.work is None
    assert len(manager.releases) == 1
    journal.close()


# Production break caught: unregistered physical withdrawals must release only
# after their exact withdrawal digest and exact terminal Slurm evidence match.
async def test_withdrawal_release_digest_requires_exact_terminal_slurm_evidence(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    job = slurm.jobs.pop()
    terminal = _terminal_from_job(job)
    terminal_digest = executable_module.ExecutablePoolExecutor._slurm_evidence_digest(terminal)
    protected_digest = "2" * 64
    observation = ProtectedIntentObservationV2.model_construct(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        claim_high_water=0,
        withdrawal=_withdrawal_receipt(launch.binding, digest=protected_digest),
        executable=True,
    )
    admission.observations[launch.binding.intent_id] = observation
    slurm.terminal_jobs = (terminal,)
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        terminal_evidence_sha256=terminal_digest,
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "released"
    assert len(manager.releases) == 1
    journal.close()


# Production break caught: prepared revocation for an unused intent must be
# proven from the exact confirmed inventory payload/digest/sequence, not Slurm
# accounting absence or a protected digest alone.
async def test_unused_release_uses_exact_confirmed_inventory_and_prepared_revocation(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    await executor.tick()
    await executor.tick()
    inventory = manager.inventories[-1]
    inventory_digest = canonical_executable_digest(inventory)
    protected_digest = "3" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        prepared_revocation=_prepared_revocation_receipt(
            launch.binding,
            digest=protected_digest,
        ),
    )
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        inventory_sequence=inventory.inventory_sequence,
        terminal_kind="unused",
        terminal_identity="unused-101",
        terminal_evidence_sha256=inventory_digest,
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "released"
    assert slurm.cancel_requests == []
    assert len(manager.releases) == 1
    journal.close()


# Production break caught: reducing protected terminal receipts to a digest let
# prepared revocation authorize a Slurm terminal release instead of only unused.
async def test_protected_terminal_prepared_revocation_rejects_slurm_terminal_kind(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    await executor.tick()
    terminal = SlurmTerminalEvidenceV2(
        cluster="oldlab",
        job_id="101",
        state="COMPLETED",
        submitter="loom-oldlab",
        account="loom-executor",
        partition="batch",
        submitted_at=_NOW,
        started_at=_NOW,
        ended_at=_NOW,
        elapsed_seconds=0,
        exit_code="0:0",
        cpus=1,
        memory_bytes=1024,
        gpus=0,
        generic_tres=(),
        nodes=(),
        ownership_token="a" * 43,
    )
    terminal_digest = executable_module.ExecutablePoolExecutor._slurm_evidence_digest(terminal)
    protected_digest = "8" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        prepared_revocation=_prepared_revocation_receipt(
            launch.binding,
            digest=protected_digest,
        ),
    )
    slurm.terminal_jobs = (terminal,)
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        terminal_kind="slurm-job",
        terminal_identity="101",
        terminal_evidence_sha256=terminal_digest,
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "quarantined"
    assert result.detail == "protected terminal evidence is absent or changed"
    assert manager.releases == []
    journal.close()


# Production break caught: a withdrawal for one Slurm job could be released
# using a different terminal job with the same withdrawal digest.
async def test_withdrawal_release_rejects_changed_terminal_slurm_job_id(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    job = slurm.jobs.pop().model_copy(update={"job_id": "999"})
    terminal = _terminal_from_job(job)
    terminal_digest = executable_module.ExecutablePoolExecutor._slurm_evidence_digest(terminal)
    protected_digest = "9" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        withdrawal=_withdrawal_receipt(
            launch.binding,
            digest=protected_digest,
            slurm_job_id="101",
        ),
    )
    slurm.terminal_jobs = (terminal,)
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        terminal_identity="999",
        terminal_evidence_sha256=terminal_digest,
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "quarantined"
    assert result.detail == "physical terminal evidence is absent or changed"
    assert manager.releases == []
    journal.close()


# Production break caught: delayed release for inventory N was rejected after a
# later confirmed complete inventory N+1 became the latest local inventory.
async def test_unused_release_accepts_exact_historical_confirmed_inventory_sequence(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, _slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    await executor.tick()
    await executor.tick()
    inventory_n = manager.inventories[-1]
    await executor.tick()
    assert manager.inventories[-1].inventory_sequence == inventory_n.inventory_sequence + 1
    protected_digest = "a" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        prepared_revocation=_prepared_revocation_receipt(
            launch.binding,
            digest=protected_digest,
        ),
    )
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        inventory_sequence=inventory_n.inventory_sequence,
        terminal_kind="unused",
        terminal_identity="unused-101",
        terminal_evidence_sha256=canonical_executable_digest(inventory_n),
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "released"
    assert len(manager.releases) == 1
    journal.close()


# Production break caught: unused release checked an impossible physical-bind
# journal key and missed the real executor-scoped durable physical binding.
async def test_unused_release_rejects_real_durable_physical_binding_record(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, _slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    await executor.tick()
    await executor.tick()
    inventory = manager.inventories[-1]
    physical = PhysicalJobBindingV2(
        operation_id=UUID(int=901),
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        slurm_job_id="101",
        ownership_evidence_sha256="e" * 64,
    )
    journal.append(
        "physical-bind-confirmed",
        canonical_executable_digest(physical),
        object_kind="executor",
        object_id=f"physical-bind:{launch.binding.intent_id}",
        payload=canonical_executable_bytes(physical),
    )
    protected_digest = "b" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        prepared_revocation=_prepared_revocation_receipt(
            launch.binding,
            digest=protected_digest,
        ),
    )
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        inventory_sequence=inventory.inventory_sequence,
        terminal_kind="unused",
        terminal_identity="unused-101",
        terminal_evidence_sha256=canonical_executable_digest(inventory),
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "quarantined"
    assert result.detail == "unused terminal still has local physical ownership"
    assert manager.releases == []
    journal.close()


@pytest.mark.parametrize("scheduler_state", ("live", "terminal"))
async def test_unused_release_rejects_exact_owned_current_slurm_match(
    tmp_path: Path,
    scheduler_state: str,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    await executor.tick()
    await executor.tick()
    inventory = manager.inventories[-1]
    rendered = executor.render_launch(launch.binding)
    executor._remember_launch(
        rendered,
        bootstrap_registration_epoch=1,
        event="slurm-submit-confirmed",
    )
    _hide_latest_launch_record(journal, launch.binding.intent_id)
    job = _job_from_launch(launch, rendered_request=rendered.request, job_id="101")
    if scheduler_state == "live":
        slurm.jobs.append(job)
    else:
        slurm.terminal_jobs = (_terminal_from_job(job),)
    protected_digest = "c" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        prepared_revocation=_prepared_revocation_receipt(
            launch.binding,
            digest=protected_digest,
        ),
    )
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        inventory_sequence=inventory.inventory_sequence,
        terminal_kind="unused",
        terminal_identity="unused-101",
        terminal_evidence_sha256=canonical_executable_digest(inventory),
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "quarantined"
    assert result.detail == "unused terminal still has local physical ownership"
    assert manager.releases == []
    journal.close()


@pytest.mark.parametrize("scheduler_state", ("live", "terminal"))
async def test_unused_release_rejects_authenticated_ownership_conflict(
    tmp_path: Path,
    scheduler_state: str,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=launch.binding,
    )
    await executor.tick()
    await executor.tick()
    inventory = manager.inventories[-1]
    rendered = executor.render_launch(launch.binding)
    executor._remember_launch(
        rendered,
        bootstrap_registration_epoch=1,
        event="slurm-submit-confirmed",
    )
    _hide_latest_launch_record(journal, launch.binding.intent_id)
    conflict = _job_from_launch(
        launch,
        rendered_request=rendered.request,
        job_id="101",
    ).model_copy(update={"cpus": rendered.request.cpus + 1})
    if scheduler_state == "live":
        slurm.jobs.append(conflict)
    else:
        slurm.terminal_jobs = (_terminal_from_job(conflict),)
    protected_digest = "d" * 64
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        prepared_revocation=_prepared_revocation_receipt(
            launch.binding,
            digest=protected_digest,
        ),
    )
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        inventory_sequence=inventory.inventory_sequence,
        terminal_kind="unused",
        terminal_identity="unused-101",
        terminal_evidence_sha256=canonical_executable_digest(inventory),
        protected_release_sha256=protected_digest,
    )

    result = await executor.tick()

    assert result.status == "quarantined"
    assert result.detail == "unused terminal still has local physical ownership"
    assert manager.releases == []
    assert slurm.jobs == ([conflict] if scheduler_state == "live" else [])
    assert slurm.terminal_jobs == (
        (_terminal_from_job(conflict),) if scheduler_state == "terminal" else ()
    )
    journal.close()


@pytest.mark.parametrize(
    ("case", "expected_detail"),
    (
        ("no-protected-evidence", "protected terminal evidence is absent or ambiguous"),
        ("multiple-protected-evidence", "protected terminal evidence is absent or ambiguous"),
        ("changed-protected-digest", "protected terminal evidence is absent or changed"),
        ("changed-protected-epoch", "protected terminal evidence is absent or changed"),
        ("changed-protected-binding", "protected terminal evidence is absent or changed"),
        ("changed-inventory-sequence", "unused terminal inventory is absent or changed"),
        ("changed-inventory-digest", "unused terminal inventory is absent or changed"),
        ("nonempty-owned-inventory", "unused terminal inventory still owns the intent"),
        ("retained-launch-envelope", "unused terminal still has local physical ownership"),
        ("physical-binding", "unused terminal still has local physical ownership"),
        ("matching-owned-slurm-work", "unused terminal still has local physical ownership"),
    ),
)
async def test_protected_terminal_matrix_quarantines_ambiguous_or_changed_release(
    tmp_path: Path,
    case: str,
    expected_detail: str,
) -> None:
    launch = launch_context_fixture()
    work: object | None = launch.binding
    if case in {"nonempty-owned-inventory", "physical-binding", "matching-owned-slurm-work"}:
        work = permit_fixture(launch.binding)
    executor, journal, manager, admission, slurm, _launch = executor_fixture(
        tmp_path,
        work=work,
    )
    protected_digest = "4" * 64
    await executor.tick()
    if case in {"nonempty-owned-inventory", "physical-binding", "matching-owned-slurm-work"}:
        retained_job = slurm.jobs[0]
        if case in {"physical-binding", "matching-owned-slurm-work"}:
            slurm.jobs.clear()
        await executor.tick()
        if case == "matching-owned-slurm-work":
            slurm.jobs.append(retained_job)
    else:
        await executor.tick()
    inventory = manager.inventories[-1]
    inventory_sequence = inventory.inventory_sequence
    inventory_digest = canonical_executable_digest(inventory)
    if case == "retained-launch-envelope":
        rendered = executor.render_launch(launch.binding)
        executor._remember_launch(
            rendered,
            bootstrap_registration_epoch=1,
            event="slurm-submit-confirmed",
        )
    if case == "changed-inventory-sequence":
        inventory_sequence += 1
    if case == "changed-inventory-digest":
        inventory_digest = "5" * 64

    observation = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        claim_high_water=0,
        prepared_revocation=_prepared_revocation_receipt(
            launch.binding,
            digest=protected_digest,
        ),
    )
    release_digest = protected_digest
    release_epoch = 2
    if case == "no-protected-evidence":
        observation = ProtectedIntentObservationV2(
            binding=launch.binding,
            bootstrap_registration_epoch=1,
            claim_high_water=0,
        )
    elif case == "multiple-protected-evidence":
        observation = ProtectedIntentObservationV2.model_construct(
            binding=launch.binding,
            bootstrap_registration_epoch=1,
            protected_registration_epoch=2,
            claim_high_water=0,
            release=_protected_release_receipt(launch.binding, digest=protected_digest),
            prepared_revocation=_prepared_revocation_receipt(
                launch.binding,
                digest=protected_digest,
            ),
            executable=True,
        )
    elif case == "changed-protected-digest":
        release_digest = "6" * 64
    elif case == "changed-protected-epoch":
        release_epoch = 3
    elif case == "changed-protected-binding":
        changed_binding = launch.binding.model_copy(update={"intent_id": UUID(int=999)})
        observation = ProtectedIntentObservationV2.model_construct(
            binding=launch.binding,
            bootstrap_registration_epoch=1,
            protected_registration_epoch=2,
            claim_high_water=0,
            prepared_revocation=_prepared_revocation_receipt(
                changed_binding,
                digest=protected_digest,
            ),
            executable=True,
        )
    admission.observations[launch.binding.intent_id] = observation
    manager.work = _release_work(
        launch.binding,
        command_sequence=2,
        inventory_sequence=inventory_sequence,
        terminal_kind="unused",
        terminal_identity="unused-101",
        terminal_evidence_sha256=inventory_digest,
        protected_release_sha256=release_digest,
        protected_registration_epoch=release_epoch,
    )

    result = await executor.tick()

    assert result.status == "quarantined"
    assert result.detail == expected_detail
    assert manager.releases == []
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
