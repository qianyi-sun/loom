from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom_capacity_agent import runtime as runtime_module
from loom_capacity_agent.admission import (
    AbandonedAdmissionPlanV1,
    NeverConvergedAdmissionPlanV1,
    ProtectedExecutableBootstrapRegistrationV2,
)
from loom_capacity_agent.admission_convergence import (
    ProtectedAdmissionPlanCleanupWork,
    ProtectedAdmissionPlanWork,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.executable_bootstrap import ProtectedExecutableBootstrapWork
from loom_capacity_agent.executable_release_reporter import (
    ExecutableProtectedReleaseReporterRuntime,
)
from loom_capacity_agent.runtime import CapacityAgentRuntime, load_database_url
from loom_capacity_guard.contracts import canonical_digest as guard_canonical_digest
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableAdmissionAcknowledgementV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
    canonical_executable_digest,
)
from tests.unit.test_capacity_agent_admission_contracts import publishable_release_fixture
from tests.unit.test_capacity_agent_admission_convergence import _proposal as _admission_proposal


def _configuration() -> ReporterConfigurationV1:
    registration = AgentRegistrationV1(
        environment_id="dev-alice",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        agent_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        candidate_digest="a" * 64,
        deployment_generation=7,
        configuration_generation=11,
    )
    return ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
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
    )


def _observation(configuration: ReporterConfigurationV1, sequence: int):
    return GuardLifecycleDemandObservationV2(
        **{field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields},
        sequence=sequence,
        source_observed_at=datetime(2026, 8, 11, tzinfo=UTC),
        attempts=(),
    )


def _release_publication(configuration: ReporterConfigurationV1):  # type: ignore[no-untyped-def]
    publication = publishable_release_fixture()
    candidate = publication.release.binding.candidate.model_copy(
        update={
            "identity": configuration.candidate_digest,
            "publication_sha256": configuration.candidate_digest,
        }
    )
    binding = publication.release.binding.model_copy(
        update={
            "subject_id": configuration.subject_id,
            "subject_incarnation": configuration.subject_incarnation,
            "deployment_generation": configuration.deployment_generation,
            "candidate": candidate,
        }
    )
    release = publication.release.model_copy(
        update={
            "binding": binding,
            "reporter_incarnation": configuration.reporter_incarnation,
        }
    )
    return publication.model_copy(
        update={
            "release": release,
            "publication_digest": canonical_executable_digest(release),
        }
    )


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def begin(self):
        return self


class _Factory:
    def __call__(self):
        return _Session()


class _CommitFailingTransaction:
    def __init__(self, factory: _CommitFailingFactory) -> None:
        self._factory = factory

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._factory.fail_commit_exit:
            raise RuntimeError("admission transaction commit failed")


class _CommitFailingSession:
    def __init__(self, factory: _CommitFailingFactory) -> None:
        self._factory = factory

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def begin(self) -> _CommitFailingTransaction:
        return _CommitFailingTransaction(self._factory)


class _CommitFailingFactory:
    def __init__(self) -> None:
        self.fail_commit_exit = False

    def __call__(self) -> _CommitFailingSession:
        return _CommitFailingSession(self)


class _Publisher:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.snapshots: list[Any] = []
        self.bootstrap_work: list[ExecutableBootstrapProposalV2] = []
        self.bootstrap_acknowledgements: list[tuple[object, object]] = []
        self.fail_bootstrap_once = False
        self.admission_work: object | None = None
        self.admission_acknowledgements: list[tuple[object, object]] = []
        self.fail_admission_once = False
        self.admission_closure_acknowledgements: list[tuple[object, object]] = []
        self.fail_admission_closure_once = False
        self.admission_fetches = 0

    async def publish(self, snapshot):
        self.snapshots.append(snapshot)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("unavailable")
        return object()

    async def next_executable_bootstrap(self):
        return self.bootstrap_work.pop(0) if self.bootstrap_work else None

    async def publish_executable_bootstrap_acknowledgement(
        self, acknowledgement, *, idempotency_key
    ):
        self.bootstrap_acknowledgements.append((acknowledgement, idempotency_key))
        if self.fail_bootstrap_once:
            self.fail_bootstrap_once = False
            raise RuntimeError("bootstrap unavailable")
        return object()

    async def next_executable_admission_plan(self):
        self.admission_fetches += 1
        return self.admission_work

    async def publish_executable_admission_acknowledgement(
        self, acknowledgement, *, idempotency_key
    ):
        self.admission_acknowledgements.append((acknowledgement, idempotency_key))
        if self.fail_admission_once:
            self.fail_admission_once = False
            raise RuntimeError("admission unavailable")
        return object()

    async def publish_executable_admission_closure_acknowledgement(
        self, acknowledgement, *, idempotency_key
    ):
        self.admission_closure_acknowledgements.append(
            (acknowledgement, idempotency_key)
        )
        if self.fail_admission_closure_once:
            self.fail_admission_closure_once = False
            raise RuntimeError("admission cleanup unavailable")
        return object()


def _bootstrap_work(
    configuration: ReporterConfigurationV1,
) -> tuple[ExecutableBootstrapProposalV2, ProtectedExecutableBootstrapWork]:
    execution = ExecutionFenceV2(
        authority_incarnation=uuid4(),
        writer_epoch=2,
        configuration_epoch=3,
        execution_epoch=4,
        execution_manifest_sha256="c" * 64,
        execution_state="active",
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        trusted_fleet_release_sha256="d" * 64,
        allocation_epoch=5,
    )
    binding = ExecutableIntentBindingV2(
        execution=execution,
        tranche_id=uuid4(),
        intent_id=uuid4(),
        shape_instance_id="shape-1",
        subject_id=configuration.subject_id,
        subject_incarnation=configuration.subject_incarnation,
        account_id="owner-1",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="a" * 64,
            publication_sha256="b" * 64,
        ),
        candidate_generation=6,
        deployment_generation=configuration.deployment_generation,
        pool_id="oldlab",
        pool_generation=8,
        executor_id="oldlab-executor",
        executor_incarnation=uuid4(),
        shape_id="one-slot",
        profile_id="profile-1",
        profile_generation=1,
        profile_digest="e" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=1_000,
            memory_bytes=1_073_741_824,
        ),
        node_ids=("node-1",),
    )
    proposal = ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=1,
        proposal_epoch=1,
        bootstrap_sha256="f" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    registration = ProtectedExecutableBootstrapRegistrationV2(
        subject_id=configuration.subject_id,
        subject_incarnation=configuration.subject_incarnation,
        intent_id=binding.intent_id,
        proposal_epoch=1,
        proposal_digest="1" * 64,
        bootstrap_registration_epoch=1,
        bootstrap_sha256=proposal.bootstrap_sha256,
        protected_admission_sha256="3" * 64,
        protected_high_water=1,
    )
    acknowledgement = ExecutableBootstrapAcknowledgementV2(
        binding=binding,
        proposal_epoch=1,
        proposal_digest="1" * 64,
        reporter_incarnation=configuration.reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="2" * 64,
        protected_admission_sha256="3" * 64,
    )
    return proposal, ProtectedExecutableBootstrapWork(
        registration=registration,
        acknowledgement=acknowledgement,
        idempotency_key=uuid4(),
    )


def _admission_work(
    configuration: ReporterConfigurationV1,
) -> tuple[ExecutableAdmissionPlanProposalV2, ProtectedAdmissionPlanWork]:
    proposal = _admission_proposal(configuration, allowance_count=0)
    acknowledgement = ExecutableAdmissionAcknowledgementV2(
        execution=proposal.shapes[0].binding.execution,
        tranche_id=proposal.shapes[0].binding.tranche_id,
        proposal_id=proposal.proposal_id,
        plan_id=proposal.plan_id,
        admission_incarnation=proposal.admission_incarnation,
        subject_id=configuration.subject_id,
        subject_incarnation=configuration.subject_incarnation,
        pool_id="oldlab",
        reporter_incarnation=configuration.reporter_incarnation,
        protected_admission_sha256=configuration.protected_admission_sha256,
        proposal_digest=canonical_executable_digest(proposal),
        prepared_plan_digest="9" * 64,
        assignment_count=0,
    )
    return proposal, ProtectedAdmissionPlanWork(
        acknowledgement=acknowledgement,
        idempotency_key=uuid4(),
    )


def _admission_cleanup_work(
    configuration: ReporterConfigurationV1,
    closure: ExecutableAdmissionPlanClosureV2,
) -> ProtectedAdmissionPlanCleanupWork:
    proposal = closure.proposal
    anchor = proposal.shapes[0].binding
    abandonment = AbandonedAdmissionPlanV1(
        **{field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields},
        closure_id=closure.closure_id,
        proposal_id=proposal.proposal_id,
        proposal_digest=canonical_executable_digest(proposal),
        plan_id=proposal.plan_id,
        admission_incarnation=proposal.admission_incarnation,
        manager_authority_incarnation=anchor.execution.authority_incarnation,
        manager_writer_epoch=anchor.execution.writer_epoch,
        manager_allocation_epoch=anchor.execution.allocation_epoch,
        manager_input_digest=proposal.manager_input_digest,
        manager_allocation_digest=proposal.manager_allocation_digest,
        pool_id="oldlab",
        close_reason=closure.close_reason,
    )
    return ProtectedAdmissionPlanCleanupWork(
        closure=closure,
        disposition=abandonment,
    )


def _never_converged_cleanup_work(
    configuration: ReporterConfigurationV1,
    closure: ExecutableAdmissionPlanClosureV2,
) -> ProtectedAdmissionPlanCleanupWork:
    registration = AgentRegistrationV1.model_validate(
        {
            field: getattr(configuration, field)
            for field in AgentRegistrationV1.model_fields
        }
    )
    tombstone = NeverConvergedAdmissionPlanV1(
        **registration.model_dump(mode="python", exclude_none=False),
        registration_digest=guard_canonical_digest(registration),
        closure=closure,
        closure_digest=canonical_executable_digest(closure),
        proposal_digest=canonical_executable_digest(closure.proposal),
    )
    return ProtectedAdmissionPlanCleanupWork(
        closure=closure,
        disposition=tombstone,
    )


async def _authorize_admission(*_args: object, **kwargs: object) -> ProtectedAdmissionPlanWork:
    return kwargs["work"]  # type: ignore[return-value]


class _LoopRuntime:
    def __init__(self, *, ready: bool = False) -> None:
        self.ready = ready
        self.started = asyncio.Event()
        self.cancelled = False
        self.poll_intervals: list[float] = []

    async def run_forever(self, *, poll_interval_seconds: float) -> None:
        self.poll_intervals.append(poll_interval_seconds)
        self.started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class _ExecutablePublisher:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.publications: list[object] = []

    async def publish_executable_protected_release(self, publication, *, idempotency_key):
        self.publications.append((publication, idempotency_key))
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("release unavailable")

        class _Receipt:
            intent_id = publication.release.binding.intent_id
            protected_release_sha256 = publication.release.protected_release_sha256
            receipt_digest = "7" * 64
            replayed = False
            executable = True

        return _Receipt()


@pytest.mark.asyncio
async def test_restart_republishes_durable_high_water_before_new_capture() -> None:
    configuration = _configuration()
    recovered = _observation(configuration, 4)
    captured: list[int] = []

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 4

    async def recover(*_args: object, **_kwargs: object):
        return recovered

    async def capture(*_args: object, **kwargs: object):
        captured.append(int(kwargs["expected_high_water"]))
        return _observation(configuration, 5)

    publisher = _Publisher()
    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
    )
    await runtime.initialize()
    assert runtime.ready is False
    await runtime.run_once()
    assert runtime.ready is True
    assert captured == []
    assert publisher.snapshots[0].sequence == 4
    await runtime.run_once()
    assert captured == [4]
    assert publisher.snapshots[1].sequence == 5


@pytest.mark.asyncio
async def test_failed_publish_retries_same_snapshot_without_recapture() -> None:
    configuration = _configuration()
    captures = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def capture(*_args: object, **_kwargs: object):
        nonlocal captures
        captures += 1
        return _observation(configuration, 1)

    publisher = _Publisher(fail_once=True)
    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
    )
    await runtime.initialize()
    with pytest.raises(RuntimeError, match="unavailable"):
        await runtime.run_once()
    assert runtime.ready is False
    await runtime.run_once()
    assert runtime.ready is True
    assert captures == 1
    assert [item.sequence for item in publisher.snapshots] == [1, 1]


@pytest.mark.asyncio
async def test_bootstrap_is_protected_after_demand_and_before_acknowledgement() -> None:
    configuration = _configuration().model_copy(update={"protected_admission_sha256": "3" * 64})
    proposal, protected = _bootstrap_work(configuration)
    events: list[str] = []

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def capture(*_args: object, **_kwargs: object):
        return _observation(configuration, 1)

    class Publisher(_Publisher):
        async def publish(self, snapshot):  # type: ignore[no-untyped-def]
            events.append("demand")
            return await super().publish(snapshot)

        async def next_executable_bootstrap(self):  # type: ignore[no-untyped-def]
            events.append("fetch")
            return await super().next_executable_bootstrap()

        async def publish_executable_bootstrap_acknowledgement(
            self, acknowledgement, *, idempotency_key
        ):  # type: ignore[no-untyped-def]
            events.append("acknowledge")
            return await super().publish_executable_bootstrap_acknowledgement(
                acknowledgement,
                idempotency_key=idempotency_key,
            )

    async def protect(*_args: object, **_kwargs: object):
        events.append("protect")
        assert _kwargs["proposal"] == proposal
        return protected

    publisher = Publisher()
    publisher.bootstrap_work.append(proposal)
    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
        protect_bootstrap=protect,
    )
    await runtime.initialize()
    await runtime.run_once()

    assert events == ["demand", "fetch", "protect", "acknowledge"]
    assert publisher.bootstrap_acknowledgements == [
        (protected.acknowledgement, protected.idempotency_key)
    ]
    assert runtime.ready is True


@pytest.mark.asyncio
async def test_failed_bootstrap_ack_retries_same_durable_work_without_recapture() -> None:
    configuration = _configuration().model_copy(update={"protected_admission_sha256": "3" * 64})
    proposal, protected = _bootstrap_work(configuration)
    captures = 0
    protections = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def capture(*_args: object, **_kwargs: object):
        nonlocal captures
        captures += 1
        return _observation(configuration, 1)

    async def protect(*_args: object, **_kwargs: object):
        nonlocal protections
        protections += 1
        return protected

    publisher = _Publisher()
    publisher.bootstrap_work.append(proposal)
    publisher.fail_bootstrap_once = True
    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
        protect_bootstrap=protect,
    )
    await runtime.initialize()
    with pytest.raises(RuntimeError, match="bootstrap unavailable"):
        await runtime.run_once()
    await runtime.run_once()

    assert captures == 1
    assert protections == 1
    assert len(publisher.snapshots) == 1
    assert publisher.bootstrap_acknowledgements == [
        (protected.acknowledgement, protected.idempotency_key),
        (protected.acknowledgement, protected.idempotency_key),
    ]
    assert runtime.ready is True


@pytest.mark.asyncio
async def test_restart_recovers_admission_work_across_each_local_manager_crash_window() -> None:
    configuration = _configuration().model_copy(update={"protected_admission_sha256": "b" * 64})
    proposal, protected = _admission_work(configuration)
    recovered = _observation(configuration, 1)
    publisher = _Publisher()
    publisher.admission_work = proposal
    converged: list[tuple[object, object]] = []
    attempts = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 1

    async def recover(*_args: object, **_kwargs: object):
        return recovered

    async def capture(*_args: object, **_kwargs: object):
        raise AssertionError("durable recovery must publish before any new capture")

    async def converge(*_args: object, **kwargs: object):
        nonlocal attempts
        attempts += 1
        converged.append((kwargs["proposal"], kwargs["observation"]))
        if attempts == 1:
            raise RuntimeError("crash before local admission commit")
        return protected

    def runtime() -> CapacityAgentRuntime:
        return CapacityAgentRuntime(
            configuration=configuration,
            session_factory=_Factory(),  # type: ignore[arg-type]
            publisher=publisher,
            max_attempts=100,
            capture=capture,
            recover=recover,
            read_high_water=high_water,
            converge_admission=converge,
            authorize_admission_publication=_authorize_admission,
        )

    before_commit = runtime()
    await before_commit.initialize()
    with pytest.raises(RuntimeError, match="before local admission commit"):
        await before_commit.run_once()
    assert before_commit.ready is False

    after_commit = runtime()
    await after_commit.initialize()
    publisher.fail_admission_once = True
    with pytest.raises(RuntimeError, match="admission unavailable"):
        await after_commit.run_once()
    assert after_commit.ready is False

    replay = runtime()
    await replay.initialize()
    await replay.run_once()

    assert replay.ready is True
    assert publisher.admission_fetches == 3
    assert converged == [(proposal, recovered)] * 3
    assert publisher.admission_acknowledgements == [
        (protected.acknowledgement, protected.idempotency_key),
        (protected.acknowledgement, protected.idempotency_key),
    ]


@pytest.mark.asyncio
async def test_restart_cleans_closed_admission_from_recovered_observation_without_reconverging() -> (
    None
):
    """Durable manager closure must retire local work from its pre-convergence view."""

    configuration = _configuration().model_copy(update={"protected_admission_sha256": "b" * 64})
    proposal, _protected = _admission_work(configuration)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="expired",
    )
    cleanup = _admission_cleanup_work(configuration, closure)
    recovered = _observation(configuration, 1)
    publisher = _Publisher()
    publisher.admission_work = closure
    cleaned: list[tuple[object, object]] = []

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 1

    async def recover(*_args: object, **_kwargs: object):
        return recovered

    async def capture(*_args: object, **_kwargs: object):
        raise AssertionError("restart must reuse the durable pre-convergence observation")

    async def converge(*_args: object, **_kwargs: object):
        raise AssertionError("closed work must never enter normal convergence")

    async def abandon(*_args: object, **kwargs: object):
        cleaned.append((kwargs["closure"], kwargs["observation"]))
        return cleanup

    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
        converge_admission=converge,
        authorize_admission_publication=_authorize_admission,
        abandon_admission=abandon,
    )
    await runtime.initialize()
    await runtime.run_once()

    assert cleaned == [(closure, recovered)]
    assert publisher.admission_acknowledgements == []
    assert publisher.admission_closure_acknowledgements == [
        (cleanup.acknowledgement, cleanup.idempotency_key)
    ]
    assert runtime.ready is True


@pytest.mark.asyncio
async def test_failed_admission_cleanup_receipt_stays_pending_without_recapture() -> None:
    """A committed cleanup must retry its exact manager receipt before any new work."""

    configuration = _configuration().model_copy(update={"protected_admission_sha256": "b" * 64})
    proposal, _protected = _admission_work(configuration)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="manager-closed",
    )
    cleanup = _admission_cleanup_work(configuration, closure)
    observation = _observation(configuration, 1)
    publisher = _Publisher()
    publisher.admission_work = closure
    publisher.fail_admission_closure_once = True
    captures = 0
    cleanups = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def capture(*_args: object, **_kwargs: object):
        nonlocal captures
        captures += 1
        return observation

    async def abandon(*_args: object, **_kwargs: object):
        nonlocal cleanups
        cleanups += 1
        return cleanup

    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
        abandon_admission=abandon,
    )
    await runtime.initialize()

    with pytest.raises(RuntimeError, match="cleanup unavailable"):
        await runtime.run_once()
    assert runtime.ready is False
    assert runtime._pending_admission_cleanup == cleanup

    publisher.admission_work = None
    await runtime.run_once()

    assert runtime.ready is True
    assert captures == 0
    assert cleanups == 1
    assert publisher.admission_fetches == 1
    assert publisher.admission_closure_acknowledgements == [
        (cleanup.acknowledgement, cleanup.idempotency_key),
        (cleanup.acknowledgement, cleanup.idempotency_key),
    ]


@pytest.mark.asyncio
async def test_restart_replays_durable_cleanup_after_receipt_publish_crash() -> None:
    """Losing in-memory pending state must replay cleanup from the recovered observation."""

    configuration = _configuration().model_copy(update={"protected_admission_sha256": "b" * 64})
    proposal, _protected = _admission_work(configuration)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="expired",
    )
    cleanup = _admission_cleanup_work(configuration, closure)
    recovered = _observation(configuration, 1)
    publisher = _Publisher()
    publisher.admission_work = closure
    publisher.fail_admission_closure_once = True
    cleanups = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 1

    async def recover(*_args: object, **_kwargs: object):
        return recovered

    async def capture(*_args: object, **_kwargs: object):
        raise AssertionError("receipt recovery must precede a new observation")

    async def abandon(*_args: object, **_kwargs: object):
        nonlocal cleanups
        cleanups += 1
        return cleanup

    def new_runtime() -> CapacityAgentRuntime:
        return CapacityAgentRuntime(
            configuration=configuration,
            session_factory=_Factory(),  # type: ignore[arg-type]
            publisher=publisher,
            max_attempts=100,
            capture=capture,
            recover=recover,
            read_high_water=high_water,
            abandon_admission=abandon,
        )

    crashed = new_runtime()
    await crashed.initialize()
    with pytest.raises(RuntimeError, match="cleanup unavailable"):
        await crashed.run_once()

    restarted = new_runtime()
    await restarted.initialize()
    await restarted.run_once()

    assert restarted.ready is True
    assert cleanups == 2
    assert publisher.admission_fetches == 2
    assert publisher.admission_closure_acknowledgements == [
        (cleanup.acknowledgement, cleanup.idempotency_key),
        (cleanup.acknowledgement, cleanup.idempotency_key),
    ]


@pytest.mark.asyncio
async def test_offline_closure_restart_replays_tombstone_without_demand_recapture() -> None:
    """A never-prepared expired plan is closed before any new demand observation."""

    configuration = _configuration().model_copy(
        update={"protected_admission_sha256": "b" * 64}
    )
    proposal, _protected = _admission_work(configuration)
    closure = ExecutableAdmissionPlanClosureV2(
        closure_id=uuid4(),
        proposal=proposal,
        close_reason="expired",
    )
    cleanup = _never_converged_cleanup_work(configuration, closure)
    publisher = _Publisher()
    publisher.admission_work = closure
    publisher.fail_admission_closure_once = True
    cleanups = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def recover(*_args: object, **_kwargs: object):
        raise AssertionError("offline agent has no prior observation")

    async def capture(*_args: object, **_kwargs: object):
        raise AssertionError("closure retry must precede demand recapture")

    async def close(*_args: object, **kwargs: object):
        nonlocal cleanups
        cleanups += 1
        assert kwargs["observation"] is None
        return cleanup

    def new_runtime() -> CapacityAgentRuntime:
        return CapacityAgentRuntime(
            configuration=configuration,
            session_factory=_Factory(),  # type: ignore[arg-type]
            publisher=publisher,
            max_attempts=100,
            capture=capture,
            recover=recover,
            read_high_water=high_water,
            abandon_admission=close,
        )

    crashed = new_runtime()
    await crashed.initialize()
    with pytest.raises(RuntimeError, match="cleanup unavailable"):
        await crashed.run_once()

    restarted = new_runtime()
    await restarted.initialize()
    await restarted.run_once()

    assert cleanups == 2
    assert publisher.admission_fetches == 2
    assert publisher.snapshots == []
    assert publisher.admission_closure_acknowledgements == [
        (cleanup.acknowledgement, cleanup.idempotency_key),
        (cleanup.acknowledgement, cleanup.idempotency_key),
    ]


@pytest.mark.asyncio
async def test_admission_commit_failure_does_not_leave_uncommitted_work_publishable() -> None:
    configuration = _configuration().model_copy(update={"protected_admission_sha256": "b" * 64})
    proposal, protected = _admission_work(configuration)
    recovered = _observation(configuration, 1)
    captured = _observation(configuration, 2)
    factory = _CommitFailingFactory()
    publisher = _Publisher()
    publisher.admission_work = proposal
    converged: list[tuple[object, object]] = []

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 1

    async def recover(*_args: object, **_kwargs: object):
        return recovered

    async def capture(*_args: object, **_kwargs: object):
        return captured

    async def converge(*_args: object, **kwargs: object):
        converged.append((kwargs["proposal"], kwargs["observation"]))
        return protected

    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=factory,  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
        converge_admission=converge,
        authorize_admission_publication=_authorize_admission,
    )
    await runtime.initialize()
    factory.fail_commit_exit = True

    with pytest.raises(RuntimeError, match="transaction commit failed"):
        await runtime.run_once()

    assert runtime.ready is False
    assert runtime._pending_admission is None
    assert publisher.admission_acknowledgements == []

    factory.fail_commit_exit = False
    await runtime.run_once()

    assert publisher.admission_fetches == 2
    assert converged == [(proposal, recovered), (proposal, captured)]
    assert publisher.admission_acknowledgements == [
        (protected.acknowledgement, protected.idempotency_key)
    ]


@pytest.mark.asyncio
async def test_failed_admission_publication_replays_same_runtime_pending_work() -> None:
    configuration = _configuration().model_copy(update={"protected_admission_sha256": "b" * 64})
    proposal, protected = _admission_work(configuration)
    observation = _observation(configuration, 1)
    publisher = _Publisher()
    publisher.admission_work = proposal
    publisher.fail_admission_once = True
    converged: list[tuple[object, object]] = []
    captures = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def capture(*_args: object, **_kwargs: object):
        nonlocal captures
        captures += 1
        return observation

    async def converge(*_args: object, **kwargs: object):
        converged.append((kwargs["proposal"], kwargs["observation"]))
        return protected

    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
        converge_admission=converge,
        authorize_admission_publication=_authorize_admission,
    )
    await runtime.initialize()
    with pytest.raises(RuntimeError, match="admission unavailable"):
        await runtime.run_once()

    assert runtime.ready is False
    await runtime.run_once()

    assert runtime.ready is True
    assert captures == 1
    assert publisher.admission_fetches == 1
    assert converged == [(proposal, observation)]
    assert publisher.admission_acknowledgements == [
        (protected.acknowledgement, protected.idempotency_key),
        (protected.acknowledgement, protected.idempotency_key),
    ]


@pytest.mark.asyncio
async def test_failed_admission_publication_reauthorizes_before_retry() -> None:
    """A cached acknowledgement must not bypass current protected authority."""

    configuration = _configuration().model_copy(update={"protected_admission_sha256": "b" * 64})
    proposal, protected = _admission_work(configuration)
    observation = _observation(configuration, 1)
    publisher = _Publisher()
    publisher.admission_work = proposal
    publisher.fail_admission_once = True
    authorization_attempts = 0

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def capture(*_args: object, **_kwargs: object):
        return observation

    async def converge(*_args: object, **_kwargs: object):
        return protected

    async def authorize(*_args: object, **_kwargs: object):
        nonlocal authorization_attempts
        authorization_attempts += 1
        if authorization_attempts == 2:
            raise RuntimeError("protected publication authority is stale")
        return protected

    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
        converge_admission=converge,
        authorize_admission_publication=authorize,
    )
    await runtime.initialize()

    with pytest.raises(RuntimeError, match="admission unavailable"):
        await runtime.run_once()
    with pytest.raises(RuntimeError, match="publication authority is stale"):
        await runtime.run_once()

    assert authorization_attempts == 2
    assert publisher.admission_acknowledgements == [
        (protected.acknowledgement, protected.idempotency_key)
    ]
    assert runtime._pending_admission == protected
    assert runtime.ready is False


@pytest.mark.asyncio
async def test_superseded_configuration_observation_is_retired_before_capture() -> None:
    configuration = _configuration()
    previous = _observation(
        configuration.model_copy(update={"configuration_generation": 10}),
        4,
    )
    captured: list[int] = []

    async def high_water(*_args: object, **_kwargs: object) -> int:
        return 4

    async def recover(*_args: object, **_kwargs: object):
        return previous

    async def capture(*_args: object, **kwargs: object):
        captured.append(int(kwargs["expected_high_water"]))
        return _observation(configuration, 5)

    publisher = _Publisher()
    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        max_attempts=100,
        capture=capture,
        recover=recover,
        read_high_water=high_water,
    )
    await runtime.initialize()
    await runtime.run_once()
    assert captured == [4]
    assert publisher.snapshots[0].sequence == 5


def test_database_url_file_is_owner_only(tmp_path: Path) -> None:
    path = tmp_path / "db-url"
    path.write_text("postgresql+psycopg://agent:secret@postgres/loom_dev_alice")
    path.chmod(0o600)
    assert load_database_url(path).startswith("postgresql+psycopg://agent:")
    path.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        load_database_url(path)


def test_capacity_agent_engine_uses_serializable_isolation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    expected_engine = object()

    def create(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return expected_engine

    monkeypatch.setattr(runtime_module, "create_async_engine", create)
    factory = getattr(runtime_module, "create_capacity_agent_engine", None)

    assert callable(factory)
    assert factory("postgresql+psycopg://agent:secret@postgres/loom") is expected_engine
    assert captured == {
        "url": "postgresql+psycopg://agent:secret@postgres/loom",
        "kwargs": {"isolation_level": "SERIALIZABLE"},
    }


def test_service_runtime_is_ready_only_when_both_loops_are_ready() -> None:
    service = runtime_module.CapacityAgentServiceRuntime(
        demand_runtime=_LoopRuntime(ready=True),
        release_runtime=_LoopRuntime(ready=False),
    )
    assert service.ready is False

    service = runtime_module.CapacityAgentServiceRuntime(
        demand_runtime=_LoopRuntime(ready=True),
        release_runtime=_LoopRuntime(ready=True),
    )
    assert service.ready is True


@pytest.mark.asyncio
async def test_service_runtime_runs_both_loops_and_cancels_them_together() -> None:
    demand = _LoopRuntime()
    release = _LoopRuntime()
    service = runtime_module.CapacityAgentServiceRuntime(
        demand_runtime=demand,
        release_runtime=release,
    )

    task = asyncio.create_task(service.run_forever(poll_interval_seconds=0.25))
    await asyncio.wait_for(demand.started.wait(), timeout=1)
    await asyncio.wait_for(release.started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert demand.poll_intervals == [0.25]
    assert release.poll_intervals == [0.25]
    assert demand.cancelled is True
    assert release.cancelled is True


@pytest.mark.asyncio
async def test_service_runtime_retries_demand_initialization_without_blocking_release_progress() -> (
    None
):
    configuration = _configuration()
    release_publication = _release_publication(configuration)
    demand_publisher = _Publisher()
    release_publisher = _ExecutablePublisher()
    demand_init_calls = 0
    demand_started = asyncio.Event()
    release_progress = asyncio.Event()
    demand_progress = asyncio.Event()
    release_reads = 0

    async def demand_high_water(*_args: object, **_kwargs: object) -> int:
        nonlocal demand_init_calls
        demand_init_calls += 1
        demand_started.set()
        if demand_init_calls == 1:
            raise runtime_module.CapacityAgentStoreError("demand init unavailable")
        return 0

    async def demand_recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def demand_capture(*_args: object, **_kwargs: object):
        observation = _observation(configuration, 1)
        demand_progress.set()
        return observation

    async def release_read_next(*_args: object, **_kwargs: object):
        nonlocal release_reads
        release_reads += 1
        if release_reads == 1:
            return release_publication
        await release_progress.wait()
        return None

    async def release_ack(*_args: object, **_kwargs: object):
        release_progress.set()
        return object()

    demand_runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=demand_publisher,
        max_attempts=100,
        capture=demand_capture,
        recover=demand_recover,
        read_high_water=demand_high_water,
    )
    release_runtime = ExecutableProtectedReleaseReporterRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=release_publisher,
        read_next=release_read_next,
        acknowledge=release_ack,
    )
    service = runtime_module.CapacityAgentServiceRuntime(
        demand_runtime=demand_runtime,
        release_runtime=release_runtime,
    )

    task = asyncio.create_task(service.run_forever(poll_interval_seconds=0.01))
    await asyncio.wait_for(demand_started.wait(), timeout=1)
    await asyncio.wait_for(release_progress.wait(), timeout=1)
    await asyncio.wait_for(demand_progress.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert demand_init_calls >= 2
    assert len(release_publisher.publications) == 1
    assert len(demand_publisher.snapshots) == 1


@pytest.mark.asyncio
async def test_service_runtime_retries_release_iteration_without_blocking_demand_publication() -> (
    None
):
    configuration = _configuration()
    release_publication = _release_publication(configuration)
    release_publisher = _ExecutablePublisher(fail_once=True)
    demand_progress = asyncio.Event()
    release_attempts = asyncio.Event()
    release_reads = 0

    async def demand_high_water(*_args: object, **_kwargs: object) -> int:
        return 0

    async def demand_recover(*_args: object, **_kwargs: object):
        raise AssertionError("zero high-water must not recover")

    async def demand_capture(*_args: object, **_kwargs: object):
        return _observation(configuration, 1)

    class _DemandPublisher(_Publisher):
        async def publish(self, snapshot):
            result = await super().publish(snapshot)
            demand_progress.set()
            return result

    demand_publisher = _DemandPublisher()
    demand_runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=demand_publisher,
        max_attempts=100,
        capture=demand_capture,
        recover=demand_recover,
        read_high_water=demand_high_water,
    )

    async def release_read_next(*_args: object, **_kwargs: object):
        nonlocal release_reads
        release_reads += 1
        if release_reads <= 2:
            return release_publication
        await release_attempts.wait()
        return None

    async def release_ack(*_args: object, **_kwargs: object):
        release_attempts.set()
        return object()

    release_runtime = ExecutableProtectedReleaseReporterRuntime(
        configuration=configuration,
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=release_publisher,
        read_next=release_read_next,
        acknowledge=release_ack,
    )
    service = runtime_module.CapacityAgentServiceRuntime(
        demand_runtime=demand_runtime,
        release_runtime=release_runtime,
    )

    task = asyncio.create_task(service.run_forever(poll_interval_seconds=0.01))
    await asyncio.wait_for(demand_progress.wait(), timeout=1)
    await asyncio.wait_for(release_attempts.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(demand_publisher.snapshots) >= 1
    assert len(release_publisher.publications) >= 2


@pytest.mark.asyncio
async def test_main_async_cancels_both_loops_and_closes_shared_resources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configuration = _configuration()
    demand = _LoopRuntime()
    release = _LoopRuntime()

    class _Engine:
        disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    class _PublisherClient:
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    class _Server:
        entered = False
        exited = False

        async def __aenter__(self):
            self.entered = True
            return self

        async def __aexit__(self, *_exc: object) -> None:
            self.exited = True

    engine = _Engine()
    publisher = _PublisherClient()
    server = _Server()

    monkeypatch.setattr(runtime_module, "load_reporter_configuration", lambda _path: configuration)
    monkeypatch.setattr(
        runtime_module,
        "load_database_url",
        lambda _path: "postgresql+psycopg://agent:secret@postgres/loom",
    )
    monkeypatch.setattr(runtime_module, "create_capacity_agent_engine", lambda _url: engine)
    monkeypatch.setattr(runtime_module, "async_sessionmaker", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        runtime_module.DemandReporterClient,
        "from_files",
        classmethod(lambda _cls, _configuration, _connection: publisher),
    )
    monkeypatch.setattr(
        runtime_module,
        "CapacityAgentRuntime",
        lambda **_kwargs: demand,
    )
    original_service_runtime = runtime_module.CapacityAgentServiceRuntime
    monkeypatch.setattr(
        runtime_module,
        "ExecutableProtectedReleaseReporterRuntime",
        lambda **_kwargs: release,
    )

    def _service_runtime(*, demand_runtime: object, release_runtime: object):
        return original_service_runtime(
            demand_runtime=demand_runtime,
            release_runtime=release_runtime,
        )

    monkeypatch.setattr(runtime_module, "CapacityAgentServiceRuntime", _service_runtime)

    async def _start_server(*_args: object, **_kwargs: object):
        return server

    monkeypatch.setattr(runtime_module.asyncio, "start_server", _start_server)

    arguments = argparse.Namespace(
        configuration_file=tmp_path / "configuration.json",
        database_url_file=tmp_path / "database-url",
        manager_origin="https://capacity.internal",
        bearer_token_file=tmp_path / "bearer-token",
        ca_file=tmp_path / "ca.pem",
        certificate_file=tmp_path / "client.pem",
        private_key_file=tmp_path / "client.key",
        poll_interval_seconds=0.5,
        max_attempts=100,
        health_port=8081,
    )

    task = asyncio.create_task(runtime_module._main_async(arguments))
    await asyncio.wait_for(demand.started.wait(), timeout=1)
    await asyncio.wait_for(release.started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert demand.cancelled is True
    assert release.cancelled is True
    assert publisher.closed is True
    assert engine.disposed is True
    assert server.entered is True
    assert server.exited is True
