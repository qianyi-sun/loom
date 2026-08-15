from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom_capacity_agent import runtime as runtime_module
from loom_capacity_agent.admission import ProtectedExecutableBootstrapRegistrationV2
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.executable_bootstrap import ProtectedExecutableBootstrapWork
from loom_capacity_agent.runtime import CapacityAgentRuntime, load_database_url
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
)


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


class _Publisher:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.snapshots: list[Any] = []
        self.bootstrap_work: list[ExecutableBootstrapProposalV2] = []
        self.bootstrap_acknowledgements: list[tuple[object, object]] = []
        self.fail_bootstrap_once = False

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
    configuration = _configuration().model_copy(
        update={"protected_admission_sha256": "3" * 64}
    )
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
    configuration = _configuration().model_copy(
        update={"protected_admission_sha256": "3" * 64}
    )
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
