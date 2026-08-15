from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom_capacity_agent import runtime as runtime_module
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.executable_release_reporter import (
    ExecutableProtectedReleaseReporterRuntime,
)
from loom_capacity_agent.runtime import CapacityAgentRuntime, load_database_url
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.unit.test_capacity_agent_admission_contracts import publishable_release_fixture


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


class _Publisher:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.snapshots: list[Any] = []

    async def publish(self, snapshot):
        self.snapshots.append(snapshot)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("unavailable")
        return object()


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
