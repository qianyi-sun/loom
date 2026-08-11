from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.runtime import CapacityAgentRuntime, load_database_url


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
        **{
            field: getattr(configuration, field)
            for field in AgentRegistrationV1.model_fields
        },
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

    async def publish(self, snapshot):
        self.snapshots.append(snapshot)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("unavailable")
        return object()


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
