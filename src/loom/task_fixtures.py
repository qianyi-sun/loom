"""Closed, untrusted service fixtures prepared with the trial's task images."""
from __future__ import annotations

import math
import re
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1, SidecarContainerV1
    from loom.models.task import EnvironmentConfig, TaskConfig, TaskSidecarConfig

_NAME = re.compile(r"[a-z][a-z0-9-]{0,54}\Z")
_HOSTNAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\Z")
_NUMERIC_HOSTNAME = re.compile(r"(?:[0-9]+|0x[0-9a-f]+)(?:\.(?:[0-9]+|0x[0-9a-f]+))*\Z")
_RESERVED = {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback",
             "execution", "runtime-materializer", "agent", "verifier", "task-sandbox", "verifier-sandbox"}


def validate_fixture_hostname(value: str | None) -> None:
    if (value is None or len(value) > 253 or not _HOSTNAME.fullmatch(value)
            or value in _RESERVED or value.endswith(".localhost")
            or _NUMERIC_HOSTNAME.fullmatch(value)):
        raise ValueError("fixture requires a non-reserved DNS hostname")


def validate_fixture_component(role: str, component: str | None) -> None:
    if component is None or not component.startswith("sidecar:"):
        raise ValueError("fixture requires its prepared sidecar component")
    name = component.removeprefix("sidecar:")
    if not _NAME.fullmatch(name) or name in _RESERVED or role != "fixture-" + name:
        raise ValueError("fixture role must match its prepared component")


def _context(value: PurePosixPath | None) -> PurePosixPath:
    if value is None or value.is_absolute() or not value.parts or ".." in value.parts:
        raise ValueError("fixture requires a dedicated confined build context")
    return value


def validate_fixture_config(value: TaskSidecarConfig) -> None:
    if not value.fixture:
        if value.ports or any(item is not None for item in (value.cpus, value.memory_mb, value.storage_mb)):
            raise ValueError("fixture resources and ports require explicit fixture isolation")
        return
    validate_fixture_component("fixture-" + value.name, "sidecar:" + value.name)
    validate_fixture_hostname(value.hostname)
    if (not isinstance(value.command, list) or not value.command or len(value.command) > 128
            or any(not item or "\x00" in item or len(item.encode()) > 4096 for item in value.command)
            or sum(len(item.encode()) for item in value.command) > 32768):
        raise ValueError("fixture requires bounded command argv")
    if value.depends_on or value.environment:
        raise ValueError("fixture dependencies and environment overrides are unsupported")
    if (not 1 <= len(value.ports) <= 16 or len(set(value.ports)) != len(value.ports)
            or any(not 1 <= port <= 65535 for port in value.ports)):
        raise ValueError("fixture requires distinct valid TCP ports")
    if any(item is None for item in (value.cpus, value.memory_mb, value.storage_mb)):
        raise ValueError("fixture requires explicit CPU, memory and storage limits")
    if value.cpus is not None and round(value.cpus * 1000) < 1:
        raise ValueError("fixture CPU must request at least one millicore")
    health = value.healthcheck
    if health is None or not health.command.strip() or "\x00" in health.command or len(health.command.encode()) > 4096:
        raise ValueError("fixture requires a bounded healthcheck command")
    timings = (health.start_period_sec, health.interval_sec, health.timeout_sec)
    if (any(not math.isfinite(item) or not float(item).is_integer() for item in timings)
            or health.start_period_sec > 300 or health.interval_sec > 60 or health.timeout_sec > 30
            or not 1 <= health.retries <= 300
            or health.start_period_sec + max(health.interval_sec, health.timeout_sec) * health.retries > 300):
        raise ValueError("fixture healthcheck must use bounded whole-second timings")
    if value.dockerfile is not None:
        context = _context(value.docker_build_context)
        if (value.docker_image is not None or value.dockerfile.is_absolute()
                or ".." in value.dockerfile.parts or not value.dockerfile.is_relative_to(context)):
            raise ValueError("fixture Dockerfile must belong to its dedicated build context")
    elif (value.docker_build_context is not None or value.docker_image is None
          or not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", value.docker_image)):
        raise ValueError("fixture requires build inputs or its prepared immutable image")


def validate_fixture_contexts(environment: EnvironmentConfig) -> None:
    fixtures = [sidecar for sidecar in environment.sidecars if sidecar.fixture]
    if not fixtures:
        return
    if len(fixtures) != 1 or len(environment.sidecars) != 1:
        raise ValueError("only one self-contained fixture is supported")
    fixture, = fixtures
    if environment.dockerfile is not None:
        primary = _context(environment.docker_build_context)
        source = _context(fixture.docker_build_context)
        if primary.is_relative_to(source) or source.is_relative_to(primary):
            raise ValueError("fixture and primary build contexts must be disjoint")


def fixture_sidecars(task: TaskConfig) -> tuple[SidecarContainerV1, ...]:
    """Compile only resolved, explicitly isolated fixtures; never shared sidecars."""
    from loom.execution_runtime_contract import ContainerResourcesV1, ProbeV1, SidecarContainerV1

    result = []
    for fixture in task.environment.sidecars:
        if not fixture.fixture or fixture.dockerfile is not None or fixture.docker_image is None:
            raise ValueError("fixture requires its prepared immutable component")
        assert fixture.cpus is not None and fixture.memory_mb is not None and fixture.storage_mb is not None
        health = fixture.healthcheck
        assert health is not None
        probe = ProbeV1(kind="exec", argv=("/bin/sh", "-c", health.command),
            initial_delay_seconds=int(health.start_period_sec), timeout_seconds=int(health.timeout_sec),
            period_seconds=int(health.interval_sec), failure_threshold=health.retries)
        result.append(SidecarContainerV1(
            role_name="fixture-" + fixture.name, image_ref=fixture.docker_image,
            argv=tuple(fixture.command or ()), task_fixture=True,
            task_image_component="sidecar:" + fixture.name, hostname=fixture.hostname,
            resources=ContainerResourcesV1(cpu_millis=round(fixture.cpus * 1000),
                memory_mib=fixture.memory_mb, ephemeral_storage_mib=fixture.storage_mb),
            startup_probe=probe, readiness_probe=probe.model_copy(update={"initial_delay_seconds": 0}),
        ))
    return tuple(result)


def validate_prepared_fixtures(
    plan: ExecutionRuntimePlanV1, *, frozen_task: TaskConfig, prepared_task: TaskConfig,
) -> None:
    """Bind the entire fixture contract to the trial's locked, frozen grant."""
    declared = [sidecar for sidecar in frozen_task.environment.sidecars if sidecar.fixture]
    actual = tuple(sidecar for sidecar in plan.sidecars if sidecar.task_fixture)
    if any(sidecar.dockerfile is None for sidecar in declared):
        raise ValueError("prepared fixture requires a built component in the frozen task")
    expected = fixture_sidecars(prepared_task) if declared else ()
    if actual != expected:
        raise ValueError("runtime plan does not match the trial's prepared fixture")
