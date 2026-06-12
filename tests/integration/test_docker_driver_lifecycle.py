"""Live DockerDriver tests. Skipped automatically if Docker daemon unreachable."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.errors import DriverAlreadyStartedError, DriverNotStartedError

pytestmark = pytest.mark.docker


@pytest.fixture
def docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture
async def docker_driver(docker_available: bool) -> AsyncGenerator[object, None]:
    if not docker_available:
        pytest.skip("Docker daemon not available")
    from loom.driver.docker import DockerDriver
    d = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    try:
        yield d
    finally:
        await d.stop(delete=True)


async def test_capabilities(docker_driver):  # type: ignore[no-untyped-def]
    assert docker_driver.os == "linux"
    assert "public" in docker_driver.capabilities.network_policies
    assert docker_driver.capabilities.dynamic_network_policy is True
    assert docker_driver.capabilities.mounted_fs is True


async def test_lifecycle_start_stop(docker_driver):  # type: ignore[no-untyped-def]
    await docker_driver.start(options=StartOptions())
    # exec lands in Task 9; only validate lifecycle here.
    assert docker_driver._state == "running"
    await docker_driver.stop(delete=True)
    assert docker_driver._state == "stopped"


async def test_stop_before_start_safe(docker_driver):  # type: ignore[no-untyped-def]
    await docker_driver.stop(delete=True)
    # Spec §2.2: stop() before start() must not lock out future start().
    await docker_driver.start(options=StartOptions())
    assert docker_driver._state == "running"


async def test_double_start_raises(docker_driver):  # type: ignore[no-untyped-def]
    await docker_driver.start(options=StartOptions())
    with pytest.raises(DriverAlreadyStartedError):
        await docker_driver.start(options=StartOptions())


async def test_exec_before_start_raises(docker_driver):  # type: ignore[no-untyped-def]
    with pytest.raises(DriverNotStartedError):
        await docker_driver.exec("echo hi")
