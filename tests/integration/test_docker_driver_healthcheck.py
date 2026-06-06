from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.errors import DriverError
from loom.models.healthcheck import HealthcheckSpec


@pytest.fixture
async def docker_driver() -> AsyncGenerator[object, None]:
    pytest.importorskip("docker")
    import docker
    try:
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not available")
    from loom.driver.docker import DockerDriver
    d = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    await d.start(options=StartOptions())
    try:
        yield d
    finally:
        await d.stop(delete=True)


async def test_healthcheck_none_is_noop(docker_driver):  # type: ignore[no-untyped-def]
    await docker_driver.run_healthcheck(None)


async def test_healthcheck_passes_immediately(docker_driver):  # type: ignore[no-untyped-def]
    hc = HealthcheckSpec(command="true", retries=1, interval_sec=0.1)
    await docker_driver.run_healthcheck(hc)


async def test_healthcheck_retries_then_fails(docker_driver):  # type: ignore[no-untyped-def]
    hc = HealthcheckSpec(command="false", retries=2, interval_sec=0.1, timeout_sec=1)
    with pytest.raises(DriverError, match="Healthcheck failed"):
        await docker_driver.run_healthcheck(hc)


async def test_healthcheck_passes_after_setup(docker_driver):  # type: ignore[no-untyped-def]
    """Marker file pre-created → healthcheck passes on the first probe."""
    await docker_driver.exec("touch /tmp/marker")
    hc = HealthcheckSpec(
        command="test -e /tmp/marker", retries=5, interval_sec=0.1, timeout_sec=1,
    )
    await docker_driver.run_healthcheck(hc)
