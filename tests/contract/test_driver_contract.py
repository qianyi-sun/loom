"""Driver contract — every Driver implementation must pass this suite.

Implementations register themselves via the `driver_impl` parametrization
below. Add new implementations by extending `_register_driver_impls`.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import PurePosixPath

import pytest

from loom.driver.base import Driver, StartOptions
from loom.errors import DriverAlreadyStartedError, DriverNotStartedError

DriverFactory = Callable[[], Awaitable[Driver]]


def _register_driver_impls() -> list[tuple[str, DriverFactory]]:
    """Returns (impl_name, factory) pairs.

    Each subsequent task adds an entry here when its concrete impl is ready.
    """
    impls: list[tuple[str, DriverFactory]] = []

    # FakeDriver — Task 5+.
    try:
        from loom.driver.fake import FakeDriver
    except ImportError:
        pass
    else:
        async def _make_fake() -> Driver:
            return FakeDriver()
        impls.append(("FakeDriver", _make_fake))

    # DockerDriver — Tasks 8+, gated on docker daemon availability.
    docker_available = False
    try:
        import docker

        from loom.driver.docker import DockerDriver
        try:
            docker.from_env().ping()
            docker_available = True
        except Exception:
            docker_available = False
    except ImportError:
        docker_available = False

    if docker_available:
        async def _make_docker() -> Driver:
            return DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
        impls.append(("DockerDriver", _make_docker))

    return impls


@pytest.fixture(params=_register_driver_impls(), ids=lambda p: p[0])
async def driver_impl(request) -> AsyncGenerator[Driver, None]:
    _impl_name, factory = request.param
    driver = await factory()
    try:
        yield driver
    finally:
        await driver.stop(delete=True)


async def test_stop_before_start_is_safe(driver_impl: Driver) -> None:
    """Spec §2.2 addendum: stop() before start() must not raise."""
    await driver_impl.stop(delete=True)
    await driver_impl.stop(delete=True)


async def test_start_twice_raises(driver_impl: Driver) -> None:
    await driver_impl.start(options=StartOptions())
    with pytest.raises(DriverAlreadyStartedError):
        await driver_impl.start(options=StartOptions())


async def test_exec_before_start_raises(driver_impl: Driver) -> None:
    with pytest.raises(DriverNotStartedError):
        await driver_impl.exec("true")


async def test_exec_after_stop_raises(driver_impl: Driver) -> None:
    await driver_impl.start(options=StartOptions())
    await driver_impl.stop(delete=True)
    with pytest.raises(DriverNotStartedError):
        await driver_impl.exec("true")
