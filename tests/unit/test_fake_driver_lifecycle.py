import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.errors import DriverAlreadyStartedError, DriverNotStartedError


@pytest.fixture
async def fake() -> FakeDriver:
    return FakeDriver()


async def test_default_capabilities(fake: FakeDriver):
    assert fake.os == "linux"
    assert "public" in fake.capabilities.network_policies
    assert "no-network" in fake.capabilities.network_policies


async def test_lifecycle_states(fake: FakeDriver):
    assert fake.state == "constructed"
    await fake.start(options=StartOptions())
    assert fake.state == "running"
    await fake.stop(delete=True)
    assert fake.state == "stopped"


async def test_double_start_raises(fake: FakeDriver):
    await fake.start(options=StartOptions())
    with pytest.raises(DriverAlreadyStartedError):
        await fake.start(options=StartOptions())


async def test_start_after_stop_raises(fake: FakeDriver):
    """Spec: start() at most once. After a real stop(), start() must not succeed."""
    await fake.start(options=StartOptions())
    await fake.stop(delete=True)
    with pytest.raises(DriverAlreadyStartedError):
        await fake.start(options=StartOptions())


async def test_stop_idempotent(fake: FakeDriver):
    """stop() must be safe (a) before start, (b) after start, (c) twice in a row."""
    await fake.stop(delete=True)  # before start: no-op
    assert fake.state == "constructed"
    await fake.start(options=StartOptions())
    await fake.stop(delete=True)
    assert fake.state == "stopped"
    await fake.stop(delete=True)  # second stop: still stopped
    assert fake.state == "stopped"


async def test_exec_before_start_raises(fake: FakeDriver):
    with pytest.raises(DriverNotStartedError):
        await fake.exec("true")


async def test_exec_after_stop_raises(fake: FakeDriver):
    await fake.start(options=StartOptions())
    await fake.stop(delete=True)
    with pytest.raises(DriverNotStartedError):
        await fake.exec("true")
