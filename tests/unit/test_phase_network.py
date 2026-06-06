import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.networking import NoNetwork, Public
from loom.trial.phase_network import phase_network


async def test_no_switch_when_phase_matches_baseline():
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    await fake.set_network_policy(Public())
    assert fake.network_policy.kind == "public"

    async with phase_network(fake, baseline=Public(), phase=Public()):
        assert fake.network_policy.kind == "public"
    assert fake.network_policy.kind == "public"


async def test_switch_and_restore():
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    await fake.set_network_policy(Public())
    async with phase_network(fake, baseline=Public(), phase=NoNetwork()):
        assert fake.network_policy.kind == "no-network"
    assert fake.network_policy.kind == "public"


async def test_restore_on_exception():
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    await fake.set_network_policy(Public())
    with pytest.raises(RuntimeError, match="boom"):
        async with phase_network(fake, baseline=Public(), phase=NoNetwork()):
            assert fake.network_policy.kind == "no-network"
            raise RuntimeError("boom")
    assert fake.network_policy.kind == "public"
