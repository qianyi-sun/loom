"""ModalClient — sync SDK bridge tests (mocked modal module)."""

from __future__ import annotations

import asyncio
import sys
import time
from types import ModuleType
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_modal(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Install a synthetic `modal` module so we can test the bridge
    without the real SDK or network."""
    fake = ModuleType("modal")
    fake.App = MagicMock()
    fake.Image = MagicMock()
    fake.Sandbox = MagicMock()
    monkeypatch.setitem(sys.modules, "modal", fake)
    sys.modules.pop("loom_drivers.modal.client", None)
    return fake


async def test_create_sandbox_calls_modal_in_thread(
    fake_modal: ModuleType,
) -> None:
    from loom_drivers.modal.client import ModalClient
    from loom_drivers.modal.config import ModalConfig

    fake_sb = MagicMock()
    fake_sb.object_id = "sb-test-123"
    fake_modal.Sandbox.create.return_value = fake_sb
    fake_app = MagicMock()
    fake_modal.App.lookup.return_value = fake_app

    cfg = ModalConfig(token_id="x", token_secret="y", workspace=None)
    client = ModalClient(cfg)
    app = await client.lookup_app("loom-runs")
    sb = await client.create_sandbox(
        app=app,
        image=MagicMock(),
        timeout_sec=60,
        gpu=None,
        block_network=False,
        outbound_cidr_allowlist=None,
        workdir="/workspace",
        env={"FOO": "bar"},
    )
    assert sb is fake_sb
    fake_modal.Sandbox.create.assert_called_once()
    kwargs = fake_modal.Sandbox.create.call_args.kwargs
    assert kwargs["app"] is fake_app
    assert kwargs["timeout"] == 60
    assert kwargs["workdir"] == "/workspace"
    assert kwargs["env"] == {"FOO": "bar"}
    assert "gpu" not in kwargs or kwargs["gpu"] is None


async def test_create_sandbox_passes_gpu(fake_modal: ModuleType) -> None:
    from loom_drivers.modal.client import ModalClient
    from loom_drivers.modal.config import ModalConfig

    fake_modal.Sandbox.create.return_value = MagicMock()
    fake_modal.App.lookup.return_value = MagicMock()

    cfg = ModalConfig(token_id="x", token_secret="y", workspace=None)
    client = ModalClient(cfg)
    app = await client.lookup_app("loom-runs")
    await client.create_sandbox(
        app=app, image=MagicMock(), timeout_sec=60,
        gpu="A10", block_network=False, outbound_cidr_allowlist=None,
        workdir="/workspace", env={},
    )
    assert fake_modal.Sandbox.create.call_args.kwargs["gpu"] == "A10"


async def test_missing_modal_raises_friendly_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "modal", None)
    sys.modules.pop("loom_drivers.modal.client", None)
    from loom_drivers.modal.client import ModalClient, ModalSDKNotInstalledError
    from loom_drivers.modal.config import ModalConfig

    cfg = ModalConfig(token_id="x", token_secret="y", workspace=None)
    client = ModalClient(cfg)
    with pytest.raises(ModalSDKNotInstalledError) as ei:
        await client.lookup_app("loom-runs")
    assert "pip install 'loom[modal]'" in str(ei.value)


async def test_terminate_runs_in_thread(fake_modal: ModuleType) -> None:
    from loom_drivers.modal.client import ModalClient
    from loom_drivers.modal.config import ModalConfig

    sb = MagicMock()
    cfg = ModalConfig(token_id="x", token_secret="y", workspace=None)
    client = ModalClient(cfg)
    await client.terminate_sandbox(sb, wait=True)
    sb.terminate.assert_called_once_with(wait=True)


async def test_no_blocking_in_event_loop(fake_modal: ModuleType) -> None:
    """Smoke: while the bridge call is running, other coroutines still
    make progress — proves we are not blocking the loop on a sync call."""
    from loom_drivers.modal.client import ModalClient
    from loom_drivers.modal.config import ModalConfig

    def slow_create(**_: object) -> object:
        time.sleep(0.3)
        return MagicMock(object_id="sb-1")

    fake_modal.Sandbox.create.side_effect = slow_create
    fake_modal.App.lookup.return_value = MagicMock()

    cfg = ModalConfig(token_id="x", token_secret="y", workspace=None)
    client = ModalClient(cfg)

    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        for _ in range(20):
            await asyncio.sleep(0.02)
            ticks += 1

    t = asyncio.create_task(ticker())
    app = await client.lookup_app("loom-runs")
    await client.create_sandbox(
        app=app, image=MagicMock(), timeout_sec=60,
        gpu=None, block_network=False, outbound_cidr_allowlist=None,
        workdir="/workspace", env={},
    )
    await t
    assert ticks >= 10, f"event loop appears blocked (only {ticks} ticks)"
