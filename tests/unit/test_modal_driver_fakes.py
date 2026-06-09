"""ModalDriver — unit tests with mocked modal SDK (no live cloud)."""

from __future__ import annotations

import sys
import time
from types import ModuleType
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_modal(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    fake = ModuleType("modal")
    fake.App = MagicMock()
    fake.Image = MagicMock()
    fake.Sandbox = MagicMock()
    monkeypatch.setitem(sys.modules, "modal", fake)
    for mod in (
        "loom_drivers.modal.client",
        "loom_drivers.modal.driver",
        "loom_drivers.modal.images",
    ):
        sys.modules.pop(mod, None)
    return fake


def _build_sandbox_mock() -> MagicMock:
    sb = MagicMock()
    sb.object_id = "sb-test-1"
    return sb


async def test_start_creates_sandbox_with_workdir_and_env(
    fake_modal: ModuleType,
) -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    sb = _build_sandbox_mock()
    fake_modal.Sandbox.create.return_value = sb
    fake_modal.App.lookup.return_value = MagicMock()

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    fake_modal.Sandbox.create.assert_called_once()
    kwargs = fake_modal.Sandbox.create.call_args.kwargs
    assert kwargs["workdir"] == "/workspace"
    assert kwargs["block_network"] is False
    await drv.stop()
    sb.terminate.assert_called_once()


async def test_start_twice_raises(fake_modal: ModuleType) -> None:
    from loom.errors import DriverAlreadyStartedError
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    fake_modal.Sandbox.create.return_value = _build_sandbox_mock()
    fake_modal.App.lookup.return_value = MagicMock()

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    with pytest.raises(DriverAlreadyStartedError):
        await drv.start()
    await drv.stop()


async def test_stop_idempotent_before_start(fake_modal: ModuleType) -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.stop()
    await drv.stop()


async def test_exec_blocks_before_start(fake_modal: ModuleType) -> None:
    from loom.errors import DriverNotStartedError
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    with pytest.raises(DriverNotStartedError):
        await drv.exec("echo hi")


async def test_exec_returns_exec_result(fake_modal: ModuleType) -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    sb = _build_sandbox_mock()
    proc = MagicMock()
    proc.stdout = MagicMock()
    proc.stdout.read.return_value = "hi\n"
    proc.stderr = MagicMock()
    proc.stderr.read.return_value = ""
    proc.wait.return_value = 0
    sb.exec.return_value = proc

    fake_modal.Sandbox.create.return_value = sb
    fake_modal.App.lookup.return_value = MagicMock()

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    try:
        r = await drv.exec("echo hi")
        assert r.return_code == 0
        assert r.stdout == b"hi\n"
        assert r.stderr == b""
        assert r.truncated is False
    finally:
        await drv.stop()


async def test_gpu_passed_to_create(fake_modal: ModuleType) -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    fake_modal.Sandbox.create.return_value = _build_sandbox_mock()
    fake_modal.App.lookup.return_value = MagicMock()

    drv = ModalDriver(
        image="python:3.12-slim",
        gpu="A10",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    kwargs = fake_modal.Sandbox.create.call_args.kwargs
    assert kwargs["gpu"] == "A10"
    await drv.stop()


async def test_invalid_gpu_rejected_at_construct(
    fake_modal: ModuleType,
) -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver
    from loom_drivers.modal.gpu import ModalGPUError

    with pytest.raises(ModalGPUError):
        ModalDriver(
            image="python:3.12-slim",
            gpu="Z9000",
            config=ModalConfig(token_id="x", token_secret="y", workspace=None),
        )


async def test_capabilities_reflect_gpu_types(
    fake_modal: ModuleType,
) -> None:
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    assert "A10" in drv.capabilities.gpu_types
    assert drv.capabilities.gpu_vendor == "nvidia"
    assert drv.os == "linux"


async def test_set_network_policy_after_start_rejects_change(
    fake_modal: ModuleType,
) -> None:
    """Modal sandbox network is create-time only; mutating it after
    start() must raise DriverError unless policy equals baseline."""
    from loom.errors import DriverError
    from loom.models.networking import Allowlist, Public
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    fake_modal.Sandbox.create.return_value = _build_sandbox_mock()
    fake_modal.App.lookup.return_value = MagicMock()

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    try:
        # Same baseline is fine (no-op):
        await drv.set_network_policy(Public())
        # Changing it is not:
        with pytest.raises(DriverError) as ei:
            await drv.set_network_policy(
                Allowlist(domains=("example.com",)),
            )
        assert "create-time" in str(ei.value).lower()
    finally:
        await drv.stop()


async def test_exec_timeout_terminates_proc(fake_modal: ModuleType) -> None:
    """When exec(timeout_sec=...) fires, the underlying ContainerProcess
    must be terminated so the sandbox isn't left burning billed seconds
    until Modal's sandbox-wide cap eventually kills it."""
    import asyncio as _asyncio

    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    sb = _build_sandbox_mock()
    proc = MagicMock()

    def _slow_read() -> str:
        # Real blocking call so wait_for has something to cancel —
        # a MagicMock returning instantly would race past wait_for.
        time.sleep(2.0)
        return ""

    proc.stdout.read.side_effect = _slow_read
    proc.stderr.read.return_value = ""
    proc.wait.return_value = 0
    sb.exec.return_value = proc

    fake_modal.Sandbox.create.return_value = sb
    fake_modal.App.lookup.return_value = MagicMock()

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    try:
        with pytest.raises(_asyncio.TimeoutError):
            await drv.exec("sleep 5", timeout_sec=0.1)
        proc.terminate.assert_called()
    finally:
        await drv.stop()


def test_modal_driver_satisfies_driver_protocol(
    fake_modal: ModuleType,
) -> None:
    from loom.driver.base import Driver
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    assert isinstance(drv, Driver)
