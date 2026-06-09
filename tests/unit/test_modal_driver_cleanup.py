"""ModalDriver cleanup hooks — atexit + SIGINT/SIGTERM tear down sandboxes."""

from __future__ import annotations

import signal
import sys
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


async def test_started_drivers_registered_for_cleanup(
    fake_modal: ModuleType,
) -> None:
    sb = MagicMock()
    sb.object_id = "sb-1"
    fake_modal.Sandbox.create.return_value = sb
    fake_modal.App.lookup.return_value = MagicMock()

    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import _LIVE_DRIVERS, ModalDriver

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    assert drv in _LIVE_DRIVERS
    await drv.stop()
    assert drv not in _LIVE_DRIVERS


async def test_atexit_terminates_sandboxes(fake_modal: ModuleType) -> None:
    sb = MagicMock()
    sb.object_id = "sb-1"
    fake_modal.Sandbox.create.return_value = sb
    fake_modal.App.lookup.return_value = MagicMock()

    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver, _atexit_cleanup

    drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig(token_id="x", token_secret="y", workspace=None),
    )
    await drv.start()
    _atexit_cleanup()
    sb.terminate.assert_called()
    assert drv._state == "stopped"


async def test_sigint_handler_installed_on_first_start(
    fake_modal: ModuleType,
) -> None:
    """The handler is installed on first start() — caller can confirm by
    inspecting signal.getsignal()."""
    sb = MagicMock()
    sb.object_id = "sb-1"
    fake_modal.Sandbox.create.return_value = sb
    fake_modal.App.lookup.return_value = MagicMock()

    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    prior = signal.getsignal(signal.SIGINT)
    try:
        drv = ModalDriver(
            image="python:3.12-slim",
            config=ModalConfig(token_id="x", token_secret="y", workspace=None),
        )
        await drv.start()
        # After start(), our handler is installed
        assert signal.getsignal(signal.SIGINT) is not prior
        await drv.stop()
    finally:
        signal.signal(signal.SIGINT, prior)
