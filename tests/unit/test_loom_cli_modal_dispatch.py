"""`build_driver` + argparse wire — --backend modal and --gpu."""

from __future__ import annotations

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


def test_build_driver_modal(
    fake_modal: ModuleType, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODAL_TOKEN_ID", "x")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "y")
    from loom_cli.run_cmd import build_driver
    from loom_drivers.modal.driver import ModalDriver

    drv = build_driver(
        backend="modal", image="python:3.12-slim", gpu="A10",
    )
    assert isinstance(drv, ModalDriver)
    assert drv.gpu == "A10"
    assert drv.image == "python:3.12-slim"


def test_docker_backend_rejects_gpu() -> None:
    from loom_cli.run_cmd import UnsupportedFlagError, build_driver

    with pytest.raises(UnsupportedFlagError) as ei:
        build_driver(
            backend="docker", image="python:3.12-slim", gpu="A10",
        )
    assert "--gpu" in str(ei.value)
    assert "docker" in str(ei.value)
    assert "modal" in str(ei.value)


def test_modal_backend_without_creds_raises_friendly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for var in ("MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET"):
        monkeypatch.delenv(var, raising=False)
    from loom_cli.run_cmd import build_driver
    from loom_drivers.modal.config import ModalConfigError

    with pytest.raises(ModalConfigError):
        build_driver(
            backend="modal", image="python:3.12-slim", gpu=None,
        )


def test_unknown_backend_clear_error() -> None:
    from loom_cli.run_cmd import build_driver

    with pytest.raises(ValueError) as ei:
        build_driver(backend="kubernetes", image="img", gpu=None)
    assert "kubernetes" in str(ei.value)
    assert "docker" in str(ei.value)
    assert "modal" in str(ei.value)
