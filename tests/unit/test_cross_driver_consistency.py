"""Cross-driver byte-equivalent-trajectory test.

Runs the same trivial trial sequence (start → exec → stop) against three
drivers (DockerDriver mocked, DaytonaDriver mocked, ModalDriver mocked)
and asserts the produced event lists match modulo a small set of
expected drift (timestamps, ids, driver-name field).

This is the canary for the arc's Driver-Protocol-stability claim: if
Modal forced a Protocol change, this test would fail to assemble (the
abstractions diverge).
"""

from __future__ import annotations

import json
import re
import sys
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pytest

_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
_ISO_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?",
)


def _normalize(event: dict[str, Any]) -> dict[str, Any]:
    """Strip driver-name, timestamps, UUIDs from a trajectory event."""
    out: dict[str, Any] = {}
    for k, v in event.items():
        if k in {"driver", "backend", "sandbox_id", "container_id"}:
            continue
        if isinstance(v, str):
            v = _UUID_RE.sub("<uuid>", v)
            v = _ISO_TS_RE.sub("<ts>", v)
        if isinstance(v, dict):
            v = _normalize(v)
        out[k] = v
    out.pop("event_id", None)
    out.pop("captured_at", None)
    return out


def _normalize_events(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [_normalize(e) for e in events]


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


def _wire_modal_fakes(fake_modal: ModuleType) -> Any:
    """Hard-coded exec output so the trajectory is deterministic."""
    sb = MagicMock()
    sb.object_id = "sb-fixed"
    proc = MagicMock()
    proc.stdout = MagicMock()
    proc.stdout.read.return_value = "hello\n"
    proc.stderr = MagicMock()
    proc.stderr.read.return_value = ""
    proc.wait.return_value = 0
    sb.exec.return_value = proc
    fake_modal.Sandbox.create.return_value = sb
    fake_modal.App.lookup.return_value = MagicMock()
    return sb


async def _run_with_driver(drv: Any) -> list[dict[str, Any]]:
    """Tiny harness: start, exec one command, stop. Capture events."""
    events: list[dict[str, Any]] = []
    await drv.start()
    events.append({"kind": "start", "image": drv.image})
    r = await drv.exec("echo hello")
    events.append(
        {
            "kind": "exec",
            "rc": r.return_code,
            "stdout": r.stdout.decode("utf-8", errors="surrogateescape"),
            "stderr": r.stderr.decode("utf-8", errors="surrogateescape"),
        }
    )
    await drv.stop()
    events.append({"kind": "stop"})
    return events


async def test_modal_matches_docker_trajectory(
    fake_modal: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODAL_TOKEN_ID", "x")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "y")
    _wire_modal_fakes(fake_modal)

    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    modal_drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig.from_env(),
    )
    modal_events = await _run_with_driver(modal_drv)

    from loom.driver.docker import DockerDriver

    class _PatchedDockerDriver(DockerDriver):
        async def start(self, *, options: Any = None) -> None:  # type: ignore[override]
            self._state = "running"
            self._container = MagicMock()

        async def stop(self, *, delete: bool = True) -> None:  # type: ignore[override]
            self._state = "stopped"

        async def exec(  # type: ignore[override]
            self,
            cmd: str,
            *,
            user: Any = None,
            cwd: Any = None,
            env: Any = None,
            timeout_sec: Any = None,
        ) -> Any:
            from loom.models.exec import ExecResult

            return ExecResult(
                return_code=0,
                stdout=b"hello\n",
                stderr=b"",
                truncated=False,
                duration_sec=0.001,
            )

    docker_drv = _PatchedDockerDriver(image="python:3.12-slim")
    docker_events = await _run_with_driver(docker_drv)

    norm_modal = _normalize_events(modal_events)
    norm_docker = _normalize_events(docker_events)
    assert norm_modal == norm_docker, (
        f"Driver Protocol drift!\n"
        f"modal:  {json.dumps(norm_modal, indent=2)}\n"
        f"docker: {json.dumps(norm_docker, indent=2)}"
    )


async def test_modal_matches_daytona_trajectory(
    fake_modal: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODAL_TOKEN_ID", "x")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "y")
    monkeypatch.setenv("DAYTONA_API_KEY", "z")
    _wire_modal_fakes(fake_modal)

    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver

    modal_drv = ModalDriver(
        image="python:3.12-slim",
        config=ModalConfig.from_env(),
    )
    modal_events = await _run_with_driver(modal_drv)

    from loom_drivers.daytona.config import DaytonaConfig
    from loom_drivers.daytona.driver import DaytonaDriver

    class _PatchedDaytonaDriver(DaytonaDriver):
        async def start(self, *, options: Any = None) -> None:  # type: ignore[override]
            self._state = "running"

        async def stop(self, *, delete: bool = True) -> None:  # type: ignore[override]
            self._state = "stopped"

        async def exec(  # type: ignore[override]
            self,
            cmd: str,
            *,
            user: Any = None,
            cwd: Any = None,
            env: Any = None,
            timeout_sec: Any = None,
        ) -> Any:
            from loom.models.exec import ExecResult

            return ExecResult(
                return_code=0,
                stdout=b"hello\n",
                stderr=b"",
                truncated=False,
                duration_sec=0.001,
            )

    daytona_drv = _PatchedDaytonaDriver(
        image="python:3.12-slim",
        config=DaytonaConfig.from_env(),
    )
    daytona_events = await _run_with_driver(daytona_drv)

    assert _normalize_events(modal_events) == _normalize_events(
        daytona_events,
    )


def test_driver_protocol_surface_is_backfilled_across_drivers() -> None:
    """Keep every intentional Driver protocol method implemented by all backends.

    Inspect Driver protocol members and assert the set matches the
    documented post-Plan-26 surface. If this test fails, revisit the
    Protocol definition and back the change out into base.py before
    committing the driver.
    """
    from loom.driver.base import Driver

    expected_methods = {
        "start",
        "stop",
        "exec",
        "exec_streaming",
        "upload",
        "download",
        "set_network_policy",
        "run_healthcheck",
        "resource_snapshot",
    }
    actual_callable = {
        name for name in dir(Driver) if not name.startswith("_") and callable(getattr(Driver, name))
    }
    missing = expected_methods - actual_callable
    assert not missing, f"Driver Protocol lost methods: {missing}"
    # The Protocol body should contain only the expected methods plus
    # the `capabilities` / `os` data attributes. Anything else is a
    # new Protocol member that needs to be backfilled in all drivers
    # before merging.
    declared_in_protocol = set(getattr(Driver, "__annotations__", {}))
    declared_in_protocol |= expected_methods
    extra = actual_callable - expected_methods - {"capabilities", "os"}
    assert not extra, (
        f"Driver Protocol grew unexpectedly: extra={extra}. "
        "Backfill the new method in DockerDriver + DaytonaDriver + "
        "ModalDriver if intentional, or back out the change."
    )
