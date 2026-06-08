"""--backend daytona dispatches to DaytonaDriver via _driver_factory.

Adapted from plan-doc's `make_driver` reference (Plan 23 ships
`_driver_factory(backend, cfg)` returning Callable[[], Driver]).
"""

from __future__ import annotations

import pytest

from loom.models.task import TaskConfig
from loom_cli.run_cmd import _driver_factory


def _stub_cfg() -> TaskConfig:
    return TaskConfig.model_validate({
        "schema_version": "1",
        "task": {"id": "x/y", "name": "y"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    })


def test_daytona_backend_returns_daytona_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_drivers.daytona.driver import DaytonaDriver

    monkeypatch.setenv("DAYTONA_API_KEY", "k")
    factory = _driver_factory("daytona", _stub_cfg())
    drv = factory()
    assert isinstance(drv, DaytonaDriver)
    assert drv.image == "python:3.12-slim"


def test_unknown_backend_raises() -> None:
    with pytest.raises(SystemExit, match="unknown backend"):
        _driver_factory("wat", _stub_cfg())
