"""--backend daytona dispatches to DaytonaDriver via _driver_factory.

Adapted from plan-doc's `make_driver` reference (Plan 23 ships
`_driver_factory(backend, cfg)` returning Callable[[], Driver]).

Signature note (post-#232): `_driver_factory` is async and takes
`task_dir` + `task_checksum` so it can resolve task images that ship
a Dockerfile via `loom.driver.task_image.resolve_task_image`. Tasks
that declare `docker_image` (this fixture) skip the build branch.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

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
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from loom_drivers.daytona.driver import DaytonaDriver

    monkeypatch.setenv("DAYTONA_API_KEY", "k")
    factory = asyncio.run(_driver_factory(
        "daytona", _stub_cfg(),
        task_dir=tmp_path, task_checksum="sha256:stub",
    ))
    drv = factory()
    assert isinstance(drv, DaytonaDriver)
    assert drv.image == "python:3.12-slim"


def test_unknown_backend_raises(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="unknown backend"):
        asyncio.run(_driver_factory(
            "wat", _stub_cfg(),
            task_dir=tmp_path, task_checksum="sha256:stub",
        ))


def _dockerfile_cfg() -> TaskConfig:
    return TaskConfig.model_validate({
        "schema_version": "1",
        "task": {"id": "x/y", "name": "y"},
        "environment": {
            "os": "linux",
            "dockerfile": "Dockerfile",
            "docker_build_context": ".",
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    })


def test_docker_backend_with_dockerfile_calls_resolve_task_image(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Regression for #232: `loom run` previously fell back to `alpine`
    when a task shipped a Dockerfile (every TB-2 task does), silently
    breaking trials with `env: can't execute 'bash'`. `_driver_factory`
    now routes Dockerfile tasks through
    `loom.driver.task_image.resolve_task_image`, the same path the
    worker uses."""
    from loom_cli import run_cmd as run_cmd_mod

    resolved: dict[str, object] = {}

    async def fake_resolve(*, task_config: object, task_dir: Path,
                           task_checksum: str) -> str:
        resolved["task_id"] = task_config.task.id  # type: ignore[attr-defined]
        resolved["task_dir"] = task_dir
        resolved["task_checksum"] = task_checksum
        return "loom-task:fake-tag"

    import loom.driver.task_image as task_image_mod
    monkeypatch.setattr(task_image_mod, "resolve_task_image", fake_resolve)
    # The run_cmd module imports resolve_task_image lazily inside
    # _driver_factory, so a fresh `from` patch on the module is enough.

    captured: list[object] = []

    class _StubDockerDriver:
        def __init__(self, *, image: str, workspace: object) -> None:
            captured.append(image)

    monkeypatch.setattr(run_cmd_mod, "DockerDriver", _StubDockerDriver)

    factory = asyncio.run(_driver_factory(
        "docker", _dockerfile_cfg(),
        task_dir=tmp_path, task_checksum="sha256:abc",
    ))
    factory()

    assert resolved == {
        "task_id": "x/y",
        "task_dir": tmp_path,
        "task_checksum": "sha256:abc",
    }
    # The Driver gets the built tag, not the pre-#232 `alpine` fallback.
    assert captured == ["loom-task:fake-tag"]


def test_docker_backend_with_docker_image_skips_dockerfile_build(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """When the task declares `docker_image`, resolve_task_image
    short-circuits — no build, no docker daemon round-trip — just
    returns the configured tag. Confirms the post-#232 path doesn't
    over-eagerly try to build for cached-image tasks."""
    called: list[str] = []

    async def fake_resolve(*, task_config: object, task_dir: Path,
                           task_checksum: str) -> str:
        called.append(task_config.environment.docker_image)  # type: ignore[attr-defined]
        return task_config.environment.docker_image  # type: ignore[attr-defined]

    import loom.driver.task_image as task_image_mod
    monkeypatch.setattr(task_image_mod, "resolve_task_image", fake_resolve)

    asyncio.run(_driver_factory(
        "docker", _stub_cfg(),
        task_dir=tmp_path, task_checksum="sha256:stub",
    ))
    assert called == ["python:3.12-slim"]


def test_back_compat_loom_worker_task_image_re_exports() -> None:
    """The module moved from `loom_worker.task_image` to
    `loom.driver.task_image` but `loom_worker.task_image` re-exports
    the public API so existing imports (loom_worker.main_loop,
    loom_worker.task_sidecars, tests/unit/test_task_image.py) keep
    working without churn."""
    from loom.driver import task_image as canonical
    from loom_worker import task_image as legacy

    assert legacy.resolve_task_image is canonical.resolve_task_image
    assert legacy.TaskImageBuildError is canonical.TaskImageBuildError
    assert legacy.task_image_tag is canonical.task_image_tag
    assert legacy.DEFAULT_TASK_IMAGE is canonical.DEFAULT_TASK_IMAGE
