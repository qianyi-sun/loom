from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

import pytest
from docker.errors import ImageNotFound

from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom_worker import task_image
from loom_worker.task_image import TaskImageBuildError, resolve_task_image, task_image_tag


def _task_config(
    *, docker_image: str | None = None,
    dockerfile: str | None = None,
) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="team-bench/task-1", name="Task 1"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image=docker_image,
            dockerfile=(PurePosixPath(dockerfile) if dockerfile else None),
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
    )


class _FakeImages:
    def __init__(self, *, cached: bool = False) -> None:
        self.cached = cached
        self.get_calls: list[str] = []
        self.build_calls: list[dict[str, Any]] = []

    def get(self, image: str) -> object:
        self.get_calls.append(image)
        if self.cached:
            return object()
        raise ImageNotFound("missing")

    def build(self, **kwargs: Any) -> tuple[object, list[object]]:
        self.build_calls.append(kwargs)
        self.cached = True
        return object(), []


class _FakeDockerClient:
    def __init__(self, images: _FakeImages) -> None:
        self.images = images
        self.closed = False

    def close(self) -> None:
        self.closed = True


async def test_resolve_task_image_prefers_explicit_docker_image(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    called = False

    def fail_from_env() -> object:
        nonlocal called
        called = True
        raise AssertionError("docker client should not be created")

    monkeypatch.setattr(task_image.docker, "from_env", fail_from_env)

    image = await resolve_task_image(
        task_config=_task_config(docker_image="python:3.11-alpine"),
        task_dir=tmp_path,
        task_checksum="abc123",
    )

    assert image == "python:3.11-alpine"
    assert called is False


async def test_resolve_task_image_builds_and_caches_dockerfile_image(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    cfg = _task_config(dockerfile="environment/Dockerfile")

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert image == task_image_tag(cfg, task_checksum="abc123")
    assert fake_images.get_calls == [image]
    assert fake_images.build_calls == [
        {
            "path": str(task_dir),
            "dockerfile": "environment/Dockerfile",
            "tag": image,
            "rm": True,
            "forcerm": True,
            "pull": False,
            "labels": {
                "loom.task_id": "team-bench/task-1",
                "loom.task_checksum": "abc123",
                "loom.task_dockerfile": "environment/Dockerfile",
            },
        }
    ]
    assert fake_client.closed is True


async def test_resolve_task_image_reuses_cached_dockerfile_image(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages(cached=True)
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)

    image = await resolve_task_image(
        task_config=_task_config(dockerfile="environment/Dockerfile"),
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert image.startswith("loom-task:")
    assert fake_images.get_calls == [image]
    assert fake_images.build_calls == []


async def test_resolve_task_image_rejects_oversized_build_context_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    (task_dir / "instruction.md").write_text("do it\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    monkeypatch.setenv("LOOM_TASK_IMAGE_BUILD_MAX_FILES", "1")

    with pytest.raises(TaskImageBuildError, match="file limit"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    assert fake_images.build_calls == []
    assert fake_client.closed is True


async def test_resolve_task_image_rejects_oversized_build_context_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    monkeypatch.setenv("LOOM_TASK_IMAGE_BUILD_MAX_BYTES", "1")

    with pytest.raises(TaskImageBuildError, match="byte limit"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    assert fake_images.build_calls == []
    assert fake_client.closed is True


async def test_resolve_task_image_rejects_missing_dockerfile(tmp_path) -> None:
    task_dir = tmp_path / "task"
    task_dir.mkdir()

    with pytest.raises(TaskImageBuildError, match="environment/Dockerfile"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )


async def test_resolve_task_image_rejects_dockerfile_path_traversal(tmp_path) -> None:
    task_dir = tmp_path / "task"
    task_dir.mkdir()

    with pytest.raises(TaskImageBuildError, match="inside the task bundle"):
        await resolve_task_image(
            task_config=_task_config(dockerfile="../Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )
