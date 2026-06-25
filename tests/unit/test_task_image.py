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
    docker_build_context: str | None = None,
) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="team-bench/task-1", name="Task 1"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image=docker_image,
            dockerfile=(PurePosixPath(dockerfile) if dockerfile else None),
            docker_build_context=(
                PurePosixPath(docker_build_context)
                if docker_build_context else None
            ),
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


async def test_resolve_task_image_passes_docker_api_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    dockerfile = task_dir / "environment" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM alpine:3.19\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    from_env_timeouts: list[float | None] = []

    def _from_env(*, timeout: float | None = None) -> _FakeDockerClient:
        from_env_timeouts.append(timeout)
        return fake_client

    monkeypatch.setattr(task_image.docker, "from_env", _from_env)

    await resolve_task_image(
        task_config=_task_config(dockerfile="environment/Dockerfile"),
        task_dir=task_dir,
        task_checksum="abc123",
        docker_api_timeout_sec=900.0,
    )

    assert from_env_timeouts == [900.0]


async def test_resolve_task_image_builds_from_explicit_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    task_dir = tmp_path / "task"
    build_context = task_dir / ".loom-build" / "client"
    build_context.mkdir(parents=True)
    (build_context / "Dockerfile").write_text("FROM alpine:3.19\n")
    (build_context / "protected.txt").write_text("build-only\n")
    (task_dir / "instruction.md").write_text("visible to the agent\n")
    fake_images = _FakeImages()
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env", lambda: fake_client)
    cfg = _task_config(
        dockerfile=".loom-build/client/Dockerfile",
        docker_build_context=".loom-build/client",
    )

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum="abc123",
    )

    assert image == task_image_tag(cfg, task_checksum="abc123")
    assert fake_images.build_calls[0]["path"] == str(build_context)
    assert fake_images.build_calls[0]["dockerfile"] == "Dockerfile"


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


class _BuildErrorImages:
    """Docker images namespace whose .build() raises BuildError with
    a populated build_log (Phase 3a #319 — tail surfaces in error)."""

    def __init__(self, *, log_lines: list[dict[str, Any]],
                 reason: str = "RUN failed") -> None:
        self._log = log_lines
        self._reason = reason

    def get(self, image: str) -> object:
        raise ImageNotFound("missing")

    def build(self, **kwargs: Any) -> tuple[object, list[object]]:
        from docker.errors import BuildError
        raise BuildError(reason=self._reason, build_log=iter(self._log))


async def test_resolve_task_image_surfaces_build_log_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """When `docker build` fails, the TaskImageBuildError must include
    the tail of the build_log so operators see WHY the RUN failed
    (e.g. pip's actual stderr), not just the failing command. Real
    motivating case from #316: `pip install pytest-jsonreport` failed
    because the canonical package name is `pytest-json-report`."""
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text(
        "FROM python:3.11-slim\nRUN pip install pytest-jsonreport\n",
    )

    log_lines = [
        {"stream": "Step 1/2 : FROM python:3.11-slim\n"},
        {"stream": "Step 2/2 : RUN pip install pytest-jsonreport\n"},
        {"stream": "ERROR: Could not find a version that satisfies the "
                   "requirement pytest-jsonreport\n"},
        {"stream": "ERROR: No matching distribution found for "
                   "pytest-jsonreport\n"},
    ]
    fake_images = _BuildErrorImages(log_lines=log_lines)
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env",
                        lambda: fake_client)

    with pytest.raises(TaskImageBuildError) as exc:
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    msg = str(exc.value)
    assert "failed to build Docker image" in msg
    assert "build log" in msg.lower()
    assert "No matching distribution" in msg
    assert "pytest-jsonreport" in msg
    assert fake_client.closed is True


async def test_resolve_task_image_truncates_build_log_to_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """Build logs from real Dockerfiles can be hundreds of lines;
    only the trailing _BUILD_LOG_TAIL_LINES are surfaced so the
    error message fits in API/SPA fields."""
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text(
        "FROM scratch\n",
    )

    # 200 lines of noise, then the actual error
    noise = [{"stream": f"noise line {i}\n"} for i in range(200)]
    error = [{"stream": "FINAL_ERROR: this should appear\n"}]
    fake_images = _BuildErrorImages(log_lines=noise + error)
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env",
                        lambda: fake_client)

    with pytest.raises(TaskImageBuildError) as exc:
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    msg = str(exc.value)
    assert "FINAL_ERROR" in msg
    # Early noise shouldn't survive truncation
    assert "noise line 0" not in msg
    assert "noise line 50" not in msg


async def test_resolve_task_image_empty_build_log_still_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """If BuildError has no log (rare — happens when docker daemon
    rejects the build before starting), we still raise TaskImageBuildError
    with the docker-py reason and don't append an empty 'build log:' section."""
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text("FROM scratch\n")

    fake_images = _BuildErrorImages(log_lines=[],
                                    reason="daemon rejected build")
    fake_client = _FakeDockerClient(fake_images)
    monkeypatch.setattr(task_image.docker, "from_env",
                        lambda: fake_client)

    with pytest.raises(TaskImageBuildError) as exc:
        await resolve_task_image(
            task_config=_task_config(dockerfile="environment/Dockerfile"),
            task_dir=task_dir,
            task_checksum="abc123",
        )

    msg = str(exc.value)
    assert "daemon rejected build" in msg
    assert "build log" not in msg.lower()
