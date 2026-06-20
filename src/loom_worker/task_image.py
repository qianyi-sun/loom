"""Resolve service-mode task images.

Published benchmarks can declare either a ready-to-run Docker image or a
Dockerfile inside the task bundle. The worker materializes the bundle before a
trial starts, so Dockerfile tasks can be built from that local directory and
cached under a deterministic tag.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
from pathlib import Path, PurePosixPath
from typing import Any

import docker
from docker.errors import ImageNotFound

from loom.models.task import TaskConfig

DEFAULT_TASK_IMAGE = "alpine"
DEFAULT_BUILD_CONTEXT_MAX_FILES = 2_000
DEFAULT_BUILD_CONTEXT_MAX_BYTES = 512 * 1024 * 1024
ENV_BUILD_CONTEXT_MAX_FILES = "LOOM_TASK_IMAGE_BUILD_MAX_FILES"
ENV_BUILD_CONTEXT_MAX_BYTES = "LOOM_TASK_IMAGE_BUILD_MAX_BYTES"


class TaskImageBuildError(RuntimeError):
    """Raised when the worker cannot resolve a task sandbox image."""


def task_image_tag(task_config: TaskConfig, *, task_checksum: str) -> str:
    """Stable local Docker tag for a task-bundle Dockerfile build."""

    dockerfile = task_config.environment.dockerfile
    dockerfile_text = dockerfile.as_posix() if dockerfile is not None else ""
    build_context = task_config.environment.docker_build_context
    build_context_text = (
        build_context.as_posix() if build_context is not None else ""
    )
    material = "\n".join([
        task_config.task.id,
        task_checksum,
        dockerfile_text,
        build_context_text,
    ])
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
    return f"loom-task:{digest}"


async def resolve_task_image(
    *,
    task_config: TaskConfig,
    task_dir: Path,
    task_checksum: str,
) -> str:
    """Return the Docker image a service worker should use for this task.

    ``environment.docker_image`` remains the fastest path. When a task declares
    ``environment.dockerfile`` instead, build it from the materialized task
    bundle if the deterministic cache tag is absent. Tasks that declare neither
    keep the historic ``alpine`` fallback.
    """

    if task_config.environment.docker_image:
        return task_config.environment.docker_image
    if task_config.environment.dockerfile is None:
        return DEFAULT_TASK_IMAGE

    dockerfile = _resolve_dockerfile_path(
        task_dir=task_dir,
        dockerfile=task_config.environment.dockerfile,
    )
    build_context = _resolve_build_context_path(
        task_dir=task_dir,
        dockerfile=dockerfile,
        docker_build_context=task_config.environment.docker_build_context,
    )
    tag = task_image_tag(task_config, task_checksum=task_checksum)
    timeout = task_config.environment.build_timeout_sec
    try:
        await asyncio.wait_for(
            asyncio.to_thread(
                _ensure_dockerfile_image,
                tag=tag,
                task_config=task_config,
                task_checksum=task_checksum,
                task_dir=task_dir,
                dockerfile=dockerfile,
                build_context=build_context,
            ),
            timeout=timeout,
        )
    except TimeoutError as exc:
        raise TaskImageBuildError(
            f"building Docker image {tag!r} from "
            f"{task_config.environment.dockerfile.as_posix()!r} exceeded "
            f"{timeout:g}s",
        ) from exc
    return tag


def _resolve_dockerfile_path(*, task_dir: Path, dockerfile: PurePosixPath) -> Path:
    if dockerfile.is_absolute() or ".." in dockerfile.parts:
        raise TaskImageBuildError(
            "environment.dockerfile must stay inside the task bundle; "
            f"got {dockerfile.as_posix()!r}",
        )
    path = task_dir.joinpath(*dockerfile.parts)
    if not path.is_file():
        raise TaskImageBuildError(
            f"environment.dockerfile {dockerfile.as_posix()!r} was not found "
            f"under materialized task bundle {task_dir}",
        )
    return path


def _resolve_build_context_path(
    *,
    task_dir: Path,
    dockerfile: Path,
    docker_build_context: PurePosixPath | None,
) -> Path:
    if docker_build_context is None:
        return task_dir
    if docker_build_context.is_absolute() or ".." in docker_build_context.parts:
        raise TaskImageBuildError(
            "environment.docker_build_context must stay inside the task "
            f"bundle; got {docker_build_context.as_posix()!r}",
        )
    path = task_dir.joinpath(*docker_build_context.parts)
    if not path.is_dir():
        raise TaskImageBuildError(
            "environment.docker_build_context "
            f"{docker_build_context.as_posix()!r} was not found under "
            f"materialized task bundle {task_dir}",
        )
    try:
        dockerfile.relative_to(path)
    except ValueError as exc:
        raise TaskImageBuildError(
            "environment.dockerfile must be inside "
            "environment.docker_build_context; got "
            f"dockerfile={dockerfile.relative_to(task_dir).as_posix()!r} "
            f"context={docker_build_context.as_posix()!r}",
        ) from exc
    return path


def _ensure_dockerfile_image(
    *,
    tag: str,
    task_config: TaskConfig,
    task_checksum: str,
    task_dir: Path,
    dockerfile: Path,
    build_context: Path,
) -> None:
    configured_dockerfile = task_config.environment.dockerfile
    assert configured_dockerfile is not None
    client: Any = docker.from_env()
    try:
        try:
            client.images.get(tag)
            return
        except ImageNotFound:
            pass

        rel_dockerfile = dockerfile.relative_to(build_context).as_posix()
        _enforce_build_context_limits(build_context)
        client.images.build(
            path=str(build_context),
            dockerfile=rel_dockerfile,
            tag=tag,
            rm=True,
            forcerm=True,
            pull=False,
            labels={
                "loom.task_id": task_config.task.id,
                "loom.task_checksum": task_checksum,
                "loom.task_dockerfile": rel_dockerfile,
            },
        )
    except TaskImageBuildError:
        raise
    except Exception as exc:
        raise TaskImageBuildError(
            f"failed to build Docker image {tag!r} from "
            f"{configured_dockerfile.as_posix()!r}: {exc}",
        ) from exc
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _enforce_build_context_limits(task_dir: Path) -> None:
    max_files = _operator_limit(
        ENV_BUILD_CONTEXT_MAX_FILES,
        DEFAULT_BUILD_CONTEXT_MAX_FILES,
    )
    max_bytes = _operator_limit(
        ENV_BUILD_CONTEXT_MAX_BYTES,
        DEFAULT_BUILD_CONTEXT_MAX_BYTES,
    )
    file_count = 0
    byte_count = 0

    for path in task_dir.rglob("*"):
        try:
            stat = path.lstat()
        except OSError as exc:
            raise TaskImageBuildError(
                f"failed to inspect Docker build context file {path}: {exc}",
            ) from exc
        if path.is_dir() and not path.is_symlink():
            continue

        file_count += 1
        if file_count > max_files:
            raise TaskImageBuildError(
                "Docker build context exceeds operator file limit "
                f"({file_count}>{max_files}) under {task_dir}",
            )

        byte_count += stat.st_size
        if byte_count > max_bytes:
            raise TaskImageBuildError(
                "Docker build context exceeds operator byte limit "
                f"({byte_count}>{max_bytes}) under {task_dir}",
            )


def _operator_limit(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise TaskImageBuildError(
            f"{name} must be a positive integer; got {raw!r}",
        ) from exc
    if value <= 0:
        raise TaskImageBuildError(
            f"{name} must be a positive integer; got {raw!r}",
        )
    return value
