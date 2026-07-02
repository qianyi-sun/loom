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
import platform
import tempfile
import threading
from pathlib import Path, PurePosixPath
from typing import Any

import docker
from docker.errors import BuildError, ImageNotFound

from loom.models.task import TaskConfig

DEFAULT_TASK_IMAGE = "alpine"
# Maximum trailing build-log lines (stdout+stderr from inside the
# Docker build) included in TaskImageBuildError messages when a
# Dockerfile build fails. Enough to show pip's actual error output
# while staying well under typical DB/JSON field limits.
_BUILD_LOG_TAIL_LINES = 40
DEFAULT_BUILD_CONTEXT_MAX_FILES = 2_000
DEFAULT_BUILD_CONTEXT_MAX_BYTES = 512 * 1024 * 1024
ENV_BUILD_CONTEXT_MAX_FILES = "LOOM_TASK_IMAGE_BUILD_MAX_FILES"
ENV_BUILD_CONTEXT_MAX_BYTES = "LOOM_TASK_IMAGE_BUILD_MAX_BYTES"
TERMINUS_2_FULL_IMAGE = "mictern2/terminus2-full:latest"
_TERMINUS_2_BASE_LOCK = threading.Lock()
_TERMINUS_2_ARM64_BASE_DOCKERFILE = """\
FROM python:3.13-slim

ENV DEBIAN_FRONTEND=noninteractive \\
    PIP_ROOT_USER_ACTION=ignore \\
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \\
    bash \\
    build-essential \\
    ca-certificates \\
    cmake \\
    coreutils \\
    curl \\
    findutils \\
    gawk \\
    git \\
    iproute2 \\
    iputils-ping \\
    jq \\
    libffi-dev \\
    libssl-dev \\
    net-tools \\
    nodejs \\
    npm \\
    openssh-client \\
    patch \\
    pkg-config \\
    procps \\
    python-is-python3 \\
    python3 \\
    python3-pip \\
    python3-venv \\
    rsync \\
    socat \\
    sudo \\
    tmux \\
    unzip \\
    xz-utils \\
    zip \\
    zlib1g-dev \\
  && rm -rf /var/lib/apt/lists/*
"""


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
    docker_api_timeout_sec: int | None = None,
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
                docker_api_timeout_sec=docker_api_timeout_sec,
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
    docker_api_timeout_sec: int | None = None,
) -> None:
    configured_dockerfile = task_config.environment.dockerfile
    assert configured_dockerfile is not None
    client: Any = (
        docker.from_env()
        if docker_api_timeout_sec is None
        else docker.from_env(timeout=docker_api_timeout_sec)
    )
    try:
        try:
            client.images.get(tag)
            return
        except ImageNotFound:
            pass

        rel_dockerfile = dockerfile.relative_to(build_context).as_posix()
        _enforce_build_context_limits(build_context)
        _ensure_terminus_2_arm64_base_if_needed(
            client=client,
            dockerfile=dockerfile,
        )
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
    except BuildError as exc:
        # docker-py's BuildError stringifies to only the failing RUN
        # command (e.g. "The command '/bin/sh -c pip install foo'
        # returned a non-zero code: 1") — useless for diagnosing WHY
        # the command failed. Walk the build_log iterator and surface
        # the tail of the captured stdout/stderr so operators can see
        # pip's actual error output (e.g. "ERROR: No matching
        # distribution found for pytest-jsonreport"). #319.
        tail = _format_build_log_tail(exc.build_log)
        raise TaskImageBuildError(
            f"failed to build Docker image {tag!r} from "
            f"{configured_dockerfile.as_posix()!r}: {exc}"
            + (f"\nbuild log (last {_BUILD_LOG_TAIL_LINES} lines):\n{tail}"
               if tail else ""),
        ) from exc
    except Exception as exc:
        raise TaskImageBuildError(
            f"failed to build Docker image {tag!r} from "
            f"{configured_dockerfile.as_posix()!r}: {exc}",
        ) from exc
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _ensure_terminus_2_arm64_base_if_needed(
    *,
    client: Any,
    dockerfile: Path,
) -> None:
    if not _dockerfile_uses_base_image(dockerfile, TERMINUS_2_FULL_IMAGE):
        return
    if not _is_linux_arm64_docker_daemon(client):
        return

    with _TERMINUS_2_BASE_LOCK:
        try:
            existing_base = client.images.get(TERMINUS_2_FULL_IMAGE)
            if _is_arm64_docker_image(existing_base):
                return
        except ImageNotFound:
            pass

        try:
            with tempfile.TemporaryDirectory(
                prefix="loom-terminus-2-arm64-base-",
            ) as context_dir:
                dockerfile_path = Path(context_dir) / "Dockerfile"
                dockerfile_path.write_text(
                    _TERMINUS_2_ARM64_BASE_DOCKERFILE,
                    encoding="utf-8",
                )
                client.images.build(
                    path=context_dir,
                    dockerfile="Dockerfile",
                    tag=TERMINUS_2_FULL_IMAGE,
                    rm=True,
                    forcerm=True,
                    pull=False,
                    labels={
                        "loom.managed_base": "terminus-2-arm64",
                        "loom.managed_base.upstream": TERMINUS_2_FULL_IMAGE,
                    },
                )
        except BuildError as exc:
            tail = _format_build_log_tail(exc.build_log)
            raise TaskImageBuildError(
                "failed to build managed arm64 Terminus 2 base image "
                f"{TERMINUS_2_FULL_IMAGE!r}: {exc}"
                + (
                    f"\nbuild log (last {_BUILD_LOG_TAIL_LINES} lines):\n{tail}"
                    if tail else ""
                ),
            ) from exc
        except Exception as exc:
            raise TaskImageBuildError(
                "failed to build managed arm64 Terminus 2 base image "
                f"{TERMINUS_2_FULL_IMAGE!r}: {exc}",
            ) from exc


def _is_linux_arm64_docker_daemon(client: Any) -> bool:
    with contextlib.suppress(Exception):
        info = client.info()
        if isinstance(info, dict):
            os_type = str(info.get("OSType") or "").lower()
            architecture = str(info.get("Architecture") or "").lower()
            if os_type and os_type != "linux":
                return False
            if architecture in {"aarch64", "arm64"}:
                return True
            if architecture:
                return False
    return _is_linux_arm64_worker()


def _is_linux_arm64_worker() -> bool:
    return platform.system().lower() == "linux" and platform.machine().lower() in {
        "aarch64",
        "arm64",
    }


def _is_arm64_docker_image(image: Any) -> bool:
    attrs = getattr(image, "attrs", None)
    if not isinstance(attrs, dict):
        return False
    architecture = str(attrs.get("Architecture") or "").lower()
    return architecture in {"aarch64", "arm64"}


def _dockerfile_uses_base_image(dockerfile: Path, image: str) -> bool:
    normalized_image = _normalize_docker_image_name(image)
    try:
        lines = dockerfile.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise TaskImageBuildError(
            f"failed to read Dockerfile {dockerfile}: {exc}",
        ) from exc

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split()
        if not parts:
            continue
        instruction = parts[0].lower()
        if instruction == "arg":
            continue
        if instruction != "from":
            continue
        from_args = [part for part in parts[1:] if not part.startswith("--")]
        if not from_args:
            return False
        return _normalize_docker_image_name(from_args[0]) == normalized_image
    return False


def _normalize_docker_image_name(image: str) -> str:
    if image.startswith("docker.io/"):
        return image.removeprefix("docker.io/")
    if image.startswith("registry-1.docker.io/"):
        return image.removeprefix("registry-1.docker.io/")
    return image


def _format_build_log_tail(build_log: Any) -> str:
    """Walk docker-py's build_log iterator and return the last
    `_BUILD_LOG_TAIL_LINES` lines of stdout/stderr from the build,
    joined with newlines. Returns empty string if the log is missing
    or empty (e.g. the build never started — caller's outer message
    is enough on its own)."""
    if build_log is None:
        return ""
    lines: list[str] = []
    try:
        for chunk in build_log:
            # Each chunk is a dict like {"stream": "Step 2/4 : RUN ...\n"}
            # or {"error": "..."} or {"errorDetail": {...}}.
            if isinstance(chunk, dict):
                text = chunk.get("stream") or chunk.get("error") or ""
            else:
                text = str(chunk)
            if not text:
                continue
            # Split multi-line chunks so the tail-N is line-accurate,
            # not chunk-accurate.
            for line in text.splitlines():
                stripped = line.rstrip()
                if stripped:
                    lines.append(stripped)
    except Exception:
        # build_log iteration is best-effort. If iterating itself
        # raises (rare — docker-py may surface a partial stream), we
        # still want the outer error message to surface.
        pass
    if not lines:
        return ""
    return "\n".join(lines[-_BUILD_LOG_TAIL_LINES:])


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
