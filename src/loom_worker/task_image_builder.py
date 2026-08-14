"""Exclusive task-image builder worker.

Run with ``python -m loom_worker.task_image_builder`` inside an exclusive
Slurm allocation. It consumes architecture-specific materialization leases,
builds every Dockerfile-backed component, and publishes immutable registry
digests before marking the prerequisite ready.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import platform
import shutil
import socket
import time
from collections.abc import Callable
from typing import Any, Protocol

from loom.driver.task_image import (
    TaskImageBuildError,
    publish_local_image_to_registry,
    resolve_task_image,
)
from loom.models.task import TaskConfig
from loom.task_image_materialization import required_task_image_architectures
from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import HttpControlPlaneClient, TaskImageBuildClaim
from loom_worker.task_sidecars import build_task_sidecar_images
from loom_worker.trial_cache import evict_stale_managed_images_from_env

logger = logging.getLogger(__name__)


class TaskImageBuilderControlPlane(Protocol):
    async def start_task_image_materialization(self, **kwargs: Any) -> bool: ...

    async def heartbeat_task_image_materialization(self, **kwargs: Any) -> bool: ...

    async def complete_task_image_materialization(self, **kwargs: Any) -> bool: ...

    async def fail_task_image_materialization(self, **kwargs: Any) -> bool: ...


class TaskImageLeaseLostError(RuntimeError):
    pass


def sha256_of_dir(directory) -> str:  # type: ignore[no-untyped-def]
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_dir():
            continue
        relative = path.relative_to(directory).as_posix().encode("utf-8")
        digest.update(b"\x00" + relative + b"\x00")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _build_worker_object_store(settings: WorkerSettings):  # type: ignore[no-untyped-def]
    from loom_worker.main_loop import _build_worker_object_store as build

    return build(settings)


async def _materialize_task_dir(**kwargs: Any):  # type: ignore[no-untyped-def]
    from loom_worker.main_loop import _materialize_task_dir as materialize

    return await materialize(**kwargs)


def host_cpu_arch() -> str:
    machine = platform.machine().lower()
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    if machine in {"amd64", "x86_64"}:
        return "x86_64"
    raise RuntimeError(f"unsupported builder CPU architecture {machine!r}")


async def materialize_and_publish_task_images(
    claim: TaskImageBuildClaim,
    settings: WorkerSettings,
) -> dict[str, str]:
    native_arch = host_cpu_arch()
    if claim.cpu_arch != native_arch:
        raise RuntimeError(
            "task image claim does not match the builder's native architecture "
            f"(claim={claim.cpu_arch}, builder={native_arch})"
        )
    task_config = TaskConfig.model_validate(claim.task_config)
    if claim.cpu_arch not in required_task_image_architectures(task_config):
        raise RuntimeError("task image claim architecture is not required by its task snapshot")
    registry_repo = settings.trial_cache_registry_repo.strip()
    if not registry_repo:
        raise RuntimeError("task image builder requires a configured registry repository")

    task_dir = await _materialize_task_dir(
        bundle={
            "source": claim.task_source,
            "checksum": claim.task_checksum,
        },
        object_store=_build_worker_object_store(settings),
        trial_id=claim.id,
        fixtures_root=settings.fixtures_root,
        benchmark_cache=settings.benchmark_cache,
        timeout_sec=settings.task_materialize_timeout_sec,
    )
    try:
        actual_checksum = sha256_of_dir(task_dir)
        if actual_checksum != claim.task_checksum:
            raise TaskImageBuildError(
                "materialized task bundle checksum mismatch "
                f"expected={claim.task_checksum} actual={actual_checksum}"
            )

        local_images: dict[str, str] = {}
        if task_config.environment.dockerfile is not None:
            local_images["task"] = await resolve_task_image(
                task_config=task_config,
                task_dir=task_dir,
                task_checksum=claim.task_checksum,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
                require_containment=False,
                cpu_arch=claim.cpu_arch,
                build_if_missing=True,
            )
        sidecars = await build_task_sidecar_images(
            task_config=task_config,
            task_dir=task_dir,
            task_checksum=claim.task_checksum,
            cpu_arch=claim.cpu_arch,
            docker_api_timeout_sec=settings.docker_api_timeout_sec,
        )
        local_images.update({f"sidecar:{name}": image for name, image in sidecars.items()})
        if not local_images:
            raise RuntimeError("task image claim contains no Dockerfile-backed components")

        published: dict[str, str] = {}
        for component, tag in local_images.items():
            published[component] = await publish_local_image_to_registry(
                tag=tag,
                registry_repo=registry_repo,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
            )
        return published
    finally:
        shutil.rmtree(task_dir)


async def _heartbeat_lease(
    control_plane: TaskImageBuilderControlPlane,
    *,
    claim: TaskImageBuildClaim,
    builder_id: str,
    interval_seconds: float,
    stop: asyncio.Event,
    lease_lost: asyncio.Event,
) -> None:
    while True:
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            return
        except TimeoutError:
            pass
        try:
            refreshed = await control_plane.heartbeat_task_image_materialization(
                materialization_id=claim.id,
                builder_id=builder_id,
                lease_epoch=claim.lease_epoch,
            )
        except Exception:
            logger.warning("task image lease heartbeat failed id=%s", claim.id, exc_info=True)
            lease_lost.set()
            return
        if not refreshed:
            lease_lost.set()
            return


def _retryable_failure(exc: BaseException) -> bool:
    message = str(exc).lower()
    return isinstance(exc, TimeoutError) or any(
        marker in message
        for marker in ("registry", "connection", "temporar", "timeout", "unavailable")
    )


async def process_task_image_claim(
    control_plane: TaskImageBuilderControlPlane,
    *,
    claim: TaskImageBuildClaim,
    builder_id: str,
    settings: WorkerSettings,
) -> None:
    started = await control_plane.start_task_image_materialization(
        materialization_id=claim.id,
        builder_id=builder_id,
        lease_epoch=claim.lease_epoch,
    )
    if not started:
        raise TaskImageLeaseLostError("task image lease was lost before build start")

    stop = asyncio.Event()
    lease_lost = asyncio.Event()
    interval = max(0.01, float(settings.heartbeat_interval_sec))
    heartbeat = asyncio.create_task(
        _heartbeat_lease(
            control_plane,
            claim=claim,
            builder_id=builder_id,
            interval_seconds=interval,
            stop=stop,
            lease_lost=lease_lost,
        )
    )
    try:
        registry_images = await materialize_and_publish_task_images(claim, settings)
        if lease_lost.is_set():
            raise TaskImageLeaseLostError("task image lease was lost during build")
        completed = await control_plane.complete_task_image_materialization(
            materialization_id=claim.id,
            builder_id=builder_id,
            lease_epoch=claim.lease_epoch,
            registry_images=registry_images,
        )
        if not completed:
            raise TaskImageLeaseLostError("task image lease was lost before completion")
    except TaskImageLeaseLostError:
        raise
    except Exception as exc:
        reported = await control_plane.fail_task_image_materialization(
            materialization_id=claim.id,
            builder_id=builder_id,
            lease_epoch=claim.lease_epoch,
            retryable=_retryable_failure(exc),
            failure_reason="task_image_build_failed",
            failure_message=str(exc)[:4000] or type(exc).__name__,
        )
        if not reported:
            raise TaskImageLeaseLostError(
                "task image lease was lost while reporting failure"
            ) from exc
        logger.exception("task image materialization failed id=%s", claim.id)
    finally:
        stop.set()
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat


async def run_builder(
    settings: WorkerSettings,
    *,
    now: Callable[[], float] = time.monotonic,
) -> None:
    native_arch = host_cpu_arch()
    await asyncio.to_thread(evict_stale_managed_images_from_env, settings)
    builder_id = f"{socket.gethostname()}:{os.getpid()}"[:128]
    control_plane = HttpControlPlaneClient(
        base_url=str(settings.control_plane_url),
        token=settings.token.get_secret_value(),
        timeout_sec=max(30.0, float(settings.docker_api_timeout_sec)),
    )
    idle_since = now()
    while True:
        claim = await control_plane.claim_task_image_materialization(
            builder_id=builder_id,
            cpu_arch=native_arch,  # type: ignore[arg-type]
        )
        if claim is None:
            idle_limit = settings.task_image_builder_idle_exit_seconds
            if idle_limit is not None and now() - idle_since >= idle_limit:
                return
            await asyncio.sleep(settings.claim_poll_interval_sec)
            continue
        idle_since = now()
        try:
            await process_task_image_claim(
                control_plane,
                claim=claim,
                builder_id=builder_id,
                settings=settings,
            )
        except TaskImageLeaseLostError:
            logger.warning("task image lease lost id=%s", claim.id)
        finally:
            await asyncio.to_thread(evict_stale_managed_images_from_env, settings)


def main() -> None:
    settings = WorkerSettings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper()))
    asyncio.run(run_builder(settings))


if __name__ == "__main__":
    main()
