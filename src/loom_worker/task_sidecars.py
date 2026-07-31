"""Service-mode sidecar containers for task bundles.

Some benchmarks, including Terminal-Bench-2, define a primary agent
container plus auxiliary services. Loom keeps the primary container on the
normal Driver path and starts these sidecars beside it on the same per-trial
Docker network.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import docker
from docker.errors import ImageNotFound

from loom.models.healthcheck import HealthcheckSpec
from loom.models.task import TaskConfig, TaskSidecarConfig
from loom_worker.task_image import (
    TaskImageBuildError,
    _enforce_build_context_limits,
    _resolve_build_context_path,
    _resolve_dockerfile_path,
)

_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]")
SetupSlotProvider = Callable[[], contextlib.AbstractAsyncContextManager[Any]]


def task_sidecar_image_tag(
    task_config: TaskConfig,
    sidecar: TaskSidecarConfig,
    *,
    task_checksum: str,
) -> str:
    dockerfile = sidecar.dockerfile
    dockerfile_text = dockerfile.as_posix() if dockerfile is not None else ""
    build_context = sidecar.docker_build_context
    build_context_text = build_context.as_posix() if build_context is not None else ""
    material = "\n".join(
        [
            task_config.task.id,
            task_checksum,
            sidecar.name,
            dockerfile_text,
            build_context_text,
        ]
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
    return f"loom-sidecar:{digest}"


class DockerTaskSidecarRuntime:
    def __init__(
        self,
        *,
        task_config: TaskConfig,
        task_dir: Path,
        task_checksum: str,
        trial_id: UUID,
        health_poll_interval_sec: float = 0.5,
        docker_api_timeout_sec: int | None = None,
        setup_slot_provider: SetupSlotProvider | None = None,
        container_cpus: float = 0.0,
        container_memory_mib: int = 0,
        container_pids: int = 0,
        runtime_identity_labels: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self.task_config = task_config
        self.task_dir = task_dir
        self.task_checksum = task_checksum
        self.trial_id = trial_id
        self.health_poll_interval_sec = health_poll_interval_sec
        self.docker_api_timeout_sec = docker_api_timeout_sec
        self.setup_slot_provider = setup_slot_provider
        # #896: per-container hard caps for setup-sidecar containers on
        # non-exclusive (packed) workers. Slurm admission requires positive caps.
        self.container_cpus = container_cpus
        self.container_memory_mib = container_memory_mib
        self.container_pids = container_pids
        self.runtime_identity_labels = runtime_identity_labels
        self._client: Any | None = None
        self._containers: list[Any] = []
        self._network: Any | None = None
        self._network_name: str | None = None

    async def start(self, network_name: str | None = None) -> str:
        self._client = (
            docker.from_env()
            if self.docker_api_timeout_sec is None
            else docker.from_env(timeout=self.docker_api_timeout_sec)
        )
        try:
            if network_name is None:
                network_name = f"loom-sidecars-{self.trial_id}"
                client = self._client
                self._network = await asyncio.to_thread(
                    client.networks.create,
                    network_name,
                    driver="bridge",
                )
            self._network_name = network_name
            for sidecar in self._ordered_sidecars():
                image = await self._resolve_sidecar_image(sidecar)
                container = await asyncio.to_thread(
                    self._run_sidecar,
                    sidecar,
                    image,
                    network_name,
                )
                self._containers.append(container)
                if sidecar.healthcheck is not None:
                    await self._wait_for_healthy(container, sidecar.healthcheck)
            return network_name
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        for container in reversed(self._containers):
            with contextlib.suppress(Exception):
                await asyncio.to_thread(container.remove, force=True)
        self._containers.clear()
        if self._network is not None:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self._network.remove)
            self._network = None
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None

    def _ordered_sidecars(self) -> list[TaskSidecarConfig]:
        sidecars = {s.name: s for s in self.task_config.environment.sidecars}
        ordered: list[TaskSidecarConfig] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            if name in visiting:
                raise TaskImageBuildError(
                    f"sidecar dependency cycle includes {name!r}",
                )
            visiting.add(name)
            sidecar = sidecars[name]
            for dep in sidecar.depends_on:
                if dep in sidecars:
                    visit(dep)
            visiting.remove(name)
            visited.add(name)
            ordered.append(sidecar)

        for name in sidecars:
            visit(name)
        return ordered

    async def _resolve_sidecar_image(self, sidecar: TaskSidecarConfig) -> str:
        if sidecar.docker_image:
            if await asyncio.to_thread(self._image_exists, sidecar.docker_image):
                return sidecar.docker_image
            async with self._setup_slot():
                await asyncio.to_thread(self._ensure_pulled_image, sidecar.docker_image)
            return sidecar.docker_image
        if sidecar.dockerfile is None:
            raise TaskImageBuildError(
                f"sidecar {sidecar.name!r} declares neither docker_image nor dockerfile",
            )
        dockerfile = _resolve_dockerfile_path(
            task_dir=self.task_dir,
            dockerfile=sidecar.dockerfile,
        )
        build_context = _resolve_build_context_path(
            task_dir=self.task_dir,
            dockerfile=dockerfile,
            docker_build_context=sidecar.docker_build_context,
        )
        tag = task_sidecar_image_tag(
            self.task_config,
            sidecar,
            task_checksum=self.task_checksum,
        )
        if await asyncio.to_thread(self._image_exists, tag):
            return tag
        async with self._setup_slot():
            await asyncio.to_thread(
                self._ensure_built_image,
                tag=tag,
                sidecar=sidecar,
                dockerfile=dockerfile,
                build_context=build_context,
            )
        return tag

    def _setup_slot(self) -> contextlib.AbstractAsyncContextManager[Any]:
        if self.setup_slot_provider is None:
            return contextlib.nullcontext()
        return self.setup_slot_provider()

    def _image_exists(self, image: str) -> bool:
        assert self._client is not None
        try:
            self._client.images.get(image)
        except ImageNotFound:
            return False
        return True

    def _ensure_pulled_image(self, image: str) -> None:
        assert self._client is not None
        try:
            self._client.images.get(image)
        except ImageNotFound:
            self._client.images.pull(image)

    def _ensure_built_image(
        self,
        *,
        tag: str,
        sidecar: TaskSidecarConfig,
        dockerfile: Path,
        build_context: Path,
    ) -> None:
        assert self._client is not None
        try:
            self._client.images.get(tag)
            return
        except ImageNotFound:
            pass
        rel_dockerfile = dockerfile.relative_to(build_context).as_posix()
        _enforce_build_context_limits(build_context)
        self._client.images.build(
            path=str(build_context),
            dockerfile=rel_dockerfile,
            tag=tag,
            rm=True,
            forcerm=True,
            pull=False,
            labels={
                "loom.setup-build": "true",
                "loom.setup-kind": "task-sidecar",
                "loom.task_id": self.task_config.task.id,
                "loom.task_checksum": self.task_checksum,
                "loom.task_sidecar": sidecar.name,
                "loom.task_dockerfile": rel_dockerfile,
            },
        )

    def _run_sidecar(
        self,
        sidecar: TaskSidecarConfig,
        image: str,
        network_name: str,
    ) -> Any:
        assert self._client is not None
        kwargs: dict[str, Any] = {
            "name": _container_name(self.trial_id, sidecar.name),
            "detach": True,
            "tty": False,
            "stdin_open": False,
            "network": network_name,
            "networking_config": {
                network_name: self._client.api.create_endpoint_config(
                    aliases=[sidecar.name],
                ),
            },
            "labels": {
                **dict(self.runtime_identity_labels),
                "loom.setup-container": "true",
                "loom.task-sidecar": "true",
                "loom.task_id": self.task_config.task.id,
                "loom.task_sidecar": sidecar.name,
                "loom.trial_id": str(self.trial_id),
            },
            "remove": False,
        }
        if sidecar.command is not None:
            kwargs["command"] = sidecar.command
        if sidecar.environment:
            kwargs["environment"] = dict(sidecar.environment)
        if sidecar.hostname:
            kwargs["hostname"] = sidecar.hostname
        healthcheck = _docker_healthcheck(sidecar.healthcheck)
        if healthcheck is not None:
            kwargs["healthcheck"] = healthcheck
        # #896: apply per-container hard caps when configured (>0); unset
        # (0) remains available to non-Slurm callers; Slurm admission rejects it.
        if self.container_cpus > 0:
            kwargs["nano_cpus"] = int(self.container_cpus * 1_000_000_000)
        if self.container_memory_mib > 0:
            kwargs["mem_limit"] = self.container_memory_mib * 1024 * 1024
        if self.container_pids > 0:
            kwargs["pids_limit"] = self.container_pids
        create_kwargs = dict(kwargs)
        create_kwargs.pop("remove", None)
        container = self._client.containers.create(image, **create_kwargs)
        try:
            container.start()
        except BaseException:
            with contextlib.suppress(Exception):
                container.remove(force=True)
            raise
        return container

    async def _wait_for_healthy(
        self,
        container: Any,
        healthcheck: HealthcheckSpec,
    ) -> None:
        loop = asyncio.get_running_loop()
        probe_attempts = max(healthcheck.retries, 1)
        deadline = loop.time() + max(
            healthcheck.start_period_sec
            + healthcheck.interval_sec * probe_attempts
            + healthcheck.timeout_sec
            + self.health_poll_interval_sec,
            healthcheck.timeout_sec,
            1.0,
        )
        while loop.time() <= deadline:
            await asyncio.to_thread(container.reload)
            status = (
                getattr(container, "attrs", {}).get("State", {}).get("Health", {}).get("Status")
            )
            if status == "healthy":
                return
            if status == "unhealthy":
                raise TaskImageBuildError(
                    f"sidecar {getattr(container, 'name', '<unknown>')} became unhealthy",
                )
            await asyncio.sleep(self.health_poll_interval_sec)
        raise TaskImageBuildError(
            f"sidecar {getattr(container, 'name', '<unknown>')} did not "
            "become healthy before timeout",
        )


def _container_name(trial_id: UUID, sidecar_name: str) -> str:
    safe = _NAME_RE.sub("-", sidecar_name).strip("-._") or "sidecar"
    return f"loom-sidecar-{trial_id}-{safe}"


def _docker_healthcheck(hc: HealthcheckSpec | None) -> dict[str, Any] | None:
    if hc is None:
        return None
    return {
        "test": ["CMD-SHELL", hc.command],
        "interval": int(hc.interval_sec * 1_000_000_000),
        "timeout": int(hc.timeout_sec * 1_000_000_000),
        "retries": hc.retries,
        "start_period": int(hc.start_period_sec * 1_000_000_000),
    }
