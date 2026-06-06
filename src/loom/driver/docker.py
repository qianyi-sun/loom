"""DockerDriver — production Driver using docker-py.

The container is created from `image` with no command (Loom holds it alive via
a sleep-infinity entrypoint override). All workloads run via `docker exec`.
Network policy switches happen via iptables rules applied inside the container
(see `loom.driver.network_policy`).

Spec §2.2 (Driver contract), §2.3 (Capabilities), §5.1 (timeouts).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import docker
from docker.errors import APIError, ImageNotFound, NotFound

from loom.driver.base import StartOptions
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy, Public
from loom.models.types import OS

logger = logging.getLogger(__name__)

_KEEPALIVE_CMD = ["sh", "-c", "exec sleep infinity"]


def _default_caps() -> Capabilities:
    return Capabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public", "no-network", "allowlist"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto", "limit", "guarantee"]),
    )


@dataclass
class DockerDriver:
    """Real-Docker Driver. Uses docker-py SDK throughout."""

    image: str
    workspace: PurePosixPath = field(default_factory=lambda: PurePosixPath("/workspace"))
    container_name: str | None = None
    capabilities: Capabilities = field(default_factory=_default_caps)
    os: OS = "linux"
    network_policy_baseline: NetworkPolicy = field(default_factory=Public)
    _client: Any | None = field(default=None, init=False, repr=False)
    _container: Any | None = field(default=None, init=False, repr=False)
    _state: str = field(default="constructed", init=False)

    async def start(self, *, options: StartOptions | None = None) -> None:
        # Spec §2.2: start() at most once. Reject when running OR stopped.
        if self._state != "constructed":
            raise DriverAlreadyStartedError(
                f"DockerDriver.start() rejected in state {self._state!r}",
            )

        opts = options or StartOptions()
        self._client = docker.from_env()

        await asyncio.to_thread(self._ensure_image, opts)

        self._container = await asyncio.to_thread(
            self._client.containers.run,
            self.image,
            command=_KEEPALIVE_CMD,
            name=self.container_name,
            detach=True,
            tty=False,
            stdin_open=False,
            working_dir=str(self.workspace),
            remove=False,
        )
        await self._wait_until_running()
        # Mark running BEFORE applying baseline policy — set_network_policy()
        # calls _require_running() which checks self._state.
        self._state = "running"
        try:
            await self.set_network_policy(self.network_policy_baseline)
        except Exception:
            # Roll back state so stop() can clean up.
            self._state = "stopped"
            raise

    def _ensure_image(self, opts: StartOptions) -> None:
        assert self._client is not None
        try:
            self._client.images.get(self.image)
            if not opts.force_build:
                return
        except ImageNotFound:
            pass
        if opts.pull:
            self._client.images.pull(self.image)

    async def _wait_until_running(self, timeout_sec: float = 10.0) -> None:
        assert self._container is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_sec
        while loop.time() < deadline:
            await asyncio.to_thread(self._container.reload)
            if self._container.status == "running":
                return
            await asyncio.sleep(0.1)
        raise DriverError(
            f"Container failed to reach 'running' within {timeout_sec}s; "
            f"final status={self._container.status!r}",
        )

    async def stop(self, *, delete: bool = True) -> None:
        # Idempotent. Only running→stopped is a real transition; calling stop()
        # from 'constructed' leaves state intact so start() can still fire.
        if self._container is not None:
            with contextlib.suppress(APIError, NotFound):
                await asyncio.to_thread(self._container.stop, timeout=10)
                if delete:
                    await asyncio.to_thread(self._container.remove, force=True)
            self._container = None
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None
        if self._state == "running":
            self._state = "stopped"

    def _require_running(self) -> None:
        if self._state != "running" or self._container is None:
            raise DriverNotStartedError(
                f"DockerDriver in state {self._state!r}",
            )

    async def exec(  # Task 9 implements
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        self._require_running()
        raise NotImplementedError("exec lands in Task 9")

    async def upload(self, src: Path, dst: PurePosixPath) -> None:  # Task 10
        self._require_running()
        raise NotImplementedError("upload lands in Task 10")

    async def download(self, src: PurePosixPath, dst: Path) -> None:  # Task 10
        self._require_running()
        raise NotImplementedError("download lands in Task 10")

    async def set_network_policy(self, policy: NetworkPolicy) -> None:  # Task 13
        self._require_running()
        # Placeholder no-op so start() can apply the baseline. Task 13 wires
        # in real iptables enforcement.
        return

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:  # Task 11
        self._require_running()
        raise NotImplementedError("healthcheck lands in Task 11")
