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
import io
import logging
import tarfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import docker
from docker.errors import APIError, ImageNotFound, NotFound

from loom.driver.base import MAX_EXEC_STREAM_BYTES, StartOptions
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

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        self._require_running()
        assert self._container is not None

        exec_kwargs: dict[str, Any] = {
            "cmd": ["/bin/sh", "-c", cmd],
            "stdout": True,
            "stderr": True,
            "tty": False,
            "detach": False,
            "stream": False,
            "demux": True,
        }
        if user is not None:
            exec_kwargs["user"] = str(user)
        if cwd is not None:
            exec_kwargs["workdir"] = str(cwd)
        if env is not None:
            exec_kwargs["environment"] = dict(env)

        loop = asyncio.get_running_loop()
        started = loop.time()

        def _sync() -> tuple[int, bytes, bytes]:
            assert self._container is not None
            result = self._container.exec_run(**exec_kwargs)
            output = result.output
            if isinstance(output, tuple):
                stdout, stderr = output
            else:
                stdout, stderr = output, b""
            return int(result.exit_code), stdout or b"", stderr or b""

        if timeout_sec is not None:
            exit_code, stdout, stderr = await asyncio.wait_for(
                asyncio.to_thread(_sync), timeout=timeout_sec,
            )
        else:
            exit_code, stdout, stderr = await asyncio.to_thread(_sync)

        duration = loop.time() - started

        truncated = False
        if len(stdout) > MAX_EXEC_STREAM_BYTES:
            stdout = stdout[:MAX_EXEC_STREAM_BYTES]
            truncated = True
        if len(stderr) > MAX_EXEC_STREAM_BYTES:
            stderr = stderr[:MAX_EXEC_STREAM_BYTES]
            truncated = True

        return ExecResult(
            return_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            truncated=truncated,
            duration_sec=duration,
        )

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self._require_running()
        assert self._container is not None
        if not src.is_file():
            raise FileNotFoundError(f"upload source {src} is not a regular file")

        def _build_tar() -> bytes:
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tf:
                info = tarfile.TarInfo(name=dst.name)
                data = src.read_bytes()
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            return buf.getvalue()

        tar_bytes = await asyncio.to_thread(_build_tar)

        # Ensure parent dir exists in the container.
        await self.exec(f"mkdir -p {dst.parent.as_posix()}", user="root")

        def _put() -> None:
            assert self._container is not None
            ok = self._container.put_archive(path=str(dst.parent), data=tar_bytes)
            if not ok:
                raise DriverError(f"put_archive returned False for {dst}")

        await asyncio.to_thread(_put)

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        self._require_running()
        assert self._container is not None

        def _get() -> bytes:
            assert self._container is not None
            try:
                stream, _stat = self._container.get_archive(str(src))
            except NotFound as exc:
                raise FileNotFoundError(f"{src} not found in container") from exc
            return b"".join(stream)

        data = await asyncio.to_thread(_get)

        def _extract() -> None:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
                members = tf.getmembers()
                if not members:
                    raise DriverError(f"empty tarball returned for {src}")
                fobj = tf.extractfile(members[0])
                if fobj is None:
                    raise DriverError(f"could not extract {src} from tarball")
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(fobj.read())

        await asyncio.to_thread(_extract)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:  # Task 13
        self._require_running()
        # Placeholder no-op so start() can apply the baseline. Task 13 wires
        # in real iptables enforcement.
        return

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:  # Task 11
        self._require_running()
        raise NotImplementedError("healthcheck lands in Task 11")
