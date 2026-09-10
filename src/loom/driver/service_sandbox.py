"""Driver for an already scheduled, isolated Linux task or verifier container.

Only a dedicated Unix socket is exposed to the trusted agent. Kubernetes owns
container lifetime, resources and network policy; this adapter cannot change
them. It reuses the ordinary Harbor environment and workspace snapshot flow.
"""

from __future__ import annotations

import asyncio
import base64
import os
import shlex
import stat
import tempfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import httpx

from loom.driver.base import DriverResourceSnapshot, ExecHandle, StartOptions
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy
from loom.models.types import OS


class ServiceSandboxDriver:
    """Connect to one native sidecar; never create Pods or host containers."""

    os: OS = "linux"

    def __init__(
        self,
        socket_path: Path,
        *,
        capabilities: Capabilities,
        network_policy: NetworkPolicy,
        max_transfer_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        if max_transfer_bytes <= 0:
            raise ValueError("positive max_transfer_bytes required")
        self.capabilities = capabilities
        self._network_policy = network_policy
        self._max_transfer = max_transfer_bytes
        self._socket_path = socket_path
        self._client: httpx.AsyncClient | None = None
        self._started = False
        self._requests: set[asyncio.Task[Any]] = set()

    async def start(self, *, options: StartOptions | None = None) -> None:
        if self._started:
            raise DriverAlreadyStartedError("sandbox driver already started")
        if options is not None and options != StartOptions():
            raise DriverError("native sandbox options must be enforced by its Pod")
        client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(self._socket_path)),
            base_url="http://sandbox",
            timeout=10,
            trust_env=False,
        )
        try:
            response = await client.get("/health")
            response.raise_for_status()
            if response.json() != {"ready": True}:
                raise DriverError("sandbox readiness response invalid")
        except BaseException:
            await client.aclose()
            raise
        self._client = client
        self._started = True

    def _running_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise DriverNotStartedError("sandbox driver is not running")
        return self._client

    async def stop(self, *, delete: bool = True) -> None:
        client, self._client = self._client, None
        current = asyncio.current_task()
        pending = [task for task in self._requests if task is not current]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if client is not None:
            await client.aclose()

    async def resource_snapshot(self) -> DriverResourceSnapshot | None:
        return None  # The existing Pod collector owns resource accounting.

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        client = self._running_client()
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        try:
            response = await client.request(method, path, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPError as exc:
            # Do not attach request bodies (commands/env may contain secrets).
            raise DriverError("sandbox RPC failed") from exc
        finally:
            self._requests.discard(task)

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        response = await self._request(
            "POST",
            "/exec",
            json={
                "argv": ["/bin/sh", "-c", cmd],
                "user": str(user) if user is not None else None,
                "cwd": str(cwd) if cwd is not None else "",
                "env": dict(env or {}),
                "timeout_sec": timeout_sec or 0,
            },
            timeout=None,  # The server bounds execution and kills its process group.
        )
        result = response.json()
        for key in ("stdout", "stderr"):
            result[key] = base64.b64decode(result[key] or "", validate=True)
        return ExecResult.model_validate(result)

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        self._running_client()
        raise DriverError("native sandbox streaming is not used by the Harbor bridge")

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self._running_client()
        fd = os.open(src, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > self._max_transfer:
                raise DriverError("upload requires a regular file within transfer limit")
            data = stream.read(self._max_transfer + 1)
            if len(data) > self._max_transfer:
                raise DriverError("upload exceeds transfer limit")
        await self._request(
            "PUT",
            "/file",
            params={"path": str(dst)},
            headers={"X-File-Mode": format(stat.S_IMODE(info.st_mode) & 0o777, "o")},
            content=data,
            timeout=120,
        )

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        client = self._running_client()
        # Download to a private temporary file and atomically publish only a
        # complete, bounded transfer. Existing output survives failed reads.
        if dst.is_symlink() or any(p.is_symlink() for p in dst.parents):
            raise DriverError("download destination must not traverse symlinks")
        dst.parent.mkdir(parents=True, exist_ok=True)
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        temporary: Path | None = None
        try:
            async with client.stream(
                "GET", "/file", params={"path": str(src)}, timeout=120
            ) as response:
                response.raise_for_status()
                if int(response.headers.get("Content-Length", "0")) > self._max_transfer:
                    raise DriverError("download exceeds transfer limit")
                with tempfile.NamedTemporaryFile(dir=dst.parent, delete=False) as output:
                    temporary = Path(output.name)
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self._max_transfer:
                            raise DriverError("download exceeds transfer limit")
                        output.write(chunk)
                temporary.replace(dst)
        except httpx.HTTPError as exc:
            raise DriverError("sandbox download failed") from exc
        finally:
            self._requests.discard(task)
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        if policy != self._network_policy:
            raise DriverError("native sandbox network policy is fixed by its Pod")

    async def stop_processes(self) -> None:
        """Stop task descendants before exporting the separate verifier snapshot."""
        await self._request("POST", "/stop-processes")

    async def export_workspace_archive(self, src: PurePosixPath, dst: Path) -> None:
        remote = PurePosixPath(f"/tmp/loom-workspace-{uuid4().hex}.tar")
        source, archive = shlex.quote(str(src)), shlex.quote(str(remote))
        special = await self.exec(
            f"find {source} \\( -type b -o -type c -o -type p -o -type s \\) -print -quit"
        )
        if special.return_code or special.stdout:
            raise DriverError("workspace has unsupported special files or cannot be inspected")
        try:
            result = await self.exec(f"tar -C {source} -cf {archive} .")
            if result.return_code or result.stderr:
                raise DriverError("unable to export a stable workspace archive")
            await self.download(remote, dst)
        finally:
            await self.exec(f"rm -f {archive}")

    async def import_workspace_archive(self, src: Path, dst: PurePosixPath) -> None:
        # workspace_snapshot validates/strips the archive in the trusted agent
        # before invoking this hook. The sandbox never chooses verifier inputs.
        remote = PurePosixPath(f"/tmp/loom-workspace-{uuid4().hex}.tar")
        destination, archive = shlex.quote(str(dst)), shlex.quote(str(remote))
        try:
            await self.upload(src, remote)
            result = await self.exec(
                f"mkdir -p {destination} && tar -C {destination} -xpf {archive}"
            )
            if result.return_code or result.stderr:
                raise DriverError("unable to restore workspace archive")
        finally:
            await self.exec(f"rm -f {archive}")

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        if hc is None:
            await self._request("GET", "/health")
            return
        await asyncio.sleep(hc.start_period_sec)
        for attempt in range(hc.retries + 1):
            result = await self.exec(hc.command, timeout_sec=hc.timeout_sec)
            if result.return_code == 0:
                return
            if attempt < hc.retries:
                await asyncio.sleep(hc.interval_sec)
        raise DriverError("sandbox healthcheck failed")
