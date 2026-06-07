"""Driver Protocol — what every sandbox backend implements (spec §2.2).

The Protocol is intentionally minimal: 7 async methods + 2 declared attributes.
New backends target ≤300 LOC; FakeDriver and DockerDriver are reference impls.

Constants:
- MAX_EXEC_STREAM_BYTES : hard cap on stdout/stderr per `exec()` call (10 MB).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol, runtime_checkable

from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy
from loom.models.types import OS

MAX_EXEC_STREAM_BYTES: int = 10 * 1024 * 1024  # 10 MB; spec §2.2 + §4.9


@dataclass(frozen=True)
class StartOptions:
    """Options for `Driver.start()`."""

    force_build: bool = False
    pull: bool = True


@dataclass
class ExecHandle:
    """Long-running process handle returned by Driver.exec_streaming.

    Caller iterates stdout/stderr (async; chunks of any size), then
    `await handle.wait()` for the exit code. Driver implementations
    buffer nothing — chunks flow through. No 10 MB cap.
    """

    pid: int
    stdout: AsyncIterator[bytes]
    stderr: AsyncIterator[bytes]
    _wait: Callable[[], Awaitable[int]]
    _kill: Callable[[], Awaitable[None]]

    async def wait(self) -> int:
        return await self._wait()

    async def kill(self) -> None:
        await self._kill()


@runtime_checkable
class Driver(Protocol):
    """Sandbox execution surface. See spec §2.2.

    Lifecycle contract (spec §2.2 addendum):
    - `start()` may be called at most once per instance. Re-`start()` raises
      `DriverAlreadyStartedError`.
    - `stop()` MUST be idempotent and safe to call:
      (a) before `start()` ever succeeded, (b) after `start()` succeeded,
      (c) multiple times.
    - `exec`/`upload`/`download` MUST raise `DriverNotStartedError` if called
      before `start()` or after `stop()`.
    """

    capabilities: Capabilities
    os: OS

    async def start(self, *, options: StartOptions | None = None) -> None: ...

    async def stop(self, *, delete: bool = True) -> None: ...

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult: ...

    async def upload(self, src: Path, dst: PurePosixPath) -> None: ...

    async def download(self, src: PurePosixPath, dst: Path) -> None: ...

    async def set_network_policy(self, policy: NetworkPolicy) -> None: ...

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None: ...
