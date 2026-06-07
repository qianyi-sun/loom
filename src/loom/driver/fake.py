"""FakeDriver — in-memory deterministic Driver for unit + contract + integration
tests. NOT production-safe; never schedule a real workload against this driver.

Behaviour:
- start/stop track state and enforce lifecycle invariants
- exec consults an injected handler — defaults to no-op success
- upload/download manipulate an in-memory filesystem dict
- set_network_policy and run_healthcheck are no-ops by default; tests can
  override to inject failures

Spec §6.2 + §6.4.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Literal

from loom.driver.base import MAX_EXEC_STREAM_BYTES, ExecHandle, StartOptions
from loom.errors import (
    DriverAlreadyStartedError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy, Public
from loom.models.types import OS

ExecHandler = Callable[
    [str, str | int | None, PurePosixPath | None, Mapping[str, str] | None],
    ExecResult,
]

StreamingHandler = Callable[
    [list[str], dict[str, str], PurePosixPath, str | int | None],
    ExecHandle,
]


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
class FakeDriver:
    """In-memory Driver for tests. See module docstring."""

    capabilities: Capabilities = field(default_factory=_default_caps)
    os: OS = "linux"

    # Optional explicit exec handler. If None, all execs return rc=0 empty.
    exec_handler: ExecHandler | None = None

    # Optional streaming-exec handler. If None, exec_streaming raises
    # NotImplementedError (tests that use streaming MUST inject a handler).
    streaming_handler: StreamingHandler | None = None

    # In-memory filesystem: dict[PurePosixPath, bytes]
    filesystem: dict[PurePosixPath, bytes] = field(default_factory=dict)

    # Mutable network policy (last value passed to set_network_policy)
    network_policy: NetworkPolicy = field(default_factory=lambda: Public())

    # Healthcheck stub: if not None, called by run_healthcheck.
    healthcheck_stub: Callable[[HealthcheckSpec | None], None] | None = None

    # Internal lifecycle state. Public for test introspection.
    state: Literal["constructed", "running", "stopped"] = "constructed"

    async def start(self, *, options: StartOptions | None = None) -> None:
        # Spec §2.2: start() may be called at most once per instance — whether
        # the instance is currently running or has already been stopped.
        if self.state != "constructed":
            raise DriverAlreadyStartedError(
                f"FakeDriver.start() rejected in state {self.state!r}",
            )
        self.state = "running"

    async def stop(self, *, delete: bool = True) -> None:
        # Idempotent. Only running→stopped is a real transition; calling stop()
        # before start() leaves state at 'constructed' so that {stop, start,
        # stop, start} doesn't lock the driver out of its single start.
        if self.state == "running":
            self.state = "stopped"

    def _require_running(self) -> None:
        if self.state != "running":
            raise DriverNotStartedError(
                f"FakeDriver is in state {self.state!r}; expected 'running'",
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
        if self.exec_handler is None:
            return ExecResult(
                return_code=0, stdout=b"", stderr=b"",
                truncated=False, duration_sec=0.0,
            )
        return self.exec_handler(cmd, user, cwd, env)

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        self._require_running()
        if self.streaming_handler is None:
            raise NotImplementedError(
                "FakeDriver constructed without streaming_handler; "
                "tests that exercise exec_streaming MUST inject one via "
                "scripted_streaming_handler(...).",
            )
        return self.streaming_handler(argv, env_vars, cwd, user)

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self._require_running()
        self.filesystem[dst] = src.read_bytes()

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        self._require_running()
        if src not in self.filesystem:
            raise FileNotFoundError(f"{src} not in FakeDriver filesystem")
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(self.filesystem[src])

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        self._require_running()
        self.network_policy = policy

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        self._require_running()
        if self.healthcheck_stub is not None:
            self.healthcheck_stub(hc)


def command_table_handler(
    table: dict[str, ExecResult],
    *,
    default: ExecResult | None = None,
) -> ExecHandler:
    """Build an exec_handler that returns pre-recorded results for known
    commands. Unmatched commands return `default` (or a no-op success).

    Truncation: if a recorded ExecResult exceeds MAX_EXEC_STREAM_BYTES, the
    handler enforces the cap as a real driver would (stdout/stderr clipped,
    `truncated=True`).
    """
    fallback = default or ExecResult(
        return_code=0, stdout=b"", stderr=b"",
        truncated=False, duration_sec=0.0,
    )

    def _handler(
        cmd: str,
        user: str | int | None,
        cwd: PurePosixPath | None,
        env: Mapping[str, str] | None,
    ) -> ExecResult:
        recorded = table.get(cmd, fallback)
        return _enforce_caps(recorded)

    return _handler


def _enforce_caps(r: ExecResult) -> ExecResult:
    """Trim stdout/stderr to MAX_EXEC_STREAM_BYTES, flipping `truncated` to True
    if either stream was clipped."""
    stdout = r.stdout
    stderr = r.stderr
    truncated = r.truncated
    if len(stdout) > MAX_EXEC_STREAM_BYTES:
        stdout = stdout[:MAX_EXEC_STREAM_BYTES]
        truncated = True
    if len(stderr) > MAX_EXEC_STREAM_BYTES:
        stderr = stderr[:MAX_EXEC_STREAM_BYTES]
        truncated = True
    if truncated == r.truncated and stdout is r.stdout and stderr is r.stderr:
        return r
    return ExecResult(
        return_code=r.return_code,
        stdout=stdout,
        stderr=stderr,
        truncated=truncated,
        duration_sec=r.duration_sec,
    )


def scripted_streaming_handler(
    *,
    stdout_chunks: list[bytes],
    stderr_chunks: list[bytes],
    return_code: int,
    sleep_before_exit: float = 0.0,
) -> StreamingHandler:
    """Build a streaming_handler that yields the scripted chunks then exits
    with `return_code`. `sleep_before_exit` keeps the process "alive" so
    `kill()` has something to interrupt; the killer wakes `wait()` early."""

    def _factory(
        argv: list[str],
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None,
    ) -> ExecHandle:
        killed = asyncio.Event()

        async def _stream(chunks: list[bytes]) -> AsyncIterator[bytes]:
            for c in chunks:
                yield c
                await asyncio.sleep(0)

        async def _wait() -> int:
            if sleep_before_exit > 0:
                try:
                    await asyncio.wait_for(
                        killed.wait(), timeout=sleep_before_exit,
                    )
                except TimeoutError:
                    pass
            return return_code

        async def _kill() -> None:
            killed.set()

        return ExecHandle(
            pid=0,
            stdout=_stream(list(stdout_chunks)),
            stderr=_stream(list(stderr_chunks)),
            _wait=_wait,
            _kill=_kill,
        )

    return _factory
