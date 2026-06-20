"""SubprocessAgent — generic AgentRuntime wrapping any loom_launcher AgentAdapter.

This is the worker-side glue that bridges loom's Driver Protocol to
loom_launcher's SandboxAccess Protocol (A11.2) and the launcher's
ExecHandle facade to loom's exec_streaming output. The launcher package
is sandbox-safe (no loom imports); SubprocessAgent lives in loom proper
and depends on both sides.

Per the agent integrations spec §4, each step gets a fresh agent.run()
invocation; multi-turn sessions across steps are v1.5.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Literal
from uuid import UUID

from loom_launcher.adapter import AgentAdapter, SandboxAccess
from loom_launcher.adapter import ExecHandle as LauncherExecHandle
from loom_launcher.adapter import ModelSpec as LauncherModelSpec

from loom.driver.base import Driver
from loom.driver.base import ExecHandle as DriverExecHandle
from loom.errors import AgentError
from loom.models.mcp import MCPConnection
from loom.models.trajectory import AgentThoughtEvent, EventKind
from loom.models.types import OS, ModelSpec
from loom.trajectory.writer import TrajectoryWriter
from loom_worker.control_plane_client import StepTokenClient

logger = logging.getLogger(__name__)
_LOOM_EVENT_REQUIRED_KEYS = frozenset({"kind", "emitted_at", "trial_id", "step_id", "seq"})
_LOOM_EVENT_KINDS = frozenset(kind.value for kind in EventKind)


def _bridge_driver(driver: Driver, *, cwd: PurePosixPath) -> SandboxAccess:
    """Adapt loom.driver.base.Driver to loom_launcher.SandboxAccess.

    `tail_log_file` calls `sandbox.read_text(path)` — implemented via
    a one-shot `cat` exec in the sandbox.
    `poll_local_http` calls `sandbox.exec_oneshot(argv)` — implemented
    directly via the Driver's buffered exec.

    The launcher Protocol mirrors a strict subset of Driver; this
    function makes a real Driver satisfy it without leaking the full
    Driver surface to the launcher (which has to stay sandbox-safe).
    """

    class _Bridge:
        async def read_text(self, path: PurePosixPath) -> str:
            # Use `test -e && cat` so we can distinguish "file doesn't
            # exist yet" (FileNotFoundError, which polling adapters
            # interpret as "keep waiting") from a real I/O failure
            # (OSError, which we surface so the trial fails fast rather
            # than looping forever on a misconfigured volume).
            path_q = shlex.quote(str(path))
            result = await driver.exec(
                f"if [ -e {path_q} ]; then cat {path_q}; else exit 66; fi",
                cwd=cwd,
                timeout_sec=10.0,
            )
            if result.return_code == 66:
                raise FileNotFoundError(str(path))
            if result.return_code != 0:
                stderr = result.stderr.decode("utf-8", errors="replace")[:200]
                raise OSError(
                    f"sandbox read_text({path}) failed rc={result.return_code}: {stderr}",
                )
            return result.stdout.decode("utf-8", errors="replace")

        async def exec_oneshot(
            self,
            argv: list[str],
            *,
            timeout_sec: float = 10.0,
        ) -> tuple[int, bytes]:
            cmd = " ".join(shlex.quote(a) for a in argv)
            result = await driver.exec(cmd, cwd=cwd, timeout_sec=timeout_sec)
            return (result.return_code, result.stdout)

    return _Bridge()


def _bridge_exec_handle(
    driver_handle: DriverExecHandle,
    sandbox: SandboxAccess,
) -> LauncherExecHandle:
    """Wrap a loom.driver.base.ExecHandle as a loom_launcher.ExecHandle
    with the SandboxAccess side-channel populated (A11.2)."""
    return LauncherExecHandle(
        pid=driver_handle.pid,
        stdout=driver_handle.stdout,
        stderr=driver_handle.stderr,
        _wait=driver_handle._wait,
        _kill=driver_handle._kill,
        sandbox=sandbox,
    )


def _bridge_model(model: ModelSpec) -> LauncherModelSpec:
    """Adapt loom's ModelSpec to the launcher's (duplicated) ModelSpec."""
    return LauncherModelSpec(
        provider=model.provider,
        name=model.name,
        tier=model.tier,
        region=model.region,
    )


@dataclass
class SubprocessAgent:
    """Generic AgentRuntime wrapping any loom_launcher AgentAdapter.

    Holds `adapter` + `model` + `cp_client` + `gateway_url` + `team_id`
    + `trial_id` at construction. Each `run()` invocation mints a fresh
    step-scoped JWT (so the agent's API key is per-step + auto-expiring),
    builds env + argv via the adapter, exec_streaming's the agent, and
    forwards adapter-emitted events to the trajectory writer.
    """

    adapter: AgentAdapter
    model: ModelSpec
    cp_client: StepTokenClient
    gateway_url: str
    team_id: UUID
    trial_id: UUID
    # Standard AgentRuntime Protocol fields:
    mode: Literal["out-of-box", "in-box"] = "out-of-box"
    name: str = field(init=False)
    version: str = "1.0"
    supports_os: frozenset[OS] = field(init=False)
    workdir: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )

    # Optional per-step JWT TTL override; defaults to 1800s (30 min) per
    # spec §6.1 typical step_timeout.
    step_token_ttl_sec: int = 1800

    def __post_init__(self) -> None:
        self.name = self.adapter.name
        # The adapter declares OS as `frozenset[str]`; loom's AgentRuntime
        # Protocol expects `frozenset[OS]` (a Literal alias). They're
        # structurally identical at runtime.
        self.supports_os = self.adapter.supports_os

    async def run(
        self,
        *,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection],
        skills_dir: PurePosixPath | None,
        step_id: str,
    ) -> None:
        # 1. Mint a step-scoped JWT (Plan 9). Errors here are fatal: we
        # can't run the agent without a Gateway-acceptable bearer.
        try:
            step_token = await self.cp_client.mint_step_token(
                team_id=self.team_id,
                trial_id=self.trial_id,
                step_id=step_id,
                ttl_sec=self.step_token_ttl_sec,
            )
        except Exception as exc:
            raise AgentError(
                f"{self.adapter.name}: failed to mint step token: {exc}",
            ) from exc

        # 2. Build env + argv via the adapter.
        env_vars: dict[str, str] = {
            self.adapter.api_key_env: step_token,
            self.adapter.base_url_env: self.gateway_url,
        }
        cwd = self.workdir
        argv = self.adapter.build_invocation(
            instruction=instruction,
            workdir=cwd,
            model=_bridge_model(self.model),
            env=env_vars,
        )

        # 3. Streaming exec inside the sandbox.
        driver_handle = await env.exec_streaming(
            argv,
            env_vars=env_vars,
            cwd=cwd,
        )
        stderr_task = asyncio.create_task(_collect_stream_tail(driver_handle.stderr))

        # 4. Build the launcher-side ExecHandle with SandboxAccess wired in.
        sandbox = _bridge_driver(env, cwd=cwd)
        launcher_handle = _bridge_exec_handle(driver_handle, sandbox)

        # 5. Forward adapter events into the trajectory.
        event_seq = 0
        async for event in self.adapter.capture_events(
            exec_handle=launcher_handle,
            step_id=step_id,
            trial_id=self.trial_id,
        ):
            # The launcher emits TrajectoryEventLike (dict-like); the
            # trajectory writer accepts dicts via .write_raw_dict (added
            # by Plan 11 task 4) or pre-validates against the event union.
            # For v1 we use write_raw_dict to stay decoupled.
            payload = event.model_dump()
            if _is_complete_loom_event_payload(payload):
                await trajectory.write_raw_dict(payload)
            else:
                await trajectory.append(
                    AgentThoughtEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.trial_id,
                        step_id=step_id,
                        seq=event_seq,
                        content=_adapter_payload_to_content(payload),
                    )
                )
                event_seq += 1

        rc = await driver_handle.wait()
        stderr_tail = await _finish_tail_task(stderr_task)
        if rc != 0:
            detail = f"{self.adapter.name} exited rc={rc} on step {step_id}"
            if stderr_tail:
                detail = f"{detail}; stderr: {stderr_tail}"
            raise AgentError(
                detail,
            )


def _is_complete_loom_event_payload(payload: dict[str, object]) -> bool:
    kind = payload.get("kind")
    return (
        isinstance(kind, str)
        and kind in _LOOM_EVENT_KINDS
        and _LOOM_EVENT_REQUIRED_KEYS.issubset(payload.keys())
    )


def _adapter_payload_to_content(payload: dict[str, object]) -> str:
    line = payload.get("line")
    if isinstance(line, str):
        return line
    return json.dumps(payload, sort_keys=True)


async def _collect_stream_tail(
    stream: AsyncIterator[bytes],
    *,
    max_bytes: int = 4096,
) -> str:
    buf = bytearray()
    async for chunk in stream:
        buf.extend(chunk)
        if len(buf) > max_bytes:
            del buf[: len(buf) - max_bytes]
    return bytes(buf).decode("utf-8", errors="replace").strip()


async def _finish_tail_task(task: asyncio.Task[str]) -> str:
    try:
        return await asyncio.wait_for(task, timeout=1.0)
    except TimeoutError:
        task.cancel()
        return ""
    except Exception:
        return ""
