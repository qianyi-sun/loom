"""Run the existing Harbor Terminus bridge against an isolated Pod sandbox.

The caller owns sandbox lifecycle and durable output upload. This module never
receives Kubernetes, worker, storage, or provider credentials. Its only HTTP
peer is the execution runtime's loopback, lease-scoped Gateway proxy.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.attempt_deadline import AttemptDeadline
from loom.driver.base import Driver
from loom.errors import AgentError
from loom.models.task import TaskConfig
from loom.models.trajectory import TrajectoryEvent
from loom.models.trial import TrialConfig

if TYPE_CHECKING:
    from loom.trajectory.writer import TrajectoryWriter

_PROXY_TOKEN = "loom_workload_proxy"
_LEDGER_MAX_BYTES = 16 * 1024 * 1024
_NATIVE_ARTIFACTS = frozenset({"trajectory.json", "recording.cast"})
TASK_IMAGE_TOOLS_REQUIRED = "task image must preinstall bash, tmux and asciinema"


@dataclass(frozen=True)
class _ProxyTokenGrant:
    token: str = _PROXY_TOKEN


class LeaseGatewayClient:
    """Narrow CP facade; the real step token stays in the Go broker.

    GET /internal/loom/llm-calls has no caller-selected trial parameter. The
    broker authenticates its Pod lease and returns an envelope containing
    trial_id, team_id, step_id='agent', and items in the CP llm-calls shape.
    """

    def __init__(self, *, gateway_url: str, trial_id: UUID, team_id: UUID) -> None:
        url = urlsplit(gateway_url)
        if (
            url.scheme != "http"
            or url.hostname not in {"127.0.0.1", "::1"}
            or url.username is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
        ):
            raise AgentError("Terminus requires the execution loopback proxy")
        self.gateway_url = gateway_url.rstrip("/")
        self.trial_id = trial_id
        self.team_id = team_id

    def _check_identity(self, *, team_id: UUID, trial_id: UUID, step_id: str) -> None:
        if (team_id, trial_id, step_id) != (self.team_id, self.trial_id, "agent"):
            raise AgentError("Terminus execution identity does not match its lease")

    async def mint_step_token(
        self, *, team_id: UUID, trial_id: UUID, step_id: str, ttl_sec: int,
    ) -> str:
        del ttl_sec
        self._check_identity(team_id=team_id, trial_id=trial_id, step_id=step_id)
        return _PROXY_TOKEN

    async def mint_attempt_step_token(
        self, *, team_id: UUID, trial_id: UUID, step_id: str, ttl_sec: int,
        attempt_deadline_wall_clock: datetime,
    ) -> _ProxyTokenGrant:
        del attempt_deadline_wall_clock
        await self.mint_step_token(
            team_id=team_id, trial_id=trial_id, step_id=step_id, ttl_sec=ttl_sec,
        )
        return _ProxyTokenGrant()

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]:
        if trial_id != self.trial_id:
            raise AgentError("Terminus cannot read another trial's ledger")
        try:
            async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
                async with client.stream(
                    "GET", self.gateway_url + "/internal/loom/llm-calls",
                ) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > _LEDGER_MAX_BYTES:
                            raise AgentError("Terminus Gateway ledger exceeds output limit")
            envelope = json.loads(body)
        except (httpx.HTTPError, ValueError) as exc:
            # Never serialize proxy responses or request objects into evidence.
            raise AgentError("Terminus Gateway ledger is unavailable") from exc
        if (
            not isinstance(envelope, dict)
            or envelope.get("trial_id") != str(self.trial_id)
            or envelope.get("team_id") != str(self.team_id)
            or envelope.get("step_id") != "agent"
            or not isinstance(envelope.get("items"), list)
        ):
            raise AgentError("Terminus Gateway ledger identity is invalid")
        rows = envelope["items"]
        if any(
            not isinstance(row, dict)
            or row.get("trial_id") != str(self.trial_id)
            or row.get("step_id") != "agent"
            for row in rows
        ):
            raise AgentError("Terminus Gateway ledger contains an unrelated call")
        return cast(list[dict[str, Any]], rows)


class _LocalTrajectory:
    """Local typed events; the existing Go output commit owns remote durability."""

    def __init__(self, path: Path, deadline: AttemptDeadline, max_bytes: int) -> None:
        self.path = path
        self.deadline = deadline
        self.max_bytes = max_bytes
        self._next_seq = 0
        self._bytes = 0
        self._lock = asyncio.Lock()

    def require_attempt_active(self) -> None:
        self.deadline.require_remaining()

    async def append(self, event: TrajectoryEvent) -> None:
        async with self._lock:
            self.require_attempt_active()
            event = event.model_copy(update={"seq": self._next_seq})
            line = event.model_dump_json().encode() + b"\n"
            if b"loom_step_" in line:
                raise AgentError("refusing to persist a step credential in trajectory")
            if self._bytes + len(line) > self.max_bytes:
                raise AgentError("Terminus trajectory exceeds output limit")
            with self.path.open("ab") as stream:
                stream.write(line)
            self._bytes += len(line)
            self._next_seq += 1


class _ArtifactDriver:
    """Keep Harbor-owned files outside the untrusted task container."""

    def __init__(self, driver: Driver, workspace: Path, workdir: PurePosixPath) -> None:
        self._driver = driver
        self._workspace = workspace
        self._artifact_dir = workdir / ".loom/agent"

    def __getattr__(self, name: str) -> Any:
        return getattr(self._driver, name)

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        if dst.parent == self._artifact_dir and dst.name in _NATIVE_ARTIFACTS:
            output = self._workspace / "harbor" / dst.name
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, output)
            output.chmod(0o600)
        else:
            await self._driver.upload(src, dst)


async def run_terminus2(
    *,
    driver: Driver,
    workspace: Path,
    task_config: TaskConfig,
    trial_config: TrialConfig,
    trial_id: UUID,
    team_id: UUID,
    instruction: str,
    gateway_url: str,
    sandbox_workdir: PurePosixPath | None = None,
    max_turns: int = 50,
    timeout_sec: float | None = None,
    max_trajectory_bytes: int = 64 * 1024 * 1024,
) -> None:
    """Execute one Terminus session; leave typed/native files in trusted workspace.

    The caller supplies the complete immutable instruction and an already
    started Driver for the isolated task sidecar. The caller must stop that
    sidecar on completion/cancellation, before handing its public snapshot to
    the separate verifier container. No input files are read from that sandbox
    to configure the trusted runtime.
    """
    if trial_config.agent_name != "terminus-2" or trial_config.agent_model is None:
        raise AgentError("Terminus execution requires terminus-2 and an explicit model")
    if trial_config.multi_model is not None and trial_config.multi_model.enabled:
        raise AgentError("Nebius Terminus does not yet support multi-model sessions")
    if len(task_config.steps) != 1:
        raise AgentError("Nebius Terminus requires exactly one task step")
    if not instruction.strip() or max_turns < 1 or max_trajectory_bytes < 1:
        raise AgentError("Terminus instruction and runtime limits must be nonempty")
    cp_client = LeaseGatewayClient(
        gateway_url=gateway_url, trial_id=trial_id, team_id=team_id,
    )
    workdir = sandbox_workdir or task_config.environment.workdir
    timeout = timeout_sec if timeout_sec is not None else (
        trial_config.override_agent_timeout_sec or task_config.agent.timeout_sec
    ) * trial_config.agent_timeout_multiplier
    deadline = AttemptDeadline.after(timeout)
    workspace.mkdir(parents=True, exist_ok=True)
    events_path = workspace / "trajectory.jsonl"
    # A repeated process must not append a second episode-1 to an old session.
    with events_path.open("xb"):
        pass
    events_path.chmod(0o600)
    trajectory = _LocalTrajectory(events_path, deadline, max_trajectory_bytes)
    protected_driver = cast(Driver, _ArtifactDriver(driver, workspace, workdir))
    runtime = LoomTerminus2Runtime(
        model=trial_config.agent_model,
        team_id=str(team_id),
        trial_id=trial_id,
        cp_client=cp_client,
        gateway_url=gateway_url,
        workdir=workdir,
        max_turns=max_turns,
        request_params=dict(trial_config.request_params),
    )
    runtime.begin_attempt(deadline)
    async with asyncio.timeout(deadline.require_remaining()):
        deps = await driver.exec(
            "command -v bash >/dev/null && command -v tmux >/dev/null "
            "&& command -v asciinema >/dev/null",
            timeout_sec=min(15.0, deadline.require_remaining()),
        )
        if deps.return_code != 0:
            raise AgentError(TASK_IMAGE_TOOLS_REQUIRED)
        # Do not call runtime.setup: the old worker method installs OS packages.
        await runtime.run(
            instruction=instruction,
            env=protected_driver,
            trajectory=cast("TrajectoryWriter", trajectory),
            mcp=[],
            skills_dir=None,
            step_id="agent",
        )
