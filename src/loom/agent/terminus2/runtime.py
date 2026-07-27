"""LoomTerminus2Runtime — Harbor Terminus2 wrapper in the worker pool (#744)."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

from loom.agent.terminus2.checkpoint_bridge import HarborCheckpointBridge
from loom.agent.terminus2.gateway_ledger import CheckpointBridgeError
from loom.agent.terminus2.harbor_environment import (
    LoomHarborEnvironment,
    ensure_sandbox_deps,
    make_trial_paths,
)
from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA, LOOM_BRIDGE_REVISION
from loom.driver.base import Driver
from loom.errors import AgentError
from loom.models.mcp import MCPConnection
from loom.models.types import OS, ModelSpec
from loom.request_params import sanitize_request_extras
from loom.trajectory.writer import TrajectoryWriter


def _import_terminus2() -> tuple[type, type]:
    try:
        from harbor.agents.terminus_2.terminus_2 import Terminus2
        from harbor.models.agent.context import AgentContext
    except ImportError as exc:
        raise AgentError(
            "terminus-2 requires harbor@527d50d preinstalled in the worker image",
        ) from exc
    return Terminus2, AgentContext


def _openai_gateway_base(gateway_url: str) -> str:
    parts = urlsplit(gateway_url.rstrip("/"))
    path = parts.path.rstrip("/")
    if not path or path == "/openai":
        return urlunsplit(parts._replace(path="/openai/v1"))
    if path == "/openai/v1" or path.startswith("/openai/v1/"):
        return urlunsplit(parts._replace(path="/openai/v1"))
    return gateway_url


def _harbor_model_name(model: ModelSpec) -> str:
    return f"openai/{model.name}"


_HARBOR_ARTIFACT_NAMES = ("trajectory.json", "recording.cast")
_HARBOR_TMUX_SESSION = "terminus-2"


async def _reset_harbor_tmux_session(env: Driver) -> None:
    """Remove Harbor's fixed-name session before a sequential agent retry.

    Harbor pins every Terminus2 run in a sandbox to ``terminus-2`` and does not
    stop that session after ``run``. Loom retries reuse the same sandbox, so a
    prior attempt otherwise makes ``tmux new-session`` fail with a duplicate
    session error. Each trial owns its sandbox; target only Harbor's fixed name.
    """
    await env.exec(
        f"tmux kill-session -t {_HARBOR_TMUX_SESSION} 2>/dev/null || true",
    )


async def _publish_harbor_artifacts_to_sandbox(
    env: Driver,
    logs_root: Path,
    workdir: PurePosixPath,
) -> dict[str, PurePosixPath]:
    """Upload Harbor-native artifacts into the sandbox for step_runner collection."""
    agent_dir = workdir / ".loom/agent"
    published: dict[str, PurePosixPath] = {}
    for name in _HARBOR_ARTIFACT_NAMES:
        src = logs_root / name
        if not src.is_file():
            continue
        dst = agent_dir / name
        await env.upload(src, dst)
        published[name] = dst
    return published


@dataclass
class LoomTerminus2Runtime:
    """Worker-side wrapper around pinned Harbor ``Terminus2``."""

    model: ModelSpec
    team_id: str
    trial_id: UUID
    cp_client: Any
    gateway_url: str
    name: str = "terminus-2"
    mode: Literal["out-of-box", "in-box"] = "in-box"
    version: str = LOOM_BRIDGE_REVISION
    supports_os: frozenset[OS] = field(default_factory=lambda: frozenset({"linux"}))
    emits_gateway_llm_call_events: bool = True
    provider_connection_id: str | None = None
    request_params: dict[str, Any] = field(default_factory=dict)
    max_turns: int = 50
    workdir: PurePosixPath = field(default_factory=lambda: PurePosixPath("/workspace"))
    step_token_ttl_sec: int = 1800

    def __post_init__(self) -> None:
        self.request_params = sanitize_request_extras(self.request_params)

    async def setup(self, env: Driver) -> None:
        await ensure_sandbox_deps(env)
        await env.exec(f"mkdir -p {(self.workdir / '.loom/agent').as_posix()}")

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
        del mcp, skills_dir
        terminus2_cls, agent_context_cls = _import_terminus2()

        step_token = await self.cp_client.mint_step_token(
            team_id=UUID(self.team_id),
            trial_id=self.trial_id,
            step_id=step_id,
            ttl_sec=self.step_token_ttl_sec,
        )
        api_base = _openai_gateway_base(self.gateway_url)

        logs_root = Path(tempfile.mkdtemp(prefix=f"loom-terminus2-{self.trial_id}-"))
        trial_paths = make_trial_paths(logs_root)
        trial_paths.mkdir()

        bridge = HarborCheckpointBridge(
            trajectory=trajectory,
            trial_id=self.trial_id,
            step_id=step_id,
            model=self.model,
            cp_client=self.cp_client,
        )
        await bridge.emit_provenance()

        harbor_env = LoomHarborEnvironment.create(
            driver=env,
            trial_paths=trial_paths,
            workdir=self.workdir,
            trial_id=self.trial_id,
            step_id=step_id,
        )

        agent = terminus2_cls(
            logs_dir=logs_root,
            model_name=_harbor_model_name(self.model),
            max_turns=self.max_turns,
            api_base=api_base,
            session_id=str(self.trial_id),
            record_terminal_session=True,
            enable_summarize=False,
            llm_kwargs={"api_key": step_token},
        )
        context = agent_context_cls()
        trajectory_path = logs_root / "trajectory.json"
        completeness = "full"
        poll_stop = asyncio.Event()
        bridge_error: CheckpointBridgeError | None = None

        async def _poll_checkpoints() -> None:
            nonlocal bridge_error
            while not poll_stop.is_set():
                try:
                    await bridge.sync_trajectory_file(trajectory_path)
                except CheckpointBridgeError as exc:
                    bridge_error = exc
                    poll_stop.set()
                    return
                try:
                    await asyncio.wait_for(poll_stop.wait(), timeout=0.5)
                except TimeoutError:
                    continue

        poll_task = asyncio.create_task(_poll_checkpoints())

        try:
            await _reset_harbor_tmux_session(env)
            await agent.setup(harbor_env)
            await agent.run(instruction, harbor_env, context)
        except asyncio.CancelledError:
            completeness = "partial"
            try:
                await bridge.sync_trajectory_file(
                    trajectory_path, completeness=completeness,
                )
            except CheckpointBridgeError:
                pass
            raise
        finally:
            poll_stop.set()
            await poll_task
            if bridge_error is not None:
                raise AgentError(str(bridge_error)) from bridge_error
            try:
                await bridge.sync_trajectory_file(
                    trajectory_path, completeness=completeness,
                )
            except CheckpointBridgeError as exc:
                raise AgentError(str(exc)) from exc
            sandbox_paths = await _publish_harbor_artifacts_to_sandbox(
                env, logs_root, self.workdir,
            )
            await bridge.emit_artifact_refs(logs_root, sandbox_paths=sandbox_paths)

        if not trajectory_path.is_file():
            raise AgentError(
                f"Harbor Terminus2 did not produce trajectory.json "
                f"(harbor_compat={HARBOR_COMPAT_SHA})",
            )
