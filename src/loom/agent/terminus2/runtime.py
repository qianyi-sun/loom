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
_TMUX_SESSION_LOST_MID_DISPATCH = (
    "Terminus2 tmux session/server lost mid-dispatch."
)
_TMUX_SOFT_RECOVER_NOTICE = (
    "Terminus2 tmux session was lost mid-dispatch and has been recreated once.\n"
    "Previous in-flight keystrokes were NOT re-run. Shell state (cwd, env) was "
    "reset.\n"
    "Filesystem in the sandbox may be partially updated — inspect before "
    "continuing.\n"
    "Current directory: {cwd}\n"
)


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


def _tmux_loss_error_text(exc: BaseException) -> str:
    return str(exc)


def _is_tmux_server_lost_error(exc: BaseException) -> bool:
    text = _tmux_loss_error_text(exc).lower()
    return "no server running" in text or "lost mid-dispatch" in text


async def _recreate_harbor_tmux_session(session: Any) -> str:
    """Recreate Harbor's fixed tmux session without replaying keystrokes.

    Uses Harbor's ``_tmux_start_session`` command when present so pane size /
    pipe-pane logging match setup. Skips asciinema re-attach (recording may
    have a gap). Returns the fresh shell's ``pwd`` for the recovery notice.
    """
    environment = getattr(session, "environment", None)
    if environment is None:
        raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH)

    session_name = getattr(session, "_session_name", None) or _HARBOR_TMUX_SESSION
    user = getattr(session, "_user", None)
    exec_kwargs: dict[str, Any] = {}
    if user is not None:
        exec_kwargs["user"] = user

    kill = await environment.exec(
        command=(
            f"tmux kill-session -t {session_name} 2>/dev/null || true; "
            f"tmux kill-server 2>/dev/null || true"
        ),
        **exec_kwargs,
    )
    del kill

    start_cmd = getattr(session, "_tmux_start_session", None)
    if isinstance(start_cmd, str) and start_cmd.strip():
        command = start_cmd
    elif callable(start_cmd):
        command = start_cmd()
    else:
        command = (
            f"export TERM=xterm-256color && export SHELL=/bin/bash && "
            f"tmux new-session -d -s {session_name} 'bash --login'"
        )

    started = await environment.exec(command=command, **exec_kwargs)
    return_code = getattr(started, "return_code", None)
    if return_code not in (0, None):
        stderr = getattr(started, "stderr", "") or ""
        raise AgentError(
            f"{_TMUX_SESSION_LOST_MID_DISPATCH} Recreate failed: {stderr}".strip()
        )

    # Incremental capture state from the dead pane is meaningless.
    if hasattr(session, "_previous_buffer"):
        session._previous_buffer = None

    is_alive = getattr(session, "is_session_alive", None)
    if is_alive is not None and not await is_alive():
        raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH)

    pwd = await environment.exec(command="pwd", **exec_kwargs)
    cwd = (getattr(pwd, "stdout", None) or "").strip() or "unknown"
    return cwd


def _install_tmux_session_alive_guard(agent: Any) -> None:
    """Soft-recover once if Harbor's tmux session dies mid-dispatch (#1068).

    After ``send_keys``, if the session/server is gone (or ``send_keys`` raises
    a ``no server running`` error):

    1. Recreate ``terminus-2`` once in the same sandbox.
    2. Do **not** replay the failed keystrokes; suppress further keys in the
       current command batch.
    3. Prepend an honest recovery notice to the next
       ``get_incremental_output`` so the model can re-orient.
    4. On recreate failure, or a second death after a recover → fail closed
       with :data:`_TMUX_SESSION_LOST_MID_DISPATCH`.

    Instance-local wrap only — GB10 workers run many Terminus2 trials
    concurrently, so a process-global monkeypatch would race.
    """
    session = getattr(agent, "_session", None)
    if session is None:
        return
    original_send_keys = session.send_keys
    original_get_incremental = getattr(session, "get_incremental_output", None)

    recovered_once = False
    suppress_keys = False
    pending_notice: str | None = None

    async def _soft_recover_or_raise(cause: BaseException | None = None) -> None:
        nonlocal recovered_once, suppress_keys, pending_notice
        if recovered_once:
            raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH) from cause
        try:
            cwd = await _recreate_harbor_tmux_session(session)
        except AgentError:
            raise
        except Exception as exc:
            raise AgentError(_TMUX_SESSION_LOST_MID_DISPATCH) from exc
        recovered_once = True
        suppress_keys = True
        pending_notice = _TMUX_SOFT_RECOVER_NOTICE.format(cwd=cwd)

    async def _send_keys_with_alive_check(*args: Any, **kwargs: Any) -> Any:
        if suppress_keys:
            # Remainder of the in-flight command batch after a soft recover.
            return None
        try:
            result = await original_send_keys(*args, **kwargs)
        except Exception as exc:
            if _is_tmux_server_lost_error(exc):
                await _soft_recover_or_raise(exc)
                return None
            raise
        is_alive = getattr(session, "is_session_alive", None)
        if is_alive is not None and not await is_alive():
            await _soft_recover_or_raise()
            return None
        return result

    session.send_keys = _send_keys_with_alive_check

    if original_get_incremental is None:
        return

    async def _get_incremental_with_recovery_notice(*args: Any, **kwargs: Any) -> str:
        nonlocal suppress_keys, pending_notice
        output = await original_get_incremental(*args, **kwargs)
        if pending_notice is None:
            return output
        notice = pending_notice
        pending_notice = None
        suppress_keys = False
        return f"{notice}\n{output}"

    session.get_incremental_output = _get_incremental_with_recovery_notice


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
            _install_tmux_session_alive_guard(agent)
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
        except AgentError:
            raise
        except Exception as exc:
            # Harbor tmux/session failures are bare RuntimeError; wrap so
            # step_runner keeps an actionable message (#1068).
            raise AgentError(str(exc)) from exc
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
