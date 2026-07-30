"""Runtime helper unit tests (#744, #782)."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

import pytest

from loom.agent.terminus2.runtime import (
    LoomTerminus2Runtime,
    _harbor_model_name,
    _install_tmux_session_alive_guard,
    _openai_gateway_base,
)
from loom.driver.fake import FakeDriver
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


def test_openai_gateway_base_appends_v1() -> None:
    assert _openai_gateway_base("http://gateway/openai") == "http://gateway/openai/v1"
    assert (
        _openai_gateway_base("http://gateway/openai/v1")
        == "http://gateway/openai/v1"
    )


def test_harbor_model_name_prefixes_openai() -> None:
    assert _harbor_model_name(ModelSpec(provider="openai", name="gpt-4")) == (
        "openai/gpt-4"
    )


class _TokenCP:
    def __init__(self) -> None:
        self._counter = 0

    async def mint_step_token(self, **kwargs: object) -> str:
        self._counter += 1
        return f"token-{self._counter}"

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
        return []


def _patch_harbor(
    monkeypatch,
    *,
    tokens_seen: list[str],
    lifecycle: list[str] | None = None,
) -> None:
    class _FakeTerminus2:
        def __init__(self, logs_dir, **kwargs: object) -> None:
            self._logs_dir = logs_dir
            llm_kwargs = kwargs.get("llm_kwargs")
            if isinstance(llm_kwargs, dict) and "api_key" in llm_kwargs:
                tokens_seen.append(str(llm_kwargs["api_key"]))

        async def setup(self, env: object) -> None:
            if lifecycle is not None:
                lifecycle.append("setup")
            await asyncio.sleep(0.05)

        async def run(
            self,
            instruction: str,
            env: object,
            context: object,
        ) -> None:
            await asyncio.sleep(0.05)
            traj = self._logs_dir / "trajectory.json"
            traj.write_text(
                json.dumps({"steps": []}),
                encoding="utf-8",
            )

    class _FakeContext:
        pass

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime._import_terminus2",
        lambda: (_FakeTerminus2, _FakeContext),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.harbor_environment._import_harbor",
        lambda: None,
    )

    class _TrialPaths:
        def __init__(self, trial_dir: Path) -> None:
            self.trial_dir = trial_dir

        def mkdir(self) -> None:
            self.trial_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.make_trial_paths",
        lambda logs_root: _TrialPaths(logs_root),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.LoomHarborEnvironment.create",
        staticmethod(lambda **kwargs: object()),
    )


@pytest.mark.asyncio
async def test_concurrent_runtimes_do_not_mutate_process_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    tokens_seen: list[str] = []
    _patch_harbor(monkeypatch, tokens_seen=tokens_seen)

    prior_api_key = os.environ.get("OPENAI_API_KEY")
    prior_base = os.environ.get("OPENAI_BASE_URL")
    cp = _TokenCP()

    async def _run_one(trial_id: UUID, path: Path) -> None:
        runtime = LoomTerminus2Runtime(
            model=ModelSpec(provider="openai", name="gpt-4"),
            team_id=str(uuid4()),
            trial_id=trial_id,
            cp_client=cp,
            gateway_url="http://127.0.0.1:19100",
        )
        driver = FakeDriver()
        await driver.start()
        writer = TrajectoryWriter(
            local_path=path,
            store=FakeObjectStore(),
            bucket="trajectories",
            key=f"{trial_id}/events.jsonl",
            flush_event_count=1000,
            flush_bytes=10_000_000,
            flush_sec=3600,
            min_part_bytes=0,
        )
        async with writer:
            await runtime.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )
        await driver.stop()

    await asyncio.gather(
        _run_one(uuid4(), tmp_path / "a.jsonl"),
        _run_one(uuid4(), tmp_path / "b.jsonl"),
    )

    assert os.environ.get("OPENAI_API_KEY") == prior_api_key
    assert os.environ.get("OPENAI_BASE_URL") == prior_base
    assert set(tokens_seen) == {"token-1", "token-2"}


@pytest.mark.asyncio
async def test_retry_clears_harbor_fixed_tmux_session_before_each_setup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    lifecycle: list[str] = []
    tokens_seen: list[str] = []
    _patch_harbor(
        monkeypatch,
        tokens_seen=tokens_seen,
        lifecycle=lifecycle,
    )

    def _record_exec(
        cmd: str,
        user: str | int | None,
        cwd: PurePosixPath | None,
        env: object,
    ):
        del user, cwd, env
        lifecycle.append(cmd)
        from loom.models.exec import ExecResult

        return ExecResult(
            return_code=0,
            stdout=b"",
            stderr=b"",
            truncated=False,
            duration_sec=0.0,
        )

    trial_id = uuid4()
    runtime = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="gpt-4"),
        team_id=str(uuid4()),
        trial_id=trial_id,
        cp_client=_TokenCP(),
        gateway_url="http://127.0.0.1:19100",
    )
    driver = FakeDriver(exec_handler=_record_exec)
    await driver.start()

    for attempt in range(2):
        writer = TrajectoryWriter(
            local_path=tmp_path / f"retry-{attempt}.jsonl",
            store=FakeObjectStore(),
            bucket="trajectories",
            key=f"{trial_id}/{attempt}/events.jsonl",
            flush_event_count=1000,
            flush_bytes=10_000_000,
            flush_sec=3600,
            min_part_bytes=0,
        )
        async with writer:
            await runtime.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )

    cleanup = "tmux kill-session -t terminus-2 2>/dev/null || true"
    assert lifecycle == [cleanup, "setup", cleanup, "setup"]
    assert tokens_seen == ["token-1", "token-2"]
    await driver.stop()


@pytest.mark.asyncio
async def test_runtime_cleanup_leaves_process_env_unchanged_on_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _FailingTerminus2:
        def __init__(self, logs_dir, **kwargs: object) -> None:
            self._logs_dir = logs_dir

        async def setup(self, env: object) -> None:
            return None

        async def run(
            self,
            instruction: str,
            env: object,
            context: object,
        ) -> None:
            traj = self._logs_dir / "trajectory.json"
            traj.write_text(json.dumps({"steps": []}), encoding="utf-8")
            raise RuntimeError("harbor boom")

    class _FakeContext:
        pass

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime._import_terminus2",
        lambda: (_FailingTerminus2, _FakeContext),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.harbor_environment._import_harbor",
        lambda: None,
    )

    class _TrialPaths:
        def __init__(self, trial_dir: Path) -> None:
            self.trial_dir = trial_dir

        def mkdir(self) -> None:
            self.trial_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.make_trial_paths",
        lambda logs_root: _TrialPaths(logs_root),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.LoomHarborEnvironment.create",
        staticmethod(lambda **kwargs: object()),
    )

    prior_api_key = os.environ.get("OPENAI_API_KEY")
    prior_base = os.environ.get("OPENAI_BASE_URL")

    runtime = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="gpt-4"),
        team_id=str(uuid4()),
        trial_id=uuid4(),
        cp_client=_TokenCP(),
        gateway_url="http://127.0.0.1:19100",
    )
    driver = FakeDriver()
    await driver.start()
    writer = TrajectoryWriter(
        local_path=tmp_path / "fail.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )

    with pytest.raises(AgentError, match="harbor boom"):
        async with writer:
            await runtime.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )

    assert os.environ.get("OPENAI_API_KEY") == prior_api_key
    assert os.environ.get("OPENAI_BASE_URL") == prior_base
    await driver.stop()


@pytest.mark.asyncio
async def test_runtime_wraps_tmux_no_server_runtime_error_as_agent_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    err = (
        "loom-trial-main: failed to send non-blocking keys: "
        "command=\"tmux send-keys -t terminus-2 -- 'chmod +x /tmp/x\\n'\", "
        "return_code=1, stderr='no server running on /tmp/tmux-0/default\\n', "
        "stdout=''"
    )

    class _FailingTerminus2:
        def __init__(self, logs_dir, **kwargs: object) -> None:
            self._logs_dir = logs_dir

        async def setup(self, env: object) -> None:
            return None

        async def run(
            self,
            instruction: str,
            env: object,
            context: object,
        ) -> None:
            traj = self._logs_dir / "trajectory.json"
            traj.write_text(json.dumps({"steps": []}), encoding="utf-8")
            raise RuntimeError(err)

    class _FakeContext:
        pass

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime._import_terminus2",
        lambda: (_FailingTerminus2, _FakeContext),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.harbor_environment._import_harbor",
        lambda: None,
    )

    class _TrialPaths:
        def __init__(self, trial_dir: Path) -> None:
            self.trial_dir = trial_dir

        def mkdir(self) -> None:
            self.trial_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.make_trial_paths",
        lambda logs_root: _TrialPaths(logs_root),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.LoomHarborEnvironment.create",
        staticmethod(lambda **kwargs: object()),
    )

    runtime = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="gpt-4"),
        team_id=str(uuid4()),
        trial_id=uuid4(),
        cp_client=_TokenCP(),
        gateway_url="http://127.0.0.1:19100",
    )
    driver = FakeDriver()
    await driver.start()
    writer = TrajectoryWriter(
        local_path=tmp_path / "tmux-fail.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )

    with pytest.raises(AgentError, match="no server running"):
        async with writer:
            await runtime.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )
    await driver.stop()


class _FakeExecResult:
    def __init__(self, return_code: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr


class _RecoverableSession:
    """Session that can die once and come back via recreate execs."""

    def __init__(self, *, recreate_ok: bool = True) -> None:
        self._alive = True
        self.calls = 0
        self._session_name = "terminus-2"
        self._user = None
        self._tmux_start_session = "tmux new-session -d -s terminus-2 'bash --login'"
        self._previous_buffer = "stale-pane"
        self._recreate_ok = recreate_ok
        self.environment = self
        self.exec_commands: list[str] = []

    async def send_keys(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.calls += 1

    async def is_session_alive(self) -> bool:
        return self._alive

    async def get_incremental_output(self) -> str:
        return "New Terminal Output:\nroot@box:/app#"

    async def exec(self, command: str, **kwargs: object) -> _FakeExecResult:
        del kwargs
        self.exec_commands.append(command)
        if command.strip() == "pwd":
            return _FakeExecResult(stdout="/app\n")
        if "new-session" in command:
            if not self._recreate_ok:
                return _FakeExecResult(return_code=1, stderr="boom")
            self._alive = True
            return _FakeExecResult(return_code=0)
        return _FakeExecResult(return_code=0)


@pytest.mark.asyncio
async def test_tmux_alive_guard_soft_recovers_once_then_fails_closed() -> None:
    session = _RecoverableSession()
    agent = type("_Agent", (), {"_session": session})()
    _install_tmux_session_alive_guard(agent)

    await agent._session.send_keys("echo ok\n")
    assert session.calls == 1

    session._alive = False
    await agent._session.send_keys("chmod +x /tmp/x\n")
    # Failed keystrokes are not replayed; original send_keys still ran once
    # before the post-check discovered death, then soft-recovered.
    assert session.calls == 2
    assert any("new-session" in cmd for cmd in session.exec_commands)
    assert session._previous_buffer is None

    # Remaining keys in the same batch are suppressed (no replay / no follow-ons).
    await agent._session.send_keys("echo should-skip\n")
    assert session.calls == 2

    output = await agent._session.get_incremental_output()
    assert "recreated once" in output
    assert "NOT re-run" in output
    assert "Current directory: /app" in output
    assert "New Terminal Output:" in output

    # After the notice is emitted, later turns can send keys again.
    await agent._session.send_keys("ls\n")
    assert session.calls == 3

    session._alive = False
    with pytest.raises(AgentError, match="lost mid-dispatch"):
        await agent._session.send_keys("second-death\n")
    assert session.calls == 4


@pytest.mark.asyncio
async def test_tmux_alive_guard_soft_recovers_from_no_server_send_keys_error() -> None:
    session = _RecoverableSession()

    async def _raise_no_server(*args: object, **kwargs: object) -> None:
        del args, kwargs
        session.calls += 1
        raise RuntimeError(
            "loom-trial-main: failed to send non-blocking keys: "
            "return_code=1, stderr='no server running on /tmp/tmux-0/default\\n'"
        )

    session.send_keys = _raise_no_server  # type: ignore[method-assign]
    agent = type("_Agent", (), {"_session": session})()
    # Re-bind original before guard wraps — install after assigning raising send_keys.
    _install_tmux_session_alive_guard(agent)

    await agent._session.send_keys("cat > /tmp/x <<'EOF'\nbody\nEOF\n")
    assert session.calls == 1
    assert any("new-session" in cmd for cmd in session.exec_commands)

    output = await agent._session.get_incremental_output()
    assert "recreated once" in output


@pytest.mark.asyncio
async def test_tmux_alive_guard_fails_closed_when_recreate_fails() -> None:
    session = _RecoverableSession(recreate_ok=False)
    agent = type("_Agent", (), {"_session": session})()
    _install_tmux_session_alive_guard(agent)

    session._alive = False
    with pytest.raises(AgentError, match="lost mid-dispatch"):
        await agent._session.send_keys("chmod +x /tmp/x\n")
    assert session.calls == 1
