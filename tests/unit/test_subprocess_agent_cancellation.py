from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

import pytest
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import stream_stdout_jsonl

from loom.agent.subprocess import SubprocessAgent
from loom.driver.fake import FakeDriver
from loom.errors import AgentError
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


class _StubControlPlaneClient:
    async def mint_step_token(self, **_: object) -> str:
        return "loom_step_test-token"


class _TrackingStreamingHandler:
    def __init__(self) -> None:
        self.kill_count = 0
        self.killed = asyncio.Event()

    def __call__(
        self,
        argv: list[str],
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None,
    ) -> ExecHandle:
        async def _empty_stream() -> AsyncIterator[bytes]:
            if False:
                yield b""

        async def _wait() -> int:
            await self.killed.wait()
            return -9

        async def _kill() -> None:
            self.kill_count += 1
            self.killed.set()

        return ExecHandle(
            pid=123,
            stdout=_empty_stream(),
            stderr=_empty_stream(),
            _wait=_wait,
            _kill=_kill,
        )


class _ScriptedStreamingHandler:
    def __init__(
        self,
        *,
        stdout_chunks: list[bytes],
        stderr_chunks: list[bytes],
        return_code: int,
    ) -> None:
        self.stdout_chunks = stdout_chunks
        self.stderr_chunks = stderr_chunks
        self.return_code = return_code

    def __call__(
        self,
        argv: list[str],
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None,
    ) -> ExecHandle:
        async def _stdout() -> AsyncIterator[bytes]:
            for chunk in self.stdout_chunks:
                yield chunk

        async def _stderr() -> AsyncIterator[bytes]:
            for chunk in self.stderr_chunks:
                yield chunk

        async def _wait() -> int:
            return self.return_code

        async def _kill() -> None:
            return None

        return ExecHandle(
            pid=123,
            stdout=_stdout(),
            stderr=_stderr(),
            _wait=_wait,
            _kill=_kill,
        )


@dataclass(frozen=True)
class _BlockingAdapter:
    capture_started: asyncio.Event

    name: str = "blocking-adapter"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = None

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        return ["sleep", "9999"]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        self.capture_started.set()
        await asyncio.Event().wait()
        if False:
            yield


@dataclass(frozen=True)
class _StdoutJsonlAdapter:
    name: str = "claude-code"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "anthropic"
    api_key_env: str = "ANTHROPIC_AUTH_TOKEN"
    base_url_env: str = "ANTHROPIC_BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = True
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = None

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        return ["claude", "--print", instruction]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in stream_stdout_jsonl(exec_handle):
            yield event


async def test_cancelled_subprocess_agent_kills_streaming_exec(
    tmp_path: Path,
) -> None:
    streaming = _TrackingStreamingHandler()
    driver = FakeDriver(streaming_handler=streaming)
    await driver.start()

    capture_started = asyncio.Event()
    agent = SubprocessAgent(
        adapter=_BlockingAdapter(capture_started=capture_started),
        model=ModelSpec(provider="openai", name="gpt-5"),
        cp_client=_StubControlPlaneClient(),
        gateway_url="http://gw",
        team_id=uuid4(),
        trial_id=uuid4(),
    )

    async with TrajectoryWriter(
        local_path=tmp_path / "trajectory.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/t/events.jsonl",
        min_part_bytes=0,
    ) as trajectory:
        task = asyncio.create_task(
            agent.run(
                instruction="solve",
                env=driver,
                trajectory=trajectory,
                mcp=[],
                skills_dir=None,
                step_id="main",
            )
        )
        await asyncio.wait_for(capture_started.wait(), timeout=1.0)

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(task, timeout=0.01)

    assert streaming.kill_count == 1
    await driver.stop()


async def test_nonzero_stdout_jsonl_error_is_in_agent_error(tmp_path: Path) -> None:
    streaming = _ScriptedStreamingHandler(
        stdout_chunks=[
            (
                b'{"type":"result","is_error":true,'
                b'"result":"Connection closed mid-response api_key=sk-DEADBEEF12345abc",'
                b'"permission_denials":[{"tool_name":"Bash","pattern":"python -m pytest"}]}'
                b"\n"
            ),
        ],
        stderr_chunks=[],
        return_code=1,
    )
    driver = FakeDriver(streaming_handler=streaming)
    await driver.start()

    agent = SubprocessAgent(
        adapter=_StdoutJsonlAdapter(),
        model=ModelSpec(provider="anthropic", name="claude-sonnet-4-6"),
        cp_client=_StubControlPlaneClient(),
        gateway_url="http://gw",
        team_id=uuid4(),
        trial_id=uuid4(),
    )

    async with TrajectoryWriter(
        local_path=tmp_path / "trajectory.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/t/events.jsonl",
        min_part_bytes=0,
    ) as trajectory:
        with pytest.raises(AgentError) as exc:
            await agent.run(
                instruction="solve",
                env=driver,
                trajectory=trajectory,
                mcp=[],
                skills_dir=None,
                step_id="main",
            )

    message = str(exc.value)
    assert "claude-code exited rc=1 on step main" in message
    assert "Connection closed mid-response" in message
    assert "sk-DEADBEEF12345abc" not in message
    assert "[REDACTED:api-key]" in message
    assert "permission_denials=1" in message
    assert "Bash" in message
    await driver.stop()


async def test_nonzero_stdout_jsonl_fallback_error_is_redacted(tmp_path: Path) -> None:
    streaming = _ScriptedStreamingHandler(
        stdout_chunks=[
            (
                b'{"type":"server_error",'
                b'"message":{"content":[{"type":"text",'
                b'"text":"504 upstream timeout against '
                b'https://yibuapi.com/v1/messages?X-Amz-Signature=secret '
                b'Authorization: Bearer loom_step_RAW-SECRET-XYZ"}]}}'
                b"\n"
            ),
        ],
        stderr_chunks=[],
        return_code=1,
    )
    driver = FakeDriver(streaming_handler=streaming)
    await driver.start()

    agent = SubprocessAgent(
        adapter=_StdoutJsonlAdapter(),
        model=ModelSpec(provider="anthropic", name="claude-sonnet-4-6"),
        cp_client=_StubControlPlaneClient(),
        gateway_url="http://gw",
        team_id=uuid4(),
        trial_id=uuid4(),
    )

    async with TrajectoryWriter(
        local_path=tmp_path / "trajectory.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/t/events.jsonl",
        min_part_bytes=0,
    ) as trajectory:
        with pytest.raises(AgentError) as exc:
            await agent.run(
                instruction="solve",
                env=driver,
                trajectory=trajectory,
                mcp=[],
                skills_dir=None,
                step_id="main",
            )

    message = str(exc.value)
    assert "upstream timeout" in message
    assert "X-Amz-Signature=secret" not in message
    assert "loom_step_RAW-SECRET-XYZ" not in message
    assert "[REDACTED" in message
    await driver.stop()
