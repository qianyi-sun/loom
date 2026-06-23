from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from uuid import UUID, uuid4

import pytest
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike

from loom.agent.subprocess import SubprocessAgent
from loom.driver.fake import FakeDriver
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


async def test_cancelled_subprocess_agent_kills_streaming_exec(
    tmp_path,
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
