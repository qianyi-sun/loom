"""SubprocessAgent end-to-end against HelloAdapter + FakeDriver + a
mocked Control Plane mint endpoint (Plan 11 Task 4).

Verifies the full chain: mint step JWT → exec_streaming launches the
agent → adapter capture_events yields events → trajectory writer's
write_raw_dict appends them to the local JSONL → finalize uploads → the
ObjectStore has the trajectory.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from loom_launcher import get_adapter

from loom.agent.subprocess import SubprocessAgent
from loom.driver.fake import FakeDriver, scripted_streaming_handler
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom_worker.control_plane_client import HttpControlPlaneClient


@pytest.fixture
def mocked_cp_client() -> tuple[HttpControlPlaneClient, httpx.AsyncClient]:
    """Build an HttpControlPlaneClient pointed at a MockTransport that
    handles POST /admin/step-tokens with a canned JWT response."""

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/admin/step-tokens":
            return httpx.Response(201, json={
                "token": "loom_step_test-mocked-token",
                "expires_at": "2026-06-07T00:00:00Z",
            })
        return httpx.Response(404)

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        base_url="http://cp",
    )
    cp = HttpControlPlaneClient(
        base_url="http://cp", token="fake-worker-token", _client=http,
    )
    return cp, http


async def test_subprocess_agent_runs_hello_adapter_end_to_end(
    tmp_path: Path, mocked_cp_client,
) -> None:
    cp, http = mocked_cp_client
    try:
        # 1. FakeDriver with a scripted streaming handler that emits the
        # HelloAdapter's expected JSONL line.
        streaming = scripted_streaming_handler(
            stdout_chunks=[b'{"line": "hello from solve fizzbuzz"}\n'],
            stderr_chunks=[],
            return_code=0,
        )
        driver = FakeDriver(streaming_handler=streaming)
        await driver.start()

        # 2. Construct SubprocessAgent around the HelloAdapter.
        hello = get_adapter("hello")
        assert hello is not None
        trial_id = uuid4()
        team_id = uuid4()
        agent = SubprocessAgent(
            adapter=hello,
            model=ModelSpec(provider="openai", name="gpt-5"),
            cp_client=cp,
            gateway_url="http://gw",
            team_id=team_id,
            trial_id=trial_id,
        )

        # 3. Run the agent. TrajectoryWriter writes to a local JSONL
        # and flushes to a FakeObjectStore at close.
        store = FakeObjectStore()
        async with TrajectoryWriter(
            local_path=tmp_path / "trajectory.jsonl",
            store=store,
            bucket="trajectories",
            key=f"{team_id}/{trial_id}/events.jsonl",
            min_part_bytes=0,   # let small test events flush
        ) as trajectory:
            await agent.run(
                instruction="solve fizzbuzz",
                env=driver,
                trajectory=trajectory,
                mcp=[],
                skills_dir=None,
                step_id="main",
            )

        # 4. The local JSONL contains the adapter's event.
        local_lines = (tmp_path / "trajectory.jsonl").read_text().splitlines()
        assert len(local_lines) == 1
        assert "hello from solve fizzbuzz" in local_lines[0]

        # 5. The ObjectStore received the upload.
        assert ("trajectories", f"{team_id}/{trial_id}/events.jsonl") in store.objects

        await driver.stop()
    finally:
        await http.aclose()


async def test_subprocess_agent_raises_on_agent_exit_nonzero(
    tmp_path: Path, mocked_cp_client,
) -> None:
    cp, http = mocked_cp_client
    try:
        streaming = scripted_streaming_handler(
            stdout_chunks=[b'{"line": "partial"}\n'],
            stderr_chunks=[],
            return_code=2,
        )
        driver = FakeDriver(streaming_handler=streaming)
        await driver.start()
        hello = get_adapter("hello")
        assert hello is not None
        agent = SubprocessAgent(
            adapter=hello,
            model=ModelSpec(provider="openai", name="gpt-5"),
            cp_client=cp,
            gateway_url="http://gw",
            team_id=uuid4(),
            trial_id=uuid4(),
        )
        store = FakeObjectStore()
        from loom.errors import AgentError
        async with TrajectoryWriter(
            local_path=tmp_path / "trajectory.jsonl",
            store=store,
            bucket="trajectories",
            key="t/t/events.jsonl",
            min_part_bytes=0,
        ) as trajectory:
            with pytest.raises(AgentError, match=r"exited rc=2"):
                await agent.run(
                    instruction="x",
                    env=driver,
                    trajectory=trajectory,
                    mcp=[],
                    skills_dir=None,
                    step_id="main",
                )
        await driver.stop()
    finally:
        await http.aclose()
