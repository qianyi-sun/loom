"""SubprocessAgent end-to-end against HelloAdapter + FakeDriver + a
mocked Control Plane mint endpoint (Plan 11 Task 4).

Verifies the full chain: mint step JWT → exec_streaming launches the
agent → adapter capture_events yields events → trajectory writer's
write_raw_dict appends them to the local JSONL → finalize uploads → the
ObjectStore has the trajectory.
"""

from __future__ import annotations

import json
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

pytestmark = pytest.mark.docker


@pytest.fixture
def mocked_cp_client() -> tuple[HttpControlPlaneClient, httpx.AsyncClient]:
    """Build an HttpControlPlaneClient pointed at a MockTransport that
    handles POST /admin/step-tokens with a canned JWT response."""

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/admin/step-tokens":
            return httpx.Response(
                201,
                json={
                    "token": "loom_step_test-mocked-token",
                    "expires_at": "2026-06-07T00:00:00Z",
                },
            )
        return httpx.Response(404)

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        base_url="http://cp",
    )
    cp = HttpControlPlaneClient(
        base_url="http://cp",
        token="fake-worker-token",
        _client=http,
    )
    return cp, http


async def test_subprocess_agent_runs_hello_adapter_end_to_end(
    tmp_path: Path,
    mocked_cp_client,
) -> None:
    cp, http = mocked_cp_client
    try:
        # 1. FakeDriver with a scripted streaming handler that emits the
        # HelloAdapter's expected JSONL line.
        streaming = scripted_streaming_handler(
            stdout_chunks=[b'{"line": "hello from solve fizzbuzz"}\n'],
            stderr_chunks=[b"bad flag\n"],
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
            min_part_bytes=0,  # let small test events flush
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
        event = json.loads(local_lines[0])
        assert event["kind"] == "agent_thought"
        assert event["content"] == "hello from solve fizzbuzz"

        # 5. The ObjectStore received the upload.
        assert ("trajectories", f"{team_id}/{trial_id}/events.jsonl") in store.objects

        await driver.stop()
    finally:
        await http.aclose()


async def test_subprocess_agent_can_use_sandbox_facing_gateway_url(
    tmp_path: Path,
    mocked_cp_client,
) -> None:
    """#379: subprocess agents run inside the trial sandbox, so their
    OpenAI SDK base URL must be configurable separately from the worker's
    own gateway URL."""
    cp, http = mocked_cp_client
    captured_env: dict[str, str] = {}

    def _capture_streaming(argv, env_vars, cwd, user):
        captured_env.update(env_vars)
        return scripted_streaming_handler(
            stdout_chunks=[b'{"line": "ok"}\n'],
            stderr_chunks=[],
            return_code=0,
        )(argv, env_vars, cwd, user)

    try:
        driver = FakeDriver(streaming_handler=_capture_streaming)
        await driver.start()
        hello = get_adapter("hello")
        assert hello is not None
        agent = SubprocessAgent(
            adapter=hello,
            model=ModelSpec(provider="openai", name="gpt-5"),
            cp_client=cp,
            gateway_url="http://worker-only-gateway:9100",
            agent_gateway_url="http://host.docker.internal:30443/openai/v1",
            team_id=uuid4(),
            trial_id=uuid4(),
        )
        store = FakeObjectStore()

        async with TrajectoryWriter(
            local_path=tmp_path / "trajectory.jsonl",
            store=store,
            bucket="trajectories",
            key="t/t/events.jsonl",
            min_part_bytes=0,
        ) as trajectory:
            await agent.run(
                instruction="x",
                env=driver,
                trajectory=trajectory,
                mcp=[],
                skills_dir=None,
                step_id="main",
            )

        assert captured_env["OPENAI_BASE_URL"] == (
            "http://host.docker.internal:30443/openai/v1"
        )
        assert captured_env["OPENAI_API_KEY"] == "loom_step_test-mocked-token"
        await driver.stop()
    finally:
        await http.aclose()


async def test_subprocess_agent_raises_on_agent_exit_nonzero(
    tmp_path: Path,
    mocked_cp_client,
) -> None:
    cp, http = mocked_cp_client
    try:
        streaming = scripted_streaming_handler(
            stdout_chunks=[b'{"line": "partial"}\n'],
            stderr_chunks=[b"bad flag\n"],
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
            with pytest.raises(AgentError, match=r"exited rc=2.*bad flag"):
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


async def test_subprocess_agent_raises_when_all_output_malformed(
    tmp_path: Path,
    mocked_cp_client,
) -> None:
    """#321: bfcl/hello reproduction. Process exits rc=0 but every
    line on stdout is unparseable JSONL — previously the trial would
    end up failed with empty failure_message. Now SubprocessAgent
    raises AgentError with the skipped-line summary so the worker can
    persist an actionable failure_message."""
    cp, http = mocked_cp_client
    try:
        streaming = scripted_streaming_handler(
            stdout_chunks=[
                b'this is not json\n',
                b'neither is this { unbalanced\n',
            ],
            stderr_chunks=[],
            return_code=0,
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
            with pytest.raises(
                AgentError, match=r"emitted no parseable events.*malformed",
            ):
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


async def test_subprocess_agent_redacts_secrets_from_capture_sample(
    tmp_path: Path,
    mocked_cp_client,
) -> None:
    """#321: last_skip_sample may contain an env-leaked provider API
    key or bearer token if the agent's stdout accidentally echoed one.
    SubprocessAgent must redact via loom.security.redaction before
    the bytes hit AgentError.message → persisted failure_message."""
    cp, http = mocked_cp_client
    try:
        # Malformed JSON line that *would* contain a real-looking
        # OpenAI-style key if not redacted.
        streaming = scripted_streaming_handler(
            stdout_chunks=[
                b'oops not json: api_key=sk-DEADBEEF12345abc\n',
            ],
            stderr_chunks=[],
            return_code=0,
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
            with pytest.raises(AgentError) as exc:
                await agent.run(
                    instruction="x",
                    env=driver,
                    trajectory=trajectory,
                    mcp=[],
                    skills_dir=None,
                    step_id="main",
                )
        msg = str(exc.value)
        # The bad line should be surfaced
        assert "first bad line" in msg
        # But the API-key-shaped secret must not leak verbatim
        assert "sk-DEADBEEF12345abc" not in msg
        assert "[REDACTED:api-key]" in msg
        await driver.stop()
    finally:
        await http.aclose()


async def test_subprocess_agent_redacts_secrets_from_stderr_tail(
    tmp_path: Path,
    mocked_cp_client,
) -> None:
    """#321: stderr_tail from a failed agent may also carry the
    LOOM_STEP_TOKEN that the worker put in env. Redact before AgentError."""
    cp, http = mocked_cp_client
    try:
        streaming = scripted_streaming_handler(
            stdout_chunks=[b'{"line": "ok"}\n'],
            stderr_chunks=[
                b'Traceback ... Authorization: Bearer loom_step_RAW-SECRET-XYZ\n',
            ],
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
            with pytest.raises(AgentError) as exc:
                await agent.run(
                    instruction="x",
                    env=driver,
                    trajectory=trajectory,
                    mcp=[],
                    skills_dir=None,
                    step_id="main",
                )
        msg = str(exc.value)
        assert "exited rc=2" in msg
        assert "loom_step_RAW-SECRET-XYZ" not in msg
        # Bearer regex captures both the Bearer prefix and the loom_token form
        assert "[REDACTED" in msg
        await driver.stop()
    finally:
        await http.aclose()


async def test_subprocess_agent_includes_capture_warning_in_rc_nonzero(
    tmp_path: Path,
    mocked_cp_client,
) -> None:
    """#321: when both rc!=0 AND there were malformed lines, the
    AgentError message includes both the rc/stderr AND the capture
    warning summary so operators see the full picture."""
    cp, http = mocked_cp_client
    try:
        streaming = scripted_streaming_handler(
            stdout_chunks=[b'garbage line\n'],
            stderr_chunks=[b"crashed\n"],
            return_code=1,
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
            with pytest.raises(AgentError) as exc:
                await agent.run(
                    instruction="x",
                    env=driver,
                    trajectory=trajectory,
                    mcp=[],
                    skills_dir=None,
                    step_id="main",
                )
        msg = str(exc.value)
        assert "exited rc=1" in msg
        assert "crashed" in msg
        assert "capture:" in msg and "malformed" in msg
        await driver.stop()
    finally:
        await http.aclose()
