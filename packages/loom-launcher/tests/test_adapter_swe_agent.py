"""SweAgentAdapter contract: build_invocation + trajectory.jsonl tail."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ExecHandle, ModelSpec, SandboxAccess


class _ScriptedSandbox:
    def __init__(self, snapshots: list[str]) -> None:
        self.snapshots = list(snapshots)
        self.idx = 0

    async def read_text(self, path: PurePosixPath) -> str:
        if self.idx < len(self.snapshots) - 1:
            self.idx += 1
        return self.snapshots[self.idx]

    async def exec_oneshot(
        self,
        argv: list[str],
        *,
        timeout_sec: float = 10.0,
    ) -> tuple[int, bytes]:
        return (1, b"")


def _handle_with_sandbox(
    sandbox: SandboxAccess,
    *,
    runtime_sec: float = 0.3,
) -> ExecHandle:
    async def _empty() -> AsyncIterator[bytes]:
        if False:
            yield b""

    async def _wait() -> int:
        await asyncio.sleep(runtime_sec)
        return 0

    async def _kill() -> None:
        pass

    return ExecHandle(
        pid=0,
        stdout=_empty(),
        stderr=_empty(),
        _wait=_wait,
        _kill=_kill,
        sandbox=sandbox,
    )


def test_build_invocation_argv() -> None:
    adapter = get_adapter("swe-agent")
    assert adapter is not None
    env: dict[str, str] = {"OPENAI_API_BASE": "http://gateway"}
    argv = adapter.build_invocation(
        instruction="fix this issue",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv[:4] == ["sh", "-c", argv[2], "loom-swe-agent"]
    assert argv[4:] == ["/workspace", "fix this issue"]
    assert 'git -C "$repo" init -b main' in argv[2]
    assert 'git -C "$repo" commit -m loom-baseline' in argv[2]
    assert 'remote add origin "$repo"' in argv[2]
    assert "--env.deployment.type local" in argv[2]
    assert "--env.repo.type preexisting" in argv[2]
    assert '--env.repo.repo_name "$repo_name"' in argv[2]
    assert "--agent.model.name openai/gpt-5" in argv[2]
    assert env["OPENAI_API_BASE"] == "http://gateway"


def test_install_and_runtime_do_not_require_python_alias() -> None:
    adapter = get_adapter("swe-agent")
    assert adapter is not None
    assert adapter.install_script is not None

    managed_python = "/opt/loom-agents/swe-agent/bin/python"
    assert "apk add --no-cache python3 py3-pip py3-virtualenv git" in (
        adapter.install_script
    )
    assert (
        "apt-get install -y --no-install-recommends "
        "python3 python3-pip python3-venv git"
    ) in adapter.install_script
    assert "python3 -m venv /opt/loom-agents/swe-agent" in adapter.install_script
    assert f"{managed_python} -m pip install" in adapter.install_script
    assert f'{managed_python} -c "import sweagent"' in adapter.install_script
    assert "\npython " not in adapter.install_script

    argv = adapter.build_invocation(
        instruction="fix this issue",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env={"OPENAI_API_BASE": "http://gateway"},
    )
    assert f"exec {managed_python} -m sweagent.run.run_single" in argv[2]


async def test_capture_via_trajectory_jsonl_tail() -> None:
    adapter = get_adapter("swe-agent")
    assert adapter is not None
    # Real swe-agent trajectory.jsonl: one JSON object per line per step.
    line1 = '{"step": 1, "action": "view file", "thought": "look at code"}'
    line2 = '{"step": 2, "action": "edit", "thought": "patch the bug"}'
    snapshots = [
        "",
        f"{line1}\n",
        f"{line1}\n{line2}\n",
    ]
    sandbox = _ScriptedSandbox(snapshots)
    handle = _handle_with_sandbox(sandbox, runtime_sec=0.4)
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle,
            step_id="main",
            trial_id=uuid4(),
        )
    ]
    # Default line_to_event wraps each line into a {"line": ...} dict.
    seen = [e["line"] for e in events]
    assert line1 in seen
    assert line2 in seen
