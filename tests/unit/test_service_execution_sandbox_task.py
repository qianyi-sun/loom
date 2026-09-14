from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.service_execution_sandbox_task import run_agent, run_verifier
from tests.unit.test_service_execution_terminus_plan import _inputs


class Sandbox(FakeDriver):
    quiesced = False

    async def stop_processes(self):
        self.quiesced = True

    async def export_workspace_archive(self, src, dst):
        assert self.quiesced
        await super().export_workspace_archive(src, dst)


@pytest.mark.asyncio
@pytest.mark.parametrize("agent_error", [False, True])
async def test_phase_handoff_keeps_tests_private_and_quiesces_before_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, agent_error: bool,
):
    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("Produce answer.txt")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/check.py").write_text("trusted assertions")
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier/check.sh").write_text("trusted verifier")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution/solve.sh").write_text("private oracle")
    agent, verifier = Sandbox(), Sandbox()
    agent.filesystem[PurePosixPath("/app/fixture.txt")] = b"baked fixture"
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", '["answer.txt", "optional_script.py"]')
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver",
                        lambda role, task: agent if role == "task-sandbox" else verifier)

    async def identity(gateway):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        assert agent.filesystem[PurePosixPath("/app/instruction.md")] == b"Produce answer.txt"
        assert not any(path.parts[2] in {"tests", "verifier", "solution", ".loom"}
                       for path in agent.filesystem)
        agent.filesystem[PurePosixPath("/app/answer.txt")] = b"42"
        # A task can create fake private paths, but these never replace evaluator inputs.
        agent.filesystem[PurePosixPath("/app/tests/check.py")] = b"forged assertions"
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        if agent_error:
            raise RuntimeError("agent stopped")

    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    if agent_error:
        with pytest.raises(RuntimeError, match="agent stopped"):
            await run_agent(tmp_path, task, trial)
        assert agent.quiesced and agent.state == "stopped"
        assert (tmp_path / ".loom/workspace.tar").exists()
        return
    await run_agent(tmp_path, task, trial)
    assert (tmp_path / ".loom/collected/answer.txt").read_bytes() == b"42"

    def check(cmd, user, cwd, env):
        assert cwd == PurePosixPath("/app") and user is None
        assert verifier.filesystem[PurePosixPath("/app/tests/check.py")] == b"trusted assertions"
        assert verifier.filesystem[PurePosixPath("/app/answer.txt")] == b"42"
        assert verifier.filesystem[PurePosixPath("/app/fixture.txt")] == b"baked fixture"
        assert not any(str(path).startswith("/app/.loom/") for path in verifier.filesystem)
        verifier.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = b'{"rewards":{"passed":0}}'
        verifier.filesystem[PurePosixPath("/logs/verifier/ctrf.json")] = b'{}'
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    verifier.exec_handler = check
    await run_verifier(tmp_path, task, trial)
    assert verifier.quiesced and verifier.state == "stopped"
    assert json.loads((tmp_path / ".loom/verifier/output.json").read_bytes())["rewards"] == {"passed": 0}
