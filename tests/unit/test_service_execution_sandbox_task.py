from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.service_execution_sandbox_task import run_agent, run_verifier
from tests.unit.test_service_execution_terminus_plan import _inputs


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot_fails", [False, True])
async def test_deadline_timeout_handoff_requires_completed_snapshot(
    tmp_path, monkeypatch, snapshot_fails,
):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("work")
    driver = Sandbox()
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    epoch = time.time() + 0.05
    monkeypatch.setenv("LOOM_EXECUTION_PHASE_DEADLINE", str(epoch))
    monkeypatch.setenv("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "1")
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        assert kwargs["deadline"].wall_deadline_epoch_sec == epoch
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        driver.filesystem[PurePosixPath("/app/answer.txt")] = b"partial answer"
        await asyncio.sleep(10)

    async def failed_snapshot(*args):
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    if snapshot_fails:
        monkeypatch.setattr(module, "_export_workspace_archive", failed_snapshot)
    expected = RuntimeError if snapshot_fails else module.AgentTimeoutFinalizedError
    with pytest.raises(expected):
        await module.run_agent(tmp_path, task, trial)
    assert driver.quiesced and driver.state == "stopped"
    assert (tmp_path / ".loom/agent/usage.json").is_file()
    assert (tmp_path / ".loom/workspace.tar").is_file() is not snapshot_fails


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["deadline_signal", "early_cancel", "second_signal", "stuck_snapshot", "late_unwind"])
async def test_signal_and_finalization_budget_do_not_grant_unsafe_handoff(
    tmp_path, monkeypatch, mode,
):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("work")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setenv("LOOM_EXECUTION_PHASE_DEADLINE", str(time.time() + 0.05))
    monkeypatch.setenv("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "0.1")
    callbacks = []
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda _, callback: callbacks.append(callback))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)

    class SignalSandbox(Sandbox):
        async def export_workspace_archive(self, src, dst):
            if mode in {"deadline_signal", "second_signal"}:
                callbacks[0]()  # Same deadline signal must not interrupt cleanup.
            if mode == "second_signal":
                callbacks[0]()  # A second stop cancels; never authorize verifier.
                await asyncio.sleep(0)
            if mode == "stuck_snapshot":
                await asyncio.sleep(10)
            if mode == "late_unwind":
                await asyncio.sleep(0.01)
            await super().export_workspace_archive(src, dst)

    driver = SignalSandbox()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        if mode == "early_cancel":
            callbacks[0]()
        try:
            await asyncio.sleep(10)
        finally:
            if mode == "late_unwind":
                # Slow unwinding cannot restart the grace period.
                time.sleep(0.2)

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    expected = {
        "deadline_signal": module.AgentTimeoutFinalizedError,
        "early_cancel": asyncio.CancelledError,
        "second_signal": asyncio.CancelledError,
        "stuck_snapshot": TimeoutError,
        "late_unwind": TimeoutError,
    }[mode]
    with pytest.raises(expected) as caught:
        await module.run_agent(tmp_path, task, trial)
    if mode != "deadline_signal":
        assert not isinstance(caught.value, module.AgentTimeoutFinalizedError)
    assert driver.quiesced and driver.state == "stopped"


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


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_during_snapshot", [False, True])
async def test_successful_agent_snapshot_crossing_deadline_still_hands_off(
    tmp_path, monkeypatch, signal_during_snapshot,
):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("work")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setenv("LOOM_EXECUTION_PHASE_DEADLINE", str(time.time() + 0.05))
    monkeypatch.setenv("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "1")
    callbacks = []
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda _, callback: callbacks.append(callback))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)

    class SlowSnapshotSandbox(Sandbox):
        async def export_workspace_archive(self, src, dst):
            await asyncio.sleep(0.08)
            if signal_during_snapshot:
                callbacks[0]()
            await super().export_workspace_archive(src, dst)

    driver = SlowSnapshotSandbox()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        assert not kwargs["deadline"].reached
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        # Agent finishes before its cutoff; only the snapshot crosses it.

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    with pytest.raises(module.AgentTimeoutFinalizedError):
        await module.run_agent(tmp_path, task, trial)
    assert driver.quiesced and driver.state == "stopped"
    assert (tmp_path / ".loom/workspace.tar").is_file()
