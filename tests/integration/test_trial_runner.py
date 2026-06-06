"""Integration: LocalTrialRunner wraps Trial.run() and propagates state via
the callback. Uses Plan 1+2+3 stack with fakes."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.oracle import OracleAgent
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult
from loom.models.result import TrialState
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.trial import TrialConfig
from loom.models.verifier import VerifierResult
from loom.trajectory.storage import FakeObjectStore
from loom_worker.trial_runner import LocalTrialRunner


class _AlwaysPassVerifier:
    name = "pass"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 1.0})


@pytest.fixture
def hello_task(tmp_path: Path) -> Path:
    d = tmp_path / "task"
    d.mkdir()
    (d / "task.toml").write_text('schema_version = "1"\n')
    (d / "instruction.md").write_text("say hello\n")
    sol = d / "solution"
    sol.mkdir()
    (sol / "solve.sh").write_text("#!/bin/sh\necho hello\n")
    (sol / "solve.sh").chmod(0o755)
    return d


def _task_config() -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )


def _driver_factory(handler: Any) -> Any:
    return lambda: FakeDriver(exec_handler=handler)


async def test_runner_invokes_run_and_reports_states(  # type: ignore[no-untyped-def]
    hello_task, tmp_path: Path,
):
    state_calls: list[tuple[str, str | None]] = []

    async def fake_state_patch(state: str, failure_reason: str | None) -> bool:
        state_calls.append((state, failure_reason))
        return True

    handler = command_table_handler({
        "chmod +x /workspace/solve.sh && /workspace/solve.sh": ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })

    trial_id = uuid4()
    runner = LocalTrialRunner(
        trial_id=trial_id, team_id=uuid4(),
        task_config=_task_config(), task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=TrialConfig(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model:
            OracleAgent(task_dir=task_dir, trial_id=trial_id),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
    )

    result = await runner.run()
    assert result.state == TrialState.SUCCEEDED
    assert ("running", None) in state_calls
    assert ("succeeded", None) in state_calls


async def test_runner_swallows_state_patch_exception(  # type: ignore[no-untyped-def]
    hello_task, tmp_path: Path,
):
    """A transient PATCH failure must not crash the trial body. The runner
    logs the warning and lets Trial.run() continue."""
    calls: list[tuple[str, str | None]] = []

    async def flaky_patch(state: str, fr: str | None) -> bool:
        calls.append((state, fr))
        if state == "running":
            raise RuntimeError("simulated network blip")
        return True

    handler = command_table_handler({
        "chmod +x /workspace/solve.sh && /workspace/solve.sh": ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })
    trial_id = uuid4()
    runner = LocalTrialRunner(
        trial_id=trial_id, team_id=uuid4(),
        task_config=_task_config(), task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=TrialConfig(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model:
            OracleAgent(task_dir=task_dir, trial_id=trial_id),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=flaky_patch,
    )

    result = await runner.run()
    assert result.state == TrialState.SUCCEEDED
    assert ("running", None) in calls
    assert ("succeeded", None) in calls


async def test_runner_logs_fenced_response(  # type: ignore[no-untyped-def]
    hello_task, tmp_path: Path, caplog,
):
    async def fenced_patch(state: str, fr: str | None) -> bool:
        return state != "running"  # Pretend `running` PATCH was fenced

    handler = command_table_handler({
        "chmod +x /workspace/solve.sh && /workspace/solve.sh": ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })
    trial_id = uuid4()
    runner = LocalTrialRunner(
        trial_id=trial_id, team_id=uuid4(),
        task_config=_task_config(), task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=TrialConfig(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model:
            OracleAgent(task_dir=task_dir, trial_id=trial_id),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fenced_patch,
    )

    import logging
    with caplog.at_level(logging.WARNING, logger="loom_worker.trial_runner"):
        await runner.run()
    assert any("state_patch_fenced" in m for m in caplog.messages)
