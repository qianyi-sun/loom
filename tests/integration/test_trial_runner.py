"""Integration: LocalTrialRunner wraps Trial.run() and propagates state via
the callback. Uses Plan 1+2+3 stack with fakes."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.oracle import OracleAgent
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult
from loom.models.result import FailureReason, TrialState
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.verifier import VerifierResult
from loom.trajectory.storage import FakeObjectStore
from loom_worker.trial_runner import LocalTrialRunner
from tests._trial_config_defaults import stub_trial_config


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


def _task_config(*, artifacts: list[str] | None = None) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="hello", name="hello"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main", artifacts=artifacts or [])],
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
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name:
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
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name:
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


async def test_runner_marks_failed_when_trajectory_upload_cannot_start(  # type: ignore[no-untyped-def]
    hello_task, tmp_path: Path,
):
    """A missing trajectories bucket makes multipart upload creation fail.

    The worker-facing runner must convert that into a terminal failed
    state instead of letting the background task crash and leave the CP
    row stuck in running.
    """

    class MissingBucketStore(FakeObjectStore):
        async def create_multipart_upload(self, *, bucket: str, key: str):  # type: ignore[no-untyped-def]
            raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")

        async def put_object(self, *, bucket: str, key: str, body: bytes):  # type: ignore[no-untyped-def]
            raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")

    calls: list[tuple[str, str | None]] = []

    async def fake_state_patch(state: str, failure_reason: str | None) -> bool:
        calls.append((state, failure_reason))
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
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name:
            OracleAgent(task_dir=task_dir, trial_id=trial_id),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=MissingBucketStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
    )

    result = await runner.run()

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.TRAJECTORY_FLUSH_FAILED
    assert ("failed", FailureReason.TRAJECTORY_FLUSH_FAILED.value) in calls


async def test_runner_marks_failed_when_artifact_upload_fails(  # type: ignore[no-untyped-def]
    hello_task, tmp_path: Path,
):
    """Artifact persistence is part of platform success.

    The verifier may still return a reward from the sandbox workspace, but
    if a declared artifact cannot be persisted to object storage, the worker
    must not report the trial as succeeded.
    """

    class MissingArtifactBucketStore(FakeObjectStore):
        async def put_object(self, *, bucket: str, key: str, body: bytes) -> str:
            if bucket == "artifacts":
                raise RuntimeError(f"NoSuchBucket: {bucket}/{key}")
            return await super().put_object(bucket=bucket, key=key, body=body)

    calls: list[tuple[str, str | None]] = []

    async def fake_state_patch(state: str, failure_reason: str | None) -> bool:
        calls.append((state, failure_reason))
        return True

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith("find "):
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/result.txt\x00",
                stderr=b"",
                truncated=False,
                duration_sec=0.01,
            )
        return ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        )

    trial_id = uuid4()
    driver = FakeDriver(exec_handler=handler)
    driver.filesystem[PurePosixPath("/workspace/result.txt")] = b"hello"
    runner = LocalTrialRunner(
        trial_id=trial_id, team_id=uuid4(),
        task_config=_task_config(artifacts=["result.txt"]),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=lambda: driver,
        agent_factory=lambda task_dir, _gw, _model, _name:
            OracleAgent(task_dir=task_dir, trial_id=trial_id),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=MissingArtifactBucketStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
    )

    result = await runner.run()

    assert result.state == TrialState.FAILED
    assert result.failure_reason == FailureReason.ARTIFACT_UPLOAD_FAILED
    assert result.steps[0].error is not None
    assert result.steps[0].error.phase == "artifacts"
    assert ("failed", FailureReason.ARTIFACT_UPLOAD_FAILED.value) in calls
    assert ("succeeded", None) not in calls


async def test_runner_projects_successful_trial_outputs(  # type: ignore[no-untyped-def]
    hello_task, tmp_path: Path,
):
    state_calls: list[tuple[str, str | None]] = []
    projection_calls: list[dict[str, Any]] = []

    async def fake_state_patch(state: str, failure_reason: str | None) -> bool:
        state_calls.append((state, failure_reason))
        return True

    async def fake_output_projection(
        result_payload: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        projection_calls.append({
            "result": result_payload,
            "trajectory_index": trajectory_index,
        })
        return True

    def handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd.startswith("find "):
            return ExecResult(
                return_code=0,
                stdout=b"/workspace/result.txt\x00",
                stderr=b"",
                truncated=False,
                duration_sec=0.01,
            )
        return ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        )

    trial_id = uuid4()
    team_id = uuid4()
    driver = FakeDriver(exec_handler=handler)
    driver.filesystem[PurePosixPath("/workspace/result.txt")] = b"hello"
    runner = LocalTrialRunner(
        trial_id=trial_id, team_id=team_id,
        task_config=_task_config(artifacts=["result.txt"]),
        task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(),
        driver_factory=lambda: driver,
        agent_factory=lambda task_dir, _gw, _model, _name:
            OracleAgent(task_dir=task_dir, trial_id=trial_id),
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=fake_state_patch,
        output_projection_callback=fake_output_projection,
    )

    result = await runner.run()

    assert result.state == TrialState.SUCCEEDED
    assert ("succeeded", None) in state_calls
    assert len(projection_calls) == 1
    projection = projection_calls[0]
    assert projection["result"]["state"] == "succeeded"
    assert projection["result"]["aggregate_reward"] == 1.0
    assert (
        projection["trajectory_index"]["trajectory_uri"]
        == result.trajectory_uri
    )
    assert projection["trajectory_index"]["atif_uri"] == result.atif_uri
    assert projection["trajectory_index"]["artifacts"] == [{
        "step_name": "main",
        "bucket": "artifacts",
        "key": f"{team_id}/{trial_id}/main/result.txt",
        "size": 5,
    }]


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
        trial_config=stub_trial_config(),
        driver_factory=_driver_factory(handler),
        agent_factory=lambda task_dir, _gw, _model, _name:
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


async def test_trial_config_agent_and_model_drive_the_factory(  # type: ignore[no-untyped-def]
    hello_task, tmp_path: Path,
):
    """Plan 23: TrialConfig.agent_name + agent_model are required and
    used directly. The worker NEVER falls back to TaskConfig.agent.*
    for service-mode trials — every submission carries explicit
    agent + model identity."""
    from loom.models.types import ModelSpec

    captured: dict[str, Any] = {}

    def capture_factory(task_dir, _gw, model, name):  # type: ignore[no-untyped-def]
        captured["name"] = name
        captured["model"] = model
        return OracleAgent(task_dir=task_dir, trial_id=uuid4())

    handler = command_table_handler({
        "chmod +x /workspace/solve.sh && /workspace/solve.sh": ExecResult(
            return_code=0, stdout=b"hello\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })
    explicit_model = ModelSpec(provider="anthropic", name="claude-opus-4-7")

    async def noop_patch(state: str, _fr: str | None) -> bool:
        return True

    # Task says oracle/None; TrialConfig says claude-code-inbox/claude-opus-4-7
    # — the factory MUST see the TrialConfig values, not the task's.
    runner = LocalTrialRunner(
        trial_id=uuid4(), team_id=uuid4(),
        task_config=_task_config(), task_checksum="0" * 64,
        task_dir=hello_task,
        trial_config=stub_trial_config(
            agent_name="claude-code-inbox",
            agent_model=explicit_model,
        ),
        driver_factory=_driver_factory(handler),
        agent_factory=capture_factory,
        verifier_factory=lambda: _AlwaysPassVerifier(),  # type: ignore[return-value]
        object_store=FakeObjectStore(),
        gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path / "trajectories",
        state_patch_callback=noop_patch,
    )
    await runner.run()
    assert captured["name"] == "claude-code-inbox"
    assert captured["model"] == explicit_model
