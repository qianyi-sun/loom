from pathlib import Path
from uuid import uuid4

import pytest

from loom.agent.oracle import OracleAgent
from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver, command_table_handler
from loom.models.exec import ExecResult
from loom.models.networking import Public
from loom.models.result import StepResult
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.models.trajectory import EventKind
from loom.models.trial import TrialConfig
from loom.models.verifier import VerifierResult
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.step_runner import run_step
from loom.trial.trial import TrialContext


class _AlwaysPassVerifier:
    name = "pass"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return VerifierResult(rewards={"passed": 1.0})


@pytest.fixture
async def context(tmp_path: Path) -> TrialContext:
    sol = tmp_path / "solution"
    sol.mkdir()
    (sol / "solve.sh").write_text("#!/bin/sh\necho ok\n")
    (sol / "solve.sh").chmod(0o755)

    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pass"),
        steps=[StepConfig(name="main")],
    )
    handler = command_table_handler({
        "chmod +x /workspace/solve.sh && /workspace/solve.sh": ExecResult(
            return_code=0, stdout=b"ok\n", stderr=b"",
            truncated=False, duration_sec=0.05,
        ),
    })
    driver = FakeDriver(exec_handler=handler)
    await driver.start(options=StartOptions())

    trial_id = uuid4()
    return TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=tmp_path,
        trial_config=TrialConfig(),
        driver=driver,
        agent=OracleAgent(task_dir=tmp_path, trial_id=trial_id),
        verifier=_AlwaysPassVerifier(),
        object_store=FakeObjectStore(),
        local_trajectory_path=tmp_path / "events.jsonl",
    )


async def test_run_step_happy_path(context: TrialContext, tmp_path: Path):
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket, key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context, step=context.task_config.steps[0],
            trajectory=writer, baseline_policy=Public(),
        )
    assert isinstance(sr, StepResult)
    assert sr.verifier_result is not None
    assert sr.verifier_result.rewards["passed"] == 1.0
    assert sr.error is None

    reader = TrajectoryReader(context.local_trajectory_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.STEP_START in kinds
    assert EventKind.STEP_END in kinds


async def test_run_step_records_agent_error(context: TrialContext, tmp_path: Path):
    """If the agent raises AgentError, run_step records it as a phase=agent
    StepError and still emits step_end."""
    from loom.errors import AgentError

    class _BoomAgent:
        mode = "out-of-box"
        name = "boom"
        version = "1.0"
        supports_os = frozenset({"linux"})
        model = None

        async def run(self, **_):  # type: ignore[no-untyped-def]
            raise AgentError("boom")

    context.agent = _BoomAgent()  # type: ignore[assignment]
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket, key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context, step=context.task_config.steps[0],
            trajectory=writer, baseline_policy=Public(),
        )
    assert sr.error is not None
    assert sr.error.phase == "agent"
    assert sr.error.reason == "exception"
    # step_end still fires.
    reader = TrajectoryReader(context.local_trajectory_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.STEP_END in kinds


async def test_run_step_records_verifier_exception(context: TrialContext):
    """Regression for Bug 3: previously only TimeoutError was caught around
    the verifier. A VerifierError used to escape step_runner entirely,
    bypassing per-step error tracking and the StepEndEvent emission."""
    from loom.errors import VerifierError

    class _BoomVerifier:
        name = "boom"

        async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
            raise VerifierError("simulated")

    context.verifier = _BoomVerifier()  # type: ignore[assignment]
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket, key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context, step=context.task_config.steps[0],
            trajectory=writer, baseline_policy=Public(),
        )
    assert sr.error is not None
    assert sr.error.phase == "verifier"
    assert sr.error.reason == "exception"
    reader = TrajectoryReader(context.local_trajectory_path)
    kinds = [e.kind for e in reader.iter_all()]
    assert EventKind.STEP_END in kinds


async def test_run_step_skip_verifier(context: TrialContext):
    """trial_config.skip_verifier=True omits the verifier phase entirely."""
    context.trial_config = TrialConfig(skip_verifier=True)
    writer = TrajectoryWriter(
        local_path=context.local_trajectory_path,
        store=context.object_store,
        bucket=context.trajectory_bucket, key=context.trajectory_key,
        min_part_bytes=0,
    )
    async with writer:
        sr = await run_step(
            ctx=context, step=context.task_config.steps[0],
            trajectory=writer, baseline_policy=Public(),
        )
    assert sr.verifier_result is None
