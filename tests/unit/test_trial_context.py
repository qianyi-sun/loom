from pathlib import Path
from uuid import uuid4

from loom.agent.oracle import OracleAgent
from loom.driver.fake import FakeDriver
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.trajectory.storage import FakeObjectStore
from loom.trial.trial import TrialContext
from loom.verifier.pytest_verifier import PytestVerifier
from tests._trial_config_defaults import stub_trial_config


def test_trial_context_construction(tmp_path: Path):
    task = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
        steps=[StepConfig(name="main")],
    )
    trial_id = uuid4()
    ctx = TrialContext(
        trial_id=trial_id,
        team_id=uuid4(),
        task_config=task,
        task_checksum="0" * 64,
        task_dir=tmp_path,
        trial_config=stub_trial_config(),
        driver=FakeDriver(),
        agent=OracleAgent(task_dir=tmp_path, trial_id=trial_id),
        verifier=PytestVerifier(),
        object_store=FakeObjectStore(),
        local_trajectory_path=tmp_path / "events.jsonl",
    )
    assert ctx.task_id == "t"
    assert ctx.trajectory_bucket == "trajectories"
    assert ctx.trajectory_key.startswith(str(ctx.team_id))
    assert ctx.trajectory_uri.startswith("s3://trajectories/")
