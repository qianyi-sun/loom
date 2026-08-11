from __future__ import annotations

from uuid import uuid4

from loom.pipeline.artifact_commit import ArtifactCommitService, CheckpointProducerV1
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import plan, upload_all


async def test_checkpoint_commits_without_committed_ready_attempt_gate() -> None:
    producer = CheckpointProducerV1(
        commit_kind="checkpoint",
        team_id=uuid4(),
        pipeline_run_id=uuid4(),
        pipeline_stage_run_id=uuid4(),
        execution_attempt_id=uuid4(),
        attempt_number=1,
        checkpoint_sequence=0,
    )
    service = ArtifactCommitService(store=FakeObjectStore(), bucket="artifacts")
    _grant, result = await upload_all(
        service,
        producer=producer,
        planned=[
            plan(
                path="checkpoint.json",
                name="checkpoint-000000000000",
                artifact_type="loom.checkpoint.v1",
            )
        ],
        payloads=[b"{}\n"],
    )
    assert result.state == "committed"
