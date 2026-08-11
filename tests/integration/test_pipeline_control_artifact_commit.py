from __future__ import annotations

from loom.pipeline.artifact_commit import ArtifactCommitService
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import final_producer, plan, upload_all


async def test_platform_output_shares_final_atomic_marker() -> None:
    service = ArtifactCommitService(store=FakeObjectStore(), bucket="artifacts")
    _grant, result = await upload_all(
        service,
        producer=final_producer(),
        planned=[
            plan(
                name="fanout",
                artifact_type="loom.fanout-manifest.v1",
                producer="platform",
            )
        ],
        payloads=[b"{}\n"],
    )
    assert result.state == "committed_ready"
