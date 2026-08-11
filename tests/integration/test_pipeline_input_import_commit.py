from __future__ import annotations

from uuid import uuid4

from loom.pipeline.artifact_commit import ArtifactCommitService, InputImportProducerV1
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import plan, upload_all


async def test_input_import_has_fixed_archive_and_sidecar_and_stays_unknown() -> None:
    artifact_id = uuid4()
    archive = b"deterministic-zstd"
    sidecar = b'{"schema_version":"behavior.input-import.v1"}\n'
    planned = [
        plan(
            index=0,
            artifact_id=artifact_id,
            path="payload.tar.zst",
            name="dataset",
            artifact_type="behavior.dataset.v1",
            payload=archive,
            role="payload_archive",
            archive_format="tar.zst",
        ),
        plan(
            index=1,
            artifact_id=artifact_id,
            path="artifact.json",
            name="dataset",
            artifact_type="behavior.dataset.v1",
            payload=sidecar,
            producer="service",
        ),
    ]
    producer = InputImportProducerV1(
        commit_kind="input_import",
        team_id=uuid4(),
        pipeline_input_import_id=uuid4(),
        actor_user_id=uuid4(),
    )
    _grant, result = await upload_all(
        ArtifactCommitService(store=FakeObjectStore(), bucket="artifacts"),
        producer=producer,
        planned=planned,
        payloads=[archive, sidecar],
    )
    assert result.artifacts[0].safety_state == "unknown"
