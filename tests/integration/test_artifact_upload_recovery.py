from __future__ import annotations

from loom.pipeline.artifact_commit import (
    ArtifactCommitService,
    InMemoryArtifactCommitRepository,
    UploadAuthV1,
)
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import chunks, digest, final_producer, plan


async def test_restart_reuses_persisted_part_and_converges() -> None:
    store = FakeObjectStore()
    repository = InMemoryArtifactCommitRepository()
    first = ArtifactCommitService(store=store, bucket="artifacts", repository=repository)
    payload = b"restart-safe\n"
    grant = await first.prepare_session(
        producer=final_producer(),
        files=[plan(payload=payload)],
        idempotency_key="restart",
        request_digest="sha256:" + "4" * 64,
    )
    auth = UploadAuthV1(upload_token=grant.upload_token)
    receipt = await first.write_part(
        session_id=grant.upload_session_id,
        file_index=0,
        part_number=1,
        content_length=len(payload),
        content_sha256=digest(payload),
        body=chunks(payload),
        auth=auth,
    )
    restarted = ArtifactCommitService(store=store, bucket="artifacts", repository=repository)
    await restarted.complete_file(
        session_id=grant.upload_session_id,
        file_index=0,
        ordered_parts=[receipt],
        auth=auth,
    )
    assert (
        await restarted.commit_session(session_id=grant.upload_session_id, auth=auth)
    ).state == ("committed_ready")
