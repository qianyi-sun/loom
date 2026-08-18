from __future__ import annotations

from loom.pipeline.artifact_commit import ArtifactCommitService, UploadAuthV1
from loom.pipeline.keys import canonical_document
from loom.trajectory.storage import FakeObjectStore
from tests.integration.pipeline_artifact_testkit import chunks, digest, final_producer, plan


async def test_platform_output_shares_final_atomic_marker() -> None:
    store = FakeObjectStore()
    service = ArtifactCommitService(store=store, bucket="artifacts")
    container_payload = b'{"schema_version":"loom.platform-fanout-index.v1","items":[]}\n'
    platform_value = {"schema_version": "loom.fanout-manifest.v1", "items": []}
    planned = [
        plan(
            index=0,
            name="index",
            artifact_type="loom.platform-fanout-index.v1",
            payload=container_payload,
        ),
        plan(
            index=1,
            name="fanout",
            artifact_type="loom.fanout-manifest.v1",
            producer="platform",
        ).model_copy(update={"expected_sha256": None, "expected_size": None}),
    ]
    grant = await service.prepare_session(
        producer=final_producer(),
        files=planned,
        idempotency_key="platform-fanout",
        request_digest="sha256:" + "a" * 64,
    )
    auth = UploadAuthV1(upload_token=grant.upload_token)
    receipt = await service.write_part(
        session_id=grant.upload_session_id,
        file_index=0,
        part_number=1,
        content_length=len(container_payload),
        content_sha256=digest(container_payload),
        body=chunks(container_payload),
        auth=auth,
    )
    await service.complete_file(
        session_id=grant.upload_session_id,
        file_index=0,
        ordered_parts=[receipt],
        auth=auth,
    )
    observed = await service.read_verified_file(
        session_id=grant.upload_session_id,
        file_index=0,
        auth=auth,
        max_bytes=1024,
    )
    assert observed == container_payload
    await service.commit_platform_document(
        session_id=grant.upload_session_id,
        file_index=1,
        value=platform_value,
        auth=auth,
    )
    result = await service.commit_session(
        session_id=grant.upload_session_id,
        auth=auth,
    )
    assert result.state == "committed_ready"
    marker_keys = [key for (_bucket, key) in store.objects if key.endswith("_COMMITTED")]
    assert len(marker_keys) == 1
    platform_payloads = [
        value
        for (_bucket, key), value in store.objects.items()
        if key.endswith("/artifact.json") and b"fanout-manifest" in value
    ]
    assert platform_payloads == [canonical_document(platform_value)]
