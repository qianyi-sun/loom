from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from loom.pipeline.artifact_commit import (
    ArtifactCommitError,
    ArtifactCommitService,
    FinalOutputProducerV1,
    UploadAuthV1,
    UploadFilePlanV1,
    multipart_part_size,
)
from loom.trajectory.storage import FakeObjectStore


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _producer() -> FinalOutputProducerV1:
    return FinalOutputProducerV1(
        commit_kind="final_output",
        team_id=uuid4(),
        pipeline_run_id=uuid4(),
        pipeline_stage_run_id=uuid4(),
        execution_attempt_id=uuid4(),
        attempt_number=1,
        stage_result_json={},
        stage_result_digest="sha256:" + "1" * 64,
        inventory_digest="sha256:" + "2" * 64,
    )


async def _chunks(*values: bytes):
    for value in values:
        yield value


async def test_multipart_replay_readback_and_marker_order() -> None:
    store = FakeObjectStore()
    service = ArtifactCommitService(store=store, bucket="artifacts")
    payload = b"canonical output\n"
    plan = UploadFilePlanV1(
        file_index=0,
        preallocated_artifact_id=uuid4(),
        relative_path="artifact.json",
        artifact_name="result",
        artifact_type="behavior.stage-result.v1",
        producer="container",
        media_type="application/json",
        role="semantic_document",
        archive_format="none",
        expected_max_bytes=1024,
        expected_sha256=_digest(payload),
        expected_size=len(payload),
    )
    request_digest = "sha256:" + "3" * 64
    grant = await service.prepare_session(
        producer=_producer(), files=[plan], idempotency_key="one", request_digest=request_digest
    )
    auth = UploadAuthV1(upload_token=grant.upload_token)
    receipt = await service.write_part(
        session_id=grant.upload_session_id,
        file_index=0,
        part_number=1,
        content_length=len(payload),
        content_sha256=_digest(payload),
        body=_chunks(payload[:4], payload[4:]),
        auth=auth,
    )
    assert (
        await service.write_part(
            session_id=grant.upload_session_id,
            file_index=0,
            part_number=1,
            content_length=len(payload),
            content_sha256=_digest(payload),
            body=_chunks(b"never-consumed"),
            auth=auth,
        )
        == receipt
    )
    await service.complete_file(
        session_id=grant.upload_session_id,
        file_index=0,
        ordered_parts=[receipt],
        auth=auth,
    )
    sealed = await service.commit_session(session_id=grant.upload_session_id, auth=auth)
    assert sealed.state == "committed_ready"
    prefix = next(
        key for bucket, key in store.objects if bucket == "artifacts" and key.endswith("_COMMITTED")
    )
    assert prefix.removesuffix("_COMMITTED") + "_manifest.json" in {
        key for bucket, key in store.objects if bucket == "artifacts"
    }


async def test_changed_part_replay_conflicts() -> None:
    service = ArtifactCommitService(store=FakeObjectStore(), bucket="artifacts")
    plan = UploadFilePlanV1(
        file_index=0,
        preallocated_artifact_id=uuid4(),
        relative_path="artifact.json",
        artifact_name="result",
        artifact_type="behavior.stage-result.v1",
        producer="container",
        media_type="application/json",
        role="semantic_document",
        archive_format="none",
        expected_max_bytes=100,
        expected_sha256=None,
        expected_size=None,
    )
    grant = await service.prepare_session(
        producer=_producer(),
        files=[plan],
        idempotency_key="one",
        request_digest="sha256:" + "4" * 64,
    )
    auth = UploadAuthV1(upload_token=grant.upload_token)
    await service.write_part(
        session_id=grant.upload_session_id,
        file_index=0,
        part_number=1,
        content_length=1,
        content_sha256=_digest(b"a"),
        body=_chunks(b"a"),
        auth=auth,
    )
    with pytest.raises(ArtifactCommitError, match="part_conflict"):
        await service.write_part(
            session_id=grant.upload_session_id,
            file_index=0,
            part_number=1,
            content_length=1,
            content_sha256=_digest(b"b"),
            body=_chunks(b"b"),
            auth=auth,
        )


def test_one_tib_part_plan_stays_below_s3_limit() -> None:
    size = 1024**4
    part = multipart_part_size(size)
    assert part <= 5 * 1024**3
    assert (size + part - 1) // part <= 9_990
