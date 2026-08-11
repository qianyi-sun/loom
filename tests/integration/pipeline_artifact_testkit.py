from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from uuid import uuid4

from loom.pipeline.artifact_commit import (
    ArtifactCommitService,
    FinalOutputProducerV1,
    PartReceiptV1,
    UploadAuthV1,
    UploadFilePlanV1,
)


def digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def final_producer() -> FinalOutputProducerV1:
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


def plan(
    *,
    index: int = 0,
    artifact_id=None,
    path: str = "artifact.json",
    name: str = "result",
    artifact_type: str = "behavior.stage-result.v1",
    payload: bytes = b"{}\n",
    role: str = "semantic_document",
    archive_format: str = "none",
    producer: str = "container",
) -> UploadFilePlanV1:
    return UploadFilePlanV1.model_validate(
        {
            "file_index": index,
            "preallocated_artifact_id": artifact_id or uuid4(),
            "relative_path": path,
            "artifact_name": name,
            "artifact_type": artifact_type,
            "producer": producer,
            "media_type": "application/json",
            "role": role,
            "archive_format": archive_format,
            "expected_max_bytes": max(1024, len(payload)),
            "expected_sha256": digest(payload),
            "expected_size": len(payload),
        }
    )


async def chunks(payload: bytes, width: int = 3) -> AsyncIterator[bytes]:
    for offset in range(0, len(payload), width):
        yield payload[offset : offset + width]


async def upload_all(
    service: ArtifactCommitService,
    *,
    producer,
    planned: list[UploadFilePlanV1],
    payloads: list[bytes],
    key: str = "request",
):
    grant = await service.prepare_session(
        producer=producer,
        files=planned,
        idempotency_key=key,
        request_digest="sha256:" + hashlib.sha256(key.encode()).hexdigest(),
    )
    auth = UploadAuthV1(upload_token=grant.upload_token)
    for item, payload in zip(planned, payloads, strict=True):
        receipt: PartReceiptV1 = await service.write_part(
            session_id=grant.upload_session_id,
            file_index=item.file_index,
            part_number=1,
            content_length=len(payload),
            content_sha256=digest(payload),
            body=chunks(payload),
            auth=auth,
        )
        await service.complete_file(
            session_id=grant.upload_session_id,
            file_index=item.file_index,
            ordered_parts=[receipt],
            auth=auth,
        )
    result = await service.commit_session(session_id=grant.upload_session_id, auth=auth)
    return grant, result
