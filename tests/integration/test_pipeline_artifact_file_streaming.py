from __future__ import annotations

import base64
import io
from datetime import UTC, datetime
from hashlib import sha256
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from loom.db.schema import (
    Artifact,
    ArtifactUploadFile,
    ArtifactUploadSession,
    PipelineRun,
    PipelineStageRun,
)
from loom.pipeline.artifact_commit import (
    ArtifactCommitManifestV1,
    ArtifactCommitMarkerV1,
    ArtifactManifestV1,
    RootArtifactRecordV1,
    StoredFileV1,
)
from loom.pipeline.keys import canonical_document, digest_bytes
from loom_service.pipeline_artifact_files import (
    public_artifact_projection,
    resolve_public_artifact,
    stream_public_artifact_file,
)


class _Body(io.BytesIO):
    pass


class _Store:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.ranges: list[str | None] = []

    def head_object(self, **kwargs):
        assert kwargs["Bucket"] == "artifacts" and kwargs["ChecksumMode"] == "ENABLED"
        payload = self.objects[kwargs["Key"]]
        return {
            "ContentLength": len(payload),
            "ChecksumSHA256": base64.b64encode(sha256(payload).digest()).decode(),
        }

    def get_object(self, **kwargs):
        assert kwargs["Bucket"] == "artifacts"
        payload = self.objects[kwargs["Key"]]
        range_header = kwargs.get("Range")
        self.ranges.append(range_header)
        if range_header is not None:
            start, end = (
                int(value) for value in range_header.removeprefix("bytes=").split("-")
            )
            payload = payload[start : end + 1]
        return {"Body": _Body(payload), "ContentLength": len(payload)}


class _Scalars:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def scalars(self):
        return self.rows


class _Session:
    def __init__(self, values: dict[tuple[type[object], UUID], object], files: list[object]) -> None:
        self.values = values
        self.files = files

    async def get(self, model: type[object], identity: UUID):
        return self.values.get((model, identity))

    async def execute(self, _statement: object):
        return _Scalars(self.files)


def _fixture():
    team_id, run_id, stage_id, attempt_id, session_id, artifact_id = (
        uuid4() for _ in range(6)
    )
    payloads = [
        b'{"schema_version":"behavior_rollout_bundle.v1"}\n',
        b"0123456789",
        b"video-payload",
    ]
    stored = [
        StoredFileV1(
            file_index=index,
            relative_path=path,
            role=role,
            archive_format="none",
            media_type=media_type,
            size_bytes=len(payload),
            sha256=digest_bytes(payload),
        )
        for index, (path, role, media_type, payload) in enumerate(
            [
                ("artifact.json", "semantic_document", "application/json", payloads[0]),
                ("payload/events.json", "payload", "application/json", payloads[1]),
                ("payload/head.mp4", "payload", "video/mp4", payloads[2]),
            ]
        )
    ]
    lineage_ids = [uuid4()]
    lineage_digests = [f"sha256:{'3' * 64}"]
    item_manifest = ArtifactManifestV1(
        artifact_id=artifact_id,
        artifact_name="rollout",
        artifact_type="behavior_rollout_bundle.v1",
        content_sha256=f"sha256:{'2' * 64}",
        stored_size_bytes=sum(map(len, payloads)),
        unpacked_size_bytes=sum(map(len, payloads)),
        file_count=len(stored),
        stored_files=stored,
        lineage_artifact_ids=lineage_ids,
        lineage_digests=lineage_digests,
    )
    record = RootArtifactRecordV1(
        artifact_id=artifact_id,
        artifact_name="rollout",
        artifact_type="behavior_rollout_bundle.v1",
        manifest_sha256=digest_bytes(canonical_document(item_manifest)),
        content_sha256=f"sha256:{'2' * 64}",
        stored_files=stored,
    )
    root = ArtifactCommitManifestV1(
        session_id=session_id,
        commit_kind="final_output",
        producer_identity={"execution_attempt_id": str(attempt_id)},
        artifacts=[record],
        total_bytes=sum(map(len, payloads)),
        input_lineage_artifact_ids=lineage_ids,
        input_lineage_digests=lineage_digests,
        request_digest=f"sha256:{'4' * 64}",
    )
    root_bytes = canonical_document(root)
    marker = canonical_document(
        ArtifactCommitMarkerV1(
            commit_kind="final_output",
            manifest_sha256=digest_bytes(root_bytes),
            session_id=session_id,
        )
    )
    prefix = "private/team/run/"
    upload = ArtifactUploadSession(
        id=session_id,
        team_id=team_id,
        commit_kind="final_output",
        pipeline_run_id=run_id,
        pipeline_stage_run_id=stage_id,
        execution_attempt_id=attempt_id,
        prefix=prefix,
        state="committed",
        canonical_manifest_json=root.model_dump(mode="json"),
        manifest_sha256=digest_bytes(root_bytes),
        committed_marker_sha256=digest_bytes(marker),
        actual_total_bytes=sum(map(len, payloads)),
    )
    artifact = Artifact(
        id=artifact_id,
        team_id=team_id,
        name="rollout",
        artifact_type="behavior_rollout_bundle.v1",
        content_hash=record.content_sha256,
        manifest_sha256=record.manifest_sha256,
        stored_size_bytes=sum(map(len, payloads)),
        unpacked_size_bytes=sum(map(len, payloads)),
        file_count=len(stored),
        pipeline_run_id=run_id,
        pipeline_stage_run_id=stage_id,
        execution_attempt_id=attempt_id,
        artifact_upload_session_id=session_id,
        producer_kind="container",
        safety_state="verified_internal",
        visibility="team",
        share_status="pending_scan",
        storage={"session_id": str(session_id), "files": [item.model_dump(mode="json") for item in stored]},
        created_at=datetime(2026, 8, 12, tzinfo=UTC),
    )
    run = PipelineRun(id=run_id, team_id=team_id)
    stage = PipelineStageRun(id=stage_id, pipeline_run_id=run_id)
    rows = [
        ArtifactUploadFile(
            session_id=session_id,
            file_index=item.file_index,
            preallocated_artifact_id=artifact_id,
            relative_path=item.relative_path,
            artifact_name="rollout",
            artifact_type="behavior_rollout_bundle.v1",
            producer="container",
            media_type=item.media_type,
            role=item.role,
            archive_format=item.archive_format,
            expected_max_bytes=item.size_bytes,
            expected_sha256=item.sha256,
            expected_size=item.size_bytes,
            state="verified",
            computed_sha256=item.sha256,
            actual_size=item.size_bytes,
        )
        for item in stored
    ]
    session = _Session(
        {
            (Artifact, artifact_id): artifact,
            (PipelineRun, run_id): run,
            (PipelineStageRun, stage_id): stage,
            (ArtifactUploadSession, session_id): upload,
        },
        rows,
    )
    objects = {
        f"{prefix}_manifest.json": root_bytes,
        f"{prefix}_COMMITTED": marker,
        **{
            f"{prefix}artifacts/{artifact_id}/{item.relative_path}": payload
            for item, payload in zip(stored, payloads, strict=True)
        },
    }
    return SimpleNamespace(
        team_id=team_id,
        run_id=run_id,
        stage_id=stage_id,
        artifact_id=artifact_id,
        session=session,
        store=_Store(objects),
        payloads=payloads,
        prefix=prefix,
    )


async def _body(response) -> bytes:
    return b"".join([chunk async for chunk in response.body_iterator])


async def test_file_streaming_supports_single_ranges_head_and_conditionals() -> None:
    case = _fixture()
    resolved = await resolve_public_artifact(
        case.session,
        team_id=case.team_id,
        artifact_id=case.artifact_id,
        run_id=case.run_id,
        stage_run_id=case.stage_id,
    )
    response = await stream_public_artifact_file(
        resolved,
        file_index=1,
        method="GET",
        range_header="bytes=2-5",
        if_none_match=None,
        if_range=None,
        client=case.store,
        bucket="artifacts",
    )
    assert response.status_code == 206
    assert response.headers["content-range"] == "bytes 2-5/10"
    assert response.headers["content-length"] == "4"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["content-disposition"].startswith("inline;")
    assert await _body(response) == b"2345"

    head = await stream_public_artifact_file(
        resolved,
        file_index=2,
        method="HEAD",
        range_header="bytes=-5",
        if_none_match=None,
        if_range=None,
        client=case.store,
        bucket="artifacts",
    )
    assert head.status_code == 206 and head.body == b""
    assert head.headers["content-length"] == "5"
    assert head.headers["content-range"].endswith("/13")

    not_modified = await stream_public_artifact_file(
        resolved,
        file_index=1,
        method="GET",
        range_header=None,
        if_none_match=f'"{resolved.files[1].sha256}"',
        if_range=None,
        client=case.store,
        bucket="artifacts",
    )
    assert not_modified.status_code == 304 and not not_modified.body


async def test_multi_range_is_416_and_if_range_mismatch_returns_full_file() -> None:
    case = _fixture()
    resolved = await resolve_public_artifact(
        case.session, team_id=case.team_id, artifact_id=case.artifact_id
    )
    rejected = await stream_public_artifact_file(
        resolved,
        file_index=1,
        method="GET",
        range_header="bytes=0-1,4-5",
        if_none_match=None,
        if_range=None,
        client=case.store,
        bucket="artifacts",
    )
    assert rejected.status_code == 416
    assert rejected.headers["content-range"] == "bytes */10"
    full = await stream_public_artifact_file(
        resolved,
        file_index=1,
        method="GET",
        range_header="bytes=2-5",
        if_none_match=None,
        if_range='"sha256:stale"',
        client=case.store,
        bucket="artifacts",
    )
    assert full.status_code == 200
    assert await _body(full) == case.payloads[1]


async def test_projection_and_denials_never_expose_object_store_coordinates() -> None:
    case = _fixture()
    resolved = await resolve_public_artifact(
        case.session, team_id=case.team_id, artifact_id=case.artifact_id
    )
    projection = public_artifact_projection(resolved)
    rendered = repr(projection)
    assert case.prefix not in rendered
    assert "storage_key" not in rendered and "credentials" not in rendered
    assert projection["files"][0]["download_path"].endswith("/files/0")
    with pytest.raises(HTTPException) as exc:
        await resolve_public_artifact(
            case.session, team_id=uuid4(), artifact_id=case.artifact_id
        )
    assert exc.value.status_code == 404


async def test_sha_or_marker_drift_fails_before_any_response_body() -> None:
    case = _fixture()
    resolved = await resolve_public_artifact(
        case.session, team_id=case.team_id, artifact_id=case.artifact_id
    )
    case.store.objects[resolved.files[2].storage_key] += b"tampered"
    with pytest.raises(HTTPException) as exc:
        await stream_public_artifact_file(
            resolved,
            file_index=2,
            method="GET",
            range_header=None,
            if_none_match=None,
            if_range=None,
            client=case.store,
            bucket="artifacts",
        )
    assert exc.value.status_code == 409
