"""Team-scoped, locator-free reads for committed Pipeline Artifact files."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import quote
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Artifact,
    ArtifactUploadFile,
    ArtifactUploadSession,
    PipelineRun,
    PipelineStageRun,
)
from loom.pipeline.artifact_access import artifact_read_allowed
from loom.pipeline.artifact_commit import (
    ArtifactCommitManifestV1,
    ArtifactCommitMarkerV1,
    ArtifactManifestV1,
)
from loom.pipeline.keys import canonical_document, digest_bytes

_CHUNK_SIZE = 1024 * 1024
_RANGE = re.compile(r"^bytes=(\d*)-(\d*)$")


@dataclass(frozen=True)
class PublicArtifactFile:
    file_index: int
    relative_path: str
    role: str
    media_type: str
    size_bytes: int
    sha256: str
    storage_key: str


@dataclass(frozen=True)
class PublicArtifact:
    artifact: Artifact
    lineage_artifact_ids: tuple[UUID, ...]
    lineage_digests: tuple[str, ...]
    files: tuple[PublicArtifactFile, ...]
    marker_keys: tuple[tuple[str, str, int], ...]


def _not_found() -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={"reason_code": "not_found", "message": "Pipeline Artifact was not found"},
    )


def _drift() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "reason_code": "artifact_descriptor_drift",
            "message": "Pipeline Artifact integrity validation failed",
        },
    )


def _object_failure(exc: Exception) -> HTTPException:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchBucket", "NoSuchKey", "404", "NotFound"}:
            return _drift()
    return HTTPException(
        status_code=503,
        detail={
            "reason_code": "artifact_store_unavailable",
            "message": "Pipeline Artifact storage is unavailable",
        },
    )


async def resolve_public_artifact(
    session: AsyncSession,
    *,
    team_id: UUID,
    artifact_id: UUID,
    user_id: UUID | None = None,
    role: str | None = None,
    platform_admin: bool = False,
    run_id: UUID | None = None,
    stage_run_id: UUID | None = None,
) -> PublicArtifact:
    artifact = await session.get(Artifact, artifact_id)
    if (
        artifact is None
        or artifact.team_id != team_id
        or artifact.pipeline_run_id is None
        or artifact.pipeline_stage_run_id is None
        or artifact.execution_attempt_id is None
        or artifact.artifact_upload_session_id is None
        or (run_id is not None and artifact.pipeline_run_id != run_id)
        or (stage_run_id is not None and artifact.pipeline_stage_run_id != stage_run_id)
    ):
        raise _not_found()
    run = await session.get(PipelineRun, artifact.pipeline_run_id)
    stage = await session.get(PipelineStageRun, artifact.pipeline_stage_run_id)
    upload = await session.get(ArtifactUploadSession, artifact.artifact_upload_session_id)
    if (
        run is None
        or stage is None
        or upload is None
        or run.team_id != team_id
        or stage.pipeline_run_id != run.id
        or upload.team_id != team_id
        or upload.pipeline_run_id != run.id
        or upload.pipeline_stage_run_id != stage.id
        or upload.execution_attempt_id != artifact.execution_attempt_id
        or upload.commit_kind not in {"final_output", "checkpoint"}
        or upload.state != "committed"
        or upload.canonical_manifest_json is None
        or upload.manifest_sha256 is None
        or upload.committed_marker_sha256 is None
    ):
        raise _not_found()
    if not artifact_read_allowed(
        getattr(artifact, "access_class", None),
        run_created_by_user_id=getattr(run, "created_by_user_id", None),
        requesting_user_id=user_id,
        requesting_role=role,
        platform_admin=platform_admin,
    ):
        raise _not_found()
    try:
        root = ArtifactCommitManifestV1.model_validate_json(
            canonical_document(upload.canonical_manifest_json)
        )
    except ValueError as exc:
        raise _drift() from exc
    root_bytes = canonical_document(root)
    marker_bytes = canonical_document(
        ArtifactCommitMarkerV1(
            commit_kind=cast(Any, upload.commit_kind),
            manifest_sha256=upload.manifest_sha256,
            session_id=upload.id,
        )
    )
    if (
        root.session_id != upload.id
        or digest_bytes(root_bytes) != upload.manifest_sha256
        or digest_bytes(marker_bytes) != upload.committed_marker_sha256
        or root.total_bytes != upload.actual_total_bytes
    ):
        raise _drift()
    record = next((item for item in root.artifacts if item.artifact_id == artifact.id), None)
    if record is None:
        raise _drift()
    stored_files = record.stored_files
    if (
        record.artifact_name != artifact.name
        or record.artifact_type != artifact.artifact_type
        or record.content_sha256 != artifact.content_hash
        or record.manifest_sha256 != artifact.manifest_sha256
        or artifact.unpacked_size_bytes is None
        or len(stored_files) != artifact.file_count
        or sum(item.size_bytes for item in stored_files) != artifact.stored_size_bytes
        or [item.file_index for item in stored_files] != list(range(len(stored_files)))
    ):
        raise _drift()
    item_manifest = ArtifactManifestV1(
        artifact_id=artifact.id,
        artifact_name=artifact.name,
        artifact_type=artifact.artifact_type,
        content_sha256=artifact.content_hash,
        stored_size_bytes=artifact.stored_size_bytes,
        unpacked_size_bytes=artifact.unpacked_size_bytes,
        file_count=artifact.file_count,
        stored_files=stored_files,
        lineage_artifact_ids=root.input_lineage_artifact_ids,
        lineage_digests=root.input_lineage_digests,
    )
    if digest_bytes(canonical_document(item_manifest)) != artifact.manifest_sha256:
        raise _drift()
    storage_files = artifact.storage.get("files") if isinstance(artifact.storage, dict) else None
    if (
        artifact.storage.get("session_id") != str(upload.id)
        or storage_files != [item.model_dump(mode="json") for item in stored_files]
    ):
        raise _drift()
    rows = list(
        (
            await session.execute(
                select(ArtifactUploadFile)
                .where(
                    ArtifactUploadFile.session_id == upload.id,
                    ArtifactUploadFile.preallocated_artifact_id == artifact.id,
                )
                .order_by(ArtifactUploadFile.file_index)
            )
        ).scalars()
    )
    if len(rows) != len(stored_files):
        raise _drift()
    files: list[PublicArtifactFile] = []
    for row, item in zip(rows, stored_files, strict=True):
        if (
            row.file_index != item.file_index
            or row.relative_path != item.relative_path
            or row.artifact_name != artifact.name
            or row.artifact_type != artifact.artifact_type
            or row.role != item.role
            or row.media_type != item.media_type
            or row.archive_format != item.archive_format
            or row.state != "verified"
            or row.actual_size != item.size_bytes
            or row.computed_sha256 != item.sha256
            or row.expected_max_bytes < item.size_bytes
            or row.expected_size not in {None, item.size_bytes}
            or row.expected_sha256 not in {None, item.sha256}
        ):
            raise _drift()
        files.append(
            PublicArtifactFile(
                file_index=item.file_index,
                relative_path=item.relative_path,
                role=item.role,
                media_type=item.media_type,
                size_bytes=item.size_bytes,
                sha256=item.sha256,
                storage_key=f"{upload.prefix}artifacts/{artifact.id}/{item.relative_path}",
            )
        )
    return PublicArtifact(
        artifact=artifact,
        lineage_artifact_ids=tuple(root.input_lineage_artifact_ids),
        lineage_digests=tuple(root.input_lineage_digests),
        files=tuple(files),
        marker_keys=(
            (f"{upload.prefix}_manifest.json", upload.manifest_sha256, len(root_bytes)),
            (f"{upload.prefix}_COMMITTED", upload.committed_marker_sha256, len(marker_bytes)),
        ),
    )


def public_artifact_projection(resolved: PublicArtifact) -> dict[str, Any]:
    artifact = resolved.artifact
    return {
        "id": str(artifact.id),
        "name": artifact.name,
        "artifact_type": artifact.artifact_type,
        "content_sha256": artifact.content_hash,
        "manifest_sha256": artifact.manifest_sha256,
        "stored_size_bytes": artifact.stored_size_bytes,
        "file_count": artifact.file_count,
        "safety_state": artifact.safety_state,
        "visibility": artifact.visibility,
        "share_status": artifact.share_status,
        "access_class": getattr(artifact, "access_class", None) or "team_runtime",
        "download_path": f"/api/v1/pipeline-artifacts/{artifact.id}/download",
        "detail_path": (
            f"/pipelines/{artifact.pipeline_run_id}/stages/"
            f"{artifact.pipeline_stage_run_id}/artifacts/{artifact.id}"
        ),
        "pipeline_run_id": str(artifact.pipeline_run_id),
        "pipeline_stage_run_id": str(artifact.pipeline_stage_run_id),
        "execution_attempt_id": str(artifact.execution_attempt_id),
        "producer_kind": artifact.producer_kind,
        "created_at": artifact.created_at.isoformat(),
        "lineage_artifact_ids": [str(value) for value in resolved.lineage_artifact_ids],
        "lineage_digests": list(resolved.lineage_digests),
        "files": [
            {
                "file_index": item.file_index,
                "relative_path": item.relative_path,
                "role": item.role,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "download_path": (
                    f"/api/v1/pipeline-artifacts/{artifact.id}/files/{item.file_index}"
                ),
            }
            for item in resolved.files
        ],
    }


def _checksum_from_head(head: dict[str, Any]) -> str | None:
    encoded = head.get("ChecksumSHA256")
    if not isinstance(encoded, str):
        return None
    try:
        raw = base64.b64decode(encoded, validate=True)
    except ValueError:
        return None
    return f"sha256:{raw.hex()}" if len(raw) == 32 else None


async def _hash_object(
    client: Any, *, bucket: str, key: str, maximum_bytes: int
) -> tuple[int, str]:
    try:
        obj = await asyncio.to_thread(client.get_object, Bucket=bucket, Key=key)
        body = obj["Body"]
        digest = hashlib.sha256()
        size = 0
        try:
            while True:
                chunk = await asyncio.to_thread(body.read, _CHUNK_SIZE)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
                if size > maximum_bytes:
                    raise _drift()
        finally:
            await asyncio.to_thread(body.close)
    except HTTPException:
        raise
    except Exception as exc:
        raise _object_failure(exc) from exc
    return size, f"sha256:{digest.hexdigest()}"


async def _validate_object(
    client: Any, *, bucket: str, key: str, size_bytes: int | None, sha256: str
) -> None:
    try:
        head = await asyncio.to_thread(
            client.head_object, Bucket=bucket, Key=key, ChecksumMode="ENABLED"
        )
    except Exception as exc:
        raise _object_failure(exc) from exc
    try:
        observed_size = int(head["ContentLength"])
        observed_digest = _checksum_from_head(head)
    except (KeyError, TypeError, ValueError) as exc:
        raise _drift() from exc
    if size_bytes is not None and observed_size != size_bytes:
        raise _drift()
    if observed_digest is None:
        observed_size, observed_digest = await _hash_object(
            client,
            bucket=bucket,
            key=key,
            maximum_bytes=observed_size if size_bytes is None else size_bytes,
        )
    if (size_bytes is not None and observed_size != size_bytes) or not hmac.compare_digest(
        observed_digest, sha256
    ):
        raise _drift()


async def validate_public_artifact(
    resolved: PublicArtifact, *, client: Any, bucket: str, file: PublicArtifactFile | None = None
) -> None:
    for key, sha256, size_bytes in resolved.marker_keys:
        await _validate_object(
            client, bucket=bucket, key=key, size_bytes=size_bytes, sha256=sha256
        )
    if file is not None:
        await _validate_object(
            client,
            bucket=bucket,
            key=file.storage_key,
            size_bytes=file.size_bytes,
            sha256=file.sha256,
        )


def _etag_matches(value: str | None, etag: str) -> bool:
    if value is None:
        return False
    return any(token.strip() in {"*", etag, f"W/{etag}"} for token in value.split(","))


def _parse_range(value: str, size: int) -> tuple[int, int]:
    if "," in value:
        raise ValueError("multiple ranges are unsupported")
    match = _RANGE.fullmatch(value.strip())
    if match is None or (not match.group(1) and not match.group(2)) or size <= 0:
        raise ValueError("invalid range")
    start_text, end_text = match.groups()
    if not start_text:
        suffix = int(end_text)
        if suffix <= 0:
            raise ValueError("invalid suffix")
        return max(0, size - suffix), size - 1
    start = int(start_text)
    if start >= size:
        raise ValueError("unsatisfied range")
    end = size - 1 if not end_text else min(int(end_text), size - 1)
    if end < start:
        raise ValueError("reversed range")
    return start, end


async def stream_public_artifact_file(
    resolved: PublicArtifact,
    *,
    file_index: int,
    method: str,
    range_header: str | None,
    if_none_match: str | None,
    if_range: str | None,
    client: Any,
    bucket: str,
) -> Response:
    if not 0 <= file_index < len(resolved.files):
        raise _not_found()
    item = resolved.files[file_index]
    if item.file_index != file_index:
        raise _drift()
    await validate_public_artifact(resolved, client=client, bucket=bucket, file=item)
    etag = f'"{item.sha256}"'
    common = {"Accept-Ranges": "bytes", "ETag": etag, "Content-Encoding": "identity"}
    if _etag_matches(if_none_match, etag):
        return Response(status_code=304, headers=common)
    start, end, status = 0, item.size_bytes - 1, 200
    if range_header is not None and (if_range is None or if_range.strip() == etag):
        try:
            start, end = _parse_range(range_header, item.size_bytes)
        except ValueError:
            return Response(
                status_code=416,
                headers=common | {"Content-Range": f"bytes */{item.size_bytes}"},
            )
        status = 206
    filename = item.relative_path.rsplit("/", 1)[-1]
    disposition = "inline" if item.media_type in {"video/mp4", "application/json"} else "attachment"
    headers = common | {
        "Content-Length": str(end - start + 1),
        "Content-Disposition": f"{disposition}; filename*=UTF-8''{quote(filename, safe='')}",
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{item.size_bytes}"
    if method == "HEAD" or item.size_bytes == 0:
        return Response(status_code=status, media_type=item.media_type, headers=headers)
    try:
        obj = await asyncio.to_thread(
            client.get_object,
            Bucket=bucket,
            Key=item.storage_key,
            Range=f"bytes={start}-{end}",
        )
        body = obj["Body"]
    except Exception as exc:
        raise _object_failure(exc) from exc

    async def iterator() -> AsyncIterator[bytes]:
        remaining = end - start + 1
        try:
            while remaining:
                chunk = await asyncio.to_thread(body.read, min(_CHUNK_SIZE, remaining))
                if not chunk:
                    raise RuntimeError("artifact stream ended before Content-Length")
                if len(chunk) > remaining:
                    raise RuntimeError("artifact stream exceeded Content-Length")
                remaining -= len(chunk)
                yield cast(bytes, chunk)
        finally:
            await asyncio.to_thread(body.close)

    return StreamingResponse(
        iterator(), status_code=status, media_type=item.media_type, headers=headers
    )


__all__ = [
    "PublicArtifact",
    "PublicArtifactFile",
    "public_artifact_projection",
    "resolve_public_artifact",
    "stream_public_artifact_file",
    "validate_public_artifact",
]
