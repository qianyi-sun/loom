"""Claim-bound, locator-free Pipeline Artifact read service."""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from fastapi import HTTPException
from starlette.responses import Response, StreamingResponse

from loom.pipeline.artifact_commit import MAX_APPLICATION_BUFFER_BYTES, ArtifactCommitError
from loom.pipeline.keys import digest_bytes
from loom.trajectory.storage import ObjectStore

_START_RANGE = re.compile(r"^bytes=([0-9]+)-$")


@dataclass(frozen=True)
class ResolvedStoredFile:
    file_index: int
    storage_key: str
    media_type: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class ResolvedArtifactInput:
    artifact_id: UUID
    manifest_bytes: bytes
    manifest_sha256: str
    root_marker_valid: bool
    files: tuple[ResolvedStoredFile, ...]


class ArtifactInputResolverV1(Protocol):
    async def resolve(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
    ) -> ResolvedArtifactInput: ...


def _etag(digest: str) -> str:
    return f'"{digest}"'


def _require_match(observed: str, expected_digest: str) -> None:
    if not hmac.compare_digest(observed, _etag(expected_digest)):
        raise HTTPException(status_code=412, detail="input_digest_precondition_failed")


class ArtifactReadService:
    def __init__(
        self,
        *,
        resolver: ArtifactInputResolverV1,
        store: ObjectStore,
        bucket: str,
    ) -> None:
        self._resolver = resolver
        self._store = store
        self._bucket = bucket

    async def _resolve(
        self, *, attempt_id: UUID, binding_name: str, item_key: str
    ) -> ResolvedArtifactInput:
        try:
            resolved = await self._resolver.resolve(
                attempt_id=attempt_id,
                binding_name=binding_name,
                item_key=item_key,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="not_found") from exc
        except ArtifactCommitError as exc:
            raise HTTPException(status_code=409, detail=exc.reason) from exc
        if (
            not resolved.root_marker_valid
            or digest_bytes(resolved.manifest_bytes) != resolved.manifest_sha256
            or [item.file_index for item in resolved.files] != list(range(len(resolved.files)))
        ):
            raise HTTPException(status_code=409, detail="input_descriptor_drift")
        return resolved

    async def read_manifest(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        if_match: str,
    ) -> Response:
        resolved = await self._resolve(
            attempt_id=attempt_id, binding_name=binding_name, item_key=item_key
        )
        _require_match(if_match, resolved.manifest_sha256)
        return Response(
            content=resolved.manifest_bytes,
            status_code=200,
            media_type="application/vnd.loom.artifact-manifest+json",
            headers={
                "Content-Length": str(len(resolved.manifest_bytes)),
                "ETag": _etag(resolved.manifest_sha256),
            },
        )

    async def read_file(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        file_index: int,
        if_match: str,
        range_header: str | None,
    ) -> Response:
        resolved = await self._resolve(
            attempt_id=attempt_id, binding_name=binding_name, item_key=item_key
        )
        if not 0 <= file_index < len(resolved.files):
            raise HTTPException(status_code=416, detail="invalid_range")
        item = resolved.files[file_index]
        _require_match(if_match, item.sha256)
        start = 0
        status = 200
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Encoding": "identity",
            "ETag": _etag(item.sha256),
        }
        if range_header is not None:
            match = _START_RANGE.fullmatch(range_header)
            if match is None:
                raise HTTPException(status_code=416, detail="invalid_range")
            start = int(match.group(1))
            if start >= item.size_bytes:
                raise HTTPException(status_code=416, detail="invalid_range")
            status = 206
            headers["Content-Range"] = f"bytes {start}-{item.size_bytes - 1}/{item.size_bytes}"
        try:
            facts = await self._store.stat_object(bucket=self._bucket, key=item.storage_key)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="object_store_transport") from exc
        if facts.content_length != item.size_bytes or (
            facts.checksum_sha256 is not None
            and not hmac.compare_digest(facts.checksum_sha256, item.sha256)
        ):
            raise HTTPException(status_code=409, detail="input_descriptor_drift")
        if facts.checksum_sha256 is None:
            digest = hashlib.sha256()
            observed_size = 0
            try:
                async for chunk in self._store.stream_object(
                    bucket=self._bucket,
                    key=item.storage_key,
                    chunk_size=MAX_APPLICATION_BUFFER_BYTES,
                ):
                    observed_size += len(chunk)
                    digest.update(chunk)
            except Exception as exc:
                raise HTTPException(status_code=503, detail="object_store_transport") from exc
            observed_digest = f"sha256:{digest.hexdigest()}"
            if observed_size != item.size_bytes or not hmac.compare_digest(
                observed_digest, item.sha256
            ):
                raise HTTPException(status_code=409, detail="input_descriptor_drift")
        headers["Content-Length"] = str(item.size_bytes - start)

        async def stream() -> AsyncIterator[bytes]:
            try:
                async for chunk in self._store.stream_object(
                    bucket=self._bucket,
                    key=item.storage_key,
                    start_offset=start,
                    chunk_size=MAX_APPLICATION_BUFFER_BYTES,
                ):
                    yield chunk
            except Exception as exc:
                raise RuntimeError("object_store_transport") from exc

        return StreamingResponse(
            stream(),
            status_code=status,
            media_type=item.media_type,
            headers=headers,
        )


__all__ = [
    "ArtifactInputResolverV1",
    "ArtifactReadService",
    "ResolvedArtifactInput",
    "ResolvedStoredFile",
]
