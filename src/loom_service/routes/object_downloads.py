"""Authenticated object download helpers for loom_service routes."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from loom_service.metrics import ARTIFACT_DOWNLOAD_BYTES

_CHUNK_SIZE = 64 * 1024


def _object_error_code(exc: ClientError) -> str | None:
    code = exc.response.get("Error", {}).get("Code")
    return code if isinstance(code, str) else None


def _iter_body(body: Any) -> Iterator[bytes]:
    try:
        iter_chunks = getattr(body, "iter_chunks", None)
        if callable(iter_chunks):
            for chunk in iter_chunks(chunk_size=_CHUNK_SIZE):
                if chunk:
                    yield bytes(chunk)
            return

        read = getattr(body, "read", None)
        if not callable(read):
            raise TypeError("object body is not readable")
        while True:
            chunk = read(_CHUNK_SIZE)
            if not chunk:
                break
            yield bytes(chunk)
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def stream_object_response(
    *,
    client: Any,
    bucket: str,
    key: str,
    filename: str,
    artifact_kind: str,
    media_type: str = "application/octet-stream",
) -> StreamingResponse:
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = _object_error_code(exc)
        if code in {"NoSuchBucket", "NoSuchKey", "404"}:
            raise HTTPException(
                status_code=404,
                detail="download object not found",
            ) from exc
        raise

    headers = {
        "Content-Disposition": (
            f"attachment; filename*=UTF-8''{quote(filename, safe='')}"
        ),
    }
    content_length = obj.get("ContentLength")
    if isinstance(content_length, int):
        headers["Content-Length"] = str(content_length)
        ARTIFACT_DOWNLOAD_BYTES.labels(
            artifact_kind=artifact_kind,
        ).inc(content_length)

    return StreamingResponse(
        _iter_body(obj["Body"]),
        headers=headers,
        media_type=media_type,
    )
