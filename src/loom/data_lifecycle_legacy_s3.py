"""Read-only exact S3/MinIO inspection for legacy lifecycle classification."""

from __future__ import annotations

import hashlib
from typing import Any

from botocore.exceptions import ClientError

from loom.data_lifecycle_legacy import LegacyClassificationError

_ABSENT_ERROR_CODES = frozenset({"404", "NoSuchKey", "NoSuchVersion"})


class S3LegacyObjectInspector:
    def __init__(self, client: Any, *, read_chunk_bytes: int = 1024 * 1024) -> None:
        if read_chunk_bytes <= 0:
            raise ValueError("read_chunk_bytes must be positive")
        self._client = client
        self._read_chunk_bytes = read_chunk_bytes

    def inspect(
        self,
        *,
        bucket: str,
        object_key: str,
        version_id: str | None,
    ) -> tuple[str | None, str, int] | None:
        params = {"Bucket": bucket, "Key": object_key}
        if version_id is not None:
            params["VersionId"] = version_id
        try:
            response = self._client.get_object(**params)
            body = response["Body"]
        except ClientError as exc:
            error = exc.response.get("Error", {})
            if error.get("Code") in _ABSENT_ERROR_CODES:
                return None
            raise LegacyClassificationError(
                f"legacy object cannot be inspected: {bucket}/{object_key}"
            ) from exc
        except Exception as exc:
            raise LegacyClassificationError(
                f"legacy object cannot be inspected: {bucket}/{object_key}"
            ) from exc
        digest = hashlib.sha256()
        try:
            while chunk := body.read(self._read_chunk_bytes):
                digest.update(chunk)
        finally:
            body.close()
        size = int(response.get("ContentLength", -1))
        if size < 0:
            raise LegacyClassificationError(
                f"legacy object size is unavailable: {bucket}/{object_key}"
            )
        observed_version = response.get("VersionId")
        if observed_version is not None and not isinstance(observed_version, str):
            raise LegacyClassificationError(
                f"legacy object version is invalid: {bucket}/{object_key}"
            )
        return observed_version, digest.hexdigest(), size


__all__ = ["S3LegacyObjectInspector"]
