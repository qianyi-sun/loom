"""Exact S3/MinIO object deletion adapter for staging lifecycle GC."""

from __future__ import annotations

import hashlib
from typing import Any

from botocore.exceptions import ClientError

from loom.data_lifecycle_gc import LifecycleGcExecutionError, RegisteredObject


def _not_found(exc: ClientError) -> bool:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}


class S3ExactObjectDeleter:
    """Verify exact version/bytes before deleting one registered object."""

    def __init__(self, client: Any, *, read_chunk_bytes: int = 1024 * 1024) -> None:
        if read_chunk_bytes <= 0:
            raise ValueError("read_chunk_bytes must be positive")
        self._client = client
        self._read_chunk_bytes = read_chunk_bytes

    @staticmethod
    def _params(item: RegisteredObject) -> dict[str, str]:
        params = {"Bucket": item.bucket, "Key": item.object_key}
        if item.version_id is not None:
            params["VersionId"] = item.version_id
        return params

    def _verify_identity(self, item: RegisteredObject) -> None:
        params = self._params(item)
        response = self._client.head_object(**params)
        if int(response.get("ContentLength", -1)) != item.size_bytes:
            raise LifecycleGcExecutionError(
                f"registered object size drifted: {item.bucket}/{item.object_key}"
            )
        observed_version = response.get("VersionId")
        if item.version_id is not None and observed_version != item.version_id:
            raise LifecycleGcExecutionError(
                f"registered object version drifted: {item.bucket}/{item.object_key}"
            )
        if item.content_sha256 is None:
            return
        body = self._client.get_object(**params)["Body"]
        digest = hashlib.sha256()
        try:
            while chunk := body.read(self._read_chunk_bytes):
                digest.update(chunk)
        finally:
            body.close()
        if digest.hexdigest() != item.content_sha256:
            raise LifecycleGcExecutionError(
                f"registered object digest drifted: {item.bucket}/{item.object_key}"
            )

    def delete_exact(self, item: RegisteredObject) -> None:
        self._verify_identity(item)
        self._client.delete_object(**self._params(item))

    def exact_absent(self, item: RegisteredObject) -> bool:
        try:
            self._client.head_object(**self._params(item))
        except ClientError as exc:
            if _not_found(exc):
                return True
            raise
        return False


__all__ = ["S3ExactObjectDeleter"]
