"""Storage primitives for journaled task-source publication and recovery.

An intent MUST be committed before a caller writes. Inventory is observational:
it neither grants publication/deletion authority nor proves absence of future
late writes. The durable lifecycle owner must fence references and retain retired
intents for reconciliation. This module does not enable source registration.
"""

from __future__ import annotations

import hashlib
from pathlib import PurePosixPath
from typing import Any, Literal
from uuid import UUID

from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict, Field, field_validator

from loom.task_image_build_plan import MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES
from loom.trajectory.storage import ObjectStore, ObjectWriteResult


def _version(value: object) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 1024
        or value == "null"
        or value != value.strip()
        or any(not 33 <= ord(char) <= 126 for char in value)
    ):
        raise ValueError("source requires an exact immutable object version")
    return value


class TaskBundleObjectIntentV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.task-bundle-object-intent.v1"] = (
        "loom.task-bundle-object-intent.v1"
    )
    id: UUID
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    object_key: str = Field(min_length=1, max_length=1024)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES, strict=True)

    @field_validator("object_key")
    @classmethod
    def _key(cls, value: str) -> str:
        if (
            len(value.encode("utf-8")) > 1024
            or value.startswith("/")
            or "\\" in value
            or PurePosixPath(value).as_posix() != value
            or any(part in {"", ".", ".."} for part in value.split("/"))
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise ValueError("source object key is invalid")
        return value

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.object_key}"

    @property
    def metadata(self) -> dict[str, str]:
        return {
            "loom-source-write-id": str(self.id),
            "loom-source-sha256": self.content_sha256,
            "loom-source-size": str(self.size_bytes),
        }


async def write_task_bundle_source_object(
    store: ObjectStore,
    intent: TaskBundleObjectIntentV1,
    body: bytes,
) -> ObjectWriteResult:
    """Write only verified bytes, with recoverable identity on every retry."""
    if (
        type(body) is not bytes
        or len(body) != intent.size_bytes
        or hashlib.sha256(body).hexdigest() != intent.content_sha256
    ):
        raise ValueError("source object bytes differ from the committed intent")
    result = await store.put_object_with_metadata(
        bucket=intent.bucket,
        key=intent.object_key,
        body=body,
        metadata=intent.metadata,
        require_versioning=True,
    )
    if result.uri != intent.uri:
        raise ValueError("source object receipt names another object")
    _version(result.version_id)
    return result


def _absent(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {
        "NoSuchKey",
        "NoSuchVersion",
        "404",
        "NotFound",
    }


class S3TaskBundleVersionInventory:
    """Bounded exact-key reconciliation, including non-current retry versions.

    An empty result is just this observation. In particular, it is NOT authority
    to forget an uncertain write intent or retire its reconciliation tombstone.
    The injected client owns transport timeouts/retries; no ambient endpoint or
    credentials are discovered here. Production callers run this blocking I/O
    outside their database transaction and event loop.
    """

    def __init__(
        self,
        client: Any,
        *,
        max_versions: int = 4096,
        max_read_bytes: int = MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
    ) -> None:
        if not 1 <= max_versions <= 16384 or not 1 <= max_read_bytes <= 2**30:
            raise ValueError("source inventory limits are invalid")
        self._client = client
        self._max_versions = max_versions
        self._max_read_bytes = max_read_bytes

    def _read_own_version(
        self, intent: TaskBundleObjectIntentV1, version: str, *, remaining_bytes: int
    ) -> ObjectWriteResult | None:
        params = dict(Bucket=intent.bucket, Key=intent.object_key, VersionId=version)
        try:
            head = self._client.head_object(**params)
        except ClientError as error:
            if _absent(error):
                return None
            raise
        metadata = head.get("Metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("source version metadata is invalid")
        if metadata.get("loom-source-write-id") != str(intent.id):
            return None

        def check(response: Any) -> None:
            if (
                response.get("VersionId") != version
                or response.get("ContentLength") != intent.size_bytes
                or response.get("Metadata") != intent.metadata
            ):
                raise ValueError("source version does not match committed intent")

        check(head)
        if intent.size_bytes > remaining_bytes:
            raise ValueError("source inventory read budget exceeded")
        try:
            response = self._client.get_object(**params)
        except ClientError as error:
            if _absent(error):
                return None
            raise
        body = response["Body"]
        try:
            check(response)
            digest, count = hashlib.sha256(), 0
            while chunk := body.read(min(1024 * 1024, intent.size_bytes - count + 1)):
                count += len(chunk)
                if count > intent.size_bytes:
                    raise ValueError("source version byte size drifted")
                digest.update(chunk)
            if count != intent.size_bytes or digest.hexdigest() != intent.content_sha256:
                raise ValueError("source version bytes drifted")
        finally:
            body.close()
        return ObjectWriteResult(uri=intent.uri, version_id=version)

    def scan(self, intent: TaskBundleObjectIntentV1) -> tuple[ObjectWriteResult, ...]:
        if self._client.get_bucket_versioning(Bucket=intent.bucket).get("Status") != "Enabled":
            raise ValueError("source inventory requires Enabled bucket versioning")
        cursor: tuple[str, str] | None = None
        seen_cursors: set[tuple[str, str]] = set()
        seen_versions: set[str] = set()
        results: list[ObjectWriteResult] = []
        examined, read_bytes = 0, 0
        for _ in range(32):
            params: dict[str, Any] = dict(
                Bucket=intent.bucket, Prefix=intent.object_key, MaxKeys=1000
            )
            if cursor is not None:
                params.update(KeyMarker=cursor[0], VersionIdMarker=cursor[1])
            page = self._client.list_object_versions(**params)
            for item in (*page.get("Versions", ()), *page.get("DeleteMarkers", ())):
                examined += 1
                if examined > self._max_versions:
                    raise ValueError("source inventory version limit exceeded")
                if item.get("Key") != intent.object_key:
                    continue
                version = _version(item.get("VersionId"))
                if version in seen_versions:
                    raise ValueError("source inventory repeated an object version")
                seen_versions.add(version)
            for item in page.get("Versions", ()):
                if item.get("Key") != intent.object_key:
                    continue
                result = self._read_own_version(
                    intent,
                    _version(item.get("VersionId")),
                    remaining_bytes=self._max_read_bytes - read_bytes,
                )
                if result is not None:
                    results.append(result)
                    read_bytes += intent.size_bytes
            if page.get("IsTruncated") is False:
                return tuple(results)
            key_marker, version_marker = page.get("NextKeyMarker"), page.get("NextVersionIdMarker")
            if (
                page.get("IsTruncated") is not True
                or type(key_marker) is not str
                or not key_marker.startswith(intent.object_key)
                or type(version_marker) is not str
                or not version_marker
            ):
                raise ValueError("source inventory pagination is invalid")
            cursor = (key_marker, version_marker)
            if cursor in seen_cursors:
                raise ValueError("source inventory pagination did not progress")
            seen_cursors.add(cursor)
        raise ValueError("source inventory pagination limit exceeded")
