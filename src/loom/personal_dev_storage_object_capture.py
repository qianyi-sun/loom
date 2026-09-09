"""Version-pinned S3 capture primitive, not a lifecycle transfer capability.

Callers must authenticate persisted retirement/current-operation authority and
own the separately configured snapshot bucket. This code never grants IAM
authority, deletes source data, restores data or publishes lifecycle readiness.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlencode
from uuid import UUID

from botocore.exceptions import ClientError
from pydantic import Field, field_validator, model_validator

from loom.personal_dev_storage_transfer import (
    PersonalDevStorageTransferBindingV1,
    storage_transfer_bucket_pairs,
)
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_digest

_PART_BYTES = 8 * 1024 * 1024
_MAX_OBJECT_BYTES = 64 * 1024**3
_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


class StorageObjectCaptureError(RuntimeError):
    """No verified snapshot receipt is available; retain source and retry evidence."""


class StorageObjectCaptureIntentV1(StrictV1Model):
    transfer_binding_sha256: Digest
    capture_id: UUID
    purpose: Literal["tasks", "trajectories", "artifacts"]
    key: str
    expected_etag: str
    size_bytes: int = Field(ge=0, le=_MAX_OBJECT_BYTES)

    @field_validator("key")
    @classmethod
    def _key(cls, value: str) -> str:
        if (
            not value or len(value.encode("utf-8")) > 1024
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
            or "\\" in value or value.startswith("/") or "//" in value
            or any(segment in {".", ".."} for segment in value.split("/"))
        ):
            raise ValueError("snapshot object key is invalid")
        # Keys are always SDK parameters, never shell commands or filesystem
        # paths. The private capture key uses a hash, not path normalization.
        return value

    @field_validator("expected_etag")
    @classmethod
    def _etag(cls, value: str) -> str:
        if not 2 < len(value) <= 256 or not value.startswith('"') or not value.endswith('"') or any(
            not 32 <= ord(char) <= 126 for char in value
        ):
            raise ValueError("snapshot source ETag is invalid")
        return value

    @model_validator(mode="after")
    def _identity(self) -> StorageObjectCaptureIntentV1:
        if self.capture_id.int == 0:
            raise ValueError("snapshot capture identity must be non-null")
        return self

    @property
    def snapshot_key(self) -> str:
        intent = json.dumps([self.purpose, self.key, self.expected_etag, self.size_bytes], separators=(",", ":")).encode()
        return f"v1/{self.transfer_binding_sha256}/{self.capture_id.hex}/{hashlib.sha256(intent).hexdigest()}"


class CapturedStorageObjectV1(StorageObjectCaptureIntentV1):
    payload_sha256: Digest
    snapshot_version_id: str = Field(min_length=1, max_length=1024)

    @field_validator("snapshot_version_id")
    @classmethod
    def _version(cls, value: str) -> str:
        if value == "null" or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("snapshot version identity must be non-null")
        return value


@dataclass(frozen=True)
class S3RetainedObjectCapture:
    client: Any
    snapshot_bucket: str

    def capture(
        self, binding: PersonalDevStorageTransferBindingV1, *, capture_id: UUID,
        purpose: Literal["tasks", "trajectories", "artifacts"], key: str, expected_etag: str, size_bytes: int,
    ) -> CapturedStorageObjectV1:
        try:
            pairs = storage_transfer_bucket_pairs(binding)
            if not isinstance(self.snapshot_bucket, str) or not _KEY_PATTERN.fullmatch(self.snapshot_bucket) or any(
                self.snapshot_bucket in (source, destination) for _, source, destination in pairs
            ):
                raise StorageObjectCaptureError("snapshot bucket must be separate from tenant storage")
            source = next(source for name, source, _ in pairs if name == purpose)
            # Validate the immutable capture intent before any object-store I/O.
            intent = StorageObjectCaptureIntentV1(
                transfer_binding_sha256=canonical_digest(binding), capture_id=capture_id,
                purpose=purpose, key=key, expected_etag=expected_etag, size_bytes=size_bytes,
            )
            if self.client.get_bucket_versioning(Bucket=self.snapshot_bucket).get("Status") != "Enabled":
                raise StorageObjectCaptureError("snapshot bucket versioning is not enabled")
            head = self.client.head_object(Bucket=source, Key=key, IfMatch=expected_etag)
            if type(head.get("ContentLength")) is not int or head["ContentLength"] != size_bytes or head.get("ETag") != expected_etag:
                raise StorageObjectCaptureError("snapshot source size or identity changed")
            copy_source = {"Bucket": source, "Key": key}
            if head.get("VersionId") not in (None, "", "null"):
                copy_source["VersionId"] = head["VersionId"]
            reply = self._copy(intent, copy_source, head)
            version = reply.get("VersionId")
            if not isinstance(version, str) or not version or version == "null":
                raise StorageObjectCaptureError("snapshot capture returned no immutable version")
            captured = self.client.get_object(Bucket=self.snapshot_bucket, Key=intent.snapshot_key, VersionId=version)
            body = captured["Body"]
            digest, count = hashlib.sha256(), 0
            try:
                if (
                    captured.get("VersionId") != version
                    or type(captured.get("ContentLength")) is not int
                    or captured["ContentLength"] != size_bytes
                ):
                    raise StorageObjectCaptureError("snapshot readback differs from captured version")
                while chunk := body.read(1024 * 1024):
                    count += len(chunk)
                    if count > size_bytes:
                        raise StorageObjectCaptureError("snapshot payload exceeds declared size")
                    digest.update(chunk)
                if count != size_bytes:
                    raise StorageObjectCaptureError("snapshot payload was truncated")
            finally:
                body.close()
            if self.client.get_bucket_versioning(Bucket=self.snapshot_bucket).get("Status") != "Enabled":
                raise StorageObjectCaptureError("snapshot versioning changed during capture")
            return CapturedStorageObjectV1(**{
                **intent.model_dump(), "payload_sha256": digest.hexdigest(), "snapshot_version_id": version,
            })
        except StorageObjectCaptureError:
            raise
        except Exception:
            # Transport errors can carry endpoints and request parameters.
            raise StorageObjectCaptureError("object snapshot capture did not complete") from None

    def _copy(self, intent: StorageObjectCaptureIntentV1, source: dict[str, str], head: dict[str, Any]) -> dict[str, Any]:
        if intent.size_bytes <= _PART_BYTES:
            result: dict[str, Any] = self.client.copy_object(
                Bucket=self.snapshot_bucket, Key=intent.snapshot_key, CopySource=source,
                CopySourceIfMatch=intent.expected_etag, MetadataDirective="COPY", TaggingDirective="COPY",
            )
            return result
        attributes = {name: head[name] for name in (
            "ContentType", "CacheControl", "ContentDisposition", "ContentEncoding", "ContentLanguage", "Expires", "Metadata",
        ) if name in head}
        tags = self.client.get_object_tagging(**source)["TagSet"]
        if tags:
            attributes["Tagging"] = urlencode([(tag["Key"], tag["Value"]) for tag in tags])
        upload = self.client.create_multipart_upload(Bucket=self.snapshot_bucket, Key=intent.snapshot_key, **attributes)
        upload_id = upload["UploadId"]
        try:
            parts = []
            for number, start in enumerate(range(0, intent.size_bytes, _PART_BYTES), 1):
                reply = self.client.upload_part_copy(
                    Bucket=self.snapshot_bucket, Key=intent.snapshot_key, UploadId=upload_id, PartNumber=number,
                    CopySource=source, CopySourceIfMatch=intent.expected_etag,
                    CopySourceRange=f"bytes={start}-{min(start + _PART_BYTES, intent.size_bytes) - 1}",
                )
                parts.append({"PartNumber": number, "ETag": reply["CopyPartResult"]["ETag"]})
            result = self.client.complete_multipart_upload(
                Bucket=self.snapshot_bucket, Key=intent.snapshot_key, UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return result
        except BaseException:
            try:
                self.client.abort_multipart_upload(Bucket=self.snapshot_bucket, Key=intent.snapshot_key, UploadId=upload_id)
            except ClientError as error:
                # A lost successful completion reply leaves a finished version,
                # not an abortable upload. It is unreferenced cleanup evidence,
                # never an inferred successful receipt.
                if error.response.get("Error", {}).get("Code") != "NoSuchUpload":
                    raise StorageObjectCaptureError("snapshot multipart cleanup was not acknowledged") from None
            raise
