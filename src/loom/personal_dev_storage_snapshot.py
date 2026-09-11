"""Complete object inventories and version-pinned snapshot manifests.

Management still owns authorization, durable attachment, quotas and cleanup.
These models and primitives do not grant permission to restore or activate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom.personal_dev_storage_object_capture import (
    CapturedStorageObjectV1,
    S3RetainedObjectCapture,
    StorageObjectCaptureError,
    StorageObjectCaptureIntentV1,
)
from loom.personal_dev_storage_transfer import (
    PersonalDevStorageTransferBindingV1,
    parse_storage_transfer_binding,
    storage_transfer_bucket_pairs,
)
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)


class StorageObjectInventoryV1(StrictV1Model):
    transfer_binding: PersonalDevStorageTransferBindingV1
    capture_id: UUID
    scanned_purposes: tuple[Literal["tasks", "trajectories", "artifacts"], ...] = Field(min_length=3, max_length=3)
    objects: tuple[StorageObjectCaptureIntentV1, ...] = Field(max_length=10_000)

    @field_validator("transfer_binding")
    @classmethod
    def _binding(cls, value: PersonalDevStorageTransferBindingV1) -> PersonalDevStorageTransferBindingV1:
        return parse_storage_transfer_binding(canonical_bytes(value), expected_sha256=canonical_digest(value))

    @field_validator("objects")
    @classmethod
    def _objects(cls, values: tuple[StorageObjectCaptureIntentV1, ...]) -> tuple[StorageObjectCaptureIntentV1, ...]:
        return tuple(StorageObjectCaptureIntentV1.model_validate_json(canonical_bytes(value)) for value in values)

    @model_validator(mode="after")
    def _complete(self) -> StorageObjectInventoryV1:
        if self.capture_id.int == 0 or self.scanned_purposes != ("tasks", "trajectories", "artifacts"):
            raise ValueError("snapshot requires one nonzero capture and all three bucket scans")
        identities = tuple((item.purpose, item.key) for item in self.objects)
        digest = canonical_digest(self.transfer_binding)
        if identities != tuple(sorted(set(identities))) or any(
            item.capture_id != self.capture_id or item.transfer_binding_sha256 != digest for item in self.objects
        ):
            raise ValueError("snapshot inventory is duplicated, unordered or mixes transfer attempts")
        if sum(item.size_bytes for item in self.objects) > 1024**4 or len(canonical_bytes(self)) > MAX_CONTRACT_BYTES:
            raise ValueError("snapshot inventory exceeds its byte budget")
        return self


class StorageObjectSnapshotV1(StrictV1Model):
    inventory: StorageObjectInventoryV1
    captures: tuple[CapturedStorageObjectV1, ...] = Field(max_length=10_000)

    @field_validator("inventory")
    @classmethod
    def _inventory(cls, value: StorageObjectInventoryV1) -> StorageObjectInventoryV1:
        return StorageObjectInventoryV1.model_validate_json(canonical_bytes(value))

    @field_validator("captures")
    @classmethod
    def _captures(cls, values: tuple[CapturedStorageObjectV1, ...]) -> tuple[CapturedStorageObjectV1, ...]:
        return tuple(CapturedStorageObjectV1.model_validate_json(canonical_bytes(value)) for value in values)

    @model_validator(mode="after")
    def _exact(self) -> StorageObjectSnapshotV1:
        captured_intents = tuple(StorageObjectCaptureIntentV1.model_validate(
            item.model_dump(exclude={"payload_sha256", "snapshot_version_id"}),
        ) for item in self.captures)
        if captured_intents != self.inventory.objects:
            raise ValueError("snapshot captures do not cover the exact selected inventory")
        if len(canonical_bytes(self)) > MAX_CONTRACT_BYTES:
            raise ValueError("snapshot manifest exceeds its byte budget")
        return self


def parse_storage_object_snapshot(payload: bytes, *, expected_sha256: str) -> StorageObjectSnapshotV1:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_CONTRACT_BYTES:
        raise ValueError("snapshot manifest exceeds its byte bound")
    snapshot = StorageObjectSnapshotV1.model_validate_json(payload)
    if canonical_bytes(snapshot) != payload or canonical_digest(snapshot) != expected_sha256:
        raise ValueError("snapshot differs from its pinned canonical manifest")
    return snapshot


@dataclass(frozen=True)
class S3RetainedObjectSnapshot:
    client: Any
    snapshot_bucket: str

    def inventory(self, binding: PersonalDevStorageTransferBindingV1, *, capture_id: UUID) -> StorageObjectInventoryV1:
        """Select exact keys before capture; this is not an atomic S3 write drain."""
        try:
            pairs = storage_transfer_bucket_pairs(binding)
            if not isinstance(capture_id, UUID) or capture_id.int == 0:
                raise ValueError("snapshot capture identity must be nonzero")
            digest = canonical_digest(binding)
            objects = []
            total_bytes = 0
            for purpose, source, _ in pairs:
                continuation = None
                seen_tokens: set[str] = set()
                while True:
                    arguments: dict[str, Any] = {"Bucket": source, "MaxKeys": 1000}
                    if continuation is not None:
                        arguments["ContinuationToken"] = continuation
                    response = self.client.list_objects_v2(**arguments)
                    rows = response.get("Contents", [])
                    if not isinstance(rows, list) or len(rows) > 1000 or type(response.get("IsTruncated")) is not bool:
                        raise ValueError("snapshot listing shape is invalid")
                    for row in rows:
                        item = StorageObjectCaptureIntentV1(
                            transfer_binding_sha256=digest, capture_id=capture_id, purpose=purpose,
                            key=row["Key"], expected_etag=row["ETag"], size_bytes=row["Size"],
                        )
                        total_bytes += item.size_bytes
                        objects.append(item)
                        if len(objects) > 10_000 or total_bytes > 1024**4:
                            raise ValueError("snapshot listing exceeds its object or byte budget")
                    if not response["IsTruncated"]:
                        break
                    continuation = response.get("NextContinuationToken")
                    if (
                        not isinstance(continuation, str) or not 0 < len(continuation) <= 4096
                        or continuation in seen_tokens or len(seen_tokens) >= 10_000
                    ):
                        raise ValueError("snapshot listing pagination is invalid")
                    seen_tokens.add(continuation)
            return StorageObjectInventoryV1(
                transfer_binding=binding, capture_id=capture_id,
                scanned_purposes=("tasks", "trajectories", "artifacts"),
                objects=tuple(sorted(objects, key=lambda item: (item.purpose, item.key))),
            )
        except Exception:
            raise StorageObjectCaptureError("object snapshot inventory did not complete") from None

    def capture_inventory(self, inventory: StorageObjectInventoryV1) -> StorageObjectSnapshotV1:
        try:
            inventory = StorageObjectInventoryV1.model_validate_json(canonical_bytes(inventory))
            capture = S3RetainedObjectCapture(self.client, snapshot_bucket=self.snapshot_bucket)
            captures = tuple(capture.capture(
                inventory.transfer_binding, capture_id=inventory.capture_id,
                purpose=item.purpose, key=item.key, expected_etag=item.expected_etag, size_bytes=item.size_bytes,
            ) for item in inventory.objects)
            return StorageObjectSnapshotV1(inventory=inventory, captures=captures)
        except StorageObjectCaptureError:
            raise
        except Exception:
            raise StorageObjectCaptureError("object snapshot manifest did not complete") from None
