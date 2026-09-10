"""Leaf retired snapshot identity, independent of member and preparation types."""

from __future__ import annotations

from uuid import UUID

from pydantic import field_validator, model_validator

from loom_capacity_manager.contracts import Digest, PositiveQuantity, Quantity, StrictV1Model


class _StrictOriginV1(StrictV1Model):
    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("retired origin schema must be integer 1")
        return value


class RetiredMembershipSnapshotReferenceV1(_StrictOriginV1):
    namespace_id: UUID
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    revision: Quantity
    head_sha256: Digest

    @model_validator(mode="after")
    def _exact_snapshot(self) -> RetiredMembershipSnapshotReferenceV1:
        if self.namespace_id.int == 0 or self.execution_manifest_sha256 == "0" * 64:
            raise ValueError("retired source identity must be nonzero")
        if (self.revision == 0) != (self.head_sha256 == "0" * 64):
            raise ValueError("retired source snapshot head differs from revision")
        return self
