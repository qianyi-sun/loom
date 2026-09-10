"""Explicit cross-epoch predecessor values, not source or release authentication."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import (
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

from loom_capacity_manager.contracts import Digest, PositiveQuantity, canonical_digest
from loom_capacity_manager.membership_contracts import PersonalReincarnationEvidenceV1
from loom_capacity_manager.retired_source_reference import RetiredMembershipSnapshotReferenceV1


class PersonalInheritedReincarnationEvidenceV2(PersonalReincarnationEvidenceV1):
    """Preserve the real own event even when current admission revision is one.

    The caller must match this to the pinned inherited origin, authenticate its
    complete source chain and recompute durable release witnesses. No event or
    runtime consumer accepts this standalone value yet.
    """

    schema_version: Literal[2] = 2  # type: ignore[assignment]
    execution_epoch: PositiveQuantity
    source: RetiredMembershipSnapshotReferenceV1
    predecessor_execution_epoch: PositiveQuantity
    predecessor_execution_manifest_sha256: Digest

    @model_serializer(mode="wrap")
    def _serialize_evidence(self, handler: SerializerFunctionWrapHandler) -> Any:
        if self.schema_version != 2:
            raise ValueError("inherited evidence serialization version changed")
        return handler(self)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 2:
            raise ValueError("inherited recreation schema must be integer 2")
        return value

    @field_validator("origin", "predecessor", mode="before")
    @classmethod
    def _exact_nested_version(cls, value: object) -> object:
        if isinstance(value, dict):
            version = value.get("schema_version", 1)
            if type(version) is not int or version != 1:
                raise ValueError("inherited predecessor and root schemas must be integer 1")
        return value

    @model_validator(mode="after")
    def _predecessor_binding(self) -> PersonalInheritedReincarnationEvidenceV2:
        self._require_predecessor_identity()
        source = self.source
        if (self.namespace_id != source.namespace_id
            or (self.origin.generation == self.predecessor.configuration_generation and (
                self.origin.subject_incarnation != self.predecessor.subject_incarnation
                or self.origin.digest != canonical_digest(self.predecessor)))
            or self.predecessor_execution_manifest_sha256 == "0" * 64
            or self.predecessor_execution_epoch > source.execution_epoch
            or source.execution_epoch >= self.execution_epoch
            or (self.predecessor_execution_epoch == source.execution_epoch and (
                self.predecessor_execution_manifest_sha256 != source.execution_manifest_sha256
                or self.predecessor_revision > source.revision
                or (self.predecessor_revision == source.revision) != (self.predecessor_head_sha256 == source.head_sha256)
            ))):
            raise ValueError("inherited recreation predecessor epoch binding changed")
        return self
