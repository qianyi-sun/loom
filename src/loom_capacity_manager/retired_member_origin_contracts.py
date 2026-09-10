"""Structural inherited provenance, not proof of retirement or admission.

Durable consumers must authenticate the complete final source snapshot, the
subject's last real event (possibly inherited), and the original root. These
values never rewrite an old member's epoch-local recreation certificate.
"""

from __future__ import annotations

from pydantic import field_validator, model_validator

from loom_capacity_manager.build_value_contracts import (
    PersonalBuildMemberV1,
    PersonalMemberV2,
    personal_build_subject_id,
)
from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    Digest,
    PositiveQuantity,
    canonical_digest,
)
from loom_capacity_manager.inherited_reincarnation_contracts import (
    PersonalInheritedReincarnationEvidenceV2,
)
from loom_capacity_manager.retired_source_reference import (
    RetiredMembershipSnapshotReferenceV1 as RetiredMembershipSnapshotReferenceV1,
)
from loom_capacity_manager.retired_source_reference import (
    _StrictOriginV1 as _StrictOriginV1,
)


class PersonalMemberEventAnchorV1(_StrictOriginV1):
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    revision: PositiveQuantity
    head_sha256: Digest
    member: PersonalMemberV2

    @model_validator(mode="after")
    def _exact_member(self) -> PersonalMemberEventAnchorV1:
        if self.execution_manifest_sha256 == "0" * 64 or self.head_sha256 == "0" * 64:
            raise ValueError("retired member event requires nonzero hashes")
        if self.member.revision != self.revision:
            raise ValueError("retired member differs from its own event revision")
        proof = self.member.reincarnation
        if proof is not None and proof.execution_manifest_sha256 != self.execution_manifest_sha256:
            raise ValueError("retired member certificate belongs to another event epoch")
        if isinstance(proof, PersonalInheritedReincarnationEvidenceV2) and proof.execution_epoch != self.execution_epoch:
            raise ValueError("retired member certificate belongs to another event epoch")
        return self


class RetiredPersonalMemberOriginV1(_StrictOriginV1):
    source: RetiredMembershipSnapshotReferenceV1
    anchor: PersonalMemberEventAnchorV1
    original_origin: ConfigurationGenerationRefV1

    @field_validator("original_origin", mode="before")
    @classmethod
    def _exact_root_version(cls, value: object) -> object:
        if isinstance(value, dict):
            version = value.get("schema_version", 1)
            if type(version) is not int or version != 1:
                raise ValueError("retired original root schema must be integer 1")
        return value

    @model_validator(mode="after")
    def _source_and_root(self) -> RetiredPersonalMemberOriginV1:
        source, anchor, root = self.source, self.anchor, self.original_origin
        member, config = anchor.member, anchor.member.configuration
        if anchor.execution_epoch > source.execution_epoch:
            raise ValueError("retired member anchor cannot be from a future epoch")
        if anchor.execution_epoch == source.execution_epoch and (
            anchor.execution_manifest_sha256 != source.execution_manifest_sha256
            or anchor.revision > source.revision
            or (anchor.revision == source.revision) != (anchor.head_sha256 == source.head_sha256)
        ):
            raise ValueError("retired member anchor differs from source snapshot")
        if (
            root.scope != "subject" or root.subject_id != config.subject_id or root.digest == "0" * 64
            or root.subject_incarnation is None or root.subject_incarnation.int == 0
            or root.generation > config.configuration_generation
            or (root.generation == config.configuration_generation and (
                root.subject_incarnation != config.subject_incarnation or root.digest != canonical_digest(config)
            ))
        ):
            raise ValueError("retired member original configuration root changed")
        if isinstance(member, PersonalBuildMemberV1) and config.subject_id != personal_build_subject_id(source.namespace_id, member.owner_id):
            raise ValueError("retired build belongs to another namespace")
        proof = member.reincarnation
        if proof is not None and (proof.namespace_id != source.namespace_id or proof.origin != root):
            raise ValueError("retired member recreation lineage changed")
        return self
