"""Immutable typed launch provenance; structural validation is not admission.

The trusted producer must resolve the exact base or personal-member event and
its configuration/acknowledgement before signing. No caller-selected source,
purpose, event reference or digest authenticates that resolution by itself.
Historical proofs remain useful after supersession, but cannot authorize new
capacity without current manager fences. Legacy executable consumers stay V2.
"""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    ConfigurationGenerationRefV1,
    Digest,
    Identifier,
    PositiveQuantity,
)
from loom_capacity_manager.executable_contracts import ExecutableIntentBindingV2, StrictV2Model


class _StrictOwnershipV3(StrictV2Model):
    schema_version: Literal[3] = 3  # type: ignore[assignment]

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("typed ownership schema must be integer 3")
        return value


def _nonzero_digest(value: str) -> str:
    if value == "0" * 64:
        raise ValueError("typed ownership digest must be nonzero")
    return value


class PersonalMembershipLaunchReferenceV3(_StrictOwnershipV3):
    """The immutable member event selected for this launch, not the latest head."""

    namespace_id: UUID
    owner_id: UUID
    revision: PositiveQuantity
    head_sha256: Digest
    execution_manifest_sha256: Digest

    _digests_nonzero = field_validator("head_sha256", "execution_manifest_sha256")(_nonzero_digest)

    @field_validator("namespace_id", "owner_id")
    @classmethod
    def _nonzero_identity(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("typed ownership identity must be nonzero")
        return value


class ExecutableSubjectAuthorityV3(_StrictOwnershipV3):
    """Exact persisted subject provenance carried into signing and recovery.

An immutable-base reference is allowed only after independent base resolution;
it is not a fallback when a delegated member cannot be authenticated.
acknowledgement_sha256 is the canonical digest of the entire acknowledgement,
not merely its embedded acknowledgement_sha256 evidence field.
"""

    source: Literal["immutable-base", "personal-membership"]
    purpose: Literal["application-worker", "personal-build-worker"]
    configuration: ConfigurationGenerationRefV1
    acknowledgement_sha256: Digest
    membership: PersonalMembershipLaunchReferenceV3 | None = None

    _ack_nonzero = field_validator("acknowledgement_sha256")(_nonzero_digest)

    @model_validator(mode="after")
    def _exact_provenance(self) -> ExecutableSubjectAuthorityV3:
        reference = self.configuration
        if (
            reference.scope != "subject" or reference.digest == "0" * 64
            or reference.subject_id is None or reference.subject_id.int == 0
            or reference.subject_incarnation is None or reference.subject_incarnation.int == 0
        ):
            raise ValueError("typed ownership requires an exact subject configuration")
        if (self.source == "personal-membership") != (self.membership is not None):
            raise ValueError("delegated ownership requires its member event exactly")
        if self.purpose == "personal-build-worker" and self.source != "personal-membership":
            raise ValueError("personal builds require delegated membership provenance")
        return self


class ExecutableOwnershipMetadataV3(_StrictOwnershipV3):
    """Purpose-preserving successor, deliberately not a V2 metadata subtype."""

    binding: ExecutableIntentBindingV2
    subject_authority: ExecutableSubjectAuthorityV3
    launch_profile_sha256: Digest
    controller_authority_sha256: Digest
    trusted_launcher_sha256: Digest
    slurm_cluster: Identifier
    submitter_identity: Identifier
    association: Identifier
    submitted_at: datetime

    _digests_nonzero = field_validator(
        "launch_profile_sha256", "controller_authority_sha256", "trusted_launcher_sha256",
    )(_nonzero_digest)

    @field_validator("submitted_at")
    @classmethod
    def _utc_submission(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("typed ownership submission must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _cross_binding(self) -> ExecutableOwnershipMetadataV3:
        binding = self.binding
        authority = self.subject_authority
        reference = authority.configuration
        if (
            reference.subject_id != binding.subject_id
            or reference.subject_incarnation != binding.subject_incarnation
            or self.trusted_launcher_sha256 != binding.execution.trusted_fleet_release_sha256
        ):
            raise ValueError("typed ownership subject or release binding changed")
        event = authority.membership
        if event is not None and (
            event.execution_manifest_sha256 != binding.execution.execution_manifest_sha256
            or binding.tier_id != "development"
            or binding.account_id != f"dev-owner-{event.owner_id.hex}"
        ):
            raise ValueError("typed ownership member owner or execution binding changed")
        if authority.purpose == "personal-build-worker" and (
            binding.candidate.algorithm != "git-sha1"
            or binding.candidate.identity == "0" * 40
            or binding.candidate.publication_sha256 == "0" * 64
            or binding.concurrency_slots != 1 or len(binding.node_ids) != 1
            or binding.rollout_surge_slots != 0 or binding.old_shape_backing_id is not None
        ):
            raise ValueError("personal build ownership requires one cold native runtime slot")
        return self


class SignedExecutableOwnershipProofV3(_StrictOwnershipV3):
    """Domain-separated signature; never accepted as a legacy executable proof."""

    metadata: ExecutableOwnershipMetadataV3
    signing_key_id: Identifier
    signature_base64: Annotated[str, Field(min_length=88, max_length=88)]

    @field_validator("signature_base64")
    @classmethod
    def _canonical_signature(cls, value: str) -> str:
        try:
            decoded = base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("typed ownership signature must be canonical base64") from exc
        if len(decoded) != 64 or base64.b64encode(decoded).decode("ascii") != value:
            raise ValueError("typed ownership signature must encode 64 bytes")
        return value


def _exact_schema_types(value: object) -> None:
    if isinstance(value, dict):
        if "schema_version" in value and type(value["schema_version"]) is not int:
            raise ValueError("ownership wire versions must be exact integers")
        for nested in value.values():
            _exact_schema_types(nested)
    elif isinstance(value, list):
        for nested in value:
            _exact_schema_types(nested)


def canonical_typed_ownership_bytes(contract: ExecutableOwnershipMetadataV3 | SignedExecutableOwnershipProofV3) -> bytes:
    """Revalidate all nested models before bounded canonical signing/verification."""
    if type(contract) not in (ExecutableOwnershipMetadataV3, SignedExecutableOwnershipProofV3):
        raise ValueError("typed ownership encoding requires an exact V3 contract")
    value = contract.model_dump(mode="json", exclude_none=False)
    _exact_schema_types(value)
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise ValueError("typed ownership exceeds its byte bound")
    checked = type(contract).model_validate_json(encoded)
    canonical = json.dumps(checked.model_dump(mode="json", exclude_none=False), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    if encoded != canonical:
        raise ValueError("typed ownership model is not canonical")
    return canonical


def parse_typed_executable_ownership(payload: bytes) -> SignedExecutableOwnershipProofV3:
    if not isinstance(payload, bytes) or len(payload) > MAX_CONTRACT_BYTES:
        raise ValueError("typed ownership payload exceeds its byte bound")
    proof = SignedExecutableOwnershipProofV3.model_validate_json(payload)
    if canonical_typed_ownership_bytes(proof) != payload:
        raise ValueError("typed ownership payload is not canonical")
    return proof
