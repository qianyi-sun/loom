"""Explicit storage layout; never infer new authority from a runtime mode change.

This pure binding grants no provisioning or transfer permission. Management must
persist it under the owner operation fence and carry it through every storage
consumer before enabling incarnation storage.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import field_validator

from loom.dev_instance import DevInstanceIdentity, derive_identity, validate_name
from loom_capacity_manager.contracts import StrictV1Model, canonical_bytes, canonical_digest

if TYPE_CHECKING:
    from loom.personal_dev_environment import PersonalDevReconciliationClaim

_MAX_BINDING_BYTES = 16 * 1024
STORAGE_BINDING_ANNOTATION = "loom.dev/storage-binding"
STORAGE_BINDING_SHA_ANNOTATION = "loom.dev/storage-binding-sha256"


class PersonalDevStorageBindingV1(StrictV1Model):
    """Owner/incarnation provenance plus a versioned, non-overridable layout."""

    layout: Literal["legacy-name-v1", "incarnation-v1"]
    environment_name: str
    subject_id: UUID
    subject_incarnation: UUID
    owner_user_id: UUID
    owner_team_id: UUID

    @field_validator("environment_name")
    @classmethod
    def _name(cls, value: str) -> str:
        validate_name(value)
        return value

    @field_validator("subject_id", "subject_incarnation", "owner_user_id", "owner_team_id")
    @classmethod
    def _nonzero_identity(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("storage identity must be nonzero")
        return value

    @property
    def identity(self) -> DevInstanceIdentity:
        legacy = derive_identity(self.environment_name)
        if self.layout == "legacy-name-v1":
            return legacy
        slug = self.environment_name.replace("-", "_")
        database = f"ld_{slug}_{self.subject_incarnation.hex}"
        bucket_prefix = f"ld-{self.environment_name}-{self.subject_incarnation.hex}"
        return replace(
            legacy,
            database=database,
            db_role=database,
            task_bucket=f"{bucket_prefix}-t",
            trajectories_bucket=f"{bucket_prefix}-j",
            artifacts_bucket=f"{bucket_prefix}-a",
            storage_incarnation=self.subject_incarnation,
            storage_binding=self,
        )

    @property
    def object_store_identity(self) -> tuple[str, str]:
        """Exact MinIO access-key identity and policy name, not secret material."""
        if self.layout == "legacy-name-v1":
            return f"loomdev-{self.environment_name}", f"loom-dev-{self.environment_name}"
        name = f"ld-{self.environment_name}-{self.subject_incarnation.hex}"
        return name, name


def parse_personal_dev_storage_binding(
    payload: bytes, *, expected_sha256: str,
) -> PersonalDevStorageBindingV1:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_BINDING_BYTES:
        raise ValueError("storage binding exceeds its byte bound")
    binding = PersonalDevStorageBindingV1.model_validate_json(payload)
    if canonical_bytes(binding) != payload or canonical_digest(binding) != expected_sha256:
        raise ValueError("storage binding differs from its pinned canonical identity")
    return binding


def validate_personal_dev_storage_identity(identity: DevInstanceIdentity) -> DevInstanceIdentity:
    """Reject partial metadata or resource overrides before credential/storage I/O."""
    if identity.storage_binding is None:
        if identity.storage_incarnation is not None:
            raise ValueError("personal storage identity is missing its owner binding")
        expected = derive_identity(identity.name)
    else:
        binding = parse_personal_dev_storage_binding(
            canonical_bytes(identity.storage_binding),
            expected_sha256=canonical_digest(identity.storage_binding),
        )
        if binding.layout != "incarnation-v1":
            raise ValueError("personal storage metadata must select the explicit incarnation layout")
        expected = binding.identity
    if identity != expected:
        raise ValueError("personal storage identity contains a noncanonical resource override")
    return expected


def personal_dev_storage_annotations(identity: DevInstanceIdentity) -> dict[str, str]:
    identity = validate_personal_dev_storage_identity(identity)
    if identity.storage_binding is None:
        return {}
    return {
        STORAGE_BINDING_ANNOTATION: canonical_bytes(identity.storage_binding).decode(),
        STORAGE_BINDING_SHA_ANNOTATION: canonical_digest(identity.storage_binding),
    }


def personal_dev_storage_secret_data(identity: DevInstanceIdentity) -> dict[str, bytes]:
    identity = validate_personal_dev_storage_identity(identity)
    if identity.storage_binding is None:
        return {}
    return {
        "storage-binding.json": canonical_bytes(identity.storage_binding),
        "storage-binding.sha256": canonical_digest(identity.storage_binding).encode(),
    }


def resolve_personal_dev_storage_identity(claim: PersonalDevReconciliationClaim) -> DevInstanceIdentity:
    """Resolve a current, owner-consistent claim without inferring layout from mode.

    This is an I/O entrypoint fence, not a substitute for the durable lease or
    namespace UID preconditions required by mutation and cleanup operations.
    """
    environment, operation, attempt, candidate = (
        claim.environment, claim.operation, claim.attempt, claim.candidate,
    )
    if (
        environment.name != operation.environment_name
        or environment.operation_id != operation.id
        or environment.operation_epoch != operation.operation_epoch
        or environment.subject_id != operation.subject_id
        or environment.subject_incarnation != operation.subject_incarnation
        or environment.owner_user_id != operation.owner_user_id
        or environment.owner_team_id != operation.owner_team_id
        or environment.storage_binding != operation.storage_binding
        or attempt.id != operation.attempt_id
        or attempt.operation_id != operation.id
        or attempt.operation_epoch != operation.operation_epoch
        or attempt.subject_id != operation.subject_id
        or attempt.subject_incarnation != operation.subject_incarnation
        or attempt.attempt_sequence != operation.attempt_sequence
        or candidate.id != operation.candidate_id
        or candidate.candidate_sha != operation.candidate_sha
        or candidate.owner_user_id != operation.owner_user_id
        or candidate.owner_team_id != operation.owner_team_id
    ):
        raise ValueError("personal storage claim has stale or inconsistent owner coordinates")
    saved = operation.storage_binding
    if saved is None:
        return derive_identity(operation.environment_name)
    binding = parse_personal_dev_storage_binding(canonical_bytes(saved), expected_sha256=canonical_digest(saved))
    if (
        binding.layout != "incarnation-v1"
        or binding.environment_name != operation.environment_name
        or binding.subject_id != operation.subject_id
        or binding.subject_incarnation != operation.subject_incarnation
        or binding.owner_user_id != operation.owner_user_id
        or binding.owner_team_id != operation.owner_team_id
    ):
        raise ValueError("personal storage binding differs from its owner claim")
    return binding.identity
