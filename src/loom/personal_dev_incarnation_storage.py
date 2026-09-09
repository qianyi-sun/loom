"""Explicit storage layout; never infer new authority from a runtime mode change.

This pure binding grants no provisioning or transfer permission. Management must
persist it under the owner operation fence and carry it through every storage
consumer before enabling incarnation storage.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Literal
from uuid import UUID

from pydantic import field_validator

from loom.dev_instance import DevInstanceIdentity, derive_identity, validate_name
from loom_capacity_manager.contracts import StrictV1Model, canonical_bytes, canonical_digest

_MAX_BINDING_BYTES = 16 * 1024


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
