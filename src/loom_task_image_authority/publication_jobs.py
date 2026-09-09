"""Owned durable publication inputs and worker fences, with no execution authority."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

import rfc8785
from pydantic import Field, field_validator, model_validator

from loom_task_image_authority.config import _validate_https_origin
from loom_task_image_authority.contracts import Digest, Identifier, SlurmClusterId, SlurmJobId
from loom_task_image_authority.http_contracts import TaskImagePublicationCandidateResponseV2
from loom_task_image_authority.publication_contracts import (
    CanonicalUUID,
    PublicationDescriptor,
    SafeNonnegativeInteger,
    SafePositiveInteger,
    _ClosedPublicationModel,
    _reject_constant,
    _unique_object,
)

MAX_SNAPSHOT_BYTES = 4 * 1024**2
OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
PublicationFailureCode = Literal["integrity", "authority_lost", "verification_failed", "deadline"]


class PublicationJobConflictError(RuntimeError):
    """Fixed operation or durable input no longer agrees with its binding."""


class PublicationJobAuthorizationError(RuntimeError):
    """Current live publication authority is unavailable."""


class PublicationJobOwnershipError(PublicationJobConflictError):
    """Worker fence expired, was superseded, or cannot be acquired."""


class PublicationSnapshotComponent(_ClosedPublicationModel):
    candidate: TaskImagePublicationCandidateResponseV2
    candidate_sha256: Digest

    @field_validator("candidate", mode="before")
    @classmethod
    def _owned_candidate(cls, value: Any) -> TaskImagePublicationCandidateResponseV2:
        if isinstance(value, TaskImagePublicationCandidateResponseV2):
            value = value.model_dump(mode="json")
        return TaskImagePublicationCandidateResponseV2.model_validate_json(json.dumps(value))

    @model_validator(mode="after")
    def _hash(self) -> PublicationSnapshotComponent:
        encoded = rfc8785.dumps(self.candidate.model_dump(mode="json", exclude_none=False))
        if hashlib.sha256(encoded).hexdigest() != self.candidate_sha256:
            raise ValueError("publication candidate hash mismatch")
        if self.candidate.manifest_size > 4 * 1024**2:
            raise ValueError("publication root exceeds manifest ceiling")
        return self

    @property
    def root(self) -> PublicationDescriptor:
        return PublicationDescriptor(
            media_type=OCI_MANIFEST_MEDIA_TYPE,
            digest=self.candidate.manifest_digest,
            size=self.candidate.manifest_size,
        )


class PublicationSnapshot(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-publication-job-input/v1"] = Field(alias="schema")
    materialization_id: CanonicalUUID
    materialization_key: Digest
    task_id: Annotated[str, Field(min_length=1, max_length=512)]
    task_checksum: Digest
    platform: Literal["linux/amd64", "linux/arm64"]
    purpose: Literal["production"]
    attempt_id: CanonicalUUID
    attempt_number: SafePositiveInteger
    lease_epoch: SafePositiveInteger
    builder_id: Annotated[str, Field(pattern=r"^rootless:[0-9a-f]{32}$")]
    grant_id: CanonicalUUID
    original_claim_session_id: CanonicalUUID
    original_claim_session_generation: SafePositiveInteger
    frozen_plan_sha256: Digest
    environment: Identifier
    pool_id: Identifier
    slurm_cluster_id: SlurmClusterId
    slurm_job_id: SlurmJobId
    build_policy_sha256: Digest
    builder_release_sha256: Digest
    supervisor_executable_sha256: Digest
    containment_attestation_sha256: Digest
    registry_origin: Annotated[str, Field(max_length=512)]
    components: Annotated[
        tuple[PublicationSnapshotComponent, ...], Field(min_length=1, max_length=128)
    ]

    @model_validator(mode="after")
    def _bindings(self) -> PublicationSnapshot:
        _validate_https_origin(self.registry_origin, label="publication registry origin")
        if (self.slurm_cluster_id, self.platform) not in {
            ("gb10", "linux/arm64"),
            ("oldlab", "linux/amd64"),
        } or self.builder_id != "rootless:" + self.original_claim_session_id.replace("-", ""):
            raise ValueError("publication snapshot provenance mismatch")
        names = tuple(item.candidate.component for item in self.components)
        if names != tuple(sorted(set(names), key=lambda name: (name != "task", name))):
            raise ValueError("publication components must be unique and canonical")
        for item in self.components:
            candidate = item.candidate
            for field in (
                "materialization_id",
                "attempt_id",
                "grant_id",
                "attempt_number",
                "lease_epoch",
                "builder_id",
                "platform",
            ):
                if str(getattr(candidate, field)) != str(getattr(self, field)):
                    raise ValueError("publication snapshot candidate binding mismatch")
        return self


class PublicationWorkerLease(_ClosedPublicationModel):
    owner_id: CanonicalUUID
    generation: SafePositiveInteger
    expires_at: datetime

    @field_validator("expires_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("publication lease requires aware UTC time")
        return value.astimezone(UTC)


class PublicationJob(_ClosedPublicationModel):
    operation_id: CanonicalUUID
    state: Literal["queued", "running", "failed", "completed"]
    snapshot_sha256: Digest
    snapshot: PublicationSnapshot
    created_at: datetime
    deadline: datetime
    available_at: datetime
    worker_generation: SafeNonnegativeInteger
    lease: PublicationWorkerLease | None = None
    failure_code: PublicationFailureCode | None = None

    @field_validator("created_at", "deadline", "available_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("publication job requires aware UTC time")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _shape(self) -> PublicationJob:
        if (
            not self.created_at < self.deadline <= self.created_at + timedelta(seconds=7200)
            or self.available_at < self.created_at
            or (self.state == "running") != (self.lease is not None)
            or (self.state == "failed") != (self.failure_code is not None)
            or (
                self.lease is not None
                and (
                    self.lease.generation != self.worker_generation
                    or not self.created_at < self.lease.expires_at <= self.deadline
                )
            )
            or hashlib.sha256(canonical_snapshot_bytes(self.snapshot)).hexdigest()
            != self.snapshot_sha256
        ):
            raise ValueError("publication job state binding mismatch")
        return self


def canonical_snapshot_bytes(snapshot: PublicationSnapshot) -> bytes:
    # Reparse to own and validate even instances constructed through unchecked APIs.
    value = snapshot.model_dump_json(by_alias=True, exclude_none=True)
    checked = PublicationSnapshot.model_validate_json(value)
    encoded = rfc8785.dumps(checked.model_dump(mode="json", by_alias=True, exclude_none=True))
    if len(encoded) > MAX_SNAPSHOT_BYTES:
        raise ValueError("publication snapshot exceeds byte ceiling")
    return encoded


def decode_publication_snapshot(encoded: bytes) -> PublicationSnapshot:
    if type(encoded) is not bytes or not 0 < len(encoded) <= MAX_SNAPSHOT_BYTES:
        raise ValueError("publication snapshot exceeds byte ceiling")
    try:
        payload = json.loads(
            encoded, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        snapshot = PublicationSnapshot.model_validate_json(json.dumps(payload))
        canonical_snapshot_bytes(snapshot)
        return snapshot
    except (UnicodeError, TypeError, RecursionError) as exc:
        raise ValueError("invalid publication snapshot") from exc
