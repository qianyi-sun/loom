"""Small durable-job projections for later authenticated fixed submit/poll APIs.

Projection is not authentication, signature verification or database completion
proof. Callers must authenticate the current session, authorize the exact job,
and verify any historical receipt through the completion store before projecting.
Never serialize PublicationJob directly into an HTTP or guard polling response.
"""

from __future__ import annotations

from typing import Annotated, Literal

import rfc8785
from pydantic import Field, model_validator

from loom_task_image_authority.contracts import Digest
from loom_task_image_authority.publication_contracts import (
    CanonicalUUID,
    SafePositiveInteger,
    _ClosedPublicationModel,
    _decode,
)
from loom_task_image_authority.publication_jobs import (
    PublicationFailureCode,
    PublicationJob,
    canonical_snapshot_bytes,
    decode_publication_snapshot,
)
from loom_task_image_authority.publication_receipts import (
    PublicationCandidateIdentity,
    PublicationReceipt,
    candidate_set_sha256,
    canonical_receipt_bytes,
    decode_publication_receipt,
)

MAX_STATUS_BYTES = 4096


class PublicationStatus(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-publication-status/v1"] = Field(alias="schema")
    grant_id: CanonicalUUID
    operation_id: CanonicalUUID
    materialization_id: CanonicalUUID
    attempt_id: CanonicalUUID
    lease_epoch: SafePositiveInteger
    state: Literal["queued", "running", "failed", "completed"]
    snapshot_sha256: Digest
    candidate_set_sha256: Digest
    component_count: Annotated[int, Field(strict=True, ge=1, le=128)]
    receipt: PublicationReceipt | None = None
    failure_code: PublicationFailureCode | None = None

    @model_validator(mode="after")
    def _terminal_binding(self) -> PublicationStatus:
        if (self.state == "completed") != (self.receipt is not None) or (
            self.state == "failed"
        ) != (self.failure_code is not None):
            raise ValueError("publication status terminal shape mismatch")
        if self.receipt is not None:
            for field in (
                "operation_id",
                "materialization_id",
                "attempt_id",
                "lease_epoch",
                "snapshot_sha256",
                "candidate_set_sha256",
                "component_count",
            ):
                if getattr(self.receipt, field) != getattr(self, field):
                    raise ValueError("publication status receipt binding mismatch")
        return self


def decode_publication_status(encoded: bytes) -> PublicationStatus:
    return _decode(encoded, PublicationStatus, MAX_STATUS_BYTES)


def canonical_status_bytes(status: PublicationStatus) -> bytes:
    checked = PublicationStatus.model_validate_json(
        status.model_dump_json(by_alias=True, exclude_none=True)
    )
    encoded = rfc8785.dumps(checked.model_dump(mode="json", by_alias=True, exclude_none=True))
    if len(encoded) > MAX_STATUS_BYTES:
        raise ValueError("publication status exceeds byte ceiling")
    return encoded


def project_publication_status(
    job: PublicationJob, *, receipt: PublicationReceipt | None = None
) -> PublicationStatus:
    """Own validated nonsecret scalar bindings; never return frozen graph inputs."""
    # Job times are internal aware datetimes, not this response's wire fields.
    # Reparse the separate canonical snapshot while preserving those strict types.
    fields = job.model_dump(by_alias=True, exclude_none=True, exclude={"snapshot"})
    fields["snapshot"] = decode_publication_snapshot(canonical_snapshot_bytes(job.snapshot))
    checked = PublicationJob.model_validate(fields)
    snapshot = checked.snapshot
    identity_hash = candidate_set_sha256(
        tuple(
            PublicationCandidateIdentity(
                candidate_id=str(item.candidate.candidate_id), component=item.candidate.component
            )
            for item in snapshot.components
        )
    )
    values: dict[str, object] = dict(
        schema="loom.task-image-publication-status/v1",
        grant_id=snapshot.grant_id,
        operation_id=checked.operation_id,
        materialization_id=snapshot.materialization_id,
        attempt_id=snapshot.attempt_id,
        lease_epoch=snapshot.lease_epoch,
        state=checked.state,
        snapshot_sha256=checked.snapshot_sha256,
        candidate_set_sha256=identity_hash,
        component_count=len(snapshot.components),
    )
    if receipt is not None:
        owned_receipt = decode_publication_receipt(canonical_receipt_bytes(receipt))
        if owned_receipt.worker_generation != checked.worker_generation:
            raise ValueError("publication status worker generation mismatch")
        values["receipt"] = owned_receipt
    if checked.failure_code is not None:
        values["failure_code"] = checked.failure_code
    return PublicationStatus.model_validate(values)
