"""Compact historical completion bindings, not signing or execution authority.

Sets use the frozen plan's order: task first when present, then lexical sidecars.
Candidate identities come from the complete V2 acknowledgement set. An envelope
identity hashes the full RFC 8785 PublicationEnvelope, including its signature,
not just its unsigned statement. The completion store must derive these identities
from validated rows; a supplied hash or receipt is never proof of readiness.
"""

from __future__ import annotations

import hashlib
from typing import Annotated, Literal

import rfc8785
from pydantic import Field

from loom_task_image_authority.contracts import Digest, TaskImageComponent
from loom_task_image_authority.publication_contracts import (
    CanonicalUUID,
    PublicationTimestamp,
    SafePositiveInteger,
    _ClosedPublicationModel,
    _decode,
)

MAX_RECEIPT_BYTES = 2048


class PublicationReceipt(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-publication-receipt/v1"] = Field(alias="schema")
    operation_id: CanonicalUUID
    materialization_id: CanonicalUUID
    attempt_id: CanonicalUUID
    lease_epoch: SafePositiveInteger
    worker_generation: SafePositiveInteger
    snapshot_sha256: Digest
    candidate_set_sha256: Digest
    publication_set_sha256: Digest
    component_count: Annotated[int, Field(strict=True, ge=1, le=128)]
    completed_at: PublicationTimestamp


class PublicationCandidateIdentity(_ClosedPublicationModel):
    candidate_id: CanonicalUUID
    component: TaskImageComponent


class PublicationEnvelopeIdentity(PublicationCandidateIdentity):
    envelope_sha256: Digest


def decode_publication_receipt(encoded: bytes) -> PublicationReceipt:
    return _decode(encoded, PublicationReceipt, MAX_RECEIPT_BYTES)


def canonical_receipt_bytes(receipt: PublicationReceipt) -> bytes:
    # Own and revalidate instances that could have come from model_copy/construct.
    checked = PublicationReceipt.model_validate(receipt.model_dump(by_alias=True))
    encoded = rfc8785.dumps(checked.model_dump(mode="json", by_alias=True))
    if len(encoded) > MAX_RECEIPT_BYTES:
        raise ValueError("publication receipt exceeds byte ceiling")
    return encoded


def _set_sha256(
    identities: tuple[PublicationCandidateIdentity, ...],
    *,
    member_type: type[PublicationCandidateIdentity],
    schema: str,
) -> str:
    if type(identities) is not tuple or not 1 <= len(identities) <= 128:
        raise ValueError("publication identity set exceeds count bound")
    checked = []
    for identity in identities:
        if type(identity) is not member_type:
            raise ValueError("publication identity has unexpected schema")
        checked.append(member_type.model_validate(identity.model_dump()))
    names = tuple(item.component for item in checked)
    if names != tuple(sorted(set(names), key=lambda name: (name != "task", name))) or len(
        {item.candidate_id for item in checked}
    ) != len(checked):
        raise ValueError("publication identity set is ambiguous or unordered")
    encoded = rfc8785.dumps(
        {"schema": schema, "components": [item.model_dump(mode="json") for item in checked]}
    )
    return hashlib.sha256(encoded).hexdigest()


def candidate_set_sha256(identities: tuple[PublicationCandidateIdentity, ...]) -> str:
    """Hash complete acknowledged component/candidate IDs, without DB-only fields."""
    return _set_sha256(
        identities,
        member_type=PublicationCandidateIdentity,
        schema="loom.task-image-publication-candidate-set/v1",
    )


def publication_set_sha256(identities: tuple[PublicationEnvelopeIdentity, ...]) -> str:
    """Hash complete component/candidate IDs and canonical full-envelope digests."""
    return _set_sha256(
        identities,
        member_type=PublicationEnvelopeIdentity,
        schema="loom.task-image-publication-envelope-set/v1",
    )
