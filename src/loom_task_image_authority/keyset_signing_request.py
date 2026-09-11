"""Canonical preparation request, not caller-selected signing authority.

The dedicated signer must independently reconstruct these public facts from its
trusted database and choose its configured root, domain and clock. A request is
neither an authenticated keyset nor evidence that an artifact was committed.
"""

from __future__ import annotations

import json
from typing import Annotated, Literal

import rfc8785
from pydantic import Field, model_validator

from loom_task_image_authority.contracts import Identifier
from loom_task_image_authority.publication_contracts import (
    SafeNonnegativeInteger,
    SafePositiveInteger,
    _ClosedPublicationModel,
    _reject_constant,
    _unique_object,
)
from loom_task_image_authority.publication_keyset import (
    MAX_KEYSET_BYTES,
    PublicationVerificationKey,
)


class KeysetSigningRequest(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-keyset-signing-request/v1"] = Field(alias="schema")
    environment: Identifier
    previous_keyset_version: SafeNonnegativeInteger
    proposed_keyset_version: SafePositiveInteger
    revocation_epoch: SafeNonnegativeInteger
    keys: Annotated[tuple[PublicationVerificationKey, ...], Field(min_length=1, max_length=128)]

    @model_validator(mode="after")
    def _bindings(self) -> KeysetSigningRequest:
        names = tuple(key.key_id for key in self.keys)
        if (
            self.proposed_keyset_version != self.previous_keyset_version + 1
            or names != tuple(sorted(set(names)))
            or len({key.public_key for key in self.keys}) != len(self.keys)
        ):
            raise ValueError("invalid keyset signing preparation")
        return self


def canonical_keyset_signing_request(request: KeysetSigningRequest) -> bytes:
    if type(request) is not KeysetSigningRequest:
        raise ValueError("invalid keyset signing request model")
    checked = KeysetSigningRequest.model_validate(request.model_dump(mode="json", by_alias=True, exclude_none=True))
    wire = rfc8785.dumps(checked.model_dump(mode="json", by_alias=True, exclude_none=True))
    if len(wire) > MAX_KEYSET_BYTES:
        raise ValueError("keyset signing request exceeds ceiling")
    return wire


def decode_keyset_signing_request(wire: bytes) -> KeysetSigningRequest:
    if type(wire) is not bytes or not 0 < len(wire) <= MAX_KEYSET_BYTES:
        raise ValueError("keyset signing request exceeds ceiling")
    try:
        request = KeysetSigningRequest.model_validate(json.loads(
            wire.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant,
        ))
        if canonical_keyset_signing_request(request) != wire:
            raise ValueError("noncanonical keyset signing request")
        return request
    except (ValueError, TypeError, RecursionError):
        raise ValueError("invalid canonical keyset signing request") from None
