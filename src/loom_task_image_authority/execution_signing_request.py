"""Bounded attachments to a committed execution request, never signing authority.

The signer reads the grant and current keyset from its own database. Attachments
must match that grant and pass full publication verification before private I/O.
"""

from __future__ import annotations

from typing import Annotated, Literal

import rfc8785
from pydantic import Field, model_validator

from loom.task_image_build_plan import MAX_TASK_IMAGE_BUILD_PLAN_BYTES
from loom.task_image_materialization import MAX_TASK_IMAGE_COMPONENTS
from loom_task_image_authority.contracts import Digest
from loom_task_image_authority.execution_grant import _canonical_object
from loom_task_image_authority.publication_contracts import (
    MAX_SIGNER_REPLY_BYTES,
    CanonicalUUID,
    SafePositiveInteger,
    _ClosedPublicationModel,
)

MAX_EXECUTION_SIGNING_REQUEST_BYTES = 2 * 1024 * 1024


class ExecutionSigningRequest(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-execution-signing-request/v1"] = Field(alias="schema")
    grant_id: CanonicalUUID
    revision: SafePositiveInteger
    grant_sha256: Digest
    frozen_plan: Annotated[str, Field(min_length=1, max_length=MAX_TASK_IMAGE_BUILD_PLAN_BYTES)]
    publications: Annotated[
        tuple[Annotated[str, Field(min_length=1, max_length=MAX_SIGNER_REPLY_BYTES)], ...],
        Field(min_length=1, max_length=MAX_TASK_IMAGE_COMPONENTS),
    ]

    @model_validator(mode="after")
    def _bounds(self) -> ExecutionSigningRequest:
        if len(self.frozen_plan.encode()) > MAX_TASK_IMAGE_BUILD_PLAN_BYTES or any(
            len(item.encode()) > MAX_SIGNER_REPLY_BYTES for item in self.publications
        ):
            raise ValueError("execution signing attachment exceeds byte ceiling")
        if len(rfc8785.dumps(self.model_dump(mode="json", by_alias=True, exclude_none=True))) > MAX_EXECUTION_SIGNING_REQUEST_BYTES:
            raise ValueError("execution signing request exceeds aggregate ceiling")
        return self

    def canonical_bytes(self) -> bytes:
        data = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        checked = ExecutionSigningRequest.model_validate(data)
        return rfc8785.dumps(checked.model_dump(mode="json", by_alias=True, exclude_none=True))


def decode_execution_signing_request(wire: bytes) -> ExecutionSigningRequest:
    request = ExecutionSigningRequest.model_validate(_canonical_object(wire, MAX_EXECUTION_SIGNING_REQUEST_BYTES))
    if request.canonical_bytes() != wire:
        raise ValueError("execution signing request changed during validation")
    return request
