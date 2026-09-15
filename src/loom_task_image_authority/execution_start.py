"""Exact online request/receipt bindings; neither type grants offline authority."""

from __future__ import annotations

import hashlib
from typing import Literal

import rfc8785
from pydantic import Field, model_validator

from loom_task_image_authority.contracts import Digest
from loom_task_image_authority.execution_grant import ExecutionClaim
from loom_task_image_authority.publication_contracts import (
    CanonicalUUID,
    PublicationTimestamp,
    SafeNonnegativeInteger,
    SafePositiveInteger,
    _ClosedPublicationModel,
)
from loom_task_image_authority.publication_keyset import _instant


class ExecutionStartRequest(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-execution-start-request/v1"] = Field(alias="schema")
    grant_id: CanonicalUUID
    revision: SafePositiveInteger
    envelope_sha256: Digest
    claim: ExecutionClaim
    keyset_sha256: Digest
    keyset_version: SafePositiveInteger
    revocation_epoch: SafeNonnegativeInteger

    @property
    def digest(self) -> str:
        # Revalidate even an in-process model_copy/model_construct before use.
        raw = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        type(self).model_validate(raw)
        return hashlib.sha256(rfc8785.dumps(raw)).hexdigest()


class ExecutionStartReceipt(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-execution-start-receipt/v1"] = Field(alias="schema")
    start_id: CanonicalUUID
    request_sha256: Digest
    consumed_at: PublicationTimestamp
    expires_at: PublicationTimestamp

    @model_validator(mode="after")
    def _lifetime(self) -> ExecutionStartReceipt:
        seconds = (_instant(self.expires_at) - _instant(self.consumed_at)).total_seconds()
        if not 0 < seconds <= 30:
            raise ValueError("execution start receipt exceeds lifetime")
        return self
