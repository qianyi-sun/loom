"""Live, claim-scoped source facts; never a transferable download capability."""

from datetime import datetime
from typing import Annotated

from pydantic import Field, field_validator

from loom_capacity_agent.build_admission import BuildClaimRequestV1
from loom_capacity_manager.contracts import Digest, PositiveQuantity, StrictV1Model


class BuildClaimSourceV1(StrictV1Model):
    claim: BuildClaimRequestV1
    claim_digest: Digest
    object_bucket: Annotated[str, Field(min_length=1, max_length=255)]
    object_key: Annotated[str, Field(min_length=1, max_length=4096)]
    archive_sha256: Digest
    archive_size_bytes: PositiveQuantity
    source_binding_sha256: Digest
    lease_not_after: datetime

    @field_validator("lease_not_after")
    @classmethod
    def _aware_deadline(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("native source deadline requires timezone")
        return value
