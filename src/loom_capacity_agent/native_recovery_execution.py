"""Versioned live execution authority bound to committed recovery finalization."""

from datetime import UTC, datetime, timedelta
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_agent.build_admission import BuildClaimRequestV1
from loom_capacity_manager.contracts import Digest
from loom_capacity_manager.executable_contracts import StrictV2Model, canonical_executable_digest


class BuildExecutionRequestV2(StrictV2Model):
    claim: BuildClaimRequestV1
    challenge: UUID
    source_binding_sha256: Digest
    recovery_finalization_sha256: Digest


class BuildExecutionExchangeV2(StrictV2Model):
    request: BuildExecutionRequestV2
    worker_credential: str = Field(min_length=43, max_length=512, pattern=r"^[A-Za-z0-9_-]+$", repr=False)


class BuildExecutionPermitV2(StrictV2Model):
    """Fresh ten-second fence, not cleanup, quiescence or physical release proof."""

    request: BuildExecutionRequestV2
    request_digest: Digest
    issued_at: datetime
    not_after: datetime
    executable: Literal[True] = True

    @field_validator("issued_at", "not_after")
    @classmethod
    def _aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("native recovery execution permission requires timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _bounded_permission(self) -> Self:
        if (self.request_digest != canonical_executable_digest(self.request)
            or not timedelta(0) < self.not_after - self.issued_at <= timedelta(seconds=10)):
            raise ValueError("native recovery execution permission binding or lifetime changed")
        return self
