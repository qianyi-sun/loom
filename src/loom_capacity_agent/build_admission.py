"""Purpose-specific management/executor build admission envelopes."""

import base64
from datetime import UTC, datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_agent.admission import ExecutableReleaseRequestV2, ExecutableWorkerRegistrationV2
from loom_capacity_manager.contracts import Digest, PositiveQuantity, Quantity, StrictV1Model
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
)


class BuildPreparationRequestV1(StrictV1Model):
    registration: ExecutableBootstrapRegistrationV2
    bootstrap_sha256: Digest


class BuildRegistrationRequestV1(StrictV1Model):
    """One sealed bootstrap exchange; never an application worker credential."""

    registration: ExecutableWorkerRegistrationV2
    bootstrap_capability: str = Field(min_length=43, max_length=512,
        pattern=r"^[A-Za-z0-9_-]+$", repr=False)


class BuildClaimRequestV1(StrictV1Model):
    """Claim only the request already allocated to this registered native worker."""

    binding: ExecutableIntentBindingV2
    operation_id: UUID
    request_id: UUID
    worker_id: UUID
    worker_incarnation: UUID


class BuildClaimReceiptV1(StrictV1Model):
    """Immutable claim evidence; replay does not renew execution authority."""

    request: BuildClaimRequestV1
    request_digest: Digest
    claim_high_water: Literal[1] = 1


class BuildClaimExchangeV1(StrictV1Model):
    """Trusted-wrapper claim authentication, excluded from retained claim bytes."""

    claim: BuildClaimRequestV1
    worker_credential: str = Field(min_length=43, max_length=512,
        pattern=r"^[A-Za-z0-9_-]+$", repr=False)


class BuildArtifactV1(StrictV1Model):
    """Unverified archive facts; not an OCI verification or publication permit."""

    archive_sha256: Digest
    archive_size_bytes: PositiveQuantity


class BuildSourceReadExchangeV1(BuildClaimExchangeV1):
    """One bounded source read, authenticated anew on every request."""

    offset: Quantity
    length: Annotated[int, Field(ge=1, le=1024 * 1024)]


class BuildSourceReadReceiptV1(StrictV1Model):
    """Source bytes only; no object-store coordinates or reusable IO capability."""

    claim_digest: Digest
    source_binding_sha256: Digest
    archive_sha256: Digest
    archive_size_bytes: PositiveQuantity
    offset: Quantity
    data_base64: Annotated[str, Field(min_length=4, max_length=1398104, repr=False)]

    @property
    def data(self) -> bytes:
        return base64.b64decode(self.data_base64, validate=True)

    @model_validator(mode="after")
    def _bounded_data(self) -> Self:
        data = self.data
        if (not 1 <= len(data) <= 1024 * 1024 or self.offset + len(data) > self.archive_size_bytes
            or base64.b64encode(data).decode("ascii") != self.data_base64):
            raise ValueError("native source response bytes are invalid")
        return self


class BuildSourceContextV1(StrictV1Model):
    """Current claim-bound metadata, never a build-start or object-store permit.

    source_sha256 already identifies the complete canonical source manifest;
    the worker verifies it inside the sealed archive instead of receiving a
    second, potentially large manifest through the admission channel.
    """

    claim_digest: Digest
    request_id: UUID
    source_binding_sha256: Digest
    platform: Literal["linux/amd64", "linux/arm64"]
    candidate_id: UUID
    candidate_sha: Digest
    source_sha256: Digest
    archive_sha256: Digest
    archive_size_bytes: PositiveQuantity
    build_contract_sha256: Digest
    source_commit: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    dirty: bool
    attempt_id: UUID
    attempt_sequence: Quantity
    lease_epoch: PositiveQuantity
    subject_id: UUID
    subject_incarnation: UUID
    operation_id: UUID
    operation_epoch: PositiveQuantity
    lease_not_after: datetime

    @field_validator("lease_not_after")
    @classmethod
    def _aware_deadline(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("native context deadline requires timezone")
        return value.astimezone(UTC)


class BuildOutcomeRequestV1(StrictV1Model):
    """Historical wrapper result for one exact claim, even after lease expiry."""

    claim: BuildClaimRequestV1
    operation_id: UUID
    result: Literal["artifact-ready", "failed", "cancelled"]
    artifact: BuildArtifactV1 | None = None

    @model_validator(mode="after")
    def _artifact_boundary(self) -> Self:
        if (self.result == "artifact-ready") != (self.artifact is not None):
            raise ValueError("only artifact-ready outcomes require archive evidence")
        return self


class BuildInterruptedOutcomeRequestV1(StrictV1Model):
    """Management-only physical-terminal settlement, never a worker report."""

    claim: BuildClaimRequestV1
    operation_id: UUID
    result: Literal["interrupted"] = "interrupted"
    terminal_inventory_sha256: Digest


class BuildOutcomeReceiptV1(StrictV1Model):
    request: Annotated[BuildOutcomeRequestV1 | BuildInterruptedOutcomeRequestV1, Field(discriminator="result")]
    request_digest: Digest
    claim_high_water: Literal[1] = 1
    live_claim_count: Literal[0] = 0
    executable: Literal[False] = False


class BuildOutcomeExchangeV1(StrictV1Model):
    """Transport-only wrapper authentication, never retained outcome evidence."""

    outcome: BuildOutcomeRequestV1
    worker_credential: str = Field(min_length=43, max_length=512,
        pattern=r"^[A-Za-z0-9_-]+$", repr=False)


class BuildReleaseExchangeV1(StrictV1Model):
    """Worker-authenticated release; terminal proof is management-only."""

    release: ExecutableReleaseRequestV2
    worker_credential: str = Field(min_length=43, max_length=512,
        pattern=r"^[A-Za-z0-9_-]+$", repr=False)


def native_build_artifact_key(claim: BuildClaimRequestV1) -> str:
    """A retry cannot overwrite another assignment's accepted archive identity."""
    claim = BuildClaimRequestV1.model_validate_json(claim.model_dump_json())
    return f"personal-dev/native-claims/{claim.request_id}/{claim.binding.intent_id}/{claim.operation_id}/artifact.tar"
