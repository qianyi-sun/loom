"""Purpose-specific management/executor build admission envelopes."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2
from loom_capacity_manager.contracts import Digest, PositiveQuantity, StrictV1Model
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


def native_build_artifact_key(claim: BuildClaimRequestV1) -> str:
    """A retry cannot overwrite another assignment's accepted archive identity."""
    claim = BuildClaimRequestV1.model_validate_json(claim.model_dump_json())
    return f"personal-dev/native-claims/{claim.request_id}/{claim.binding.intent_id}/{claim.operation_id}/artifact.tar"
