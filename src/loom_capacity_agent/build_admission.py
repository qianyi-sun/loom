"""Purpose-specific management/executor build admission envelopes."""

from typing import Literal
from uuid import UUID

from pydantic import Field

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2
from loom_capacity_manager.contracts import Digest, StrictV1Model
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
