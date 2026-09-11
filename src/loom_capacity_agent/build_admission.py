"""Purpose-specific management/executor build admission envelopes."""

from pydantic import Field

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2
from loom_capacity_manager.contracts import Digest, StrictV1Model
from loom_capacity_manager.executable_contracts import ExecutableBootstrapRegistrationV2


class BuildPreparationRequestV1(StrictV1Model):
    registration: ExecutableBootstrapRegistrationV2
    bootstrap_sha256: Digest


class BuildRegistrationRequestV1(StrictV1Model):
    """One sealed bootstrap exchange; never an application worker credential."""

    registration: ExecutableWorkerRegistrationV2
    bootstrap_capability: str = Field(min_length=43, max_length=512,
        pattern=r"^[A-Za-z0-9_-]+$", repr=False)
