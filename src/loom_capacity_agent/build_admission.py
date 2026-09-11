"""Credential-free management/executor build admission envelope."""

from loom_capacity_manager.contracts import Digest, StrictV1Model
from loom_capacity_manager.executable_contracts import ExecutableBootstrapRegistrationV2


class BuildPreparationRequestV1(StrictV1Model):
    registration: ExecutableBootstrapRegistrationV2
    bootstrap_sha256: Digest
