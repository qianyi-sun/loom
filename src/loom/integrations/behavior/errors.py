"""Stable BEHAVIOR adapter errors and process exit codes."""

from __future__ import annotations

from enum import IntEnum


class BehaviorExitCode(IntEnum):
    SUCCESS = 0
    CONTRACT_ERROR = 20
    PROVIDER_TRANSIENT = 21
    INFRASTRUCTURE_TRANSIENT = 22
    INTERNAL_DEFECT = 23
    SIGINT = 130
    SIGTERM = 143


class BehaviorContractError(ValueError):
    """The persisted document violates the closed BEHAVIOR wire contract."""


class BehaviorProviderTransientError(RuntimeError):
    """A classified Gateway/provider failure eligible for Attempt retry."""


class BehaviorInfrastructureTransientError(RuntimeError):
    """A local shim/runner/MCP startup or crash eligible for Attempt retry."""


class BehaviorInterruptedError(Exception):
    """A supervised adapter received SIGINT or SIGTERM."""

    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(f"BEHAVIOR adapter interrupted by signal {signum}")


class CanonicalDocumentError(BehaviorContractError):
    """The input is not the one canonical JCS+LF representation."""
