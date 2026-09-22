"""Narrow provider boundary; errors carry codes, never upstream response bodies."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from loom_service.environment_management.registry import OperationLease
from loom_service.environment_management.steps import ProvisioningStep


class ProviderError(RuntimeError):
    def __init__(self, code: str):
        if re.fullmatch(r"[a-z][a-z0-9_]{0,95}", code) is None:
            raise ValueError("invalid provider error code")
        self.code = code
        super().__init__(code)


class ProviderRetryError(ProviderError):
    """The effect may already exist: retry the SAME durable intent."""


class ProviderWaitingError(ProviderRetryError):
    """Normal readiness progress, not a failed external mutation attempt."""


class ProviderBlockedError(ProviderError):
    """Ownership, configuration or policy needs repair; do not retry blindly."""


@dataclass(frozen=True)
class ProvisioningContext:
    lease: OperationLease
    registration: dict[str, Any]
    config: dict[str, Any]
    identities: dict[str, str]
    documents: dict[str, dict[str, Any]] = field(default_factory=dict)
    action: Literal["create", "destroy_retained"] = "create"
    source: ProvisioningContext | None = None

    @property
    def namespaces(self) -> tuple[str, ...]:
        return tuple(self.registration[key] for key in (
            "application_namespace", "execution_namespace", "build_namespace",
        ))


class EnvironmentProvider(Protocol):
    async def apply(self, context: ProvisioningContext, step: ProvisioningStep) -> str:
        """Reconcile immutable intent and return its verified provider identity."""
        ...
