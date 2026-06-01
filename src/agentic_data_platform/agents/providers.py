from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class AgentCatalogEntry:
    agent_id: str
    display_name: str
    provider: str
    source: str
    runner_kind: str
    execution_mode: str
    supported_harness_ids: list[str]
    supported_sandbox_backends: list[str]
    required_secret_refs: list[str]
    supports_trajectory: bool
    capabilities: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("agent_id", self.agent_id)
        _require_non_empty("display_name", self.display_name)
        _require_non_empty("provider", self.provider)
        _require_non_empty("source", self.source)
        _require_non_empty("runner_kind", self.runner_kind)
        _require_non_empty("execution_mode", self.execution_mode)
        _require_strings("supported_harness_ids", self.supported_harness_ids)
        _require_strings("supported_sandbox_backends", self.supported_sandbox_backends)
        _require_strings("capabilities", self.capabilities)
        _require_safe_secret_refs(self.required_secret_refs)

    def to_payload(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "provider": self.provider,
            "source": self.source,
            "runner_kind": self.runner_kind,
            "execution_mode": self.execution_mode,
            "supported_harness_ids": list(self.supported_harness_ids),
            "supported_sandbox_backends": list(self.supported_sandbox_backends),
            "required_secret_refs": list(self.required_secret_refs),
            "supports_trajectory": self.supports_trajectory,
            "capabilities": list(self.capabilities),
            "metadata": dict(self.metadata),
        }


class AgentProvider(Protocol):
    def list_agents(self) -> list[AgentCatalogEntry]:
        """Return platform-selectable agent configs without exposing raw secrets."""


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_strings(name: str, values: list[str]) -> None:
    if isinstance(values, str) or not values:
        raise ValueError(f"{name} must be a non-empty list of strings")
    for value in values:
        _require_non_empty(name, value)


def _require_safe_secret_refs(values: list[str]) -> None:
    if not isinstance(values, list):
        raise ValueError("required_secret_refs must be a list")
    for value in values:
        _require_non_empty("required_secret_refs", value)
        if "=" in value or not value.startswith(("env:", "secret:", "vault:")):
            raise ValueError(
                "required_secret_refs must contain safe secret references, not raw secret values"
            )
