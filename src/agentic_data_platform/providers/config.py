from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from agentic_data_platform.service.config import ServiceSettings


REDACTED = "[redacted]"
_SAFE_REFERENCE_KEYS = {"agent_required_secret_refs", "provider_config_id", "secret_ref"}
_SENSITIVE_MARKERS = ("api_key", "apikey", "access_token", "authorization", "credential", "password", "secret", "token")


class ProviderRole(str, Enum):
    AGENT_MODEL = "agent_model"
    EVALUATOR_MODEL = "evaluator_model"


@dataclass(frozen=True)
class ProviderSecret:
    value: str

    def __post_init__(self) -> None:
        _require_non_empty("value", self.value)

    @property
    def redacted(self) -> str:
        return "********"


@dataclass(frozen=True)
class ProviderConfigRef:
    config_id: str
    role: ProviderRole | str
    provider: str
    model_name: str
    secret_ref: str
    base_url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("config_id", self.config_id)
        _require_non_empty("provider", self.provider)
        _require_non_empty("model_name", self.model_name)
        validate_secret_ref(self.secret_ref)
        object.__setattr__(self, "role", ProviderRole(self.role))
        object.__setattr__(self, "metadata", redact_sensitive_metadata(self.metadata))

    def to_safe_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "config_id": self.config_id,
            "role": self.role.value,
            "provider": self.provider,
            "model_name": self.model_name,
            "secret_ref": self.secret_ref,
            "metadata": dict(self.metadata),
        }
        if self.base_url:
            payload["base_url"] = self.base_url
        return payload


class DevProviderConfigRegistry:
    def __init__(
        self,
        *,
        refs: list[ProviderConfigRef],
        secrets: Mapping[str, str],
    ) -> None:
        self._refs = {ref.config_id: ref for ref in refs}
        self._secrets = dict(secrets)

    @classmethod
    def from_settings(cls, settings: ServiceSettings) -> DevProviderConfigRegistry:
        refs: list[ProviderConfigRef] = []
        secrets: dict[str, str] = {}
        if settings.model_provider_api_key:
            secret_ref = "env:MODEL_PROVIDER_API_KEY"
            refs.append(
                ProviderConfigRef(
                    config_id="default-agent-model",
                    role=ProviderRole.AGENT_MODEL,
                    provider="dev-api-provider",
                    model_name="configured-agent-model",
                    base_url=settings.model_provider_base_url,
                    secret_ref=secret_ref,
                )
            )
            secrets[secret_ref] = settings.model_provider_api_key
        if settings.evaluator_provider_api_key:
            secret_ref = "env:EVALUATOR_PROVIDER_API_KEY"
            refs.append(
                ProviderConfigRef(
                    config_id="default-evaluator-model",
                    role=ProviderRole.EVALUATOR_MODEL,
                    provider="dev-evaluator-provider",
                    model_name="configured-evaluator-model",
                    base_url=settings.evaluator_provider_base_url,
                    secret_ref=secret_ref,
                )
            )
            secrets[secret_ref] = settings.evaluator_provider_api_key
        return cls(refs=refs, secrets=secrets)

    def get(self, config_id: str) -> ProviderConfigRef:
        try:
            return self._refs[config_id]
        except KeyError as exc:
            raise KeyError(f"Unknown provider config: {config_id}") from exc

    def list_refs(self, *, role: ProviderRole | str | None = None) -> list[ProviderConfigRef]:
        refs = list(self._refs.values())
        if role is None:
            return refs
        expected = ProviderRole(role)
        return [ref for ref in refs if ref.role == expected]

    def resolve_secret(self, secret_ref: str) -> ProviderSecret:
        validate_secret_ref(secret_ref)
        try:
            return ProviderSecret(self._secrets[secret_ref])
        except KeyError as exc:
            raise KeyError(f"Unknown provider secret reference: {secret_ref}") from exc


def validate_secret_ref(secret_ref: str) -> None:
    _require_non_empty("secret_ref", secret_ref)
    if not secret_ref.startswith("env:"):
        raise ValueError("secret_ref must use env:<VARIABLE_NAME>")
    _require_non_empty("secret_ref variable", secret_ref.removeprefix("env:"))


def redact_sensitive_metadata(value: Any, *, key: str | None = None) -> Any:
    if key is not None and _is_sensitive_key(key):
        return REDACTED
    if isinstance(value, dict):
        return {
            item_key: redact_sensitive_metadata(item_value, key=item_key)
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive_metadata(item) for item in value]
    return value


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    if normalized in _SAFE_REFERENCE_KEYS:
        return False
    return any(marker in normalized for marker in _SENSITIVE_MARKERS)


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
