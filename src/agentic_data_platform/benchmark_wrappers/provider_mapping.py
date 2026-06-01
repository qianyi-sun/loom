from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from agentic_data_platform.providers.config import validate_secret_ref


@dataclass(frozen=True)
class UpstreamProviderMapping:
    provider_family: str
    provider: str
    model_name: str
    secret_ref: str | None
    api_key_env_var: str | None
    base_url_env_var: str | None
    base_url: str | None
    skillflow_import_path: str | None
    skillflow_model_name: str | None
    skilllearnbench_agent: str | None
    skilllearnbench_model_name: str | None

    @property
    def requires_secret(self) -> bool:
        return self.api_key_env_var is not None

    def to_safe_dict(self) -> dict[str, str]:
        payload = {
            "provider_family": self.provider_family,
            "provider": self.provider,
            "model_name": self.model_name,
        }
        if self.secret_ref is not None:
            payload["secret_ref"] = self.secret_ref
        if self.api_key_env_var is not None:
            payload["upstream_env_var"] = self.api_key_env_var
        if self.base_url_env_var is not None:
            payload["upstream_base_url_env_var"] = self.base_url_env_var
        if self.skillflow_import_path is not None:
            payload["skillflow_import_path"] = self.skillflow_import_path
        if self.skillflow_model_name is not None:
            payload["skillflow_model_name"] = self.skillflow_model_name
        if self.skilllearnbench_agent is not None:
            payload["upstream_agent"] = self.skilllearnbench_agent
        if self.skilllearnbench_model_name is not None:
            payload["upstream_model_name"] = self.skilllearnbench_model_name
        return payload


class WrapperConfigurationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def mapping_for_suite(*, suite_name: str, model: Mapping[str, object]) -> UpstreamProviderMapping:
    provider = _string(model.get("provider")) or "unknown"
    model_name = _string(model.get("model_name"))
    if model_name is None:
        raise WrapperConfigurationError(
            "missing_model_name",
            f"{suite_name} provider mapping requires model.model_name",
        )

    family = _provider_family(model=model, provider=provider, model_name=model_name)
    secret_ref = _string(model.get("secret_ref"))
    base_url = _string(model.get("base_url"))

    if family == "mock":
        return UpstreamProviderMapping(
            provider_family=family,
            provider=provider,
            model_name=model_name,
            secret_ref=secret_ref,
            api_key_env_var=None,
            base_url_env_var=None,
            base_url=base_url,
            skillflow_import_path="libs.harbor_noinstall_agents.agents:NoInstallClaudeCode",
            skillflow_model_name=model_name,
            skilllearnbench_agent="claude-code",
            skilllearnbench_model_name=model_name,
        )

    if secret_ref is None:
        raise WrapperConfigurationError(
            "missing_secret_ref",
            f"{suite_name} provider mapping for provider '{provider}' requires model.secret_ref like env:MODEL_PROVIDER_API_KEY",
        )
    validate_secret_ref(secret_ref)

    if family == "anthropic":
        return UpstreamProviderMapping(
            provider_family=family,
            provider=provider,
            model_name=model_name,
            secret_ref=secret_ref,
            api_key_env_var="ANTHROPIC_API_KEY",
            base_url_env_var="ANTHROPIC_BASE_URL" if base_url else None,
            base_url=base_url,
            skillflow_import_path="libs.harbor_noinstall_agents.agents:NoInstallClaudeCode",
            skillflow_model_name=_prefixed_model_name("anthropic", model_name),
            skilllearnbench_agent="claude-code",
            skilllearnbench_model_name=_strip_provider_prefix(model_name),
        )

    if family == "openai":
        return UpstreamProviderMapping(
            provider_family=family,
            provider=provider,
            model_name=model_name,
            secret_ref=secret_ref,
            api_key_env_var="OPENAI_API_KEY",
            base_url_env_var="OPENAI_BASE_URL" if base_url else None,
            base_url=base_url,
            skillflow_import_path="libs.harbor_noinstall_agents.agents:NoInstallCodex",
            skillflow_model_name=_strip_provider_prefix(model_name),
            skilllearnbench_agent="codex",
            skilllearnbench_model_name=_strip_provider_prefix(model_name),
        )

    if family == "gemini":
        if suite_name == "SkillFlow":
            raise WrapperConfigurationError(
                "unsupported_provider_mapping",
                "SkillFlow has no durable no-install Gemini agent mapping yet; use Anthropic/Claude or OpenAI/Codex, or add a SkillFlow Gemini agent adapter first",
            )
        return UpstreamProviderMapping(
            provider_family=family,
            provider=provider,
            model_name=model_name,
            secret_ref=secret_ref,
            api_key_env_var="GEMINI_API_KEY",
            base_url_env_var=None,
            base_url=base_url,
            skillflow_import_path=None,
            skillflow_model_name=None,
            skilllearnbench_agent="gemini-code",
            skilllearnbench_model_name=_strip_provider_prefix(model_name),
        )

    raise WrapperConfigurationError(
        "unsupported_provider_mapping",
        (
            f"{suite_name} does not know how to map provider '{provider}' and model '{model_name}' "
            "to an upstream agent/env contract. Supported real mappings are Anthropic/Claude, "
            "OpenAI/Codex, and SkillLearnBench Gemini."
        ),
    )


def runtime_environment_for_mapping(
    mapping: UpstreamProviderMapping,
    *,
    environ: Mapping[str, str] | None = None,
    require_secret: bool,
) -> tuple[dict[str, str], list[str]]:
    env: dict[str, str] = {}
    secrets: list[str] = []
    if mapping.api_key_env_var is not None:
        if mapping.secret_ref is None:
            raise WrapperConfigurationError(
                "missing_secret_ref",
                f"provider mapping for '{mapping.provider}' requires a secret_ref",
            )
        source_env_var = mapping.secret_ref.removeprefix("env:")
        source = os.environ if environ is None else environ
        secret_value = source.get(source_env_var, "")
        if not secret_value.strip():
            if require_secret:
                raise WrapperConfigurationError(
                    "missing_provider_secret",
                    (
                        f"provider mapping for '{mapping.provider}' expected {mapping.secret_ref} "
                        f"so it can set {mapping.api_key_env_var} for the upstream runner"
                    ),
                )
        else:
            env[mapping.api_key_env_var] = secret_value
            secrets.append(secret_value)

    if mapping.base_url_env_var is not None and mapping.base_url:
        env[mapping.base_url_env_var] = mapping.base_url
    return env, secrets


def _provider_family(*, model: Mapping[str, object], provider: str, model_name: str) -> str:
    explicit = _string(model.get("provider_family")) or _string(model.get("upstream_provider"))
    if explicit is not None:
        normalized = explicit.strip().lower().replace("_", "-")
        if normalized in {"anthropic", "claude"}:
            return "anthropic"
        if normalized in {"openai", "openai-compatible", "codex"}:
            return "openai"
        if normalized in {"google", "gemini"}:
            return "gemini"
        if normalized in {"mock", "mock-api", "smoke"}:
            return "mock"

    provider_lower = provider.strip().lower()
    model_lower = model_name.strip().lower()
    combined = f"{provider_lower} {model_lower}"
    if "mock" in provider_lower or provider_lower.startswith("smoke"):
        return "mock"
    if "anthropic" in combined or model_lower.startswith("claude") or model_lower.startswith("anthropic/claude"):
        return "anthropic"
    if (
        "openai" in combined
        or "codex" in combined
        or model_lower.startswith("gpt")
        or model_lower.startswith("o1")
        or model_lower.startswith("o3")
        or model_lower.startswith("o4")
        or model_lower.startswith("openai/")
    ):
        return "openai"
    if "gemini" in combined or "google" in provider_lower or model_lower.startswith("google/"):
        return "gemini"
    return "unsupported"


def _prefixed_model_name(provider_prefix: str, model_name: str) -> str:
    stripped = model_name.strip()
    if "/" in stripped:
        return stripped
    return f"{provider_prefix}/{stripped}"


def _strip_provider_prefix(model_name: str) -> str:
    stripped = model_name.strip()
    if "/" not in stripped:
        return stripped
    return stripped.split("/", 1)[1]


def _string(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
