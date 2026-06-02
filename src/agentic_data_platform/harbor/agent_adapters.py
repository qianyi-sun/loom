from __future__ import annotations

import re
from dataclasses import dataclass

from agentic_data_platform.providers.config import ProviderConfigRef, ProviderSecret


_ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class HarborAgentModelAdapterSpec:
    adapter_id: str
    display_name: str
    agent_names: tuple[str, ...]
    api_key_env_names: tuple[str, ...]
    base_url_env_names: tuple[str, ...] = ()
    endpoint_dialects: tuple[str, ...] = ("openai_compatible",)
    model_family_hints: tuple[str, ...] = (
        "openai",
        "deepseek",
        "claude",
        "gemini",
        "qwen",
        "kimi",
        "glm",
        "grok",
        "minimax",
    )
    model_name_template: str = "{model_id}"
    base_url_surface: str = "openai"
    process_env: bool = True
    base_url_agent_kwarg: str | None = None
    default_agent_kwargs: tuple[str, ...] = ()
    mainstream: bool = True

    @property
    def required_secret_refs(self) -> list[str]:
        return [f"env:{name}" for name in self.api_key_env_names]

    def to_metadata(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "display_name": self.display_name,
            "endpoint_dialects": list(self.endpoint_dialects),
            "api_key_env_names": list(self.api_key_env_names),
            "base_url_env_names": list(self.base_url_env_names),
            "model_family_hints": list(self.model_family_hints),
            "model_name_template": self.model_name_template,
            "base_url_surface": self.base_url_surface,
            "process_env": self.process_env,
            "base_url_agent_kwarg": self.base_url_agent_kwarg,
            "default_agent_kwargs": list(self.default_agent_kwargs),
            "mainstream": self.mainstream,
        }


@dataclass(frozen=True)
class HarborAgentModelInvocation:
    adapter: HarborAgentModelAdapterSpec | None
    harbor_model_name: str
    agent_env: list[str]
    process_env: list[str]
    agent_kwargs: list[str]


_ADAPTER_SPECS = [
    HarborAgentModelAdapterSpec(
        adapter_id="codex-openai-compatible",
        display_name="Codex OpenAI Responses adapter",
        agent_names=("codex",),
        api_key_env_names=("OPENAI_API_KEY",),
        base_url_env_names=("OPENAI_BASE_URL",),
        endpoint_dialects=("openai_responses",),
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="opencode-openai-compatible",
        display_name="OpenCode OpenAI-compatible adapter",
        agent_names=("opencode",),
        api_key_env_names=("OPENAI_API_KEY",),
        base_url_env_names=("OPENAI_BASE_URL", "OPENAI_API_BASE"),
        model_name_template="openai/{model_id}",
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="aider-openai-compatible",
        display_name="Aider OpenAI-compatible adapter",
        agent_names=("aider",),
        api_key_env_names=("OPENAI_API_KEY",),
        base_url_env_names=("OPENAI_BASE_URL", "OPENAI_API_BASE"),
        model_name_template="openai/{model_id}",
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="openhands-openai-compatible",
        display_name="OpenHands OpenAI-compatible adapter",
        agent_names=("openhands",),
        api_key_env_names=("LLM_API_KEY", "OPENAI_API_KEY"),
        base_url_env_names=("LLM_BASE_URL", "OPENAI_BASE_URL", "OPENAI_API_BASE"),
        model_name_template="openai/{model_id}",
        default_agent_kwargs=("version=1.6.0",),
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="openhands-sdk-openai-compatible",
        display_name="OpenHands SDK OpenAI-compatible adapter",
        agent_names=("openhands-sdk",),
        api_key_env_names=("LLM_API_KEY", "OPENAI_API_KEY"),
        base_url_env_names=("LLM_BASE_URL", "OPENAI_BASE_URL", "OPENAI_API_BASE"),
        model_name_template="openai/{model_id}",
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="swe-agent-openai-compatible",
        display_name="SWE-agent OpenAI-compatible adapter",
        agent_names=("swe-agent",),
        api_key_env_names=("OPENAI_API_KEY",),
        base_url_env_names=("OPENAI_BASE_URL", "OPENAI_API_BASE"),
        model_name_template="openai/{model_id}",
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="mini-swe-agent-openai-compatible",
        display_name="Mini SWE-agent OpenAI-compatible adapter",
        agent_names=("mini-swe-agent",),
        api_key_env_names=("OPENAI_API_KEY", "MSWEA_API_KEY"),
        base_url_env_names=("OPENAI_API_BASE", "OPENAI_BASE_URL"),
        model_name_template="openai/{model_id}",
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="anthropic-cli",
        display_name="Anthropic CLI adapter",
        agent_names=("claude-code",),
        api_key_env_names=("ANTHROPIC_API_KEY",),
        base_url_env_names=("ANTHROPIC_BASE_URL",),
        endpoint_dialects=("anthropic",),
        base_url_surface="anthropic",
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="gemini-cli",
        display_name="Gemini CLI adapter",
        agent_names=("gemini-cli",),
        api_key_env_names=("GOOGLE_API_KEY", "GEMINI_API_KEY"),
        base_url_env_names=("GOOGLE_GEMINI_BASE_URL", "GEMINI_BASE_URL"),
        endpoint_dialects=("gemini",),
        model_name_template="google/{model_id}",
        base_url_surface="gemini",
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="qwen-cli",
        display_name="Qwen CLI adapter",
        agent_names=("qwen-coder",),
        api_key_env_names=("OPENAI_API_KEY", "QWEN_API_KEY", "DASHSCOPE_API_KEY"),
        base_url_env_names=("OPENAI_BASE_URL", "OPENAI_API_BASE", "DASHSCOPE_BASE_URL"),
        endpoint_dialects=("openai_compatible", "dashscope"),
    ),
    HarborAgentModelAdapterSpec(
        adapter_id="kimi-cli",
        display_name="Kimi CLI adapter",
        agent_names=("kimi-cli",),
        api_key_env_names=("OPENAI_API_KEY", "KIMI_API_KEY", "MOONSHOT_API_KEY"),
        base_url_env_names=("OPENAI_BASE_URL", "OPENAI_API_BASE", "MOONSHOT_BASE_URL"),
        endpoint_dialects=("openai_compatible", "moonshot"),
        model_name_template="openai/{model_id}",
        base_url_agent_kwarg="base_url",
    ),
]

_ADAPTERS_BY_AGENT = {
    agent_name: spec
    for spec in _ADAPTER_SPECS
    for agent_name in spec.agent_names
}


def mainstream_adapter_specs() -> dict[str, HarborAgentModelAdapterSpec]:
    return dict(_ADAPTERS_BY_AGENT)


def adapter_for_agent(agent_name: str | None) -> HarborAgentModelAdapterSpec | None:
    normalized = _normalize_agent_name(agent_name)
    if normalized in {"oracle", "nop"}:
        return None
    return _ADAPTERS_BY_AGENT.get(normalized)


def provider_dialect_gap(
    *,
    adapter: HarborAgentModelAdapterSpec | None,
    provider_ref: ProviderConfigRef,
) -> str | None:
    if adapter is None:
        return None
    provider_dialects = provider_endpoint_dialects(provider_ref)
    if any(dialect in provider_dialects for dialect in adapter.endpoint_dialects):
        return None
    return (
        f"{adapter.display_name} requires provider endpoint dialect "
        f"{', '.join(adapter.endpoint_dialects)}, but provider config "
        f"{provider_ref.config_id} exposes {', '.join(sorted(provider_dialects)) or 'unknown'}"
    )


def provider_endpoint_dialects(provider_ref: ProviderConfigRef) -> set[str]:
    configured = provider_ref.metadata.get("endpoint_dialects")
    if isinstance(configured, list):
        dialects = {str(item).strip() for item in configured if str(item).strip()}
        if dialects:
            return dialects

    normalized = f"{provider_ref.provider} {provider_ref.base_url}".lower()
    if "api.openai.com" in normalized:
        return {"openai_compatible", "openai_responses"}
    if "anthropic" in normalized or "claude" in normalized:
        return {"anthropic"}
    if "gemini" in normalized or "generativelanguage.googleapis.com" in normalized:
        return {"gemini"}
    if "dashscope" in normalized:
        return {"openai_compatible", "dashscope"}
    if "moonshot" in normalized or "kimi" in normalized:
        return {"openai_compatible", "moonshot"}
    return {"openai_compatible"}


def build_agent_model_env(
    *,
    agent_name: str | None,
    provider_ref: ProviderConfigRef,
    provider_secret: ProviderSecret,
    existing_agent_env: list[str],
    explicit_required_secret_refs: list[str] | None = None,
    model_id: str | None = None,
) -> list[str]:
    return build_agent_model_invocation(
        agent_name=agent_name,
        model_id=model_id or provider_ref.model_name,
        provider_ref=provider_ref,
        provider_secret=provider_secret,
        existing_agent_env=existing_agent_env,
        explicit_required_secret_refs=explicit_required_secret_refs,
    ).agent_env


def build_agent_model_invocation(
    *,
    agent_name: str | None,
    model_id: str,
    provider_ref: ProviderConfigRef,
    provider_secret: ProviderSecret,
    existing_agent_env: list[str],
    explicit_required_secret_refs: list[str] | None = None,
) -> HarborAgentModelInvocation:
    spec = adapter_for_agent(agent_name)
    api_key_env_names = list(spec.api_key_env_names) if spec else []
    base_url_env_names = list(spec.base_url_env_names) if spec else []
    base_url = _adapted_base_url(provider_ref.base_url, spec.base_url_surface if spec else "openai")

    if explicit_required_secret_refs:
        for secret_ref in explicit_required_secret_refs:
            env_name = env_name_from_secret_ref(secret_ref)
            if env_name not in api_key_env_names:
                api_key_env_names.append(env_name)
        if not base_url_env_names:
            base_url_env_names.extend(["OPENAI_BASE_URL", "OPENAI_API_BASE"])

    if not api_key_env_names:
        return HarborAgentModelInvocation(
            adapter=spec,
            harbor_model_name=model_id,
            agent_env=[],
            process_env=[],
            agent_kwargs=[],
        )

    existing_names = {item.split("=", 1)[0] for item in existing_agent_env if "=" in item}
    env_values: list[str] = []
    for env_name in api_key_env_names:
        if env_name in existing_names:
            continue
        env_values.append(f"{env_name}={provider_secret.value}")
        existing_names.add(env_name)

    if base_url:
        for env_name in base_url_env_names:
            if env_name in existing_names:
                continue
            env_values.append(f"{env_name}={base_url}")
            existing_names.add(env_name)

    process_env = list(env_values) if spec is None or spec.process_env else []
    agent_kwargs = list(spec.default_agent_kwargs) if spec is not None else []
    if spec is not None and spec.base_url_agent_kwarg and base_url:
        agent_kwargs.append(f"{spec.base_url_agent_kwarg}={base_url}")

    return HarborAgentModelInvocation(
        adapter=spec,
        harbor_model_name=_adapted_model_name(model_id, spec),
        agent_env=env_values,
        process_env=process_env,
        agent_kwargs=agent_kwargs,
    )


def env_name_from_secret_ref(secret_ref: str) -> str:
    if not isinstance(secret_ref, str) or not secret_ref.startswith("env:"):
        raise ValueError("harbor_run.agent_required_secret_refs must contain env:<VARIABLE_NAME> values")
    env_name = secret_ref.removeprefix("env:").strip()
    if not _ENV_NAME_PATTERN.match(env_name):
        raise ValueError("harbor_run.agent_required_secret_refs must contain env:<VARIABLE_NAME> values")
    return env_name


def _normalize_agent_name(agent_name: str | None) -> str:
    if not isinstance(agent_name, str):
        return ""
    value = agent_name.strip()
    if value.startswith("harbor:"):
        value = value.removeprefix("harbor:")
    return value


def _adapted_model_name(model_id: str, spec: HarborAgentModelAdapterSpec | None) -> str:
    if spec is None:
        return model_id
    return spec.model_name_template.format(model_id=model_id)


def _adapted_base_url(base_url: str, surface: str) -> str:
    normalized = base_url.rstrip("/")
    if not normalized:
        return ""
    if surface == "anthropic":
        return normalized.removesuffix("/v1")
    if surface == "gemini":
        return normalized.removesuffix("/v1").removesuffix("/v1beta") + "/v1beta"
    return normalized
