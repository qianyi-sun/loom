from __future__ import annotations

import re
from dataclasses import dataclass, field

from agentic_data_platform.agents.providers import AgentCatalogEntry
from agentic_data_platform.harbor.capabilities import probe_harbor_native_capabilities


HARBOR_AGENT_CAPABILITIES = ["terminal-agent", "harbor-run", "harbor-trial-events"]
HARBOR_HARNESS_IDS = ["harbor-local-docker"]
HARBOR_SANDBOX_BACKENDS = ["docker_terminal"]


@dataclass(frozen=True)
class HarborBuiltInAgentSpec:
    name: str
    display_name: str
    execution_mode: str
    required_secret_refs: list[str] = field(default_factory=list)


HARBOR_BUILTIN_AGENT_SPECS = [
    HarborBuiltInAgentSpec("oracle", "Oracle", "harbor_builtin"),
    HarborBuiltInAgentSpec("nop", "No-op", "harbor_builtin"),
    HarborBuiltInAgentSpec("claude-code", "Claude Code", "external_cli", ["env:ANTHROPIC_API_KEY"]),
    HarborBuiltInAgentSpec("cline-cli", "Cline CLI", "external_cli"),
    HarborBuiltInAgentSpec("terminus", "Terminus", "external_cli"),
    HarborBuiltInAgentSpec("terminus-1", "Terminus 1", "external_cli"),
    HarborBuiltInAgentSpec("terminus-2", "Terminus 2", "external_cli"),
    HarborBuiltInAgentSpec("aider", "Aider", "external_cli"),
    HarborBuiltInAgentSpec("codex", "Codex", "external_cli", ["env:OPENAI_API_KEY"]),
    HarborBuiltInAgentSpec("cursor-cli", "Cursor CLI", "external_cli"),
    HarborBuiltInAgentSpec("gemini-cli", "Gemini CLI", "external_cli", ["env:GOOGLE_API_KEY"]),
    HarborBuiltInAgentSpec("rovodev-cli", "Rovo Dev CLI", "external_cli"),
    HarborBuiltInAgentSpec("goose", "Goose", "external_cli"),
    HarborBuiltInAgentSpec("hermes", "Hermes", "external_cli"),
    HarborBuiltInAgentSpec("mini-swe-agent", "Mini SWE Agent", "external_cli"),
    HarborBuiltInAgentSpec("nemo-agent", "Nemo Agent", "external_cli"),
    HarborBuiltInAgentSpec("swe-agent", "SWE Agent", "external_cli"),
    HarborBuiltInAgentSpec("opencode", "OpenCode", "external_cli"),
    HarborBuiltInAgentSpec("openhands", "OpenHands", "external_cli"),
    HarborBuiltInAgentSpec("openhands-sdk", "OpenHands SDK", "external_cli"),
    HarborBuiltInAgentSpec("kimi-cli", "Kimi CLI", "external_cli", ["env:KIMI_API_KEY"]),
    HarborBuiltInAgentSpec("pi", "Pi", "external_cli"),
    HarborBuiltInAgentSpec("qwen-coder", "Qwen Coder", "external_cli", ["env:QWEN_API_KEY"]),
    HarborBuiltInAgentSpec("copilot-cli", "Copilot CLI", "external_cli", ["env:GITHUB_TOKEN"]),
    HarborBuiltInAgentSpec("devin", "Devin", "external_cli", ["env:DEVIN_API_KEY"]),
    HarborBuiltInAgentSpec("trae-agent", "Trae Agent", "external_cli", ["env:TRAE_AGENT_API_KEY"]),
]


class HarborAgentProvider:
    def __init__(self, built_in_specs: list[HarborBuiltInAgentSpec] | None = None) -> None:
        self.built_in_specs = built_in_specs or list(HARBOR_BUILTIN_AGENT_SPECS)
        self.native_capabilities = probe_harbor_native_capabilities()

    def list_agents(self) -> list[AgentCatalogEntry]:
        return [self._entry_for_builtin(spec) for spec in self.built_in_specs]

    def agent_for_import_path(
        self,
        agent_import_path: str,
        *,
        display_name: str | None = None,
        required_secret_refs: list[str] | None = None,
    ) -> AgentCatalogEntry:
        module, _, symbol = _validate_agent_import_path(agent_import_path)
        return AgentCatalogEntry(
            agent_id=f"harbor-custom:{agent_import_path}",
            display_name=display_name or symbol,
            provider="harbor",
            source="harbor_custom_import",
            runner_kind="harbor",
            execution_mode="custom_import",
            supported_harness_ids=list(HARBOR_HARNESS_IDS),
            supported_sandbox_backends=list(HARBOR_SANDBOX_BACKENDS),
            required_secret_refs=list(required_secret_refs or []),
            supports_trajectory=True,
            capabilities=list(HARBOR_AGENT_CAPABILITIES),
            metadata={
                "provider": "harbor",
                "harbor_agent_import_path": agent_import_path,
                "harbor_import_module": module,
                "harbor_import_symbol": symbol,
                "harbor_cli_args": ["--agent-import-path", agent_import_path],
                "backend_modes": ["cli", "native"],
                "native_runner_available": self.native_capabilities.native_runner_available,
                "harbor_package_version": self.native_capabilities.package_version,
            },
        )

    def resolve_agent(
        self,
        *,
        agent_id: str | None = None,
        agent_import_path: str | None = None,
    ) -> AgentCatalogEntry:
        if agent_import_path:
            return self.agent_for_import_path(agent_import_path)

        normalized_id = _normalize_agent_id(agent_id)
        for agent in self.list_agents():
            if agent.agent_id == normalized_id:
                return agent
        raise ValueError(f"Unknown Harbor agent: {agent_id}")

    def _entry_for_builtin(self, spec: HarborBuiltInAgentSpec) -> AgentCatalogEntry:
        return AgentCatalogEntry(
            agent_id=f"harbor:{spec.name}",
            display_name=spec.display_name,
            provider="harbor",
            source="harbor_builtin",
            runner_kind="harbor",
            execution_mode=spec.execution_mode,
            supported_harness_ids=list(HARBOR_HARNESS_IDS),
            supported_sandbox_backends=list(HARBOR_SANDBOX_BACKENDS),
            required_secret_refs=list(spec.required_secret_refs),
            supports_trajectory=True,
            capabilities=list(HARBOR_AGENT_CAPABILITIES),
            metadata={
                "provider": "harbor",
                "harbor_agent_name": spec.name,
                "harbor_cli_args": ["--agent", spec.name],
                "backend_modes": ["cli", "native"],
                "native_runner_available": self.native_capabilities.native_runner_available,
                "harbor_package_version": self.native_capabilities.package_version,
            },
        )


def _normalize_agent_id(agent_id: str | None) -> str:
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise ValueError("agent_id must be a non-empty string")
    value = agent_id.strip()
    return value if value.startswith("harbor:") else f"harbor:{value}"


def _validate_agent_import_path(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise ValueError("agent_import_path must use module.path:ClassName format")
    module, separator, symbol = value.strip().partition(":")
    if separator != ":" or not _PYTHON_DOTTED_NAME.match(module) or not _PYTHON_DOTTED_NAME.match(symbol):
        raise ValueError("agent_import_path must use module.path:ClassName format")
    return module, separator, symbol


_PYTHON_DOTTED_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
