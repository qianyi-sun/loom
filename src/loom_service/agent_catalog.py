"""Agent catalog — the union of built-in agents and registered
loom-launcher adapters that a service-mode trial may name.

The SPA fetches this catalog via `GET /api/v1/agents` so users pick
from a dropdown instead of typing a name into a text input. The same
list backs request-time validation in routes/trials.py and
routes/batches.py — a typo'd or hostile `agent_name` is rejected at
the API boundary rather than blowing up the worker mid-trial.

Each entry also declares what models it accepts:

- `supported_providers`: tuple of provider names (`"anthropic"`,
  `"openai"`, …) or `("*",)` for "any provider the LLM Gateway can
  route". CLI adapters lock to one provider; generic agents (litellm,
  aider, openhands) accept anything.
- `supported_model_sources`: subset of `{"api", "local-server", "hf"}`
  matching the `ModelSpec.source` discriminator. Empty tuple means the
  agent doesn't take a model at all (oracle).

Validation at submit time uses these to reject incompatible
(agent_name, agent_model) combos with a 400, rather than letting them
fail mid-trial on the worker. See validate_agent_model_compat below.

Adding a new builtin: append to `_BUILTIN`. Adding a new adapter: the
adapter ships in `loom-launcher` and is registered via
`register_adapter`; the catalog picks it up automatically (with a
generic "any provider, api+local-server+hf" support set — adapters
that should restrict can be overridden in `_ADAPTER_OVERRIDES`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from loom.models.types import ModelSpec

AgentKind = Literal["builtin", "adapter"]


@dataclass(frozen=True)
class AgentEntry:
    name: str
    needs_model: bool
    kind: AgentKind
    description: str
    supported_providers: tuple[str, ...] = ()
    supported_model_sources: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "needs_model": self.needs_model,
            "kind": self.kind,
            "description": self.description,
            "supported_providers": list(self.supported_providers),
            "supported_model_sources": list(self.supported_model_sources),
        }


# Built-in agents — the worker's `_default_agent_factory` knows these
# names natively (no loom-launcher round trip).
_BUILTIN: tuple[AgentEntry, ...] = (
    AgentEntry(
        name="oracle",
        needs_model=False,
        kind="builtin",
        description=(
            "Runs the task's solution/solve.sh script as ground truth. "
            "Use for canary trials and smoke tests; no LLM call."
        ),
    ),
    AgentEntry(
        name="litellm",
        needs_model=True,
        kind="builtin",
        description=(
            "Multi-provider tool-loop agent. Routes through the LLM "
            "Gateway via LiteLLM — pick any provider+model the rate "
            "card knows about, or a HuggingFace / local-server model."
        ),
        supported_providers=("*",),
        supported_model_sources=("api", "local-server", "hf"),
    ),
    AgentEntry(
        name="claude-code-inbox",
        needs_model=True,
        kind="builtin",
        description=(
            "Claude Code running in-process inside the sandbox env "
            "container (v0.7-style in-box runtime)."
        ),
        supported_providers=("anthropic",),
        supported_model_sources=("api",),
    ),
)


# Per-adapter overrides for the auto-discovered supported sets. Adapters
# that wrap a CLI bound to one provider get listed here so the SPA
# dropdown only offers compatible models. Adapters NOT in this map fall
# back to the permissive ("*", api+local-server+hf) defaults below.
_ADAPTER_OVERRIDES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "claude-code": (("anthropic",), ("api",)),
    "codex": (("openai",), ("api",)),
    "gemini-cli": (("google",), ("api",)),
    "kimi-cli": (("moonshot",), ("api",)),
    "qwen-cli": (("alibaba",), ("api", "local-server", "hf")),
    # Generic agents — keep open to any provider + any source.
    "aider": (("*",), ("api", "local-server", "hf")),
    "openhands": (("*",), ("api", "local-server", "hf")),
    "openhands-sdk": (("*",), ("api", "local-server", "hf")),
    "opencode": (("*",), ("api", "local-server", "hf")),
    "swe-agent": (("*",), ("api", "local-server", "hf")),
    "mini-swe-agent": (("*",), ("api", "local-server", "hf")),
    # "hello" is a canary that doesn't actually need an LLM in practice,
    # but adapters self-declare needs_model — keep permissive.
    "hello": (("*",), ("api", "local-server", "hf")),
}
_DEFAULT_ADAPTER_SUPPORT: tuple[tuple[str, ...], tuple[str, ...]] = (
    ("*",), ("api", "local-server", "hf"),
)


def list_agents() -> list[AgentEntry]:
    """Return the catalog: builtins + every registered launcher adapter.

    Adapters are loaded lazily so a deployment that doesn't ship
    `loom-launcher` (rare, but possible) still gets the builtins.
    """
    entries: list[AgentEntry] = list(_BUILTIN)
    try:
        # Importing `loom_launcher` runs its adapters package, which
        # self-registers every shipped adapter into the registry.
        import loom_launcher  # noqa: F401
        from loom_launcher.registry import all_adapters
    except ImportError:
        return entries
    for adapter in all_adapters():
        providers, sources = _ADAPTER_OVERRIDES.get(
            adapter.name, _DEFAULT_ADAPTER_SUPPORT,
        )
        entries.append(
            AgentEntry(
                name=adapter.name,
                needs_model=True,
                kind="adapter",
                description=(
                    f"loom-launcher adapter (dialect "
                    f"{getattr(adapter, 'endpoint_dialect', 'unknown')}). "
                    "Drives the agent's CLI inside the sandbox."
                ),
                supported_providers=providers,
                supported_model_sources=sources,
            ),
        )
    return entries


def known_names() -> frozenset[str]:
    """Set of valid `agent_name` values for fast membership check at
    the request boundary. Recomputed per call because adapter
    registration is import-time (idempotent + cheap)."""
    return frozenset(e.name for e in list_agents())


def get_agent(name: str) -> AgentEntry | None:
    """Look up an entry by name; returns None for unknown agents."""
    for e in list_agents():
        if e.name == name:
            return e
    return None


def validate_agent_model_compat(
    agent_name: str, model: ModelSpec | None,
) -> str | None:
    """Check that `(agent, model)` is a runnable combo.

    Returns None on success, or a human-readable error string on
    rejection. Callers convert non-None returns into a 400 HTTPException.
    """
    agent = get_agent(agent_name)
    if agent is None:
        return f"unknown agent_name {agent_name!r}"

    if agent.needs_model and model is None:
        return (
            f"agent {agent_name!r} requires a model — got null"
        )
    if not agent.needs_model and model is not None:
        return (
            f"agent {agent_name!r} does not take a model — got "
            f"{model.provider}/{model.name}"
        )
    if model is None:
        return None

    if "*" not in agent.supported_providers:
        if model.provider not in agent.supported_providers:
            return (
                f"agent {agent_name!r} supports providers "
                f"{list(agent.supported_providers)}, got "
                f"{model.provider!r}"
            )

    if model.source not in agent.supported_model_sources:
        return (
            f"agent {agent_name!r} supports sources "
            f"{list(agent.supported_model_sources)}, got "
            f"{model.source!r}"
        )

    # source-specific structural checks: callers shouldn't send a
    # local_server when source isn't local-server, and shouldn't omit
    # it when it is. Same idea for hf_execution.
    if model.source == "local-server" and not model.local_server:
        return (
            "model.source='local-server' requires model.local_server "
            "to name an operator-configured server"
        )
    if model.source != "local-server" and model.local_server is not None:
        return (
            f"model.local_server set but source is {model.source!r} "
            "(not 'local-server')"
        )

    return None


__all__ = [
    "AgentEntry",
    "AgentKind",
    "get_agent",
    "known_names",
    "list_agents",
    "validate_agent_model_compat",
]
