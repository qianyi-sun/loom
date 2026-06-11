"""Agent catalog — the union of built-in agents and registered
loom-launcher adapters that a service-mode trial may name.

The SPA fetches this catalog via `GET /api/v1/agents` so users pick
from a dropdown instead of typing a name into a text input. The same
list backs request-time validation in routes/trials.py and
routes/campaigns.py — a typo'd or hostile `agent_name` is rejected at
the API boundary rather than blowing up the worker mid-trial.

Adding a new builtin: append to `_BUILTIN`. Adding a new adapter: the
adapter ships in `loom-launcher` and is registered via
`register_adapter`; the catalog picks it up automatically.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

AgentKind = Literal["builtin", "adapter"]


@dataclass(frozen=True)
class AgentEntry:
    name: str
    needs_model: bool
    kind: AgentKind
    description: str

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "needs_model": self.needs_model,
            "kind": self.kind,
            "description": self.description,
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
            "card knows about."
        ),
    ),
    AgentEntry(
        name="claude-code-inbox",
        needs_model=True,
        kind="builtin",
        description=(
            "Claude Code running in-process inside the sandbox env "
            "container (v0.7-style in-box runtime)."
        ),
    ),
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
            ),
        )
    return entries


def known_names() -> frozenset[str]:
    """Set of valid `agent_name` values for fast membership check at
    the request boundary. Recomputed per call because adapter
    registration is import-time (idempotent + cheap)."""
    return frozenset(e.name for e in list_agents())
