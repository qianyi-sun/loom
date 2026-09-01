"""Terminus-style prompt helpers for the OpenHands SDK runner."""

from __future__ import annotations

import importlib
from typing import Any

TERMINUS_STYLE_SYSTEM_SUFFIX = """<REASONING_FORMAT>
Before each batch of tool calls:

1. reasoning_content (when supported): put detailed internal scratchpad reasoning here.
2. message content: write exactly:
   Analysis: <what you observe from prior results and current state>
   Plan: <specific next actions and why>
3. Then issue the tool calls needed to execute the plan.

Do not use a separate think/logging tool. Keep Analysis and Plan concise but complete.
</REASONING_FORMAT>"""


def build_terminus_style_agent_kwargs() -> dict[str, Any]:
    """Return Agent kwargs for Terminus-style reasoning (no ThinkTool)."""
    agent_context_module = importlib.import_module("openhands.sdk.context.agent_context")
    agent_context = agent_context_module.AgentContext(
        system_message_suffix=TERMINUS_STYLE_SYSTEM_SUFFIX,
    )
    return {
        "include_default_tools": ["FinishTool"],
        "agent_context": agent_context,
    }


def terminus_style_argv_suffix(env: dict[str, str]) -> list[str]:
    """Return runner argv suffix when batch env enables Terminus-style mode."""
    value = env.get("LOOM_OPENHANDS_TERMINUS_STYLE", "").strip().lower()
    if value in {"1", "true", "yes"}:
        return ["--terminus-style"]
    return []
