"""Parse/format Terminus-2 agent reasoning fields from Harbor ATIF messages."""

from __future__ import annotations


def format_agent_message(*, analysis: str, plan: str) -> str:
    """Render Harbor-style agent message text from structured reasoning fields."""
    parts: list[str] = []
    if analysis:
        parts.append(f"Analysis: {analysis}")
    if plan:
        parts.append(f"Plan: {plan}")
    return "\n".join(parts)


def parse_agent_message(message: str) -> tuple[str, str]:
    """Extract analysis/plan from Harbor ``message`` text."""
    text = message.strip()
    if not text:
        return "", ""
    if not text.startswith("Analysis:"):
        return "", ""
    if "\nPlan:" in text:
        analysis_part, plan_part = text.split("\nPlan:", 1)
        return (
            analysis_part.removeprefix("Analysis:").strip(),
            plan_part.strip(),
        )
    return text.removeprefix("Analysis:").strip(), ""
