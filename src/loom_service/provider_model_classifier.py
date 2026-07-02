"""Classify provider-discovered model ids for launch selectors.

BYO OpenAI-compatible endpoints can expose a noisy `/models` list:
actual chat/LLM ids mixed with tool/API integrations. This module is
deliberately deterministic and dependency-free so routes, CLI, and tests
can share the same default-vs-raw decision.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelClassification:
    agent_capable: bool
    recommended: bool
    visibility: str
    reason: str | None
    family: str | None = None


_NON_LLM_MARKERS = (
    "amap-",
    "apisports-",
    "tushare",
    "google-maps",
    "mapbox",
    "weatherapi",
    "stripe-",
    "github-",
    "slack-",
    "notion-",
)

_LLM_FAMILY_MARKERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("openai", ("gpt-", "o1", "o3", "o4")),
    ("anthropic", ("claude",)),
    ("google", ("gemini", "palm")),
    ("deepseek", ("deepseek",)),
    ("zhipu", ("glm", "zhipu")),
    ("qwen", ("qwen",)),
    ("llama", ("llama", "meta-llama")),
    ("mistral", ("mistral", "mixtral", "codestral")),
    ("moonshot", ("moonshot", "kimi")),
    ("cohere", ("command-r", "command-")),
    ("xai", ("grok",)),
    ("yi", ("yi-", "01-ai")),
    ("phi", ("phi-", "phi3", "phi4")),
    ("starcoder", ("starcoder",)),
    ("granite", ("granite",)),
)

_AGENT_CAPABLE_FAMILIES = {
    "chat",
    "llm",
    "language-model",
    "text-generation",
    "code",
}


def _family_from_model_id(model_id: str) -> str | None:
    normalized = model_id.casefold()
    for family, markers in _LLM_FAMILY_MARKERS:
        if any(marker in normalized for marker in markers):
            return family
    return None


def classify_model_id(
    model_id: str,
    *,
    source: str = "discovered",
    family: str | None = None,
) -> ModelClassification:
    """Return selector metadata for one provider model id."""

    normalized = model_id.casefold()
    if source == "manual":
        return ModelClassification(
            agent_capable=True,
            recommended=True,
            visibility="default",
            reason=None,
            family=family or _family_from_model_id(model_id),
        )

    if any(marker in normalized for marker in _NON_LLM_MARKERS):
        return ModelClassification(
            agent_capable=False,
            recommended=False,
            visibility="advanced",
            reason="classifier-non-llm",
            family=family,
        )

    if family is not None and family.casefold() in _AGENT_CAPABLE_FAMILIES:
        return ModelClassification(
            agent_capable=True,
            recommended=True,
            visibility="default",
            reason=None,
            family=family,
        )

    inferred_family = _family_from_model_id(model_id)
    if inferred_family is not None:
        return ModelClassification(
            agent_capable=True,
            recommended=True,
            visibility="default",
            reason=None,
            family=family or inferred_family,
        )

    return ModelClassification(
        agent_capable=False,
        recommended=False,
        visibility="advanced",
        reason="classifier-unknown",
        family=family,
    )
