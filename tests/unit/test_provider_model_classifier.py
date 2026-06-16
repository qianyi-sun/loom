"""BYO provider model classifier.

Issue #4: noisy OpenAI-compatible `/models` responses can include
tool/API integrations alongside actual chat/LLM model ids. The launch
selector should default to agent-capable models while raw/debug views
explain why the rest were suppressed.
"""

from __future__ import annotations

from loom_service.provider_model_classifier import classify_model_id


def test_classifier_marks_mainstream_llm_ids_agent_capable() -> None:
    for model_id in [
        "deepseek-chat",
        "deepseek-reasoner",
        "gpt-4o",
        "claude-opus-4-7",
        "gemini-2.5-pro",
        "Qwen/Qwen2.5-Coder-32B-Instruct",
        "meta-llama/Llama-3.1-70B-Instruct",
        "mistral-large-latest",
        "moonshot-v1-128k",
    ]:
        c = classify_model_id(model_id)
        assert c.agent_capable is True, model_id
        assert c.recommended is True, model_id
        assert c.visibility == "default", model_id
        assert c.reason is None, model_id


def test_classifier_suppresses_known_tool_api_integrations() -> None:
    for model_id in [
        "amap-coordinate-convert",
        "apisports-afl-games",
        "tushare-stock-basic",
        "google-maps-geocode",
    ]:
        c = classify_model_id(model_id)
        assert c.agent_capable is False, model_id
        assert c.recommended is False, model_id
        assert c.visibility == "advanced", model_id
        assert c.reason == "classifier-non-llm", model_id


def test_classifier_keeps_manual_unknown_ids_selectable() -> None:
    c = classify_model_id("my-lab-checkpoint-20260616", source="manual")
    assert c.agent_capable is True
    assert c.recommended is True
    assert c.visibility == "default"
    assert c.reason is None
