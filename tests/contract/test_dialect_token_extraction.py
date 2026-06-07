"""Dialect adapter token extraction (Plan 9 Task 5).

Canned responses use the actual provider response shapes — Anthropic
Messages, OpenAI Responses, Gemini generateContent — so the adapters
are exercised against real-world keys, not invented ones.
"""

import pytest

from loom_llm_gateway.dialect import DIALECTS


@pytest.mark.parametrize(
    "dialect, response, expected_in, expected_out, expected_extras",
    [
        (
            "openai_chat",
            {"usage": {"prompt_tokens": 10, "completion_tokens": 20}},
            10, 20, {},
        ),
        (
            "openai_responses",
            {
                "usage": {
                    "input_tokens": 15,
                    "output_tokens": 25,
                    "output_tokens_details": {"reasoning_tokens": 7},
                },
            },
            15, 25, {"reasoning_tokens": 7},
        ),
        (
            "anthropic",
            {
                "usage": {
                    "input_tokens": 30,
                    "output_tokens": 40,
                    "cache_creation_input_tokens": 8,
                    "cache_read_input_tokens": 16,
                },
            },
            30, 40,
            {"cache_creation_input_tokens": 8, "cache_read_input_tokens": 16},
        ),
        (
            "gemini",
            {
                "usageMetadata": {
                    "promptTokenCount": 11,
                    "candidatesTokenCount": 22,
                    "cachedContentTokenCount": 5,
                    "thoughtsTokenCount": 9,
                },
            },
            11, 22,
            {"cachedContentTokenCount": 5, "thoughtsTokenCount": 9},
        ),
    ],
)
def test_extract_tokens(
    dialect: str,
    response: dict,
    expected_in: int,
    expected_out: int,
    expected_extras: dict,
) -> None:
    adapter = DIALECTS[dialect]
    usage = adapter.extract_tokens(response)
    assert usage.input_tokens == expected_in
    assert usage.output_tokens == expected_out
    assert usage.provider_extras == expected_extras


def test_extract_tokens_missing_usage_returns_zeros() -> None:
    """A response with no usage block (e.g., streamed response without
    final summary) yields (0, 0, {}) — never raises."""
    for dialect in DIALECTS.values():
        usage = dialect.extract_tokens({})
        assert usage.input_tokens == 0
        assert usage.output_tokens == 0


def test_cached_and_cache_write_helpers() -> None:
    """The convenience properties sum the dialect-specific counters into
    the categories `compute_cost_usd` needs."""
    anthropic = DIALECTS["anthropic"]
    usage = anthropic.extract_tokens({
        "usage": {
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_creation_input_tokens": 30,
            "cache_read_input_tokens": 70,
        },
    })
    assert usage.cached_input_tokens == 70
    assert usage.cache_write_tokens == 30

    gemini = DIALECTS["gemini"]
    g_usage = gemini.extract_tokens({
        "usageMetadata": {
            "promptTokenCount": 100,
            "candidatesTokenCount": 50,
            "cachedContentTokenCount": 40,
        },
    })
    assert g_usage.cached_input_tokens == 40
    assert g_usage.cache_write_tokens == 0
