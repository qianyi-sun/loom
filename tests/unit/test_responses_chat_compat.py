from __future__ import annotations

import httpx

from loom_llm_gateway.routes.responses_chat_compat import (
    should_fallback_to_chat_completions,
)


def test_detects_chat_only_missing_messages_error() -> None:
    response = httpx.Response(400, json={
        "error": {
            "message": "you must provide a messages parameter",
            "type": "invalid_request_error",
            "param": "message",
            "code": "missing_required_parameter",
        },
    })

    assert should_fallback_to_chat_completions(
        response, {"model": "qwen", "input": "hi"},
    )


def test_does_not_fallback_on_unrelated_missing_required_parameter() -> None:
    response = httpx.Response(400, json={
        "error": {
            "message": "model is required",
            "type": "invalid_request_error",
            "param": "model",
            "code": "missing_required_parameter",
        },
    })

    assert not should_fallback_to_chat_completions(
        response, {"model": "qwen", "input": "hi"},
    )
