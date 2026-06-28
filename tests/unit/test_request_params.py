from loom_llm_gateway.request_params import (
    coerce_request_params,
    legacy_request_params,
    normalize_request_params,
    sanitize_request_extras,
)


def test_normalize_request_params_keeps_generation_controls_and_omits_sensitive_payload() -> None:
    params = normalize_request_params(
        {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "secret prompt"}],
            "input": "raw prompt",
            "instructions": "system secret",
            "temperature": 0,
            "top_p": 0.2,
            "seed": 1234,
            "max_tokens": 512,
            "reasoning": {"effort": "low", "api_key": "sk-hidden"},
            "tool_choice": "auto",
            "stream": False,
            "headers": {"Authorization": "Bearer hidden"},
            "api_key": "sk-hidden",
            "extra_body": {
                "top_k": 40,
                "repetition_penalty": 1.1,
                "authorization": "Bearer hidden",
            },
        },
        defaults={"presence_penalty": 0},
    )

    assert params == {
        "status": "available",
        "parameters": {
            "temperature": 0,
            "top_p": 0.2,
            "seed": 1234,
            "max_tokens": 512,
            "presence_penalty": 0,
            "reasoning": {"effort": "low"},
            "tool_choice": "auto",
            "stream": False,
            "top_k": 40,
            "repetition_penalty": 1.1,
        },
    }


def test_legacy_request_params_marks_unavailable_rows_explicitly() -> None:
    assert legacy_request_params() == {
        "status": "unavailable_legacy",
        "parameters": {},
    }


def test_coerce_request_params_enforces_public_allowlist_on_stored_values() -> None:
    params = coerce_request_params(
        {
            "status": "available",
            "parameters": {
                "temperature": 0,
                "unknown_provider_payload": {"prompt": "secret"},
                "authorization": "Bearer hidden",
                "max_tokens": 128,
            },
        }
    )

    assert params == {
        "status": "available",
        "parameters": {
            "temperature": 0,
            "max_tokens": 128,
        },
    }


def test_sanitize_request_extras_preserves_allowed_controls_without_sensitive_payload() -> None:
    extras = sanitize_request_extras(
        {
            "temperature": 0,
            "top_p": 0.5,
            "seed": 1234,
            "messages": [{"role": "user", "content": "do not copy"}],
            "api_key": "sk-hidden",
            "headers": {"Authorization": "Bearer hidden"},
            "extra_body": {
                "top_k": 40,
                "repetition_penalty": 1.1,
                "prompt": "secret",
                "x-provider-ignored": "not a generation control",
            },
        }
    )

    assert extras == {
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "extra_body": {
            "top_k": 40,
            "repetition_penalty": 1.1,
        },
    }


def test_sanitize_request_extras_preserves_allowed_max_token_controls() -> None:
    extras = sanitize_request_extras(
        {
            "max_tokens": 7,
            "max_output_tokens": 8,
            "max_completion_tokens": 9,
            "min_tokens": 1,
            "api_token": "secret",
            "token": "secret",
            "extra_body": {
                "max_tokens": 11,
                "prompt_tokens": 12,
                "token": "secret",
            },
        }
    )

    assert extras == {
        "max_tokens": 7,
        "max_output_tokens": 8,
        "max_completion_tokens": 9,
        "min_tokens": 1,
        "extra_body": {
            "max_tokens": 11,
        },
    }
