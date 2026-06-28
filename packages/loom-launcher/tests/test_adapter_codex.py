"""CodexAdapter contract: build_invocation + JSONL stdout capture."""

from __future__ import annotations

import json
from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("codex")
    assert adapter is not None
    env: dict[str, str] = {
        "OPENAI_API_KEY": "step-token",
        "OPENAI_BASE_URL": "http://gateway",
    }
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv[:3] == ["sh", "-c", argv[2]]
    assert "codex exec --ignore-user-config --json" in argv[2]
    assert "printf '%s' \"$4\" | exec codex exec" in argv[2]
    assert ' "$4" </dev/null' not in argv[2]
    assert argv[2].endswith(' -')
    assert argv[3:] == [
        "loom-codex",
        "gpt-5",
        "/workspace",
        (
            'model_providers.loom={ name = "Loom", '
            'base_url = "http://gateway/v1", env_key = "OPENAI_API_KEY", '
            'wire_api = "responses" }'
        ),
        "solve fizzbuzz",
    ]
    # Under the trial workdir so codex 0.141+ doesn't refuse to
    # write helper binaries (it rejects /tmp-rooted CODEX_HOME).
    assert env["CODEX_HOME"] == "/workspace/.codex-home"


def test_build_invocation_keeps_existing_v1_base_url() -> None:
    adapter = get_adapter("codex")
    assert adapter is not None
    env: dict[str, str] = {
        "OPENAI_API_KEY": "step-token",
        "OPENAI_BASE_URL": "http://gateway/openai/v1",
    }
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )

    assert 'base_url = "http://gateway/openai/v1"' in argv[6]
    assert 'base_url = "http://gateway/openai/v1/v1"' not in argv[6]


def test_build_invocation_encodes_codex_settings_as_provider_query_params() -> None:
    adapter = get_adapter("codex")
    assert adapter is not None
    env: dict[str, str] = {
        "OPENAI_API_KEY": "step-token",
        "OPENAI_BASE_URL": "http://gateway",
        "LOOM_CODEX_SETTINGS_JSON": '{"temperature": 0, "top_p": 1, "seed": 42}',
    }
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )

    assert "--settings" not in argv[2]
    assert "$5" not in argv[2]
    assert argv[3:] == [
        "loom-codex",
        "gpt-5",
        "/workspace",
        (
            'model_providers.loom={ name = "Loom", '
            'base_url = "http://gateway/v1", env_key = "OPENAI_API_KEY", '
            'wire_api = "responses", query_params = { '
            'loom_request_params = "{\\"temperature\\":0,'
            '\\"top_p\\":1,\\"seed\\":42}" } }'
        ),
        "solve fizzbuzz",
    ]


def test_build_invocation_sanitizes_codex_settings_before_query_params() -> None:
    adapter = get_adapter("codex")
    assert adapter is not None
    env: dict[str, str] = {
        "OPENAI_API_KEY": "step-token",
        "OPENAI_BASE_URL": "http://gateway",
        "LOOM_CODEX_SETTINGS_JSON": json.dumps({
            "temperature": 0,
            "api_key": "sk-hidden",
            "messages": [{"role": "user", "content": "secret prompt"}],
            "extra_body": {
                "top_k": 40,
                "max_tokens": 11,
                "prompt": "secret prompt",
            },
        }),
    }
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )

    provider_config = argv[6]
    assert "temperature" in provider_config
    assert "top_k" in provider_config
    assert "max_tokens" in provider_config
    assert "api_key" not in provider_config
    assert "sk-hidden" not in provider_config
    assert "messages" not in provider_config
    assert "secret prompt" not in provider_config


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("codex")
    assert adapter is not None
    handle = make_handle(
        stdout_chunks=[
            b'{"type": "turn.started"}\n',
            b'{"type": "turn.completed", "usage": {"input_tokens": 1}}\n',
        ]
    )
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle,
            step_id="main",
            trial_id=uuid4(),
        )
    ]
    assert events == [
        {"type": "turn.started"},
        {"type": "turn.completed", "usage": {"input_tokens": 1}},
    ]
