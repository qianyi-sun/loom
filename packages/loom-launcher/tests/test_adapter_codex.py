"""CodexAdapter contract: build_invocation + JSONL stdout capture."""

from __future__ import annotations

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
