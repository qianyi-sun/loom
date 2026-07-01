"""Terminus-2 runner JSONL contract + LocalContainer subprocess shim.

The runner runs INSIDE the sandbox and replaces upstream's docker-py
Container (used by `TmuxSession.exec_run`) with a local subprocess
shim. These tests pin:
- The LocalContainer shim returns an `exec_code`/`output` shape that
  upstream's TmuxSession will accept.
- The runner emits a stable JSONL contract on stdout when the agent
  loop succeeds or raises.

Upstream `Terminus` and `LiteLLM` are mocked here because they require
the install-script venv (terminal-bench-core, litellm, tmux) that
isn't available in launcher unit tests.
"""

from __future__ import annotations

import io
import json
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom_launcher import terminus_2_runner


def test_local_container_exec_run_list_cmd() -> None:
    """`TmuxSession` calls `container.exec_run([...])` with explicit
    argv — no shell. Our shim must NOT wrap in bash -c for the list
    case or tmux options like `-V` will be misparsed."""
    container = terminus_2_runner.LocalContainer()
    # echo is part of POSIX coreutils on every supported sandbox image.
    result = container.exec_run(["echo", "tmux-2.0"])
    assert result.exit_code == 0
    assert result.output.strip() == b"tmux-2.0"


def test_local_container_exec_run_string_cmd_uses_bash() -> None:
    """Upstream sometimes passes a string (e.g. a piped command);
    docker-py wraps in /bin/sh -c, which translates to bash -lc for us
    so login-style PATH (e.g. /opt/loom-agents/.../bin) is honored.
    Confirm the shell interprets pipe-style compound commands."""
    container = terminus_2_runner.LocalContainer()
    result = container.exec_run("echo loom-marker | tr a-z A-Z")
    assert result.exit_code == 0
    assert b"LOOM-MARKER" in result.output


def test_patch_litellm_extracts_tool_use_when_content_empty() -> None:
    """Real cluster smoke of terminus-2 hit
    `terminal_bench.llms.base_llm.ParseError: Failed to parse LLM
    response` (trial 0d546c41). Root cause: LiteLLM's Anthropic dialect
    routes `response_format=CommandBatchResponse` through Anthropic's
    tool_use, which puts the JSON in
    `message.tool_calls[0].function.arguments` — NOT `message.content`.
    Upstream `LiteLLM.call` returns `content` unconditionally so
    Terminus's `CommandBatchResponse.model_validate_json` receives an
    empty string.

    Our runner monkey-patches upstream `LiteLLM.call` to fall back to
    the tool_use arguments when content is empty. This test simulates
    the exact response shape LiteLLM produces for Anthropic tool_use +
    asserts the patched call returns the arguments JSON."""

    class _FakeMessage:
        def __init__(self, content: str, tool_calls: list[object]) -> None:
            self.content = content
            self.tool_calls = tool_calls

    class _FakeFunction:
        def __init__(self, arguments: str) -> None:
            self.arguments = arguments

    class _FakeToolCall:
        def __init__(self, arguments: str) -> None:
            self.function = _FakeFunction(arguments)

    class _FakeResponse:
        def __init__(self, message: _FakeMessage) -> None:
            self.choices = [type("_Choice", (), {"message": message, "finish_reason": "stop"})()]

        def __getitem__(self, key: str) -> object:
            if key == "choices":
                return self.choices
            raise KeyError(key)

    # Build a fake `terminal_bench.llms.lite_llm` module with the
    # symbols the patcher needs. Upstream `LiteLLM.call` calls
    # `completion(...)` from its module namespace then returns
    # `choices[0].message.content`. We mirror that (via a closure over
    # `fake_module` so the patcher's completion-swap is what our fake
    # `call` sees).
    def _make_fake_module() -> Any:
        fake_module = type("_M", (), {})()

        class _FakeLiteLLM:
            def call(self, prompt: str, *_args: object, **_kwargs: object) -> str:
                # Look up completion dynamically from the module so the
                # patcher's swap is what we see — mirrors upstream's
                # `response = completion(...)` line.
                response = fake_module.completion(prompt=prompt)
                return response["choices"][0].message.content

        def _fake_completion(**_kwargs: object) -> _FakeResponse:
            return _FakeResponse(
                _FakeMessage(
                    content="",
                    tool_calls=[
                        _FakeToolCall(arguments='{"commands": [], "is_task_complete": true}'),
                    ],
                ),
            )

        fake_module.LiteLLM = _FakeLiteLLM
        fake_module.completion = _fake_completion
        return fake_module

    fake_module = _make_fake_module()
    terminus_2_runner._patch_litellm_response_extraction(fake_module)

    result = fake_module.LiteLLM().call("some prompt")
    assert result == '{"commands": [], "is_task_complete": true}'


def test_patch_litellm_is_idempotent() -> None:
    """Re-running the patcher (e.g. when the runner is imported twice
    in the same process) must not stack-wrap `LiteLLM.call`. Uses the
    `_loom_patched` sentinel."""

    class _FakeLiteLLM2:
        def call(self, *_a: object, **_k: object) -> str:
            return "original"

    fake_module = type("_M2", (), {})()
    fake_module.LiteLLM = _FakeLiteLLM2
    fake_module.completion = lambda **_: {"choices": []}

    terminus_2_runner._patch_litellm_response_extraction(fake_module)
    once = _FakeLiteLLM2.call

    terminus_2_runner._patch_litellm_response_extraction(fake_module)
    twice = _FakeLiteLLM2.call

    assert once is twice


def test_patch_litellm_preserves_content_when_nonempty() -> None:
    """If upstream `content` is non-empty, the patch must not overwrite
    it with tool_use arguments — the openai path stays working."""

    class _FakeLiteLLM3:
        def call(self, *_a: object, **_k: object) -> str:
            return "real content"

    fake_module = type("_M3", (), {})()
    fake_module.LiteLLM = _FakeLiteLLM3
    fake_module.completion = lambda **_: {"choices": []}

    terminus_2_runner._patch_litellm_response_extraction(fake_module)
    assert _FakeLiteLLM3().call("prompt") == "real content"


def test_local_container_put_archive_extracts_to_dir(tmp_path) -> None:
    """Upstream `TmuxSession.__init__` calls
    `DockerComposeManager.copy_to_container` which in turn calls
    `container.put_archive(container_dir, tar_bytes)` to inject the
    `get-asciinema-timestamp.sh` helper. Our shim extracts the tar
    locally because we ARE the target container. Regression for the
    real cluster failure `'LocalContainer' object has no attribute
    'put_archive'` (trial 63566218 pre-fix)."""
    import io
    import tarfile

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        payload = b"echo timestamp\n"
        info = tarfile.TarInfo(name="get-timestamp.sh")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    tar_bytes = buf.getvalue()

    dest = tmp_path / "container-dir"
    container = terminus_2_runner.LocalContainer()
    assert container.put_archive(str(dest), tar_bytes) is True

    extracted = dest / "get-timestamp.sh"
    assert extracted.exists()
    assert extracted.read_bytes() == b"echo timestamp\n"


def test_local_container_put_archive_returns_false_on_bad_tar() -> None:
    """docker-py's put_archive convention: return True/False, do not
    raise. Upstream `DockerComposeManager.copy_to_container` checks the
    return value in some code paths."""
    container = terminus_2_runner.LocalContainer()
    assert container.put_archive("/tmp/loom-test-put-archive-bad", b"not-a-tar") is False


def test_local_container_exec_run_missing_binary_returns_127() -> None:
    """A missing executable should produce a docker-py-shaped
    `ExecResult` with exit_code=127 (POSIX "command not found"), NOT
    raise FileNotFoundError — upstream TmuxSession catches the result
    code, not exceptions."""
    container = terminus_2_runner.LocalContainer()
    result = container.exec_run([
        "definitely-not-a-real-binary-loom-test-only",
        "-V",
    ])
    assert result.exit_code == 127


def test_runner_emits_start_and_end_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_session = MagicMock(name="TmuxSession")
    fake_agent = MagicMock(name="Terminus")
    fake_agent.perform_task.return_value = MagicMock(
        total_input_tokens=408,
        total_output_tokens=62,
        failure_mode="FailureMode.NONE",
        timestamped_markers=[(0.1, "marker-1"), (0.2, "marker-2")],
    )

    monkeypatch.setattr(
        terminus_2_runner, "_setup_tmux_session", lambda: fake_session,
    )
    monkeypatch.setattr(
        terminus_2_runner,
        "_build_agent",
        lambda model, max_episodes: fake_agent,
    )

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    rc = terminus_2_runner.main([
        "--model", "anthropic/claude-haiku-4-5",
        "--task", "make hello.txt",
        "--workdir", "/app",
    ])

    assert rc == 0
    lines = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    assert lines[0] == {
        "kind": "terminus2_start",
        "model": "anthropic/claude-haiku-4-5",
        "max_episodes": 50,
        "workdir": "/app",
    }
    assert lines[-1] == {
        "kind": "terminus2_end",
        "total_input_tokens": 408,
        "total_output_tokens": 62,
        "failure_mode": "FailureMode.NONE",
        "marker_count": 2,
    }
    # `perform_task` was called with the task + session — pin the
    # signature so a future Terminus signature bump (the public API
    # hasn't changed since the v0.1.1 pin) doesn't silently break.
    call_args = fake_agent.perform_task.call_args
    assert call_args.kwargs["task_description"] == "make hello.txt"
    assert call_args.kwargs["session"] is fake_session


def test_runner_emits_error_on_setup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Setup-time failure (e.g. tmux missing from the install script,
    upstream import error) must surface as a structured
    `terminus2_error` JSONL line, not an uncaught traceback. The
    worker's capture loop reads JSONL — anything else is lost."""
    def _boom() -> Any:
        raise RuntimeError("tmux not installed")

    monkeypatch.setattr(terminus_2_runner, "_setup_tmux_session", _boom)
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    rc = terminus_2_runner.main([
        "--model", "openai/x",
        "--task", "t",
        "--workdir", "/app",
    ])

    assert rc == 1
    lines = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    error_event = next(e for e in lines if e["kind"] == "terminus2_error")
    assert error_event["error_type"] == "RuntimeError"
    assert error_event["message"] == "tmux not installed"


def test_propagate_dialect_env_mirrors_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LiteLLM checks `OPENAI_API_BASE` first for the openai dialect;
    the subprocess agent only sets `OPENAI_BASE_URL`. Mirror the value
    so LiteLLM-backed Terminus actually reaches the gateway."""
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.setenv("OPENAI_BASE_URL", "https://loom-gateway.local")
    terminus_2_runner._propagate_dialect_env()
    assert os_env("OPENAI_API_BASE") == "https://loom-gateway.local"


def test_propagate_dialect_env_preserves_existing_api_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_BASE_URL", "https://loom-gateway.local")
    monkeypatch.setenv("OPENAI_API_BASE", "https://operator-override")
    terminus_2_runner._propagate_dialect_env()
    assert os_env("OPENAI_API_BASE") == "https://operator-override"


def os_env(name: str) -> str | None:
    import os
    return os.environ.get(name)
