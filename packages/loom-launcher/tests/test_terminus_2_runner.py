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
        "--model", "openai/claude-haiku-4-5",
        "--task", "make hello.txt",
        "--workdir", "/app",
    ])

    assert rc == 0
    lines = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    assert lines[0] == {
        "kind": "terminus2_start",
        "model": "openai/claude-haiku-4-5",
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
