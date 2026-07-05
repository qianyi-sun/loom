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


def test_local_container_exec_run_emits_bounded_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout / reward-0 canaries need command-level evidence, but
    command stdout may contain sensitive task output. Emit only bounded
    metadata about each local exec."""
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    container = terminus_2_runner.LocalContainer()
    result = container.exec_run("echo diagnostic")

    assert result.exit_code == 0
    event = json.loads(buf.getvalue().splitlines()[-1])
    assert event["kind"] == "terminus2_exec_run"
    assert event["cmd_excerpt"] == "echo diagnostic"
    assert event["exit_code"] == 0
    assert event["output_len"] == len(result.output)
    assert isinstance(event["duration_sec"], float)


def test_patch_asciinema_timestamp_replaces_method() -> None:
    """Upstream `TmuxSession.get_asciinema_timestamp` reads from a
    `.cast` file recorded by asciinema, which the Loom sandbox does
    not produce. Patch replaces it with a wall-clock stub so
    Terminus's marker-timestamping side-effect doesn't crash the
    agent loop. Regression for trial 5548dfa7."""
    class _FakeTmuxSession:
        def get_asciinema_timestamp(self) -> float:
            raise RuntimeError("real asciinema reader — should be replaced")

    fake_module = type("_M", (), {})()
    fake_module.TmuxSession = _FakeTmuxSession
    terminus_2_runner._patch_asciinema_timestamp(fake_module, lambda: 12345.678)

    session = _FakeTmuxSession()
    assert session.get_asciinema_timestamp() == 12345.678


def test_patch_asciinema_timestamp_is_idempotent() -> None:
    class _FakeTmuxSession2:
        def get_asciinema_timestamp(self) -> float:
            return -1.0

    fake_module = type("_M2", (), {})()
    fake_module.TmuxSession = _FakeTmuxSession2

    terminus_2_runner._patch_asciinema_timestamp(fake_module, lambda: 1.0)
    once = _FakeTmuxSession2.get_asciinema_timestamp

    terminus_2_runner._patch_asciinema_timestamp(fake_module, lambda: 2.0)
    twice = _FakeTmuxSession2.get_asciinema_timestamp

    assert once is twice


def test_try_extract_json_object_returns_naked_json_unchanged() -> None:
    text = '{"commands": [], "is_task_complete": true}'
    assert terminus_2_runner._try_extract_json_object(text) == text


def test_try_extract_json_object_strips_markdown_fence() -> None:
    """Common Haiku / Sonnet failure mode under LiteLLM's prompt-based
    structured output: wrap the JSON in ```json ... ```."""
    text = "```json\n{\"commands\": [], \"is_task_complete\": true}\n```"
    assert terminus_2_runner._try_extract_json_object(text) == (
        '{"commands": [], "is_task_complete": true}'
    )


def test_try_extract_json_object_strips_prefix_narration() -> None:
    text = "Sure! Here's the JSON response:\n{\"commands\": [\"ls\"]}\nHope that helps."
    assert terminus_2_runner._try_extract_json_object(text) == (
        '{"commands": ["ls"]}'
    )


def test_try_extract_json_object_handles_nested_objects() -> None:
    """Real CommandBatchResponse has nested `commands: [{...}, {...}]`
    with `Command` objects. Depth-balance must span nested braces."""
    text = '{"commands": [{"keystrokes": "ls", "is_blocking": true}], "is_task_complete": false}'
    assert terminus_2_runner._try_extract_json_object(text) == text


def test_try_extract_json_object_ignores_braces_inside_strings() -> None:
    """A `}` inside a JSON string literal must not close the outer
    object early."""
    text = '{"cmd": "echo }not-a-brace{"}'
    assert terminus_2_runner._try_extract_json_object(text) == text


def test_try_extract_json_object_returns_none_when_no_object() -> None:
    assert terminus_2_runner._try_extract_json_object("just plain text") is None
    assert terminus_2_runner._try_extract_json_object("") is None


def test_normalize_command_batch_response_keeps_valid_batch() -> None:
    text = (
        '{"state_analysis": "ready", "explanation": "inspect", '
        '"commands": [{"keystrokes": "ls", "is_blocking": true, "timeout_sec": 5}], '
        '"is_task_complete": false}'
    )

    assert terminus_2_runner._normalize_command_batch_response_json(text) == text


def test_normalize_command_batch_response_defaults_missing_completion() -> None:
    """GLM sometimes returns a nearly-valid CommandBatchResponse but
    omits `is_task_complete`, which makes upstream Terminus abort with
    a Pydantic validation error. Treat that as an incomplete step so
    the agent can continue instead of failing the trial."""
    result = terminus_2_runner._normalize_command_batch_response_json(
        '{"state_analysis": "need inspect", "explanation": "list files", '
        '"commands": [{"keystrokes": "ls -la /app", "is_blocking": true, '
        '"timeout_sec": 5}]}',
    )

    assert json.loads(result) == {
        "state_analysis": "need inspect",
        "explanation": "list files",
        "commands": [{
            "keystrokes": "ls -la /app",
            "is_blocking": True,
            "timeout_sec": 5,
        }],
        "is_task_complete": False,
    }


def test_normalize_command_batch_response_wraps_single_command() -> None:
    """The failed 5005 run saw GLM return a single Command-shaped
    object at the top level (`keystrokes`, `is_blocking`,
    `timeout_sec`) instead of a CommandBatchResponse. Wrap it as one
    incomplete command batch so Terminus can execute it."""
    result = terminus_2_runner._normalize_command_batch_response_json(
        '{"keystrokes": "pytest -q", "is_blocking": true, "timeout_sec": 30}',
    )

    assert json.loads(result) == {
        "state_analysis": "Model returned a single command object.",
        "explanation": "Executing the command returned by the model.",
        "commands": [{
            "keystrokes": "pytest -q",
            "is_blocking": True,
            "timeout_sec": 30,
        }],
        "is_task_complete": False,
    }


def test_normalize_command_batch_response_wraps_command_strings() -> None:
    result = terminus_2_runner._normalize_command_batch_response_json(
        '{"state_analysis": "need tests", "explanation": "run tests", '
        '"commands": ["pytest -q"], "is_task_complete": false}',
    )

    assert json.loads(result) == {
        "state_analysis": "need tests",
        "explanation": "run tests",
        "commands": [{
            "keystrokes": "pytest -q",
            "is_blocking": True,
            "timeout_sec": 5,
        }],
        "is_task_complete": False,
    }


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
    events: list[str] = []
    fake_session = MagicMock(name="TmuxSession")
    fake_session._session_name = "loom-terminus-2"
    fake_session.container.exec_run.return_value = terminus_2_runner._ExecResult(
        exit_code=0,
        output=b"ready",
    )
    fake_session.start.side_effect = lambda: events.append("start")
    fake_session.stop.side_effect = lambda: events.append("stop")
    fake_agent = MagicMock(name="Terminus")

    def _perform_task(**_: object) -> MagicMock:
        assert events == ["start"]
        events.append("perform")
        return MagicMock(
            total_input_tokens=408,
            total_output_tokens=62,
            failure_mode="FailureMode.NONE",
            timestamped_markers=[(0.1, "marker-1"), (0.2, "marker-2")],
        )

    fake_agent.perform_task.side_effect = _perform_task

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
        "--model", "openai/glm-5.1-thinking",
        "--task", "make hello.txt",
        "--workdir", "/app",
    ])

    assert rc == 0
    assert events == ["start", "perform", "stop"]
    fake_session.start.assert_called_once_with()
    fake_session.stop.assert_called_once_with()
    lines = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    assert lines[0] == {
        "kind": "terminus2_start",
        "model": "openai/glm-5.1-thinking",
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


def test_runner_recovers_when_upstream_start_does_not_create_tmux_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live #445 evidence showed `session.start()` returned but the
    first `capture-pane -t loom-terminus-2` still failed. The runner
    must verify the actual tmux session before letting Terminus spend
    provider calls against a broken control channel."""
    events: list[str] = []
    commands: list[list[str] | str] = []

    class _FakeContainer:
        ready = False

        def exec_run(self, cmd: list[str] | str, *_: object, **__: object) -> object:
            commands.append(cmd)
            if (
                isinstance(cmd, list)
                and cmd[:3] == ["tmux", "capture-pane", "-p"]
            ):
                return terminus_2_runner._ExecResult(
                    exit_code=0 if self.ready else 1,
                    output=b"ready" if self.ready else b"can't find session",
                )
            if (
                isinstance(cmd, list)
                and cmd[:2] == ["tmux", "new-session"]
            ):
                self.ready = True
                return terminus_2_runner._ExecResult(exit_code=0, output=b"")
            return terminus_2_runner._ExecResult(exit_code=0, output=b"")

    fake_container = _FakeContainer()
    fake_session = MagicMock(name="TmuxSession")
    fake_session._session_name = "loom-terminus-2"
    fake_session.container = fake_container
    fake_session.start.side_effect = lambda: events.append("start")
    fake_session.stop.side_effect = lambda: events.append("stop")
    fake_agent = MagicMock(name="Terminus")

    def _perform_task(**_: object) -> MagicMock:
        assert fake_container.ready is True
        events.append("perform")
        return MagicMock(
            total_input_tokens=0,
            total_output_tokens=0,
            failure_mode="FailureMode.NONE",
            timestamped_markers=[],
        )

    fake_agent.perform_task.side_effect = _perform_task

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
        "--model", "openai/glm-5.1-thinking",
        "--task", "make hello.txt",
        "--workdir", "/app",
    ])

    assert rc == 0
    assert events == ["start", "perform", "stop"]
    assert ["tmux", "capture-pane", "-p", "-t", "loom-terminus-2"] in commands
    assert [
        "tmux",
        "new-session",
        "-x",
        "160",
        "-y",
        "40",
        "-d",
        "-s",
        "loom-terminus-2",
    ] in commands
    lines = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    assert any(e["kind"] == "terminus2_session_recovered" for e in lines)


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


def test_runner_stops_started_session_after_agent_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_session = MagicMock(name="TmuxSession")
    fake_session._session_name = "loom-terminus-2"
    fake_session.container.exec_run.return_value = terminus_2_runner._ExecResult(
        exit_code=0,
        output=b"ready",
    )
    fake_agent = MagicMock(name="Terminus")
    fake_agent.perform_task.side_effect = RuntimeError("agent crashed")

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
        "--model", "openai/x",
        "--task", "t",
        "--workdir", "/app",
    ])

    assert rc == 1
    fake_session.start.assert_called_once_with()
    fake_session.stop.assert_called_once_with()
    lines = [json.loads(line) for line in buf.getvalue().splitlines() if line]
    error_event = next(e for e in lines if e["kind"] == "terminus2_error")
    assert error_event["error_type"] == "RuntimeError"
    assert error_event["message"] == "agent crashed"


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
