"""ClaudeCodeAdapter contract: sh -c wrapper + stream-json capture.

Verifies Amendment A12.1: --print, no --workdir flag (wrapped in
sh -c "cd ... && claude ..."), non-interactive permissions,
telemetry+auto-update disabled via env.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv_uses_sh_c_cd_and_print() -> None:
    adapter = get_adapter("claude-code")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="anthropic", name="claude-sonnet-4"),
        env=env,
    )
    assert argv[:2] == ["sh", "-c"]
    cmd = argv[2]
    assert cmd.startswith("cd /workspace && ")
    assert "claude --verbose --output-format stream-json" in cmd
    assert "--model claude-sonnet-4" in cmd
    assert "--print 'solve fizzbuzz'" in cmd
    assert "--permission-mode bypassPermissions" in cmd
    # Telemetry / auto-update disabled via env, NOT flags.
    assert env["DISABLE_TELEMETRY"] == "1"
    assert env["CLAUDE_CODE_AUTO_UPDATE"] == "false"
    # Verify no --workdir / --instruction CLI flags slipped in.
    assert "--workdir" not in cmd
    assert "--instruction" not in cmd


def test_build_invocation_shell_escapes_instruction() -> None:
    """Instructions with quotes/specials must shell-escape cleanly."""
    adapter = get_adapter("claude-code")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="say 'hi'; rm -rf /",
        workdir=PurePosixPath("/work"),
        model=ModelSpec(provider="anthropic", name="claude-sonnet-4"),
        env=env,
    )
    # shlex.quote wraps in single-quotes, escaping embedded ones.
    assert "'\"'\"'" in argv[2] or "rm -rf /" in argv[2]
    # Crucially, the dangerous payload is inside the quoted region.
    assert "; rm" not in argv[2].split("'\"'\"'")[0].replace("--print '", "")


async def test_capture_via_stream_json(make_handle) -> None:
    adapter = get_adapter("claude-code")
    assert adapter is not None
    # Real claude-code stream-json shape: one JSON object per line, each with
    # a discriminating `type` field.
    handle = make_handle(
        stdout_chunks=[
            b'{"type": "assistant", "text": "Let me look at the code."}\n',
            b'{"type": "tool_use", "name": "Read", "input": {"path": "main.py"}}\n',
            b'{"type": "assistant", "text": "Done."}\n',
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
        {"type": "assistant", "text": "Let me look at the code."},
        {"type": "tool_use", "name": "Read", "input": {"path": "main.py"}},
        {"type": "assistant", "text": "Done."},
    ]
