"""Terminus2Adapter contract: registration, build_invocation, capture.

The runner module itself relies on `terminal-bench` being
installed in the sandbox venv at trial time — not available in the
loom_launcher unit test environment, so we don't import it here.
The CLI-shape, install-script, and JSONL capture contract is what's
worth pinning at this layer.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec
from loom_launcher.adapters._terminus_2_runtime import (
    LOOM_LAUNCHER_REF,
    LOOM_LAUNCHER_REQUIREMENT,
)


def test_terminus_2_is_registered() -> None:
    adapter = get_adapter("terminus-2")
    assert adapter is not None
    assert adapter.name == "terminus-2"
    assert adapter.endpoint_dialect == "openai_chat"
    assert adapter.api_key_env == "OPENAI_API_KEY"
    assert adapter.base_url_env == "OPENAI_BASE_URL"
    assert adapter.supports_multi_turn is False


def test_build_invocation_argv() -> None:
    adapter = get_adapter("terminus-2")
    assert adapter is not None
    env: dict[str, str] = {"OPENAI_BASE_URL": "http://gateway"}
    argv = adapter.build_invocation(
        instruction="Create hello.txt with 'hi'",
        workdir=PurePosixPath("/app"),
        model=ModelSpec(provider="openai", name="glm-5.1-thinking"),
        env=env,
    )
    # Runner is invoked via the dedicated /opt venv python — same
    # pattern as openhands-sdk — to keep terminal-bench's
    # dependencies isolated from the task image's site-packages.
    assert argv[0] == "/opt/loom-agents/terminus-2/bin/python"
    assert argv[1] == "-m"
    assert argv[2] == "loom_launcher.terminus_2_runner"
    assert argv[3:] == [
        "--model",
        # LiteLLM dispatches `openai/<id>` through its openai-compatible
        # client → OPENAI_BASE_URL → Loom gateway. The model_id is the
        # bare upstream id, NOT the team-scoped slug.
        "openai/glm-5.1-thinking",
        "--workdir",
        "/app",
        "--task",
        "Create hello.txt with 'hi'",
    ]


def test_install_script_pins_terminal_bench_core() -> None:
    adapter = get_adapter("terminus-2")
    assert adapter is not None
    assert adapter.install_script is not None
    # Same upstream pin as Loom's TB-2 adapter
    # (packages/loom-benchmark-terminal-bench-2/.../upstream.py at
    # commit 91e10457). Bumping this requires moving both in lockstep
    # so the agent prompt + verifier semantics stay aligned.
    assert "91e10457b5410f16c44364da1a34cb6de8c488a5" in adapter.install_script
    assert "terminal-bench@git+" in adapter.install_script
    # Upstream TmuxSession shells out to tmux and requires asciinema
    # whenever Terminal-Bench recording is enabled.
    assert "tmux" in adapter.install_script
    assert "asciinema" in adapter.install_script


def test_install_script_pins_current_public_loom_launcher_ref() -> None:
    # This SHA must be reachable from qianyi-sun/loom so fresh GB10
    # agent-layer builds can install loom-launcher without relying on
    # a warm local cache.
    assert LOOM_LAUNCHER_REF == "4ce8225bf67e88d12dd86e4e1ab3f009f9d4eee2"
    assert f"qianyi-sun/loom.git@{LOOM_LAUNCHER_REF}" in LOOM_LAUNCHER_REQUIREMENT


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("terminus-2")
    assert adapter is not None
    handle = make_handle(
        stdout_chunks=[
            b'{"kind": "terminus2_start", "model": "openai/x", "max_episodes": 50}\n',
            b'{"kind": "terminus2_end", "total_input_tokens": 408, '
            b'"total_output_tokens": 62, "failure_mode": "FailureMode.NONE", '
            b'"marker_count": 3}\n',
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
    assert events[0] == {
        "kind": "terminus2_start",
        "model": "openai/x",
        "max_episodes": 50,
    }
    assert events[1]["kind"] == "terminus2_end"
    assert events[1]["total_input_tokens"] == 408
