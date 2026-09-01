"""OpenHandsAdapter contract: SDK-backed invocation + JSONL capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec
from loom_launcher.adapters._openhands_runtime import (
    LOOM_LAUNCHER_REF,
    OPENHANDS_SDK_VERSION,
)


def test_build_invocation_argv() -> None:
    adapter = get_adapter("openhands")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="solve it",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == [
        "/opt/loom-agents/openhands-sdk/bin/python",
        "-m",
        "loom_launcher.openhands_sdk_runner",
        "--model",
        "openai/gpt-5",
        "--workdir",
        "/workspace",
        "--output",
        "jsonl",
        "--task",
        "solve it",
    ]


def test_build_invocation_argv_terminus_style_env() -> None:
    adapter = get_adapter("openhands")
    assert adapter is not None
    argv = adapter.build_invocation(
        instruction="solve it",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env={"LOOM_OPENHANDS_TERMINUS_STYLE": "true"},
    )
    assert argv[-1] == "--terminus-style"


def test_install_script_uses_managed_python312_venv() -> None:
    adapter = get_adapter("openhands")
    assert adapter is not None
    assert adapter.install_script is not None
    assert "https://astral.sh/uv/0.11.21/install.sh" in adapter.install_script
    assert "uv python install 3.12" in adapter.install_script
    assert "uv venv --python 3.12 /opt/loom-agents/openhands-sdk" in adapter.install_script
    assert "/opt/loom-agents/openhands-sdk/bin/python" in adapter.install_script
    assert f"openhands-tools=={OPENHANDS_SDK_VERSION}" in adapter.install_script
    assert "tmux" in adapter.install_script
    assert (
        f"git+https://github.com/qianyi-sun/loom.git@{LOOM_LAUNCHER_REF}"
        "#subdirectory=packages/loom-launcher"
    ) in adapter.install_script
    assert "--break-system-packages" not in adapter.install_script


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("openhands")
    assert adapter is not None
    handle = make_handle(
        stdout_chunks=[
            b'{"kind": "thought", "text": "starting"}\n',
            b'{"kind": "result", "ok": true}\n',
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
        {"kind": "thought", "text": "starting"},
        {"kind": "result", "ok": True},
    ]
