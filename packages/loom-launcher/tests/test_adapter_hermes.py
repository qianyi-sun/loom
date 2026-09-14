"""HermesAdapter contract: build_invocation + JSONL capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec
from loom_launcher.adapters._hermes_runtime import (
    HERMES_AGENT_REF,
    HERMES_AGENT_REQUIREMENT,
    LOOM_LAUNCHER_REF,
)


def test_build_invocation_argv() -> None:
    adapter = get_adapter("hermes")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="do the thing",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="glm-5.2"),
        env=env,
    )
    assert argv == [
        "/opt/loom-agents/hermes/bin/python",
        "-m",
        "loom_launcher.hermes_runner",
        "--model",
        "glm-5.2",
        "--workdir",
        "/workspace",
        "--output",
        "jsonl",
        "--task",
        "do the thing",
    ]


def test_adapter_env_contract() -> None:
    adapter = get_adapter("hermes")
    assert adapter is not None
    assert adapter.endpoint_dialect == "openai_chat"
    assert adapter.api_key_env == "OPENAI_API_KEY"
    assert adapter.base_url_env == "OPENAI_BASE_URL"
    assert adapter.model_name_template == "{model_id}"
    assert adapter.supports_multi_turn is False


def test_install_script_thin_uv_never_install_sh() -> None:
    adapter = get_adapter("hermes")
    assert adapter is not None
    assert adapter.install_script is not None
    script = adapter.install_script
    assert "https://astral.sh/uv/0.11.21/install.sh" in script
    assert "uv python install 3.12" in script
    assert "uv venv --python 3.12 /opt/loom-agents/hermes" in script
    assert "/opt/loom-agents/hermes/bin/python" in script
    assert HERMES_AGENT_REF in script
    assert "/opt/src/hermes-agent" in script
    assert "uv pip install --python /opt/loom-agents/hermes/bin/python --no-cache-dir -e /opt/src/hermes-agent" in script
    # Requirement constant still documents the pinned git form for tooling/lint.
    assert HERMES_AGENT_REQUIREMENT.startswith("hermes-agent@git+")
    assert "from run_agent import AIAgent" in script
    assert (
        f"git+https://github.com/qianyi-sun/loom.git@{LOOM_LAUNCHER_REF}"
        "#subdirectory=packages/loom-launcher"
    ) in script
    assert "hermes-agent.nousresearch.com" not in script
    assert "nousresearch.com/install.sh" not in script
    assert "playwright" not in script.lower()
    assert "--break-system-packages" not in script
    # Skip-if-baked path
    assert "if [ -x /opt/loom-agents/hermes/bin/python ]" in script


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("hermes")
    assert adapter is not None
    handle = make_handle(
        stdout_chunks=[
            b'{"kind": "agent_thought", "content": "starting"}\n',
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
        {"kind": "agent_thought", "content": "starting"},
        {"kind": "result", "ok": True},
    ]
