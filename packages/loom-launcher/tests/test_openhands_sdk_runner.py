from __future__ import annotations

import json
from pathlib import Path

from types import SimpleNamespace

from loom_launcher import openhands_sdk_runner


def test_runner_invokes_openhands_sdk_and_emits_jsonl(monkeypatch, tmp_path, capsys) -> None:
    calls: dict[str, object] = {}

    class FakeEvent:
        def model_dump(self, *, mode: str = "python") -> dict[str, object]:
            calls["event_mode"] = mode
            return {"message": "working"}

    class FakeTool:
        def __init__(self, *, name: str) -> None:
            self.name = name

    class FakeLLM:
        def __init__(self, **kwargs: object) -> None:
            calls["llm"] = kwargs

    class FakeAgent:
        def __init__(self, *, llm: FakeLLM, tools: list[FakeTool]) -> None:
            calls["agent_llm"] = llm
            calls["agent_tools"] = tools

    class FakeConversation:
        def __init__(self, agent: FakeAgent, **kwargs: object) -> None:
            calls["conversation_agent"] = agent
            calls["conversation"] = kwargs
            self._callbacks = kwargs["callbacks"]
            self.state = SimpleNamespace(events=[])

        def send_message(self, message: str) -> None:
            calls["message"] = message

        def run(self) -> None:
            event = FakeEvent()
            self.state.events = [event]
            for callback in self._callbacks:
                callback(event)

    monkeypatch.setenv("LLM_API_KEY", "step-token")
    monkeypatch.setenv("LLM_BASE_URL", "https://gateway.example/v1")
    monkeypatch.setenv("LOOM_TRIAL_ID", "00000000-0000-4000-8000-000000000099")
    monkeypatch.setenv("LOOM_STEP_ID", "main")
    monkeypatch.setattr(
        openhands_sdk_runner,
        "_load_sdk_types",
        lambda: (FakeLLM, FakeAgent, FakeConversation, FakeTool),
    )
    monkeypatch.setattr(
        openhands_sdk_runner,
        "_load_default_tools",
        lambda tool_type: [
            tool_type(name="terminal"),
            tool_type(name="file_editor"),
            tool_type(name="task_tracker"),
        ],
    )

    rc = openhands_sdk_runner.main(
        [
            "--model",
            "openai/test-model",
            "--workdir",
            str(tmp_path),
            "--output",
            "jsonl",
            "--task",
            "solve it",
            "--max-iterations",
            "3",
        ]
    )

    assert rc == 0
    assert calls["llm"] == {
        "model": "openai/test-model",
        "api_key": "step-token",
        "base_url": "https://gateway.example/v1",
    }
    assert [tool.name for tool in calls["agent_tools"]] == [
        "terminal",
        "file_editor",
        "task_tracker",
    ]
    assert calls["conversation"]["workspace"] == Path(tmp_path)
    assert calls["conversation"]["max_iteration_per_run"] == 3
    assert calls["conversation"]["visualizer"] is None
    assert calls["message"] == "solve it"

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["kind"] == "agent_thought"
    assert lines[0]["content"] == "status: openhands-sdk runner started"
    assert lines[1]["kind"] == "agent_thought"
    assert "FakeEvent" in lines[1]["content"]
    assert lines[-1]["kind"] == "agent_thought"
    assert lines[-1]["content"] == "result: ok"
    assert any(line["kind"] == "openhands_sdk_runtime_provenance" for line in lines)
    artifact_refs = [
        line for line in lines if line["kind"] == "openhands_sdk_artifact_ref"
    ]
    assert len(artifact_refs) == 1
    assert artifact_refs[0]["artifact_kind"] == "openhands_sdk.events"
    assert artifact_refs[0]["sandbox_path"] == ".loom/agent/openhands_sdk_events.json"
    native_path = tmp_path / ".loom" / "agent" / "openhands_sdk_events.json"
    assert native_path.exists()


def test_runner_requires_jsonl_output(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.setenv("LLM_API_KEY", "step-token")

    rc = openhands_sdk_runner.main(
        [
            "--model",
            "openai/test-model",
            "--workdir",
            str(tmp_path),
            "--output",
            "text",
            "--task",
            "solve it",
        ]
    )

    assert rc == 2
    assert "only --output jsonl is supported" in capsys.readouterr().err


def test_runner_reports_missing_openhands_sdk(monkeypatch, tmp_path, capsys) -> None:
    def raise_missing(name: str) -> object:
        assert name == "openhands.sdk"
        raise ImportError("missing")

    monkeypatch.setenv("LLM_API_KEY", "step-token")
    monkeypatch.setattr(openhands_sdk_runner.importlib, "import_module", raise_missing)

    rc = openhands_sdk_runner.main(
        [
            "--model",
            "openai/test-model",
            "--workdir",
            str(tmp_path),
            "--output",
            "jsonl",
            "--task",
            "solve it",
        ]
    )

    assert rc == 2
    assert "openhands-sdk is required" in capsys.readouterr().err


def test_runner_reports_missing_openhands_tools(monkeypatch, tmp_path, capsys) -> None:
    class FakeTool:
        def __init__(self, *, name: str) -> None:
            self.name = name

    class FakeLLM:
        def __init__(self, **kwargs: object) -> None:
            pass

    class FakeAgent:
        def __init__(self, *, llm: FakeLLM, tools: list[FakeTool]) -> None:
            pass

    class FakeConversation:
        def __init__(self, agent: FakeAgent, **kwargs: object) -> None:
            pass

        def send_message(self, message: str) -> None:
            pass

        def run(self) -> None:
            pass

    monkeypatch.setenv("LLM_API_KEY", "step-token")
    monkeypatch.setattr(
        openhands_sdk_runner,
        "_load_sdk_types",
        lambda: (FakeLLM, FakeAgent, FakeConversation, FakeTool),
    )

    def raise_missing_tools(_tool_type: type[FakeTool]) -> list[FakeTool]:
        raise RuntimeError(
            "openhands-tools is required for the openhands-sdk adapter; "
            "install openhands-tools at the same version as openhands-sdk"
        )

    monkeypatch.setattr(openhands_sdk_runner, "_load_default_tools", raise_missing_tools)

    rc = openhands_sdk_runner.main(
        [
            "--model",
            "openai/test-model",
            "--workdir",
            str(tmp_path),
            "--output",
            "jsonl",
            "--task",
            "solve it",
        ]
    )

    assert rc == 2
    assert "openhands-tools is required" in capsys.readouterr().err
