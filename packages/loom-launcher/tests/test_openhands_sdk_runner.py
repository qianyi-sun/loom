from __future__ import annotations

import json
from pathlib import Path

from loom_launcher import openhands_sdk_runner


def test_runner_invokes_openhands_sdk_and_emits_jsonl(monkeypatch, tmp_path, capsys) -> None:
    calls: dict[str, object] = {}

    class FakeEvent:
        def model_dump(self, *, mode: str = "python") -> dict[str, object]:
            calls["event_mode"] = mode
            return {"message": "working"}

    class FakeLLM:
        def __init__(self, **kwargs: object) -> None:
            calls["llm"] = kwargs

    class FakeAgent:
        def __init__(self, *, llm: FakeLLM) -> None:
            calls["agent_llm"] = llm

    class FakeConversation:
        def __init__(self, agent: FakeAgent, **kwargs: object) -> None:
            calls["conversation_agent"] = agent
            calls["conversation"] = kwargs
            self._callbacks = kwargs["callbacks"]

        def send_message(self, message: str) -> None:
            calls["message"] = message

        def run(self) -> None:
            for callback in self._callbacks:
                callback(FakeEvent())

    monkeypatch.setenv("LLM_API_KEY", "step-token")
    monkeypatch.setenv("LLM_BASE_URL", "https://gateway.example/v1")
    monkeypatch.setattr(
        openhands_sdk_runner,
        "_load_sdk_types",
        lambda: (FakeLLM, FakeAgent, FakeConversation),
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
    assert calls["conversation"]["workspace"] == Path(tmp_path)
    assert calls["conversation"]["max_iteration_per_run"] == 3
    assert calls["conversation"]["visualizer"] is None
    assert calls["message"] == "solve it"

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines == [
        {"kind": "status", "message": "openhands-sdk runner started"},
        {
            "kind": "openhands_sdk_event",
            "event_type": "FakeEvent",
            "event": {"message": "working"},
        },
        {"kind": "result", "ok": True},
    ]


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
