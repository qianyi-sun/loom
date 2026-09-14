from __future__ import annotations

import json
import os

from loom_launcher import hermes_runner


def test_runner_invokes_aiagent_and_emits_jsonl(monkeypatch, tmp_path, capsys) -> None:
    calls: dict[str, object] = {}

    class FakeAIAgent:
        def __init__(self, **kwargs: object) -> None:
            calls["agent_kwargs"] = kwargs

        def run_conversation(self, task: str) -> dict[str, object]:
            calls["task"] = task
            return {
                "final_response": "done",
                "session_id": "sess-test",
                "messages": [
                    {"role": "user", "content": task},
                    {
                        "role": "assistant",
                        "content": "listing",
                        "reasoning_content": "use terminal",
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "function": {
                                    "name": "terminal",
                                    "arguments": '{"command": "ls"}',
                                },
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": "call-1",
                        "content": "a.txt\n",
                    },
                    {"role": "assistant", "content": "done"},
                ],
            }

    monkeypatch.setenv("OPENAI_API_KEY", "step-token")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://gateway.example/v1")
    monkeypatch.setenv("LOOM_TRIAL_ID", "00000000-0000-4000-8000-000000000099")
    monkeypatch.setenv("LOOM_STEP_ID", "main")
    monkeypatch.setattr(hermes_runner, "_load_ai_agent", lambda: FakeAIAgent)

    rc = hermes_runner.main(
        [
            "--model",
            "glm-5.2",
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
    assert calls["task"] == "solve it"
    assert calls["agent_kwargs"] == {
        "provider": "custom",
        "base_url": "https://gateway.example/v1",
        "api_key": "step-token",
        "model": "glm-5.2",
        "enabled_toolsets": ["terminal", "file"],
        "quiet_mode": True,
        "skip_memory": True,
        "skip_context_files": True,
        "max_iterations": 3,
    }
    assert os_environ_has_yolo()

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["kind"] == "agent_thought"
    assert lines[0]["content"] == "status: hermes runner started"
    assert any(line["kind"] == "tool_use" for line in lines)
    assert any(line["kind"] == "hermes_runtime_provenance" for line in lines)
    artifact_refs = [line for line in lines if line["kind"] == "hermes_artifact_ref"]
    assert len(artifact_refs) == 1
    assert artifact_refs[0]["artifact_kind"] == "hermes.session"
    assert artifact_refs[0]["sandbox_path"] == ".loom/agent/hermes_session.json"
    native_path = tmp_path / ".loom" / "agent" / "hermes_session.json"
    assert native_path.exists()
    assert lines[-1]["content"] == "result: ok"


def os_environ_has_yolo() -> bool:
    import os

    return os.environ.get("HERMES_YOLO_MODE") == "1" and os.environ.get("TERMINAL_ENV") == "local"


def test_runner_requires_openai_env(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    rc = hermes_runner.main(
        [
            "--model",
            "m",
            "--workdir",
            str(tmp_path),
            "--output",
            "jsonl",
            "--task",
            "t",
        ]
    )
    assert rc == 2
    assert "OPENAI_API_KEY is required" in capsys.readouterr().err


def test_runner_missing_hermes_package(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://gateway.example/v1")

    def _boom() -> type:
        raise RuntimeError("hermes-agent is required for the hermes adapter")

    monkeypatch.setattr(hermes_runner, "_load_ai_agent", _boom)
    rc = hermes_runner.main(
        [
            "--model",
            "m",
            "--workdir",
            str(tmp_path),
            "--output",
            "jsonl",
            "--task",
            "t",
        ]
    )
    assert rc == 2
    assert "hermes-agent is required" in capsys.readouterr().err
